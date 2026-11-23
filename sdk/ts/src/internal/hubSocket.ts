// The hub socket (spec/hub-socket.md), kept up while the device is
// enrolled, and the relay's asks over it (spec/relay.md): this device's
// asks of its peers, and its answers to theirs.
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { DialError } from "../Errors.ts";
import { type Conn, WebSockets } from "../Platform.ts";
import * as Base64Url from "../wire/base64url.ts";
import type { Connections } from "../wire/device.ts";
import { decodePublicKey, sameKey } from "../wire/deviceKey.ts";
import {
  CLOSE_DEVICE_REMOVED,
  CLOSE_SUPERSEDED,
  PING,
  PONG,
  RelayTo,
  ServiceMessage,
} from "../wire/hubSocket.ts";
import { type HandshakeState, type KeyPair, tryNoise } from "../wire/noise.ts";
import {
  type Answer,
  ASK_TIMEOUT_MS,
  type Ask,
  type ConnectInfoInput,
  decodeRelayFrame,
  encodeAnswer,
  encodeAsk,
  encodeRelayFrame,
  openAnswer,
  openAsk,
  type RelayFrame,
  SEEN_ASK_MS,
  sealAnswer,
  sealAsk,
} from "../wire/relay.ts";
import type * as HubApi from "./hubApi.ts";
import * as Liveness from "./liveness.ts";
import { reconnectLadder, type Run, type Wakes } from "./wake.ts";

// The first presence comes within this long of the socket opening, or
// the attempt failed. The ticket's mint and the socket's opening get as
// long each.
const ADMISSION_MS = 10_000;

// After the socket is admitted, a peer missing from the roster may be
// one still reconnecting, as every device does after the service
// restarts (spec/link.md, "When a peer goes").
const SETTLE_MS = 10_000;

export interface Peer {
  readonly deviceId: string;
  readonly publicKey: Uint8Array;
  readonly connections: Connections;
}

export interface Roster {
  // The device's peers as the latest presence named them, the device
  // itself left out.
  readonly peers: ReadonlyMap<string, Peer>;
  // Whether the settle period since the socket was admitted ended.
  readonly settled: boolean;
}

// Why the device stops, until the person acts (spec/hub-socket.md,
// "Close codes" and "Reconnecting").
export type Stop =
  | "removed"
  | "superseded"
  | "credential-rejected"
  | "update-required";

export interface Identity {
  readonly deviceId: string;
  readonly credential: string;
  readonly key: KeyPair;
}

interface Pending {
  readonly deviceId: string;
  readonly handshake: HandshakeState;
  readonly answered: Deferred.Deferred<Answer, DialError>;
}

// One admitted socket: what an ask goes out on, and the asks waiting for
// an answer on it.
interface Live {
  readonly conn: Conn;
  readonly pending: Map<string, Pending>;
  nextId: number;
}

// The peer as the latest roster names it, while the hub socket is up.
export const peerOf = (
  roster: SubscriptionRef.SubscriptionRef<Option.Option<Roster>>,
  deviceId: string,
): Option.Option<Peer> =>
  Option.flatMapNullishOr(SubscriptionRef.getUnsafe(roster), (now) =>
    now.peers.get(deviceId),
  );

const encodeRelay = Schema.encodeSync(Schema.fromJsonString(RelayTo));
const decodeMessage = Schema.decodeOption(
  Schema.fromJsonString(ServiceMessage),
);

const relayTo = (to: string, frame: RelayFrame) =>
  encodeRelay({ t: "relay", to, frame: encodeRelayFrame(frame) });

const connectUrl = (hub: string, ticket: string) =>
  `${hub.replace(/\/+$/, "").replace(/^http/, "ws")}/connect?ticket=${encodeURIComponent(ticket)}`;

const rosterOf = (
  online: ServiceMessage & { readonly t: "presence" },
  self: string,
): Roster["peers"] =>
  new Map(
    online.online.flatMap((device) =>
      device.deviceId === self
        ? []
        : Option.match(decodePublicKey(device.publicKey), {
            onNone: () => [],
            onSome: (publicKey) => [
              [
                device.deviceId,
                {
                  deviceId: device.deviceId,
                  publicKey,
                  connections: device.connections,
                },
              ] as const,
            ],
          }),
    ),
  );

// A call's error that stops the device as its close code would.
export const stopOfError = (error: unknown): Stop | null =>
  Predicate.isTagged(error, "DeviceRevoked")
    ? "removed"
    : Predicate.isTagged(error, "CredentialRejected")
      ? "credential-rejected"
      : Predicate.isTagged(error, "UpdateRequired")
        ? "update-required"
        : null;

const stopOf = (code: number): Stop | null =>
  code === CLOSE_DEVICE_REMOVED
    ? "removed"
    : code === CLOSE_SUPERSEDED
      ? "superseded"
      : null;

const failPending = (
  pending: Map<string, Pending>,
  which: (entry: Pending) => boolean,
  reason: DialError["reason"],
) => {
  for (const [id, entry] of pending) {
    if (!which(entry)) continue;
    pending.delete(id);
    Deferred.doneUnsafe(
      entry.answered,
      Exit.fail(new DialError({ deviceId: entry.deviceId, reason })),
    );
  }
};

const readAnswer = (
  pending: Map<string, Pending>,
  from: string,
  frame: RelayFrame,
) => {
  const entry = pending.get(frame.id);
  if (entry === undefined || entry.deviceId !== from) return;
  pending.delete(frame.id);
  Deferred.doneUnsafe(
    entry.answered,
    Option.match(openAnswer(entry.handshake, frame.sealed), {
      onNone: () =>
        Exit.fail(new DialError({ deviceId: from, reason: "unreachable" })),
      onSome: Exit.succeed,
    }),
  );
};

// An attempt that failed, and the least it waits before the next.
const failed = (waitAtLeastMs = 0): Run => ({ uptimeMs: 0, waitAtLeastMs });

export const make = Effect.fnUntraced(function* (options: {
  readonly hub: string;
  readonly call: HubApi.Call;
  readonly identity: Identity;
  // The socket's name to the service, the same on every reconnect.
  readonly connectionId: string;
  // Set by the socket: none while it is down.
  readonly roster: SubscriptionRef.SubscriptionRef<Option.Option<Roster>>;
  // This device's answer to a peer's ask that opened.
  readonly answer: (from: string, ask: Ask) => Effect.Effect<Answer>;
  readonly wakes: Wakes;
}) {
  const sockets = yield* WebSockets;
  const { identity, roster } = options;
  const stopped = yield* Deferred.make<Stop>();
  let live: Live | null = null;
  // The asks this device opened, by handshake hash, each until its
  // memory ends. Kept across reconnects.
  const seenAsks = new Map<string, number>();

  const answerAsk = Effect.fnUntraced(function* (
    conn: Conn,
    from: string,
    frame: RelayFrame,
  ) {
    const peer = peerOf(roster, from);
    if (Option.isNone(peer)) return;
    const opened = openAsk({
      asker: from,
      asked: identity.deviceId,
      key: identity.key,
      sealed: frame.sealed,
    });
    if (
      Option.isNone(opened) ||
      !sameKey(opened.value.askerKey, peer.value.publicKey)
    ) {
      return;
    }
    // The service could replay an ask under any id: one already opened
    // is dropped by its handshake, which no other ask shares.
    const now = yield* Liveness.monotonicMs;
    // Oldest first, as each is kept for as long.
    for (const [hash, until] of seenAsks) {
      if (until > now) break;
      seenAsks.delete(hash);
    }
    const hash = Base64Url.encode(opened.value.handshakeHash);
    if (seenAsks.has(hash)) return;
    seenAsks.set(hash, now + SEEN_ASK_MS);
    const answer = yield* options.answer(from, opened.value.ask);
    conn.send(
      relayTo(from, {
        kind: "answer",
        id: frame.id,
        sealed: sealAnswer(opened.value.handshake, encodeAnswer(answer)),
      }),
    );
  });

  // One socket's life, from the ticket's mint to its close: how long it
  // was up, or why the device stops.
  const attempt: Effect.Effect<Run, Stop, never> = Effect.gen(function* () {
    const minted = yield* options
      .call(identity.credential, (client) =>
        client.mintTicket({ payload: { connectionId: options.connectionId } }),
      )
      .pipe(Effect.timeoutOption(ADMISSION_MS), Effect.result);
    if (minted._tag === "Failure") {
      const stop = stopOfError(minted.failure);
      if (stop !== null) return yield* Effect.fail(stop);
      return minted.failure._tag === "RateLimited"
        ? failed(minted.failure.retryAfterSeconds * 1000)
        : failed();
    }
    if (Option.isNone(minted.success)) return failed();
    const opened = yield* sockets
      .open(connectUrl(options.hub, minted.success.value.ticket))
      .pipe(Effect.timeoutOption(ADMISSION_MS), Effect.result);
    if (opened._tag === "Failure") {
      return opened.failure.status === 429
        ? failed((opened.failure.retryAfterSeconds ?? 0) * 1000)
        : failed();
    }
    if (Option.isNone(opened.success)) return failed();
    const conn = opened.success.value;
    const scope = yield* Effect.scope;
    const current: Live = {
      conn,
      pending: new Map(),
      nextId: Math.floor(Math.random() * 2 ** 40),
    };
    const heard = yield* Liveness.makeHeard;
    const admitted = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        if (live === current) live = null;
        failPending(current.pending, () => true, "not-connected");
        yield* SubscriptionRef.set(roster, Option.none());
      }),
    );

    const handle = (text: string) =>
      Effect.gen(function* () {
        if (text === PONG) return;
        const message = decodeMessage(text);
        if (Option.isNone(message)) return;
        switch (message.value.t) {
          case "presence": {
            const peers = rosterOf(message.value, identity.deviceId);
            failPending(
              current.pending,
              (entry) => !peers.has(entry.deviceId),
              "offline",
            );
            // Asks go out on it from the first presence, which admits it.
            live = current;
            yield* SubscriptionRef.update(roster, (before) =>
              Option.some({
                peers,
                settled: Option.exists(before, (then) => then.settled),
              }),
            );
            yield* Deferred.succeed(admitted, undefined);
            return;
          }
          case "nack": {
            const { to, reason } = message.value;
            failPending(
              current.pending,
              (entry) => entry.deviceId === to,
              reason === "offline" ? "offline" : "unreachable",
            );
            return;
          }
          case "relay": {
            const { from } = message.value;
            const frame = decodeRelayFrame(message.value.frame);
            if (Option.isNone(frame)) return;
            if (frame.value.kind === "answer") {
              readAnswer(current.pending, from, frame.value);
              return;
            }
            // Answered apart, since an answer may wait on the tunnel.
            yield* answerAsk(conn, from, frame.value).pipe(
              Effect.forkIn(scope),
            );
            return;
          }
        }
      });

    yield* Effect.gen(function* () {
      while (true) {
        const message = yield* conn.receive;
        yield* Liveness.hear(heard);
        if (typeof message === "string") yield* handle(message);
      }
    }).pipe(Effect.ignore, Effect.forkScoped);

    const first = yield* Effect.raceFirst(
      Effect.map(conn.closed, (close) => Option.some(close.code)),
      Deferred.await(admitted).pipe(
        Effect.as(Option.none<number>()),
        Effect.timeoutOrElse({
          duration: ADMISSION_MS,
          orElse: () => Effect.succeedSome(0),
        }),
      ),
    );
    if (Option.isSome(first)) {
      const stop = stopOf(first.value);
      return stop === null ? failed() : yield* Effect.fail(stop);
    }
    yield* Effect.sleep(SETTLE_MS).pipe(
      Effect.andThen(
        SubscriptionRef.update(
          roster,
          Option.map((now) => ({ ...now, settled: true })),
        ),
      ),
      Effect.forkScoped,
    );

    const upSince = yield* Liveness.monotonicMs;
    const code = yield* Effect.raceFirst(
      Effect.map(conn.closed, (close) => close.code),
      Liveness.watch({
        ping: Effect.sync(() => conn.send(PING)),
        heard,
        wakes: options.wakes,
      }).pipe(Effect.as(null)),
    );
    const uptimeMs = (yield* Liveness.monotonicMs) - upSince;
    const stop = code === null ? null : stopOf(code);
    if (stop !== null) return yield* Effect.fail(stop);
    return { uptimeMs, waitAtLeastMs: 0 };
  }).pipe(Effect.scoped);

  yield* attempt.pipe(
    Effect.repeat(reconnectLadder(options.wakes)),
    Effect.catch((stop) => Deferred.succeed(stopped, stop)),
    Effect.forkScoped,
  );

  // The asker's side: the peer's connectInfo result, or why there is
  // none.
  const ask = (deviceId: string, input: ConnectInfoInput) =>
    Effect.gen(function* () {
      const current = live;
      if (current === null) {
        return yield* new DialError({ deviceId, reason: "not-connected" });
      }
      const peer = peerOf(roster, deviceId);
      if (Option.isNone(peer)) {
        return yield* new DialError({ deviceId, reason: "offline" });
      }
      const id = String(current.nextId++);
      // Nothing for a roster key no handshake can use.
      const sealedAsk = tryNoise(() =>
        sealAsk({
          asker: identity.deviceId,
          asked: deviceId,
          key: identity.key,
          askedKey: peer.value.publicKey,
          payload: encodeAsk({ ask: "connectInfo", input }),
        }),
      );
      if (Option.isNone(sealedAsk)) {
        return yield* new DialError({
          deviceId,
          reason: "unreachable",
          cause: "the roster's key for the device is unusable",
        });
      }
      const { sealed, handshake } = sealedAsk.value;
      const answered = Deferred.makeUnsafe<Answer, DialError>();
      current.pending.set(id, { deviceId, handshake, answered });
      current.conn.send(relayTo(deviceId, { kind: "ask", id, sealed }));
      const answer = yield* Deferred.await(answered).pipe(
        Effect.timeoutOrElse({
          duration: ASK_TIMEOUT_MS,
          orElse: () =>
            Effect.fail(new DialError({ deviceId, reason: "timed-out" })),
        }),
        Effect.ensuring(Effect.sync(() => current.pending.delete(id))),
      );
      if (answer.ok) {
        const result = answer.result;
        // A candidate of a kind this device did not ask for is skipped.
        return result.available
          ? {
              ...result,
              candidates: result.candidates.filter((candidate) =>
                input.dialableKinds.includes(candidate.kind),
              ),
            }
          : result;
      }
      return yield* new DialError({
        deviceId,
        reason: answer.code === "no-listener" ? "no-listener" : "unreachable",
        cause: answer.code,
      });
    });

  return { ask, stopped: Deferred.await(stopped) };
});

export type HubSocket = Effect.Success<ReturnType<typeof make>>;
