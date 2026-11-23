// Dialing a peer (spec/link.md, "Dialing"): ask how to reach it, open
// every candidate at once, send `hello` on one at a time in the order
// their handshakes finished, and keep the first welcome.
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { DialError, LinkRefusedError } from "../Errors.ts";
import { type Conn, WebSockets } from "../Platform.ts";
import {
  CLOSE_GOING_AWAY,
  CLOSE_PROTOCOL_ERROR,
  finishHandshake,
  openTransportMessage,
  RACE_TIMEOUT_MS,
  startHandshake,
} from "../wire/link.ts";
import type { Transport } from "../wire/noise.ts";
import { newConnectionId } from "../wire/ids.ts";
import type { Candidate } from "../wire/relay.ts";
import {
  type HubSocket,
  type Identity,
  peerOf,
  type Roster,
} from "./hubSocket.ts";
import * as PeerLink from "./peerLink.ts";
import type { Wakes } from "./wake.ts";

// Why one candidate failed, for the dialer's DialError.
interface CandidateFailed {
  readonly kind: Candidate["kind"];
  readonly url: string;
  readonly reason: unknown;
}

// A socket to the candidate, past the handshake.
const shake = Effect.fnUntraced(function* (
  candidate: Candidate,
  identity: Identity,
  listenerKey: Uint8Array,
) {
  const sockets = yield* WebSockets;
  const fail = (reason: unknown) =>
    Effect.fail<CandidateFailed>({
      kind: candidate.kind,
      url: candidate.url,
      reason,
    });
  const conn = yield* sockets
    .open(candidate.url)
    .pipe(Effect.catch((error) => fail(error)));
  const { first, handshake } = startHandshake({
    ticket: candidate.ticket,
    key: identity.key,
    listenerKey,
  });
  conn.send(first);
  const second = yield* conn.receive.pipe(Effect.catch((close) => fail(close)));
  const transport =
    typeof second === "string"
      ? Option.none<Transport>()
      : finishHandshake(handshake, second);
  if (Option.isNone(transport)) return yield* fail("the handshake failed");
  return { conn, transport: transport.value };
});

export const dial = Effect.fnUntraced(function* (options: {
  readonly hub: HubSocket;
  readonly roster: SubscriptionRef.SubscriptionRef<Option.Option<Roster>>;
  readonly identity: Identity;
  readonly deviceId: string;
  readonly appHello: Uint8Array;
  readonly join: PeerLink.Join;
  readonly wakes: Wakes;
}) {
  const sockets = yield* WebSockets;
  const { deviceId, identity } = options;
  const connectionId = newConnectionId();
  const info = yield* options.hub.ask(deviceId, {
    dialableKinds: sockets.dialableKinds,
    connectionId,
  });
  if (!info.available || info.candidates.length === 0) {
    return yield* new DialError({ deviceId, reason: "unavailable" });
  }
  const peer = peerOf(options.roster, deviceId);
  if (Option.isNone(peer)) {
    return yield* new DialError({ deviceId, reason: "offline" });
  }

  // Each candidate's socket in a scope of its own: the winner's stays
  // open in the caller's scope, and the others close with 1000.
  const outer = yield* Effect.scope;
  const scopes = yield* Effect.forEach(info.candidates, () =>
    Scope.fork(outer),
  );
  let refusal: LinkRefusedError | null = null;
  const failures: Array<unknown> = [];

  const race = Effect.gen(function* () {
    const shaken = yield* Queue.unbounded<{
      readonly index: number;
      readonly exit: Exit.Exit<
        { readonly conn: Conn; readonly transport: Transport },
        CandidateFailed
      >;
    }>();
    yield* Effect.forEach(
      info.candidates,
      (candidate, index) =>
        shake(candidate, identity, peer.value.publicKey).pipe(
          Scope.provide(scopes[index] as Scope.Scope),
          Effect.exit,
          Effect.flatMap((exit) => Queue.offer(shaken, { index, exit })),
          Effect.forkScoped,
        ),
      { discard: true },
    );
    for (let tried = 0; tried < info.candidates.length; tried += 1) {
      const { index, exit } = yield* Queue.take(shaken);
      const candidate = info.candidates[index] as Candidate;
      if (Exit.isFailure(exit)) {
        failures.push(exit.cause);
        continue;
      }
      const { conn, transport } = exit.value;
      yield* PeerLink.sendSealed(conn, transport, {
        type: "hello",
        hello: {
          deviceId: identity.deviceId,
          connectionId,
          appHello: options.appHello,
        },
      });
      const answer = yield* conn.receive.pipe(
        Effect.map((data) =>
          typeof data === "string"
            ? Option.none()
            : openTransportMessage(transport, data),
        ),
        Effect.catch((close) => {
          failures.push({
            kind: candidate.kind,
            url: candidate.url,
            reason: close,
          } satisfies CandidateFailed);
          return Effect.succeedNone;
        }),
      );
      const message = Option.getOrUndefined(answer);
      if (message?.type === "welcome") {
        return { index, conn, transport, welcome: message.welcome };
      }
      if (message?.type === "refuse") {
        const refuse = message.refuse;
        refusal ??= new LinkRefusedError({
          deviceId,
          by: refuse.by,
          reason: refuse.reason,
          ...(refuse.by === "app" && refuse.appRefusal !== undefined
            ? { appRefusal: refuse.appRefusal }
            : {}),
        });
      } else if (conn.isOpen()) {
        failures.push({
          kind: candidate.kind,
          url: candidate.url,
          reason: "expected welcome or refuse",
        } satisfies CandidateFailed);
        conn.close(CLOSE_PROTOCOL_ERROR, "expected welcome or refuse");
      }
    }
    return yield* (
      refusal ??
        new DialError({ deviceId, reason: "unreachable", cause: failures })
    );
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: RACE_TIMEOUT_MS,
      orElse: () =>
        Effect.fail(
          refusal ?? new DialError({ deviceId, reason: "timed-out" }),
        ),
    }),
  );
  // From the win to the link's join in one go, so the session's end
  // leaves no link unseen, and no closed one among its links. The losers
  // close outside `restore`, or an interrupt that came while they closed
  // would fail the dial with the winner's socket open and no link.
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const won = yield* restore(race).pipe(
        Effect.onExit((exit) =>
          Effect.forEach(
            scopes,
            (scope, index) =>
              Exit.isSuccess(exit) && exit.value.index === index
                ? Effect.void
                : Scope.close(scope, Exit.void),
            { discard: true },
          ),
        ),
      );
      const scope = scopes[won.index] as Scope.Closeable;
      const link = yield* PeerLink.make({
        conn: won.conn,
        transport: won.transport,
        role: "dialer",
        deviceId,
        peerKey: peer.value.publicKey,
        connectionId,
        arrival: (info.candidates[won.index] as Candidate).kind,
        greeting: won.welcome.appWelcome,
        wakes: options.wakes,
      }).pipe(Scope.provide(scope));
      // The roster may have moved on, or gone, while the race ran.
      const joined = yield* options.join(link).pipe(Scope.provide(scope));
      if (joined !== "joined") {
        yield* link.end(CLOSE_GOING_AWAY, "left-roster");
        yield* Scope.close(scope, Exit.void);
        return yield* new DialError({
          deviceId,
          reason: joined === "refused" ? "offline" : "not-connected",
        });
      }
      // Let go as it closes, or with the caller's scope at the latest.
      yield* link.closed.pipe(
        Effect.andThen(Scope.close(scope, Exit.void)),
        Effect.forkScoped,
      );
      return link;
    }),
  );
});
