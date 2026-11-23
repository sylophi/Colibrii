// The listener (spec/link.md, "Admission"), and this device's answer to
// a connectInfo ask (spec/relay.md, "connectInfo").
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { ListenOptions } from "../Device.ts";
import type { AppRefusal } from "../Errors.ts";
import {
  type Arrival,
  type Conn,
  ListenerSockets,
  type NetworkAddress,
  type RunningTunnel,
  Tunnel,
  type TunnelProvision,
} from "../Platform.ts";
import { sameKey } from "../wire/deviceKey.ts";
import {
  answerHandshake,
  CLOSE_GOING_AWAY,
  CLOSE_HANDSHAKE_REFUSED,
  CLOSE_HELLO_REFUSED,
  CLOSE_LOCKED_OUT,
  CLOSE_PROTOCOL_ERROR,
  CLOSE_TRY_AGAIN,
  decodeFirstMessage,
  fitsTransportMessage,
  LOCKOUT_GUESSES,
  LOCKOUT_MS,
  MAX_APP_BYTES,
  MAX_SOCKETS,
  MAX_UNWELCOMED_SOCKETS,
  openTransportMessage,
  type Refuse,
  WELCOME_WITHIN_MS,
} from "../wire/link.ts";
import {
  type CandidateKind,
  type ConnectInfoInput,
  type ConnectInfoResult,
  MAX_LAN_CANDIDATES,
  TUNNEL_WAIT_MS,
} from "../wire/relay.ts";
import type { Identity, Roster } from "./hubSocket.ts";
import * as Liveness from "./liveness.ts";
import * as PeerLink from "./peerLink.ts";
import { LinkTickets } from "./tickets.ts";
import type { Wakes } from "./wake.ts";

// A peer that asks again after this many asks that offered the tunnel
// and never reached the listener has the tunnel checked
// (spec/tunnels.md, "Checking again").
const UNREACHED_ASKS = 2;

const isLoopback = (address: string) =>
  address.startsWith("127.") ||
  address === "::1" ||
  address.startsWith("::ffff:127.");

const isLinkLocal = (address: NetworkAddress) =>
  address.family === "IPv4"
    ? address.address.startsWith("169.254.")
    : /^fe[89ab]/i.test(address.address);

// The LAN candidates' addresses: not internal, link-local or
// zone-scoped, IPv4 first, at most six.
export const lanAddresses = (addresses: ReadonlyArray<NetworkAddress>) => {
  const usable = addresses.filter(
    (address) =>
      !address.internal &&
      !isLinkLocal(address) &&
      !address.address.includes("%"),
  );
  return [
    ...usable.filter((address) => address.family === "IPv4"),
    ...usable.filter((address) => address.family === "IPv6"),
  ]
    .slice(0, MAX_LAN_CANDIDATES)
    .map((address) =>
      address.family === "IPv6" ? `[${address.address}]` : address.address,
    );
};

export const make = Effect.fnUntraced(function* (options: {
  readonly identity: Identity;
  readonly roster: SubscriptionRef.SubscriptionRef<Option.Option<Roster>>;
  // The port to listen on, 0 for any. A port that is taken gives way to
  // any.
  readonly port: number;
  readonly listen: ListenOptions;
  // Every link of the device, both ways. A new one replaces only links
  // this device listened for.
  readonly links: Set<PeerLink.LiveLink>;
  readonly join: PeerLink.Join;
  readonly wakes: Wakes;
  // `POST /tunnel` for the port, for the tunnel's run.
  readonly provision: (port: number) => Effect.Effect<TunnelProvision, unknown>;
}) {
  const sockets = yield* ListenerSockets;
  const tunnel = yield* Effect.serviceOption(Tunnel);
  const { identity } = options;
  const tickets = new LinkTickets();
  // Guesses in a row by address, and when the last one was.
  const guesses = new Map<string, { count: number; atMs: number }>();
  // The sockets not yet welcomed, with the device their ticket names
  // once it checked out.
  const unwelcomed = new Map<Conn, string | null>();
  // By device, the asks in a row offered the tunnel since a ticket of
  // its last reached the listener.
  const unreached = new Map<string, number>();
  let open = 0;

  const rosterNow = () =>
    Option.getOrNull(SubscriptionRef.getUnsafe(options.roster))?.peers ?? null;

  const lockedOut = (address: string, nowMs: number) => {
    const entry = guesses.get(address);
    if (entry === undefined) return false;
    if (nowMs - entry.atMs >= LOCKOUT_MS) {
      guesses.delete(address);
      return false;
    }
    return entry.count >= LOCKOUT_GUESSES;
  };

  const guessed = (address: string) =>
    Effect.map(Liveness.monotonicMs, (atMs) => {
      for (const [other, entry] of guesses) {
        if (atMs - entry.atMs >= LOCKOUT_MS) guesses.delete(other);
      }
      const entry = guesses.get(address);
      guesses.set(address, { count: (entry?.count ?? 0) + 1, atMs });
    });

  // From the upgrade to the welcome: the link, or nothing once the
  // socket was closed.
  const admit = Effect.fnUntraced(function* (
    conn: Conn,
    address: string,
    arrival: CandidateKind,
  ) {
    const refuseWith = (
      transport: Parameters<typeof PeerLink.sendSealed>[1],
      refuse: Refuse,
    ) =>
      Effect.gen(function* () {
        yield* PeerLink.sendSealed(conn, transport, { type: "refuse", refuse });
        conn.close(CLOSE_HELLO_REFUSED, "refused");
      });
    const refuseHandshake = (guess: boolean) =>
      Effect.gen(function* () {
        if (guess) yield* guessed(address);
        conn.close(CLOSE_HANDSHAKE_REFUSED, "refused");
      });

    const first = yield* conn.receive;
    const decoded =
      typeof first === "string" ? Option.none() : decodeFirstMessage(first);
    if (Option.isNone(decoded)) return yield* refuseHandshake(true);
    const { ticket, message } = decoded.value;
    if (rosterNow() === null) {
      return conn.close(CLOSE_TRY_AGAIN, "roster unavailable");
    }
    const check = tickets.check(
      ticket,
      arrival,
      yield* Clock.currentTimeMillis,
    );
    if (check._tag !== "Pending") {
      return yield* refuseHandshake(check._tag === "Unknown");
    }
    unwelcomed.set(conn, check.deviceId);
    unreached.delete(check.deviceId);
    const peer = rosterNow()?.get(check.deviceId);
    const opened = answerHandshake({ ticket, message, key: identity.key });
    if (
      peer === undefined ||
      Option.isNone(opened) ||
      !sameKey(opened.value.dialerKey, peer.publicKey)
    ) {
      return yield* refuseHandshake(true);
    }
    const { transport, dialerKey } = opened.value;
    conn.send(opened.value.second);

    const data = yield* conn.receive;
    const said =
      typeof data === "string"
        ? Option.none()
        : openTransportMessage(transport, data);
    if (Option.isNone(said) || said.value.type !== "hello") {
      return conn.close(CLOSE_PROTOCOL_ERROR, "expected hello");
    }
    const { hello } = said.value;
    if (
      hello.deviceId !== check.deviceId ||
      hello.connectionId !== check.connectionId
    ) {
      return yield* refuseWith(transport, {
        by: "colibrii",
        reason: "the hello does not match its ticket",
      });
    }
    const roster = rosterNow();
    if (roster === null) {
      return conn.close(CLOSE_TRY_AGAIN, "roster unavailable");
    }
    const current = roster.get(hello.deviceId);
    if (current === undefined || !sameKey(current.publicKey, dialerKey)) {
      return yield* refuseWith(transport, {
        by: "colibrii",
        reason: "the device is not in the roster",
      });
    }
    if (!tickets.spend(ticket, arrival, yield* Clock.currentTimeMillis)) {
      return yield* refuseWith(transport, {
        by: "colibrii",
        reason: "the ticket expired",
      });
    }
    const welcome = yield* options.listen
      .admit({
        deviceId: hello.deviceId,
        connectionId: hello.connectionId,
        arrival,
        appHello: hello.appHello,
      })
      .pipe(Effect.result);
    // The peer went, or left the roster, while the app decided.
    if (!conn.isOpen()) return;
    const welcomed = welcome._tag === "Success";
    const appBytes = welcomed ? welcome.success : welcome.failure.appRefusal;
    if (appBytes !== undefined && appBytes.length > MAX_APP_BYTES) {
      return yield* refuseWith(transport, {
        by: "colibrii",
        reason: `the app's ${welcomed ? "welcome" : "refusal"} is over 16 KiB`,
      });
    }
    if (welcome._tag === "Failure") {
      const refusal: AppRefusal = welcome.failure;
      const refuse: Refuse = {
        by: "app",
        reason: refusal.reason,
        ...(refusal.appRefusal === undefined
          ? {}
          : { appRefusal: refusal.appRefusal }),
      };
      return yield* refuseWith(
        transport,
        fitsTransportMessage({ type: "refuse", refuse })
          ? refuse
          : { by: "colibrii", reason: "the app's refusal is too long" },
      );
    }
    // From the link to its join in one go, so the session's end leaves
    // no link unseen, and no closed one in `links`.
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const link = yield* PeerLink.make({
          conn,
          transport,
          role: "listener",
          deviceId: hello.deviceId,
          peerKey: dialerKey,
          connectionId: hello.connectionId,
          arrival,
          greeting: hello.appHello,
          wakes: options.wakes,
        });
        // The roster may have moved on, or gone, while the app decided.
        const joined = yield* options.join(link);
        if (joined === "unavailable") {
          tickets.unspend(ticket);
          return conn.close(CLOSE_TRY_AGAIN, "roster unavailable");
        }
        if (joined === "refused") {
          return yield* refuseWith(transport, {
            by: "colibrii",
            reason: "the device is not in the roster",
          });
        }
        for (const other of options.links) {
          if (
            other !== link &&
            other.role === "listener" &&
            other.deviceId === hello.deviceId &&
            (current.connections === "single" ||
              other.connectionId === hello.connectionId)
          ) {
            yield* other.end(CLOSE_GOING_AWAY, "replaced");
          }
        }
        yield* PeerLink.sendSealed(conn, transport, {
          type: "welcome",
          welcome: { appWelcome: welcome.success },
        });
        guesses.delete(address);
        return link;
      }),
    );
  });

  const serve = (arrival: Arrival) =>
    Effect.gen(function* () {
      const { conn } = arrival;
      const tunneled =
        isLoopback(arrival.remoteAddress) && arrival.connectingIp !== undefined;
      const address = tunneled
        ? (arrival.connectingIp ?? "")
        : arrival.remoteAddress;
      if (open >= MAX_SOCKETS || unwelcomed.size >= MAX_UNWELCOMED_SOCKETS) {
        return conn.close(CLOSE_TRY_AGAIN, "over capacity");
      }
      if (lockedOut(address, yield* Liveness.monotonicMs)) {
        return conn.close(CLOSE_LOCKED_OUT, "locked out");
      }
      open += 1;
      unwelcomed.set(conn, null);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          open -= 1;
          unwelcomed.delete(conn);
        }),
      );
      const link = yield* admit(
        conn,
        address,
        tunneled ? "tunnel" : "lan",
      ).pipe(
        Effect.timeoutOrElse({
          duration: WELCOME_WITHIN_MS,
          orElse: () =>
            Effect.sync(() => conn.close(CLOSE_HELLO_REFUSED, "too late")),
        }),
        // The socket closed before the welcome.
        Effect.catch(() => Effect.void),
      );
      if (link === undefined) return;
      unwelcomed.delete(conn);
      yield* options.listen.onLink(link).pipe(Effect.forkScoped);
      yield* link.closed;
    }).pipe(Effect.scoped);

  const listenOn = (port: number) =>
    sockets.listen({
      port,
      allowOrigin: (origin) =>
        origin === undefined || (options.listen.origins ?? []).includes(origin),
      serve,
    });
  const remembered = options.port;
  const port = yield* listenOn(remembered).pipe(
    Effect.catchIf(
      (error) => error.inUse && remembered !== 0,
      () => listenOn(0),
    ),
  );

  // Once the roster settled, a peer that left it spends no ticket, and
  // its sockets not yet welcomed close. Its links close in the device's
  // own sweep.
  yield* SubscriptionRef.changes(options.roster).pipe(
    Stream.runForEach((roster) =>
      Effect.sync(() => {
        if (Option.isNone(roster) || !roster.value.settled) return;
        const online = new Set(roster.value.peers.keys());
        tickets.keepDevices(online);
        for (const deviceId of unreached.keys()) {
          if (!online.has(deviceId)) unreached.delete(deviceId);
        }
        for (const [conn, deviceId] of unwelcomed) {
          if (deviceId !== null && !online.has(deviceId)) {
            conn.close(CLOSE_GOING_AWAY, "left the roster");
          }
        }
      }),
    ),
    Effect.forkScoped,
  );

  const reachedAt = options.listen.advertisedPort ?? port;
  const running: Option.Option<RunningTunnel> = Option.isSome(tunnel)
    ? Option.some(
        yield* tunnel.value.run({
          port: reachedAt,
          provision: options.provision(reachedAt),
        }),
      )
    : Option.none();
  if (Option.isSome(running)) {
    yield* Stream.fromPubSub(options.wakes).pipe(
      Stream.runForEach(() => running.value.check),
      Effect.forkScoped,
    );
  }

  const upHostname = Option.match(running, {
    onNone: () => Effect.succeed(Option.none<string>()),
    onSome: (started) =>
      Stream.runHead(started.state).pipe(
        Effect.map(
          Option.flatMap((state) =>
            state.status === "up" ? Option.some(state.hostname) : Option.none(),
          ),
        ),
      ),
  });

  const offer = Effect.fnUntraced(function* (
    from: string,
    input: ConnectInfoInput,
  ) {
    const candidates: Array<{ kind: CandidateKind; url: string }> = [];
    if (input.dialableKinds.includes("lan")) {
      for (const address of lanAddresses(yield* sockets.addresses)) {
        candidates.push({ kind: "lan", url: `ws://${address}:${reachedAt}` });
      }
    }
    const hostname = yield* upHostname;
    if (input.dialableKinds.includes("tunnel") && Option.isSome(hostname)) {
      candidates.push({ kind: "tunnel", url: `wss://${hostname.value}` });
    }
    if (candidates.length === 0) return { available: false } as const;
    const minted = tickets.mint(
      from,
      input.connectionId,
      candidates.map((candidate) => candidate.kind),
      yield* Clock.currentTimeMillis,
    );
    if (minted === null) return { available: false } as const;
    if (candidates.some((candidate) => candidate.kind === "tunnel")) {
      unreached.set(from, (unreached.get(from) ?? 0) + 1);
    }
    return {
      available: true,
      candidates: candidates.map(({ kind, url }, index) => ({
        kind,
        url,
        ticket: minted[index] ?? "",
      })),
    } as const;
  });

  // An asker that can dial the tunnel and would get nothing waits a
  // little for a tunnel that is starting.
  const connectInfo = (
    from: string,
    input: ConnectInfoInput,
  ): Effect.Effect<ConnectInfoResult> =>
    Effect.gen(function* () {
      if (Option.isNone(running)) return yield* offer(from, input);
      if (unreached.get(from) === UNREACHED_ASKS) yield* running.value.check;
      const now = yield* offer(from, input);
      if (now.available || !input.dialableKinds.includes("tunnel")) {
        return now;
      }
      const up = yield* running.value.state.pipe(
        Stream.takeUntil((state) => state.status !== "starting"),
        Stream.runLast,
        Effect.timeoutOption(TUNNEL_WAIT_MS),
      );
      return Option.flatten(up).pipe(
        Option.exists((state) => state.status === "up"),
      )
        ? yield* offer(from, input)
        : now;
    });

  return { port, connectInfo, tunnel: running };
});

export type Listener = Effect.Success<ReturnType<typeof make>>;
