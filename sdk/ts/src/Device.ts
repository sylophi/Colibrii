// A device: one install of one app. It enrolls, keeps its hub socket up
// while enrolled, follows its peers, dials them, and, when it listens,
// is dialed by them. It lives as long as the scope it is made in.
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FiberHandle from "effect/FiberHandle";
import * as HttpClient from "effect/http/HttpClient";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import {
  type AppRefusal,
  DialError,
  type HubUnreachableError,
  InvalidInputError,
  type LinkRefusedError,
  NotEnrolledError,
  type RateLimitedError,
} from "./Errors.ts";
import * as Dial from "./internal/dial.ts";
import * as HubApi from "./internal/hubApi.ts";
import * as HubSocket from "./internal/hubSocket.ts";
import * as Listener from "./internal/listener.ts";
import type * as PeerLink from "./internal/peerLink.ts";
import * as Stored from "./internal/stored.ts";
import type { Wakes as WakeHub } from "./internal/wake.ts";
import type { Link } from "./Link.ts";
import {
  CredentialStore,
  type CredentialStoreError,
  type ListenError,
  ListenerSockets,
  Tunnel,
  type TunnelState,
  Wakes,
  WebSockets,
} from "./Platform.ts";
import * as Base64Url from "./wire/base64url.ts";
import {
  type Connections,
  type Device as DeviceRecord,
  DeviceData,
  DeviceName,
} from "./wire/device.ts";
import { DevicePatch } from "./wire/api.ts";
import { DeviceId, newConnectionId } from "./wire/ids.ts";
import { sameKey } from "./wire/deviceKey.ts";
import { CLOSE_GOING_AWAY, MAX_APP_BYTES } from "./wire/link.ts";
import { generateKeyPair, keyPairFromPrivateKey } from "./wire/noise.ts";
import {
  type Answer,
  type Ask,
  type CandidateKind,
  ConnectInfoInput,
} from "./wire/relay.ts";

export type { DeviceRecord };

export type Peer = HubSocket.Peer;

export type AccountState =
  | { readonly _tag: "SignedOut"; readonly removed: boolean }
  | {
      readonly _tag: "Enrolled";
      readonly deviceId: string;
      readonly accountId: string;
      // Whether the hub socket is up.
      readonly connected: boolean;
      // Whether the 10 seconds after the hub socket connected are over.
      // Until then a peer missing from the roster may be one still
      // reconnecting, so its links stay up.
      readonly settled: boolean;
    }
  | {
      readonly _tag: "Stopped";
      readonly deviceId: string;
      readonly accountId: string;
      // Until the person acts: another copy of a `single` device took
      // over, its credential was replaced so it enrolls again, or the
      // service wants a newer SDK.
      readonly reason: Exclude<HubSocket.Stop, "removed">;
    };

// A dialer that passed Colibrii's checks, for the listening app to
// welcome or refuse.
export interface IncomingHello {
  readonly deviceId: string;
  readonly connectionId: string;
  readonly arrival: CandidateKind;
  readonly appHello: Uint8Array;
}

export interface ListenOptions {
  // The browser origins that may dial, such as the app's web client's.
  // A dialer that is not a browser sends none, and is let in.
  readonly origins?: ReadonlyArray<string>;
  // The app welcome, of at most 16 KiB, or a refusal.
  readonly admit: (
    hello: IncomingHello,
  ) => Effect.Effect<Uint8Array, AppRefusal>;
  // Each link once welcomed, in a fiber of its own.
  readonly onLink: (link: Link) => Effect.Effect<void>;
  // The port peers reach the listener at, when something on this machine
  // (a proxy, a port forward) takes connections there and passes them on
  // to the port the listener bound: the LAN candidates name it, and the
  // tunnel routes to it. The listener's own port without one.
  readonly advertisedPort?: number;
}

export interface DeviceOptions {
  // The instance's hub address, such as `https://hub.example.com`.
  readonly hub: string;
  readonly app: string;
  readonly connections: Connections;
  // The device id to enroll under, a UUID in lowercase canonical form,
  // which then outlives sign-outs. Without one, the device makes one up
  // when it first enrolls, and forgets it once the service heard it
  // signed out.
  readonly deviceId?: string;
  // Listening for links from the person's other devices. A device that
  // does not listen tells a peer asking so.
  readonly listen?: ListenOptions;
}

type WireErrors<K extends keyof HubApi.Client> = HubApi.Client[K] extends (
  ...args: ReadonlyArray<never>
) => Effect.Effect<unknown, infer E>
  ? Exclude<E, { readonly _tag: "HttpClientError" | "SchemaError" }>
  : never;

type CallErrors<K extends keyof HubApi.Client> =
  | WireErrors<K>
  | HubApiErrors
  | NotEnrolledError;

type HubApiErrors = HubUnreachableError | RateLimitedError;

export interface Device {
  // The account state now, then every change.
  readonly account: Stream.Stream<AccountState>;
  // The peers online now, with their keys, then every change. Empty
  // while the hub socket is down.
  readonly roster: Stream.Stream<ReadonlyArray<Peer>>;
  // The tunnel's state now, then every change. `off` while the device
  // is not enrolled, does not listen, or has no Tunnel to run one.
  readonly tunnel: Stream.Stream<TunnelState>;
  // Enrolls under the account the login token (a Clerk session token or
  // OAuth access token) names, with a new key pair. Enrolling again
  // keeps the device id.
  readonly enroll: (input: {
    readonly loginToken: string;
    readonly name: string;
    readonly data?: typeof DeviceData.Type;
  }) => Effect.Effect<
    void,
    WireErrors<"enroll"> | HubApiErrors | InvalidInputError | SessionFailure
  >;
  // Forgets the credential and reads as signed out at once, then
  // removes this device from the account. A removal the service could
  // not be told of is kept in the store, and delivered when a device is
  // next made, on a wake, before an enrollment, and on the next sign-out.
  // True once the service let the device go and nothing is owed. A
  // device that reads as removed reads as signed out after it.
  readonly signOut: Effect.Effect<boolean, CredentialStoreError>;
  readonly devices: {
    readonly list: Effect.Effect<
      ReadonlyArray<DeviceRecord>,
      CallErrors<"listDevices">
    >;
    readonly rename: (
      deviceId: string,
      name: string,
    ) => Effect.Effect<void, CallErrors<"updateDevice"> | InvalidInputError>;
    // Replaces the device's data whole.
    readonly setData: (
      deviceId: string,
      data: typeof DeviceData.Type,
    ) => Effect.Effect<void, CallErrors<"updateDevice"> | InvalidInputError>;
    // Removes another device. Removing this one reads as removed.
    readonly remove: (
      deviceId: string,
    ) => Effect.Effect<void, CallErrors<"removeDevice"> | InvalidInputError>;
  };
  // A link to a peer, with the app hello (at most 16 KiB) its app checks.
  // Closing the scope closes it.
  readonly dial: (
    deviceId: string,
    appHello: Uint8Array,
  ) => Effect.Effect<
    Link,
    DialError | LinkRefusedError | InvalidInputError,
    Scope.Scope
  >;
  // The machine woke or the network went or came back: the hub socket
  // and the dialed links check in at once, and a reconnect waits no
  // longer.
  readonly wake: Effect.Effect<void>;
  // Every wake the device hears, its platform's and `wake`. An app that
  // keeps links to its peers cuts its redial waits short on each.
  readonly wakes: Stream.Stream<void>;
}

type Services = WebSockets | CredentialStore | HttpClient.HttpClient;

type SessionFailure = CredentialStoreError | ListenError;

interface Session {
  readonly identity: HubSocket.Identity;
  readonly hub: HubSocket.HubSocket;
  readonly roster: SubscriptionRef.SubscriptionRef<
    Option.Option<HubSocket.Roster>
  >;
  readonly join: PeerLink.Join;
  // Done as the session ends.
  readonly ended: Deferred.Deferred<void>;
}

const isDeviceId = Schema.is(DeviceId);
const isName = Schema.is(DeviceName);
const isData = Schema.is(DeviceData);
const decodeConnectInfo = Schema.decodeUnknownOption(ConnectInfoInput);

const SIGNED_OUT: AccountState = { _tag: "SignedOut", removed: false };
const REMOVED: AccountState = { _tag: "SignedOut", removed: true };

const valid =
  <A>(is: (value: A) => boolean, field: InvalidInputError["field"]) =>
  (value: A) =>
    is(value) ? Effect.void : Effect.fail(new InvalidInputError({ field }));
const validDeviceId = valid(isDeviceId, "deviceId");
const validName = valid(isName, "name");
const validData = valid(isData, "data");

const makeDevice = Effect.fnUntraced(function* (options: DeviceOptions) {
  if (options.deviceId !== undefined) yield* validDeviceId(options.deviceId);
  const scope = yield* Effect.scope;
  const listenerSockets = yield* Effect.serviceOption(ListenerSockets);
  const tunnel = yield* Effect.serviceOption(Tunnel);
  // Only these: the caller's scope stays the caller's.
  const services = (yield* Effect.context<Services>()).pipe(
    Context.pick(WebSockets, CredentialStore, HttpClient.HttpClient),
    Option.isSome(tunnel)
      ? Context.add(Tunnel, tunnel.value)
      : (context) => context,
  );
  const call = yield* HubApi.make(options.hub);
  const readStored = Stored.read(options.deviceId).pipe(
    Effect.map(Option.getOrNull),
  );
  const shared = (yield* CredentialStore).shared;
  const exclusive = shared?.exclusive ?? ((effect) => effect);
  const wakes: WakeHub = yield* PubSub.unbounded<void>();
  yield* (yield* Wakes).pipe(
    Stream.runForEach(() => PubSub.publish(wakes, undefined)),
    Effect.forkScoped,
  );
  const first = yield* readStored;
  const account = yield* SubscriptionRef.make<AccountState>(
    Stored.isRemoved(first) ? REMOVED : SIGNED_OUT,
  );
  const peers = yield* SubscriptionRef.make<ReadonlyArray<Peer>>([]);
  const tunnelState = yield* SubscriptionRef.make<TunnelState>({
    status: "off",
  });
  const lifecycle = yield* Semaphore.make(1);
  // Enrolling, signing out and stopping, one at a time, in this run and
  // across the runs that share the store.
  const locked = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    exclusive(lifecycle.withPermits(1)(effect));
  const sessions = yield* FiberHandle.make();
  // What the store holds, and the session it runs.
  let enrollment: Stored.Enrollment | null = null;
  let session: Session | null = null;
  // The hub socket's name, kept for the life of the device.
  const connectionId = newConnectionId();

  const answerFor =
    (connectInfo: Listener.Listener["connectInfo"] | null) =>
    (from: string, ask: Ask): Effect.Effect<Answer> =>
      Effect.gen(function* () {
        if (ask.ask !== "connectInfo")
          return { ok: false, code: "unknown-ask" };
        const input = decodeConnectInfo(ask.input);
        if (Option.isNone(input)) return { ok: false, code: "bad-input" };
        if (connectInfo === null) return { ok: false, code: "no-listener" };
        return { ok: true, result: yield* connectInfo(from, input.value) };
      });

  const runSession = (
    stored: Stored.Enrollment,
    ready: Deferred.Deferred<Session, SessionFailure>,
  ) =>
    Effect.gen(function* () {
      const identity: HubSocket.Identity = {
        deviceId: stored.deviceId,
        credential: stored.credential,
        key: keyPairFromPrivateKey(stored.privateKey),
      };
      const links = new Set<PeerLink.LiveLink>();
      const ended = yield* Deferred.make<void>();
      const roster = yield* SubscriptionRef.make<
        Option.Option<HubSocket.Roster>
      >(Option.none());
      // A link joins only while the session runs and the roster names its
      // peer with the key the link proved, checked in the same step, so
      // the sweep below and the session's end see every link that joined.
      // A dialer chose its peer from the roster moments ago, so it leaves
      // one missing from a roster not yet settled to the sweep, as it
      // does an older link.
      const join: PeerLink.Join = (link) =>
        Effect.suspend(() => {
          const now = SubscriptionRef.getUnsafe(roster);
          if (Deferred.isDoneUnsafe(ended) || Option.isNone(now)) {
            return Effect.succeed("unavailable");
          }
          const peer = now.value.peers.get(link.deviceId);
          const named =
            peer === undefined
              ? link.role === "dialer" && !now.value.settled
              : sameKey(peer.publicKey, link.peerKey);
          if (!named) return Effect.succeed("refused");
          links.add(link);
          return Effect.addFinalizer(() =>
            Effect.sync(() => links.delete(link)),
          ).pipe(Effect.as("joined"));
        });
      let connectInfo: Listener.Listener["connectInfo"] | null = null;
      if (options.listen !== undefined && Option.isSome(listenerSockets)) {
        // After the tunnel's own end.
        yield* Effect.addFinalizer(() =>
          SubscriptionRef.set(tunnelState, { status: "off" }),
        );
        const listener = yield* Listener.make({
          identity,
          roster,
          port: stored.port ?? 0,
          listen: options.listen,
          links,
          join,
          wakes,
          provision: (port) =>
            call(stored.credential, (client) =>
              client.provisionTunnel({ payload: { port } }),
            ).pipe(
              // Apart from this session, which settling ends.
              Effect.tapError((error) =>
                settleOn(stored.credential, error).pipe(Effect.forkIn(scope)),
              ),
            ),
        }).pipe(Effect.provideService(ListenerSockets, listenerSockets.value));
        connectInfo = listener.connectInfo;
        if (Option.isSome(listener.tunnel)) {
          yield* listener.tunnel.value.state.pipe(
            Stream.runForEach((state) =>
              SubscriptionRef.set(tunnelState, state),
            ),
            Effect.forkScoped,
          );
        }
        if (
          listener.port !== stored.port &&
          enrollment?.credential === stored.credential
        ) {
          enrollment = { ...enrollment, port: listener.port };
          yield* Stored.write(enrollment);
        }
      }
      const hub = yield* HubSocket.make({
        hub: options.hub,
        call,
        identity,
        connectionId,
        roster,
        answer: answerFor(connectInfo),
        wakes,
      });
      // Before the listener's own close, so every link ends going away.
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(ended, undefined);
          yield* Effect.forEach(
            links,
            (link) => link.end(CLOSE_GOING_AWAY, "stopped"),
            {
              discard: true,
            },
          );
          yield* SubscriptionRef.set(peers, []);
        }),
      );
      yield* SubscriptionRef.changes(roster).pipe(
        Stream.runForEach((now) =>
          Effect.gen(function* () {
            if (Option.isSome(now)) {
              const { peers: online, settled } = now.value;
              for (const link of links) {
                const peer = online.get(link.deviceId);
                if (
                  peer === undefined
                    ? settled
                    : !sameKey(peer.publicKey, link.peerKey)
                ) {
                  yield* link.end(CLOSE_GOING_AWAY, "left-roster");
                }
              }
            }
            yield* SubscriptionRef.set(
              peers,
              Option.match(now, {
                onNone: () => [],
                onSome: (online) => [...online.peers.values()],
              }),
            );
            yield* SubscriptionRef.update(account, (state) =>
              state._tag === "Enrolled" && state.deviceId === stored.deviceId
                ? {
                    ...state,
                    connected: Option.isSome(now),
                    settled: Option.exists(now, (online) => online.settled),
                  }
                : state,
            );
          }),
        ),
        Effect.forkScoped,
      );
      yield* Deferred.succeed(ready, {
        identity,
        hub,
        roster,
        join,
        ended,
      });
      const stop = yield* hub.stopped;
      // Apart from this session, which settling ends.
      yield* settle(stored.credential, stop).pipe(Effect.forkIn(scope));
      return yield* Effect.never;
    }).pipe(
      Effect.onError((cause) => Deferred.failCause(ready, cause)),
      Effect.scoped,
      Effect.provideContext(services),
    );

  const end = Effect.gen(function* () {
    yield* FiberHandle.clear(sessions);
    session = null;
  });

  const start = (stored: Stored.Enrollment) =>
    Effect.gen(function* () {
      yield* end;
      yield* SubscriptionRef.set(account, {
        _tag: "Enrolled",
        deviceId: stored.deviceId,
        accountId: stored.accountId,
        connected: false,
        settled: false,
      });
      const ready = yield* Deferred.make<Session, SessionFailure>();
      yield* FiberHandle.run(sessions, runSession(stored, ready));
      session = yield* Deferred.await(ready);
    });

  // The device stops, if `credential` is still the one it holds.
  const settle = (
    credential: string,
    stop: HubSocket.Stop,
  ): Effect.Effect<void> =>
    locked(
      Effect.gen(function* () {
        const current = enrollment;
        if (current === null || current.credential !== credential) return;
        yield* end;
        // Another run sharing the store may have acted since. One that
        // enrolled again is run.
        const stored = yield* Effect.option(readStored);
        const now = Stored.enrollmentOf(Option.getOrNull(stored));
        if (now !== null && now.credential !== credential) {
          enrollment = now;
          return yield* Effect.ignore(start(now));
        }
        if (stop === "removed") {
          enrollment = null;
          if (now !== null) {
            yield* Effect.ignore(
              Stored.write({ deviceId: current.deviceId, removed: true }),
            );
          }
          // One that signed out left nothing to mark.
          const signedOut =
            Option.isSome(stored) &&
            now === null &&
            !Stored.isRemoved(stored.value);
          yield* SubscriptionRef.set(account, signedOut ? SIGNED_OUT : REMOVED);
        } else {
          yield* SubscriptionRef.set(account, {
            _tag: "Stopped",
            deviceId: current.deviceId,
            accountId: current.accountId,
            reason: stop,
          });
        }
      }),
    ).pipe(Effect.provideContext(services));

  const update = (
    deviceId: string,
    check: Effect.Effect<void, InvalidInputError>,
    payload: typeof DevicePatch.Type,
  ) =>
    Effect.all([validDeviceId(deviceId), check]).pipe(
      Effect.andThen(
        withCredential((client) =>
          client.updateDevice({ params: { deviceId }, payload }),
        ),
      ),
    );

  const settleOn = (credential: string, error: unknown) => {
    const stop = HubSocket.stopOfError(error);
    return stop === null ? Effect.void : settle(credential, stop);
  };

  // A call under the device credential. One the service answers with
  // the device removed, its credential replaced, or the SDK too old
  // stops the device as the hub socket would.
  const withCredential = <A, E>(
    run: (client: HubApi.Client) => Effect.Effect<A, E>,
  ) =>
    Effect.gen(function* () {
      const current = enrollment;
      if (current === null) return yield* new NotEnrolledError();
      return yield* call(current.credential, run).pipe(
        Effect.tapError((error) => settleOn(current.credential, error)),
      );
    });

  // Tells the service of a sign-out it did not hear of: true once it
  // answered, and the store owes it no more. Under the lifecycle lock.
  const deliver = (owed: Stored.Owed) =>
    call(owed.credential, (client) =>
      client.removeDevice({ params: { deviceId: owed.deviceId } }),
    ).pipe(
      Effect.as(true),
      Effect.catchTags({
        CredentialRejected: () => Effect.succeed(true),
        DeviceRevoked: () => Effect.succeed(true),
        UnknownDevice: () => Effect.succeed(true),
      }),
      Effect.orElseSucceed(() => false),
      Effect.tap((done) => (done ? Stored.remove : Effect.void)),
    );

  const deliverOwed = Effect.gen(function* () {
    const stored = yield* readStored;
    if (!Stored.isOwed(stored)) return;
    yield* locked(
      Effect.gen(function* () {
        // Another run sharing the store may have delivered it since.
        const now = yield* readStored;
        if (Stored.isOwed(now)) yield* deliver(now);
      }),
    );
  }).pipe(Effect.ignore, Effect.provideContext(services));

  const loaded = Stored.enrollmentOf(first);
  if (loaded !== null) {
    enrollment = loaded;
    yield* start(loaded);
  }
  yield* Stream.succeed(undefined).pipe(
    Stream.concat(Stream.fromPubSub(wakes)),
    Stream.runForEach(() => deliverOwed),
    Effect.forkScoped,
  );

  // Another run sharing the store changed it: this one runs what it
  // holds now, or reads as signed out.
  if (shared !== undefined) {
    yield* shared.changes.pipe(
      Stream.runForEach(() =>
        lifecycle.withPermits(1)(
          Effect.gen(function* () {
            const stored = yield* readStored;
            const now = Stored.enrollmentOf(stored);
            if (now !== null) {
              if (now.credential === enrollment?.credential) return;
              enrollment = now;
              return yield* start(now);
            }
            const removed = Stored.isRemoved(stored);
            const state = yield* SubscriptionRef.get(account);
            if (state._tag === "SignedOut" && state.removed === removed) return;
            enrollment = null;
            yield* end;
            yield* SubscriptionRef.set(account, removed ? REMOVED : SIGNED_OUT);
          }).pipe(Effect.ignore),
        ),
      ),
      Effect.provideContext(services),
      Effect.forkScoped,
    );
  }

  const device: Device = {
    account: SubscriptionRef.changes(account),
    roster: SubscriptionRef.changes(peers),
    tunnel: SubscriptionRef.changes(tunnelState),
    enroll: (input) =>
      Effect.gen(function* () {
        yield* validName(input.name);
        if (input.data !== undefined) yield* validData(input.data);
        const seen = enrollment?.credential;
        yield* locked(
          Effect.gen(function* () {
            const stored = yield* readStored;
            const previous = Stored.enrollmentOf(stored);
            // Another run sharing the store enrolled while this one
            // waited: this one runs that enrollment.
            if (previous !== null && previous.credential !== seen) {
              if (enrollment?.credential !== previous.credential) {
                enrollment = previous;
                yield* start(previous);
              }
              return;
            }
            const owed = Stored.isOwed(stored) ? stored : null;
            const owing = owed !== null && !(yield* deliver(owed));
            // A removal still owed keeps its device id, so enrolling
            // replaces the credential it was for, or meets
            // DeviceEnrolledElsewhere and delivers it. Once the service
            // heard of the removal, a made-up id is made up again. A
            // device the service removed keeps its id.
            const fresh = options.deviceId ?? crypto.randomUUID();
            const removedId = Stored.isRemoved(stored) ? stored.deviceId : null;
            const key = generateKeyPair();
            const enrollAs = (deviceId: string) =>
              call(input.loginToken, (client) =>
                client.enroll({
                  payload: {
                    app: options.app,
                    deviceId,
                    name: input.name,
                    publicKey: Base64Url.encode(key.publicKey),
                    connections: options.connections,
                    ...(input.data === undefined ? {} : { data: input.data }),
                  },
                }),
              );
            // The device id is still enrolled under the account it
            // signed out of, until the service hears of it.
            const enrolled = yield* enrollAs(
              owing
                ? owed.deviceId
                : (previous?.deviceId ?? removedId ?? fresh),
            ).pipe(
              Effect.catchTag("DeviceEnrolledElsewhere", (error) =>
                Effect.gen(function* () {
                  if (owing && (yield* deliver(owed))) {
                    return yield* enrollAs(fresh);
                  }
                  return yield* error;
                }),
              ),
            );
            const next: Stored.Enrollment = {
              deviceId: enrolled.device.deviceId,
              accountId: enrolled.accountId,
              credential: enrolled.credential,
              privateKey: key.privateKey,
              ...(previous?.port === undefined ? {} : { port: previous.port }),
            };
            yield* Stored.write(next);
            enrollment = next;
            yield* start(next);
          }),
        );
      }).pipe(Effect.provideContext(services)),
    signOut: locked(
      Effect.gen(function* () {
        // Another run sharing the store may have enrolled again, or
        // signed out, since this one last heard.
        const current = (yield* readStored) ?? enrollment;
        if (current === null) {
          const state = yield* SubscriptionRef.get(account);
          if (state._tag === "SignedOut" && state.removed) {
            yield* SubscriptionRef.set(account, SIGNED_OUT);
          }
          return true;
        }
        // The service let the device go: nothing to revoke.
        if (Stored.isRemoved(current)) {
          yield* Stored.remove;
          yield* end;
          enrollment = null;
          yield* SubscriptionRef.set(account, SIGNED_OUT);
          return true;
        }
        if (Stored.isOwed(current)) return yield* deliver(current);
        const owed: Stored.Owed = {
          deviceId: current.deviceId,
          credential: current.credential,
          signedOut: true,
        };
        // The store first, so a write that fails leaves the device
        // running as it was.
        yield* Stored.write(owed);
        yield* end;
        enrollment = null;
        yield* SubscriptionRef.set(account, SIGNED_OUT);
        return yield* deliver(owed);
      }),
    ).pipe(Effect.provideContext(services)),
    devices: {
      list: withCredential((client) => client.listDevices()).pipe(
        Effect.map((answer) => answer.devices),
      ),
      rename: (deviceId, name) => update(deviceId, validName(name), { name }),
      setData: (deviceId, data) => update(deviceId, validData(data), { data }),
      remove: (deviceId) =>
        Effect.gen(function* () {
          yield* validDeviceId(deviceId);
          const self = enrollment;
          yield* withCredential((client) =>
            client.removeDevice({ params: { deviceId } }),
          );
          // Without its hub socket to say so, as while stopped.
          if (self?.deviceId === deviceId) {
            yield* settle(self.credential, "removed");
          }
        }),
    },
    dial: (deviceId, appHello) =>
      Effect.gen(function* () {
        if (appHello.length > MAX_APP_BYTES) {
          return yield* new InvalidInputError({ field: "appHello" });
        }
        const current = session;
        if (current === null) {
          return yield* new DialError({ deviceId, reason: "not-connected" });
        }
        return yield* Dial.dial({
          hub: current.hub,
          roster: current.roster,
          identity: current.identity,
          deviceId,
          appHello,
          join: current.join,
          wakes,
        }).pipe(
          Effect.raceFirst(
            Deferred.await(current.ended).pipe(
              Effect.andThen(
                Effect.fail(
                  new DialError({ deviceId, reason: "not-connected" }),
                ),
              ),
            ),
          ),
        );
      }).pipe(Effect.provideContext(services)),
    wake: PubSub.publish(wakes, undefined).pipe(Effect.asVoid),
    wakes: Stream.fromPubSub(wakes),
  };
  return device;
});

type Requirements = Scope.Scope | Services;

// Makes the device, which starts its hub socket at once when the store
// holds an enrollment.
export function make(
  options: DeviceOptions & { readonly listen: ListenOptions },
): Effect.Effect<
  Device,
  SessionFailure | InvalidInputError,
  Requirements | ListenerSockets
>;
export function make(
  options: DeviceOptions,
): Effect.Effect<Device, SessionFailure | InvalidInputError, Requirements>;
export function make(options: DeviceOptions) {
  return makeDevice(options);
}
