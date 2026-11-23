// Devices in this process against the real service in workerd. Each
// test file starts its own service.
import { createServer, type AddressInfo, type Socket } from "node:net";
import { networkInterfaces } from "node:os";
import { newIp, sessionToken } from "@colibrii/harness";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { afterAll } from "vitest";
import * as Colibrii from "../src/index.ts";
import { lanAddresses } from "../src/internal/listener.ts";
import { layerListenerSockets, layerWebSockets } from "../src/node/sockets.ts";

// What a test waits for, at most.
export const WAIT = Duration.seconds(8);

// Where a request goes instead of the hub: "refused" fails as if the
// hub could not be reached, and "silent" meets one that takes the
// connection and never answers.
type Diversion = "refused" | "silent";

export interface Net {
  divert: (request: HttpClientRequest.HttpClientRequest) => Diversion | null;
}

// A port that takes connections and never answers.
const silentPort = await new Promise<number>((resolve) => {
  const held = new Set<Socket>();
  const server = createServer((socket) => held.add(socket));
  server.listen(0, "127.0.0.1", () =>
    resolve((server.address() as AddressInfo).port),
  );
  afterAll(() => {
    for (const socket of held) socket.destroy();
    server.close();
  });
});

// The service budgets the routes that open things per client address.
// Each request here comes from an address of its own, so only a test
// that means to reach a limit meets one. A refused request goes to a
// port nothing listens on, and a silenced one to a port that never
// answers. Both go there once the hub's address is in front, which
// comes after any mapRequest.
const makeHttp = (net: Net) =>
  Layer.effect(
    HttpClient.HttpClient,
    Effect.map(HttpClient.HttpClient, (client) =>
      HttpClient.makeWith(
        (request) =>
          client.postprocess(
            Effect.map(request, (final) => {
              const diversion = net.divert(final);
              return diversion === null
                ? HttpClientRequest.setHeader(
                    final,
                    "cf-connecting-ip",
                    newIp(),
                  )
                : HttpClientRequest.setUrl(
                    final,
                    `http://127.0.0.1:${diversion === "refused" ? 1 : silentPort}/`,
                  );
            }),
          ),
        client.preprocess,
      ),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer));

// A socket the device opened, and everything it sent on it. Once deaf,
// its close never reaches the device.
export interface Opened {
  readonly url: string;
  readonly conn: Colibrii.Conn;
  readonly sent: Array<string | Uint8Array>;
  deaf: boolean;
}

// The Node websockets, with every URL the devices try and every socket
// they open kept for the test to look at. While `refusing`, no socket
// opens.
export interface Spy {
  readonly tried: Array<string>;
  readonly opened: Array<Opened>;
  refusing: boolean;
  readonly layer: Layer.Layer<Colibrii.WebSockets>;
}

export const makeSpy = (): Spy => {
  const spy: Spy = {
    tried: [] as Array<string>,
    opened: [] as Array<Opened>,
    refusing: false,
    layer: Layer.effect(
      Colibrii.WebSockets,
      Effect.map(Colibrii.WebSockets, (real) => ({
        ...real,
        open: (url: string) =>
          Effect.map(
            Effect.andThen(
              Effect.suspend(() => {
                spy.tried.push(url);
                return spy.refusing
                  ? Effect.fail(new Colibrii.WebSocketOpenError({}))
                  : Effect.void;
              }),
              real.open(url),
            ),
            (conn) => {
              const entry: Opened = { url, conn, sent: [], deaf: false };
              spy.opened.push(entry);
              return {
                ...conn,
                send: (message: string | Uint8Array) => {
                  entry.sent.push(message);
                  conn.send(message);
                },
                receive: Effect.catch(conn.receive, (close) =>
                  entry.deaf ? Effect.never : Effect.fail(close),
                ),
                closed: Effect.filterOrElse(
                  conn.closed,
                  () => !entry.deaf,
                  () => Effect.never,
                ),
              };
            },
          ),
      })),
    ).pipe(Layer.provide(layerWebSockets)),
  };
  return spy;
};

const memoryStore = () => {
  let text = Option.none<string>();
  return Colibrii.CredentialStore.of({
    read: Effect.sync(() => text),
    write: (next) =>
      Effect.sync(() => {
        text = Option.some(next);
      }),
    remove: Effect.sync(() => {
      text = Option.none();
    }),
  });
};

export const makeKit = (hub: string) => {
  const spy = makeSpy();
  const net: Net = { divert: () => null };
  const layer = Layer.mergeAll(spy.layer, layerListenerSockets, makeHttp(net));

  const run = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      | Scope.Scope
      | Colibrii.WebSockets
      | Colibrii.ListenerSockets
      | HttpClient.HttpClient
    >,
  ) => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(layer)));

  // A device, with a store of its own unless it shares one.
  const device = (
    options: {
      readonly listen?: Colibrii.ListenOptions;
      readonly connections?: "single" | "multi";
      readonly store?: Colibrii.CredentialStore["Service"];
      readonly deviceId?: string;
    } = {},
  ) =>
    Colibrii.make({
      hub,
      app: "inkwell",
      connections: options.connections ?? "single",
      ...(options.listen === undefined ? {} : { listen: options.listen }),
      ...(options.deviceId === undefined ? {} : { deviceId: options.deviceId }),
    }).pipe(
      Effect.provideService(
        Colibrii.CredentialStore,
        options.store ?? memoryStore(),
      ),
    );

  const enroll = (
    target: Colibrii.Device,
    account: string,
    name = "Test device",
  ) =>
    Effect.gen(function* () {
      const loginToken = yield* Effect.promise(() => sessionToken(account));
      yield* target.enroll({ loginToken, name });
      return yield* waitFor(
        target.account,
        (state) => state._tag === "Enrolled" && state.connected,
      );
    });

  const diverted =
    (diversion: Diversion) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          net.divert = () => diversion;
        }),
        () => effect,
        () =>
          Effect.sync(() => {
            net.divert = () => null;
          }),
      );
  // Runs `effect` with every request to the hub failing.
  const offline = diverted("refused");
  // Runs `effect` with every request to the hub unanswered.
  const silenced = diverted("silent");

  return { spy, net, run, device, enroll, memoryStore, offline, silenced };
};

// Runs `effect` until its value passes `test`.
export const eventually = <A, E>(
  effect: Effect.Effect<A, E>,
  test: (value: A) => boolean,
) =>
  Effect.gen(function* () {
    while (true) {
      const value = yield* effect;
      if (test(value)) return value;
      yield* Effect.sleep(Duration.millis(100));
    }
  }).pipe(Effect.timeout(WAIT), Effect.orDie);

export const waitFor = <A>(
  stream: Stream.Stream<A>,
  test: (value: A) => boolean,
) =>
  stream.pipe(
    Stream.filter(test),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
    Effect.timeout(WAIT),
    Effect.orDie,
  );

export const deviceIdOf = (state: Colibrii.AccountState) =>
  state._tag === "SignedOut" ? "" : state.deviceId;

// A dial between two devices on this machine goes to an address the
// listener offers, which needs a LAN.
export const hasLanAddress =
  lanAddresses(
    Object.values(networkInterfaces()).flatMap((entries) => entries ?? []),
  ).length > 0;
