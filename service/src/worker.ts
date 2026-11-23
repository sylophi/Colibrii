// The Worker: the HTTP API (sdk wire/api.ts) served with its handlers,
// the version floor, the two kinds of bearer, the rate limits and open
// CORS. It also exports the Durable Object class bound as HUB.
import * as Base64Url from "@colibrii/sdk/wire/base64url";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import {
  AccountFullError,
  Api,
  Caller,
  CredentialRejectedError,
  DeviceAuth,
  DeviceEnrolledElsewhereError,
  DeviceRevokedError,
  LoginAuth,
  LoginRejectedError,
  LoginToken,
  SignInTokensOffError,
  TicketMalformedError,
  UnavailableError,
  UnknownAppError,
  UnknownDeviceError,
  UpdateRequiredError,
  UpgradeRequiredError,
  VERSION_HEADER,
  VersionFloor,
} from "@colibrii/sdk/wire/api";
import {
  credentialHash,
  DEVICE_CREDENTIAL_PREFIX,
  DeviceCredential,
} from "@colibrii/sdk/wire/credential";
import { mintSignInToken, verifyLogin } from "./clerk.ts";
import { Instance, readInstance } from "./config.ts";
import { type Env, WaitUntil, WorkerEnv } from "./env.ts";
import { CONNECT_RANDOM_PARAM, type EnrollOutcome } from "./hub.ts";
import * as Registry from "./Registry.ts";
import { buildTicket, parseTicket } from "./ticket.ts";
import { provisionTunnel, removeTunnel } from "./tunnel.ts";

export { Hub } from "./hub.ts";

const VERSION_FLOOR = 1;

// How long a removal's answer waits on the tunnel's, well inside the 15
// seconds a device waits for it.
const TUNNEL_REMOVAL_WAIT = Duration.seconds(8);

const hubOf = (env: Env, account: Registry.Account) =>
  env.HUB.get(env.HUB.idFromName(`${account.appId}/${account.accountId}`));

// A failed step behind a route, its cause logged: the answer says only
// which step.
const unavailable =
  (operation: UnavailableError["operation"]) => (cause: unknown) =>
    Effect.logError(`${operation} failed`, cause).pipe(
      Effect.andThen(Effect.fail(new UnavailableError({ operation }))),
    );

const attempt = <A>(
  operation: UnavailableError["operation"],
  promise: () => Promise<A>,
) =>
  Effect.tryPromise(promise).pipe(
    Effect.catchTags({ UnknownError: unavailable(operation) }),
  );

// ---- Middleware ----

const versionFloor = Layer.succeed(VersionFloor, (effect) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const version = Number(request.headers[VERSION_HEADER]);
    if (!Number.isInteger(version) || version < VERSION_FLOOR) {
      return yield* new UpdateRequiredError({ floor: VERSION_FLOOR });
    }
    return yield* effect;
  }),
);

const loginAuth = Layer.succeed(LoginAuth, {
  bearer: (effect, { credential }) =>
    Effect.provideService(effect, LoginToken, credential),
});

const isCredential = Schema.is(DeviceCredential);

// A credential that matches no device: one removed in the last 14 days,
// or one never issued or since replaced.
const unmatched = Effect.fnUntraced(function* (
  registry: Registry.Registry["Service"],
  hash: string,
) {
  return yield* (yield* registry.isRevoked(hash))
    ? new DeviceRevokedError()
    : new CredentialRejectedError();
});

const deviceAuth = Layer.effect(
  DeviceAuth,
  Effect.gen(function* () {
    const registry = yield* Registry.Registry;
    return DeviceAuth.of({
      bearer: (effect, { credential }) =>
        Effect.gen(function* () {
          const token = Redacted.value(credential);
          if (!isCredential(token)) return yield* new CredentialRejectedError();
          const hash = yield* Effect.promise(() => credentialHash(token));
          const device = yield* registry.byCredentialHash(hash);
          if (device === null) return yield* unmatched(registry, hash);
          return yield* Effect.provideService(effect, Caller, {
            appId: device.app_id,
            accountId: device.account_id,
            deviceId: device.device_id,
            credentialHash: hash,
          });
        }).pipe(Effect.catchTags({ RegistryError: unavailable("credential") })),
    });
  }),
);

// ---- Handlers ----

const handlers = Effect.gen(function* () {
  const env = yield* WorkerEnv;
  const instance = yield* Instance;
  const registry = yield* Registry.Registry;

  // The app is read on each request, so one the owner adds, changes or
  // removes counts from the next.
  const loginFor = Effect.fnUntraced(function* (
    appId: string,
    operation: "enroll" | "sign-in-token",
  ) {
    const app = yield* registry
      .app(appId)
      .pipe(Effect.catchTags({ RegistryError: unavailable(operation) }));
    if (app === null) return yield* new UnknownAppError({ app: appId });
    const token = Redacted.value(yield* LoginToken);
    const login = yield* Effect.promise(() => verifyLogin(token, app));
    if (login === null) return yield* new LoginRejectedError();
    return { app, login };
  });

  // After the answer, and best effort (tunnel.ts), for a device other
  // than the caller.
  const removeTunnelLater = Effect.fnUntraced(function* (
    account: Registry.Account,
    deviceId: string,
  ) {
    const waitUntil = yield* WaitUntil;
    waitUntil(removeTunnel(instance.tunnel, account, deviceId));
  });

  const loginRoutes = HttpApiBuilder.group(Api, "login", (handle) =>
    handle
      .handle("enroll", ({ payload }) =>
        Effect.gen(function* () {
          const { app, login } = yield* loginFor(payload.app, "enroll");
          const account = { appId: app.id, accountId: login.accountId };
          const credential = DEVICE_CREDENTIAL_PREFIX + Base64Url.random(32);
          const hash = yield* Effect.promise(() => credentialHash(credential));
          const outcome = yield* attempt(
            "enroll",
            async (): Promise<EnrollOutcome> =>
              await hubOf(env, account).enroll({
                ...account,
                deviceId: payload.deviceId,
                name: payload.name,
                data: payload.data ?? {},
                connections: payload.connections,
                publicKey: payload.publicKey,
                credentialHash: hash,
                deviceCap: app.deviceCap,
              }),
          );
          if (outcome.status === "enrolled-elsewhere") {
            return yield* new DeviceEnrolledElsewhereError();
          }
          if (outcome.status === "account-full") {
            return yield* new AccountFullError({ limit: app.deviceCap });
          }
          if (outcome.evicted !== null) {
            yield* removeTunnelLater(account, outcome.evicted);
          }
          return {
            credential,
            accountId: login.accountId,
            device: outcome.device,
          };
        }),
      )
      .handle("signInToken", ({ payload }) =>
        Effect.gen(function* () {
          const { app, login } = yield* loginFor(payload.app, "sign-in-token");
          // Whoever holds a session token has a session already.
          if (!login.oauth) return yield* new LoginRejectedError();
          const secretName = app.clerkSecretName;
          if (secretName === undefined) {
            return yield* new SignInTokensOffError();
          }
          const secretKey = env[secretName];
          if (typeof secretKey !== "string" || secretKey === "") {
            return yield* Effect.die(
              new Error(
                `the secret ${secretName} that app ${app.id} names is not set`,
              ),
            );
          }
          const token = yield* attempt("sign-in-token", () =>
            mintSignInToken(secretKey, login.accountId),
          );
          return { token };
        }),
      ),
  );

  const deviceRoutes = HttpApiBuilder.group(Api, "devices", (handle) =>
    handle
      .handle("listDevices", () =>
        Effect.gen(function* () {
          const caller = yield* Caller;
          const [rows, online] = yield* Effect.all(
            [
              registry.list(caller),
              Effect.tryPromise(() => hubOf(env, caller).online()),
            ],
            { concurrency: 2 },
          );
          const present = new Set(online);
          return {
            devices: rows.map((row) =>
              Registry.toDevice(row, present.has(row.device_id)),
            ),
          };
        }).pipe(
          Effect.catchTags({
            RegistryError: unavailable("list"),
            UnknownError: unavailable("list"),
          }),
        ),
      )
      .handle("updateDevice", ({ params, payload }) =>
        Effect.gen(function* () {
          const caller = yield* Caller;
          const updated = yield* registry.update(
            caller,
            params.deviceId,
            payload,
          );
          if (!updated) {
            return yield* new UnknownDeviceError({ deviceId: params.deviceId });
          }
        }).pipe(Effect.catchTags({ RegistryError: unavailable("update") })),
      )
      .handle("removeDevice", ({ params }) =>
        Effect.gen(function* () {
          const caller = yield* Caller;
          const outcome = yield* attempt("revoke", () =>
            hubOf(env, caller).remove(
              caller,
              params.deviceId,
              caller.credentialHash,
            ),
          );
          if (outcome === "rejected") {
            return yield* unmatched(registry, caller.credentialHash);
          }
          if (outcome === "unknown") {
            return yield* new UnknownDeviceError({ deviceId: params.deviceId });
          }
          // Before the answer, best effort (tunnel.ts): a device that
          // enrolls again once answered finds no tunnel left to remove
          // under it. A removal slower than TUNNEL_REMOVAL_WAIT goes on
          // after the answer, even once the caller left, and may delete
          // the tunnel such a device provisions meanwhile.
          const waitUntil = yield* WaitUntil;
          const removal = removeTunnel(
            instance.tunnel,
            caller,
            params.deviceId,
          );
          waitUntil(removal);
          yield* Effect.promise(() => removal).pipe(
            Effect.timeoutOption(TUNNEL_REMOVAL_WAIT),
          );
        }).pipe(Effect.catchTags({ RegistryError: unavailable("revoke") })),
      )
      .handle("mintTicket", ({ payload }) =>
        Effect.gen(function* () {
          const caller = yield* Caller;
          const random = yield* attempt("ticket", () =>
            hubOf(env, caller).mintTicket({
              deviceId: caller.deviceId,
              connectionId: payload.connectionId,
              credentialHash: caller.credentialHash,
            }),
          );
          const ticket = yield* Effect.promise(() =>
            buildTicket(instance.ticketSigningKey, caller, random),
          );
          return { ticket };
        }),
      )
      .handle("provisionTunnel", ({ payload }) =>
        Effect.gen(function* () {
          const caller = yield* Caller;
          return yield* attempt("tunnel", () =>
            provisionTunnel(
              instance.tunnel,
              caller,
              caller.deviceId,
              payload.port,
            ),
          );
        }),
      ),
  );

  // A ticket that is malformed or not signed here names no object, so
  // this unauthenticated route wakes none of a caller's choosing. What
  // the object refuses (spent, expired, a replaced credential) is a close
  // code after the upgrade.
  const socketRoute = HttpApiBuilder.group(Api, "socket", (handle) =>
    handle.handle("connect", ({ request, query }) =>
      Effect.gen(function* () {
        if (request.headers["upgrade"]?.toLowerCase() !== "websocket") {
          return yield* new UpgradeRequiredError();
        }
        const ticket = yield* Effect.promise(() =>
          parseTicket(instance.ticketSigningKey, query.ticket ?? ""),
        );
        if (ticket === null) return yield* new TicketMalformedError();
        const source = request.source;
        if (!(source instanceof Request)) {
          return yield* Effect.die("the connect route serves web requests");
        }
        const url = new URL("https://hub.internal/");
        url.searchParams.set(CONNECT_RANDOM_PARAM, ticket.random);
        const upgraded = yield* Effect.promise(() =>
          hubOf(env, ticket.account).fetch(new Request(url, source)),
        );
        // A fresh Response: the object's headers are immutable, and CORS
        // writes into them on the way out.
        return HttpServerResponse.raw(
          new Response(null, { status: 101, webSocket: upgraded.webSocket }),
        );
      }),
    ),
  );

  return Layer.mergeAll(loginRoutes, deviceRoutes, socketRoute);
});

// ---- Rate limits, CORS and logs ----

const RETRY_AFTER_SECONDS = 60;

// The routes that work before a device credential is checked, by the
// route the router matched.
const OPEN_ROUTES: ReadonlySet<string> = new Set(
  [
    Api.groups.login.endpoints.enroll,
    Api.groups.login.endpoints.signInToken,
    Api.groups.socket.endpoints.connect,
  ].map((endpoint) => `${endpoint.method} ${endpoint.path}`),
);

// Per client IP, which Cloudflare's edge sets and a client cannot. A
// request without it did not come through the edge. The budgets bound
// what a stranger can cost, so a limiter that fails lets the request on.
const rateLimit = HttpRouter.middleware(
  Effect.gen(function* () {
    const env = yield* WorkerEnv;
    return (httpEffect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const ip = request.headers["cf-connecting-ip"];
        if (ip === undefined) return yield* httpEffect;
        const { route } = yield* HttpRouter.RouteContext;
        const limiter = OPEN_ROUTES.has(`${route.method} ${route.path}`)
          ? env.RATE_LIMIT_OPEN
          : env.RATE_LIMIT;
        const { success } = yield* Effect.tryPromise(() =>
          limiter.limit({ key: ip }),
        ).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("rate limiter failed", cause).pipe(
              Effect.as({ success: true }),
            ),
          ),
        );
        if (success) return yield* httpEffect;
        return HttpServerResponse.empty({
          status: 429,
          headers: { "retry-after": String(RETRY_AFTER_SECONDS) },
        });
      });
  }),
);

// A defect (the router's 500) is logged once, by its route, never by
// the request's URL, whose query can hold a ticket.
const logDefects = HttpRouter.middleware((httpEffect) =>
  Effect.tapCause(httpEffect, (cause) =>
    Effect.gen(function* () {
      const defects = cause.reasons.filter(
        (reason) =>
          Cause.isDieReason(reason) && !ErrorReporter.isIgnored(reason.defect),
      );
      if (defects.length === 0) return;
      const { route } = yield* HttpRouter.RouteContext;
      yield* Effect.logError(
        `${route.method} ${route.path} failed`,
        Cause.fromReasons(defects),
      );
    }),
  ),
);

// No route reads a cookie or any other ambient credential, so any origin
// may call.
const cors = HttpRouter.cors({
  allowedMethods: ["GET", "POST", "PATCH", "DELETE"],
  allowedHeaders: ["authorization", "content-type", VERSION_HEADER],
  exposedHeaders: ["retry-after"],
  maxAge: 86_400,
});

const logs = Logger.layer([Logger.withLeveledConsole(Logger.formatLogFmt)]);

// ---- The Worker ----

const buildHandler = (env: Env) => {
  const instance = readInstance(env);
  const api = HttpApiBuilder.layer(Api).pipe(
    Layer.provide(Layer.unwrap(handlers)),
    Layer.provide([versionFloor, loginAuth, deviceAuth]),
    Layer.provide(rateLimit.layer),
    Layer.provide(logDefects.layer),
    Layer.provide(cors),
    Layer.provideMerge(logs),
    Layer.provide(Registry.layer),
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(WorkerEnv, env),
        Layer.succeed(Instance, instance),
      ),
    ),
    Layer.provide(HttpServer.layerServices),
  );
  return HttpRouter.toWebHandler(api, { disableLogger: true }).handler;
};

// One per env: an isolate can outlive a change of bindings or secrets,
// which hands it a new env.
const built = new WeakMap<Env, ReturnType<typeof buildHandler>>();

export default {
  async fetch(request, env, ctx) {
    let handler = built.get(env);
    if (handler === undefined) {
      handler = buildHandler(env);
      built.set(env, handler);
    }
    // A body is read as JSON whatever its content-type.
    if (request.method === "POST" || request.method === "PATCH") {
      const headers = new Headers(request.headers);
      headers.set("content-type", "application/json");
      request = new Request(request, { headers });
    }
    return await handler(
      request,
      Context.make(WaitUntil, (promise) => ctx.waitUntil(promise)),
    );
  },
} satisfies ExportedHandler<Env>;
