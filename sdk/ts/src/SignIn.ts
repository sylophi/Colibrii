// Signing in with a code (spec/service.md, "Signing in with a code"):
// Clerk's device authorization grant, at the app's Clerk instance and
// under its OAuth client, for a device with no browser to sign in
// through. The access token it ends with is a login token for
// enrollment, and `signInToken` trades it for a Clerk sign-in token.
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { CodeSignInError } from "./Errors.ts";
import * as HubApi from "./internal/hubApi.ts";

const DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code";
// What `slow_down` adds to the wait, from then on.
const SLOW_DOWN_SECONDS = 5;
const DEFAULT_INTERVAL_SECONDS = 5;

export interface CodeSignInOptions {
  // The app's Clerk publishable key, which encodes its Frontend API.
  readonly publishableKey: string;
  // The app's OAuth client.
  readonly clientId: string;
}

// What to show the person, and what finishing needs.
export interface SignInCode {
  readonly userCode: string;
  readonly verificationUri: string;
  // The page with the code already in it.
  readonly verificationUriComplete: string | undefined;
  readonly deviceCode: string;
  readonly intervalSeconds: number;
  // When the code expires, in milliseconds since the Unix epoch.
  readonly expiresAt: number;
}

const Discovery = Schema.Struct({
  token_endpoint: Schema.String,
  device_authorization_endpoint: Schema.optional(Schema.String),
});

const DeviceCode = Schema.Struct({
  device_code: Schema.String,
  user_code: Schema.String,
  verification_uri: Schema.String,
  verification_uri_complete: Schema.optional(Schema.String),
  expires_in: Schema.Finite,
  interval: Schema.optional(Schema.Finite),
});

const Tokens = Schema.Struct({ access_token: Schema.String });

const OAuthError = Schema.Struct({
  error: Schema.String,
  error_description: Schema.optional(Schema.String),
});

const unreachable = (cause: unknown) =>
  new CodeSignInError({ reason: "unreachable", cause });

// The Frontend API's host: the key is `pk_test_` or `pk_live_` and the
// base64 of the host followed by `$`.
const frontendHost = (publishableKey: string) =>
  Effect.try({
    try: () => atob(publishableKey.replace(/^pk_(test|live)_/, "")),
    catch: (cause) =>
      new CodeSignInError({
        reason: "refused",
        words: "bad publishable key",
        cause,
      }),
  }).pipe(Effect.map((decoded) => decoded.replace(/\$$/, "")));

// The answer's status and JSON, whatever the status.
const fetchJson = (request: HttpClientRequest.HttpClientRequest) =>
  HttpClient.execute(request).pipe(
    Effect.flatMap((response) =>
      Effect.map(response.json, (body) => ({
        ok: response.status < 300,
        body,
      })),
    ),
    Effect.mapError(unreachable),
    Effect.timeoutOrElse({
      duration: HubApi.ANSWER_WITHIN_MS,
      orElse: () => Effect.fail(unreachable("no answer in time")),
    }),
  );

const endpoints = Effect.fnUntraced(function* (options: CodeSignInOptions) {
  const host = yield* frontendHost(options.publishableKey);
  const { body } = yield* fetchJson(
    HttpClientRequest.get(`https://${host}/.well-known/openid-configuration`),
  );
  const discovery = yield* Schema.decodeUnknownEffect(Discovery)(body).pipe(
    Effect.mapError(unreachable),
  );
  if (discovery.device_authorization_endpoint === undefined) {
    return yield* new CodeSignInError({ reason: "off" });
  }
  return {
    device: discovery.device_authorization_endpoint,
    token: discovery.token_endpoint,
  };
});

const wordsOf = (body: unknown) =>
  Option.match(Schema.decodeUnknownOption(OAuthError)(body), {
    onNone: () => undefined,
    onSome: (error) => error.error_description ?? error.error,
  });

export const startCodeSignIn = Effect.fn("startCodeSignIn")(function* (
  options: CodeSignInOptions,
) {
  const { device } = yield* endpoints(options);
  const askedAt = yield* Clock.currentTimeMillis;
  const answer = yield* fetchJson(
    HttpClientRequest.post(device).pipe(
      HttpClientRequest.bodyUrlParams({ client_id: options.clientId }),
    ),
  );
  if (!answer.ok) {
    return yield* new CodeSignInError({
      reason: "refused",
      words: wordsOf(answer.body),
    });
  }
  const code = yield* Schema.decodeUnknownEffect(DeviceCode)(answer.body).pipe(
    Effect.mapError(unreachable),
  );
  return {
    userCode: code.user_code,
    verificationUri: code.verification_uri,
    verificationUriComplete: code.verification_uri_complete,
    deviceCode: code.device_code,
    intervalSeconds: code.interval ?? DEFAULT_INTERVAL_SECONDS,
    expiresAt: askedAt + code.expires_in * 1000,
  } satisfies SignInCode;
});

// Asks Clerk at the pace it set until the person approves or denies the
// code, or it expires, and answers the access token. A turn that fails
// to reach Clerk asks again at the next.
export const finishCodeSignIn = Effect.fn("finishCodeSignIn")(function* (
  options: CodeSignInOptions,
  code: SignInCode,
) {
  // Each step ends as the code does.
  const beforeExpiry = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(Clock.currentTimeMillis, (now) =>
      now >= code.expiresAt
        ? Effect.fail(new CodeSignInError({ reason: "expired" }))
        : effect.pipe(
            Effect.timeoutOrElse({
              duration: code.expiresAt - now,
              orElse: () =>
                Effect.fail(new CodeSignInError({ reason: "expired" })),
            }),
          ),
    );
  const { token } = yield* beforeExpiry(endpoints(options));
  let intervalSeconds = code.intervalSeconds;
  while (true) {
    yield* beforeExpiry(Effect.sleep(intervalSeconds * 1000));
    const answer = yield* beforeExpiry(
      fetchJson(
        HttpClientRequest.post(token).pipe(
          HttpClientRequest.bodyUrlParams({
            grant_type: DEVICE_GRANT,
            device_code: code.deviceCode,
            client_id: options.clientId,
          }),
        ),
      ).pipe(Effect.option),
    );
    if (Option.isNone(answer)) continue;
    if (answer.value.ok) {
      return yield* Schema.decodeUnknownEffect(Tokens)(answer.value.body).pipe(
        Effect.map((tokens) => tokens.access_token),
        Effect.mapError(unreachable),
      );
    }
    const error = Schema.decodeUnknownOption(OAuthError)(answer.value.body);
    switch (Option.getOrUndefined(error)?.error) {
      case "authorization_pending":
        continue;
      case "slow_down":
        intervalSeconds += SLOW_DOWN_SECONDS;
        continue;
      case "access_denied":
        return yield* new CodeSignInError({ reason: "denied" });
      case "expired_token":
        return yield* new CodeSignInError({ reason: "expired" });
      default:
        return yield* new CodeSignInError({
          reason: "refused",
          words: wordsOf(answer.value.body),
        });
    }
  }
});

// `POST /sign-in-token`: a Clerk sign-in token for the account the OAuth
// access token names, which the app redeems with Clerk for a session.
export const signInToken = Effect.fn("signInToken")(function* (options: {
  readonly hub: string;
  readonly app: string;
  readonly accessToken: string;
}) {
  const call = yield* HubApi.make(options.hub);
  const { token } = yield* call(options.accessToken, (client) =>
    client.signInToken({ payload: { app: options.app } }),
  );
  return token;
});
