// The service's HTTP API as the device calls it: every request names the
// wire version, under the bearer the route takes. A failure to reach the
// hub, no answer in time, or an answer the wire does not describe, reads
// as HubUnreachable, and a 429 as RateLimited.
import * as Effect from "effect/Effect";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as HttpClient from "effect/http/HttpClient";
import type * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import { HubUnreachableError, RateLimitedError } from "../Errors.ts";
import { Api, VERSION_HEADER, WIRE_VERSION } from "../wire/api.ts";

export type Client = HttpApiClient.ForApi<typeof Api>;

// How long a call waits for its answer. A fetch on a network that went
// quiet would wait minutes, and in a page for good.
export const ANSWER_WITHIN_MS = 15_000;

// Without a readable `retry-after`, a 429 waits this long.
const DEFAULT_RETRY_AFTER_SECONDS = 60;

export const retryAfterOf = (header: string | undefined): number => {
  const seconds = Number(header);
  return header !== undefined &&
    header.trim() !== "" &&
    Number.isFinite(seconds) &&
    seconds >= 0
    ? seconds
    : DEFAULT_RETRY_AFTER_SECONDS;
};

const unreachable = (
  error: HttpClientError.HttpClientError | Schema.SchemaError,
): HubUnreachableError | RateLimitedError =>
  error._tag === "HttpClientError" && error.response?.status === 429
    ? new RateLimitedError({
        retryAfterSeconds: retryAfterOf(error.response.headers["retry-after"]),
      })
    : new HubUnreachableError({ cause: error });

const isTransport = (
  error: unknown,
): error is HttpClientError.HttpClientError | Schema.SchemaError =>
  Predicate.isTagged(error, "HttpClientError") ||
  Predicate.isTagged(error, "SchemaError");

export const make = Effect.fnUntraced(function* (hub: string) {
  const http = (yield* HttpClient.HttpClient).pipe(
    HttpClient.mapRequest(
      HttpClientRequest.setHeader(VERSION_HEADER, String(WIRE_VERSION)),
    ),
  );
  // One call under `bearer`: a login token, or the device credential.
  return <A, E>(
    bearer: string,
    run: (client: Client) => Effect.Effect<A, E>,
  ): Effect.Effect<
    A,
    | Exclude<E, HttpClientError.HttpClientError | Schema.SchemaError>
    | HubUnreachableError
    | RateLimitedError
  > =>
    HttpApiClient.makeWith(Api, {
      httpClient: http.pipe(
        HttpClient.mapRequest(HttpClientRequest.bearerToken(bearer)),
      ),
      baseUrl: hub,
    }).pipe(
      Effect.flatMap(run),
      Effect.mapError((error) =>
        isTransport(error) ? unreachable(error) : error,
      ),
      Effect.timeoutOrElse({
        duration: ANSWER_WITHIN_MS,
        orElse: () =>
          Effect.fail(new HubUnreachableError({ cause: "no answer in time" })),
      }),
      // No trace headers: the service reads none, and a browser could
      // not send them past its CORS allowlist.
      Effect.provideService(HttpClient.TracerPropagationEnabled, false),
    ) as never;
});

export type Call = Effect.Success<ReturnType<typeof make>>;
