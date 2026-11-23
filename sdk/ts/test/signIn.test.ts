import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect, it } from "vitest";
import * as Colibrii from "../src/index.ts";

const options = {
  publishableKey: `pk_test_${btoa("clerk.test$")}`,
  clientId: "inkwell-oauth",
};

// Clerk with the device grant on, handing out a code that lives 10
// seconds and is asked about every 5, and that no one approves.
const clerk = () => {
  const tokenAsks: Array<string> = [];
  const client = HttpClient.make((request) => {
    const json = (body: unknown, status = 200) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(JSON.stringify(body), { status }),
        ),
      );
    if (request.url.endsWith("/.well-known/openid-configuration")) {
      return json({
        token_endpoint: "https://clerk.test/token",
        device_authorization_endpoint: "https://clerk.test/device",
      });
    }
    if (request.url.endsWith("/device")) {
      return json({
        device_code: "device-code",
        user_code: "ABCD-EFGH",
        verification_uri: "https://clerk.test/verify",
        expires_in: 10,
        interval: 5,
      });
    }
    tokenAsks.push(request.url);
    return json({ error: "authorization_pending" }, 400);
  });
  return { client, tokenAsks };
};

// Moves the test clock a second at a time, letting the stub's answers
// settle between.
const pass = (seconds: number) =>
  Effect.gen(function* () {
    for (let i = 0; i < seconds; i += 1) {
      yield* Effect.promise(
        () => new Promise((resolve) => setTimeout(resolve)),
      );
      yield* TestClock.adjust("1 second");
    }
  });

describe("signing in with a code", () => {
  it("asks for no token past the code's life, counted from its issue", () => {
    const { client, tokenAsks } = clerk();
    return Effect.runPromise(
      Effect.gen(function* () {
        const code = yield* Colibrii.startCodeSignIn(options);
        yield* pass(8);
        const finishing = yield* Colibrii.finishCodeSignIn(options, code).pipe(
          Effect.flip,
          Effect.forkChild,
        );
        yield* pass(20);
        expect(yield* Fiber.join(finishing)).toMatchObject({
          reason: "expired",
        });
        expect(tokenAsks).toEqual([]);
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(TestClock.layer()),
      ),
    );
  });
});
