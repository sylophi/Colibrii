import { describe, expect, it } from "vitest";
import { clerkPublicKey as key, startService } from "@colibrii/harness";

const app = (id: string, origin: string, oauthClientId: string) => ({
  id,
  clerkPublicKey: key,
  origins: [origin],
  oauthClientId,
});

// A configuration the Worker refuses stops it before it answers, so
// every request fails.
const answerFrom = async (apps: ReadonlyArray<object>) => {
  const { url } = await startService(apps);
  return async () =>
    (
      await fetch(new URL("/devices", url), {
        headers: { "colibrii-version": "1" },
      })
    ).status;
};

const [apart, sharedOrigin, sharedClient, sharedLocalhost, unsetSecret] =
  await Promise.all([
    answerFrom([
      app("inkwell", "inkwell://app", "inkwell-oauth"),
      app("inkwell-dev", "inkwell-dev://app", "inkwell-dev-oauth"),
    ]),
    answerFrom([
      app("inkwell", "inkwell://app", "inkwell-oauth"),
      app("inkwell-dev", "inkwell://app", "inkwell-dev-oauth"),
    ]),
    answerFrom([
      app("inkwell", "inkwell://app", "inkwell-oauth"),
      {
        ...app("inkwell-dev", "inkwell-dev://app", "inkwell-oauth"),
        clerkPublicKey: `  ${key.trim().replaceAll("\n", "\r\n")}\n\n`,
      },
    ]),
    answerFrom([
      app("inkwell", "http://localhost:5173", "inkwell-oauth"),
      app("inkwell-dev", "http://localhost:*", "inkwell-dev-oauth"),
    ]),
    answerFrom([
      {
        ...app("inkwell", "inkwell://app", "inkwell-oauth"),
        clerkSecretName: "UNSET_CLERK_SECRET_KEY",
      },
    ]),
  ]);

describe("the instance's apps", () => {
  it("may share a Clerk instance with no origin or OAuth client in common", async () => {
    expect(await apart()).toBe(401);
  });

  it("refuse to start when two on one Clerk instance share an origin or an OAuth client", async () => {
    expect(await sharedOrigin()).toBe(500);
    expect(await sharedClient()).toBe(500);
    expect(await sharedLocalhost()).toBe(500);
  });

  it("refuse to start when one names a secret that is not set", async () => {
    expect(await unsetSecret()).toBe(500);
  });
});
