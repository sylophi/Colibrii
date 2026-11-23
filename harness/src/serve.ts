// The service for tests outside vitest, such as the Rust SDK's: run with
// node, it prints the hub's address as a JSON line, then answers each
// JSON line on stdin with a login token as one, and stops once stdin
// ends, as it does when the process that started it exits.
import { createInterface } from "node:readline";
import { oauthToken, serve, sessionToken } from "./index.ts";

interface Ask {
  readonly account: string;
  // A Clerk session token unless "oauth".
  readonly kind?: "session" | "oauth";
  // The session token's origin, or the access token's OAuth client.
  readonly from?: string;
  // Signed with the key of quill's Clerk instance instead.
  readonly otherKey?: boolean;
}

const service = await serve();
process.stdout.write(`${JSON.stringify({ url: service.url })}\n`);
for await (const line of createInterface({ input: process.stdin })) {
  const ask = JSON.parse(line) as Ask;
  const options = ask.otherKey === true ? { key: "b" as const } : {};
  const token =
    ask.kind === "oauth"
      ? await oauthToken(ask.account, ask.from, options)
      : await sessionToken(ask.account, ask.from, options);
  process.stdout.write(`${JSON.stringify({ token })}\n`);
}
await service.close();
