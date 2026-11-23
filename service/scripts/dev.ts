// Serves the Worker at http://localhost:8787 from wrangler.jsonc, with
// one app, inkwell, whose Clerk key pair this makes in .local/ on its
// first run. Login tokens signed with .local/clerk.key.pem sign in.
import { execFileSync } from "node:child_process";
import { generateKeyPairSync } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { unstable_startWorker } from "wrangler";

const serviceDir = join(import.meta.dirname, "..");
const local = join(serviceDir, ".local");
const publicKeyFile = join(local, "clerk.pub.pem");

if (!existsSync(publicKeyFile)) {
  const pair = generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });
  mkdirSync(local, { recursive: true });
  writeFileSync(join(local, "clerk.key.pem"), pair.privateKey, {
    mode: 0o600,
  });
  writeFileSync(publicKeyFile, pair.publicKey);
}

execFileSync(
  process.execPath,
  [
    join(serviceDir, "scripts/app.ts"),
    "--local",
    "add",
    "inkwell",
    "--key",
    publicKeyFile,
    "--origin",
    "inkwell://app",
    "--origin",
    "http://localhost:5173",
    "--oauth-client",
    "local-oauth-client",
  ],
  { stdio: "inherit" },
);

const worker = await unstable_startWorker({
  config: join(serviceDir, "wrangler.jsonc"),
  bindings: {
    TICKET_SIGNING_KEY: { type: "secret_text", value: "local-only" },
    CLOUDFLARE_API_TOKEN: { type: "secret_text", value: "local-only" },
  },
  dev: { server: { port: 8787 } },
});
// A handle wrangler keeps open would hold the process after it.
process.on(
  "SIGINT",
  () => void worker.dispose().finally(() => process.exit(130)),
);
