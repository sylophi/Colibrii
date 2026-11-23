// Adds, lists and removes the instance's apps, in its database through
// wrangler with instance.jsonc, as the Cloudflare account wrangler is
// signed in to. With --local, in the database `pnpm dev` serves instead.
//
//   app add <id> --key <PEM file> [--origin <origin>]...
//       [--oauth-client <id>] [--secret-name <name>] [--device-cap <n>]
//   app list
//   app remove <id>
//
// add replaces the app already under that id.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import {
  clash,
  decodeApp,
  decodeAppRow,
  LIST_APPS_SQL,
  putAppSql,
  removeAppSql,
  type App,
} from "../src/apps.ts";

const serviceDir = join(import.meta.dirname, "..");

function fail(message: string): never {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

const parse = () => {
  try {
    return parseArgs({
      allowPositionals: true,
      options: {
        local: { type: "boolean", default: false },
        key: { type: "string" },
        origin: { type: "string", multiple: true, default: [] },
        "oauth-client": { type: "string" },
        "secret-name": { type: "string" },
        "device-cap": { type: "string" },
      },
    });
  } catch (error) {
    return fail(error instanceof Error ? error.message : String(error));
  }
};
const { values, positionals } = parse();

const execute = (sql: string) => {
  const target = values.local
    ? ["--local", "-c", "wrangler.jsonc"]
    : ["--remote", "-c", "instance.jsonc"];
  let output: string;
  try {
    output = execFileSync(
      join(serviceDir, "node_modules/.bin/wrangler"),
      ["d1", "execute", "DB", ...target, "--json", "--command", sql],
      { cwd: serviceDir, encoding: "utf8", stdio: ["inherit", "pipe", "pipe"] },
    );
  } catch (error) {
    const { stdout, stderr } = error as { stdout?: string; stderr?: string };
    return fail(
      `wrangler failed:\n${stdout ?? ""}${stderr ?? String(error)}`.trimEnd(),
    );
  }
  const [result] = JSON.parse(output) as Array<{
    readonly results: ReadonlyArray<unknown>;
  }>;
  if (result === undefined) return fail("wrangler answered nothing");
  return result;
};

const apps = (): ReadonlyArray<App> =>
  execute(LIST_APPS_SQL).results.map(decodeAppRow);

const show = (app: App) =>
  [
    app.id,
    `  origins: ${app.origins.join(" ") || "none"}`,
    `  OAuth client id: ${app.oauthClientId ?? "none"}`,
    `  Clerk secret name: ${app.clerkSecretName ?? "none"}`,
    `  device cap: ${app.deviceCap}`,
  ].join("\n");

const [command, id, ...rest] = positionals;
if (rest.length > 0) fail(`unexpected: ${rest.join(" ")}`);

if (command === "add" && id !== undefined) {
  if (values.key === undefined) fail("add needs --key <PEM file>");
  const cap = values["device-cap"];
  let app: App;
  try {
    app = decodeApp({
      id,
      // pnpm runs this in service/, and names where it was run from.
      clerkPublicKey: readFileSync(
        resolve(process.env["INIT_CWD"] ?? process.cwd(), values.key),
        "utf8",
      ),
      origins: values.origin,
      ...(values["oauth-client"] === undefined
        ? {}
        : { oauthClientId: values["oauth-client"] }),
      ...(values["secret-name"] === undefined
        ? {}
        : { clerkSecretName: values["secret-name"] }),
      ...(cap === undefined ? {} : { deviceCap: Number(cap) }),
    });
  } catch (error) {
    fail(error instanceof Error ? error.message : String(error));
  }
  const clashing = clash(app, apps());
  if (clashing !== undefined) fail(clashing);
  execute(putAppSql(app));
  process.stdout.write(`${show(app)}\n`);
} else if (command === "list" && id === undefined) {
  const all = apps();
  process.stdout.write(
    all.length === 0 ? "no apps\n" : `${all.map(show).join("\n")}\n`,
  );
} else if (command === "remove" && id !== undefined) {
  if (execute(removeAppSql(id)).results.length === 0) fail(`no app ${id}`);
  process.stdout.write(`removed ${id}\n`);
} else {
  fail(
    "usage: app add <id> --key <PEM file> [--origin <origin>]... [--oauth-client <id>] [--secret-name <name>] [--device-cap <n>]\n" +
      "       app list\n" +
      "       app remove <id>\n" +
      "With --local, in the database `pnpm dev` serves.",
  );
}
