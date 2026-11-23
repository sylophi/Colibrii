// The instance's apps (spec/service.md, "What an instance is set up
// with"), kept in D1 (migrations/0002_apps.sql). scripts/app.ts checks
// an app against these rules before it writes one, so the service reads
// only apps that keep them.
import * as Schema from "effect/Schema";
import { MAX_DEVICE_CAP } from "@colibrii/sdk/wire/hubSocket";
import { AppId } from "@colibrii/sdk/wire/ids";

const DEFAULT_DEVICE_CAP = 16;

// An app as the owner gives it.
export const AppConfig = Schema.Struct({
  id: AppId,
  // The Clerk instance's PEM public key, which checks login tokens.
  clerkPublicKey: Schema.String.check(
    Schema.isPattern(/-----BEGIN PUBLIC KEY-----/),
  ),
  origins: Schema.Array(Schema.NonEmptyString),
  oauthClientId: Schema.optionalKey(Schema.NonEmptyString),
  // The name of the Worker secret holding the Clerk secret key.
  clerkSecretName: Schema.optionalKey(Schema.NonEmptyString),
  deviceCap: Schema.optionalKey(
    Schema.Int.check(
      Schema.isGreaterThanOrEqualTo(1),
      Schema.isLessThanOrEqualTo(MAX_DEVICE_CAP),
    ),
  ),
});

export interface App {
  readonly id: string;
  readonly clerkPublicKey: string;
  readonly origins: ReadonlyArray<string>;
  readonly oauthClientId: string | undefined;
  readonly clerkSecretName: string | undefined;
  readonly deviceCap: number;
}

export const decodeApp = (input: unknown): App => {
  const app = Schema.decodeUnknownSync(AppConfig)(input, { errors: "all" });
  return {
    id: app.id,
    clerkPublicKey: app.clerkPublicKey,
    origins: app.origins,
    oauthClientId: app.oauthClientId,
    clerkSecretName: app.clerkSecretName,
    deviceCap: app.deviceCap ?? DEFAULT_DEVICE_CAP,
  };
};

// Stands for every port of http://localhost, for a development web
// client whose port differs per checkout.
const ANY_LOCALHOST_PORT = "http://localhost:*";

export const originAllowed = (
  origins: ReadonlyArray<string>,
  origin: string,
): boolean =>
  origins.includes(origin) ||
  (origins.includes(ANY_LOCALHOST_PORT) &&
    /^http:\/\/localhost:\d{1,5}$/.test(origin));

// A PEM key as Clerk imports it: its base64 alone.
const keyOf = (pem: string) =>
  pem.replace(/-----BEGIN.*?-----|-----END.*?-----|\s/g, "");

// A login token names its app by its origin or OAuth client alone, so
// two apps on one Clerk instance must share neither.
const loginsApart = (a: App, b: App): boolean =>
  keyOf(a.clerkPublicKey) !== keyOf(b.clerkPublicKey) ||
  (!a.origins.some((origin) => originAllowed(b.origins, origin)) &&
    !b.origins.some((origin) => originAllowed(a.origins, origin)) &&
    (a.oauthClientId === undefined || a.oauthClientId !== b.oauthClientId));

// Why `app` cannot join `apps`, which it replaces any of under its id,
// or undefined when it can.
export const clash = (
  app: App,
  apps: ReadonlyArray<App>,
): string | undefined => {
  const other = apps.find(
    (each) => each.id !== app.id && !loginsApart(app, each),
  );
  return other === undefined
    ? undefined
    : `${app.id} and ${other.id} share a Clerk public key, so they must share no origin and no OAuth client id`;
};

// ---- Rows ----

export const APP_COLUMNS =
  "id, clerk_public_key, origins, oauth_client_id, clerk_secret_name, device_cap";

const AppRow = Schema.Struct({
  id: Schema.String,
  clerk_public_key: Schema.String,
  origins: Schema.fromJsonString(Schema.Array(Schema.String)),
  oauth_client_id: Schema.NullOr(Schema.String),
  clerk_secret_name: Schema.NullOr(Schema.String),
  device_cap: Schema.Int,
});

export const decodeAppRow = (row: unknown): App => {
  const decoded = Schema.decodeUnknownSync(AppRow)(row);
  return {
    id: decoded.id,
    clerkPublicKey: decoded.clerk_public_key,
    origins: decoded.origins,
    oauthClientId: decoded.oauth_client_id ?? undefined,
    clerkSecretName: decoded.clerk_secret_name ?? undefined,
    deviceCap: decoded.device_cap,
  };
};

// The statements scripts/app.ts has wrangler run, which takes SQL text
// and no parameters. Each text value goes in as a hex literal of its
// UTF-8, so all a value adds to the statement is hex digits.
const text = (value: string | undefined) =>
  value === undefined
    ? "NULL"
    : `CAST(X'${Array.from(new TextEncoder().encode(value), (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join("")}' AS TEXT)`;

// Adds the app, or replaces the one under its id.
export const putAppSql = (app: App): string =>
  `INSERT OR REPLACE INTO apps (${APP_COLUMNS}) VALUES (${[
    text(app.id),
    text(app.clerkPublicKey),
    text(JSON.stringify(app.origins)),
    text(app.oauthClientId),
    text(app.clerkSecretName),
    String(app.deviceCap),
  ].join(", ")})`;

// Answers the id it removed, if any.
export const removeAppSql = (id: string): string =>
  `DELETE FROM apps WHERE id = ${text(id)} RETURNING id`;

export const LIST_APPS_SQL = `SELECT ${APP_COLUMNS} FROM apps ORDER BY id`;
