// What the instance's owner set up (spec/service.md, "What an instance
// is set up with"), read from the Worker's vars and secrets once as it
// starts. A missing or malformed value stops the Worker there.
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import { MAX_DEVICE_CAP } from "@colibrii/sdk/wire/hubSocket";
import { AppId } from "@colibrii/sdk/wire/ids";
import type { Env } from "./env.ts";

const DEFAULT_DEVICE_CAP = 16;

const AppConfig = Schema.Struct({
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
const loginsApart = (
  a: typeof AppConfig.Type,
  b: typeof AppConfig.Type,
): boolean =>
  keyOf(a.clerkPublicKey) !== keyOf(b.clerkPublicKey) ||
  (!a.origins.some((origin) => originAllowed(b.origins, origin)) &&
    !b.origins.some((origin) => originAllowed(a.origins, origin)) &&
    (a.oauthClientId === undefined || a.oauthClientId !== b.oauthClientId));

const InstanceConfig = Schema.Struct({
  APPS: Schema.Array(AppConfig).check(
    Schema.makeFilter(
      (apps) =>
        new Set(apps.map((app) => app.id)).size === apps.length ||
        "app ids must be unique",
    ),
    Schema.makeFilter(
      (apps) =>
        apps.every((app, index) =>
          apps.slice(index + 1).every((other) => loginsApart(app, other)),
        ) ||
        "apps that share a Clerk public key must share no origin and no OAuth client id",
    ),
  ),
  TUNNEL_DOMAIN: Schema.NonEmptyString,
  TUNNEL_ZONE_ID: Schema.NonEmptyString,
  CLOUDFLARE_ACCOUNT_ID: Schema.NonEmptyString,
  CLOUDFLARE_API_TOKEN: Schema.NonEmptyString,
  TICKET_SIGNING_KEY: Schema.NonEmptyString,
});

export interface App {
  readonly id: string;
  readonly clerkPublicKey: string;
  readonly origins: ReadonlyArray<string>;
  readonly oauthClientId: string | undefined;
  readonly clerkSecretKey: string | undefined;
  readonly deviceCap: number;
}

export interface TunnelConfig {
  readonly domain: string;
  readonly zoneId: string;
  readonly accountId: string;
  readonly apiToken: string;
}

export class Instance extends Context.Service<
  Instance,
  {
    readonly apps: ReadonlyMap<string, App>;
    readonly tunnel: TunnelConfig;
    readonly ticketSigningKey: string;
  }
>()("colibrii/service/Instance") {}

export const readInstance = (env: Env): Instance["Service"] => {
  const config = Schema.decodeUnknownSync(InstanceConfig)(env, {
    errors: "all",
  });
  const secret = (app: typeof AppConfig.Type) => {
    const name = app.clerkSecretName;
    if (name === undefined) return undefined;
    const value = env[name];
    if (typeof value !== "string" || value === "") {
      throw new Error(`the secret ${name} that app ${app.id} names is not set`);
    }
    return value;
  };
  return {
    apps: new Map(
      config.APPS.map((app) => [
        app.id,
        {
          id: app.id,
          clerkPublicKey: app.clerkPublicKey,
          origins: app.origins,
          oauthClientId: app.oauthClientId,
          clerkSecretKey: secret(app),
          deviceCap: app.deviceCap ?? DEFAULT_DEVICE_CAP,
        },
      ]),
    ),
    tunnel: {
      domain: config.TUNNEL_DOMAIN,
      zoneId: config.TUNNEL_ZONE_ID,
      accountId: config.CLOUDFLARE_ACCOUNT_ID,
      apiToken: config.CLOUDFLARE_API_TOKEN,
    },
    ticketSigningKey: config.TICKET_SIGNING_KEY,
  };
};
