// What the instance's owner set up (spec/service.md, "What an instance
// is set up with"), read from the Worker's vars and secrets once as it
// starts. A missing or malformed value stops the Worker there.
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import { AppConfig, clash, decodeApp } from "./apps.ts";
import type { Env } from "./env.ts";

const InstanceConfig = Schema.Struct({
  APPS: Schema.Array(AppConfig).check(
    Schema.makeFilter(
      (apps) =>
        new Set(apps.map((app) => app.id)).size === apps.length ||
        "app ids must be unique",
    ),
    Schema.makeFilter(
      (apps) =>
        apps
          .map(decodeApp)
          .every(
            (app, index, all) => clash(app, all.slice(index + 1)) === undefined,
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
          deviceCap: decodeApp(app).deviceCap,
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
