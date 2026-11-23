// What the instance's owner set up in the Worker's vars and secrets
// (spec/service.md, "What an instance is set up with"), read once as it
// starts. A missing or malformed value stops the Worker there. Its apps
// are in the database instead (apps.ts).
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import type { Env } from "./env.ts";

const InstanceConfig = Schema.Struct({
  TUNNEL_DOMAIN: Schema.NonEmptyString,
  TUNNEL_ZONE_ID: Schema.NonEmptyString,
  CLOUDFLARE_ACCOUNT_ID: Schema.NonEmptyString,
  CLOUDFLARE_API_TOKEN: Schema.NonEmptyString,
  TICKET_SIGNING_KEY: Schema.NonEmptyString,
});

export interface TunnelConfig {
  readonly domain: string;
  readonly zoneId: string;
  readonly accountId: string;
  readonly apiToken: string;
}

export class Instance extends Context.Service<
  Instance,
  {
    readonly tunnel: TunnelConfig;
    readonly ticketSigningKey: string;
  }
>()("colibrii/service/Instance") {}

export const readInstance = (env: Env): Instance["Service"] => {
  const config = Schema.decodeUnknownSync(InstanceConfig)(env, {
    errors: "all",
  });
  return {
    tunnel: {
      domain: config.TUNNEL_DOMAIN,
      zoneId: config.TUNNEL_ZONE_ID,
      accountId: config.CLOUDFLARE_ACCOUNT_ID,
      apiToken: config.CLOUDFLARE_API_TOKEN,
    },
    ticketSigningKey: config.TICKET_SIGNING_KEY,
  };
};
