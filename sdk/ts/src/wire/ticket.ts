import * as Schema from "effect/Schema";

export const CONNECT_TICKET_PREFIX = "colibrii_ct_";

// A ticket opens one hub socket, once, within this long of being minted.
export const CONNECT_TICKET_TTL_MS = 60_000;

// The prefix, then three parts joined by `.`: the route
// (`<app id>/<account id>`, at most 128 characters encoded), 16 random
// bytes, and an HMAC-SHA256 over the first two. All base64url. A device
// treats it as opaque.
export const ConnectTicket = Schema.String.check(
  Schema.isPattern(
    /^colibrii_ct_[A-Za-z0-9_-]{1,128}\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}$/,
  ),
);
