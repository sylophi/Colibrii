import * as Hex from "effect/encoding/Hex";
import * as Schema from "effect/Schema";

export const AppId = Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,32}$/));

// The app's Clerk user id.
export const AccountId = Schema.NonEmptyString;

export const DeviceId = Schema.String.check(
  Schema.isPattern(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
  ),
);

export const ConnectionId = Schema.String.check(
  Schema.isPattern(/^[0-9a-f]{32}$/),
);

export const newConnectionId = (): string =>
  Hex.encode(crypto.getRandomValues(new Uint8Array(16)));

// An X25519 public key, 32 bytes as base64url.
export const PublicKey = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9_-]{43}$/),
);
