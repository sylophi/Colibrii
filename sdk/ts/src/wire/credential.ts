import * as Schema from "effect/Schema";
import { sha256Hex } from "./sha256.ts";

export const DEVICE_CREDENTIAL_PREFIX = "colibrii_dc_";

// The prefix and base64url of 32 random bytes.
export const DeviceCredential = Schema.String.check(
  Schema.isPattern(/^colibrii_dc_[A-Za-z0-9_-]{43}$/),
);

// What the service stores of a credential: its SHA-256, lowercase hex.
export const credentialHash = (credential: string): Promise<string> =>
  sha256Hex(credential);
