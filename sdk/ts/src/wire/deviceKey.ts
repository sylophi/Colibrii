// A device key is the X25519 key pair a device makes at each enrollment
// (noise.ts's generateKeyPair). The service and the roster carry its
// public half as base64url, 43 characters.
import * as Option from "effect/Option";
import * as Base64Url from "./base64url.ts";

export const decodePublicKey = (text: string): Option.Option<Uint8Array> =>
  Option.filter(Base64Url.decode(text), (key) => key.length === 32);

// Public keys, so no constant-time compare is needed.
export const sameKey = (a: Uint8Array, b: Uint8Array): boolean =>
  a.length === b.length && a.every((byte, i) => byte === b[i]);
