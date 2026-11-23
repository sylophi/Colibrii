// Base64url (RFC 4648, section 5). Encoding never pads. Decoding takes
// the input with or without its padding, and nothing outside the URL-safe
// alphabet.
import * as Base64Url from "effect/encoding/Base64Url";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";

export const encode = (bytes: Uint8Array): string => Base64Url.encode(bytes);

export const random = (byteLength: number): string =>
  encode(crypto.getRandomValues(new Uint8Array(byteLength)));

const SHAPE =
  /^(?:[A-Za-z0-9_-]{4})*(?:[A-Za-z0-9_-]{2}(?:==)?|[A-Za-z0-9_-]{3}=?)?$/;

export const decode = (text: string): Option.Option<Uint8Array> =>
  SHAPE.test(text) ? Result.getSuccess(Base64Url.decode(text)) : Option.none();

// Bytes carried in JSON as base64url.
export const Bytes = Schema.String.check(Schema.isPattern(SHAPE)).pipe(
  Schema.decodeTo(Schema.Uint8Array, {
    decode: SchemaGetter.decodeBase64Url(),
    encode: SchemaGetter.encodeBase64Url(),
  }),
);
