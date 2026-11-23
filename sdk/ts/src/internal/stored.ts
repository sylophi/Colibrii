// What a device keeps between runs (spec/service.md, "What a device
// keeps"), as the one document the credential store holds.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { CredentialStore } from "../Platform.ts";
import * as Base64Url from "../wire/base64url.ts";
import { AccountId, DeviceId } from "../wire/ids.ts";

export const Enrollment = Schema.Struct({
  deviceId: DeviceId,
  accountId: AccountId,
  credential: Schema.String,
  privateKey: Base64Url.Bytes,
  // The port the listener last listened on, so a peer's LAN address for
  // it and the tunnel's ingress stay right across runs.
  port: Schema.optionalKey(Schema.Int),
});
export type Enrollment = typeof Enrollment.Type;

// Signed out before the service heard of it: the device removes itself
// with the credential once it can.
export const Owed = Schema.Struct({
  deviceId: DeviceId,
  credential: Schema.String,
  signedOut: Schema.Literal(true),
});
export type Owed = typeof Owed.Type;

export const Stored = Schema.Union([Enrollment, Owed]);
export type Stored = typeof Stored.Type;

export const isOwed = (stored: Stored): stored is Owed => "signedOut" in stored;

export const enrollmentOf = (stored: Stored | null) =>
  stored === null || isOwed(stored) ? null : stored;

const StoredJson = Schema.fromJsonString(Stored);

// A document that does not decode reads as none: the device enrolls
// again. So does one of another device than `deviceId`, when given.
export const read = (deviceId: string | undefined) =>
  CredentialStore.use((store) => store.read).pipe(
    Effect.map(
      Option.flatMap((text) =>
        Schema.decodeOption(StoredJson)(text).pipe(
          Option.filter(
            (stored) => deviceId === undefined || stored.deviceId === deviceId,
          ),
        ),
      ),
    ),
  );

export const write = (stored: Stored) =>
  CredentialStore.use((store) =>
    store.write(Schema.encodeSync(StoredJson)(stored)),
  );

export const remove = CredentialStore.use((store) => store.remove);
