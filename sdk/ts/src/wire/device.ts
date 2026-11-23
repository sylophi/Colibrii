import * as Schema from "effect/Schema";
import { DeviceId } from "./ids.ts";

const utf8Length = (text: string): number =>
  new TextEncoder().encode(text).byteLength;

export const MAX_DEVICE_DATA_BYTES = 1024;

export const DeviceName = Schema.String.check(
  Schema.makeFilter(
    (name: string) =>
      (name.length > 0 && utf8Length(name) <= 256) || "1 to 256 bytes of UTF-8",
  ),
);

// Measured as the service stores it, encoded again compactly.
export const DeviceData = Schema.JsonObject.check(
  Schema.makeFilter(
    (data) =>
      utf8Length(JSON.stringify(data)) <= MAX_DEVICE_DATA_BYTES ||
      `at most ${MAX_DEVICE_DATA_BYTES} bytes of JSON`,
  ),
);

export const Connections = Schema.Literals(["single", "multi"]);
export type Connections = typeof Connections.Type;

export const Device = Schema.Struct({
  deviceId: DeviceId,
  name: DeviceName,
  data: DeviceData,
  connections: Connections,
  createdAt: Schema.Int,
  lastSeenAt: Schema.NullOr(Schema.Int),
  online: Schema.Boolean,
});
export type Device = typeof Device.Type;
