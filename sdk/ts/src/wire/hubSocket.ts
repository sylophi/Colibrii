// The hub socket's messages, limits and close codes.
import * as Schema from "effect/Schema";
import { Connections } from "./device.ts";
import { DeviceId, PublicKey } from "./ids.ts";
import { ReadableArray } from "./readable.ts";

export const PING = "ping";
export const PONG = "pong";

export const MAX_MESSAGE_BYTES = 65_536;
// An app's device cap is at most this, so a presence stays within the
// cap below.
export const MAX_DEVICE_CAP = 1_024;
// A message from the service: a relay is a device's message with `from`
// in place of `to`, and a presence names at most MAX_DEVICE_CAP devices.
export const MAX_SERVICE_MESSAGE_BYTES = 262_144;
export const MESSAGE_BURST = 300;
export const MESSAGES_PER_SECOND = 30;
export const SILENT_AFTER_MS = 90_000;
export const MAX_DEVICE_SOCKETS = 32;

export const CLOSE_TICKET_REFUSED = 4101;
export const CLOSE_DEVICE_REMOVED = 4102;
export const CLOSE_SUPERSEDED = 4103;
export const CLOSE_ENROLLED_AGAIN = 4104;
export const CLOSE_TOO_MANY_MESSAGES = 4105;
export const CLOSE_SILENT = 4106;
export const CLOSE_TOO_BIG = 1009;

// From the device.
export const RelayTo = Schema.Struct({
  t: Schema.Literal("relay"),
  to: DeviceId,
  frame: Schema.String,
});

export const OnlineDevice = Schema.Struct({
  deviceId: DeviceId,
  publicKey: PublicKey,
  connections: Connections,
});
export type OnlineDevice = typeof OnlineDevice.Type;

// From the service. An entry a device cannot read leaves that device
// out of its roster.
export const Presence = Schema.Struct({
  t: Schema.Literal("presence"),
  online: ReadableArray(OnlineDevice),
});

export const RelayFrom = Schema.Struct({
  t: Schema.Literal("relay"),
  from: DeviceId,
  frame: Schema.String,
});

// `offline`. A reason the device does not know ends its asks all the
// same.
export const Nack = Schema.Struct({
  t: Schema.Literal("nack"),
  to: DeviceId,
  reason: Schema.String,
});

export const ServiceMessage = Schema.Union([Presence, RelayFrom, Nack]);
export type ServiceMessage = typeof ServiceMessage.Type;
