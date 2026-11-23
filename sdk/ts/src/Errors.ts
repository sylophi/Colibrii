// The SDK's own errors. The service's errors are the wire's
// (wire/api.ts), and cross into the SDK as they are.
import * as Schema from "effect/Schema";

export class HubUnreachableError extends Schema.TaggedError<HubUnreachableError>()(
  "HubUnreachable",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "The hub could not be reached, or answered something unreadable.";
  }
}

export class RateLimitedError extends Schema.TaggedError<RateLimitedError>()(
  "RateLimited",
  { retryAfterSeconds: Schema.Finite },
) {
  override get message(): string {
    return `The hub asks to wait ${this.retryAfterSeconds} seconds.`;
  }
}

export class InvalidInputError extends Schema.TaggedError<InvalidInputError>()(
  "InvalidInput",
  {
    field: Schema.Literals(["name", "data", "deviceId", "appHello", "message"]),
  },
) {
  override get message(): string {
    switch (this.field) {
      case "name":
        return "A device name is 1 to 256 bytes of UTF-8.";
      case "data":
        return "Device data is a JSON object of at most 1024 bytes.";
      case "deviceId":
        return "A device id is a UUID in lowercase canonical form.";
      case "appHello":
        return "An app hello is at most 16 KiB.";
      case "message":
        return "An app message is at most 32 MiB.";
    }
  }
}

export class NotEnrolledError extends Schema.TaggedError<NotEnrolledError>()(
  "NotEnrolled",
  {},
) {
  override get message(): string {
    return "This device is not enrolled.";
  }
}

// A dial that reached no link and was refused by no one.
export class DialError extends Schema.TaggedError<DialError>()("DialError", {
  deviceId: Schema.String,
  reason: Schema.Literals([
    // This device's hub socket is down, or it is not enrolled.
    "not-connected",
    // The peer is not in the roster, or left it or changed its key while
    // dialed.
    "offline",
    // The peer never listens, such as a browser.
    "no-listener",
    // The peer listens, but offers nothing this device can dial now.
    "unavailable",
    // No answer to the ask, or no welcome, in time.
    "timed-out",
    // Every candidate failed before its welcome. The cause holds why.
    "unreachable",
  ]),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Could not dial ${this.deviceId}: ${this.reason}.`;
  }
}

// The listener's `refuse`: by Colibrii's checks, or by the listening app
// with its own bytes.
export class LinkRefusedError extends Schema.TaggedError<LinkRefusedError>()(
  "LinkRefused",
  {
    deviceId: Schema.String,
    by: Schema.Literals(["colibrii", "app"]),
    reason: Schema.String,
    appRefusal: Schema.optional(Schema.Uint8Array),
  },
) {
  override get message(): string {
    return `${this.deviceId} refused the link (${this.by}): ${this.reason}`;
  }
}

// What a listening app's `admit` fails with to refuse a link. The dialer
// reads it as a LinkRefusedError by the app.
export class AppRefusal extends Schema.TaggedError<AppRefusal>()("AppRefusal", {
  reason: Schema.String,
  appRefusal: Schema.optional(Schema.Uint8Array),
}) {}

export class LinkClosedError extends Schema.TaggedError<LinkClosedError>()(
  "LinkClosed",
  {},
) {
  override get message(): string {
    return "The link is closed.";
  }
}

export class CodeSignInError extends Schema.TaggedError<CodeSignInError>()(
  "CodeSignInError",
  {
    reason: Schema.Literals([
      // The Clerk instance has the device grant off.
      "off",
      "denied",
      "expired",
      // Clerk refused, in `words`.
      "refused",
      "unreachable",
    ]),
    words: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "off":
        return "Signing in with a code is off for this app.";
      case "denied":
        return "The sign-in was denied.";
      case "expired":
        return "The code expired before it was approved.";
      case "refused":
        return `Clerk refused the sign-in: ${this.words ?? "no reason given"}`;
      case "unreachable":
        return "Clerk could not be reached.";
    }
  }
}
