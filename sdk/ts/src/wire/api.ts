// The service's HTTP API, declared once for the service and every
// client. Each error crosses the wire as its `_tag` and fields.
import * as Context from "effect/Context";
import type * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiMiddleware from "effect/http-api/HttpApiMiddleware";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import * as HttpApiSecurity from "effect/http-api/HttpApiSecurity";
import { Connections, Device, DeviceData, DeviceName } from "./device.ts";
import { AccountId, AppId, ConnectionId, DeviceId, PublicKey } from "./ids.ts";
import { ReadableArray } from "./readable.ts";

export const VERSION_HEADER = "colibrii-version";
export const WIRE_VERSION = 1;

// ---- Errors ----

export class UpdateRequiredError extends Schema.TaggedError<UpdateRequiredError>()(
  "UpdateRequired",
  { floor: Schema.Int },
  { httpApiStatus: 403 },
) {
  override get message(): string {
    return `The service needs wire version ${this.floor} or later.`;
  }
}

export class LoginRejectedError extends Schema.TaggedError<LoginRejectedError>()(
  "LoginRejected",
  {},
  { httpApiStatus: 401 },
) {
  override get message(): string {
    return "The service did not accept this sign-in.";
  }
}

export class UnknownAppError extends Schema.TaggedError<UnknownAppError>()(
  "UnknownApp",
  { app: Schema.String },
  { httpApiStatus: 404 },
) {
  override get message(): string {
    return `No app is registered as ${this.app}.`;
  }
}

export class CredentialRejectedError extends Schema.TaggedError<CredentialRejectedError>()(
  "CredentialRejected",
  {},
  { httpApiStatus: 401 },
) {
  override get message(): string {
    return "The service does not accept this device credential.";
  }
}

export class DeviceRevokedError extends Schema.TaggedError<DeviceRevokedError>()(
  "DeviceRevoked",
  {},
  { httpApiStatus: 403 },
) {
  override get message(): string {
    return "This device was removed.";
  }
}

export class DeviceEnrolledElsewhereError extends Schema.TaggedError<DeviceEnrolledElsewhereError>()(
  "DeviceEnrolledElsewhere",
  {},
  { httpApiStatus: 409 },
) {
  override get message(): string {
    return "This device is enrolled under another app or account.";
  }
}

export class AccountFullError extends Schema.TaggedError<AccountFullError>()(
  "AccountFull",
  { limit: Schema.Int },
  { httpApiStatus: 409 },
) {
  override get message(): string {
    return `The account has ${this.limit} devices, all online.`;
  }
}

export class UnknownDeviceError extends Schema.TaggedError<UnknownDeviceError>()(
  "UnknownDevice",
  { deviceId: Schema.String },
  { httpApiStatus: 404 },
) {
  override get message(): string {
    return "The account has no such device.";
  }
}

export class TicketMalformedError extends Schema.TaggedError<TicketMalformedError>()(
  "TicketMalformed",
  {},
  { httpApiStatus: 403 },
) {
  override get message(): string {
    return "The connect ticket is malformed.";
  }
}

export class UpgradeRequiredError extends Schema.TaggedError<UpgradeRequiredError>()(
  "UpgradeRequired",
  {},
  { httpApiStatus: 426 },
) {
  override get message(): string {
    return "The connect route takes a websocket upgrade.";
  }
}

export class SignInTokensOffError extends Schema.TaggedError<SignInTokensOffError>()(
  "SignInTokensOff",
  {},
  { httpApiStatus: 403 },
) {
  override get message(): string {
    return "The service mints no sign-in tokens for this app.";
  }
}

export class UnavailableError extends Schema.TaggedError<UnavailableError>()(
  "Unavailable",
  {
    operation: Schema.Literals([
      "enroll",
      "credential",
      "list",
      "revoke",
      "update",
      "ticket",
      "tunnel",
      "sign-in-token",
    ]),
  },
  { httpApiStatus: 502 },
) {
  override get message(): string {
    return `The service could not complete the ${this.operation} request.`;
  }
}

// ---- Middleware ----

export class VersionFloor extends HttpApiMiddleware.Service<VersionFloor>()(
  "colibrii/wire/VersionFloor",
  { error: UpdateRequiredError },
) {}

// The bearer as sent, checked by the route against the app it names.
export class LoginToken extends Context.Service<
  LoginToken,
  Redacted.Redacted<string>
>()("colibrii/wire/LoginToken") {}

export class LoginAuth extends HttpApiMiddleware.Service<
  LoginAuth,
  { provides: LoginToken }
>()("colibrii/wire/LoginAuth", {
  security: { bearer: HttpApiSecurity.bearer },
}) {}

// The device a credential belongs to, and the credential's hash, which
// the tickets it mints are bound to.
export class Caller extends Context.Service<
  Caller,
  {
    readonly appId: string;
    readonly accountId: string;
    readonly deviceId: string;
    readonly credentialHash: string;
  }
>()("colibrii/wire/Caller") {}

export class DeviceAuth extends HttpApiMiddleware.Service<
  DeviceAuth,
  { provides: Caller }
>()("colibrii/wire/DeviceAuth", {
  security: { bearer: HttpApiSecurity.bearer },
  error: [CredentialRejectedError, DeviceRevokedError, UnavailableError],
}) {}

// ---- Bodies ----

export const EnrollRequest = Schema.Struct({
  app: AppId,
  deviceId: DeviceId,
  name: DeviceName,
  publicKey: PublicKey,
  connections: Connections,
  data: Schema.optionalKey(DeviceData),
});

export const EnrollResponse = Schema.Struct({
  credential: Schema.String,
  accountId: AccountId,
  device: Device,
});

export const SignInTokenRequest = Schema.Struct({ app: AppId });

export const SignInTokenResponse = Schema.Struct({ token: Schema.String });

// A device leaves out an entry it cannot read.
export const DeviceList = Schema.Struct({ devices: ReadableArray(Device) });

export const DevicePatch = Schema.Struct({
  name: Schema.optionalKey(DeviceName),
  data: Schema.optionalKey(DeviceData),
}).check(
  Schema.makeFilter(
    (patch) =>
      patch.name !== undefined ||
      patch.data !== undefined ||
      "name, data, or both",
  ),
);

export const TicketRequest = Schema.Struct({ connectionId: ConnectionId });

export const TicketResponse = Schema.Struct({ ticket: Schema.String });

export const TunnelRequest = Schema.Struct({
  port: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_535 })),
});

export const TunnelResponse = Schema.Struct({
  hostname: Schema.String,
  connectorToken: Schema.String,
  fresh: Schema.Boolean,
});

// ---- Routes ----

const login = HttpApiGroup.make("login", { topLevel: true })
  .add(
    HttpApiEndpoint.post("enroll", "/devices/enroll", {
      payload: EnrollRequest,
      success: EnrollResponse,
      error: [
        LoginRejectedError,
        UnknownAppError,
        DeviceEnrolledElsewhereError,
        AccountFullError,
        UnavailableError,
      ],
    }),
    HttpApiEndpoint.post("signInToken", "/sign-in-token", {
      payload: SignInTokenRequest,
      success: SignInTokenResponse,
      error: [
        LoginRejectedError,
        UnknownAppError,
        SignInTokensOffError,
        UnavailableError,
      ],
    }),
  )
  .middleware(LoginAuth)
  .middleware(VersionFloor);

const devices = HttpApiGroup.make("devices", { topLevel: true })
  .add(
    HttpApiEndpoint.get("listDevices", "/devices", {
      success: DeviceList,
      error: UnavailableError,
    }),
    HttpApiEndpoint.patch("updateDevice", "/devices/:deviceId", {
      params: { deviceId: DeviceId },
      payload: DevicePatch,
      error: [UnknownDeviceError, UnavailableError],
    }),
    HttpApiEndpoint.delete("removeDevice", "/devices/:deviceId", {
      params: { deviceId: DeviceId },
      // The credential is checked again as the removal runs.
      error: [
        UnknownDeviceError,
        UnavailableError,
        CredentialRejectedError,
        DeviceRevokedError,
      ],
    }),
    HttpApiEndpoint.post("mintTicket", "/tickets", {
      payload: TicketRequest,
      success: TicketResponse,
      error: UnavailableError,
    }),
    HttpApiEndpoint.post("provisionTunnel", "/tunnel", {
      payload: TunnelRequest,
      success: TunnelResponse,
      error: UnavailableError,
    }),
  )
  .middleware(DeviceAuth)
  .middleware(VersionFloor);

// A socket refused past the upgrade is closed with a code instead
// (hubSocket.ts).
const socket = HttpApiGroup.make("socket", { topLevel: true }).add(
  HttpApiEndpoint.get("connect", "/connect", {
    query: { ticket: Schema.optional(Schema.String) },
    success: HttpApiSchema.Empty(101),
    error: [TicketMalformedError, UpgradeRequiredError],
  }),
);

export class Api extends HttpApi.make("colibrii")
  .add(login)
  .add(devices)
  .add(socket) {}
