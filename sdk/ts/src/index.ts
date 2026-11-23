// @colibrii/sdk: a device of an app, on any runtime a binding serves
// (`@colibrii/sdk/node`).
export {
  type AccountState,
  type Device,
  type DeviceOptions,
  type DeviceRecord,
  type IncomingHello,
  type ListenOptions,
  make,
  type Peer,
} from "./Device.ts";
export * from "./Errors.ts";
export type { Link, LinkClosed, LinkCloseCause } from "./Link.ts";
export * from "./Platform.ts";
export {
  type CodeSignInOptions,
  finishCodeSignIn,
  type SignInCode,
  signInToken,
  startCodeSignIn,
} from "./SignIn.ts";
export {
  AccountFullError,
  CredentialRejectedError,
  DeviceEnrolledElsewhereError,
  DeviceRevokedError,
  LoginRejectedError,
  SignInTokensOffError,
  UnavailableError,
  UnknownAppError,
  UnknownDeviceError,
  UpdateRequiredError,
} from "./wire/api.ts";
export type { CandidateKind } from "./wire/relay.ts";
