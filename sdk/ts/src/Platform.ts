// What the SDK needs from the runtime it runs in, each a small service a
// binding provides (`@colibrii/sdk/node` for Node). The rest of the SDK
// is written against these alone.
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { TunnelResponse } from "./wire/api.ts";
import type { CandidateKind } from "./wire/relay.ts";

// ---- Websockets ----

export interface SocketClose {
  readonly code: number;
  readonly reason: string;
}

// One open websocket. `send` and `close` act at once, so a message is
// sealed and sent with nothing between, and both do nothing once the
// socket is closing.
export interface Conn {
  readonly send: (message: string | Uint8Array) => void;
  readonly close: (code: number, reason: string) => void;
  // False once the socket is closing, by either end.
  readonly isOpen: () => boolean;
  // The next message, in order. Once the socket closed and every message
  // before its close was taken, it fails with the close.
  readonly receive: Effect.Effect<string | Uint8Array, SocketClose>;
  readonly closed: Effect.Effect<SocketClose>;
}

export class WebSocketOpenError extends Schema.TaggedError<WebSocketOpenError>()(
  "WebSocketOpenError",
  {
    // The HTTP status the upgrade was answered with instead, when the
    // platform shows it.
    status: Schema.optional(Schema.Int),
    retryAfterSeconds: Schema.optional(Schema.Finite),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.status === undefined
      ? "The websocket did not open."
      : `The websocket upgrade was answered ${this.status}.`;
  }
}

export class WebSockets extends Context.Service<
  WebSockets,
  {
    // The kinds of candidate this platform can dial: a page served over
    // HTTPS cannot open a `ws://` socket.
    readonly dialableKinds: ReadonlyArray<CandidateKind>;
    // A websocket to `url`, once open, with no subprotocol and no
    // compression. Closing the scope closes it.
    readonly open: (
      url: string,
    ) => Effect.Effect<Conn, WebSocketOpenError, Scope.Scope>;
  }
>()("colibrii/WebSockets") {}

// ---- Listening ----

export interface Arrival {
  readonly conn: Conn;
  readonly remoteAddress: string;
  // The `CF-Connecting-IP` header, which the tunnel sets.
  readonly connectingIp: string | undefined;
}

export interface NetworkAddress {
  readonly address: string;
  readonly family: "IPv4" | "IPv6";
  readonly internal: boolean;
}

export class ListenError extends Schema.TaggedError<ListenError>()(
  "ListenError",
  { inUse: Schema.Boolean, cause: Schema.Defect() },
) {
  override get message(): string {
    return this.inUse
      ? "The listener's port is taken."
      : "The listener could not start.";
  }
}

export class ListenerSockets extends Context.Service<
  ListenerSockets,
  {
    // Listens on every interface at `port`, or any port for 0, and
    // answers the port. An upgrade whose `Origin` `allowOrigin` refuses
    // is turned away. Each socket is served by `serve`, and closed when
    // it returns. Closing the scope closes them all.
    readonly listen: (options: {
      readonly port: number;
      readonly allowOrigin: (origin: string | undefined) => boolean;
      readonly serve: (arrival: Arrival) => Effect.Effect<void>;
    }) => Effect.Effect<number, ListenError, Scope.Scope>;
    readonly addresses: Effect.Effect<ReadonlyArray<NetworkAddress>>;
  }
>()("colibrii/ListenerSockets") {}

// The listening device's tunnel (spec/tunnels.md), run apart from the
// SDK's core. A listener without one offers only LAN candidates.
export type TunnelState =
  | { readonly status: "off" | "no-binary" | "starting" | "error" }
  | { readonly status: "up"; readonly hostname: string };

export type TunnelProvision = typeof TunnelResponse.Type;

export interface RunningTunnel {
  // The state now, then every change.
  readonly state: Stream.Stream<TunnelState>;
  // Asks a tunnel that is up whether it still routes.
  readonly check: Effect.Effect<void>;
}

export class Tunnel extends Context.Service<
  Tunnel,
  {
    // Runs the tunnel to the listener at `port` until the scope closes.
    // `provision` is `POST /tunnel` for the port.
    readonly run: (options: {
      readonly port: number;
      readonly provision: Effect.Effect<TunnelProvision, unknown>;
    }) => Effect.Effect<RunningTunnel, never, Scope.Scope>;
  }
>()("colibrii/Tunnel") {}

// ---- Keeping the credential ----

export class CredentialStoreError extends Schema.TaggedError<CredentialStoreError>()(
  "CredentialStoreError",
  {
    operation: Schema.Literals(["read", "write", "remove"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Could not ${this.operation} the device's credential.`;
  }
}

// Where the device keeps its id, credential and private key between
// runs, as one text document. Only the person's own user account should
// be able to read it.
export class CredentialStore extends Context.Service<
  CredentialStore,
  {
    readonly read: Effect.Effect<Option.Option<string>, CredentialStoreError>;
    readonly write: (text: string) => Effect.Effect<void, CredentialStoreError>;
    readonly remove: Effect.Effect<void, CredentialStoreError>;
    // For a document several runs of the device hold at once, such as
    // the tabs of a browser profile: each time another of them changed
    // it, and a lock under which one run at a time enrolls or signs out.
    readonly shared?: {
      readonly changes: Stream.Stream<void>;
      readonly exclusive: <A, E, R>(
        effect: Effect.Effect<A, E, R>,
      ) => Effect.Effect<A, E, R>;
    };
  }
>()("colibrii/CredentialStore") {}

// ---- Waking ----

// Each time the machine wakes or the network goes or comes back, as far
// as the platform can tell. The app can say so too (Device's `wake`).
export const Wakes = Context.Reference<Stream.Stream<void>>("colibrii/Wakes", {
  defaultValue: () => Stream.never,
});
