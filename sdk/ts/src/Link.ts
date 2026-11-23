// An open peer link, as the app holds it at either end (spec/link.md,
// "What the app gets").
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type { InvalidInputError, LinkClosedError } from "./Errors.ts";
import type { CandidateKind } from "./wire/relay.ts";

// Why a link closed.
export type LinkCloseCause =
  // This app closed it, or the scope it was dialed in closed.
  | "closed"
  // The other side closed it, or the socket dropped.
  | "peer"
  // The peer left the roster, or came back to it with another key.
  | "left-roster"
  // Nothing was heard from the peer for too long.
  | "silent"
  // A newer link from the same peer replaced it.
  | "replaced"
  // This device signed out, was removed or stopped, or was closed.
  | "stopped"
  // The other side broke the protocol.
  | "protocol-error"
  // The other side sent an app message over the cap.
  | "too-big";

export interface LinkClosed {
  readonly code: number;
  readonly cause: LinkCloseCause;
}

export interface Link {
  // The other device.
  readonly deviceId: string;
  // The connection id the dialer named the link with, the same at both
  // ends.
  readonly connectionId: string;
  readonly role: "dialer" | "listener";
  readonly arrival: CandidateKind;
  // What the other side's app sent as the link opened: its app hello at
  // the listener, its app welcome at the dialer.
  readonly greeting: Uint8Array;
  // Sends one app message, of at most 32 MiB, whole and in order.
  readonly send: (
    message: Uint8Array,
  ) => Effect.Effect<void, LinkClosedError | InvalidInputError>;
  // Every app message received, in order, ending once the link closed.
  // It has one reader.
  readonly messages: Stream.Stream<Uint8Array>;
  readonly close: Effect.Effect<void>;
  // Waits for the link to close, and says why.
  readonly closed: Effect.Effect<LinkClosed>;
}
