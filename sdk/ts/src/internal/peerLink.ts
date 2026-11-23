// A peer link once welcomed (spec/link.md, "Transport messages" and
// "Liveness"): app messages split and joined, pings answered, and the
// link's end, said once.
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type * as Cause from "effect/Cause";
import type * as Scope from "effect/Scope";
import { InvalidInputError, LinkClosedError } from "../Errors.ts";
import type { Link, LinkClosed, LinkCloseCause } from "../Link.ts";
import type { Conn } from "../Platform.ts";
import {
  AppMessageJoiner,
  CLOSE_GOING_AWAY,
  CLOSE_NORMAL,
  CLOSE_PROTOCOL_ERROR,
  CLOSE_TOO_BIG,
  MAX_APP_MESSAGE_BYTES,
  openTransportMessage,
  sealTransportMessage,
  splitAppMessage,
  type TransportMessage,
} from "../wire/link.ts";
import type { Transport } from "../wire/noise.ts";
import type { CandidateKind } from "../wire/relay.ts";
import * as Liveness from "./liveness.ts";
import type { Wakes } from "./wake.ts";

// What the device keeps of a link: the app's Link, the key its peer
// proved, and its end with a cause of the device's own.
export interface LiveLink extends Link {
  readonly peerKey: Uint8Array;
  readonly end: (code: number, cause: LinkCloseCause) => Effect.Effect<void>;
}

// Adds the link to its session's until the scope closes ("joined"),
// unless the session ended or its roster is unknown ("unavailable"), or
// the roster does not name the link's peer with the key it proved
// ("refused").
export type Join = (
  link: LiveLink,
) => Effect.Effect<"joined" | "unavailable" | "refused", never, Scope.Scope>;

// Seals and sends at once, so the sending cipher's counter and the
// socket keep one order.
export const sendSealed = (
  conn: Conn,
  transport: Transport,
  message: TransportMessage,
) => Effect.sync(() => conn.send(sealTransportMessage(transport, message)));

// The link lives until it ends or the scope closes.
export const make = Effect.fnUntraced(function* (options: {
  readonly conn: Conn;
  readonly transport: Transport;
  readonly role: "dialer" | "listener";
  readonly deviceId: string;
  readonly peerKey: Uint8Array;
  readonly connectionId: string;
  readonly arrival: CandidateKind;
  readonly greeting: Uint8Array;
  readonly wakes: Wakes;
}) {
  const { conn, transport } = options;
  const received = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const closed = yield* Deferred.make<LinkClosed>();
  const heard = yield* Liveness.makeHeard;
  const send = (message: TransportMessage) =>
    sendSealed(conn, transport, message);

  // Closes the socket unless the peer did, once, before anyone waiting
  // on the close hears of it. Hearing of it closes the link's scope,
  // whose fibers (the reader, the liveness watch) may be the one ending
  // it, so the messages end along with it, whatever is interrupted.
  let ended = false;
  const end = (code: number, cause: LinkCloseCause) =>
    Effect.suspend(() => {
      if (ended) return Effect.void;
      ended = true;
      if (cause !== "peer") conn.close(code, cause);
      return Deferred.succeed(closed, { code, cause }).pipe(
        Effect.andThen(Queue.end(received)),
        Effect.uninterruptible,
      );
    });

  const read = Effect.gen(function* () {
    const joiner = new AppMessageJoiner();
    while (true) {
      const data = yield* conn.receive;
      yield* Liveness.hear(heard);
      const message =
        typeof data === "string"
          ? Option.none()
          : openTransportMessage(transport, data);
      if (Option.isNone(message)) {
        return yield* end(CLOSE_PROTOCOL_ERROR, "protocol-error");
      }
      switch (message.value.type) {
        case "data": {
          const joined = joiner.add(message.value);
          if (joined._tag === "TooBig")
            return yield* end(CLOSE_TOO_BIG, "too-big");
          if (joined._tag === "Message") {
            yield* Queue.offer(received, joined.message);
          }
          break;
        }
        case "ping":
          yield* send({ type: "pong" });
          break;
        case "pong":
          break;
        default:
          return yield* end(CLOSE_PROTOCOL_ERROR, "protocol-error");
      }
    }
  }).pipe(
    Effect.catch((close) => end(close.code, "peer")),
    Effect.catchDefect(() => end(CLOSE_PROTOCOL_ERROR, "protocol-error")),
  );
  yield* Effect.forkScoped(read);

  yield* (
    options.role === "dialer"
      ? Liveness.watch({
          ping: send({ type: "ping" }),
          heard,
          wakes: options.wakes,
        })
      : Liveness.silence(heard)
  ).pipe(
    Effect.andThen(end(CLOSE_GOING_AWAY, "silent")),
    Effect.scoped,
    Effect.forkScoped,
  );
  yield* Effect.addFinalizer(() => end(CLOSE_NORMAL, "closed"));

  return {
    deviceId: options.deviceId,
    peerKey: options.peerKey,
    connectionId: options.connectionId,
    role: options.role,
    arrival: options.arrival,
    greeting: options.greeting,
    // Every piece in one step, so no other message comes between them
    // and none is cut short.
    send: (message) =>
      Effect.gen(function* () {
        if (message.length > MAX_APP_MESSAGE_BYTES) {
          return yield* new InvalidInputError({ field: "message" });
        }
        if (Deferred.isDoneUnsafe(closed) || !conn.isOpen()) {
          return yield* new LinkClosedError();
        }
        for (const piece of splitAppMessage(message)) {
          conn.send(sealTransportMessage(transport, piece));
        }
      }),
    messages: Stream.fromQueue(received),
    close: end(CLOSE_NORMAL, "closed"),
    closed: Deferred.await(closed),
    end,
  } satisfies LiveLink;
});
