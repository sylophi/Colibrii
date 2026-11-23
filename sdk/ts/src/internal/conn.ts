// A Conn over a websocket, as a binding wires its events in: each
// message and the close as they come. `send` and `close` act only while
// the socket is open.
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import type { Conn, SocketClose } from "../Platform.ts";

export const makeConn = (socket: {
  readonly isOpen: () => boolean;
  readonly send: (message: string | Uint8Array) => void;
  readonly close: (code: number, reason: string) => void;
}) => {
  const inbound = Effect.runSync(
    Queue.unbounded<string | Uint8Array, SocketClose>(),
  );
  const closed = Deferred.makeUnsafe<SocketClose>();
  const conn: Conn = {
    send: (message) => {
      if (socket.isOpen()) socket.send(message);
    },
    close: (code, reason) => {
      if (socket.isOpen()) socket.close(code, reason);
    },
    isOpen: socket.isOpen,
    receive: Queue.take(inbound),
    closed: Deferred.await(closed),
  };
  return {
    conn,
    received: (message: string | Uint8Array) => {
      Queue.offerUnsafe(inbound, message);
    },
    ended: (close: SocketClose) => {
      Queue.failCauseUnsafe(inbound, Cause.fail(close));
      Deferred.doneUnsafe(closed, Exit.succeed(close));
    },
  };
};
