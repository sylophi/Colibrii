// Websockets in a browser, through the page's WebSocket. A page sees
// neither why a socket did not open nor the status a refused upgrade
// was answered with, sends only some close codes, and has no way to cut
// a connection: a socket it closes finishes the closing handshake on its
// own, as long as that takes.
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import { makeConn } from "../internal/conn.ts";
import { type Conn, WebSocketOpenError, WebSockets } from "../Platform.ts";

// What a page may close with. Any other code, such as going away
// (1001), closes with 1000 and the same reason.
const sendable = (code: number) =>
  code === 1000 || (code >= 3000 && code <= 4999) ? code : 1000;

// The socket's events into a Conn. Called as the socket is made, in the
// same turn, so no event comes before its listener.
const connOf = (ws: WebSocket): Conn => {
  const { conn, received, ended } = makeConn({
    isOpen: () => ws.readyState === WebSocket.OPEN,
    // Never a view of a SharedArrayBuffer, which `send` refuses.
    send: (message) => ws.send(message as string | Uint8Array<ArrayBuffer>),
    close: (code, reason) => ws.close(sendable(code), reason),
  });
  ws.binaryType = "arraybuffer";
  ws.addEventListener("message", (event: MessageEvent<unknown>) => {
    received(
      typeof event.data === "string"
        ? event.data
        : new Uint8Array(event.data as ArrayBuffer),
    );
  });
  ws.addEventListener("close", (event) =>
    ended({ code: event.code, reason: event.reason }),
  );
  return conn;
};

const open = (url: string) =>
  Effect.gen(function* () {
    const opened = yield* Deferred.make<void, WebSocketOpenError>();
    const { conn } = yield* Effect.acquireRelease(
      Effect.try({
        // A page served over HTTPS throws here for a `ws://` URL.
        try: () => new WebSocket(url),
        catch: (cause) => new WebSocketOpenError({ cause }),
      }).pipe(
        Effect.map((ws) => {
          const created = connOf(ws);
          const fail = () =>
            Deferred.doneUnsafe(opened, Exit.fail(new WebSocketOpenError({})));
          ws.addEventListener("open", () =>
            Deferred.doneUnsafe(opened, Exit.void),
          );
          ws.addEventListener("error", fail);
          ws.addEventListener("close", fail);
          return { ws, conn: created };
        }),
      ),
      ({ ws }) =>
        Effect.sync(() => {
          if (ws.readyState < WebSocket.CLOSING) ws.close(1000, "closed");
        }),
    );
    yield* Deferred.await(opened);
    return conn;
  });

export const layerWebSockets = Layer.succeed(WebSockets, {
  dialableKinds: ["tunnel"],
  open,
});
