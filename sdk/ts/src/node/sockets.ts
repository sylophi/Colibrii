// Websockets in Node, through `ws`: the dialer, which shows why a socket
// did not open (a refused connection, the status a refused upgrade was
// answered with), and the listener on every interface.
import type { IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { networkInterfaces } from "node:os";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import { WebSocket, WebSocketServer } from "ws";
import {
  type Conn,
  ListenError,
  ListenerSockets,
  type NetworkAddress,
  WebSocketOpenError,
  WebSockets,
} from "../Platform.ts";
import { makeConn } from "../internal/conn.ts";
import { retryAfterOf } from "../internal/hubApi.ts";
import { MAX_SERVICE_MESSAGE_BYTES } from "../wire/hubSocket.ts";
import { MAX_MESSAGE_BYTES } from "../wire/noise.ts";

// How long a closing socket waits for the other end to drop the
// connection before it cuts it, and only then reads as closed. `ws`
// would wait 30 seconds, and a hub in workerd keeps the connection past
// its close frame.
const CLOSE_GRACE_MS = 1_000;

// `ws` takes it, though its types do not have it yet.
declare module "ws" {
  namespace WebSocket {
    interface ClientOptions {
      closeTimeout?: number;
    }
    // oxlint-disable-next-line no-unused-vars
    interface ServerOptions<U, V> {
      closeTimeout?: number;
    }
  }
}

const utf8 = new TextDecoder();

const bytesOf = (data: WebSocket.RawData): Uint8Array => {
  const buffer = Array.isArray(data)
    ? Buffer.concat(data)
    : Buffer.isBuffer(data)
      ? data
      : Buffer.from(data);
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
};

// The socket's events into a Conn. Called as the socket is made, in the
// same turn, so no event comes before its listener.
const connOf = (ws: WebSocket): Conn => {
  const { conn, received, ended } = makeConn({
    isOpen: () => ws.readyState === WebSocket.OPEN,
    send: (message) => ws.send(message),
    close: (code, reason) => ws.close(code, reason),
  });
  ws.on("message", (data, isBinary) => {
    const bytes = bytesOf(data);
    received(isBinary ? bytes : utf8.decode(bytes));
  });
  ws.on("close", (code, reason) => ended({ code, reason: reason.toString() }));
  // `ws` ends the socket after an error, such as a message past
  // maxPayload, and its close settles the Conn.
  ws.on("error", () => {});
  return conn;
};

// Closes the socket and waits for its close.
const shutdown = (ws: WebSocket) =>
  Effect.callback<void>((resume) => {
    if (ws.readyState === WebSocket.CLOSED) return resume(Effect.void);
    if (ws.readyState === WebSocket.CONNECTING) {
      ws.terminate();
      return resume(Effect.void);
    }
    ws.once("close", () => resume(Effect.void));
    ws.close(1000, "closed");
  });

const open = (url: string) =>
  Effect.gen(function* () {
    const opened = yield* Deferred.make<void, WebSocketOpenError>();
    const { conn } = yield* Effect.acquireRelease(
      Effect.sync(() => {
        const ws = new WebSocket(url, {
          closeTimeout: CLOSE_GRACE_MS,
          perMessageDeflate: false,
          // What the hub sends. A peer's message past Noise's cap fails
          // to open, which closes its link as well.
          maxPayload: MAX_SERVICE_MESSAGE_BYTES,
        });
        const created = connOf(ws);
        const fail = (error: WebSocketOpenError) =>
          Deferred.doneUnsafe(opened, Exit.fail(error));
        ws.on("open", () => Deferred.doneUnsafe(opened, Exit.void));
        // A listener here keeps `ws` from aborting the request itself.
        ws.on("unexpected-response", (request, response) => {
          fail(
            new WebSocketOpenError({
              status: response.statusCode ?? 0,
              retryAfterSeconds: retryAfterOf(
                response.headers["retry-after"]?.toString(),
              ),
            }),
          );
          request.destroy();
        });
        // Before the socket opens, why it did not. After, a close
        // follows, which says enough.
        ws.on("error", (error) =>
          fail(new WebSocketOpenError({ cause: error })),
        );
        ws.on("close", () => fail(new WebSocketOpenError({})));
        return { ws, conn: created };
      }),
      ({ ws }) => shutdown(ws),
    );
    yield* Deferred.await(opened);
    return conn;
  });

const headerOf = (request: IncomingMessage, name: string) => {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
};

const listen: ListenerSockets["Service"]["listen"] = (options) =>
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.callback<WebSocketServer, ListenError>((resume) => {
        const made = new WebSocketServer({
          port: options.port,
          closeTimeout: CLOSE_GRACE_MS,
          perMessageDeflate: false,
          maxPayload: MAX_MESSAGE_BYTES + 1,
          verifyClient: (info: { req: IncomingMessage }) =>
            options.allowOrigin(headerOf(info.req, "origin")),
        });
        made.once("listening", () => resume(Effect.succeed(made)));
        made.once("error", (error: NodeJS.ErrnoException) =>
          resume(
            Effect.fail(
              new ListenError({
                inUse: error.code === "EADDRINUSE",
                cause: error,
              }),
            ),
          ),
        );
      }),
      // After every socket's own close, below: the server's close waits
      // on its connections.
      (made) =>
        Effect.callback<void>((resume) => {
          made.close(() => resume(Effect.void));
        }),
    );
    const serving = yield* FiberSet.make();
    const run = yield* FiberSet.runtime(serving)();
    server.on("connection", (ws, request) => {
      const conn = connOf(ws);
      run(
        options
          .serve({
            conn,
            remoteAddress: request.socket.remoteAddress ?? "",
            connectingIp: headerOf(request, "cf-connecting-ip"),
          })
          .pipe(Effect.ensuring(shutdown(ws))),
      );
    });
    return (server.address() as AddressInfo).port;
  });

const addresses = Effect.sync(() =>
  Object.values(networkInterfaces()).flatMap((entries) =>
    (entries ?? []).map((entry): NetworkAddress => ({
      address: entry.address,
      family: entry.family,
      internal: entry.internal,
    })),
  ),
);

export const layerWebSockets = Layer.succeed(WebSockets, {
  dialableKinds: ["lan", "tunnel"],
  open,
});

export const layerListenerSockets = Layer.succeed(ListenerSockets, {
  listen,
  addresses,
});
