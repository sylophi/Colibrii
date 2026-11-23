// The peer link (spec/link.md): its connection tickets, its handshake
// and transport messages, and the limits and timings the spec names.
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Base64Url from "./base64url.ts";
import { ConnectionId, DeviceId } from "./ids.ts";
import {
  concat,
  HandshakeState,
  type KeyPair,
  MAX_MESSAGE_BYTES,
  TAG_BYTES,
  type Transport,
  tryNoise,
} from "./noise.ts";

const EMPTY = new Uint8Array(0);
const utf8 = new TextEncoder();
const fromUtf8 = new TextDecoder();

// ---- Tickets ----

export const LINK_TICKET_PREFIX = "colibrii_lt_";

// The prefix and base64url of 16 random bytes. Only the listener that
// made a ticket reads it.
export const makeLinkTicket = (): string =>
  `${LINK_TICKET_PREFIX}${Base64Url.random(16)}`;

export const TICKET_TTL_MS = 60_000;
export const MAX_TICKET_SETS_PER_DEVICE = 32;
export const MAX_TICKETS = 256;
export const RETIRED_TICKETS_KEPT = 1_024;

// ---- Admission ----

export const MAX_SOCKETS = 64;
export const MAX_UNWELCOMED_SOCKETS = 16;
export const LOCKOUT_GUESSES = 5;
export const LOCKOUT_MS = 30_000;
// From the upgrade to `welcome`.
export const WELCOME_WITHIN_MS = 10_000;

// ---- Liveness ----

export const PING_EVERY_MS = 15_000;
// The dialer's: nothing for this long after a ping, and then nothing
// within SECOND_PING_WITHIN_MS of a second one.
export const DEAD_AFTER_MS = 40_000;
export const SECOND_PING_WITHIN_MS = 5_000;
export const LISTENER_SILENT_MS = 120_000;

// ---- Dialing ----

export const RACE_TIMEOUT_MS = 10_000;
// The wait before each redial, staying at the last.
export const REDIAL_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 16_000];
// A link up this long starts the redial waits over.
export const REDIAL_RESET_AFTER_MS = 30_000;

// ---- Close codes ----

export const CLOSE_NORMAL = 1000;
export const CLOSE_GOING_AWAY = 1001;
export const CLOSE_PROTOCOL_ERROR = 1002;
export const CLOSE_TOO_BIG = 1009;
export const CLOSE_TRY_AGAIN = 1013;
export const CLOSE_HELLO_REFUSED = 4002;
export const CLOSE_LOCKED_OUT = 4003;
export const CLOSE_HANDSHAKE_REFUSED = 4004;

// ---- The handshake ----

export const linkPrologue = (ticket: string): Uint8Array =>
  utf8.encode(`colibrii-link-v1:${ticket}`);

// The ticket's length in two bytes, the ticket, then Noise message 1.
export const encodeFirstMessage = (
  ticket: string,
  message: Uint8Array,
): Uint8Array => {
  const ticketBytes = utf8.encode(ticket);
  const length = new Uint8Array(2);
  new DataView(length.buffer).setUint16(0, ticketBytes.length);
  return concat(length, ticketBytes, message);
};

export const decodeFirstMessage = (
  bytes: Uint8Array,
): Option.Option<{ readonly ticket: string; readonly message: Uint8Array }> => {
  if (bytes.length < 2) return Option.none();
  const length = new DataView(bytes.buffer, bytes.byteOffset).getUint16(0);
  if (bytes.length < 2 + length) return Option.none();
  return Option.some({
    ticket: fromUtf8.decode(bytes.subarray(2, 2 + length)),
    message: bytes.subarray(2 + length),
  });
};

// The dialer's first message, and the handshake that reads the answer.
// Only the test vectors pass e.
export const startHandshake = (options: {
  readonly ticket: string;
  readonly key: KeyPair;
  readonly listenerKey: Uint8Array;
  readonly e?: KeyPair;
}): { readonly first: Uint8Array; readonly handshake: HandshakeState } => {
  const handshake = new HandshakeState({
    initiator: true,
    prologue: linkPrologue(options.ticket),
    s: options.key,
    rs: options.listenerKey,
    e: options.e,
  });
  const { message } = handshake.writeMessage(EMPTY);
  return { first: encodeFirstMessage(options.ticket, message), handshake };
};

// The listener's side, once the ticket checks out: the key the dialer
// proved, which must be the roster's before the second message is sent.
export const answerHandshake = (options: {
  readonly ticket: string;
  readonly message: Uint8Array;
  readonly key: KeyPair;
  readonly e?: KeyPair;
}): Option.Option<{
  readonly dialerKey: Uint8Array;
  readonly second: Uint8Array;
  readonly transport: Transport;
  readonly handshakeHash: Uint8Array;
}> =>
  Option.flatMap(
    tryNoise(() => {
      const handshake = new HandshakeState({
        initiator: false,
        prologue: linkPrologue(options.ticket),
        s: options.key,
        e: options.e,
      });
      const { payload } = handshake.readMessage(options.message);
      const { message, transport } = handshake.writeMessage(EMPTY);
      return { handshake, payload, message, transport };
    }),
    ({ handshake, payload, message, transport }) => {
      const dialerKey = handshake.remoteStaticKey();
      return dialerKey === null || transport === null || payload.length > 0
        ? Option.none()
        : Option.some({
            dialerKey,
            second: message,
            transport,
            handshakeHash: handshake.handshakeHash(),
          });
    },
  );

export const finishHandshake = (
  handshake: HandshakeState,
  second: Uint8Array,
): Option.Option<Transport> =>
  Option.flatMap(
    tryNoise(() => handshake.readMessage(second)),
    (read) =>
      read.payload.length > 0
        ? Option.none()
        : Option.fromNullishOr(read.transport),
  );

// ---- Transport messages ----

// The app hello, welcome and refusal each, before encoding, so each of
// their messages fits one transport message.
export const MAX_APP_BYTES = 16_384;

const AppBytes = Base64Url.Bytes.check(
  Schema.makeFilter(
    (bytes: Uint8Array) =>
      bytes.length <= MAX_APP_BYTES || `at most ${MAX_APP_BYTES} bytes`,
  ),
);

export const Hello = Schema.Struct({
  deviceId: DeviceId,
  connectionId: ConnectionId,
  appHello: AppBytes,
});
export type Hello = typeof Hello.Type;

export const Welcome = Schema.Struct({ appWelcome: AppBytes });
export type Welcome = typeof Welcome.Type;

export const Refuse = Schema.Union([
  Schema.Struct({ by: Schema.Literal("colibrii"), reason: Schema.String }),
  Schema.Struct({
    by: Schema.Literal("app"),
    reason: Schema.String,
    appRefusal: Schema.optionalKey(AppBytes),
  }),
]);
export type Refuse = typeof Refuse.Type;

export const TYPE_DATA = 0x00;
export const TYPE_DATA_MORE = 0x01;
export const TYPE_HELLO = 0x10;
export const TYPE_WELCOME = 0x11;
export const TYPE_REFUSE = 0x12;
export const TYPE_PING = 0x20;
export const TYPE_PONG = 0x21;

// `more` is a piece of an app message with more to come (0x01).
export type TransportMessage =
  | {
      readonly type: "data";
      readonly more: boolean;
      readonly body: Uint8Array;
    }
  | { readonly type: "hello"; readonly hello: Hello }
  | { readonly type: "welcome"; readonly welcome: Welcome }
  | { readonly type: "refuse"; readonly refuse: Refuse }
  | { readonly type: "ping" }
  | { readonly type: "pong" };

const HelloJson = Schema.fromJsonString(Hello);
const WelcomeJson = Schema.fromJsonString(Welcome);
const RefuseJson = Schema.fromJsonString(Refuse);

const json = <S extends Schema.Codec<unknown, string>>(
  schema: S,
  value: S["Type"],
): Uint8Array => utf8.encode(Schema.encodeSync(schema)(value));

const withType = (type: number, body: Uint8Array): Uint8Array =>
  concat(Uint8Array.of(type), body);

// A transport message's plaintext: its type, then its body.
export const encodeTransportMessage = (
  message: TransportMessage,
): Uint8Array => {
  switch (message.type) {
    case "data":
      return withType(message.more ? TYPE_DATA_MORE : TYPE_DATA, message.body);
    case "hello":
      return withType(TYPE_HELLO, json(HelloJson, message.hello));
    case "welcome":
      return withType(TYPE_WELCOME, json(WelcomeJson, message.welcome));
    case "refuse":
      return withType(TYPE_REFUSE, json(RefuseJson, message.refuse));
    case "ping":
      return Uint8Array.of(TYPE_PING);
    case "pong":
      return Uint8Array.of(TYPE_PONG);
  }
};

// Nothing for an unknown type or a body that does not decode, which
// closes the link with 1002.
export const decodeTransportMessage = (
  plaintext: Uint8Array,
): Option.Option<TransportMessage> => {
  const body = plaintext.subarray(1);
  const text = () => fromUtf8.decode(body);
  switch (plaintext[0]) {
    case TYPE_DATA:
    case TYPE_DATA_MORE:
      return Option.some({
        type: "data",
        more: plaintext[0] === TYPE_DATA_MORE,
        body,
      });
    case TYPE_HELLO:
      return Option.map(Schema.decodeOption(HelloJson)(text()), (hello) => ({
        type: "hello",
        hello,
      }));
    case TYPE_WELCOME:
      return Option.map(
        Schema.decodeOption(WelcomeJson)(text()),
        (welcome) => ({ type: "welcome", welcome }),
      );
    case TYPE_REFUSE:
      return Option.map(Schema.decodeOption(RefuseJson)(text()), (refuse) => ({
        type: "refuse",
        refuse,
      }));
    case TYPE_PING:
      return body.length === 0 ? Option.some({ type: "ping" }) : Option.none();
    case TYPE_PONG:
      return body.length === 0 ? Option.some({ type: "pong" }) : Option.none();
    default:
      return Option.none();
  }
};

// Whether the message fits one transport message once sealed.
export const fitsTransportMessage = (message: TransportMessage): boolean =>
  encodeTransportMessage(message).length + TAG_BYTES <= MAX_MESSAGE_BYTES;

// One websocket message: the plaintext under the sending cipher, with
// empty associated data.
export const sealTransportMessage = (
  transport: Transport,
  message: TransportMessage,
): Uint8Array =>
  transport.send.encryptWithAd(EMPTY, encodeTransportMessage(message));

export const openTransportMessage = (
  transport: Transport,
  ciphertext: Uint8Array,
): Option.Option<TransportMessage> =>
  Option.flatMap(
    tryNoise(() => transport.receive.decryptWithAd(EMPTY, ciphertext)),
    decodeTransportMessage,
  );

// ---- App messages ----

// What fits one transport message beside its type byte and tag.
export const MAX_PIECE_BYTES = 65_518;
export const MAX_APP_MESSAGE_BYTES = 32 * 1024 * 1024;

export const splitAppMessage = (
  message: Uint8Array,
): Array<TransportMessage & { readonly type: "data" }> => {
  const pieces: Array<TransportMessage & { readonly type: "data" }> = [];
  let offset = 0;
  do {
    const body = message.subarray(offset, offset + MAX_PIECE_BYTES);
    offset += body.length;
    pieces.push({ type: "data", more: offset < message.length, body });
  } while (offset < message.length);
  return pieces;
};

export type Joined =
  | { readonly _tag: "Partial" }
  | { readonly _tag: "Message"; readonly message: Uint8Array }
  | { readonly _tag: "TooBig" };

// Joins the pieces of one app message at a time, into a buffer that
// doubles as it fills, so pieces however small or many cost only their
// bytes. TooBig closes the link with 1009.
export class AppMessageJoiner {
  private buffer = EMPTY;
  private length = 0;

  add(piece: { readonly more: boolean; readonly body: Uint8Array }): Joined {
    if (this.length === 0 && !piece.more) {
      return { _tag: "Message", message: piece.body };
    }
    const length = this.length + piece.body.length;
    if (length > MAX_APP_MESSAGE_BYTES) return { _tag: "TooBig" };
    if (length > this.buffer.length) {
      const grown = new Uint8Array(
        Math.min(
          MAX_APP_MESSAGE_BYTES,
          Math.max(length, this.buffer.length * 2),
        ),
      );
      grown.set(this.buffer.subarray(0, this.length));
      this.buffer = grown;
    }
    this.buffer.set(piece.body, this.length);
    this.length = length;
    if (piece.more) return { _tag: "Partial" };
    const message = this.buffer.subarray(0, length);
    this.buffer = EMPTY;
    this.length = 0;
    return { _tag: "Message", message };
  }
}
