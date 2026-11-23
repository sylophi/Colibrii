// The relay (spec/relay.md): the `ask` and `answer` frames, each ask
// sealed as one Noise handshake, and the connectInfo ask.
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";
import * as Base64Url from "./base64url.ts";
import { ConnectionId } from "./ids.ts";
import { HandshakeState, type KeyPair, tryNoise } from "./noise.ts";
import { ReadableArray } from "./readable.ts";

const utf8 = new TextEncoder();
const fromUtf8 = new TextDecoder();

export const ASK_TIMEOUT_MS = 10_000;
// How long the asked device remembers an ask it opened.
export const SEEN_ASK_MS = 10 * 60_000;
// How long a device whose tunnel is starting waits for it before it
// answers an asker that would get nothing else.
export const TUNNEL_WAIT_MS = 6_000;
export const MAX_LAN_CANDIDATES = 6;

// ---- Frames ----

// `id` is compared as a string, never as a number.
export interface RelayFrame {
  readonly kind: "ask" | "answer";
  readonly id: string;
  readonly sealed: string;
}

const FRAME = /^(ask|answer):(\d{1,16}):([A-Za-z0-9_-]+)$/;

export const encodeRelayFrame = (frame: RelayFrame): string =>
  `${frame.kind}:${frame.id}:${frame.sealed}`;

export const decodeRelayFrame = (text: string): Option.Option<RelayFrame> => {
  const match = FRAME.exec(text);
  return match === null
    ? Option.none()
    : Option.some({
        kind: match[1] === "ask" ? "ask" : "answer",
        id: match[2] ?? "",
        sealed: match[3] ?? "",
      });
};

// ---- Payloads ----

export const CandidateKind = Schema.Literals(["lan", "tunnel"]);
export type CandidateKind = typeof CandidateKind.Type;

const isCandidateKind = Schema.is(CandidateKind);

// The asked device answers for the kinds it knows.
export const ConnectInfoInput = Schema.Struct({
  dialableKinds: ReadableArray(CandidateKind),
  connectionId: ConnectionId,
});
export type ConnectInfoInput = typeof ConnectInfoInput.Type;

const OCTET = "(?:25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)";
// The IPv6 address in brackets is checked by URL's own parser.
const LAN_URL = new RegExp(
  `^ws://(?:${OCTET}(?:\\.${OCTET}){3}|\\[[0-9A-Fa-f:.]+\\]):([1-9]\\d{0,4})$`,
);
const LABEL = "[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?";
const TUNNEL_URL = new RegExp(`^wss://${LABEL}(?:\\.${LABEL})+$`);

const isCandidateUrl = (kind: CandidateKind, url: string): boolean => {
  if (kind === "tunnel") return TUNNEL_URL.test(url);
  const port = LAN_URL.exec(url)?.[1];
  return port !== undefined && Number(port) <= 65_535 && URL.canParse(url);
};

export const Candidate = Schema.Struct({
  kind: CandidateKind,
  url: Schema.String,
  ticket: Schema.String,
}).check(
  Schema.makeFilter(
    (candidate) =>
      isCandidateUrl(candidate.kind, candidate.url) ||
      "a lan URL is ws://<IP address>:<port>, a tunnel URL wss://<hostname>",
  ),
);
export type Candidate = typeof Candidate.Type;

const countOf = (
  candidates: ReadonlyArray<Candidate>,
  kind: CandidateKind,
): number => candidates.filter((candidate) => candidate.kind === kind).length;

// Only its kind is looked at here, and the rest decodes as a Candidate.
const isOfKnownKind = (entry: unknown): entry is typeof Candidate.Encoded =>
  Predicate.hasProperty(entry, "kind") && isCandidateKind(entry.kind);

// The asker skips a candidate of a kind it does not know, and one of a
// kind it knows must decode.
const Candidates = Schema.Array(Schema.Unknown).pipe(
  Schema.decodeTo(
    Schema.Array(Candidate).check(
      Schema.makeFilter(
        (candidates) =>
          (countOf(candidates, "lan") <= MAX_LAN_CANDIDATES &&
            countOf(candidates, "tunnel") <= 1) ||
          `at most ${MAX_LAN_CANDIDATES} lan candidates and one tunnel`,
      ),
    ),
    SchemaTransformation.transform<
      ReadonlyArray<typeof Candidate.Encoded>,
      ReadonlyArray<unknown>
    >({
      decode: (entries) => entries.filter(isOfKnownKind),
      encode: (candidates) => candidates,
    }),
  ),
);

export const ConnectInfoResult = Schema.Union([
  Schema.Struct({ available: Schema.Literal(false) }),
  Schema.Struct({ available: Schema.Literal(true), candidates: Candidates }),
]);
export type ConnectInfoResult = typeof ConnectInfoResult.Type;

// An ask as the asked device opens it. An ask it does not know is
// answered `unknown-ask`, and an input that does not decode `bad-input`.
export const Ask = Schema.Struct({
  ask: Schema.String,
  input: Schema.optionalKey(Schema.Unknown),
});
export type Ask = typeof Ask.Type;

export const ConnectInfoAsk = Schema.Struct({
  ask: Schema.Literal("connectInfo"),
  input: ConnectInfoInput,
});
export type ConnectInfoAsk = typeof ConnectInfoAsk.Type;

// `no-listener`, `unknown-ask` or `bad-input`. The asker reads a code it
// does not know as a refusal all the same.
export const Answer = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), result: ConnectInfoResult }),
  Schema.Struct({ ok: Schema.Literal(false), code: Schema.String }),
]);
export type Answer = typeof Answer.Type;

const AskJson = Schema.fromJsonString(Ask);
const ConnectInfoAskJson = Schema.fromJsonString(ConnectInfoAsk);
const AnswerJson = Schema.fromJsonString(Answer);

// The exact bytes each payload is sealed as.
export const encodeAsk = (ask: ConnectInfoAsk): Uint8Array =>
  utf8.encode(Schema.encodeSync(ConnectInfoAskJson)(ask));

export const encodeAnswer = (answer: Answer): Uint8Array =>
  utf8.encode(Schema.encodeSync(AnswerJson)(answer));

// ---- Sealing ----

export const relayPrologue = (asker: string, asked: string): Uint8Array =>
  utf8.encode(`colibrii-relay-v1:${asker}>${asked}`);

// The asker's side: the sealed ask, and the handshake that opens the
// answer. Only the test vectors pass e.
export const sealAsk = (options: {
  readonly asker: string;
  readonly asked: string;
  readonly key: KeyPair;
  readonly askedKey: Uint8Array;
  readonly payload: Uint8Array;
  readonly e?: KeyPair;
}): { readonly sealed: string; readonly handshake: HandshakeState } => {
  const handshake = new HandshakeState({
    initiator: true,
    prologue: relayPrologue(options.asker, options.asked),
    s: options.key,
    rs: options.askedKey,
    e: options.e,
  });
  const { message } = handshake.writeMessage(options.payload);
  return { sealed: Base64Url.encode(message), handshake };
};

// The asked device's side. The caller checks `askerKey` against the
// roster and `handshakeHash` against the asks it has seen, and answers
// through `handshake`. Nothing for an ask that does not open.
export const openAsk = (options: {
  readonly asker: string;
  readonly asked: string;
  readonly key: KeyPair;
  readonly sealed: string;
  readonly e?: KeyPair;
}): Option.Option<{
  readonly ask: Ask;
  readonly askerKey: Uint8Array;
  readonly handshakeHash: Uint8Array;
  readonly handshake: HandshakeState;
}> =>
  Option.flatMap(Base64Url.decode(options.sealed), (message) => {
    const handshake = new HandshakeState({
      initiator: false,
      prologue: relayPrologue(options.asker, options.asked),
      s: options.key,
      e: options.e,
    });
    return Option.flatMap(
      tryNoise(() => handshake.readMessage(message).payload),
      (payload) => {
        const askerKey = handshake.remoteStaticKey();
        return askerKey === null
          ? Option.none()
          : Option.map(
              Schema.decodeOption(AskJson)(fromUtf8.decode(payload)),
              (ask) => ({
                ask,
                askerKey,
                handshakeHash: handshake.handshakeHash(),
                handshake,
              }),
            );
      },
    );
  });

export const sealAnswer = (
  handshake: HandshakeState,
  payload: Uint8Array,
): string => Base64Url.encode(handshake.writeMessage(payload).message);

export const openAnswer = (
  handshake: HandshakeState,
  sealed: string,
): Option.Option<Answer> =>
  Option.flatMap(Base64Url.decode(sealed), (message) =>
    Option.flatMap(
      tryNoise(() => handshake.readMessage(message).payload),
      (payload) => Schema.decodeOption(AnswerJson)(fromUtf8.decode(payload)),
    ),
  );
