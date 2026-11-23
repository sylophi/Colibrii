// Builds the test vectors, and with `--write` writes them to
// spec/vectors/. `pnpm run vectors` writes and formats
// them. A released vector never changes (spec/vectors.md).
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import * as Schema from "effect/Schema";
import * as Hex from "effect/encoding/Hex";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Api from "../src/wire/api.ts";
import * as Base64Url from "../src/wire/base64url.ts";
import { credentialHash } from "../src/wire/credential.ts";
import * as HubSocket from "../src/wire/hubSocket.ts";
import * as Link from "../src/wire/link.ts";
import * as Noise from "../src/wire/noise.ts";
import * as Relay from "../src/wire/relay.ts";
import { tunnelLabel } from "../src/wire/tunnel.ts";

export interface Case {
  readonly name: string;
  readonly inputs: Record<string, unknown>;
  readonly expected: Record<string, unknown>;
}

const bytes = (from: number, length: number) =>
  Uint8Array.from({ length }, (_, i) => (from + i) % 256);

const hex = (data: Uint8Array) => Hex.encode(data);

export const fromHex = (text: string) => Result.getOrThrow(Hex.decode(text));

// ---- base64url ----

const encodeCases = [
  "",
  "66",
  "666f",
  "666f6f",
  "666f6f62",
  "666f6f6261",
  "666f6f626172",
  "fbffbf",
  "fbff",
];

const decodeCases = [
  "",
  "Zg",
  "Zg==",
  "Zm8",
  "Zm8=",
  "Zm9v",
  "Zm9vYg",
  "Zm9vYg==",
  "-_-_",
  "-_8",
  "-_8=",
  "Z",
  "Zg=",
  "Zm9vY===",
  "Zg==Zg",
  "+/+/",
  "Zm9v+A",
  "Zm 9v",
];

const base64url = (): Case[] => [
  ...encodeCases.map((input) => ({
    name: `encode ${input === "" ? "nothing" : input}`,
    inputs: { hex: input },
    expected: { base64url: Base64Url.encode(fromHex(input)) },
  })),
  ...decodeCases.map((input) => ({
    name: `decode ${input === "" ? "nothing" : input}`,
    inputs: { base64url: input },
    expected: Option.match(Base64Url.decode(input), {
      onNone: () => ({ invalid: true }),
      onSome: (data) => ({ hex: hex(data) }),
    }),
  })),
];

// ---- hub.json ----

const deviceA = "6f1c2a9e-0b7d-4d0e-9a51-3c2b8e4f7a10";
const deviceB = "0b6e3c1d-2f4a-4b8c-9d0e-1f2a3b4c5d6e";
const keyA = Base64Url.encode(bytes(1, 32));
const keyB = Base64Url.encode(bytes(101, 32));
const accountId = "user_2xK9mQ4bW7nR3tY8vZ1pL6sD0fG";
const credential = `colibrii_dc_${Base64Url.encode(bytes(201, 32))}`;
const ticket = `colibrii_ct_${Base64Url.encode(new TextEncoder().encode(`inkwell/${accountId}`))}.${Base64Url.encode(bytes(7, 16))}.${Base64Url.encode(bytes(77, 32))}`;
const frame = `ask:4096:${Base64Url.encode(bytes(31, 96))}`;

const device = {
  deviceId: deviceA,
  name: "Studio MacBook",
  data: { icon: "laptop" },
  connections: "single",
  createdAt: 1793100000000,
  lastSeenAt: 1793180000000,
  online: true,
} as const;

const browser = {
  deviceId: deviceB,
  name: "Firefox on the studio desktop",
  data: {},
  connections: "multi",
  createdAt: 1793150000000,
  lastSeenAt: null,
  online: false,
} as const;

// Each sample, by what it is, with the schema that reads it.
export const hubSamples: ReadonlyArray<{
  readonly name: string;
  readonly inputs: Record<string, string>;
  readonly schema: Schema.Codec<unknown, unknown>;
  readonly value: unknown;
}> = [
  {
    name: "socket: relay from the device",
    inputs: { socket: "relay", from: "device" },
    schema: HubSocket.RelayTo,
    value: { t: "relay", to: deviceB, frame },
  },
  {
    name: "socket: relay from the service",
    inputs: { socket: "relay", from: "service" },
    schema: HubSocket.RelayFrom,
    value: { t: "relay", from: deviceA, frame },
  },
  {
    name: "socket: presence",
    inputs: { socket: "presence", from: "service" },
    schema: HubSocket.Presence,
    value: {
      t: "presence",
      online: [
        { deviceId: deviceB, publicKey: keyB, connections: "multi" },
        { deviceId: deviceA, publicKey: keyA, connections: "single" },
      ],
    },
  },
  {
    name: "socket: nack",
    inputs: { socket: "nack", from: "service" },
    schema: HubSocket.Nack,
    value: { t: "nack", to: deviceB, reason: "offline" },
  },
  {
    name: "POST /devices/enroll request",
    inputs: { route: "POST /devices/enroll", body: "request" },
    schema: Api.EnrollRequest,
    value: {
      app: "inkwell",
      deviceId: deviceA,
      name: "Studio MacBook",
      publicKey: keyA,
      connections: "single",
      data: { icon: "laptop" },
    },
  },
  {
    name: "POST /devices/enroll request without data",
    inputs: { route: "POST /devices/enroll", body: "request" },
    schema: Api.EnrollRequest,
    value: {
      app: "inkwell",
      deviceId: deviceB,
      name: "Firefox on the studio desktop",
      publicKey: keyB,
      connections: "multi",
    },
  },
  {
    name: "POST /devices/enroll answer",
    inputs: { route: "POST /devices/enroll", body: "answer" },
    schema: Api.EnrollResponse,
    value: { credential, accountId, device },
  },
  {
    name: "POST /sign-in-token request",
    inputs: { route: "POST /sign-in-token", body: "request" },
    schema: Api.SignInTokenRequest,
    value: { app: "inkwell" },
  },
  {
    name: "POST /sign-in-token answer",
    inputs: { route: "POST /sign-in-token", body: "answer" },
    schema: Api.SignInTokenResponse,
    value: { token: "sit_3c9d0e1f2a4b5c6d7e8f9a0b1c2d3e4f" },
  },
  {
    name: "GET /devices answer",
    inputs: { route: "GET /devices", body: "answer" },
    schema: Api.DeviceList,
    value: { devices: [device, browser] },
  },
  {
    name: "PATCH /devices/:deviceId request with a name",
    inputs: { route: "PATCH /devices/:deviceId", body: "request" },
    schema: Api.DevicePatch,
    value: { name: "Studio MacBook Pro" },
  },
  {
    name: "PATCH /devices/:deviceId request with data",
    inputs: { route: "PATCH /devices/:deviceId", body: "request" },
    schema: Api.DevicePatch,
    value: { data: { icon: "desktop", pinned: true, order: 2 } },
  },
  {
    name: "PATCH /devices/:deviceId request with both",
    inputs: { route: "PATCH /devices/:deviceId", body: "request" },
    schema: Api.DevicePatch,
    value: { name: "Studio", data: {} },
  },
  {
    name: "POST /tickets request",
    inputs: { route: "POST /tickets", body: "request" },
    schema: Api.TicketRequest,
    value: { connectionId: "3f9a0c1e5b7d4f2a8c6e0b1d3f5a7c9e" },
  },
  {
    name: "POST /tickets answer",
    inputs: { route: "POST /tickets", body: "answer" },
    schema: Api.TicketResponse,
    value: { ticket },
  },
  {
    name: "POST /tunnel request",
    inputs: { route: "POST /tunnel", body: "request" },
    schema: Api.TunnelRequest,
    value: { port: 41734 },
  },
  {
    name: "POST /tunnel answer",
    inputs: { route: "POST /tunnel", body: "answer" },
    schema: Api.TunnelResponse,
    value: {
      hostname: "4f1c0d9e2b7a6c3e8d5f0a1b2c3d4e5f.example.link",
      connectorToken: "eyJhIjoiMDAwMCIsInQiOiIxMTExIiwicyI6IjIyMjIifQ",
      fresh: true,
    },
  },
];

const later = "3e8f2a6b-9c1d-4e7f-8a2b-5c6d7e8f9a0b";
const keyLater = Base64Url.encode(bytes(151, 32));

// What a later version may send, as it arrives, with the schema a reader
// reads it with (spec/README.md, "Growing the wire").
export const readSamples: ReadonlyArray<{
  readonly name: string;
  readonly inputs: Record<string, string>;
  readonly schema: Schema.Codec<unknown, unknown>;
  readonly received: unknown;
}> = [
  {
    name: "socket: presence with an entry the reader cannot read",
    inputs: { socket: "presence", from: "service" },
    schema: HubSocket.Presence,
    received: {
      t: "presence",
      online: [
        { deviceId: deviceB, publicKey: keyB, connections: "multi" },
        { deviceId: later, publicKey: keyLater, connections: "fleet" },
        {
          deviceId: deviceA,
          publicKey: keyA,
          connections: "single",
          since: 1793180000000,
        },
      ],
    },
  },
  {
    name: "socket: nack with a reason the reader does not know",
    inputs: { socket: "nack", from: "service" },
    schema: HubSocket.Nack,
    received: { t: "nack", to: deviceB, reason: "busy" },
  },
  {
    name: "GET /devices answer with an entry the reader cannot read",
    inputs: { route: "GET /devices", body: "answer" },
    schema: Api.DeviceList,
    received: {
      devices: [device, { ...browser, connections: "fleet" }],
      more: false,
    },
  },
];

// Each error, with a sample of its fields.
export const errorSamples: ReadonlyArray<{
  readonly schema: Schema.Codec<unknown, unknown>;
  readonly status: number;
  readonly value: unknown;
}> = [
  {
    schema: Api.UpdateRequiredError,
    status: 403,
    value: new Api.UpdateRequiredError({ floor: 1 }),
  },
  {
    schema: Api.LoginRejectedError,
    status: 401,
    value: new Api.LoginRejectedError(),
  },
  {
    schema: Api.UnknownAppError,
    status: 404,
    value: new Api.UnknownAppError({ app: "slate" }),
  },
  {
    schema: Api.CredentialRejectedError,
    status: 401,
    value: new Api.CredentialRejectedError(),
  },
  {
    schema: Api.DeviceRevokedError,
    status: 403,
    value: new Api.DeviceRevokedError(),
  },
  {
    schema: Api.DeviceEnrolledElsewhereError,
    status: 409,
    value: new Api.DeviceEnrolledElsewhereError(),
  },
  {
    schema: Api.AccountFullError,
    status: 409,
    value: new Api.AccountFullError({ limit: 16 }),
  },
  {
    schema: Api.UnknownDeviceError,
    status: 404,
    value: new Api.UnknownDeviceError({ deviceId: deviceB }),
  },
  {
    schema: Api.TicketMalformedError,
    status: 403,
    value: new Api.TicketMalformedError(),
  },
  {
    schema: Api.UpgradeRequiredError,
    status: 426,
    value: new Api.UpgradeRequiredError(),
  },
  {
    schema: Api.SignInTokensOffError,
    status: 403,
    value: new Api.SignInTokensOffError(),
  },
  {
    schema: Api.UnavailableError,
    status: 502,
    value: new Api.UnavailableError({ operation: "sign-in-token" }),
  },
];

const hub = (): Case[] => [
  ...hubSamples.map(({ name, inputs, schema, value }) => ({
    name,
    inputs,
    expected: { json: Schema.encodeUnknownSync(schema)(value) },
  })),
  ...readSamples.map(({ name, inputs, schema, received }) => ({
    name,
    inputs: { ...inputs, json: received },
    expected: {
      json: Schema.encodeUnknownSync(schema)(
        Schema.decodeUnknownSync(schema)(received),
      ),
    },
  })),
  {
    name: "socket: ping",
    inputs: { socket: "ping", from: "device" },
    expected: { text: HubSocket.PING },
  },
  {
    name: "socket: pong",
    inputs: { socket: "pong", from: "service" },
    expected: { text: HubSocket.PONG },
  },
  ...errorSamples.map(({ schema, status, value }) => {
    const json = Schema.encodeUnknownSync(schema)(value) as { _tag: string };
    return {
      name: `error: ${json._tag}`,
      inputs: { error: json._tag },
      expected: { status, json },
    };
  }),
];

// ---- tunnel-label.json and credential-hash.json ----

const tunnelLabels = async (): Promise<Case[]> =>
  Promise.all(
    [
      ["inkwell", accountId, deviceA],
      ["inkwell", accountId, deviceB],
      ["inkwell-dev", accountId, deviceA],
      ["quill", "user_01HZY3QJ5R8W2E6T4N7M9K0B1C", deviceB],
    ].map(async ([app = "", account = "", deviceId = ""]) => ({
      name: `${app}, ${account}, ${deviceId}`,
      inputs: { app, account, device: deviceId },
      expected: { label: await tunnelLabel(app, account, deviceId) },
    })),
  );

const credentialHashes = async (): Promise<Case[]> =>
  Promise.all(
    [
      credential,
      `colibrii_dc_${Base64Url.encode(bytes(0, 32))}`,
      `colibrii_dc_${Base64Url.encode(new Uint8Array(32).fill(255))}`,
    ].map(async (input) => ({
      name: input,
      inputs: { credential: input },
      expected: { sha256: await credentialHash(input) },
    })),
  );

// ---- The handshakes ----
//
// Every key is fixed, ephemeral keys included, and each private key is
// given beside its public key. Payloads are given as their exact bytes.

const key = (from: number) => Noise.keyPairFromPrivateKey(bytes(from, 32));

const utf8 = (text: string) => new TextEncoder().encode(text);

export const EMPTY = new Uint8Array(0);

export const b64 = Base64Url.encode;

const keyInputs = (role: string, pair: Noise.KeyPair) => ({
  [`${role}PrivateKey`]: b64(pair.privateKey),
});

// ---- noise.json ----

export type NoiseSide = "initiator" | "responder";

const noiseCase = (
  name: string,
  prologue: Uint8Array,
  keys: {
    readonly initiatorStatic: Noise.KeyPair;
    readonly initiatorEphemeral: Noise.KeyPair;
    readonly responderStatic: Noise.KeyPair;
    readonly responderEphemeral: Noise.KeyPair;
  },
  handshake: readonly [Uint8Array, Uint8Array],
  transport: ReadonlyArray<readonly [NoiseSide, Uint8Array]>,
): Case => {
  const initiator = new Noise.HandshakeState({
    initiator: true,
    prologue,
    s: keys.initiatorStatic,
    e: keys.initiatorEphemeral,
    rs: keys.responderStatic.publicKey,
  });
  const responder = new Noise.HandshakeState({
    initiator: false,
    prologue,
    s: keys.responderStatic,
    e: keys.responderEphemeral,
  });
  const one = initiator.writeMessage(handshake[0]);
  responder.readMessage(one.message);
  const two = responder.writeMessage(handshake[1]);
  const initiatorTransport = initiator.readMessage(two.message).transport;
  const responderTransport = two.transport;
  if (initiatorTransport === null || responderTransport === null) {
    throw new Error("the handshake did not finish");
  }
  return {
    name,
    inputs: {
      prologue: b64(prologue),
      ...keyInputs("initiatorStatic", keys.initiatorStatic),
      ...keyInputs("initiatorEphemeral", keys.initiatorEphemeral),
      ...keyInputs("responderStatic", keys.responderStatic),
      ...keyInputs("responderEphemeral", keys.responderEphemeral),
      // The handshake's two messages, then transport messages.
      messages: [
        ["initiator", handshake[0]] as const,
        ["responder", handshake[1]] as const,
        ...transport,
      ].map(([from, payload]) => ({ from, payload: b64(payload) })),
    },
    expected: {
      initiatorStaticPublicKey: b64(keys.initiatorStatic.publicKey),
      responderStaticPublicKey: b64(keys.responderStatic.publicKey),
      handshakeHash: b64(initiator.handshakeHash()),
      messages: [
        one.message,
        two.message,
        ...transport.map(([from, payload]) =>
          (from === "initiator"
            ? initiatorTransport
            : responderTransport
          ).send.encryptWithAd(EMPTY, payload),
        ),
      ].map(b64),
    },
  };
};

const noise = (): Case[] => [
  noiseCase(
    "payloads in both handshake messages",
    utf8("colibrii noise vectors"),
    {
      initiatorStatic: key(0x10),
      initiatorEphemeral: key(0x30),
      responderStatic: key(0x50),
      responderEphemeral: key(0x70),
    },
    [utf8("the first handshake message"), utf8("the second handshake message")],
    [
      ["initiator", utf8("one")],
      ["initiator", utf8("two")],
      ["responder", utf8("three")],
      ["responder", EMPTY],
      ["initiator", bytes(0, 256)],
    ],
  ),
  noiseCase(
    "empty payloads and prologue",
    EMPTY,
    {
      initiatorStatic: key(0x90),
      initiatorEphemeral: key(0xb0),
      responderStatic: key(0xd0),
      responderEphemeral: key(0xf0),
    },
    [EMPTY, EMPTY],
    [
      ["responder", utf8("the responder first")],
      ["initiator", utf8("then the initiator")],
    ],
  ),
];

// ---- relay.json ----

const deviceKeyA = key(0x21);
const deviceKeyB = key(0x41);
const connectionId = "5c1e9a7d3b2f4e6a8c0d1f3b5a7c9e2d";
const linkTicket = (from: number) =>
  `${Link.LINK_TICKET_PREFIX}${b64(bytes(from, 16))}`;

const relayCase = (
  name: string,
  options: {
    readonly asker: string;
    readonly askerKey: Noise.KeyPair;
    readonly asked: string;
    readonly askedKey: Noise.KeyPair;
    readonly askerEphemeral: Noise.KeyPair;
    readonly askedEphemeral: Noise.KeyPair;
    readonly id: string;
    // As sent, and so as sealed.
    readonly ask: unknown;
    readonly answer: unknown;
  },
): Case => {
  const askPayload = utf8(JSON.stringify(options.ask));
  const answerPayload = utf8(JSON.stringify(options.answer));
  const { sealed, handshake } = Relay.sealAsk({
    asker: options.asker,
    asked: options.asked,
    key: options.askerKey,
    askedKey: options.askedKey.publicKey,
    payload: askPayload,
    e: options.askerEphemeral,
  });
  const opened = Relay.openAsk({
    asker: options.asker,
    asked: options.asked,
    key: options.askedKey,
    sealed,
    e: options.askedEphemeral,
  }).pipe(Option.getOrThrow);
  const answer = Relay.sealAnswer(opened.handshake, answerPayload);
  const read = Relay.openAnswer(handshake, answer).pipe(Option.getOrThrow);
  return {
    name,
    inputs: {
      asker: options.asker,
      asked: options.asked,
      ...keyInputs("askerStatic", options.askerKey),
      ...keyInputs("askerEphemeral", options.askerEphemeral),
      ...keyInputs("askedStatic", options.askedKey),
      ...keyInputs("askedEphemeral", options.askedEphemeral),
      id: options.id,
      askPayload: b64(askPayload),
      answerPayload: b64(answerPayload),
    },
    expected: {
      askerStaticPublicKey: b64(options.askerKey.publicKey),
      askedStaticPublicKey: b64(options.askedKey.publicKey),
      prologue: new TextDecoder().decode(
        Relay.relayPrologue(options.asker, options.asked),
      ),
      askFrame: Relay.encodeRelayFrame({ kind: "ask", id: options.id, sealed }),
      answerFrame: Relay.encodeRelayFrame({
        kind: "answer",
        id: options.id,
        sealed: answer,
      }),
      handshakeHash: b64(opened.handshakeHash),
      // As the reader reads them.
      ask: Schema.encodeSync(Relay.ConnectInfoAsk)(
        Schema.decodeUnknownSync(Relay.ConnectInfoAsk)(opened.ask),
      ),
      answer: Schema.encodeSync(Relay.Answer)(read),
    },
  };
};

const relay = (): Case[] => [
  relayCase("connectInfo answered with lan and tunnel candidates", {
    asker: deviceA,
    askerKey: deviceKeyA,
    asked: deviceB,
    askedKey: deviceKeyB,
    askerEphemeral: key(0x61),
    askedEphemeral: key(0x81),
    id: "4096",
    ask: {
      ask: "connectInfo",
      input: { dialableKinds: ["lan", "tunnel"], connectionId },
    },
    answer: {
      ok: true,
      result: {
        available: true,
        candidates: [
          {
            kind: "lan",
            url: "ws://192.168.1.20:41734",
            ticket: linkTicket(0x01),
          },
          {
            kind: "lan",
            url: "ws://[2001:db8::20]:41734",
            ticket: linkTicket(0x11),
          },
          {
            kind: "tunnel",
            url: "wss://4f1c0d9e2b7a6c3e8d5f0a1b2c3d4e5f.example.link",
            ticket: linkTicket(0x21),
          },
        ],
      },
    },
  }),
  relayCase("connectInfo from a web page, answered with the tunnel", {
    asker: deviceB,
    askerKey: deviceKeyB,
    asked: deviceA,
    askedKey: deviceKeyA,
    askerEphemeral: key(0xa1),
    askedEphemeral: key(0xc1),
    id: "9007199254740993",
    ask: {
      ask: "connectInfo",
      input: { dialableKinds: ["tunnel"], connectionId },
    },
    answer: {
      ok: true,
      result: {
        available: true,
        candidates: [
          {
            kind: "tunnel",
            url: "wss://4f1c0d9e2b7a6c3e8d5f0a1b2c3d4e5f.example.link",
            ticket: linkTicket(0x31),
          },
        ],
      },
    },
  }),
  relayCase("connectInfo answered with nothing available", {
    asker: deviceB,
    askerKey: deviceKeyB,
    asked: deviceA,
    askedKey: deviceKeyA,
    askerEphemeral: key(0xe1),
    askedEphemeral: key(0x01),
    id: "0",
    ask: {
      ask: "connectInfo",
      input: { dialableKinds: ["tunnel"], connectionId },
    },
    answer: { ok: true, result: { available: false } },
  }),
  relayCase("connectInfo refused by a device that never listens", {
    asker: deviceA,
    askerKey: deviceKeyA,
    asked: deviceB,
    askedKey: deviceKeyB,
    askerEphemeral: key(0x63),
    askedEphemeral: key(0x83),
    id: "0000000000000017",
    ask: {
      ask: "connectInfo",
      input: { dialableKinds: ["lan", "tunnel"], connectionId },
    },
    answer: { ok: false, code: "no-listener" },
  }),
  relayCase("connectInfo naming a kind the asked device does not know", {
    asker: deviceA,
    askerKey: deviceKeyA,
    asked: deviceB,
    askedKey: deviceKeyB,
    askerEphemeral: key(0x67),
    askedEphemeral: key(0x87),
    id: "52",
    ask: {
      ask: "connectInfo",
      input: {
        dialableKinds: ["lan", "carrier-pigeon", "tunnel"],
        connectionId,
        since: 2,
      },
    },
    answer: {
      ok: true,
      result: {
        available: true,
        candidates: [
          {
            kind: "tunnel",
            url: "wss://4f1c0d9e2b7a6c3e8d5f0a1b2c3d4e5f.example.link",
            ticket: linkTicket(0x41),
          },
        ],
      },
    },
  }),
  relayCase(
    "connectInfo answered with a candidate of a kind the asker does not know",
    {
      asker: deviceA,
      askerKey: deviceKeyA,
      asked: deviceB,
      askedKey: deviceKeyB,
      askerEphemeral: key(0x69),
      askedEphemeral: key(0x89),
      id: "53",
      ask: {
        ask: "connectInfo",
        input: { dialableKinds: ["lan", "tunnel"], connectionId },
      },
      answer: {
        ok: true,
        result: {
          available: true,
          candidates: [
            {
              kind: "lan",
              url: "ws://192.168.1.20:41734",
              ticket: linkTicket(0x51),
            },
            { kind: "carrier-pigeon", url: "coo://192.168.1.20", loft: 3 },
            {
              kind: "tunnel",
              url: "wss://4f1c0d9e2b7a6c3e8d5f0a1b2c3d4e5f.example.link",
              ticket: linkTicket(0x61),
              since: 2,
            },
          ],
        },
      },
    },
  ),
  relayCase("connectInfo refused with a code the asker does not know", {
    asker: deviceB,
    askerKey: deviceKeyB,
    asked: deviceA,
    askedKey: deviceKeyA,
    askerEphemeral: key(0xa7),
    askedEphemeral: key(0xc7),
    id: "54",
    ask: {
      ask: "connectInfo",
      input: { dialableKinds: ["tunnel"], connectionId },
    },
    answer: { ok: false, code: "busy" },
  }),
];

// ---- link-handshake.json and link-transport.json ----

interface LinkKeys {
  readonly ticket: string;
  readonly dialerId: string;
  readonly dialer: Noise.KeyPair;
  readonly dialerEphemeral: Noise.KeyPair;
  readonly listener: Noise.KeyPair;
  readonly listenerEphemeral: Noise.KeyPair;
}

const desktopDials: LinkKeys = {
  ticket: linkTicket(0x01),
  dialerId: deviceA,
  dialer: deviceKeyA,
  dialerEphemeral: key(0x65),
  listener: deviceKeyB,
  listenerEphemeral: key(0x85),
};

const browserDials: LinkKeys = {
  ticket: linkTicket(0x31),
  dialerId: deviceB,
  dialer: deviceKeyB,
  dialerEphemeral: key(0xa5),
  listener: deviceKeyA,
  listenerEphemeral: key(0xc5),
};

const linkKeyInputs = (keys: LinkKeys) => ({
  ticket: keys.ticket,
  ...keyInputs("dialerStatic", keys.dialer),
  ...keyInputs("dialerEphemeral", keys.dialerEphemeral),
  ...keyInputs("listenerStatic", keys.listener),
  ...keyInputs("listenerEphemeral", keys.listenerEphemeral),
});

const linkHandshake = (keys: LinkKeys) => {
  const { first, handshake } = Link.startHandshake({
    ticket: keys.ticket,
    key: keys.dialer,
    listenerKey: keys.listener.publicKey,
    e: keys.dialerEphemeral,
  });
  const parsed = Link.decodeFirstMessage(first).pipe(Option.getOrThrow);
  const answered = Link.answerHandshake({
    ticket: parsed.ticket,
    message: parsed.message,
    key: keys.listener,
    e: keys.listenerEphemeral,
  }).pipe(Option.getOrThrow);
  const dialer = Link.finishHandshake(handshake, answered.second).pipe(
    Option.getOrThrow,
  );
  return { first, answered, dialer, listener: answered.transport };
};

const linkHandshakes = (): Case[] =>
  [desktopDials, browserDials].map((keys) => {
    const { first, answered } = linkHandshake(keys);
    return {
      name: `dialing with ${keys.ticket}`,
      inputs: linkKeyInputs(keys),
      expected: {
        dialerStaticPublicKey: b64(keys.dialer.publicKey),
        listenerStaticPublicKey: b64(keys.listener.publicKey),
        prologue: new TextDecoder().decode(Link.linkPrologue(keys.ticket)),
        firstMessage: b64(first),
        secondMessage: b64(answered.second),
        handshakeHash: b64(answered.handshakeHash),
      },
    };
  });

export type LinkSide = "dialer" | "listener";

// What a side sends: a transport message, or an app message, which is
// sent as its pieces.
type LinkSend =
  | {
      readonly from: LinkSide;
      readonly message: Exclude<
        Link.TransportMessage,
        { readonly type: "data" }
      >;
    }
  | { readonly from: LinkSide; readonly app: Uint8Array };

const linkTransportCase = (
  name: string,
  keys: LinkKeys,
  sends: ReadonlyArray<LinkSend>,
): Case => {
  const { dialer, listener } = linkHandshake(keys);
  const transports = { dialer, listener };
  return {
    name,
    inputs: {
      ...linkKeyInputs(keys),
      messages: sends.map((send) => {
        if ("app" in send) {
          return { from: send.from, type: "app", body: b64(send.app) };
        }
        const plaintext = Link.encodeTransportMessage(send.message);
        return plaintext.length === 1
          ? { from: send.from, type: send.message.type }
          : {
              from: send.from,
              type: send.message.type,
              body: b64(plaintext.subarray(1)),
            };
      }),
    },
    expected: {
      messages: sends.map((send) => ({
        frames: ("app" in send
          ? Link.splitAppMessage(send.app)
          : [send.message]
        ).map((message) => ({
          plaintext: b64(Link.encodeTransportMessage(message)),
          ciphertext: b64(
            Link.sealTransportMessage(transports[send.from], message),
          ),
        })),
      })),
    },
  };
};

const appHello = utf8("inkwell/3");

const hello = (keys: LinkKeys, appHelloBytes: Uint8Array): LinkSend => ({
  from: "dialer",
  message: {
    type: "hello",
    hello: {
      deviceId: keys.dialerId,
      connectionId,
      appHello: appHelloBytes,
    },
  },
});

const linkTransport = (): Case[] => [
  linkTransportCase("a welcomed link", desktopDials, [
    hello(desktopDials, appHello),
    {
      from: "listener",
      message: { type: "welcome", welcome: { appWelcome: appHello } },
    },
    { from: "dialer", message: { type: "ping" } },
    { from: "listener", message: { type: "pong" } },
    { from: "dialer", app: bytes(0, Link.MAX_PIECE_BYTES + 2) },
    { from: "listener", app: utf8("received") },
    { from: "listener", app: EMPTY },
    { from: "listener", message: { type: "ping" } },
    { from: "dialer", message: { type: "pong" } },
  ]),
  linkTransportCase("a link refused by Colibrii", browserDials, [
    hello(browserDials, appHello),
    {
      from: "listener",
      message: {
        type: "refuse",
        refuse: { by: "colibrii", reason: "the ticket expired" },
      },
    },
  ]),
  linkTransportCase("a link refused by the app", desktopDials, [
    hello(desktopDials, EMPTY),
    {
      from: "listener",
      message: {
        type: "refuse",
        refuse: {
          by: "app",
          reason: "a version mismatch",
          appRefusal: utf8("inkwell/2 < inkwell/3"),
        },
      },
    },
  ]),
];

// A hello, a welcome and a refuse as a later version may send them, and
// as a reader reads them: by their plaintext, since the reading is the point.
const linkReads = (): Case[] =>
  (
    [
      [
        "hello",
        Link.TYPE_HELLO,
        {
          deviceId: deviceA,
          connectionId,
          appHello: b64(appHello),
          supports: ["carrier-pigeon"],
        },
      ],
      [
        "welcome",
        Link.TYPE_WELCOME,
        { appWelcome: b64(appHello), supports: ["carrier-pigeon"] },
      ],
      [
        "refuse",
        Link.TYPE_REFUSE,
        { by: "colibrii", reason: "not in the roster", retryAfter: 30 },
      ],
    ] as const
  ).map(([type, byte, json]) => ({
    name: `a ${type} with a field the reader does not know`,
    inputs: { type, json },
    expected: { json: readLinkJson(byte, json) },
  }));

// The body of the message a reader reads from a JSON body, encoded again.
export const readLinkJson = (type: number, json: unknown): unknown => {
  const message = Link.decodeTransportMessage(
    Noise.concat(Uint8Array.of(type), utf8(JSON.stringify(json))),
  ).pipe(Option.getOrThrow);
  return JSON.parse(
    new TextDecoder().decode(Link.encodeTransportMessage(message).subarray(1)),
  );
};

export const buildVectors = async (): Promise<Record<string, Case[]>> => ({
  "base64url.json": base64url(),
  "noise.json": noise(),
  "relay.json": relay(),
  "link-handshake.json": linkHandshakes(),
  "link-transport.json": [...linkTransport(), ...linkReads()],
  "hub.json": hub(),
  "tunnel-label.json": await tunnelLabels(),
  "credential-hash.json": await credentialHashes(),
});

export const VECTORS_DIR = join(import.meta.dirname, "../../../spec/vectors");

if (process.argv.includes("--write")) {
  for (const [file, cases] of Object.entries(await buildVectors())) {
    writeFileSync(
      join(VECTORS_DIR, file),
      `${JSON.stringify(cases, null, 2)}\n`,
    );
  }
}
