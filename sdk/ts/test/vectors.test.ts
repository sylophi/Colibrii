import { readFileSync } from "node:fs";
import { join } from "node:path";
import * as Hex from "effect/encoding/Hex";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vitest";
import {
  buildVectors,
  b64,
  type Case,
  EMPTY,
  errorSamples,
  fromHex,
  hubSamples,
  readLinkJson,
  readSamples,
  VECTORS_DIR,
} from "../scripts/vectors.ts";
import * as Base64Url from "../src/wire/base64url.ts";
import { credentialHash } from "../src/wire/credential.ts";
import * as Link from "../src/wire/link.ts";
import * as Noise from "../src/wire/noise.ts";
import * as Relay from "../src/wire/relay.ts";
import { tunnelLabel } from "../src/wire/tunnel.ts";

const read = (file: string): Case[] =>
  JSON.parse(readFileSync(join(VECTORS_DIR, file), "utf8"));

const bytes = (value: unknown) =>
  Option.getOrThrow(Base64Url.decode(String(value)));

const keyPair = (value: unknown) => Noise.keyPairFromPrivateKey(bytes(value));

const list = <A>(value: unknown) => value as ReadonlyArray<A>;

describe("vectors", () => {
  it("are what the wire builds", async () => {
    for (const [file, cases] of Object.entries(await buildVectors())) {
      expect(read(file), file).toEqual(cases);
    }
  });

  it("base64url", () => {
    for (const { name, inputs, expected } of read("base64url.json")) {
      if (typeof inputs["hex"] === "string") {
        expect(Base64Url.encode(fromHex(inputs["hex"])), name).toBe(
          expected["base64url"],
        );
      } else {
        const decoded = Base64Url.decode(String(inputs["base64url"]));
        expect(Option.map(decoded, Hex.encode), name).toEqual(
          expected["invalid"] === true
            ? Option.none()
            : Option.some(expected["hex"]),
        );
      }
    }
  });

  it("hub.json reads back as it was written", () => {
    const schemas = new Map<string, Schema.Codec<unknown, unknown>>([
      ...[...hubSamples, ...readSamples].map(
        ({ name, schema }) => [name, schema] as const,
      ),
      ...errorSamples.map(
        ({ schema, value }) =>
          [`error: ${(value as { _tag: string })._tag}`, schema] as const,
      ),
    ]);
    for (const { name, inputs, expected } of read("hub.json")) {
      if (expected["json"] === undefined) continue;
      const schema = schemas.get(name);
      if (schema === undefined) throw new Error(`no schema for ${name}`);
      // A sample a later version may send is read as `expected` says.
      for (const json of [inputs["json"], expected["json"]]) {
        if (json === undefined) continue;
        const decoded = Schema.decodeSync(schema)(json);
        expect(Schema.encodeUnknownSync(schema)(decoded), name).toEqual(
          expected["json"],
        );
      }
    }
  });

  it("tunnel-label.json", async () => {
    for (const { name, inputs, expected } of read("tunnel-label.json")) {
      expect(
        await tunnelLabel(
          String(inputs["app"]),
          String(inputs["account"]),
          String(inputs["device"]),
        ),
        name,
      ).toBe(expected["label"]);
    }
  });

  it("credential-hash.json", async () => {
    for (const { name, inputs, expected } of read("credential-hash.json")) {
      expect(await credentialHash(String(inputs["credential"])), name).toBe(
        expected["sha256"],
      );
    }
  });

  it("noise.json", () => {
    for (const { name, inputs, expected } of read("noise.json")) {
      const responderStatic = keyPair(inputs["responderStaticPrivateKey"]);
      const initiatorStatic = keyPair(inputs["initiatorStaticPrivateKey"]);
      const initiator = new Noise.HandshakeState({
        initiator: true,
        prologue: bytes(inputs["prologue"]),
        s: initiatorStatic,
        e: keyPair(inputs["initiatorEphemeralPrivateKey"]),
        rs: responderStatic.publicKey,
      });
      const responder = new Noise.HandshakeState({
        initiator: false,
        prologue: bytes(inputs["prologue"]),
        s: responderStatic,
        e: keyPair(inputs["responderEphemeralPrivateKey"]),
      });
      const messages = list<{ from: string; payload: string }>(
        inputs["messages"],
      );
      const [first, second, ...rest] = messages.map((m) => bytes(m.payload));
      if (first === undefined || second === undefined) throw new Error(name);
      const one = initiator.writeMessage(first);
      const read1 = responder.readMessage(one.message);
      const two = responder.writeMessage(second);
      const read2 = initiator.readMessage(two.message);
      const sides = {
        initiator: [read2.transport, two.transport],
        responder: [two.transport, read2.transport],
      } as const;
      const sent = [
        one.message,
        two.message,
        ...rest.map((payload, i) => {
          const [writer, reader] =
            messages[i + 2]?.from === "initiator"
              ? sides.initiator
              : sides.responder;
          if (writer == null || reader == null) throw new Error(name);
          const ciphertext = writer.send.encryptWithAd(EMPTY, payload);
          expect(b64(reader.receive.decryptWithAd(EMPTY, ciphertext))).toBe(
            b64(payload),
          );
          return ciphertext;
        }),
      ];
      expect([read1.payload, read2.payload].map(b64), name).toEqual(
        [first, second].map(b64),
      );
      expect(b64(initiatorStatic.publicKey), name).toBe(
        expected["initiatorStaticPublicKey"],
      );
      expect(b64(responderStatic.publicKey), name).toBe(
        expected["responderStaticPublicKey"],
      );
      expect(sent.map(b64), name).toEqual(expected["messages"]);
      expect(b64(initiator.handshakeHash()), name).toBe(
        expected["handshakeHash"],
      );
      expect(b64(responder.handshakeHash()), name).toBe(
        expected["handshakeHash"],
      );
    }
  });

  it("relay.json", () => {
    for (const { name, inputs, expected } of read("relay.json")) {
      const asker = String(inputs["asker"]);
      const asked = String(inputs["asked"]);
      const askerKey = keyPair(inputs["askerStaticPrivateKey"]);
      const askedKey = keyPair(inputs["askedStaticPrivateKey"]);
      const id = String(inputs["id"]);
      const ask = Relay.sealAsk({
        asker,
        asked,
        key: askerKey,
        askedKey: askedKey.publicKey,
        payload: bytes(inputs["askPayload"]),
        e: keyPair(inputs["askerEphemeralPrivateKey"]),
      });
      expect(
        Relay.encodeRelayFrame({ kind: "ask", id, sealed: ask.sealed }),
        name,
      ).toBe(expected["askFrame"]);

      const askFrame = Option.getOrThrow(
        Relay.decodeRelayFrame(String(expected["askFrame"])),
      );
      expect(askFrame.id, name).toBe(id);
      const open = (from: string, to: string) =>
        Relay.openAsk({
          asker: from,
          asked: to,
          key: askedKey,
          sealed: askFrame.sealed,
          e: keyPair(inputs["askedEphemeralPrivateKey"]),
        });
      expect(Option.isNone(open(asked, asker)), name).toBe(true);
      const opened = Option.getOrThrow(open(asker, asked));
      expect(b64(opened.askerKey), name).toBe(expected["askerStaticPublicKey"]);
      expect(b64(opened.handshakeHash), name).toBe(expected["handshakeHash"]);
      expect(
        Schema.encodeSync(Relay.ConnectInfoAsk)(
          Schema.decodeUnknownSync(Relay.ConnectInfoAsk)(opened.ask),
        ),
        name,
      ).toEqual(expected["ask"]);

      const answer = Relay.sealAnswer(
        opened.handshake,
        bytes(inputs["answerPayload"]),
      );
      expect(
        Relay.encodeRelayFrame({ kind: "answer", id, sealed: answer }),
        name,
      ).toBe(expected["answerFrame"]);
      const answerFrame = Option.getOrThrow(
        Relay.decodeRelayFrame(String(expected["answerFrame"])),
      );
      expect(answerFrame, name).toEqual({ kind: "answer", id, sealed: answer });
      expect(
        Schema.encodeSync(Relay.Answer)(
          Option.getOrThrow(
            Relay.openAnswer(ask.handshake, answerFrame.sealed),
          ),
        ),
        name,
      ).toEqual(expected["answer"]);
    }
  });

  const linkHandshake = (inputs: Record<string, unknown>) => {
    const ticket = String(inputs["ticket"]);
    const listenerKey = keyPair(inputs["listenerStaticPrivateKey"]);
    const dialerKey = keyPair(inputs["dialerStaticPrivateKey"]);
    const { first, handshake } = Link.startHandshake({
      ticket,
      key: dialerKey,
      listenerKey: listenerKey.publicKey,
      e: keyPair(inputs["dialerEphemeralPrivateKey"]),
    });
    const parsed = Option.getOrThrow(Link.decodeFirstMessage(first));
    const answer = (asTicket: string) =>
      Link.answerHandshake({
        ticket: asTicket,
        message: parsed.message,
        key: listenerKey,
        e: keyPair(inputs["listenerEphemeralPrivateKey"]),
      });
    expect(parsed.ticket).toBe(ticket);
    expect(Option.isNone(answer(`${ticket}x`))).toBe(true);
    const answered = Option.getOrThrow(answer(parsed.ticket));
    const dialer = Option.getOrThrow(
      Link.finishHandshake(handshake, answered.second),
    );
    return { first, dialerKey, listenerKey, answered, dialer };
  };

  it("link-handshake.json", () => {
    for (const { name, inputs, expected } of read("link-handshake.json")) {
      const { first, dialerKey, listenerKey, answered } = linkHandshake(inputs);
      expect(
        {
          dialerStaticPublicKey: b64(dialerKey.publicKey),
          listenerStaticPublicKey: b64(listenerKey.publicKey),
          prologue: new TextDecoder().decode(
            Link.linkPrologue(String(inputs["ticket"])),
          ),
          firstMessage: b64(first),
          secondMessage: b64(answered.second),
          handshakeHash: b64(answered.handshakeHash),
        },
        name,
      ).toEqual(expected);
      expect(b64(answered.dialerKey), name).toBe(
        expected["dialerStaticPublicKey"],
      );
    }
  });

  it("link-transport.json", () => {
    const types: Record<string, number> = {
      hello: Link.TYPE_HELLO,
      welcome: Link.TYPE_WELCOME,
      refuse: Link.TYPE_REFUSE,
      ping: Link.TYPE_PING,
      pong: Link.TYPE_PONG,
    };
    for (const { name, inputs, expected } of read("link-transport.json")) {
      if (inputs["json"] !== undefined) {
        expect(
          readLinkJson(types[String(inputs["type"])] ?? -1, inputs["json"]),
          name,
        ).toEqual(expected["json"]);
        continue;
      }
      const { dialer, answered } = linkHandshake(inputs);
      const transports = { dialer, listener: answered.transport };
      const joiners = {
        dialer: new Link.AppMessageJoiner(),
        listener: new Link.AppMessageJoiner(),
      };
      const sends = list<{
        from: "dialer" | "listener";
        type: string;
        body?: string;
      }>(inputs["messages"]);
      const apps: Array<string> = [];
      const frames = sends.map(({ from, type, body }) => {
        const to = from === "dialer" ? "listener" : "dialer";
        const content = body === undefined ? EMPTY : bytes(body);
        const plaintexts =
          type === "app"
            ? Link.splitAppMessage(content).map(Link.encodeTransportMessage)
            : [Noise.concat(Uint8Array.of(types[type] ?? -1), content)];
        return {
          frames: plaintexts.map((plaintext) => {
            const message = Option.getOrThrow(
              Link.decodeTransportMessage(plaintext),
            );
            expect(b64(Link.encodeTransportMessage(message)), name).toBe(
              b64(plaintext),
            );
            const ciphertext = Link.sealTransportMessage(
              transports[from],
              message,
            );
            const received = Option.getOrThrow(
              Link.openTransportMessage(transports[to], ciphertext),
            );
            if (received.type === "data") {
              const joined = joiners[to].add(received);
              if (joined._tag === "Message") apps.push(b64(joined.message));
            } else {
              expect(received.type, name).toBe(type);
            }
            return { plaintext: b64(plaintext), ciphertext: b64(ciphertext) };
          }),
        };
      });
      expect(frames, name).toEqual(expected["messages"]);
      expect(apps, name).toEqual(
        sends.filter((send) => send.type === "app").map((send) => send.body),
      );
    }
  });
});
