// The Noise core against vectors from another implementation, so the
// spec's own vectors are not only consistent with themselves.
// cacophony.json is cacophony's vector for this protocol, where its
// `source` says.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import * as Hex from "effect/encoding/Hex";
import * as Result from "effect/Result";
import { describe, expect, it } from "vitest";
import {
  HandshakeState,
  keyPairFromPrivateKey,
  PROTOCOL_NAME,
} from "../src/wire/noise.ts";

interface CacophonyVector {
  readonly protocol_name: string;
  readonly init_prologue: string;
  readonly init_static: string;
  readonly init_ephemeral: string;
  readonly init_remote_static: string;
  readonly resp_prologue: string;
  readonly resp_static: string;
  readonly resp_ephemeral: string;
  readonly handshake_hash: string;
  readonly messages: ReadonlyArray<{
    readonly payload: string;
    readonly ciphertext: string;
  }>;
}

const { vectors } = JSON.parse(
  readFileSync(join(import.meta.dirname, "cacophony.json"), "utf8"),
) as { readonly vectors: ReadonlyArray<CacophonyVector> };

const fromHex = (text: string) => Result.getOrThrow(Hex.decode(text));

const EMPTY = new Uint8Array(0);

describe("noise", () => {
  it("has cacophony's vectors for its protocol", () => {
    expect(vectors.map((vector) => vector.protocol_name)).toEqual([
      PROTOCOL_NAME,
    ]);
  });

  it.each(vectors)("reproduces cacophony's $protocol_name", (vector) => {
    const initiator = new HandshakeState({
      initiator: true,
      prologue: fromHex(vector.init_prologue),
      s: keyPairFromPrivateKey(fromHex(vector.init_static)),
      e: keyPairFromPrivateKey(fromHex(vector.init_ephemeral)),
      rs: fromHex(vector.init_remote_static),
    });
    const responder = new HandshakeState({
      initiator: false,
      prologue: fromHex(vector.resp_prologue),
      s: keyPairFromPrivateKey(fromHex(vector.resp_static)),
      e: keyPairFromPrivateKey(fromHex(vector.resp_ephemeral)),
    });
    const [first, second, ...rest] = vector.messages;
    if (first === undefined || second === undefined) throw new Error("short");
    const one = initiator.writeMessage(fromHex(first.payload));
    expect(Hex.encode(one.message)).toBe(first.ciphertext);
    expect(Hex.encode(responder.readMessage(one.message).payload)).toBe(
      first.payload,
    );
    const two = responder.writeMessage(fromHex(second.payload));
    expect(Hex.encode(two.message)).toBe(second.ciphertext);
    const read = initiator.readMessage(two.message);
    expect(Hex.encode(read.payload)).toBe(second.payload);
    const sides = [read.transport, two.transport];
    // The two sides take turns, the initiator first.
    rest.forEach(({ payload, ciphertext }, i) => {
      const writer = sides[i % 2];
      const reader = sides[(i + 1) % 2];
      if (writer == null || reader == null) throw new Error("no transport");
      const sent = writer.send.encryptWithAd(EMPTY, fromHex(payload));
      expect(Hex.encode(sent), `transport message ${i}`).toBe(ciphertext);
      expect(
        Hex.encode(reader.receive.decryptWithAd(EMPTY, sent)),
        `transport message ${i}`,
      ).toBe(payload);
    });
    expect(Hex.encode(initiator.handshakeHash())).toBe(vector.handshake_hash);
    expect(Hex.encode(responder.handshakeHash())).toBe(vector.handshake_hash);
  });
});
