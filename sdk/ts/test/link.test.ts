import { describe, expect, it } from "vitest";
import {
  AppMessageJoiner,
  MAX_APP_MESSAGE_BYTES,
  MAX_PIECE_BYTES,
  splitAppMessage,
} from "../src/wire/link.ts";

const pieceSizes = (length: number) =>
  splitAppMessage(new Uint8Array(length)).map((piece) => [
    piece.body.length,
    piece.more,
  ]);

describe("app messages", () => {
  it("split into pieces of at most the piece size", () => {
    expect(pieceSizes(0)).toEqual([[0, false]]);
    expect(pieceSizes(MAX_PIECE_BYTES)).toEqual([[MAX_PIECE_BYTES, false]]);
    expect(pieceSizes(MAX_PIECE_BYTES + 1)).toEqual([
      [MAX_PIECE_BYTES, true],
      [1, false],
    ]);
  });

  it("join up to the cap, and no further", () => {
    const joiner = new AppMessageJoiner();
    const lastJoined = (length: number) =>
      splitAppMessage(new Uint8Array(length))
        .map((piece) => joiner.add(piece)._tag)
        .at(-1);
    expect(lastJoined(MAX_APP_MESSAGE_BYTES)).toBe("Message");
    expect(lastJoined(MAX_APP_MESSAGE_BYTES + 1)).toBe("TooBig");
  });

  it("join a message of many small pieces", () => {
    const joiner = new AppMessageJoiner();
    const body = Uint8Array.of(7);
    for (let i = 1; i < 500_000; i += 1) {
      joiner.add({ more: true, body: i % 2 === 0 ? body : new Uint8Array() });
    }
    const joined = joiner.add({ more: false, body });
    expect(joined._tag === "Message" && joined.message).toEqual(
      new Uint8Array(250_000).fill(7),
    );
  });
});
