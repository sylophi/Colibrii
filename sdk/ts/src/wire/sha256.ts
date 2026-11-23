import * as Hex from "effect/encoding/Hex";

export const sha256Hex = async (text: string): Promise<string> =>
  Hex.encode(
    new Uint8Array(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)),
    ),
  );
