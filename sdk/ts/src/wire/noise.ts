// The Noise Protocol Framework (revision 34, noiseprotocol.org/noise.html)
// as the one protocol the wire uses: Noise_IK_25519_ChaChaPoly_SHA256,
// without extensions. The objects are the specification's CipherState,
// SymmetricState and HandshakeState (its section 5), with its method
// names, and the comments cite its sections. The primitives come from
// the noble libraries.
//
// IK (section 7.5): the initiator knows the responder's static key
// before it starts, and sends its own, encrypted, in the first message.
// Two messages, after which each side holds a CipherState per
// direction. The responder reads the initiator's static key off the
// handshake (remoteStaticKey), and checking it is the caller's job.
import { chacha20poly1305 } from "@noble/ciphers/chacha.js";
import { x25519 } from "@noble/curves/ed25519.js";
import { hkdf } from "@noble/hashes/hkdf.js";
import { sha256 } from "@noble/hashes/sha2.js";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const PROTOCOL_NAME = "Noise_IK_25519_ChaChaPoly_SHA256";

// Section 4: DHLEN for 25519, HASHLEN for SHA256, and the AEAD tag.
const DHLEN = 32;
const HASHLEN = 32;
export const TAG_BYTES = 16;

// Section 3.
export const MAX_MESSAGE_BYTES = 65_535;

// Section 5.1 reserves 2^64-1. A counter never gets near the largest
// integer a double holds exactly, which is the bound kept.
const MAX_NONCE = Number.MAX_SAFE_INTEGER;

const EMPTY = new Uint8Array(0);

// A handshake or transport message that cannot go on. The caller drops
// the connection, and tells the far end nothing about why.
export class NoiseError extends Schema.TaggedError<NoiseError>()("NoiseError", {
  reason: Schema.Literals(["authentication", "length", "turn", "nonce", "key"]),
}) {
  override get message(): string {
    switch (this.reason) {
      case "authentication":
        return "A Noise message failed authentication.";
      case "length":
        return "A Noise message has the wrong length.";
      case "turn":
        return "A Noise handshake message came out of turn.";
      case "nonce":
        return "A Noise cipher ran out of nonces.";
      case "key":
        return "A Noise public key is not usable.";
    }
  }
}

export interface KeyPair {
  readonly publicKey: Uint8Array;
  readonly privateKey: Uint8Array;
}

// ---- Section 4.1: DH functions ----

// GENERATE_KEYPAIR().
export const generateKeyPair = (): KeyPair => {
  const { secretKey, publicKey } = x25519.keygen();
  return { publicKey, privateKey: secretKey };
};

export const keyPairFromPrivateKey = (privateKey: Uint8Array): KeyPair => ({
  publicKey: x25519.getPublicKey(privateKey),
  privateKey,
});

// DH(key_pair, public_key). noble refuses a public key whose result is
// all zeros (a low-order point), which section 12.1 allows.
const dh = (keyPair: KeyPair, publicKey: Uint8Array): Uint8Array => {
  try {
    return x25519.getSharedSecret(keyPair.privateKey, publicKey);
  } catch {
    throw new NoiseError({ reason: "key" });
  }
};

// ---- Section 4.2: cipher functions ----

// ChaChaPoly's 96-bit nonce: 32 bits of zeros, then n as a 64-bit
// little-endian integer.
const nonceBytes = (n: number): Uint8Array => {
  const nonce = new Uint8Array(12);
  const view = new DataView(nonce.buffer);
  view.setUint32(4, n % 2 ** 32, true);
  view.setUint32(8, Math.floor(n / 2 ** 32), true);
  return nonce;
};

// ---- Section 4.3: hash functions ----

// HKDF(chaining_key, input_key_material, 2): RFC 5869's HKDF with the
// chaining key as the salt and no info gives output1 and output2 as its
// first 64 bytes.
const hkdf2 = (
  chainingKey: Uint8Array,
  inputKeyMaterial: Uint8Array,
): [Uint8Array, Uint8Array] => {
  const output = hkdf(
    sha256,
    inputKeyMaterial,
    chainingKey,
    EMPTY,
    2 * HASHLEN,
  );
  return [output.slice(0, HASHLEN), output.slice(HASHLEN)];
};

export const concat = (...parts: ReadonlyArray<Uint8Array>): Uint8Array => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

// ---- Section 5.1: CipherState ----

export class CipherState {
  private k: Uint8Array | null = null;
  private n = 0;

  initializeKey(key: Uint8Array): void {
    this.k = key;
    this.n = 0;
  }

  hasKey(): boolean {
    return this.k !== null;
  }

  encryptWithAd(ad: Uint8Array, plaintext: Uint8Array): Uint8Array {
    if (this.k === null) return plaintext;
    if (plaintext.length + TAG_BYTES > MAX_MESSAGE_BYTES) {
      throw new NoiseError({ reason: "length" });
    }
    if (this.n >= MAX_NONCE) throw new NoiseError({ reason: "nonce" });
    const ciphertext = chacha20poly1305(this.k, nonceBytes(this.n), ad).encrypt(
      plaintext,
    );
    this.n += 1;
    return ciphertext;
  }

  // A failed decryption leaves n where it was (section 5.1).
  decryptWithAd(ad: Uint8Array, ciphertext: Uint8Array): Uint8Array {
    if (this.k === null) return ciphertext;
    if (ciphertext.length > MAX_MESSAGE_BYTES) {
      throw new NoiseError({ reason: "length" });
    }
    if (this.n >= MAX_NONCE) throw new NoiseError({ reason: "nonce" });
    let plaintext: Uint8Array;
    try {
      plaintext = chacha20poly1305(this.k, nonceBytes(this.n), ad).decrypt(
        ciphertext,
      );
    } catch {
      throw new NoiseError({ reason: "authentication" });
    }
    this.n += 1;
    return plaintext;
  }
}

// ---- Section 5.2: SymmetricState ----

class SymmetricState {
  private readonly cipherState = new CipherState();
  private ck: Uint8Array;
  private h: Uint8Array;

  // InitializeSymmetric(protocol_name). The name is exactly HASHLEN
  // bytes, so it is h as it is.
  constructor() {
    this.h = new TextEncoder().encode(PROTOCOL_NAME);
    this.ck = this.h;
  }

  // HASHLEN is 32, so temp_k needs no truncation.
  mixKey(inputKeyMaterial: Uint8Array): void {
    const [ck, tempK] = hkdf2(this.ck, inputKeyMaterial);
    this.ck = ck;
    this.cipherState.initializeKey(tempK);
  }

  mixHash(data: Uint8Array): void {
    this.h = sha256(concat(this.h, data));
  }

  hasKey(): boolean {
    return this.cipherState.hasKey();
  }

  handshakeHash(): Uint8Array {
    return this.h;
  }

  encryptAndHash(plaintext: Uint8Array): Uint8Array {
    const ciphertext = this.cipherState.encryptWithAd(this.h, plaintext);
    this.mixHash(ciphertext);
    return ciphertext;
  }

  decryptAndHash(ciphertext: Uint8Array): Uint8Array {
    const plaintext = this.cipherState.decryptWithAd(this.h, ciphertext);
    this.mixHash(ciphertext);
    return plaintext;
  }

  split(): [CipherState, CipherState] {
    const [tempK1, tempK2] = hkdf2(this.ck, EMPTY);
    const c1 = new CipherState();
    const c2 = new CipherState();
    c1.initializeKey(tempK1);
    c2.initializeKey(tempK2);
    return [c1, c2];
  }
}

// ---- Section 7.5: the IK pattern ----
//
//   IK:
//     <- s
//     ...
//     -> e, es, s, ss
//     <- e, ee, se

type Token = "e" | "s" | "ee" | "es" | "se" | "ss";

const IK: ReadonlyArray<ReadonlyArray<Token>> = [
  ["e", "es", "s", "ss"],
  ["e", "ee", "se"],
];

// A finished handshake's ciphers for one side: Split's c1 is the
// initiator's sending cipher, c2 the responder's (section 5.3).
export interface Transport {
  readonly send: CipherState;
  readonly receive: CipherState;
}

// ---- Section 5.3: HandshakeState ----

export class HandshakeState {
  private readonly symmetricState = new SymmetricState();
  private readonly initiator: boolean;
  private readonly s: KeyPair;
  private e: KeyPair | null;
  private rs: Uint8Array | null;
  private re: Uint8Array | null = null;
  // The index of the next message pattern. The initiator writes the
  // even ones.
  private next = 0;

  // Initialize(handshake_pattern, initiator, prologue, s, e, rs, re).
  // The initiator passes rs, the responder's static public key. Only
  // the test vectors pass e.
  constructor(
    options:
      | {
          readonly initiator: true;
          readonly prologue: Uint8Array;
          readonly s: KeyPair;
          readonly rs: Uint8Array;
          readonly e?: KeyPair;
        }
      | {
          readonly initiator: false;
          readonly prologue: Uint8Array;
          readonly s: KeyPair;
          readonly e?: KeyPair;
        },
  ) {
    this.initiator = options.initiator;
    this.s = options.s;
    this.e = options.e ?? null;
    this.rs = options.initiator ? options.rs : null;
    this.symmetricState.mixHash(options.prologue);
    // The pre-message "<- s".
    this.symmetricState.mixHash(
      options.initiator ? options.rs : options.s.publicKey,
    );
  }

  // The far end's static public key, which the responder learns from
  // the first message.
  remoteStaticKey(): Uint8Array | null {
    return this.rs;
  }

  handshakeHash(): Uint8Array {
    return this.symmetricState.handshakeHash();
  }

  // WriteMessage(payload, message_buffer), with this side's transport
  // after the last message.
  writeMessage(payload: Uint8Array): {
    readonly message: Uint8Array;
    readonly transport: Transport | null;
  } {
    const parts: Array<Uint8Array> = [];
    for (const token of this.takePattern(true)) {
      if (token === "e") {
        this.e ??= generateKeyPair();
        parts.push(this.e.publicKey);
        this.symmetricState.mixHash(this.e.publicKey);
      } else if (token === "s") {
        parts.push(this.symmetricState.encryptAndHash(this.s.publicKey));
      } else {
        this.symmetricState.mixKey(this.dhFor(token));
      }
    }
    parts.push(this.symmetricState.encryptAndHash(payload));
    const message = concat(...parts);
    if (message.length > MAX_MESSAGE_BYTES) {
      throw new NoiseError({ reason: "length" });
    }
    return { message, transport: this.transportIfDone() };
  }

  // ReadMessage(message, payload_buffer), with this side's transport
  // after the last message.
  readMessage(message: Uint8Array): {
    readonly payload: Uint8Array;
    readonly transport: Transport | null;
  } {
    if (message.length > MAX_MESSAGE_BYTES) {
      throw new NoiseError({ reason: "length" });
    }
    let offset = 0;
    const take = (length: number): Uint8Array => {
      if (offset + length > message.length) {
        throw new NoiseError({ reason: "length" });
      }
      const bytes = message.slice(offset, offset + length);
      offset += length;
      return bytes;
    };
    for (const token of this.takePattern(false)) {
      if (token === "e") {
        this.re = take(DHLEN);
        this.symmetricState.mixHash(this.re);
      } else if (token === "s") {
        this.rs = this.symmetricState.decryptAndHash(
          take(this.symmetricState.hasKey() ? DHLEN + TAG_BYTES : DHLEN),
        );
      } else {
        this.symmetricState.mixKey(this.dhFor(token));
      }
    }
    const payload = this.symmetricState.decryptAndHash(
      message.subarray(offset),
    );
    return { payload, transport: this.transportIfDone() };
  }

  private takePattern(writing: boolean): ReadonlyArray<Token> {
    const pattern = IK[this.next];
    if (
      pattern === undefined ||
      writing !== ((this.next % 2 === 0) === this.initiator)
    ) {
      throw new NoiseError({ reason: "turn" });
    }
    this.next += 1;
    return pattern;
  }

  // The DH a token names, from this side: "es" is the initiator's e
  // with the responder's s, so the initiator computes DH(e, rs) and the
  // responder DH(s, re).
  private dhFor(token: "ee" | "es" | "se" | "ss"): Uint8Array {
    const need = <T>(value: T | null): T => {
      if (value === null) throw new NoiseError({ reason: "turn" });
      return value;
    };
    switch (token) {
      case "ee":
        return dh(need(this.e), need(this.re));
      case "es":
        return this.initiator
          ? dh(need(this.e), need(this.rs))
          : dh(this.s, need(this.re));
      case "se":
        return this.initiator
          ? dh(this.s, need(this.re))
          : dh(need(this.e), need(this.rs));
      case "ss":
        return dh(this.s, need(this.rs));
    }
  }

  private transportIfDone(): Transport | null {
    if (this.next < IK.length) return null;
    const [c1, c2] = this.symmetricState.split();
    return this.initiator
      ? { send: c1, receive: c2 }
      : { send: c2, receive: c1 };
  }
}

// What f answers, or nothing when a Noise message does not open.
export const tryNoise = <A>(f: () => A): Option.Option<A> => {
  try {
    return Option.some(f());
  } catch (error) {
    if (error instanceof NoiseError) return Option.none();
    throw error;
  }
};
