// The credential store as one file at a path the app gives, readable
// only by its owner. On Windows a file's mode does not limit who reads
// it, so an app there uses the encrypting store, with a cipher such as
// Electron's safeStorage.
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { CredentialStore, CredentialStoreError } from "../Platform.ts";

export interface Cipher {
  readonly encrypt: (text: string) => Uint8Array | Promise<Uint8Array>;
  readonly decrypt: (data: Buffer) => string | Promise<string>;
}

const plain: Cipher = {
  encrypt: (text) => Buffer.from(text, "utf8"),
  decrypt: (data) => data.toString("utf8"),
};

const make = (path: string, cipher: Cipher) =>
  CredentialStore.of({
    read: Effect.tryPromise({
      try: async () => {
        const data = await readFile(path).catch((error: unknown) => {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
          throw error;
        });
        return data === null
          ? Option.none()
          : Option.some(await cipher.decrypt(data));
      },
      catch: (cause) => new CredentialStoreError({ operation: "read", cause }),
    }),
    // Whole or not at all: a write cut short leaves the file before it.
    write: (text) =>
      Effect.tryPromise({
        try: async () => {
          await mkdir(dirname(path), { recursive: true, mode: 0o700 });
          await writeFile(`${path}.next`, await cipher.encrypt(text), {
            mode: 0o600,
          });
          await rename(`${path}.next`, path);
        },
        catch: (cause) =>
          new CredentialStoreError({ operation: "write", cause }),
      }),
    remove: Effect.tryPromise({
      try: () => rm(path, { force: true }),
      catch: (cause) =>
        new CredentialStoreError({ operation: "remove", cause }),
    }),
  });

export const layerFileStore = (path: string) =>
  Layer.succeed(CredentialStore, make(path, plain));

// The same file, its contents what `encrypt` makes of the document.
export const layerEncryptedFileStore = (path: string, cipher: Cipher) =>
  Layer.succeed(CredentialStore, make(path, cipher));
