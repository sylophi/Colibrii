import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { afterAll, expect, it } from "vitest";
import { CredentialStore } from "../src/index.ts";
import { layerEncryptedFileStore, layerFileStore } from "../src/node/index.ts";

const dir = await mkdtemp(join(tmpdir(), "colibrii-store-"));
afterAll(() => rm(dir, { recursive: true, force: true }));

const roundTrip = CredentialStore.use((store) =>
  Effect.gen(function* () {
    const before = yield* store.read;
    yield* store.write("first");
    yield* store.write("second");
    const after = yield* store.read;
    yield* store.remove;
    return { before, after, removed: yield* store.read };
  }),
);

it.runIf(process.platform !== "win32")(
  "keeps the file readable only by its owner",
  async () => {
    const path = join(dir, "nested", "device.json");
    await Effect.runPromise(
      CredentialStore.use((store) => store.write("{}")).pipe(
        Effect.provide(layerFileStore(path)),
      ),
    );
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect((await stat(join(dir, "nested"))).mode & 0o777).toBe(0o700);
  },
);

it("reads what it wrote last, and nothing once removed", async () => {
  const result = await Effect.runPromise(
    roundTrip.pipe(Effect.provide(layerFileStore(join(dir, "plain.json")))),
  );
  expect(result).toEqual({
    before: Option.none(),
    after: Option.some("second"),
    removed: Option.none(),
  });
});

const flip = (data: Uint8Array) => data.map((byte) => byte ^ 0xff);

it("writes only what the cipher makes of the text", async () => {
  const path = join(dir, "sealed.bin");
  const layer = layerEncryptedFileStore(path, {
    encrypt: (text) => flip(new TextEncoder().encode(text)),
    decrypt: async (data) => new TextDecoder().decode(flip(data)),
  });
  const read = await Effect.runPromise(
    CredentialStore.use((store) =>
      store.write("secret").pipe(Effect.andThen(store.read)),
    ).pipe(Effect.provide(layer)),
  );
  expect(read).toEqual(Option.some("secret"));
  expect((await readFile(path)).includes("secret")).toBe(false);
});
