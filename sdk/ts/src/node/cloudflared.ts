// The cloudflared the SDK pins (spec/tunnels.md, "Running it"): one
// release, with its asset and the asset's SHA-256 for each system the
// SDK supports. `fetchCloudflared` is for an app's build step or first
// run, and the tunnel runner looks for its copy before the `PATH`.
import { createHash } from "node:crypto";
import { chmod, mkdir, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const CLOUDFLARED_VERSION = "2026.10.4";

// A Mac's asset is a .tgz holding the binary, the others the binary.
const ASSETS: Record<
  string,
  { readonly name: string; readonly sha256: string }
> = {
  "darwin-arm64": {
    name: "cloudflared-darwin-arm64.tgz",
    sha256: "68b6bc4080ac1330234db2a9002d07f98b000537a1bd531a687b660d4160fa7f",
  },
  "darwin-x64": {
    name: "cloudflared-darwin-amd64.tgz",
    sha256: "f11fbef4207d715892331917dafd437f032cf4b7137ed64dea3008338c1c613d",
  },
  "linux-x64": {
    name: "cloudflared-linux-amd64",
    sha256: "ae5e8add7884b0531d66bbca4570d1de491ff6da51e4ebe7eb160cdf241d9e42",
  },
  "win32-x64": {
    name: "cloudflared-windows-amd64.exe",
    sha256: "411b16d40c8d872243efeb52c550053d00af0a8a97fbb32e730fb8aba429eaa8",
  },
};

const BINARY = "cloudflared";

// Where `fetchCloudflared` puts it in `directory`.
export const pinnedPath = (directory: string) =>
  join(directory, process.platform === "win32" ? `${BINARY}.exe` : BINARY);

export class CloudflaredFetchError extends Schema.TaggedError<CloudflaredFetchError>()(
  "CloudflaredFetchError",
  {
    reason: Schema.Literals([
      // No pinned asset for this system.
      "unsupported",
      "download",
      "checksum",
      "unpack",
      "write",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "unsupported":
        return `No cloudflared is pinned for ${process.platform}-${process.arch}.`;
      case "download":
        return "The pinned cloudflared could not be downloaded.";
      case "checksum":
        return "The downloaded cloudflared is not the pinned one.";
      case "unpack":
        return "The downloaded cloudflared could not be unpacked.";
      case "write":
        return "The pinned cloudflared could not be written.";
    }
  }
}

const ascii = new TextDecoder("ascii");

const field = (header: Uint8Array, start: number, length: number) => {
  const bytes = header.subarray(start, start + length);
  const end = bytes.indexOf(0);
  return ascii.decode(end < 0 ? bytes : bytes.subarray(0, end));
};

// The file named `name` in a tar archive: each entry is a 512-byte
// header and its body padded to 512 bytes, until two empty blocks.
const fromTar = (tar: Uint8Array, name: string): Uint8Array | null => {
  for (let offset = 0; offset + 512 <= tar.length;) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) return null;
    const size = Number.parseInt(field(header, 124, 12).trim(), 8);
    const path = field(header, 0, 100).replace(/^\.\//, "");
    const type = header[156];
    if (path === name && (type === 0x30 || type === 0)) {
      return tar.subarray(offset + 512, offset + 512 + size);
    }
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  return null;
};

// Downloads the pinned cloudflared for this system into `directory`,
// checks its SHA-256, unpacks it and answers its path.
export const fetchCloudflared = Effect.fnUntraced(function* (
  directory: string,
) {
  const asset = ASSETS[`${process.platform}-${process.arch}`];
  if (asset === undefined) {
    return yield* new CloudflaredFetchError({ reason: "unsupported" });
  }
  const url = `https://github.com/cloudflare/cloudflared/releases/download/${CLOUDFLARED_VERSION}/${asset.name}`;
  const download = yield* Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(url, { signal });
      if (!response.ok) throw new Error(`answered ${response.status}`);
      return new Uint8Array(await response.arrayBuffer());
    },
    catch: (cause) => new CloudflaredFetchError({ reason: "download", cause }),
  });
  if (createHash("sha256").update(download).digest("hex") !== asset.sha256) {
    return yield* new CloudflaredFetchError({ reason: "checksum" });
  }
  const binary = asset.name.endsWith(".tgz")
    ? yield* Effect.try({
        try: () => fromTar(gunzipSync(download), BINARY),
        catch: (cause) =>
          new CloudflaredFetchError({ reason: "unpack", cause }),
      })
    : download;
  if (binary === null) {
    return yield* new CloudflaredFetchError({ reason: "unpack" });
  }
  const path = pinnedPath(directory);
  // Whole or not at all, so the runner never finds half a binary.
  yield* Effect.tryPromise({
    try: async () => {
      await mkdir(directory, { recursive: true });
      await writeFile(`${path}.next`, binary);
      await chmod(`${path}.next`, 0o755);
      await rename(`${path}.next`, path);
    },
    catch: (cause) => new CloudflaredFetchError({ reason: "write", cause }),
  });
  return path;
});
