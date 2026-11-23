# Working on Colibrii

- `spec/` is the source of truth for the wire. A change to what crosses
  it starts there.
- `sdk/ts` is `@colibrii/sdk`. Its `src/wire/` holds the wire as pure
  code: the schemas and helpers the service and the SDK share, and
  Noise, the relay and the peer link between devices.
  `scripts/vectors.ts` writes the test vectors in `spec/vectors/`
  (`pnpm --filter @colibrii/sdk run vectors`). A released vector never
  changes.
- `sdk/ts/src/` outside `wire/` is the device runtime, written against
  the services in `Platform.ts`. `src/node/` binds them in Node, and
  nothing else in `src/` imports `node:*` or `ws`. `src/browser/`
  binds them in a web page, and the browser tests check that the core
  and it bundle without either.
- `sdk/rust` is the `colibrii` crate, the same device in Rust. Its
  tests start the service through `harness/src/serve.ts`, and its
  checks run in `sdk/rust`: `cargo fmt --check`,
  `cargo clippy --all-targets -- -D warnings` and `cargo test`.
- `service/` is the Worker. Its README says how an instance is set up.
- `harness/` runs the service in workerd for the service's tests and
  the SDK's.
- A release is a GitHub release tagged `v` plus the version both SDKs
  carry (`sdk/ts/package.json`, `sdk/rust/Cargo.toml`). Publishing one
  attaches the TypeScript SDK's tarball to it
  (`.github/workflows/release.yml`), which is what an app installs. A
  Rust app takes the crate by the tag.
- Install with pnpm. Install scripts are off, and new releases wait 7
  days: keep both settings.
- Checks, from the root: `pnpm typecheck`, `pnpm lint`,
  `pnpm format:check` (`pnpm format` fixes), `pnpm test`, or all four
  with `pnpm check`.
- Read Effect's own guide in `node_modules/effect/AGENTS.md` before
  writing Effect code, and the pinned source for anything in its
  unstable modules (`http`, `http-api`, `sql`).
- Docs say what is and why. Comments only say what the code can't.
