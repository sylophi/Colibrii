// The page the browser tests drive. Each tab makes a device of the
// `inkwell` app as a browser does (`multi`, its store in localStorage),
// on the hub and under the store key its address names, and serves it
// through `window.colibrii`.
import * as Colibrii from "@colibrii/sdk";
import * as ColibriiBrowser from "@colibrii/sdk/browser";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

const params = new URLSearchParams(location.search);

// How a call ended, as plain data the test can read.
export type Outcome =
  | { readonly ok: true }
  | { readonly ok: false; readonly tag: string; readonly reason?: string };

const runtime = ManagedRuntime.make(
  Layer.merge(
    ColibriiBrowser.layer,
    ColibriiBrowser.layerLocalStorageStore(params.get("store") ?? ""),
  ),
);

const device = await runtime.runPromise(
  Colibrii.make({
    hub: params.get("hub") ?? "",
    app: "inkwell",
    connections: "multi",
  }).pipe(Scope.provide(Scope.makeUnsafe())),
);

const state = {
  account: null as Colibrii.AccountState | null,
  // The peers' device ids.
  roster: [] as ReadonlyArray<string>,
  // How many times the hub socket went down once up.
  drops: 0,
};

runtime.runFork(
  device.account.pipe(
    Stream.runForEach((next) =>
      Effect.sync(() => {
        const was = state.account;
        if (was?._tag === "Enrolled" && was.connected) {
          if (next._tag !== "Enrolled" || !next.connected) state.drops += 1;
        }
        state.account = next;
      }),
    ),
  ),
);
runtime.runFork(
  device.roster.pipe(
    Stream.runForEach((peers) =>
      Effect.sync(() => {
        state.roster = peers.map((peer) => peer.deviceId);
      }),
    ),
  ),
);

const settle = <E extends { readonly _tag: string }>(
  effect: Effect.Effect<unknown, E>,
) =>
  runtime.runPromise(
    effect.pipe(
      Effect.match({
        onSuccess: (): Outcome => ({ ok: true }),
        onFailure: (error): Outcome => ({
          ok: false,
          tag: error._tag,
          ...("reason" in error ? { reason: String(error.reason) } : {}),
        }),
      }),
    ),
  );

const api = {
  state,
  enroll: (loginToken: string) =>
    settle(device.enroll({ loginToken, name: "Browser" })),
  signOut: () => settle(device.signOut),
  dial: (deviceId: string) =>
    settle(Effect.scoped(device.dial(deviceId, new Uint8Array()))),
};

declare global {
  interface Window {
    colibrii: typeof api;
  }
}

window.colibrii = api;
