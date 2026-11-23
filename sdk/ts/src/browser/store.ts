// The credential store as one localStorage entry under a key the app
// gives, which every tab of the browser profile shares. A tab hears of
// another's change through the `storage` event, and enrolls or signs
// out under a lock of the same name (navigator.locks). Whatever runs
// script on the page's origin can read the entry.
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { CredentialStore, CredentialStoreError } from "../Platform.ts";

// Runs `effect` while this tab holds the lock `name`.
const exclusive =
  (name: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.callback<() => void>((resume, signal) => {
        navigator.locks
          .request(name, { signal }, () =>
            // Granted as the wait was given up: let it go at once.
            signal.aborted
              ? undefined
              : new Promise<void>((release) =>
                  resume(Effect.succeed(() => release())),
                ),
          )
          // A request given up rejects.
          .catch(() => {});
      }),
      () => effect,
      (release) => Effect.sync(release),
    );

export const layerLocalStorageStore = (key: string) =>
  Layer.sync(CredentialStore, () =>
    CredentialStore.of({
      read: Effect.try({
        try: () => Option.fromNullishOr(localStorage.getItem(key)),
        catch: (cause) =>
          new CredentialStoreError({ operation: "read", cause }),
      }),
      write: (text) =>
        Effect.try({
          try: () => localStorage.setItem(key, text),
          catch: (cause) =>
            new CredentialStoreError({ operation: "write", cause }),
        }),
      remove: Effect.try({
        try: () => localStorage.removeItem(key),
        catch: (cause) =>
          new CredentialStoreError({ operation: "remove", cause }),
      }),
      shared: {
        // A null key is the whole storage cleared.
        changes: Stream.fromEventListener<StorageEvent>(window, "storage").pipe(
          Stream.filter(
            (event) =>
              event.storageArea === localStorage &&
              (event.key === key || event.key === null),
          ),
          Stream.as(undefined),
        ),
        exclusive: exclusive(key),
      },
    }),
  );
