// A wake as a page can tell one: the page coming back into view, as its
// timers may have been held back while hidden, and the network going or
// coming back.
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { Wakes } from "../Platform.ts";

export const layerWakes = Layer.sync(Wakes, () =>
  Stream.mergeAll(
    [
      Stream.fromEventListener(document, "visibilitychange").pipe(
        Stream.filter(() => document.visibilityState === "visible"),
      ),
      Stream.fromEventListener(window, "online"),
      Stream.fromEventListener(window, "offline"),
    ],
    { concurrency: "unbounded" },
  ).pipe(Stream.as(undefined)),
);
