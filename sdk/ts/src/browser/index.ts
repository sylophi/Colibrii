// The SDK in a browser: websockets and HTTP through the page's own,
// wakes when the page comes back into view or the network comes back,
// and the credential store in localStorage. A page only dials, and only
// a peer's tunnel.
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { layerWebSockets } from "./sockets.ts";
import { layerWakes } from "./wakes.ts";

export { layerLocalStorageStore } from "./store.ts";

// Everything a device needs in a browser but its credential store.
export const layer = Layer.mergeAll(
  layerWebSockets,
  layerWakes,
  FetchHttpClient.layer,
);
