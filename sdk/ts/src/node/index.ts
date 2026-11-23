// The SDK in Node: websockets through `ws`, the listener, wakes from
// sleep, HTTP through fetch, the credential store as a file, and the
// tunnel through cloudflared.
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { layerListenerSockets, layerWebSockets } from "./sockets.ts";
import { layerWakes } from "./wakes.ts";

export {
  CLOUDFLARED_VERSION,
  CloudflaredFetchError,
  fetchCloudflared,
} from "./cloudflared.ts";
export {
  type Cipher,
  layerEncryptedFileStore,
  layerFileStore,
} from "./store.ts";
export {
  endLeftoverCloudflared,
  layerTunnel,
  type TunnelOptions,
} from "./tunnel.ts";

// Everything a device needs in Node but its credential store.
export const layer = Layer.mergeAll(
  layerWebSockets,
  layerListenerSockets,
  layerWakes,
  FetchHttpClient.layer,
);
