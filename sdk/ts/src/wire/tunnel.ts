import { sha256Hex } from "./sha256.ts";

// The first 32 hex characters of the SHA-256 of `<app>:<account>:<device>`.
export const tunnelLabel = async (
  appId: string,
  accountId: string,
  deviceId: string,
): Promise<string> =>
  (await sha256Hex(`${appId}:${accountId}:${deviceId}`)).slice(0, 32);
