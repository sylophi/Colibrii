// Each device's Cloudflare tunnel (spec/tunnels.md). The service sets it
// up through the Cloudflare API and never carries its traffic. The
// tunnel's name comes from the device's ids alone, so provisioning finds
// or creates it with nothing stored, and removal finds it the same way.
// The label is 128 bits of a hash a device cannot choose its way into:
// a found tunnel is handed over, connector token and all.
import { tunnelLabel } from "@colibrii/sdk/wire/tunnel";
import type { TunnelConfig } from "./config.ts";
import type { Account } from "./Registry.ts";

const API = "https://api.cloudflare.com/client/v4";

interface Found {
  readonly id: string;
  readonly content?: string;
  readonly proxied?: boolean;
  readonly created_on?: string;
}

interface IngressConfig {
  readonly config?: {
    readonly ingress?: ReadonlyArray<{
      readonly hostname?: string;
      readonly service: string;
    }>;
  };
}

const cloudflare = (config: TunnelConfig) => {
  // Throws on a non-2xx status or `success: false`, so a failure is never
  // read as a result. The caller names the `result` it reads.
  const call = async <T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> => {
    const response = await fetch(`${API}${path}`, {
      method,
      headers: {
        authorization: `Bearer ${config.apiToken}`,
        "content-type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const envelope = (await response.json().catch(() => null)) as {
      success?: boolean;
      result?: unknown;
    } | null;
    if (!response.ok || envelope?.success !== true) {
      throw new Error(
        `cloudflare ${method} ${path.split("?")[0]} answered ${response.status}`,
      );
    }
    return envelope.result as T;
  };
  const first = async (path: string) =>
    (await call<Found[]>("GET", path))[0] ?? null;
  const tunnels = `/accounts/${config.accountId}/cfd_tunnel`;
  const records = `/zones/${config.zoneId}/dns_records`;
  return {
    call,
    tunnels,
    records,
    findTunnel: (name: string) =>
      first(`${tunnels}?name=${encodeURIComponent(name)}&is_deleted=false`),
    findRecord: (hostname: string) =>
      first(`${records}?type=CNAME&name=${encodeURIComponent(hostname)}`),
  };
};

const names = async (
  config: TunnelConfig,
  account: Account,
  deviceId: string,
) => {
  const label = await tunnelLabel(account.appId, account.accountId, deviceId);
  return { name: `colibrii-${label}`, hostname: `${label}.${config.domain}` };
};

export const provisionTunnel = async (
  config: TunnelConfig,
  account: Account,
  deviceId: string,
  port: number,
): Promise<{ hostname: string; connectorToken: string; fresh: boolean }> => {
  const api = cloudflare(config);
  const { name, hostname } = await names(config, account, deviceId);
  const tunnel =
    (await api.findTunnel(name)) ??
    (await api.call<Found>("POST", api.tunnels, {
      name,
      config_src: "cloudflare",
    }));
  const [, fresh, connectorToken] = await Promise.all([
    writeIngress(
      api,
      `${api.tunnels}/${tunnel.id}/configurations`,
      hostname,
      port,
    ),
    ensureRecord(api, hostname, `${tunnel.id}.cfargotunnel.com`),
    api.call<string>("GET", `${api.tunnels}/${tunnel.id}/token`),
  ]);
  return { hostname, connectorToken, fresh };
};

// Cloudflare can answer an ingress write with success and keep the port
// it held before, so the write holds only once a read gives it back. It
// is written again once, then the provision fails.
const writeIngress = async (
  api: ReturnType<typeof cloudflare>,
  path: string,
  hostname: string,
  port: number,
): Promise<void> => {
  const service = `http://127.0.0.1:${port}`;
  const body = {
    config: {
      ingress: [{ hostname, service }, { service: "http_status:404" }],
    },
  };
  for (let attempt = 0; attempt < 2; attempt += 1) {
    // oxlint-disable-next-line no-await-in-loop -- each write is read back before the next
    await api.call("PUT", path, body);
    // oxlint-disable-next-line no-await-in-loop -- the read back of the write before it
    const held = await api.call<IngressConfig | null>("GET", path);
    const holds = held?.config?.ingress?.find(
      (rule) => rule.hostname === hostname,
    )?.service;
    if (holds === service) return;
  }
  throw new Error(`the tunnel's ingress did not take port ${port}`);
};

// A record made this recently may not be out yet, whichever call made
// it: a provision can fail after its record went in.
const FRESH_FOR_MS = 60_000;

// Whether the record is fresh.
const ensureRecord = async (
  api: ReturnType<typeof cloudflare>,
  hostname: string,
  target: string,
): Promise<boolean> => {
  const body = {
    type: "CNAME",
    name: hostname,
    content: target,
    proxied: true,
  };
  const record = await api.findRecord(hostname);
  if (record === null) {
    await api.call("POST", api.records, body);
    return true;
  }
  if (record.content !== target || record.proxied !== true) {
    await api.call("PUT", `${api.records}/${record.id}`, body);
  }
  return Date.now() - Date.parse(record.created_on ?? "") < FRESH_FOR_MS;
};

// Deletes the tunnel and its record, each on its own: a removal never
// fails for want of it. A tunnel left behind leads only to the removed
// device's loopback, and only that device held its token.
export const removeTunnel = async (
  config: TunnelConfig,
  account: Account,
  deviceId: string,
): Promise<void> => {
  const api = cloudflare(config);
  const { name, hostname } = await names(config, account, deviceId);
  const remove = async (
    found: Promise<Found | null>,
    path: (id: string) => string,
  ) => {
    try {
      const item = await found;
      if (item !== null) await api.call("DELETE", path(item.id));
    } catch (error) {
      console.warn(`tunnel ${name}: removal failed`, error);
    }
  };
  await Promise.all([
    remove(api.findTunnel(name), (id) => `${api.tunnels}/${id}?cascade=true`),
    remove(api.findRecord(hostname), (id) => `${api.records}/${id}`),
  ]);
};
