import { credentialHash } from "@colibrii/sdk/wire/credential";
import { describe, expect, it } from "vitest";
import {
  client,
  INKWELL_CAP,
  newAccountId,
  newConnectionId,
  newDeviceId,
  newPublicKey,
  oauthToken,
  openSocket,
  sessionToken,
  startService,
  cloudflareRequests,
} from "@colibrii/harness";

const { url, worker } = await startService();
const api = client(url);

const { enrolled } = api;

const list = async (credential: string) =>
  (await api.call("GET", "/devices", { bearer: credential })).json;

describe("the version floor", () => {
  it("refuses a request without the header, or below the floor, before anything else", async () => {
    for (const headers of [
      { "colibrii-version": "" },
      { "colibrii-version": "0" },
    ]) {
      const answer = await api.call("POST", "/devices/enroll", {
        headers,
        rawBody: "not json",
      });
      expect(answer.status).toBe(403);
      expect(answer.json).toEqual({ _tag: "UpdateRequired", floor: 1 });
    }
    const unauthenticated = await api.call("GET", "/devices", {
      headers: { "colibrii-version": "0" },
    });
    expect(unauthenticated.json).toEqual({ _tag: "UpdateRequired", floor: 1 });
  });

  it("does not apply to GET /connect", async () => {
    const answer = await api.call("GET", "/connect?ticket=x", {
      headers: { "colibrii-version": "0" },
    });
    expect(answer.status).toBe(426);
  });
});

describe("enrollment", () => {
  it("enrolls with a session token", async () => {
    const account = newAccountId();
    const deviceId = newDeviceId();
    const answer = await api.enroll({
      token: await sessionToken(account),
      deviceId,
      data: { icon: "laptop" },
      name: "Studio MacBook",
    });
    expect(answer.status).toBe(200);
    expect(answer.credential).toMatch(/^colibrii_dc_[A-Za-z0-9_-]{43}$/);
    expect(answer.json).toMatchObject({
      accountId: account,
      device: {
        deviceId,
        name: "Studio MacBook",
        data: { icon: "laptop" },
        connections: "single",
        lastSeenAt: null,
        online: false,
      },
    });
  });

  it("enrolls with an OAuth access token of either type", async () => {
    for (const typ of ["at+jwt", "application/at+jwt"]) {
      const account = newAccountId();
      const answer = await api.enroll({
        token: await oauthToken(account, "inkwell-oauth", { typ }),
      });
      expect(answer.status, typ).toBe(200);
      expect(answer.json.accountId).toBe(account);
      expect(answer.json.device.data).toEqual({});
    }
  });

  it("reads a body as JSON whatever its content-type", async () => {
    const answer = await api.call("POST", "/devices/enroll", {
      bearer: await sessionToken(newAccountId()),
      headers: { "content-type": "text/plain" },
      body: {
        app: "inkwell",
        deviceId: newDeviceId(),
        name: "Plain",
        publicKey: newPublicKey(),
        connections: "multi",
      },
    });
    expect(answer.status).toBe(200);
  });

  it("refuses a login token not issued for the app", async () => {
    const account = newAccountId();
    const rejected = [
      undefined,
      "",
      "not-a-jwt",
      // Another app's Clerk instance.
      await sessionToken(account, "inkwell://app", { key: "b" }),
      await oauthToken(account, "inkwell-oauth", { key: "b" }),
      // An origin the app did not register, or none.
      await sessionToken(account, "https://evil.example"),
      await sessionToken(account, "inkwell://app", {
        claims: { azp: undefined },
      }),
      // Another OAuth client, or none.
      await oauthToken(account, "quill-oauth"),
      await oauthToken(account, "inkwell-oauth", {
        claims: { client_id: undefined },
      }),
      // A session token with an OAuth client, and a type that is neither.
      await sessionToken(account, "inkwell://app", {
        claims: { azp: undefined, client_id: "inkwell-oauth" },
      }),
      await sessionToken(account, "inkwell://app", { typ: "JOSE" }),
      // Expired.
      await sessionToken(account, "inkwell://app", {
        claims: { exp: Math.floor(Date.now() / 1000) - 60 },
      }),
    ];
    for (const [index, token] of rejected.entries()) {
      const answer = await api.enroll({ token: token ?? "" });
      expect(answer.status, `token ${index}`).toBe(401);
      expect(answer.json, `token ${index}`).toEqual({ _tag: "LoginRejected" });
    }
  });

  it("takes any port of localhost from an app that registered http://localhost:*", async () => {
    const account = newAccountId();
    const token = (azp: string) => sessionToken(account, azp, { key: "b" });
    const answer = await api.enroll({
      token: await token("http://localhost:6185"),
      app: "quill",
    });
    expect(answer.status).toBe(200);
    for (const azp of [
      "http://localhost",
      "https://localhost:6185",
      "http://localhost:6185.evil.example",
      "http://127.0.0.1:6185",
    ]) {
      const refused = await api.enroll({
        token: await token(azp),
        app: "quill",
      });
      expect(refused.status, azp).toBe(401);
    }
    const inkwell = await api.enroll({
      token: await sessionToken(account, "http://localhost:6185"),
    });
    expect(inkwell.status).toBe(401);
  });

  it("refuses an unknown app", async () => {
    const answer = await api.enroll({
      token: await sessionToken(newAccountId()),
      app: "slate",
    });
    expect(answer.status).toBe(404);
    expect(answer.json).toEqual({ _tag: "UnknownApp", app: "slate" });
  });

  it("enrolls again, replacing the credential and keeping createdAt", async () => {
    const first = await enrolled({ data: { icon: "laptop" } });
    const again = await enrolled({
      account: first.account,
      deviceId: first.deviceId,
      name: "Renamed",
      connections: "multi",
    });
    expect(again.credential).not.toBe(first.credential);
    expect(again.json.device.createdAt).toBe(first.json.device.createdAt);
    expect(again.json.device).toMatchObject({
      name: "Renamed",
      data: {},
      connections: "multi",
    });
    const old = await api.call("GET", "/devices", { bearer: first.credential });
    expect(old.status).toBe(401);
    expect(old.json).toEqual({ _tag: "CredentialRejected" });
    expect((await list(again.credential)).devices).toHaveLength(1);
  });

  it("refuses a device enrolled under another account or app", async () => {
    const first = await enrolled();
    const otherAccount = await api.enroll({
      token: await sessionToken(newAccountId()),
      deviceId: first.deviceId,
    });
    expect(otherAccount.status).toBe(409);
    expect(otherAccount.json).toEqual({ _tag: "DeviceEnrolledElsewhere" });
    const otherApp = await api.enroll({
      token: await sessionToken(first.account, "quill://app", { key: "b" }),
      app: "quill",
      deviceId: first.deviceId,
    });
    expect(otherApp.status).toBe(409);
  });

  it("bounds the name and the data", async () => {
    const token = await sessionToken(newAccountId());
    const accepted = [
      { name: "é".repeat(128) },
      { data: { pad: "x".repeat(1024 - '{"pad":""}'.length) } },
    ];
    for (const fields of accepted) {
      expect((await api.enroll({ token, ...fields })).status).toBe(200);
    }
    const refused = [
      { name: "" },
      { name: `${"é".repeat(128)}x` },
      { data: { pad: "x".repeat(1025 - '{"pad":""}'.length) } },
      { data: ["not", "an", "object"] },
    ];
    for (const fields of refused) {
      const answer = await api.enroll({ token, ...fields });
      expect(answer.status, JSON.stringify(fields).slice(0, 40)).toBe(400);
      expect(answer.text).toBe("");
    }
  });

  it("measures data as it encodes again compactly", async () => {
    const data = { pad: "x".repeat(1024 - '{"pad":""}'.length) };
    const answer = await api.call("POST", "/devices/enroll", {
      bearer: await sessionToken(newAccountId()),
      rawBody: JSON.stringify(
        {
          app: "inkwell",
          deviceId: newDeviceId(),
          name: "Spaced",
          publicKey: newPublicKey(),
          connections: "single",
          data,
        },
        null,
        8,
      ),
    });
    expect(answer.status).toBe(200);
  });

  it("makes room by removing the offline device seen longest ago", async () => {
    const account = newAccountId();
    const devices = [];
    for (let i = 0; i < INKWELL_CAP; i += 1) {
      devices.push(await enrolled({ account }));
    }
    const [oldest, second] = devices;
    if (oldest === undefined || second === undefined) throw new Error();
    // The oldest is online, so the next stalest goes.
    const socket = await openSocket(url, await api.ticket(oldest.credential));
    const newcomer = await enrolled({ account });
    const evicted = await api.call("GET", "/devices", {
      bearer: second.credential,
    });
    expect(evicted.status).toBe(403);
    expect(evicted.json).toEqual({ _tag: "DeviceRevoked" });
    const ids = (await list(newcomer.credential)).devices.map(
      (device: { deviceId: string }) => device.deviceId,
    );
    expect(ids).toHaveLength(INKWELL_CAP);
    expect(ids).not.toContain(second.deviceId);
    expect(ids).toContain(newcomer.deviceId);
    socket.close();
  });

  it("removes no device for a new one whose row fails to go in", async () => {
    const account = newAccountId();
    const devices = [];
    for (let i = 0; i < INKWELL_CAP; i += 1) {
      devices.push(await enrolled({ account }));
    }
    const deviceId = newDeviceId();
    const { DB } = await worker.getEnv();
    await DB.prepare(
      `CREATE TRIGGER refuse_enroll BEFORE INSERT ON devices
       WHEN NEW.device_id = '${deviceId}'
       BEGIN SELECT RAISE(ABORT, 'refused'); END`,
    ).run();
    const answer = await api.enroll({
      token: await sessionToken(account),
      deviceId,
    });
    await DB.prepare("DROP TRIGGER refuse_enroll").run();
    expect(answer.status).toBe(502);
    const ids = (await list(devices.at(-1)?.credential ?? "")).devices.map(
      (device: { deviceId: string }) => device.deviceId,
    );
    expect(ids.toSorted()).toEqual(
      devices.map((device) => device.deviceId).toSorted(),
    );
  });

  it("makes room in the same write as the new row, so a retry still does", async () => {
    const account = newAccountId();
    const devices = [];
    for (let i = 0; i < INKWELL_CAP; i += 1) {
      devices.push(await enrolled({ account }));
    }
    const { DB } = await worker.getEnv();
    await DB.prepare(
      `CREATE TRIGGER refuse_evict BEFORE DELETE ON devices
       WHEN OLD.account_id = '${account}'
       BEGIN SELECT RAISE(ABORT, 'refused'); END`,
    ).run();
    const token = await sessionToken(account);
    const deviceId = newDeviceId();
    const failed = await api.enroll({ token, deviceId });
    await DB.prepare("DROP TRIGGER refuse_evict").run();
    expect(failed.status).toBe(502);
    const retried = await api.enroll({ token, deviceId });
    expect(retried.status).toBe(200);
    const ids = (await list(retried.credential)).devices.map(
      (device: { deviceId: string }) => device.deviceId,
    );
    expect(ids).toHaveLength(INKWELL_CAP);
    expect(ids).toContain(deviceId);
  });

  it("is full when every device is online", async () => {
    const account = newAccountId();
    const sockets = [];
    for (let i = 0; i < INKWELL_CAP; i += 1) {
      const device = await enrolled({ account });
      sockets.push(await openSocket(url, await api.ticket(device.credential)));
    }
    const answer = await api.enroll({ token: await sessionToken(account) });
    expect(answer.status).toBe(409);
    expect(answer.json).toEqual({ _tag: "AccountFull", limit: INKWELL_CAP });
    for (const socket of sockets) socket.close();
  });
});

describe("the device credential", () => {
  it("refuses a malformed or unknown credential", async () => {
    for (const bearer of [
      undefined,
      "",
      "colibrii_dc_short",
      await sessionToken(newAccountId()),
      `colibrii_dc_${newPublicKey()}`,
    ]) {
      const answer = await api.call("GET", "/devices", { bearer });
      expect(answer.status).toBe(401);
      expect(answer.json).toEqual({ _tag: "CredentialRejected" });
    }
  });

  it("reads a removed device's credential as revoked for 14 days", async () => {
    const device = await enrolled();
    const removed = await api.call("DELETE", `/devices/${device.deviceId}`, {
      bearer: device.credential,
    });
    expect(removed.status).toBe(204);
    const answer = await api.call("GET", "/devices", {
      bearer: device.credential,
    });
    expect(answer.status).toBe(403);
    expect(answer.json).toEqual({ _tag: "DeviceRevoked" });

    const { DB } = await worker.getEnv();
    await DB.prepare(
      "UPDATE revoked_credentials SET revoked_at = revoked_at - ?",
    )
      .bind(14 * 24 * 60 * 60 * 1000 + 1)
      .run();
    const later = await api.call("GET", "/devices", {
      bearer: device.credential,
    });
    expect(later.json).toEqual({ _tag: "CredentialRejected" });
  });
});

describe("removing a device", () => {
  it("checks the caller's credential again as the removal runs", async () => {
    const device = await enrolled();
    // The removal passed the Worker's check, and waited for the account's
    // object behind an enrollment of the same device that replaced its
    // credential.
    const again = await api.enroll({
      token: await sessionToken(device.account),
      deviceId: device.deviceId,
    });
    const { HUB } = await worker.getEnv();
    const hub = HUB.get(HUB.idFromName(`inkwell/${device.account}`));
    expect(
      await hub.remove(
        { appId: "inkwell", accountId: device.account },
        device.deviceId,
        await credentialHash(device.credential),
      ),
    ).toBe("rejected");
    const { devices } = await list(again.credential);
    expect(devices).toHaveLength(1);
  });
});

describe("the registry", () => {
  it("lists the account's devices oldest first, with who is online", async () => {
    const first = await enrolled();
    const second = await enrolled({
      account: first.account,
      connections: "multi",
    });
    // Another account and another app, under the same account id.
    await enrolled();
    await api.enroll({
      token: await sessionToken(first.account, "quill://app", { key: "b" }),
      app: "quill",
    });
    const socket = await openSocket(url, await api.ticket(second.credential));
    await socket.next();
    const { devices } = await list(first.credential);
    expect(
      devices.map((device: { deviceId: string }) => device.deviceId),
    ).toEqual([first.deviceId, second.deviceId]);
    expect(devices[0].online).toBe(false);
    expect(devices[1]).toMatchObject({ online: true, connections: "multi" });
    expect(devices[1].lastSeenAt).toEqual(expect.any(Number));
    socket.close();
  });

  it("changes a device's name, data, or both", async () => {
    const first = await enrolled({ data: { icon: "laptop" } });
    const second = await enrolled({ account: first.account });
    const patch = (deviceId: string, body: unknown) =>
      api.call("PATCH", `/devices/${deviceId}`, {
        bearer: first.credential,
        body,
      });
    expect((await patch(first.deviceId, { name: "Desk" })).status).toBe(204);
    expect(
      (await patch(second.deviceId, { data: { pinned: true } })).status,
    ).toBe(204);
    const { devices } = await list(first.credential);
    expect(devices[0]).toMatchObject({
      name: "Desk",
      data: { icon: "laptop" },
    });
    expect(devices[1]).toMatchObject({
      name: "Test device",
      data: { pinned: true },
    });

    expect((await patch(first.deviceId, { name: "A", data: {} })).status).toBe(
      204,
    );
    expect((await list(first.credential)).devices[0]).toMatchObject({
      name: "A",
      data: {},
    });

    for (const body of [{}, { name: "" }, { data: "x" }]) {
      expect((await patch(first.deviceId, body)).status).toBe(400);
    }
    expect((await patch("not-a-uuid", { name: "A" })).status).toBe(400);
    const stranger = await enrolled();
    const unknown = await patch(stranger.deviceId, { name: "A" });
    expect(unknown.status).toBe(404);
    expect(unknown.json).toEqual({
      _tag: "UnknownDevice",
      deviceId: stranger.deviceId,
    });
  });

  it("removes another device, or the caller itself", async () => {
    const first = await enrolled();
    const second = await enrolled({ account: first.account });
    const stranger = await enrolled();
    const remove = (deviceId: string) =>
      api.call("DELETE", `/devices/${deviceId}`, { bearer: first.credential });
    const unknown = await remove(stranger.deviceId);
    expect(unknown.status).toBe(404);
    expect(unknown.json).toEqual({
      _tag: "UnknownDevice",
      deviceId: stranger.deviceId,
    });
    expect((await remove(second.deviceId)).status).toBe(204);
    expect((await remove(second.deviceId)).status).toBe(404);
    expect((await list(first.credential)).devices).toHaveLength(1);
    expect((await remove(first.deviceId)).status).toBe(204);
    expect(
      (await api.call("GET", "/devices", { bearer: first.credential })).json,
    ).toEqual({ _tag: "DeviceRevoked" });
    expect((await list(stranger.credential)).devices).toHaveLength(1);
  });
});

describe("removing a device", () => {
  it("looks for its tunnel and record at Cloudflare's API as the harness answers it", async () => {
    const device = await enrolled();
    const before = cloudflareRequests.length;
    const removed = await api.call("DELETE", `/devices/${device.deviceId}`, {
      bearer: device.credential,
    });
    expect(removed.status).toBe(204);
    const asked = cloudflareRequests.slice(before).map(({ url: to }) => to);
    expect(asked.some((to) => to.endsWith("/cfd_tunnel"))).toBe(true);
    expect(asked.some((to) => to.endsWith("/dns_records"))).toBe(true);
  });
});

describe("tickets and tunnels", () => {
  it("mints a ticket routed to the app and account", async () => {
    const device = await enrolled();
    const answer = await api.call("POST", "/tickets", {
      bearer: device.credential,
      body: { connectionId: newConnectionId() },
    });
    expect(answer.status).toBe(200);
    const [route] = answer.json.ticket.slice("colibrii_ct_".length).split(".");
    expect(Buffer.from(route, "base64url").toString()).toBe(
      `inkwell/${device.account}`,
    );
    const bad = await api.call("POST", "/tickets", {
      bearer: device.credential,
      body: { connectionId: "3F9A0C1E5B7D4F2A8C6E0B1D3F5A7C9E" },
    });
    expect(bad.status).toBe(400);
  });

  it("refuses a tunnel port out of range", async () => {
    const device = await enrolled();
    for (const port of [0, 65_536, 80.5]) {
      const answer = await api.call("POST", "/tunnel", {
        bearer: device.credential,
        body: { port },
      });
      expect(answer.status).toBe(400);
    }
  });
});

describe("sign-in tokens", () => {
  it("are off for an app without a Clerk secret key", async () => {
    const answer = await api.call("POST", "/sign-in-token", {
      bearer: await oauthToken(newAccountId()),
      body: { app: "inkwell" },
    });
    expect(answer.status).toBe(403);
    expect(answer.json).toEqual({ _tag: "SignInTokensOff" });
  });

  it("take an OAuth access token, never a session token", async () => {
    const answer = await api.call("POST", "/sign-in-token", {
      bearer: await sessionToken(newAccountId(), "quill://app", { key: "b" }),
      body: { app: "quill" },
    });
    expect(answer.status).toBe(401);
    expect(answer.json).toEqual({ _tag: "LoginRejected" });
  });
});

describe("HTTP", () => {
  it("answers an unmatched path 404 and a malformed body 400, with no body", async () => {
    const missing = await api.call("GET", "/nothing");
    expect(missing.status).toBe(404);
    expect(missing.text).toBe("");
    const malformed = await api.call("POST", "/devices/enroll", {
      bearer: await sessionToken(newAccountId()),
      rawBody: "{",
    });
    expect(malformed.status).toBe(400);
    expect(malformed.text).toBe("");
  });

  it("allows any origin", async () => {
    const preflight = await fetch(new URL("/devices", url), {
      method: "OPTIONS",
      headers: {
        origin: "https://inkwell.example",
        "access-control-request-method": "PATCH",
        "access-control-request-headers": "authorization,colibrii-version",
      },
    });
    expect(preflight.status).toBe(204);
    expect(Object.fromEntries(preflight.headers)).toMatchObject({
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, PATCH, DELETE",
      "access-control-allow-headers":
        "authorization,content-type,colibrii-version",
      "access-control-max-age": "86400",
    });
    for (const answer of [
      await api.call("GET", "/devices"),
      await api.call("GET", "/nothing"),
    ]) {
      expect(answer.headers.get("access-control-allow-origin")).toBe("*");
      expect(answer.headers.get("access-control-expose-headers")).toBe(
        "retry-after",
      );
    }
  });

  it("limits the open routes to 60 a minute per client", async () => {
    const headers = { "cf-connecting-ip": "203.0.113.9" };
    const statuses = [];
    for (let i = 0; i < 61; i += 1) {
      statuses.push(
        (await api.call("POST", "/devices/enroll", { headers })).status,
      );
    }
    expect(statuses.slice(0, 60).every((status) => status === 400)).toBe(true);
    expect(statuses[60]).toBe(429);
    const limited = await api.call("POST", "/sign-in-token", { headers });
    expect(limited.status).toBe(429);
    expect(limited.text).toBe("");
    expect(limited.headers.get("retry-after")).toBe("60");
    expect(limited.headers.get("access-control-allow-origin")).toBe("*");
    // The other routes have a budget of their own.
    expect((await api.call("GET", "/devices", { headers })).status).toBe(401);
  });
});
