import { describe, expect, it } from "vitest";
import {
  clerkPublicKey,
  client,
  isPresence,
  newAccountId,
  newDeviceId,
  oauthToken,
  openSocket,
  rosterOf,
  sessionToken,
  startService,
} from "@colibrii/harness";

const { url, putApp, removeApp } = await startService();
const api = client(url);

// An app on inkwell's Clerk instance, apart from it.
const app = (id: string, origins = [`${id}://app`]) => ({
  id,
  clerkPublicKey,
  origins,
  oauthClientId: `${id}-oauth`,
});

const enroll = async (
  appId: string,
  account = newAccountId(),
  deviceId: string = newDeviceId(),
  origin = `${appId}://app`,
) =>
  await api.enroll({
    token: await sessionToken(account, origin),
    app: appId,
    deviceId,
  });

describe("an app the owner adds while the service runs", () => {
  it("enrolls devices from then on", async () => {
    const before = await enroll("slate");
    expect(before.json).toEqual({ _tag: "UnknownApp", app: "slate" });
    await putApp(app("slate"));
    expect((await enroll("slate")).status).toBe(200);
  });

  it("changes when added again", async () => {
    await putApp(app("folio"));
    expect((await enroll("folio")).status).toBe(200);
    await putApp(app("folio", ["https://folio.example"]));
    const account = newAccountId();
    expect((await enroll("folio", account)).status).toBe(401);
    const web = await enroll(
      "folio",
      account,
      newDeviceId(),
      "https://folio.example",
    );
    expect(web.status).toBe(200);
  });
});

describe("an app the owner removes while the service runs", () => {
  it("takes no new enrollments, and its devices go on working", async () => {
    await putApp(app("vellum"));
    const account = newAccountId();
    const a = await enroll("vellum", account);
    const b = await enroll("vellum", account);
    const socketA = await openSocket(url, await api.ticket(a.credential));
    const socketB = await openSocket(url, await api.ticket(b.credential));
    await socketA.until(
      (message) => isPresence(message) && rosterOf(message).length === 2,
    );

    await removeApp("vellum");

    expect((await enroll("vellum", account)).json).toEqual({
      _tag: "UnknownApp",
      app: "vellum",
    });
    expect((await enroll("vellum", account, a.deviceId)).status).toBe(404);

    // The hub socket stays, and relays.
    socketA.send(
      JSON.stringify({ t: "relay", to: b.deviceId, frame: "ask:1:AAAA" }),
    );
    expect(
      await socketB.until(
        (message) => (message as { t: string }).t === "relay",
      ),
    ).toEqual({ t: "relay", from: a.deviceId, frame: "ask:1:AAAA" });

    const listed = await api.call("GET", "/devices", { bearer: a.credential });
    expect(listed.status).toBe(200);
    expect(
      listed.json.devices.map(
        (device: { deviceId: string; online: boolean }) => [
          device.deviceId,
          device.online,
        ],
      ),
    ).toEqual([
      [a.deviceId, true],
      [b.deviceId, true],
    ]);
    const renamed = await api.call("PATCH", `/devices/${b.deviceId}`, {
      bearer: a.credential,
      body: { name: "Renamed" },
    });
    expect(renamed.status).toBe(204);

    // A new socket is admitted, and replaces the old.
    const again = await openSocket(url, await api.ticket(a.credential));
    expect(isPresence(await again.next())).toBe(true);
    expect((await socketA.closed).code).toBe(4103);

    again.close();
    socketB.close();
  });
});

describe("an app that names a Clerk secret that is not set", () => {
  it("fails a sign-in token with 500, and enrolls as ever", async () => {
    await putApp({
      ...app("quire"),
      clerkSecretName: "UNSET_CLERK_SECRET_KEY",
    });
    const answer = await api.call("POST", "/sign-in-token", {
      bearer: await oauthToken(newAccountId(), "quire-oauth"),
      body: { app: "quire" },
    });
    expect(answer.status).toBe(500);
    expect((await enroll("quire")).status).toBe(200);
  });
});
