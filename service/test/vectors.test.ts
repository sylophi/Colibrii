// The service against the samples in spec/vectors/: what it sends, and
// what it reads.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { tunnelLabel } from "@colibrii/sdk/wire/tunnel";
import { describe, expect, it } from "vitest";
import {
  client,
  newAccountId,
  newConnectionId,
  newDeviceId,
  openSocket,
  oauthToken,
  sessionToken,
  startService,
} from "@colibrii/harness";

interface Case {
  readonly name: string;
  readonly inputs: Record<string, string>;
  readonly expected: Record<string, unknown>;
}

const read = (file: string): Case[] =>
  JSON.parse(
    readFileSync(join(import.meta.dirname, "../../spec/vectors", file), "utf8"),
  );

const hub = read("hub.json");
const sample = (name: string) => {
  const found = hub.find((entry) => entry.name === name);
  if (found === undefined) throw new Error(`no sample named ${name}`);
  return found.expected["json"] as Record<string, unknown>;
};
const error = (tag: string) =>
  hub.find((entry) => entry.inputs["error"] === tag)?.expected;

const { url, worker } = await startService();
const api = client(url);

describe("hub.json", () => {
  const enrollA = sample("POST /devices/enroll request");
  const enrollB = sample("POST /devices/enroll request without data");
  const answer = sample("POST /devices/enroll answer");
  const account = answer["accountId"] as string;

  it("is what the service reads and answers", async () => {
    const token = await sessionToken(account);
    const a = await api.call("POST", "/devices/enroll", {
      bearer: token,
      body: enrollA,
    });
    expect(a.status).toBe(200);
    expect(a.json.accountId).toBe(account);
    expect(Object.keys(a.json).toSorted()).toEqual(
      Object.keys(answer).toSorted(),
    );
    expect(Object.keys(a.json.device).toSorted()).toEqual(
      Object.keys(answer["device"] as object).toSorted(),
    );
    const b = await api.call("POST", "/devices/enroll", {
      bearer: token,
      body: enrollB,
    });
    expect(b.status).toBe(200);

    for (const name of [
      "PATCH /devices/:deviceId request with a name",
      "PATCH /devices/:deviceId request with data",
      "PATCH /devices/:deviceId request with both",
    ]) {
      const patched = await api.call(
        "PATCH",
        `/devices/${enrollA["deviceId"]}`,
        { bearer: a.json.credential, body: sample(name) },
      );
      expect(patched.status, name).toBe(204);
    }
    const tickets = await api.call("POST", "/tickets", {
      bearer: a.json.credential,
      body: sample("POST /tickets request"),
    });
    expect(tickets.status).toBe(200);
    expect(tickets.json.ticket).toMatch(/^colibrii_ct_/);

    // Both online: the roster is the sample's, byte for byte as JSON.
    const socketA = await openSocket(url, tickets.json.ticket);
    await socketA.next();
    const socketB = await openSocket(
      url,
      await api.ticket(b.json.credential, newConnectionId()),
    );
    expect(await socketB.next()).toEqual(sample("socket: presence"));
    expect(await socketA.next()).toEqual(sample("socket: presence"));

    socketA.send(JSON.stringify(sample("socket: relay from the device")));
    expect(await socketB.next()).toEqual(
      sample("socket: relay from the service"),
    );

    socketB.close();
    await socketA.next();
    socketA.send(JSON.stringify(sample("socket: relay from the device")));
    expect(await socketA.next()).toEqual(sample("socket: nack"));

    socketA.send("ping");
    expect(await socketA.next()).toBe(
      hub.find((entry) => entry.name === "socket: pong")?.expected["text"],
    );
    socketA.close();
  });

  it("holds the errors the service answers", async () => {
    const expectError = async (
      tag: string,
      result: Promise<{ status: number; json: unknown }>,
    ) => {
      const { status, json } = await result;
      expect({ status, json }, tag).toEqual(error(tag));
    };
    const stranger = await api.enroll({
      token: await sessionToken(newAccountId()),
    });
    const removed = await api.enroll({
      token: await sessionToken(newAccountId()),
    });
    await api.call("DELETE", `/devices/${removed.deviceId}`, {
      bearer: removed.credential,
    });

    await expectError(
      "UpdateRequired",
      api.call("GET", "/devices", { headers: { "colibrii-version": "0" } }),
    );
    await expectError("LoginRejected", api.enroll({ token: "x" }));
    await expectError(
      "UnknownApp",
      api.enroll({ token: await sessionToken(newAccountId()), app: "slate" }),
    );
    await expectError("CredentialRejected", api.call("GET", "/devices"));
    await expectError(
      "DeviceRevoked",
      api.call("GET", "/devices", { bearer: removed.credential }),
    );
    await expectError(
      "DeviceEnrolledElsewhere",
      api.enroll({
        token: await sessionToken(newAccountId()),
        deviceId: enrollA["deviceId"] as string,
      }),
    );
    await expectError(
      "UnknownDevice",
      api.call("PATCH", `/devices/${enrollB["deviceId"]}`, {
        bearer: stranger.credential,
        body: { name: "x" },
      }),
    );
    await expectError("UpgradeRequired", api.call("GET", "/connect"));
    const malformed = await worker.fetch("/connect?ticket=x", {
      headers: { upgrade: "websocket" },
    });
    expect({ status: malformed.status, json: await malformed.json() }).toEqual(
      error("TicketMalformed"),
    );
    await expectError(
      "SignInTokensOff",
      api.call("POST", "/sign-in-token", {
        bearer: await oauthToken(newAccountId()),
        body: { app: "inkwell" },
      }),
    );
  });
});

describe("credential-hash.json", () => {
  it("is how the service finds a credential", async () => {
    const { DB } = await worker.getEnv();
    for (const { inputs, expected } of read("credential-hash.json")) {
      await DB.prepare(
        `INSERT INTO devices (device_id, app_id, account_id, name, data, connections, public_key, credential_hash, created_at)
         VALUES (?, 'inkwell', ?, 'Vector', '{}', 'single', ?, ?, 0)`,
      )
        .bind(newDeviceId(), newAccountId(), "A".repeat(43), expected["sha256"])
        .run();
      const answer = await api.call("GET", "/devices", {
        bearer: inputs["credential"],
      });
      expect(answer.status, inputs["credential"]).toBe(200);
    }
  });
});

describe("tunnel-label.json", () => {
  it("is the label the service names tunnels with", async () => {
    for (const { inputs, expected } of read("tunnel-label.json")) {
      expect(
        await tunnelLabel(
          inputs["app"] ?? "",
          inputs["account"] ?? "",
          inputs["device"] ?? "",
        ),
      ).toBe(expected["label"]);
    }
  });
});
