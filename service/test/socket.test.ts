import { describe, expect, it } from "vitest";
import {
  client,
  isPresence,
  newConnectionId,
  newDeviceId,
  openSocket,
  rosterOf,
  startService,
} from "@colibrii/harness";

const { url, worker } = await startService();
const api = client(url);

const { enrolled } = api;

const isNack = (message: unknown) => (message as { t?: string }).t === "nack";

const connect = async (
  credential: string,
  connectionId = newConnectionId(),
) => {
  const socket = await openSocket(
    url,
    await api.ticket(credential, connectionId),
  );
  expect(isPresence(await socket.next())).toBe(true);
  return socket;
};

describe("opening the hub socket", () => {
  it("admits a socket with the roster, the device itself included", async () => {
    const device = await enrolled();
    const socket = await openSocket(url, await api.ticket(device.credential));
    expect(await socket.next()).toEqual({
      t: "presence",
      online: [
        {
          deviceId: device.deviceId,
          publicKey: device.publicKey,
          connections: "single",
        },
      ],
    });
    socket.close();
  });

  it("refuses a malformed ticket, or a request without an upgrade", async () => {
    for (const ticket of [
      "",
      "colibrii_ct_x.y.z",
      // Signed with another key.
      "colibrii_ct_aW5rd2VsbC91c2Vy.AAAAAAAAAAAAAAAAAAAAAA.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ]) {
      const answer = await worker.fetch(
        `/connect?ticket=${encodeURIComponent(ticket)}`,
        { headers: { upgrade: "websocket" } },
      );
      expect(answer.status).toBe(403);
      expect(await answer.json()).toEqual({ _tag: "TicketMalformed" });
    }
    const device = await enrolled();
    const answer = await api.call(
      "GET",
      `/connect?ticket=${await api.ticket(device.credential)}`,
    );
    expect(answer.status).toBe(426);
    expect(answer.json).toEqual({ _tag: "UpgradeRequired" });
  });

  it("closes with 4101 a ticket spent, expired, or minted under a replaced credential", async () => {
    const device = await enrolled();
    const ticket = await api.ticket(device.credential);
    const first = await openSocket(url, ticket);
    await first.next();
    const spent = await openSocket(url, ticket);
    expect((await spent.closed).code).toBe(4101);

    const expiring = await api.ticket(device.credential);
    const storage = await worker.getDurableObjectStorage("HUB", {
      name: `inkwell/${device.account}`,
    });
    await storage.exec("UPDATE tickets SET expires_at = 0");
    expect((await (await openSocket(url, expiring)).closed).code).toBe(4101);

    const stale = await api.ticket(device.credential);
    await enrolled({ account: device.account, deviceId: device.deviceId });
    expect((await first.closed).code).toBe(4104);
    expect((await (await openSocket(url, stale)).closed).code).toBe(4101);
  });

  it("closes with 4101 a ticket of a removed device", async () => {
    const device = await enrolled();
    const ticket = await api.ticket(device.credential);
    await api.call("DELETE", `/devices/${device.deviceId}`, {
      bearer: device.credential,
    });
    expect((await (await openSocket(url, ticket)).closed).code).toBe(4101);
  });
});

describe("presence and the relay", () => {
  it("announces every change to the roster, sorted by device id", async () => {
    const a = await enrolled();
    const b = await enrolled({ account: a.account, connections: "multi" });
    const socketA = await connect(a.credential);
    const socketB = await connect(b.credential);
    const expected = [a, b]
      .map((device) => device.deviceId)
      .toSorted((x, y) => (x < y ? -1 : 1));
    const seenByA = await socketA.until(isPresence);
    expect(rosterOf(seenByA)).toEqual(expected);
    expect(
      (seenByA as { online: Array<{ connections: string }> }).online.find(
        (device) => device.connections === "multi",
      ),
    ).toBeDefined();
    socketB.close();
    expect(rosterOf(await socketA.until(isPresence))).toEqual([a.deviceId]);
    socketA.close();
  });

  it("relays a frame to every socket of the device, and nacks an offline one", async () => {
    const a = await enrolled();
    const b = await enrolled({ account: a.account, connections: "multi" });
    const offline = await enrolled({ account: a.account });
    const socketA = await connect(a.credential);
    const tabs = [await connect(b.credential), await connect(b.credential)];
    await socketA.until((message) => rosterOf(message).length === 2);

    socketA.send(
      JSON.stringify({ t: "relay", to: b.deviceId, frame: "ask:1:AAAA" }),
    );
    for (const tab of tabs) {
      expect(
        await tab.until((message) => (message as { t: string }).t === "relay"),
      ).toEqual({ t: "relay", from: a.deviceId, frame: "ask:1:AAAA" });
    }

    socketA.send(
      JSON.stringify({ t: "relay", to: offline.deviceId, frame: "ask:2:AAAA" }),
    );
    expect(await socketA.until(isNack)).toEqual({
      t: "nack",
      to: offline.deviceId,
      reason: "offline",
    });

    // A device of another account reads as offline.
    const stranger = await enrolled();
    const strangerSocket = await connect(stranger.credential);
    socketA.send(
      JSON.stringify({ t: "relay", to: stranger.deviceId, frame: "x" }),
    );
    expect(await socketA.until(isNack)).toMatchObject({
      to: stranger.deviceId,
    });

    // What does not parse is dropped, and the socket stays.
    socketA.send("{");
    socketA.send(JSON.stringify({ t: "relay", to: "not-a-device" }));
    socketA.send(new Uint8Array([1, 2, 3]));
    socketA.send("ping");
    expect(await socketA.until((message) => message === "pong")).toBe("pong");

    for (const socket of [socketA, ...tabs, strangerSocket]) socket.close();
  });
});

describe("one device, several sockets", () => {
  it("closes a single device's older sockets with 4103", async () => {
    const device = await enrolled();
    const older = await connect(device.credential);
    const newer = await connect(device.credential);
    expect((await older.closed).code).toBe(4103);
    newer.send("ping");
    expect(await newer.until((message) => message === "pong")).toBe("pong");
    newer.close();
  });

  it("keeps a multi device's sockets, replacing only the same connection", async () => {
    const device = await enrolled({ connections: "multi" });
    const connectionId = newConnectionId();
    const tab = await connect(device.credential, connectionId);
    const other = await connect(device.credential);
    const again = await connect(device.credential, connectionId);
    expect((await tab.closed).code).toBe(4103);
    for (const socket of [other, again]) {
      socket.send("ping");
      expect(await socket.until((message) => message === "pong")).toBe("pong");
    }
    other.close();
    again.close();
  });

  it("closes a multi device's oldest socket past 32", async () => {
    const device = await enrolled({ connections: "multi" });
    const sockets = [];
    for (let i = 0; i < 33; i += 1)
      sockets.push(await connect(device.credential));
    const [oldest, second] = sockets;
    expect((await oldest?.closed)?.code).toBe(4103);
    second?.send("ping");
    expect(await second?.until((message) => message === "pong")).toBe("pong");
    for (const socket of sockets) socket.close();
  });
});

describe("closing", () => {
  it("closes a removed device's sockets with 4102, and it leaves the roster", async () => {
    const a = await enrolled();
    const b = await enrolled({ account: a.account });
    const socketA = await connect(a.credential);
    const socketB = await connect(b.credential);
    await socketA.until((message) => rosterOf(message).length === 2);
    await api.call("DELETE", `/devices/${b.deviceId}`, {
      bearer: a.credential,
    });
    expect((await socketB.closed).code).toBe(4102);
    expect(rosterOf(await socketA.until(isPresence))).toEqual([a.deviceId]);
    socketA.close();
  });

  it("leaves out a socket it closed that never answered the close", async () => {
    const a = await enrolled();
    const b = await enrolled({ account: a.account });
    const socketA = await connect(a.credential);
    const socketB = await openSocket(url, await api.ticket(b.credential), {
      unanswering: true,
    });
    await socketA.until((message) => rosterOf(message).length === 2);
    await api.call("DELETE", `/devices/${b.deviceId}`, {
      bearer: a.credential,
    });
    expect((await socketB.closed).code).toBe(4102);
    expect(rosterOf(await socketA.until(isPresence))).toEqual([a.deviceId]);

    // The next roster, a relay to it, and its own messages leave it out.
    const c = await enrolled({ account: a.account });
    const socketC = await connect(c.credential);
    expect(rosterOf(await socketA.until(isPresence))).toEqual(
      [a.deviceId, c.deviceId].toSorted(),
    );
    socketA.send(JSON.stringify({ t: "relay", to: b.deviceId, frame: "x" }));
    expect(await socketA.until(isNack)).toMatchObject({ to: b.deviceId });
    socketB.send(JSON.stringify({ t: "relay", to: a.deviceId, frame: "b" }));
    socketC.send(JSON.stringify({ t: "relay", to: a.deviceId, frame: "c" }));
    expect(
      await socketA.until(
        (message) => (message as { t: string }).t === "relay",
      ),
    ).toMatchObject({ from: c.deviceId });
    for (const socket of [socketA, socketB, socketC]) socket.close();
  });

  it("closes a socket with 1009 for a message too big", async () => {
    const socket = await connect((await enrolled()).credential);
    socket.send(
      JSON.stringify({
        t: "relay",
        to: newDeviceId(),
        frame: "x".repeat(65_536),
      }),
    );
    expect((await socket.closed).code).toBe(1009);
  });

  it("closes a socket with 4105 past its message budget", async () => {
    const socket = await connect((await enrolled()).credential);
    const message = JSON.stringify({
      t: "relay",
      to: newDeviceId(),
      frame: "x",
    });
    for (let i = 0; i < 400; i += 1) socket.send(message);
    expect((await socket.closed).code).toBe(4105);
  });
});
