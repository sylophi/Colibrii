import {
  newAccountId,
  newDeviceId,
  sessionToken,
  startService,
} from "@colibrii/harness";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";
import type * as Colibrii from "../src/index.ts";
import { CLOSE_TOO_BIG } from "../src/wire/hubSocket.ts";
import { deviceIdOf, eventually, makeKit, waitFor } from "./lib.ts";

const { url } = await startService();
const { spy, net, run, device, enroll, memoryStore, offline, silenced } =
  makeKit(url);

// The ids of the devices the account lists.
const listed = (target: Colibrii.Device) =>
  Effect.map(target.devices.list, (devices) =>
    devices.map((entry) => entry.deviceId),
  );

describe("enrolling", () => {
  it("enrolls, and enrolls again under the same device id", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const laptop = yield* device();
        expect((yield* waitFor(laptop.account, () => true))._tag).toBe(
          "SignedOut",
        );
        const first = yield* enroll(laptop, account, "Laptop");
        const again = yield* enroll(laptop, account, "Laptop again");
        expect(deviceIdOf(again)).toBe(deviceIdOf(first));
        const devices = yield* laptop.devices.list;
        expect(devices.map((entry) => [entry.deviceId, entry.name])).toEqual([
          [deviceIdOf(first), "Laptop again"],
        ]);
      }),
    ));

  it("starts from the store, and signs out", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const account = newAccountId();
        const enrolled = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* enroll(yield* device({ store }), account);
          }),
        );
        const later = yield* device({ store });
        const started = yield* waitFor(
          later.account,
          (state) => state._tag === "Enrolled" && state.connected,
        );
        expect(deviceIdOf(started)).toBe(deviceIdOf(enrolled));
        expect(yield* later.signOut).toBe(true);
        expect(yield* waitFor(later.account, () => true)).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
        expect(Option.isNone(yield* store.read)).toBe(true);
      }),
    ));

  it("signs out what the store holds, which another tab enrolled since", () =>
    run(
      Effect.gen(function* () {
        // Two tabs of one browser profile, which never hear of each
        // other's changes.
        const lock = Semaphore.makeUnsafe(1);
        const store = {
          ...memoryStore(),
          shared: { changes: Stream.never, exclusive: lock.withPermits(1) },
        };
        const account = newAccountId();
        const stale = yield* device({ store, connections: "multi" });
        yield* enroll(stale, account);
        const fresh = yield* device({ store, connections: "multi" });
        yield* enroll(fresh, account);
        yield* stale.signOut;
        expect(
          yield* waitFor(fresh.account, (state) => state._tag === "SignedOut"),
        ).toEqual({ _tag: "SignedOut", removed: false });
        expect(Option.isNone(yield* store.read)).toBe(true);
      }),
    ));
});

describe("signing out while the hub cannot be reached", () => {
  it("reads as signed out at once, and removes the device once made again", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        const laptopId = yield* Effect.scoped(
          Effect.gen(function* () {
            const laptop = yield* device({ store });
            const id = deviceIdOf(yield* enroll(laptop, account));
            expect(yield* offline(laptop.signOut)).toBe(false);
            expect(yield* waitFor(laptop.account, () => true)).toEqual({
              _tag: "SignedOut",
              removed: false,
            });
            return id;
          }),
        );
        expect(JSON.parse(Option.getOrThrow(yield* store.read))).toMatchObject({
          deviceId: laptopId,
          signedOut: true,
        });
        expect(yield* listed(desk)).toContain(laptopId);
        const later = yield* device({ store });
        expect(yield* waitFor(later.account, () => true)).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
        yield* eventually(store.read, Option.isNone);
        expect(yield* listed(desk)).not.toContain(laptopId);
      }),
    ));

  it(
    "gives up on a hub that never answers, and owes the removal",
    { timeout: 30_000 },
    () =>
      run(
        Effect.gen(function* () {
          const store = memoryStore();
          const laptop = yield* device({ store });
          yield* enroll(laptop, newAccountId());
          expect(yield* silenced(laptop.signOut)).toBe(false);
          expect(
            JSON.parse(Option.getOrThrow(yield* store.read)),
          ).toMatchObject({ signedOut: true });
        }),
      ),
  );

  it("removes the device on a wake", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        const laptop = yield* device({ store });
        const laptopId = deviceIdOf(yield* enroll(laptop, account));
        yield* offline(laptop.signOut);
        expect(yield* listed(desk)).toContain(laptopId);
        yield* laptop.wake;
        yield* eventually(store.read, Option.isNone);
        expect(yield* listed(desk)).not.toContain(laptopId);
      }),
    ));

  it("removes the device on the next sign-out, and says when it did", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        const laptop = yield* device({ store });
        const laptopId = deviceIdOf(yield* enroll(laptop, account));
        yield* offline(laptop.signOut);
        expect(yield* offline(laptop.signOut)).toBe(false);
        expect(Option.isSome(yield* store.read)).toBe(true);
        expect(yield* laptop.signOut).toBe(true);
        expect(Option.isNone(yield* store.read)).toBe(true);
        expect(yield* listed(desk)).not.toContain(laptopId);
        expect(yield* laptop.signOut).toBe(true);
      }),
    ));

  it("removes the device before it enrolls again", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        const deskId = deviceIdOf(yield* enroll(desk, account));
        const laptop = yield* device();
        yield* enroll(laptop, account);
        yield* offline(laptop.signOut);
        const again = deviceIdOf(yield* enroll(laptop, account));
        expect(yield* listed(desk)).toEqual([deskId, again]);
      }),
    ));

  it("enrolls again under the device id whose removal it still owes", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const laptop = yield* device();
        const laptopId = deviceIdOf(yield* enroll(laptop, account));
        yield* offline(laptop.signOut);
        // Every removal fails, the one tried before the enrollment too.
        net.divert = (request) =>
          request.method === "DELETE" ? "refused" : null;
        const again = yield* enroll(laptop, account).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              net.divert = () => null;
            }),
          ),
        );
        expect(deviceIdOf(again)).toBe(laptopId);
        expect(yield* listed(laptop)).toEqual([laptopId]);
      }),
    ));

  it.each(["a chosen", "a made-up"])(
    "enrolls %s device id under another account once the old one heard",
    (which) =>
      run(
        Effect.gen(function* () {
          const chosen = newDeviceId();
          const laptop = yield* device(
            which === "a chosen" ? { deviceId: chosen } : {},
          );
          const deviceId = deviceIdOf(yield* enroll(laptop, newAccountId()));
          yield* offline(laptop.signOut);
          // The removal tried before the enrollment fails, and the one
          // after its DeviceEnrolledElsewhere goes through.
          let deletes = 0;
          net.divert = (request) =>
            request.method === "DELETE" && (deletes += 1) === 1
              ? "refused"
              : null;
          const other = newAccountId();
          const enrolled = yield* enroll(laptop, other).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                net.divert = () => null;
              }),
            ),
          );
          expect(enrolled).toMatchObject({ accountId: other });
          // A made-up id goes with the removal the service heard.
          expect(deviceIdOf(enrolled) === deviceId).toBe(which === "a chosen");
          expect(deletes).toBe(2);
        }),
      ),
  );
});

describe("a device id the app chose", () => {
  it("enrolls under it, and keeps it across a sign-out", () =>
    run(
      Effect.gen(function* () {
        const deviceId = newDeviceId();
        const laptop = yield* device({ deviceId });
        expect(deviceIdOf(yield* enroll(laptop, newAccountId()))).toBe(
          deviceId,
        );
        yield* laptop.signOut;
        expect(deviceIdOf(yield* enroll(laptop, newAccountId()))).toBe(
          deviceId,
        );
      }),
    ));

  it("reads a store holding another device as signed out", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* enroll(yield* device({ store }), newAccountId());
          }),
        );
        const deviceId = newDeviceId();
        const laptop = yield* device({ store, deviceId });
        expect(yield* waitFor(laptop.account, () => true)).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
        expect(deviceIdOf(yield* enroll(laptop, newAccountId()))).toBe(
          deviceId,
        );
      }),
    ));

  it("refuses one that is not a lowercase UUID", () =>
    run(
      Effect.gen(function* () {
        const refused = yield* Effect.flip(
          device({ deviceId: newDeviceId().toUpperCase() }),
        );
        expect(refused).toMatchObject({
          _tag: "InvalidInput",
          field: "deviceId",
        });
      }),
    ));
});

describe("the device list", () => {
  it("lists, renames, and replaces device data", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        const phone = yield* device();
        const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
        const phoneId = deviceIdOf(yield* enroll(phone, account, "Phone"));
        yield* desk.devices.rename(phoneId, "Pocket");
        yield* desk.devices.setData(deskId, { icon: "desktop" });
        const devices = yield* phone.devices.list;
        expect(
          devices.map(({ deviceId, name, data, online }) => ({
            deviceId,
            name,
            data,
            online,
          })),
        ).toEqual([
          {
            deviceId: deskId,
            name: "Desk",
            data: { icon: "desktop" },
            online: true,
          },
          { deviceId: phoneId, name: "Pocket", data: {}, online: true },
        ]);
        const refused = yield* Effect.flip(desk.devices.rename(phoneId, ""));
        expect(refused._tag).toBe("InvalidInput");
      }),
    ));

  it("reads as signed out once another device removes it", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        const store = memoryStore();
        const phone = yield* device({ store });
        yield* enroll(desk, account);
        const phoneId = deviceIdOf(yield* enroll(phone, account));
        yield* desk.devices.remove(phoneId);
        expect(
          yield* waitFor(phone.account, (state) => state._tag === "SignedOut"),
        ).toEqual({ _tag: "SignedOut", removed: true });
        expect(JSON.parse(Option.getOrThrow(yield* store.read))).toEqual({
          deviceId: phoneId,
          removed: true,
        });
        expect((yield* Effect.flip(phone.devices.list))._tag).toBe(
          "NotEnrolled",
        );
        expect(yield* phone.signOut).toBe(true);
        expect(yield* waitFor(phone.account, () => true)).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
        expect(Option.isNone(yield* store.read)).toBe(true);
      }),
    ));

  it("stops a single device when a second copy of it connects", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const first = yield* device({ store });
        const enrolled = yield* enroll(first, newAccountId());
        const second = yield* device({ store });
        yield* waitFor(
          second.account,
          (state) => state._tag === "Enrolled" && state.connected,
        );
        expect(
          yield* waitFor(first.account, (state) => state._tag === "Stopped"),
        ).toEqual({
          _tag: "Stopped",
          deviceId: deviceIdOf(enrolled),
          accountId: (enrolled as { accountId: string }).accountId,
          reason: "superseded",
        });
      }),
    ));
});

describe("a removal", () => {
  const REMOVED = { _tag: "SignedOut", removed: true };

  // A device in `store` that another device of the account removed
  // while it ran.
  const removedIn = (store: Colibrii.CredentialStore["Service"]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        const phone = yield* device({ store });
        yield* desk.devices.remove(deviceIdOf(yield* enroll(phone, account)));
        yield* waitFor(phone.account, (state) => state._tag === "SignedOut");
        return account;
      }),
    );

  it("lasts into the next run on the same store", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        yield* removedIn(store);
        const later = yield* device({ store });
        expect(yield* waitFor(later.account, () => true)).toEqual(REMOVED);
      }),
    ));

  it("is kept by a device that learns of it as it starts", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        const phoneId = yield* Effect.scoped(
          Effect.gen(function* () {
            return deviceIdOf(yield* enroll(yield* device({ store }), account));
          }),
        );
        yield* desk.devices.remove(phoneId);
        const phone = yield* device({ store });
        yield* waitFor(phone.account, (state) => state._tag === "SignedOut");
        const later = yield* device({ store });
        expect(yield* waitFor(later.account, () => true)).toEqual(REMOVED);
      }),
    ));

  it("is cleared by enrolling", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        const account = yield* removedIn(store);
        const removed = JSON.parse(Option.getOrThrow(yield* store.read));
        const phone = yield* device({ store });
        const enrolled = yield* enroll(phone, account);
        expect(deviceIdOf(enrolled)).toBe(removed.deviceId);
        const later = yield* device({ store });
        expect(
          yield* waitFor(
            later.account,
            (state) => state._tag === "Enrolled" && state.connected,
          ),
        ).toMatchObject({ deviceId: deviceIdOf(enrolled) });
      }),
    ));

  it("is cleared by signing out", () =>
    run(
      Effect.gen(function* () {
        const store = memoryStore();
        yield* removedIn(store);
        const phone = yield* device({ store });
        expect(yield* phone.signOut).toBe(true);
        const later = yield* device({ store });
        expect(yield* waitFor(later.account, () => true)).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
      }),
    ));

  it("runs the enrollment another run made since", () =>
    run(
      Effect.gen(function* () {
        // Two tabs of one browser profile, which never hear of each
        // other's changes.
        const lock = Semaphore.makeUnsafe(1);
        const store = {
          ...memoryStore(),
          shared: { changes: Stream.never, exclusive: lock.withPermits(1) },
        };
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        const slow = yield* device({ store, connections: "multi" });
        const phoneId = deviceIdOf(yield* enroll(slow, account));
        const slowSocket = spy.opened.findLast((entry) =>
          entry.url.includes("/connect?"),
        );
        if (slowSocket !== undefined) slowSocket.deaf = true;
        const quick = yield* device({ store, connections: "multi" });
        yield* waitFor(
          quick.account,
          (state) => state._tag === "Enrolled" && state.connected,
        );
        yield* desk.devices.remove(phoneId);
        yield* waitFor(quick.account, (state) => state._tag === "SignedOut");
        const enrolled = yield* enroll(quick, account);
        // The old credential, which the service no longer takes.
        yield* Effect.flip(slow.devices.list);
        expect(
          yield* waitFor(
            slow.account,
            (state) => state._tag === "Enrolled" && state.connected,
          ),
        ).toMatchObject({ deviceId: deviceIdOf(enrolled) });
        expect(JSON.parse(Option.getOrThrow(yield* store.read))).toMatchObject({
          deviceId: deviceIdOf(enrolled),
        });
      }),
    ));

  it("reads as signed out where another run signed out since", () =>
    run(
      Effect.gen(function* () {
        const lock = Semaphore.makeUnsafe(1);
        const store = {
          ...memoryStore(),
          shared: { changes: Stream.never, exclusive: lock.withPermits(1) },
        };
        const account = newAccountId();
        const slow = yield* device({ store, connections: "multi" });
        yield* enroll(slow, account);
        const slowSocket = spy.opened.findLast((entry) =>
          entry.url.includes("/connect?"),
        );
        if (slowSocket !== undefined) slowSocket.deaf = true;
        const quick = yield* device({ store, connections: "multi" });
        yield* waitFor(
          quick.account,
          (state) => state._tag === "Enrolled" && state.connected,
        );
        expect(yield* quick.signOut).toBe(true);
        expect((yield* Effect.flip(slow.devices.list))._tag).toBe(
          "DeviceRevoked",
        );
        expect(yield* waitFor(slow.account, () => true)).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
        expect(Option.isNone(yield* store.read)).toBe(true);
      }),
    ));
});

describe("the hub socket", () => {
  it("follows the roster as devices come and go", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const phone = yield* device();
            const phoneId = deviceIdOf(yield* enroll(phone, account));
            const roster = yield* waitFor(
              desk.roster,
              (peers) => peers.length > 0,
            );
            expect(roster.map((peer) => peer.deviceId)).toEqual([phoneId]);
            expect(roster[0]?.publicKey.length).toBe(32);
          }),
        );
        yield* waitFor(desk.roster, (peers) => peers.length === 0);
      }),
    ));

  it("connects again after the service drops it", () =>
    run(
      Effect.gen(function* () {
        const desk = yield* device();
        yield* enroll(desk, newAccountId());
        const hubSocket = spy.opened.findLast((entry) =>
          entry.url.includes("/connect?"),
        );
        // A message past the cap, which the service closes the socket
        // for.
        hubSocket?.conn.send("x".repeat(70_000));
        expect(
          (yield* hubSocket?.conn.closed ?? Effect.die("no socket")).code,
        ).toBe(CLOSE_TOO_BIG);
        yield* waitFor(
          desk.account,
          (state) => state._tag === "Enrolled" && !state.connected,
        );
        yield* waitFor(
          desk.account,
          (state) => state._tag === "Enrolled" && state.connected,
        );
      }),
    ));

  it("refuses a login token for another app's Clerk instance", () =>
    run(
      Effect.gen(function* () {
        const desk = yield* device();
        const loginToken = yield* Effect.promise(() =>
          sessionToken(newAccountId(), "inkwell://app", { key: "b" }),
        );
        const refused = yield* Effect.flip(
          desk.enroll({ loginToken, name: "Desk" }),
        );
        expect(refused._tag).toBe("LoginRejected");
      }),
    ));
});
