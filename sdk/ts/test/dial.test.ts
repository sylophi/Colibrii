import { createServer, connect, type AddressInfo } from "node:net";
import {
  client,
  newAccountId,
  openSocket,
  sessionToken,
  startService,
} from "@colibrii/harness";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scheduler from "effect/Scheduler";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import * as Colibrii from "../src/index.ts";
import { layerListenerSockets, layerWebSockets } from "../src/node/sockets.ts";
import * as Base64Url from "../src/wire/base64url.ts";
import {
  CLOSE_HANDSHAKE_REFUSED,
  CLOSE_TOO_BIG,
  makeLinkTicket,
  MAX_APP_MESSAGE_BYTES,
} from "../src/wire/link.ts";
import { generateKeyPair } from "../src/wire/noise.ts";
import {
  decodeRelayFrame,
  encodeRelayFrame,
  openAsk,
  sealAnswer,
} from "../src/wire/relay.ts";
import {
  deviceIdOf,
  hasLanAddress,
  makeKit,
  makeSpy,
  type Spy,
  WAIT,
  waitFor,
} from "./lib.ts";

const { url } = await startService();
const { spy, run, device, enroll } = makeKit(url);

const bytes = (text: string) => new TextEncoder().encode(text);
const text = (data: Uint8Array) => new TextDecoder().decode(data);

// Past one piece (65,518 bytes), so it goes in several.
const big = Uint8Array.from({ length: 200_000 }, (_, index) => index % 251);

// What a test puts in place of a device's own services.
type Override = Layer.Layer<never, never, Colibrii.WebSockets>;

// A listening desk and a phone that dials it, both enrolled and each in
// the other's roster. The desk welcomes every hello, unless told to
// refuse.
const pair = (
  admit?: Colibrii.ListenOptions["admit"],
  overrides: { readonly desk?: Override; readonly phone?: Override } = {},
) =>
  Effect.gen(function* () {
    const account = newAccountId();
    const links = yield* Queue.unbounded<Colibrii.Link>();
    const desk = yield* device({
      listen: {
        admit:
          admit ??
          ((hello) => Effect.succeed(bytes(`welcome ${text(hello.appHello)}`))),
        onLink: (link) => Queue.offer(links, link).pipe(Effect.asVoid),
      },
    }).pipe(Effect.provide(overrides.desk ?? Layer.empty));
    const phone = yield* device().pipe(
      Effect.provide(overrides.phone ?? Layer.empty),
    );
    const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
    const phoneId = deviceIdOf(yield* enroll(phone, account, "Phone"));
    yield* waitFor(phone.roster, (peers) =>
      peers.some((peer) => peer.deviceId === deskId),
    );
    yield* waitFor(desk.roster, (peers) =>
      peers.some((peer) => peer.deviceId === phoneId),
    );
    return { account, desk, phone, deskId, phoneId, links };
  });

// Listening that welcomes every hello, and hands each link to `links`.
const listenInto = (
  links: Queue.Queue<Colibrii.Link>,
): Colibrii.ListenOptions => ({
  admit: () => Effect.succeed(new Uint8Array()),
  onLink: (link) => Queue.offer(links, link).pipe(Effect.asVoid),
});

// A socket of the test's own to the listener, which waits for how it
// ends: refused at the upgrade with a status, or closed with a code.
const rawSocket = (
  target: string,
  options: { readonly origin?: string; readonly first?: Uint8Array | string },
) =>
  new Promise<{ readonly status?: number; readonly code?: number }>(
    (resolve) => {
      const ws = new WebSocket(
        target,
        options.origin === undefined ? {} : { origin: options.origin },
      );
      ws.on("unexpected-response", (request, response) => {
        request.destroy();
        resolve({ status: response.statusCode ?? 0 });
      });
      ws.on("error", () => {});
      ws.on("open", () => {
        if (options.first !== undefined) ws.send(options.first);
      });
      ws.on("close", (code) => resolve({ code }));
    },
  );

// A listener of the test's own, enrolled through the service's API, which
// answers each ask with whatever candidates it is told to. `publicKey`
// enrolls it under a key that is not its own.
const lyingListener = (account: string, publicKey?: Uint8Array) =>
  Effect.promise(async () => {
    const api = client(url);
    const key = generateKeyPair();
    const enrolled = await api.enroll({
      token: await sessionToken(account),
      publicKey: Base64Url.encode(publicKey ?? key.publicKey),
    });
    const socket = await openSocket(url, await api.ticket(enrolled.credential));
    const answerNext = (candidates: unknown) =>
      Effect.promise(async () => {
        const relay = (await socket.until(
          (message) => (message as { t?: string }).t === "relay",
        )) as { from: string; frame: string };
        const frame = Option.getOrThrow(decodeRelayFrame(relay.frame));
        const opened = Option.getOrThrow(
          openAsk({
            asker: relay.from,
            asked: enrolled.deviceId,
            key,
            sealed: frame.sealed,
          }),
        );
        const answer = JSON.stringify({
          ok: true,
          result: { available: true, candidates },
        });
        socket.send(
          JSON.stringify({
            t: "relay",
            to: relay.from,
            frame: encodeRelayFrame({
              kind: "answer",
              id: frame.id,
              sealed: sealAnswer(opened.handshake, bytes(answer)),
            }),
          }),
        );
      });
    return { deviceId: enrolled.deviceId, answerNext, close: socket.close };
  });

const lan = (address: string) => ({
  kind: "lan",
  url: address,
  ticket: makeLinkTicket(),
});
const tunnel = (address: string) => ({
  kind: "tunnel",
  url: address,
  ticket: makeLinkTicket(),
});

// The candidate socket the phone won with: the one it sent a hello on.
const winner = () => {
  const candidates = spy.opened.filter(
    (entry) => !entry.url.includes("/connect?"),
  );
  const won = candidates.findLast((entry) => entry.sent.length > 1);
  if (won === undefined) throw new Error("no candidate socket sent a hello");
  return won;
};

// The hub socket a device opened last through `target`.
const hubSocket = (target: Spy) =>
  target.opened.findLast((entry) => entry.url.includes("/connect?"));

// The device's hub socket closes, and the device never hears of it.
const closeUnheard = (target: Spy) => {
  const hub = hubSocket(target);
  if (hub === undefined) return;
  hub.deaf = true;
  hub.conn.close(1000, "down");
};

// An admit that holds each hello until `decided`, and says in `deciding`
// that one came.
const held = Effect.gen(function* () {
  const deciding = yield* Deferred.make<void>();
  const decided = yield* Deferred.make<void>();
  const admit: Colibrii.ListenOptions["admit"] = () =>
    Deferred.succeed(deciding, undefined).pipe(
      Effect.andThen(Deferred.await(decided)),
      Effect.as(new Uint8Array()),
    );
  return { deciding, decided, admit };
});

// Whether a link's messages end within 2 seconds, in the background.
const messagesEnd = (link: Colibrii.Link) =>
  link.messages.pipe(
    Stream.runDrain,
    Effect.timeoutOption("2 seconds"),
    Effect.forkChild,
  );

describe.runIf(hasLanAddress)("a link between two devices", () => {
  it("carries app messages both ways, one of them in several pieces", () =>
    run(
      Effect.gen(function* () {
        const { phone, deskId, phoneId, links } = yield* pair();
        const dialed = yield* phone.dial(deskId, bytes("hi"));
        const admitted = yield* Queue.take(links);
        expect(dialed).toMatchObject({
          deviceId: deskId,
          role: "dialer",
          arrival: "lan",
        });
        expect(admitted).toMatchObject({
          deviceId: phoneId,
          role: "listener",
          arrival: "lan",
          connectionId: dialed.connectionId,
        });
        expect(text(dialed.greeting)).toBe("welcome hi");
        expect(text(admitted.greeting)).toBe("hi");

        yield* dialed.send(bytes("one"));
        yield* dialed.send(big);
        yield* admitted.send(big);
        yield* admitted.send(bytes("two"));
        const atDesk = yield* admitted.messages.pipe(
          Stream.take(2),
          Stream.runCollect,
        );
        const atPhone = yield* dialed.messages.pipe(
          Stream.take(2),
          Stream.runCollect,
        );
        expect(text(atDesk[0] as Uint8Array)).toBe("one");
        expect(atDesk[1]).toEqual(big);
        expect(atPhone[0]).toEqual(big);
        expect(text(atPhone[1] as Uint8Array)).toBe("two");

        yield* dialed.close;
        expect(yield* dialed.closed).toEqual({ code: 1000, cause: "closed" });
        expect(yield* admitted.closed).toEqual({ code: 1000, cause: "peer" });
      }),
    ));

  it("keeps both links up when two listening single devices dial each other", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const atDesk = yield* Queue.unbounded<Colibrii.Link>();
        const atLaptop = yield* Queue.unbounded<Colibrii.Link>();
        const desk = yield* device({ listen: listenInto(atDesk) });
        const laptop = yield* device({ listen: listenInto(atLaptop) });
        const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
        const laptopId = deviceIdOf(yield* enroll(laptop, account, "Laptop"));
        yield* waitFor(desk.roster, (peers) =>
          peers.some((peer) => peer.deviceId === laptopId),
        );
        yield* waitFor(laptop.roster, (peers) =>
          peers.some((peer) => peer.deviceId === deskId),
        );
        const toLaptop = yield* desk.dial(laptopId, bytes("desk"));
        yield* Queue.take(atLaptop);
        const toDesk = yield* laptop.dial(deskId, bytes("laptop"));
        yield* Queue.take(atDesk);
        expect(
          yield* Effect.timeoutOption(toLaptop.closed, "1 second"),
        ).toEqual(Option.none());
        expect(yield* Effect.timeoutOption(toDesk.closed, "1 second")).toEqual(
          Option.none(),
        );
      }),
    ));

  it("is dialed at the port in front of the listener, through what is there", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const links = yield* Queue.unbounded<Colibrii.Link>();
        // A TCP front that passes each connection on to the desk's
        // listener, wherever it bound.
        let bound = 0;
        let carried = 0;
        const front = yield* Effect.acquireRelease(
          Effect.promise(
            () =>
              new Promise<ReturnType<typeof createServer>>((listening) => {
                const server = createServer((socket) => {
                  carried += 1;
                  const upstream = connect(bound, socket.localAddress);
                  socket.pipe(upstream);
                  upstream.pipe(socket);
                  upstream.on("error", () => socket.destroy());
                  socket.on("error", () => upstream.destroy());
                });
                server.listen(0, () => listening(server));
              }),
          ),
          (server) => Effect.sync(() => server.close()),
        );
        const frontPort = (front.address() as AddressInfo).port;
        const binding = Layer.effect(
          Colibrii.ListenerSockets,
          Effect.map(Colibrii.ListenerSockets, (real) => ({
            ...real,
            listen: (options: Parameters<typeof real.listen>[0]) =>
              Effect.tap(real.listen(options), (port) =>
                Effect.sync(() => {
                  bound = port;
                }),
              ),
          })),
        ).pipe(Layer.provide(layerListenerSockets));
        const desk = yield* device({
          listen: { ...listenInto(links), advertisedPort: frontPort },
        }).pipe(Effect.provide(binding));
        const phone = yield* device();
        const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
        yield* enroll(phone, account, "Phone");
        yield* waitFor(phone.roster, (peers) =>
          peers.some((peer) => peer.deviceId === deskId),
        );
        yield* phone.dial(deskId, bytes("hi"));
        yield* Queue.take(links);
        expect(new URL(winner().url).port).toBe(String(frontPort));
        expect(carried).toBeGreaterThan(0);
      }),
    ));

  it("sends a message whole or not at all when its send is interrupted", () =>
    run(
      Effect.gen(function* () {
        const { phone, deskId, links } = yield* pair();
        const dialed = yield* phone.dial(deskId, bytes("hi"));
        const admitted = yield* Queue.take(links);
        const largest = new Uint8Array(MAX_APP_MESSAGE_BYTES);
        const socket = winner();
        const before = socket.sent.length;
        // A fiber that yields as often as it may, interrupted once a
        // piece went.
        const sending = yield* dialed
          .send(largest)
          .pipe(
            Effect.provideService(Scheduler.MaxOpsBeforeYield, 1),
            Effect.forkChild,
          );
        while (socket.sent.length === before) yield* Effect.yieldNow;
        yield* Fiber.interrupt(sending);
        yield* dialed.send(bytes("next"));
        const next = yield* admitted.messages.pipe(
          Stream.filter((message) => message.length !== largest.length),
          Stream.runHead,
        );
        expect(Option.map(next, text)).toEqual(Option.some("next"));
      }),
    ));

  it("ends its messages as it closes, however it closed", () =>
    run(
      Effect.gen(function* () {
        const { phone, deskId, links } = yield* pair();
        const dialed = yield* phone.dial(deskId, bytes("hi"));
        const admitted = yield* Queue.take(links);
        const atPhone = yield* messagesEnd(dialed);
        const atDesk = yield* messagesEnd(admitted);
        yield* dialed.close;
        expect(yield* Fiber.join(atPhone)).toEqual(Option.some(undefined));
        expect(yield* Fiber.join(atDesk)).toEqual(Option.some(undefined));
      }),
    ));

  it("lets a dialed link go once it closed, before its scope does", () =>
    run(
      Effect.gen(function* () {
        const { phone, deskId } = yield* pair();
        const dialed = yield* phone.dial(deskId, bytes("hi"));
        const socket = winner();
        yield* dialed.close;
        yield* socket.conn.closed;
        const sent = socket.sent.length;
        // A link still kept would ping at once.
        yield* phone.wake;
        yield* phone.dial(deskId, bytes("again"));
        expect(socket.sent.length).toBe(sent);
      }),
    ));

  it("gives the dialer the app's refusal and its bytes", () =>
    run(
      Effect.gen(function* () {
        const { phone, deskId } = yield* pair(() =>
          Effect.fail(
            new Colibrii.AppRefusal({
              reason: "busy",
              appRefusal: bytes("later"),
            }),
          ),
        );
        const refused = yield* Effect.flip(phone.dial(deskId, bytes("hi")));
        expect(refused).toMatchObject({
          _tag: "LinkRefused",
          by: "app",
          reason: "busy",
        });
        expect(
          text(
            (refused as Colibrii.LinkRefusedError).appRefusal ??
              new Uint8Array(),
          ),
        ).toBe("later");
      }),
    ));

  it("refuses a socket from an origin it does not allow, and a ticket used before", () =>
    run(
      Effect.gen(function* () {
        const { phone, deskId } = yield* pair();
        yield* phone.dial(deskId, bytes("hi"));
        const won = winner();
        const refused = yield* Effect.promise(() =>
          rawSocket(won.url, { origin: "https://elsewhere.example" }),
        );
        expect(refused.status).toBe(401);
        const replayed = yield* Effect.promise(() =>
          rawSocket(won.url, { first: won.sent[0] as Uint8Array }),
        );
        expect(replayed.code).toBe(CLOSE_HANDSHAKE_REFUSED);
      }),
    ));

  it("closes when the peer is removed", () =>
    run(
      Effect.gen(function* () {
        const { desk, phone, deskId, phoneId, links } = yield* pair();
        const dialed = yield* phone.dial(deskId, bytes("hi"));
        const admitted = yield* Queue.take(links);
        yield* desk.devices.remove(phoneId);
        // The phone's own stop closes both ends: the desk, still
        // settling, leaves the link to it.
        expect(yield* dialed.closed).toEqual({ code: 1001, cause: "stopped" });
        expect(yield* admitted.closed).toEqual({ code: 1001, cause: "peer" });
        yield* waitFor(phone.account, (state) => state._tag === "SignedOut");
      }),
    ));

  it("drops a hello whose socket closed while the app decided", () =>
    run(
      Effect.gen(function* () {
        const deciding = yield* Deferred.make<void>();
        const decided = yield* Deferred.make<void>();
        const { phone, deskId, links } = yield* pair((hello) =>
          text(hello.appHello) === "gone"
            ? Deferred.succeed(deciding, undefined).pipe(
                Effect.andThen(Deferred.await(decided)),
                Effect.as(new Uint8Array()),
              )
            : Effect.succeed(new Uint8Array()),
        );
        const kept = yield* phone.dial(deskId, bytes("kept"));
        const admitted = yield* Queue.take(links);
        const gone = yield* phone
          .dial(deskId, bytes("gone"))
          .pipe(Effect.scoped, Effect.forkChild);
        yield* Deferred.await(deciding);
        // Closes its sockets, and waits for their close.
        yield* Fiber.interrupt(gone);
        yield* Deferred.succeed(decided, undefined);
        // Welcomed, it would have replaced the phone's link.
        yield* kept.send(bytes("still"));
        const next = yield* admitted.messages.pipe(Stream.runHead);
        expect(Option.map(next, text)).toEqual(Option.some("still"));
      }),
    ));

  it.each(["listener", "dialer"] as const)(
    "takes no link once the roster gives the %s another key while the app decides",
    (withdrawn) =>
      run(
        Effect.gen(function* () {
          const { deciding, decided, admit } = yield* held;
          const deskSpy = makeSpy();
          const phoneSpy = makeSpy();
          const { account, desk, phone, deskId, phoneId, links } = yield* pair(
            admit,
            { desk: deskSpy.layer, phone: phoneSpy.layer },
          );
          const dialing = yield* phone
            .dial(deskId, bytes("hi"))
            .pipe(Effect.flip, Effect.forkChild);
          yield* Deferred.await(deciding);

          // The withdrawn end enrolls again as another copy. The old one,
          // its hub socket deaf, never hears of it and keeps its key and
          // its roster, as a copy left behind would.
          const [staleSpy, staleId, other] =
            withdrawn === "listener"
              ? [deskSpy, deskId, phone]
              : [phoneSpy, phoneId, desk];
          for (const entry of staleSpy.opened) {
            entry.deaf = entry.url.includes("/connect?");
          }
          const oldKey = (yield* waitFor(other.roster, () => true)).find(
            (peer) => peer.deviceId === staleId,
          )?.publicKey;
          yield* enroll(yield* device({ deviceId: staleId }), account);
          yield* waitFor(other.roster, (peers) =>
            peers.some(
              (peer) =>
                peer.deviceId === staleId &&
                Base64Url.encode(peer.publicKey) !==
                  Base64Url.encode(oldKey ?? new Uint8Array()),
            ),
          );
          yield* Deferred.succeed(decided, undefined);
          expect(yield* Fiber.join(dialing)).toMatchObject(
            withdrawn === "listener"
              ? { _tag: "DialError", reason: "offline" }
              : { _tag: "LinkRefused", by: "colibrii" },
          );
          if (withdrawn === "dialer") expect(yield* Queue.size(links)).toBe(0);
        }),
      ),
  );

  it.each(["listener", "dialer"] as const)(
    "takes no link while the %s's hub socket is down",
    (down) =>
      run(
        Effect.gen(function* () {
          const { deciding, decided, admit } = yield* held;
          const deskSpy = makeSpy();
          const phoneSpy = makeSpy();
          const { desk, phone, deskId, links } = yield* pair(admit, {
            desk: deskSpy.layer,
            phone: phoneSpy.layer,
          });
          const dialing = yield* phone
            .dial(deskId, bytes("hi"))
            .pipe(Effect.flip, Effect.forkChild);
          yield* Deferred.await(deciding);

          // The phone leaving the service would have the desk refuse it,
          // so the desk keeps its roster: its hub socket closes unheard.
          if (down === "dialer") closeUnheard(deskSpy);
          const [downSpy, downDevice] =
            down === "listener" ? [deskSpy, desk] : [phoneSpy, phone];
          downSpy.refusing = true;
          hubSocket(downSpy)?.conn.close(1000, "down");
          yield* waitFor(
            downDevice.account,
            (state) => state._tag === "Enrolled" && !state.connected,
          );
          yield* Deferred.succeed(decided, undefined);

          const failed = yield* Fiber.join(dialing);
          if (down === "listener") {
            expect(failed).toMatchObject({
              _tag: "DialError",
              reason: "unreachable",
            });
            expect(
              (failed as Colibrii.DialError).cause as ReadonlyArray<unknown>,
            ).toContainEqual(
              expect.objectContaining({
                reason: expect.objectContaining({ code: 1013 }),
              }),
            );
            expect(yield* Queue.size(links)).toBe(0);
          } else {
            expect(failed).toMatchObject({
              _tag: "DialError",
              reason: "not-connected",
            });
          }
        }),
      ),
  );

  it(
    "takes a link from a peer missing from a roster not yet settled, until it settles",
    { timeout: 30_000 },
    () =>
      run(
        Effect.gen(function* () {
          const { deciding, decided, admit } = yield* held;
          const deskSpy = makeSpy();
          const { phone, deskId } = yield* pair(admit, { desk: deskSpy.layer });
          const dialing = yield* phone
            .dial(deskId, bytes("hi"))
            .pipe(Effect.forkChild);
          yield* Deferred.await(deciding);

          // The desk drops off the service while its app decides, as
          // after a restart before it reconnected, and keeps its roster.
          closeUnheard(deskSpy);
          yield* waitFor(
            phone.roster,
            (peers) => !peers.some((peer) => peer.deviceId === deskId),
          );
          expect(yield* waitFor(phone.account, () => true)).toMatchObject({
            connected: true,
            settled: false,
          });
          yield* Deferred.succeed(decided, undefined);
          const link = yield* Fiber.join(dialing);

          yield* phone.account.pipe(
            Stream.filter(
              (state) => state._tag === "Enrolled" && state.settled,
            ),
            Stream.runHead,
            Effect.timeout("12 seconds"),
          );
          expect(yield* link.closed.pipe(Effect.timeout("1 second"))).toEqual({
            code: 1001,
            cause: "left-roster",
          });
        }),
      ),
  );

  it("ends a dial still under way when the device signs out", () =>
    run(
      Effect.gen(function* () {
        const deciding = yield* Deferred.make<void>();
        const { phone, deskId } = yield* pair(() =>
          Deferred.succeed(deciding, undefined).pipe(
            Effect.andThen(Effect.never),
          ),
        );
        const dialing = yield* phone
          .dial(deskId, bytes("hi"))
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(deciding);
        yield* phone.signOut;
        expect(
          yield* Fiber.join(dialing).pipe(Effect.timeout("1 second")),
        ).toMatchObject({ _tag: "DialError", reason: "not-connected" });
      }),
    ));

  it("keeps the socket a dial won when interrupted as the others close", () =>
    run(
      Effect.gen(function* () {
        // Each address twice, so the dial has a loser to close.
        const twice = Layer.effect(
          Colibrii.ListenerSockets,
          Effect.map(Colibrii.ListenerSockets, (real) => ({
            ...real,
            addresses: Effect.map(real.addresses, (found) => [
              ...found,
              ...found,
            ]),
          })),
        ).pipe(Layer.provide(layerListenerSockets));
        // Candidate sockets that take a while to close, as in Node.
        const closing = yield* Deferred.make<void>();
        const slow = Layer.effect(
          Colibrii.WebSockets,
          Effect.map(Colibrii.WebSockets, (real) => ({
            ...real,
            open: (target: string) =>
              target.includes("/connect?")
                ? real.open(target)
                : Effect.addFinalizer(() =>
                    Deferred.succeed(closing, undefined).pipe(
                      Effect.andThen(Effect.sleep("300 millis")),
                    ),
                  ).pipe(Effect.andThen(real.open(target))),
          })),
        ).pipe(Layer.provide(layerWebSockets));
        const { phone, deskId, links } = yield* pair(undefined, {
          desk: twice,
          phone: slow,
        });
        const dialing = yield* phone
          .dial(deskId, bytes("hi"))
          .pipe(Effect.forkChild);
        yield* Deferred.await(closing);
        yield* Fiber.interrupt(dialing);
        const admitted = yield* Queue.take(links);
        // The phone's own end closes it, before the desk's roster
        // drops the phone.
        yield* phone.signOut;
        expect(yield* admitted.closed.pipe(Effect.timeout(WAIT))).toEqual({
          code: 1001,
          cause: "peer",
        });
      }),
    ));

  it(
    "keeps a link to a peer missing from the roster until the roster settled",
    { timeout: 30_000 },
    () =>
      run(
        Effect.gen(function* () {
          const account = newAccountId();
          const links = yield* Queue.unbounded<Colibrii.Link>();
          const desk = yield* device({
            listen: {
              admit: () => Effect.succeed(new Uint8Array()),
              onLink: (link) => Queue.offer(links, link).pipe(Effect.asVoid),
            },
          });
          const phoneSpy = makeSpy();
          const phone = yield* device().pipe(Effect.provide(phoneSpy.layer));
          const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
          const phoneId = deviceIdOf(yield* enroll(phone, account, "Phone"));
          yield* waitFor(phone.roster, (peers) =>
            peers.some((peer) => peer.deviceId === deskId),
          );
          yield* waitFor(desk.roster, (peers) =>
            peers.some((peer) => peer.deviceId === phoneId),
          );
          yield* phone.dial(deskId, bytes("hi"));
          const admitted = yield* Queue.take(links);

          // The phone's hub socket goes down and stays down, as a device
          // still reconnecting after the service restarted.
          phoneSpy.refusing = true;
          hubSocket(phoneSpy)?.conn.close(1000, "down");
          yield* waitFor(desk.roster, (peers) => peers.length === 0);
          expect(yield* waitFor(desk.account, () => true)).toMatchObject({
            connected: true,
            settled: false,
          });
          expect(
            yield* Effect.timeoutOption(admitted.closed, "1 second"),
          ).toEqual(Option.none());

          yield* desk.account.pipe(
            Stream.filter(
              (state) => state._tag === "Enrolled" && state.settled,
            ),
            Stream.runHead,
            Effect.timeout("12 seconds"),
          );
          expect(
            yield* admitted.closed.pipe(Effect.timeout("1 second")),
          ).toEqual({ code: 1001, cause: "left-roster" });
        }),
      ),
  );

  it("closes when the roster gives the peer another key", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device({
          listen: {
            admit: () => Effect.succeed(new Uint8Array()),
            onLink: () => Effect.void,
          },
        });
        const phoneSpy = makeSpy();
        const phone = yield* device().pipe(Effect.provide(phoneSpy.layer));
        const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
        yield* enroll(phone, account, "Phone");
        yield* waitFor(phone.roster, (peers) =>
          peers.some((peer) => peer.deviceId === deskId),
        );
        yield* waitFor(desk.roster, (peers) => peers.length > 0);
        const dialed = yield* phone.dial(deskId, bytes("hi"));

        // The phone's hub socket goes down and stays down while the desk
        // enrolls again, and the desk's close of the link never reaches
        // it, as over a network that dropped.
        for (const entry of phoneSpy.opened) {
          entry.deaf = !entry.url.includes("/connect?");
        }
        phoneSpy.refusing = true;
        hubSocket(phoneSpy)?.conn.close(1000, "down");
        yield* waitFor(
          phone.account,
          (state) => state._tag === "Enrolled" && !state.connected,
        );
        yield* enroll(desk, account, "Desk");
        phoneSpy.refusing = false;
        yield* phone.wake;
        expect(yield* dialed.closed.pipe(Effect.timeout(WAIT))).toEqual({
          code: 1001,
          cause: "left-roster",
        });
      }),
    ));
});

describe("dialing", () => {
  // A lying listener, and a phone and a page that dial it. `answer`
  // gives what the dial failed with and every URL it tried.
  const liars = Effect.gen(function* () {
    const account = newAccountId();
    const desk = yield* lyingListener(account);
    const phone = yield* device();
    const page = yield* device().pipe(
      Effect.updateService(Colibrii.WebSockets, (real) => ({
        ...real,
        dialableKinds: ["tunnel"] as const,
      })),
    );
    yield* enroll(phone, account);
    yield* enroll(page, account);
    for (const target of [phone, page]) {
      yield* waitFor(target.roster, (peers) =>
        peers.some((peer) => peer.deviceId === desk.deviceId),
      );
    }
    const answer = (
      dialer: Colibrii.Device,
      candidates: ReadonlyArray<unknown>,
    ) =>
      Effect.gen(function* () {
        const before = spy.tried.length;
        const [failed] = yield* Effect.all(
          [
            Effect.flip(dialer.dial(desk.deviceId, bytes("hi"))),
            desk.answerNext(candidates),
          ],
          { concurrency: "unbounded" },
        );
        return { failed, tried: spy.tried.slice(before) };
      });
    return { phone, page, answer, close: desk.close };
  });

  it("fails on a candidate the spec does not allow, and opens nothing", () =>
    run(
      Effect.gen(function* () {
        const { phone, page, answer, close } = yield* liars;
        for (const candidates of [
          [lan("ws://desk.example:41734")],
          [lan("ws://192.168.1.20")],
          [lan("ws://192.168.1.20:41734/")],
          [lan("ws://me@192.168.1.20:41734")],
          [lan("ws://192.168.1.256:41734")],
          [lan("ws://[2001:db8::20:41734")],
          [lan("wss://192.168.1.20:41734")],
          [tunnel("wss://desk.example.link/path")],
          [tunnel("wss://desk.example.link?x=1")],
          [tunnel("wss://desk.example.link#x")],
          [tunnel("wss://desk.example.link:8443")],
          [tunnel("ws://desk.example.link")],
          [lan("wss://desk.example.link")],
          Array.from({ length: 7 }, (_, index) =>
            lan(`ws://10.0.0.${index}:1`),
          ),
          [tunnel("wss://a.example.link"), tunnel("wss://b.example.link")],
        ]) {
          expect(yield* answer(phone, candidates)).toMatchObject({
            failed: { _tag: "DialError", reason: "unreachable" },
            tried: [],
          });
        }
        expect(
          yield* answer(page, [lan("ws://192.168.1.20:41734")]),
        ).toMatchObject({
          failed: { _tag: "DialError", reason: "unavailable" },
          tried: [],
        });
        close();
      }),
    ));

  it("skips a candidate of a kind it does not know or did not ask for, and dials the rest", () =>
    run(
      Effect.gen(function* () {
        const { phone, page, answer, close } = yield* liars;
        const later = {
          kind: "carrier-pigeon",
          url: "coo://192.168.1.20",
          ticket: "a ticket of another form",
        };
        expect(
          (yield* answer(phone, [
            later,
            lan("ws://127.0.0.1:1"),
            "a later form",
          ])).tried,
        ).toEqual(["ws://127.0.0.1:1"]);
        expect(
          (yield* answer(page, [
            lan("ws://127.0.0.1:1"),
            tunnel("wss://desk.invalid"),
            later,
          ])).tried,
        ).toEqual(["wss://desk.invalid"]);
        close();
      }),
    ));

  it("fails a dial to a device whose roster key no handshake can use", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        // All zeros is a point every X25519 exchange refuses.
        const desk = yield* lyingListener(account, new Uint8Array(32));
        const phone = yield* device();
        yield* enroll(phone, account);
        yield* waitFor(phone.roster, (peers) =>
          peers.some((peer) => peer.deviceId === desk.deviceId),
        );
        const failed = yield* Effect.flip(
          phone.dial(desk.deviceId, bytes("hi")),
        );
        expect(failed).toMatchObject({
          _tag: "DialError",
          reason: "unreachable",
        });
        desk.close();
      }),
    ));

  it("answers no-listener from a device that does not listen", () =>
    run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        const phone = yield* device();
        const deskId = deviceIdOf(yield* enroll(desk, account));
        yield* enroll(phone, account);
        yield* waitFor(phone.roster, (peers) => peers.length > 0);
        const failed = yield* Effect.flip(phone.dial(deskId, bytes("hi")));
        expect(failed).toMatchObject({
          _tag: "DialError",
          reason: "no-listener",
        });
      }),
    ));
});

describe("the listener's sockets", () => {
  it("close a socket that sends a message past the cap, before any handshake", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const closed = yield* Deferred.make<void>();
        const port = yield* (yield* Colibrii.ListenerSockets).listen({
          port: 0,
          allowOrigin: () => true,
          serve: ({ conn }) =>
            conn.closed.pipe(
              Effect.andThen(Deferred.succeed(closed, undefined)),
            ),
        });
        const told = new Promise<number>((resolve) => {
          const ws = new WebSocket(`ws://127.0.0.1:${port}`);
          ws.on("open", () => ws.send(new Uint8Array(70_000)));
          ws.on("close", resolve);
        });
        yield* Deferred.await(closed);
        expect(yield* Effect.promise(() => told)).toBe(CLOSE_TOO_BIG);
      }).pipe(Effect.scoped, Effect.provide(layerListenerSockets)),
    ));
});
