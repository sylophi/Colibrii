// The browser binding in a real Chrome, against the real service in
// workerd: a page served from here is a device of its own, beside
// devices in this process. A link from a page needs a peer's tunnel,
// which there is none of here, so no test opens one.
import { existsSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { newAccountId, sessionToken, startService } from "@colibrii/harness";
import { build } from "esbuild";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import {
  type Browser,
  type BrowserContext,
  chromium,
  type Page,
} from "playwright-core";
import {
  afterAll,
  beforeAll,
  describe,
  expect,
  it,
  onTestFinished,
} from "vitest";
import { deviceIdOf, makeKit, waitFor } from "./lib.ts";

const CHROMES = [
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "/usr/bin/google-chrome",
  "/usr/bin/chromium",
];
const chrome = CHROMES.find((path) => existsSync(path));

const STORE_KEY = "colibrii-device";

// What a test waits for in a page, at most.
const WAIT_MS = 8_000;

it("bundles the core and the binding for a browser, without Node's built-ins or ws", async () => {
  // A Node built-in fails the build for a browser.
  const { metafile } = await build({
    stdin: {
      contents: `export * from "@colibrii/sdk"; export * from "@colibrii/sdk/browser";`,
      resolveDir: join(import.meta.dirname, ".."),
    },
    bundle: true,
    platform: "browser",
    format: "esm",
    write: false,
    metafile: true,
    logLevel: "silent",
  });
  const inputs = Object.keys(metafile.inputs);
  expect(inputs.some((input) => input.includes("src/browser/"))).toBe(true);
  expect(
    inputs.filter((input) => /node_modules\/(\.pnpm\/)?ws[@/]/.test(input)),
  ).toEqual([]);
});

// Enrolls the tab under the account, with a token as the app's web
// client would have it.
const enrollIn = async (tab: Page, account: string) => {
  const token = await sessionToken(account, "https://inkwell.example");
  return tab.evaluate((t) => window.colibrii.enroll(t), token);
};

// The device id once the tab's hub socket is up.
const connected = (tab: Page) =>
  tab
    .waitForFunction(
      () => {
        const account = window.colibrii.state.account;
        return account?._tag === "Enrolled" && account.connected
          ? account.deviceId
          : false;
      },
      undefined,
      { timeout: WAIT_MS, polling: 50 },
    )
    .then((handle) => handle.jsonValue() as Promise<string>);

const signedOut = (tab: Page) =>
  tab
    .waitForFunction(
      () => {
        const account = window.colibrii.state.account;
        return account?._tag === "SignedOut" ? account : false;
      },
      undefined,
      { timeout: WAIT_MS, polling: 50 },
    )
    .then((handle) => handle.jsonValue());

const stored = (tab: Page) =>
  tab.evaluate((key) => localStorage.getItem(key), STORE_KEY);

const { url: hub } = await startService();
const { run, device, enroll } = makeKit(hub);

describe.runIf(chrome !== undefined)("a page", () => {
  let browser: Browser;
  let origin: string;
  const server = createServer();

  beforeAll(async () => {
    const { outputFiles } = await build({
      entryPoints: [join(import.meta.dirname, "browser/page.ts")],
      bundle: true,
      platform: "browser",
      format: "esm",
      write: false,
    });
    const script = outputFiles[0]?.contents ?? new Uint8Array();
    server.on("request", (request, response) => {
      if (request.url === "/page.js") {
        response.writeHead(200, { "content-type": "text/javascript" });
        response.end(script);
        return;
      }
      response.writeHead(200, { "content-type": "text/html" });
      response.end(
        `<!doctype html><script type="module" src="/page.js"></script>`,
      );
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    browser = await chromium.launch({ executablePath: chrome });
  });

  afterAll(async () => {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
  });

  // A browser profile of its own, closed when the test ends.
  const profile = async () => {
    const context = await browser.newContext();
    onTestFinished(() => context.close());
    return context;
  };

  const openTab = async (context: BrowserContext) => {
    const tab = await context.newPage();
    await tab.goto(
      `${origin}/?${new URLSearchParams({ hub, store: STORE_KEY })}`,
    );
    await tab.waitForFunction(() => window.colibrii !== undefined, undefined, {
      timeout: WAIT_MS,
    });
    return tab;
  };

  it("enrolls, and sees a Node device in its roster as the Node device sees it", async () => {
    const tab = await openTab(await profile());
    await run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
        expect(yield* Effect.promise(() => enrollIn(tab, account))).toEqual({
          ok: true,
        });
        const tabId = yield* Effect.promise(() => connected(tab));
        yield* Effect.promise(() =>
          tab.waitForFunction(
            (id) => window.colibrii.state.roster.includes(id),
            deskId,
            { timeout: WAIT_MS, polling: 50 },
          ),
        );
        const roster = yield* waitFor(desk.roster, (peers) =>
          peers.some((peer) => peer.deviceId === tabId),
        );
        expect(
          roster.find((peer) => peer.deviceId === tabId)?.connections,
        ).toBe("multi");
      }),
    );
  });

  it("enrolls two tabs at once as one multi device, each connected on a connection of its own", async () => {
    const context = await profile();
    const connectionIds = new Set<string>();
    context.on("request", (request) => {
      if (request.url().endsWith("/tickets")) {
        connectionIds.add(
          (request.postDataJSON() as { connectionId: string }).connectionId,
        );
      }
    });
    const first = await openTab(context);
    const second = await openTab(context);
    await run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account, "Desk");
        const both = yield* Effect.promise(() =>
          Promise.all([first, second].map((tab) => enrollIn(tab, account))),
        );
        expect(both).toEqual([{ ok: true }, { ok: true }]);
        const ids = yield* Effect.promise(() =>
          Promise.all([first, second].map(connected)),
        );
        expect(ids[0]).toBe(ids[1]);
        expect(connectionIds.size).toBe(2);
        expect((yield* desk.devices.list).length).toBe(2);
        // Neither replaces the other's socket.
        yield* Effect.sleep(Duration.millis(1_500));
        for (const tab of [first, second]) {
          const state = yield* Effect.promise(() =>
            tab.evaluate(() => window.colibrii.state),
          );
          expect(state.drops).toBe(0);
          expect(state.account).toMatchObject({ connected: true });
        }
      }),
    );
  });

  it("follows another tab's enrollment and sign-out", async () => {
    const context = await profile();
    const first = await openTab(context);
    const second = await openTab(context);
    expect(await enrollIn(first, newAccountId())).toEqual({ ok: true });
    const id = await connected(first);
    expect(await connected(second)).toBe(id);
    expect(await second.evaluate(() => window.colibrii.signOut())).toEqual({
      ok: true,
    });
    expect(await signedOut(first)).toEqual({
      _tag: "SignedOut",
      removed: false,
    });
    expect(await stored(first)).toBeNull();
  });

  it("signs out offline, and a tab removes the device once the network is back", async () => {
    const context = await profile();
    const first = await openTab(context);
    const second = await openTab(context);
    await run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account, "Desk");
        yield* Effect.promise(() => enrollIn(first, account));
        const tabId = yield* Effect.promise(() => connected(first));
        yield* Effect.promise(() => connected(second));
        yield* Effect.promise(() => context.setOffline(true));
        expect(
          yield* Effect.promise(() =>
            first.evaluate(() => window.colibrii.signOut()),
          ),
        ).toEqual({ ok: true });
        expect(yield* Effect.promise(() => signedOut(second))).toEqual({
          _tag: "SignedOut",
          removed: false,
        });
        expect(
          JSON.parse((yield* Effect.promise(() => stored(second))) ?? ""),
        ).toMatchObject({ deviceId: tabId, signedOut: true });
        yield* Effect.promise(() => context.setOffline(false));
        yield* Effect.promise(() =>
          second.waitForFunction(
            (key) => localStorage.getItem(key) === null,
            STORE_KEY,
            { timeout: WAIT_MS, polling: 50 },
          ),
        );
        const listed = yield* desk.devices.list;
        expect(listed.map((entry) => entry.deviceId)).not.toContain(tabId);
      }),
    );
  });

  it("reads as signed out and removed once a Node device removes it", async () => {
    const tab = await openTab(await profile());
    await run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device();
        yield* enroll(desk, account, "Desk");
        yield* Effect.promise(() => enrollIn(tab, account));
        const tabId = yield* Effect.promise(() => connected(tab));
        yield* desk.devices.remove(tabId);
        expect(yield* Effect.promise(() => signedOut(tab))).toEqual({
          _tag: "SignedOut",
          removed: true,
        });
        expect(yield* Effect.promise(() => stored(tab))).toBeNull();
      }),
    );
  });

  it("answers a Node device's dial with no-listener, and fails its own to a peer with no tunnel as unavailable", async () => {
    const tab = await openTab(await profile());
    await run(
      Effect.gen(function* () {
        const account = newAccountId();
        const desk = yield* device({
          listen: {
            admit: () => Effect.succeed(new Uint8Array()),
            onLink: () => Effect.void,
          },
        });
        const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
        yield* Effect.promise(() => enrollIn(tab, account));
        const tabId = yield* Effect.promise(() => connected(tab));
        yield* waitFor(desk.roster, (peers) =>
          peers.some((peer) => peer.deviceId === tabId),
        );
        expect(
          yield* Effect.flip(desk.dial(tabId, new Uint8Array())),
        ).toMatchObject({
          _tag: "DialError",
          reason: "no-listener",
        });
        const dialed = yield* Effect.promise(() =>
          tab.evaluate((id) => window.colibrii.dial(id), deskId),
        ).pipe(Effect.timeout(Duration.seconds(5)), Effect.orDie);
        expect(dialed).toEqual({
          ok: false,
          tag: "DialError",
          reason: "unavailable",
        });
      }),
    );
  });
});
