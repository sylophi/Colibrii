// The tunnel runner against a fake cloudflared (a Node script that
// notes each run and stays up until ended), with a local HTTP stub for
// the hostname and for `POST /tunnel`, whose real route needs
// Cloudflare. The fake runs as a script, so these run where a shebang
// does.
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { newAccountId, startService } from "@colibrii/harness";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { afterAll, describe, expect, it } from "vitest";
import * as Colibrii from "../src/index.ts";
import { pinnedPath } from "../src/node/cloudflared.ts";
import {
  endLeftoverCloudflared,
  makeTunnel,
  pidRecord,
  type Timings,
} from "../src/node/tunnel.ts";
import { deviceIdOf, eventually, makeKit, waitFor } from "./lib.ts";

const FAKE = `#!${process.execPath}
const { appendFileSync } = require("node:fs");
const { dirname, join } = require("node:path");
if (process.argv[2] === "--version") {
  console.log("cloudflared version fake");
  process.exit(0);
}
if (require("node:fs").existsSync(join(dirname(process.argv[1]), "registers"))) {
  console.error("INF Registered tunnel connection connIndex=0 protocol=quic");
}
appendFileSync(
  join(dirname(process.argv[1]), "runs.log"),
  JSON.stringify({
    pid: process.pid,
    args: process.argv.slice(2),
    token: process.env.TUNNEL_TOKEN,
    logsToFile: require("node:fs").fstatSync(2).isFile(),
  }) + "\\n",
);
setInterval(() => {}, 60_000);
`;

const TOKEN = "connector-token-for-tests";
const HOSTNAME = "a1b2c3.tunnel.test";

const FAST: Timings = {
  tryWithin: 500,
  freshTries: [150, 100],
  existingTries: [50, 100],
  routeWithin: 1_500,
  restarts: [100, 200],
  healthyFor: 60_000,
  stopGrace: 500,
};

const { url } = await startService();
const { run, device, enroll } = makeKit(url);

const root = await mkdtemp(join(tmpdir(), "colibrii-tunnel-"));
afterAll(() => rm(root, { recursive: true, force: true }));
let made = 0;

// A directory of its own, with the fake cloudflared as its pinned copy.
const directory = async () => {
  made += 1;
  const path = join(root, String(made));
  await mkdir(path);
  await writeFile(pinnedPath(path), FAKE, { mode: 0o755 });
  return path;
};

interface Run {
  readonly pid: number;
  readonly args: ReadonlyArray<string>;
  readonly token?: string;
  readonly logsToFile: boolean;
}

const runsIn = async (path: string): Promise<Array<Run>> =>
  (await readFile(join(path, "runs.log"), "utf8").catch(() => ""))
    .split("\n")
    .filter((line) => line !== "")
    .map((line) => JSON.parse(line) as Run);

const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

// Polls until `test` holds, at most WAIT.
const until = (test: () => boolean | Promise<boolean>) =>
  eventually(
    Effect.promise(async () => test()),
    (held) => held,
  );

// The hostname's edge and `POST /tunnel`, on this machine.
const stub = await new Promise<{
  origin: string;
  routing: { status: number };
  provisioned: Array<{ port: number; bearer: string | undefined }>;
}>((resolve) => {
  const routing = { status: 502 };
  const provisioned: Array<{ port: number; bearer: string | undefined }> = [];
  const server = createServer((request, response) => {
    if (request.method === "POST" && request.url === "/tunnel") {
      let body = "";
      request.on("data", (chunk: Buffer) => {
        body += chunk.toString();
      });
      request.on("end", () => {
        provisioned.push({
          port: (JSON.parse(body) as { port: number }).port,
          bearer: request.headers.authorization,
        });
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            hostname: HOSTNAME,
            connectorToken: TOKEN,
            fresh: false,
          }),
        );
      });
      return;
    }
    response.writeHead(routing.status).end();
  });
  server.listen(0, "127.0.0.1", () => {
    afterAll(() => new Promise<void>((done) => server.close(() => done())));
    resolve({
      origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
      routing,
      provisioned,
    });
  });
});

const probeUrl = (hostname: string) => `${stub.origin}/${hostname}`;

const provisionFor = (fresh: boolean) => {
  const counted = { provisions: 0 };
  const provision = Effect.sync(() => {
    counted.provisions += 1;
    return { hostname: HOSTNAME, connectorToken: TOKEN, fresh };
  });
  return { counted, provision };
};

const stateOf = (tunnel: Colibrii.RunningTunnel) =>
  Effect.map(
    Stream.runHead(tunnel.state),
    (state) => Option.getOrThrow(state).status,
  );

const status = (tunnel: Colibrii.RunningTunnel, wanted: string) =>
  waitFor(tunnel.state, (state) => state.status === wanted);

describe.skipIf(process.platform === "win32")("the tunnel runner", () => {
  it("finds no cloudflared, then prefers the app's over the pinned one", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* Effect.promise(() => directory());
        const { provision } = provisionFor(false);
        const nowhere = makeTunnel(
          { directory: join(path, "empty"), cloudflared: join(path, "none") },
          FAST,
          probeUrl,
        );
        const before = process.env.PATH;
        process.env.PATH = "";
        const unfound = yield* nowhere.run({ port: 4000, provision }).pipe(
          Effect.flatMap((tunnel) => status(tunnel, "no-binary")),
          Effect.ensuring(Effect.sync(() => (process.env.PATH = before))),
          Effect.scoped,
        );
        expect(unfound).toEqual({ status: "no-binary" });

        const appCopy = yield* Effect.promise(() => directory());
        const preferred = makeTunnel(
          { directory: path, cloudflared: pinnedPath(appCopy) },
          FAST,
          probeUrl,
        );
        yield* preferred
          .run({ port: 4000, provision })
          .pipe(
            Effect.andThen(
              until(async () => (await runsIn(appCopy)).length > 0),
            ),
            Effect.scoped,
          );
        expect(yield* Effect.promise(() => runsIn(path))).toEqual([]);
      }),
    ));

  it("comes up once the hostname routes, its token in the environment alone and its log in a file", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 502;
        const path = yield* Effect.promise(() => directory());
        const { provision } = provisionFor(true);
        const tunnel = yield* makeTunnel(
          { directory: path },
          { ...FAST, routeWithin: 5_000 },
          probeUrl,
        ).run({ port: 4000, provision });
        yield* until(async () => (await runsIn(path)).length === 1);
        expect(yield* stateOf(tunnel)).toBe("starting");
        const [first] = yield* Effect.promise(() => runsIn(path));
        expect(first?.args).toEqual([
          "tunnel",
          "--no-autoupdate",
          "--ha-connections",
          "1",
          "run",
        ]);
        expect(first?.token).toBe(TOKEN);
        expect(first?.logsToFile).toBe(true);
        expect(
          yield* Effect.promise(() =>
            readFile(join(path, "cloudflared.pid"), "utf8"),
          ),
        ).toBe(yield* pidRecord(first?.pid ?? 0));
        stub.routing.status = 426;
        expect(yield* status(tunnel, "up")).toEqual({
          status: "up",
          hostname: HOSTNAME,
        });
      }).pipe(Effect.scoped),
    ));

  it("tries the hostname as soon as cloudflared registers", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 426;
        const path = yield* Effect.promise(() => directory());
        yield* Effect.promise(() => writeFile(join(path, "registers"), ""));
        const { provision } = provisionFor(false);
        const tunnel = yield* makeTunnel(
          { directory: path },
          { ...FAST, existingTries: [60_000], routeWithin: 120_000 },
          probeUrl,
        ).run({ port: 4000, provision });
        yield* status(tunnel, "up");
      }).pipe(Effect.scoped),
    ));

  it("tries a fresh hostname it routed in an earlier run as soon as cloudflared registers", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 426;
        const path = yield* Effect.promise(() => directory());
        yield* Effect.promise(() => writeFile(join(path, "registers"), ""));
        const { provision } = provisionFor(true);
        yield* makeTunnel({ directory: path }, FAST, probeUrl)
          .run({ port: 4000, provision })
          .pipe(
            Effect.flatMap((tunnel) => status(tunnel, "up")),
            Effect.scoped,
          );
        const tunnel = yield* makeTunnel(
          { directory: path },
          {
            ...FAST,
            freshTries: [60_000],
            existingTries: [60_000],
            routeWithin: 120_000,
          },
          probeUrl,
        ).run({ port: 4000, provision });
        yield* status(tunnel, "up");
      }).pipe(Effect.scoped),
    ));

  it("restarts after cloudflared exits, with the provision that routed", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 404;
        const path = yield* Effect.promise(() => directory());
        const { counted, provision } = provisionFor(false);
        const tunnel = yield* makeTunnel(
          { directory: path },
          FAST,
          probeUrl,
        ).run({ port: 4000, provision });
        yield* status(tunnel, "up");
        yield* until(async () => (await runsIn(path)).length === 1);
        const [first] = yield* Effect.promise(() => runsIn(path));
        process.kill(first?.pid ?? 0, "SIGKILL");
        yield* status(tunnel, "error");
        yield* until(async () => (await runsIn(path)).length === 2);
        yield* status(tunnel, "up");
        expect(counted.provisions).toBe(1);
      }).pipe(Effect.scoped),
    ));

  it("errors when the hostname never routes, and provisions again", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 530;
        const path = yield* Effect.promise(() => directory());
        const { counted, provision } = provisionFor(true);
        const tunnel = yield* makeTunnel(
          { directory: path },
          FAST,
          probeUrl,
        ).run({ port: 4000, provision });
        yield* until(async () => (await runsIn(path)).length === 1);
        yield* status(tunnel, "error");
        const [first] = yield* Effect.promise(() => runsIn(path));
        yield* until(() => !alive(first?.pid ?? 0));
        yield* until(() => counted.provisions === 2);
      }).pipe(Effect.scoped),
    ));

  it("goes back to starting when a check finds it not routing", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 404;
        const path = yield* Effect.promise(() => directory());
        const { provision } = provisionFor(false);
        const tunnel = yield* makeTunnel(
          { directory: path },
          { ...FAST, routeWithin: 5_000 },
          probeUrl,
        ).run({ port: 4000, provision });
        yield* status(tunnel, "up");
        stub.routing.status = 502;
        yield* tunnel.check;
        yield* status(tunnel, "starting");
        stub.routing.status = 404;
        yield* status(tunnel, "up");
        yield* until(async () => (await runsIn(path)).length > 0);
        expect(yield* Effect.promise(() => runsIn(path))).toHaveLength(1);
      }).pipe(Effect.scoped),
    ));

  it("ends cloudflared when its scope closes", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        stub.routing.status = 404;
        const path = yield* Effect.promise(() => directory());
        const { provision } = provisionFor(false);
        const scope = yield* Scope.make();
        const tunnel = yield* makeTunnel({ directory: path }, FAST, probeUrl)
          .run({ port: 4000, provision })
          .pipe(Scope.provide(scope));
        yield* status(tunnel, "up");
        yield* until(async () => (await runsIn(path)).length === 1);
        const [first] = yield* Effect.promise(() => runsIn(path));
        yield* Scope.close(scope, Exit.void);
        expect(alive(first?.pid ?? 0)).toBe(false);
        expect(existsSync(join(path, "cloudflared.pid"))).toBe(false);
      }),
    ));

  it("ends the cloudflared a crashed run left, and nothing else under its pid", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* Effect.promise(() => directory());
        // Named as cloudflared is, which says nothing of whose it is.
        // Node, since some systems' sleep refuses to run under a name of
        // another's.
        yield* Effect.promise(() =>
          symlink(process.execPath, join(path, "other-cloudflared")),
        );
        const stale = spawn("/bin/sleep", ["30"], { stdio: "ignore" });
        const other = spawn(
          join(path, "other-cloudflared"),
          ["-e", "setTimeout(() => {}, 30_000)"],
          { stdio: "ignore" },
        );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            stale.kill("SIGKILL");
            other.kill("SIGKILL");
          }),
        );
        const leftover = (record: string) =>
          Effect.gen(function* () {
            yield* Effect.promise(() =>
              writeFile(join(path, "cloudflared.pid"), record),
            );
            const { provision } = provisionFor(false);
            yield* makeTunnel({ directory: path }, FAST, probeUrl)
              .run({ port: 4000, provision })
              .pipe(
                Effect.andThen(
                  until(async () => (await runsIn(path)).length > 0),
                ),
                Effect.scoped,
              );
            yield* Effect.promise(() => rm(join(path, "runs.log")));
          });

        // A pid given to another process since: it started at another
        // time than the one recorded.
        yield* leftover(`${other.pid}\nThu Jan  1 00:00:00 1970\n`);
        yield* leftover(`${other.pid}\n`);
        expect(alive(other.pid ?? 0)).toBe(true);
        yield* leftover(yield* pidRecord(stale.pid ?? 0));
        yield* until(() => !alive(stale.pid ?? 0));
      }).pipe(Effect.scoped),
    ));

  it("ends a leftover cloudflared without a run, for an app at startup", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* Effect.promise(() => directory());
        const stale = spawn("/bin/sleep", ["30"], { stdio: "ignore" });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => stale.kill("SIGKILL")),
        );
        const record = yield* pidRecord(stale.pid ?? 0);
        yield* Effect.promise(() =>
          writeFile(join(path, "cloudflared.pid"), record),
        );
        yield* endLeftoverCloudflared(path);
        yield* until(() => !alive(stale.pid ?? 0));
        expect(existsSync(join(path, "cloudflared.pid"))).toBe(false);
      }).pipe(Effect.scoped),
    ));

  it("asks a leftover cloudflared to end, and kills one that does not", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* Effect.promise(() => directory());
        const leftover = (name: string, onTerm: string) =>
          Effect.gen(function* () {
            const ready = join(path, `${name}-ready`);
            const stale = spawn(
              "/bin/sh",
              [
                "-c",
                `trap '${onTerm}' TERM; touch ${ready}; while :; do sleep 0.05; done`,
              ],
              { stdio: "ignore" },
            );
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => stale.kill("SIGKILL")),
            );
            yield* until(() => existsSync(ready));
            const record = yield* pidRecord(stale.pid ?? 0);
            yield* Effect.promise(() =>
              writeFile(join(path, "cloudflared.pid"), record),
            );
            return stale.pid ?? 0;
          });
        const asked = join(path, "asked");
        const polite = yield* leftover("polite", `touch ${asked}; exit 0`);
        yield* endLeftoverCloudflared(path, 5_000);
        expect(existsSync(asked)).toBe(true);
        yield* until(() => !alive(polite));
        const stubborn = yield* leftover("stubborn", "");
        yield* endLeftoverCloudflared(path, 200);
        yield* until(() => !alive(stubborn));
      }).pipe(Effect.scoped),
    ));
});

describe.skipIf(process.platform === "win32")(
  "a listening device's tunnel",
  () => {
    it(
      "offers its tunnel only while up, and checks it when a peer never arrives",
      { timeout: 30_000 },
      () =>
        run(
          Effect.gen(function* () {
            stub.routing.status = 502;
            const path = yield* Effect.promise(() => directory());
            const account = newAccountId();
            const desk = yield* device({
              listen: {
                admit: () => Effect.succeed(new Uint8Array()),
                onLink: () => Effect.void,
              },
            }).pipe(
              Effect.provideService(
                Colibrii.Tunnel,
                makeTunnel(
                  { directory: path },
                  { ...FAST, routeWithin: 20_000 },
                  probeUrl,
                ),
              ),
              // The service's own `POST /tunnel` needs Cloudflare. The
              // hub's address goes in front of a request after any
              // mapRequest, so the stub's goes in after that.
              Effect.updateService(HttpClient.HttpClient, (client) =>
                HttpClient.makeWith(
                  (request) =>
                    client.postprocess(
                      Effect.map(request, (final) =>
                        final.url.endsWith("/tunnel")
                          ? HttpClientRequest.setUrl(
                              final,
                              `${stub.origin}/tunnel`,
                            )
                          : final,
                      ),
                    ),
                  client.preprocess,
                ),
              ),
            );
            const tunnelDials: Array<string> = [];
            // A page's sockets: the tunnel alone, which never opens here.
            const phone = yield* device().pipe(
              Effect.updateService(Colibrii.WebSockets, (real) => ({
                dialableKinds: ["tunnel"] as const,
                open: (target: string) => {
                  if (!target.startsWith("wss://")) return real.open(target);
                  tunnelDials.push(target);
                  return Effect.fail(new Colibrii.WebSocketOpenError({}));
                },
              })),
            );
            expect(yield* waitFor(desk.tunnel, () => true)).toEqual({
              status: "off",
            });
            const deskId = deviceIdOf(yield* enroll(desk, account, "Desk"));
            yield* enroll(phone, account, "Phone");
            yield* waitFor(phone.roster, (peers) =>
              peers.some((peer) => peer.deviceId === deskId),
            );
            yield* waitFor(desk.tunnel, (state) => state.status === "starting");
            yield* until(() => stub.provisioned.length > 0);
            expect(stub.provisioned[0]?.port).toBeGreaterThan(0);
            expect(stub.provisioned[0]?.bearer).toMatch(/^Bearer colibrii_dc_/);

            // Starting: nothing to offer after the wait.
            expect(
              yield* Effect.flip(phone.dial(deskId, new Uint8Array())),
            ).toMatchObject({ reason: "unavailable" });
            expect(tunnelDials).toEqual([]);

            stub.routing.status = 404;
            yield* waitFor(desk.tunnel, (state) => state.status === "up");
            const dialTunnel = Effect.flip(
              phone.dial(deskId, new Uint8Array()),
            );
            expect(yield* dialTunnel).toMatchObject({ reason: "unreachable" });
            expect(tunnelDials).toEqual([`wss://${HOSTNAME}`]);

            // Two asks that never arrived: the third has the tunnel checked.
            yield* dialTunnel;
            stub.routing.status = 502;
            yield* dialTunnel;
            yield* waitFor(desk.tunnel, (state) => state.status === "starting");
          }),
        ),
    );
  },
);
