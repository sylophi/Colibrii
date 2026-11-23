// The device's tunnel in Node (spec/tunnels.md, "What the device
// does"): finds a cloudflared, provisions the tunnel through the
// service, runs it, waits until the hostname routes, restarts it on a
// ladder, and ends one a crashed run left behind. The connector token
// reaches cloudflared in its environment alone.
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { mkdir, open, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as SubscriptionRef from "effect/SubscriptionRef";
import {
  type RunningTunnel,
  Tunnel,
  type TunnelProvision,
  type TunnelState,
} from "../Platform.ts";
import { monotonicMs } from "../internal/liveness.ts";
import { ladder } from "../internal/wake.ts";
import { pinnedPath } from "./cloudflared.ts";

export interface TunnelOptions {
  // A directory of the device's own. The runner keeps the pid of its
  // cloudflared there, and finds the copy `fetchCloudflared` put there.
  readonly directory: string;
  // A cloudflared the app gives, tried first.
  readonly cloudflared?: string;
}

// The spec's timings, in milliseconds.
export interface Timings {
  // One fetch of the hostname.
  readonly tryWithin: number;
  // The waits before each try, staying on the last, for a hostname the
  // provision just created and for one that existed.
  readonly freshTries: ReadonlyArray<number>;
  readonly existingTries: ReadonlyArray<number>;
  // From the start, or from a check that found it not routing.
  readonly routeWithin: number;
  readonly restarts: ReadonlyArray<number>;
  // A run up this long starts the restarts over.
  readonly healthyFor: number;
  // From asking cloudflared to end to killing it.
  readonly stopGrace: number;
}

const TIMINGS: Timings = {
  tryWithin: 5_000,
  freshTries: [5_000, 8_000],
  existingTries: [1_000, 2_000, 4_000, 8_000],
  routeWithin: 60_000,
  restarts: [1_000, 2_000, 4_000, 8_000, 16_000, 60_000],
  healthyFor: 30_000,
  stopGrace: 3_000,
};

const ARGS = ["tunnel", "--no-autoupdate", "--ha-connections", "1", "run"];
const PID_FILE = "cloudflared.pid";
const LOG_FILE = "cloudflared.log";
// The hostname a fresh provision last routed with. Its record is out,
// however new, so the next run tries it as cloudflared registers.
const REACHED_FILE = "cloudflared.reached";
// How often the log is read until it says cloudflared registered.
const LOG_READ_EVERY_MS = 100;
// How often a leftover asked to end is checked for until it has.
const LEFTOVER_CHECK_EVERY_MS = 50;
const VERSION_WITHIN_MS = 10_000;
const IDENTIFY_WITHIN_MS = 5_000;

const run = (
  file: string,
  args: ReadonlyArray<string>,
  timeout: number,
  env?: NodeJS.ProcessEnv,
) =>
  Effect.callback<string | null>((resume) => {
    execFile(
      file,
      args,
      { timeout, windowsHide: true, ...(env === undefined ? {} : { env }) },
      (error, stdout) => resume(Effect.succeed(error === null ? stdout : null)),
    );
  });

// The first candidate that answers `--version`: the app's, the pinned
// copy, then one on the `PATH`.
const findBinary = (options: TunnelOptions) =>
  Effect.gen(function* () {
    const candidates = [
      ...(options.cloudflared === undefined ? [] : [options.cloudflared]),
      pinnedPath(options.directory),
      "cloudflared",
    ];
    for (const candidate of candidates) {
      if ((yield* run(candidate, ["--version"], VERSION_WITHIN_MS)) !== null) {
        return Option.some(candidate);
      }
    }
    return Option.none<string>();
  });

// When the process `pid` started, as the system's process table says,
// or "" when it can't tell. With the pid, it names one process: a pid
// given to another program since names another start.
const startOf = (pid: number) =>
  Effect.map(
    process.platform === "win32"
      ? run(
          "powershell.exe",
          [
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            `(Get-Process -Id ${pid}).StartTime.ToFileTimeUtc()`,
          ],
          IDENTIFY_WITHIN_MS,
        )
      : run("ps", ["-o", "lstart=", "-p", String(pid)], IDENTIFY_WITHIN_MS, {
          ...process.env,
          LC_ALL: "C",
          TZ: "UTC",
        }),
    (stdout) => (stdout ?? "").trim(),
  );

// The pid file: the pid, and when that process started.
export const pidRecord = (pid: number) =>
  Effect.map(startOf(pid), (started) => `${pid}\n${started}\n`);

const isAlive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

// Ends the cloudflared a run that crashed left running in a tunnel's
// directory, and only that process: its pid may have gone to another
// program since. Asked to end, it leaves the edge's routing at once and
// closes the links it carries as their listener goes, which may be a
// moment after the run that crashed. Killed outright, it would leave
// both to the edge, which notices seconds later.
export const endLeftoverCloudflared = (
  directory: string,
  grace: number = TIMINGS.stopGrace,
) =>
  Effect.gen(function* () {
    const pidFile = join(directory, PID_FILE);
    const text = yield* Effect.tryPromise(() => readFile(pidFile, "utf8")).pipe(
      Effect.option,
    );
    if (Option.isNone(text)) return;
    const [pidText = "", started = ""] = text.value.split("\n");
    const pid = Number(pidText);
    const still = Effect.map(startOf(pid), (now) => now === started);
    const signal = (name: NodeJS.Signals) =>
      Effect.try(() => process.kill(pid, name)).pipe(Effect.ignore);
    if (
      Number.isSafeInteger(pid) &&
      pid > 1 &&
      started !== "" &&
      (yield* still)
    ) {
      yield* signal("SIGTERM");
      yield* Effect.sync(() => isAlive(pid)).pipe(
        Effect.repeat({
          schedule: Schedule.spaced(LEFTOVER_CHECK_EVERY_MS),
          while: (alive) => alive,
        }),
        Effect.timeoutOrElse({
          duration: grace,
          // Its pid may have gone to another program while it ended.
          orElse: () =>
            Effect.flatMap(still, (same) =>
              same ? signal("SIGKILL") : Effect.void,
            ),
        }),
      );
    }
    yield* Effect.tryPromise(() => rm(pidFile, { force: true })).pipe(
      Effect.ignore,
    );
  });

const exited = (child: ChildProcess) =>
  Effect.callback<void>((resume) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      return resume(Effect.void);
    }
    child.once("exit", () => resume(Effect.void));
  });

// Asks it to end, and kills it if it has not within the grace. On
// Windows the first already ends it outright.
const stop = (child: ChildProcess, grace: number) =>
  Effect.gen(function* () {
    child.kill("SIGTERM");
    yield* exited(child).pipe(
      Effect.timeoutOrElse({
        duration: grace,
        orElse: () =>
          Effect.sync(() => child.kill("SIGKILL")).pipe(
            Effect.andThen(exited(child)),
          ),
      }),
    );
  });

// cloudflared, running until the scope closes, its pid and start in
// the directory meanwhile. Its log goes to a file there, never a pipe: it
// ends at a write to a pipe no one reads, and one the device's process
// left behind still closes the links it carries as their listener goes.
// `registered` is done once the log says it registered a connection at
// the edge.
const spawnCloudflared = (
  binary: string,
  connectorToken: string,
  directory: string,
  grace: number,
  registered: Deferred.Deferred<void>,
) =>
  Effect.gen(function* () {
    const pidFile = join(directory, PID_FILE);
    const logFile = join(directory, LOG_FILE);
    yield* Effect.tryPromise(() => mkdir(directory, { recursive: true }));
    const running = yield* Effect.acquireRelease(
      Effect.acquireUseRelease(
        Effect.tryPromise(() => open(logFile, "w")),
        (log) =>
          Effect.callback<ChildProcess, Error>((resume) => {
            const child = spawn(binary, ARGS, {
              env: { ...process.env, TUNNEL_TOKEN: connectorToken },
              stdio: ["ignore", "ignore", log.fd],
              windowsHide: true,
            });
            child.once("spawn", () => resume(Effect.succeed(child)));
            child.once("error", (error) => resume(Effect.fail(error)));
          }),
        (log) => Effect.promise(() => log.close()),
      ).pipe(
        Effect.tap((child) =>
          pidRecord(child.pid ?? 0).pipe(
            Effect.flatMap((record) =>
              Effect.tryPromise(() => writeFile(pidFile, record)),
            ),
            Effect.ignore,
          ),
        ),
      ),
      (child) =>
        stop(child, grace).pipe(
          Effect.andThen(
            Effect.tryPromise(() => rm(pidFile, { force: true })).pipe(
              Effect.ignore,
            ),
          ),
        ),
    );
    yield* Effect.tryPromise(() => readFile(logFile, "utf8")).pipe(
      Effect.map((log) => log.includes("Registered tunnel connection")),
      Effect.orElseSucceed(() => false),
      Effect.repeat({
        schedule: Schedule.spaced(LOG_READ_EVERY_MS),
        until: (said) => said,
      }),
      Effect.andThen(Deferred.succeed(registered, undefined)),
      Effect.forkScoped,
    );
    return running;
  });

const makeRun =
  (
    options: TunnelOptions,
    timings: Timings,
    probeUrl: (hostname: string) => string,
  ): Tunnel["Service"]["run"] =>
  ({ provision }) =>
    Effect.gen(function* () {
      const state = yield* SubscriptionRef.make<TunnelState>({
        status: "starting",
      });
      const checks = yield* Queue.sliding<void>(1);

      // One fetch of the hostname: anything below 500 came from the
      // listener.
      const routes = (hostname: string) =>
        Effect.tryPromise(async (signal) => {
          const response = await fetch(probeUrl(hostname), {
            signal,
            redirect: "manual",
          });
          await response.body?.cancel();
          return response.status < 500;
        }).pipe(
          Effect.timeoutOption(timings.tryWithin),
          Effect.map(Option.getOrElse(() => false)),
          Effect.orElseSucceed(() => false),
        );

      // Whether the hostname routes within the deadline, trying after
      // each wait in turn.
      const routesSoon = (hostname: string, waits: ReadonlyArray<number>) =>
        Effect.sleep(waits[0] ?? 0).pipe(
          Effect.andThen(
            routes(hostname).pipe(
              Effect.repeat({
                schedule: ladder(waits.slice(1)),
                until: (routed) => routed,
              }),
            ),
          ),
          Effect.timeoutOption(timings.routeWithin),
          Effect.map(Option.isSome),
        );

      // Up until a check finds it not routing and it does not route
      // again in time.
      const whileUp = (hostname: string) =>
        Effect.gen(function* () {
          while (true) {
            yield* Queue.take(checks);
            if (yield* routes(hostname)) continue;
            yield* SubscriptionRef.set(state, { status: "starting" });
            if (!(yield* routesSoon(hostname, timings.existingTries))) return;
            yield* SubscriptionRef.set(state, { status: "up", hostname });
          }
        });

      // The provision the last run routed with, which the next run
      // reuses.
      let routed: TunnelProvision | null = null;
      const reachedFile = join(options.directory, REACHED_FILE);
      const reachedBefore = (hostname: string) =>
        Effect.tryPromise(() => readFile(reachedFile, "utf8")).pipe(
          Effect.map((reached) => reached === hostname),
          Effect.orElseSucceed(() => false),
        );

      // One run from provisioning to failing, answering how long it was
      // up.
      const attempt = (binary: string) =>
        Effect.gen(function* () {
          yield* SubscriptionRef.set(state, { status: "starting" });
          const reused = routed;
          routed = null;
          const current =
            reused ??
            Option.getOrNull(
              yield* provision.pipe(
                Effect.tapError((cause) =>
                  Effect.logWarning("tunnel provisioning failed", cause),
                ),
                Effect.option,
              ),
            );
          let upAt: number | null = null;
          if (current !== null) {
            const { hostname } = current;
            const fresh =
              reused === null &&
              current.fresh &&
              !(yield* reachedBefore(hostname));
            const waits = fresh ? timings.freshTries : timings.existingTries;
            const registered = yield* Deferred.make<void>();
            const routing = Effect.gen(function* () {
              // For a hostname that existed, a try as cloudflared
              // registers, which the ladder would likely miss by seconds,
              // and the ladder for when it says nothing or the hostname
              // does not route yet. A fresh one waits out its first wait:
              // a lookup before its record is out is a miss resolvers
              // keep for the zone's negative TTL.
              const reached = yield* fresh
                ? routesSoon(hostname, waits)
                : Effect.raceFirst(
                    routesSoon(hostname, waits),
                    Deferred.await(registered).pipe(
                      Effect.andThen(
                        routesSoon(hostname, [0, ...waits.slice(1)]),
                      ),
                    ),
                  );
              if (!reached) return;
              routed = current;
              if (fresh) {
                yield* Effect.tryPromise(() =>
                  writeFile(reachedFile, hostname),
                ).pipe(Effect.ignore);
              }
              upAt = yield* monotonicMs;
              yield* Queue.clear(checks);
              yield* SubscriptionRef.set(state, { status: "up", hostname });
              yield* whileUp(hostname);
            });
            yield* Effect.raceFirst(
              spawnCloudflared(
                binary,
                current.connectorToken,
                options.directory,
                timings.stopGrace,
                registered,
              ).pipe(Effect.flatMap(exited)),
              routing,
            ).pipe(Effect.ignore);
          }
          // Before cloudflared is stopped, so it never reads as up
          // meanwhile.
          yield* SubscriptionRef.set(state, { status: "error" });
          return upAt === null ? 0 : (yield* monotonicMs) - upAt;
        }).pipe(Effect.scoped);

      yield* Effect.gen(function* () {
        const [, binary] = yield* Effect.all(
          [
            endLeftoverCloudflared(options.directory, timings.stopGrace),
            findBinary(options),
          ],
          { concurrency: 2 },
        );
        if (Option.isNone(binary)) {
          return yield* SubscriptionRef.set(state, { status: "no-binary" });
        }
        yield* attempt(binary.value).pipe(
          Effect.repeat(
            ladder(
              timings.restarts,
              (upMs: number) => upMs >= timings.healthyFor,
            ),
          ),
        );
      }).pipe(Effect.forkScoped);

      return {
        state: SubscriptionRef.changes(state),
        check: Effect.gen(function* () {
          if ((yield* SubscriptionRef.get(state)).status === "up") {
            yield* Queue.offer(checks, undefined);
          }
        }),
      } satisfies RunningTunnel;
    });

// The runner with other timings, and the hostname fetched at another
// address, for the SDK's tests.
export const makeTunnel = (
  options: TunnelOptions,
  timings: Timings = TIMINGS,
  probeUrl: (hostname: string) => string = (hostname) => `https://${hostname}`,
) => Tunnel.of({ run: makeRun(options, timings, probeUrl) });

export const layerTunnel = (options: TunnelOptions) =>
  Layer.succeed(Tunnel, makeTunnel(options));
