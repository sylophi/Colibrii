// A wake as Node can tell one: the monotonic clock stands still while
// the machine sleeps (on macOS and Linux) and the wall clock runs on, so
// a turn after which the wall clock moved further than the monotonic one
// came after a sleep. A process gets no word of the network going or
// coming back, which the app passes on through Device's `wake`.
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { monotonicMs } from "../internal/liveness.ts";
import { Wakes } from "../Platform.ts";

const CHECK_EVERY_MS = 2_000;
// More than a late timer, less than any sleep worth a reconnect.
const SLEPT_MS = 5_000;

const readClocks = Effect.all({
  wall: Clock.currentTimeMillis,
  mono: monotonicMs,
});

export const wakes: Stream.Stream<void> = Stream.unwrap(
  Effect.gen(function* () {
    let last = yield* readClocks;
    return Stream.fromEffectRepeat(
      Effect.gen(function* () {
        yield* Effect.sleep(CHECK_EVERY_MS);
        const now = yield* readClocks;
        const slept = now.wall - last.wall - (now.mono - last.mono);
        last = now;
        return slept > SLEPT_MS;
      }),
    ).pipe(
      Stream.filter((woke) => woke),
      Stream.as(undefined),
    );
  }),
);

export const layerWakes = Layer.succeed(Wakes, wakes);
