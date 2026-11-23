// Wakes as the device hears them (the platform's and the app's), and the
// waits they cut short.
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Schedule from "effect/Schedule";
import { REDIAL_DELAYS_MS, REDIAL_RESET_AFTER_MS } from "../wire/link.ts";

export type Wakes = PubSub.PubSub<void>;

// Sleeps for `duration`, or until the next wake.
const sleepUntilWake = (wakes: Wakes, duration: Duration.Input) =>
  Effect.scoped(
    Effect.gen(function* () {
      const woken = yield* PubSub.subscribe(wakes);
      yield* PubSub.take(woken).pipe(
        Effect.timeoutOrElse({ duration, orElse: () => Effect.void }),
      );
    }),
  );

// How one run of a connection went: how long it stayed up, and the
// least wait the far end asked for before the next.
export interface Run {
  readonly uptimeMs: number;
  readonly waitAtLeastMs: number;
}

// Waits each of `delays` in turn, staying on the last, and starts over
// after an input `reset` holds for.
export const ladder = <A>(
  delays: ReadonlyArray<number>,
  reset: (input: A) => boolean = () => false,
): Schedule.Schedule<number, A> =>
  Schedule.fromStep(
    Effect.sync(() => {
      let rung = 0;
      return (_now: number, input: A) => {
        if (reset(input)) rung = 0;
        const delay = delays[Math.min(rung, delays.length - 1)] ?? 0;
        rung += 1;
        return Effect.succeed([rung, Duration.millis(delay)] as [
          number,
          Duration.Duration,
        ]);
      };
    }),
  );

// The hub socket's reconnect ladder: 1, 2, 4, 8, then 16 seconds,
// staying at 16, and starting over after a run that stayed up 30
// seconds. A wake cuts a wait on the ladder short, never one the far end
// asked for.
export const reconnectLadder = (wakes: Wakes): Schedule.Schedule<number, Run> =>
  ladder<Run>(
    REDIAL_DELAYS_MS,
    (run) => run.uptimeMs >= REDIAL_RESET_AFTER_MS,
  ).pipe(
    Schedule.modifyDelay(({ input, duration }) =>
      Effect.as(
        input.waitAtLeastMs > 0
          ? Effect.sleep(
              Math.max(Duration.toMillis(duration), input.waitAtLeastMs),
            )
          : sleepUntilWake(wakes, duration),
        Duration.zero,
      ),
    ),
  );
