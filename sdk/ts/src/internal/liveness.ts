// Whether the far end of a socket still answers (spec/hub-socket.md,
// "Reconnecting", and spec/link.md, "Liveness"). Elapsed time is the
// monotonic clock's, which stands still while the machine sleeps, so a
// sleep spends none of a window: a wake asks afresh instead.
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import {
  DEAD_AFTER_MS,
  LISTENER_SILENT_MS,
  PING_EVERY_MS,
  SECOND_PING_WITHIN_MS,
} from "../wire/link.ts";
import type { Wakes } from "./wake.ts";

export const monotonicMs = Effect.map(Clock.monotonicTimeNanos, (nanos) =>
  Number(nanos / 1_000_000n),
);

// What the far end has said: how many messages, and when the last came.
interface Heard {
  count: number;
  atMs: number;
}

export const makeHeard = Effect.map(monotonicMs, (atMs): Heard => ({
  count: 0,
  atMs,
}));

export const hear = (heard: Heard) =>
  Effect.map(monotonicMs, (atMs) => {
    heard.count += 1;
    heard.atMs = atMs;
  });

// Pings every 15 seconds, and returns once the far end is dead: nothing
// arrived for 40 seconds after a ping, and a second ping then got
// nothing back within 5. A wake sends that second ping at once.
export const watch = Effect.fnUntraced(function* (options: {
  readonly ping: Effect.Effect<void>;
  readonly heard: Heard;
  readonly wakes: Wakes;
}) {
  const woken = yield* PubSub.subscribe(options.wakes);
  // Whether a ping sent now gets any answer within the second window.
  const probe = Effect.gen(function* () {
    const before = options.heard.count;
    yield* options.ping;
    yield* Effect.sleep(SECOND_PING_WITHIN_MS);
    return options.heard.count > before;
  });
  let pingedAt = yield* monotonicMs;
  // The first ping since the far end was last heard, and what had been
  // heard by then.
  let unanswered: { readonly atMs: number; readonly count: number } | null =
    null;
  while (true) {
    const now = yield* monotonicMs;
    const nextPing = pingedAt + PING_EVERY_MS;
    const deadline =
      unanswered === null ? nextPing : unanswered.atMs + DEAD_AFTER_MS;
    const woke = yield* PubSub.take(woken).pipe(
      Effect.as(true),
      Effect.timeoutOrElse({
        duration: Math.max(0, Math.min(nextPing, deadline) - now),
        orElse: () => Effect.succeed(false),
      }),
    );
    if (unanswered !== null && options.heard.count > unanswered.count) {
      unanswered = null;
    }
    const at = yield* monotonicMs;
    if (woke || (unanswered !== null && at >= deadline)) {
      if (!(yield* probe)) return;
      unanswered = null;
      pingedAt = yield* monotonicMs;
    } else if (at >= nextPing) {
      yield* options.ping;
      pingedAt = at;
      unanswered ??= { atMs: at, count: options.heard.count };
    }
  }
});

// Returns once nothing was heard for 120 seconds, the listener's limit.
export const silence = (heard: Heard) =>
  Effect.gen(function* () {
    while (true) {
      const quietMs = (yield* monotonicMs) - heard.atMs;
      if (quietMs >= LISTENER_SILENT_MS) return;
      yield* Effect.sleep(LISTENER_SILENT_MS - quietMs);
    }
  });
