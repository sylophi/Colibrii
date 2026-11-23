// The Worker's bindings, vars and secrets, and the services that hand
// them to the Effect side. What each holds is service/README.md's.
import * as Context from "effect/Context";
import type { Hub } from "./hub.ts";

export interface Env {
  readonly DB: D1Database;
  readonly HUB: DurableObjectNamespace<Hub>;
  // RATE_LIMIT_OPEN is the tighter budget, for the routes that work
  // before a device credential is checked.
  readonly RATE_LIMIT: RateLimit;
  readonly RATE_LIMIT_OPEN: RateLimit;
  // The rest is checked as the Worker starts (config.ts). An app's
  // Clerk secret key is a secret under the name its config gives.
  readonly [name: string]: unknown;
}

export class WorkerEnv extends Context.Service<WorkerEnv, Env>()(
  "colibrii/service/WorkerEnv",
) {}

// Work that may finish after the response.
export class WaitUntil extends Context.Service<
  WaitUntil,
  (promise: Promise<unknown>) => void
>()("colibrii/service/WaitUntil") {}
