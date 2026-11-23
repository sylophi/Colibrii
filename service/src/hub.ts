// One Durable Object per app and account, named `<app id>/<account id>`.
// It holds the hub sockets of the account's devices, announces who is
// online, relays between them, keeps the account's connect tickets, and
// runs its enrollments and removals one at a time. A socket only ever
// lives in its own account's object, so a relay cannot reach another app
// or account.
//
// The Worker calls its methods directly. Its fetch takes only the
// websocket upgrade of GET /connect, which a method call cannot carry.
//
// Sockets use the hibernation API. Each is tagged with its device id and
// its connection, and carries its key and connections in its attachment,
// all of which outlive the object's eviction from memory.
import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import { DurableObject } from "cloudflare:workers";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type { Connections, Device } from "@colibrii/sdk/wire/device";
import {
  CLOSE_DEVICE_REMOVED,
  CLOSE_ENROLLED_AGAIN,
  CLOSE_SILENT,
  CLOSE_SUPERSEDED,
  CLOSE_TICKET_REFUSED,
  CLOSE_TOO_BIG,
  CLOSE_TOO_MANY_MESSAGES,
  MAX_DEVICE_SOCKETS,
  MAX_MESSAGE_BYTES,
  MESSAGE_BURST,
  MESSAGES_PER_SECOND,
  type OnlineDevice,
  PING,
  PONG,
  RelayTo,
  type ServiceMessage,
  SILENT_AFTER_MS,
} from "@colibrii/sdk/wire/hubSocket";
import { type Env, WorkerEnv } from "./env.ts";
import * as Registry from "./Registry.ts";
import * as Tickets from "./Tickets.ts";

// The query parameter the Worker hands the ticket's random part in.
export const CONNECT_RANDOM_PARAM = "random";

export interface EnrollRequest extends Omit<Registry.Enrollment, "createdAt"> {
  readonly deviceCap: number;
}

export type EnrollOutcome =
  | {
      readonly status: "enrolled";
      readonly device: Device;
      // The device removed to make room, whose tunnel the Worker removes.
      readonly evicted: string | null;
    }
  | { readonly status: "enrolled-elsewhere" }
  | { readonly status: "account-full" };

interface SocketState {
  readonly publicKey: string;
  readonly connections: Connections;
  readonly connectedAt: number;
  // When it last sent a message. A ping is answered without waking the
  // object, and the runtime keeps its time apart.
  readonly heardAt: number;
}

const connectionTag = (deviceId: string, connectionId: string) =>
  `${deviceId}#${connectionId}`;

const stateOf = (ws: WebSocket) => ws.deserializeAttachment() as SocketState;

// getWebSockets lists a socket the service closed until its device
// answers the close, as CLOSING, in memory and after hibernation alike.
const isOpen = (ws: WebSocket) => ws.readyState === WebSocket.OPEN;

const encode = (message: ServiceMessage) => JSON.stringify(message);

const decodeRelay = Schema.decodeUnknownOption(Schema.fromJsonString(RelayTo));

const utf8 = new TextEncoder();

const withinSize = (text: string) =>
  text.length * 3 <= MAX_MESSAGE_BYTES ||
  utf8.encode(text).byteLength <= MAX_MESSAGE_BYTES;

export class Hub extends DurableObject<Env> {
  private readonly runtime: ManagedRuntime.ManagedRuntime<
    Registry.Registry | Tickets.Tickets,
    unknown
  >;
  // One enrollment, removal or socket admission at a time, so the cap is
  // read and the new row written with nothing between, and no socket is
  // admitted under a credential being replaced.
  private readonly admission = Semaphore.makeUnsafe(1);
  private readonly budgets = new WeakMap<
    WebSocket,
    { tokens: number; at: number }
  >();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair(PING, PONG));
    this.runtime = ManagedRuntime.make(
      Layer.mergeAll(
        Tickets.layer.pipe(
          Layer.provide(SqliteClient.layer({ storage: ctx.storage })),
        ),
        Registry.layer,
      ).pipe(Layer.provide(Layer.succeed(WorkerEnv, env))),
    );
    // The tickets table's migration runs before the object takes a call.
    void ctx.blockConcurrencyWhile(() => this.runtime.runPromise(Effect.void));
  }

  async mintTicket(holder: Tickets.TicketHolder): Promise<string> {
    return await this.runtime.runPromise(
      Effect.flatMap(Tickets.Tickets, (tickets) => tickets.mint(holder)),
    );
  }

  online(): string[] {
    return this.onlineDevices(this.liveSockets()).map(
      (device) => device.deviceId,
    );
  }

  // The row and its credential go first, so a failure leaves the device
  // as it was. Its sockets close even when no row went (a retry whose
  // answer was lost), since no device of the account holds a socket
  // without one. The caller's credential, which the Worker checked, is
  // checked again here, where an enrollment that replaced it since has
  // already run: "rejected" then.
  async remove(
    account: Registry.Account,
    deviceId: string,
    callerHash: string,
  ): Promise<"removed" | "unknown" | "rejected"> {
    return await this.runtime.runPromise(
      Effect.gen({ self: this }, function* () {
        const registry = yield* Registry.Registry;
        if ((yield* registry.byCredentialHash(callerHash)) === null) {
          return "rejected";
        }
        const removed = yield* registry.remove(account, deviceId);
        this.closeDevice(deviceId, CLOSE_DEVICE_REMOVED, "device removed");
        return removed ? "removed" : "unknown";
      }).pipe(this.admission.withPermits(1)),
    );
  }

  // A new device past the cap takes the place of the offline device seen
  // longest ago, which is removed as a removal would remove it, in the
  // same write as the new row. Enrolling again replaces the credential and key, and
  // closes the device's sockets so they connect again with the new
  // credential.
  async enroll(request: EnrollRequest): Promise<EnrollOutcome> {
    return await this.runtime.runPromise(
      Effect.gen({ self: this }, function* () {
        const registry = yield* Registry.Registry;
        const { deviceCap, ...enrollment } = request;
        const existing = yield* registry.byId(request.deviceId);
        if (
          existing !== null &&
          (existing.app_id !== request.appId ||
            existing.account_id !== request.accountId)
        ) {
          return { status: "enrolled-elsewhere" } as const;
        }
        let evicted: string | null = null;
        if (existing === null) {
          const stalestFirst = yield* registry.stalestFirst(request);
          if (stalestFirst.length >= deviceCap) {
            const online = new Set(this.online());
            evicted = stalestFirst.find((id) => !online.has(id)) ?? null;
            if (evicted === null) return { status: "account-full" } as const;
          }
        }
        const createdAt =
          existing?.created_at ?? (yield* Clock.currentTimeMillis);
        // The statement holds against an enrollment of the same device
        // under another account, whose object this permit does not cover.
        const wrote = yield* registry.enroll(
          { ...enrollment, createdAt },
          evicted,
        );
        if (!wrote) return { status: "enrolled-elsewhere" } as const;
        if (evicted !== null) {
          this.closeDevice(evicted, CLOSE_DEVICE_REMOVED, "device removed");
        }
        if (existing !== null) {
          this.closeDevice(
            request.deviceId,
            CLOSE_ENROLLED_AGAIN,
            "enrolled again",
          );
        }
        return {
          status: "enrolled",
          device: {
            deviceId: request.deviceId,
            name: request.name,
            data: request.data,
            connections: request.connections,
            createdAt,
            lastSeenAt: existing?.last_seen_at ?? null,
            online: false,
          },
          evicted,
        } as const;
      }).pipe(this.admission.withPermits(1)),
    );
  }

  // GET /connect, past the Worker's checks of the ticket's signature. A
  // socket refused here is accepted and closed at once with 4101, which a
  // browser shows where it would hide an HTTP status.
  override async fetch(request: Request): Promise<Response> {
    const random = new URL(request.url).searchParams.get(CONNECT_RANDOM_PARAM);
    const admitted =
      random === null
        ? null
        : await this.runtime.runPromise(
            Effect.gen(function* () {
              const holder = yield* (yield* Tickets.Tickets).take(random);
              if (holder === null) return null;
              const device = yield* (yield* Registry.Registry).byId(
                holder.deviceId,
              );
              return device?.credential_hash === holder.credentialHash
                ? { holder, device }
                : null;
            }).pipe(
              this.admission.withPermits(1),
              Effect.orElseSucceed(() => null),
            ),
          );
    if (admitted === null) {
      const pair = new WebSocketPair();
      pair[1].accept();
      pair[1].close(CLOSE_TICKET_REFUSED, "ticket refused");
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    const { holder, device } = admitted;
    const tag = connectionTag(holder.deviceId, holder.connectionId);
    const superseded =
      device.connections === "single"
        ? this.ctx.getWebSockets(holder.deviceId)
        : this.overCap(holder.deviceId, tag);
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1], [holder.deviceId, tag]);
    const now = Date.now();
    pair[1].serializeAttachment({
      publicKey: device.public_key,
      connections: device.connections,
      connectedAt: now,
      heardAt: now,
    } satisfies SocketState);
    // Everyone gets the roster, the new socket included, which is its
    // sign that it is in.
    this.closeAndAnnounce(superseded, CLOSE_SUPERSEDED, "superseded");
    this.touchLastSeen(holder.deviceId);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  // A multi device's sockets a new one replaces: the older ones with its
  // connection id, and its oldest others past MAX_DEVICE_SOCKETS.
  private overCap(deviceId: string, tag: string): WebSocket[] {
    const own = this.ctx.getWebSockets(tag).filter(isOpen);
    const others = this.ctx
      .getWebSockets(deviceId)
      .filter((ws) => isOpen(ws) && !own.includes(ws))
      .map((ws) => ({ ws, connectedAt: stateOf(ws).connectedAt }))
      .toSorted((a, b) => a.connectedAt - b.connectedAt)
      .map(({ ws }) => ws);
    const excess = others.length + 1 - MAX_DEVICE_SOCKETS;
    return excess > 0 ? [...own, ...others.slice(0, excess)] : own;
  }

  override webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    if (!isOpen(ws)) return;
    ws.serializeAttachment({
      ...stateOf(ws),
      heardAt: Date.now(),
    } satisfies SocketState);
    if (!this.withinBudget(ws)) {
      this.closeAndAnnounce([ws], CLOSE_TOO_MANY_MESSAGES, "too many messages");
      return;
    }
    if (typeof message !== "string") return;
    if (!withinSize(message)) {
      this.closeAndAnnounce([ws], CLOSE_TOO_BIG, "message too big");
      return;
    }
    const relay = decodeRelay(message);
    const [from] = this.ctx.getTags(ws);
    if (Option.isNone(relay) || from === undefined) return;
    const live = new Set(this.liveSockets());
    const targets = this.ctx
      .getWebSockets(relay.value.to)
      .filter((target) => live.has(target));
    if (targets.length === 0) {
      safeSend(
        ws,
        encode({ t: "nack", to: relay.value.to, reason: "offline" }),
      );
      return;
    }
    const delivered = encode({ t: "relay", from, frame: relay.value.frame });
    for (const target of targets) safeSend(target, delivered);
  }

  // Runs for a close the device makes. One the service makes runs
  // neither this nor webSocketError, and is handled where it is made.
  override webSocketClose(ws: WebSocket) {
    this.depart(ws);
  }

  // What the runtime calls instead on an abnormal end.
  override webSocketError(ws: WebSocket) {
    this.depart(ws);
  }

  private depart(ws: WebSocket): void {
    this.announce(new Set([ws]));
    this.seen([ws]);
  }

  private withinBudget(ws: WebSocket): boolean {
    const now = Date.now();
    const budget = this.budgets.get(ws) ?? { tokens: MESSAGE_BURST, at: now };
    budget.tokens = Math.min(
      MESSAGE_BURST,
      budget.tokens +
        (Math.max(0, now - budget.at) / 1000) * MESSAGES_PER_SECOND,
    );
    budget.at = now;
    this.budgets.set(ws, budget);
    if (budget.tokens < 1) return false;
    budget.tokens -= 1;
    return true;
  }

  private closeDevice(deviceId: string, code: number, reason: string): void {
    const sockets = this.ctx.getWebSockets(deviceId);
    if (sockets.length > 0) this.closeAndAnnounce(sockets, code, reason);
  }

  // A close the service makes does not run webSocketClose, so the roster
  // without these sockets goes out here, and their devices are seen.
  private closeAndAnnounce(
    sockets: WebSocket[],
    code: number,
    reason: string,
  ): void {
    for (const ws of sockets) closeQuietly(ws, code, reason);
    this.announce(new Set(sockets));
    this.seen(sockets);
  }

  // Everyone still standing gets the whole roster. getWebSockets can
  // still list sockets that are closing, so the departing ones are named.
  private announce(departing: ReadonlySet<WebSocket>): void {
    this.sendRoster(this.liveSockets().filter((ws) => !departing.has(ws)));
  }

  private sendRoster(sockets: WebSocket[]): void {
    const roster = encode({
      t: "presence",
      online: this.onlineDevices(sockets),
    });
    for (const ws of sockets) safeSend(ws, roster);
  }

  // Every socket but the silent, which are closed on the way. Silence is
  // judged whenever the object runs, not on a timer of its own, which
  // would wake a hibernating object.
  private liveSockets(): WebSocket[] {
    const now = Date.now();
    const live: WebSocket[] = [];
    const silent: WebSocket[] = [];
    for (const ws of this.ctx.getWebSockets().filter(isOpen)) {
      const pinged =
        this.ctx.getWebSocketAutoResponseTimestamp(ws)?.getTime() ?? 0;
      const heard =
        now - pinged <= SILENT_AFTER_MS ||
        now - stateOf(ws).heardAt <= SILENT_AFTER_MS;
      (heard ? live : silent).push(ws);
    }
    if (silent.length > 0) {
      for (const ws of silent) closeQuietly(ws, CLOSE_SILENT, "silent");
      this.sendRoster(live);
      this.seen(silent);
    }
    return live;
  }

  // Each device once, with the key its sockets were admitted under,
  // sorted by device id.
  private onlineDevices(sockets: WebSocket[]): OnlineDevice[] {
    const devices = new Map<string, OnlineDevice>();
    for (const ws of sockets) {
      const [deviceId] = this.ctx.getTags(ws);
      if (deviceId === undefined) continue;
      const { publicKey, connections } = stateOf(ws);
      devices.set(deviceId, { deviceId, publicKey, connections });
    }
    return [...devices.values()].toSorted((a, b) =>
      a.deviceId < b.deviceId ? -1 : a.deviceId > b.deviceId ? 1 : 0,
    );
  }

  // The devices of sockets that just closed.
  private seen(sockets: WebSocket[]): void {
    const devices = new Set(sockets.map((ws) => this.ctx.getTags(ws)[0]));
    for (const deviceId of devices) {
      if (deviceId !== undefined) this.touchLastSeen(deviceId);
    }
  }

  // Bookkeeping nothing waits on.
  private touchLastSeen(deviceId: string): void {
    this.runtime.runFork(
      Effect.ignore(
        Effect.flatMap(Registry.Registry, (registry) =>
          registry.touchLastSeen(deviceId),
        ),
      ),
    );
  }
}

// A socket the service closed stays listed until its device answers the
// close, and may be closed again meanwhile.
const closeQuietly = (ws: WebSocket, code: number, reason: string): void => {
  try {
    ws.close(code, reason);
  } catch {
    // Already closing.
  }
};

// A socket can close between listing and sending. Nothing sent here is
// lost for good: presence goes out again on every change, and a relayed
// frame is the app's to ask again.
const safeSend = (ws: WebSocket, text: string): void => {
  try {
    ws.send(text);
  } catch {
    // Dropped, see above.
  }
};
