// The listener's connection tickets (spec/link.md, "Connection
// tickets"), in memory. A set is the tickets of one connectInfo answer,
// one per candidate, for one device and connection id. Times are the
// wall clock's, which a sleep moves too: a ticket is good for a minute
// of real time.
import {
  makeLinkTicket,
  MAX_TICKET_SETS_PER_DEVICE,
  MAX_TICKETS,
  RETIRED_TICKETS_KEPT,
  TICKET_TTL_MS,
} from "../wire/link.ts";
import type { CandidateKind } from "../wire/relay.ts";

interface Ticket {
  readonly deviceId: string;
  readonly connectionId: string;
  readonly kind: CandidateKind;
  readonly expiresAt: number;
  readonly set: string;
}

export type Check =
  | {
      readonly _tag: "Pending";
      readonly deviceId: string;
      readonly connectionId: string;
    }
  // Issued here, but it opens nothing now: spent, expired, retired, or
  // for the other kind of arrival.
  | { readonly _tag: "Stale" }
  // Never issued here: a guess.
  | { readonly _tag: "Unknown" };

const setKey = (deviceId: string, connectionId: string) =>
  `${deviceId} ${connectionId}`;

export class LinkTickets {
  private readonly pending = new Map<string, Ticket>();
  // Spent, and still in their sets, so one spent at a hello the listener
  // could not finish is good again while its set is.
  private readonly spent = new Map<string, Ticket>();
  // Each set's tickets, oldest set first.
  private readonly sets = new Map<
    string,
    { readonly deviceId: string; readonly tickets: ReadonlyArray<string> }
  >();
  // Oldest first.
  private readonly retired = new Set<string>();

  // One ticket per kind, in order, or null past the cap.
  mint(
    deviceId: string,
    connectionId: string,
    kinds: ReadonlyArray<CandidateKind>,
    now: number,
  ): Array<string> | null {
    const set = setKey(deviceId, connectionId);
    this.dropSet(set);
    const own = [...this.sets].filter(
      ([, entry]) => entry.deviceId === deviceId,
    );
    for (const [oldest] of own.slice(
      0,
      Math.max(0, own.length + 1 - MAX_TICKET_SETS_PER_DEVICE),
    )) {
      this.dropSet(oldest);
    }
    for (const [ticket, entry] of this.pending) {
      if (entry.expiresAt <= now) this.forget(ticket);
    }
    if (this.pending.size + kinds.length > MAX_TICKETS) return null;
    const tickets = kinds.map((kind) => {
      const ticket = makeLinkTicket();
      this.pending.set(ticket, {
        deviceId,
        connectionId,
        kind,
        expiresAt: now + TICKET_TTL_MS,
        set,
      });
      return ticket;
    });
    this.sets.set(set, { deviceId, tickets });
    return tickets;
  }

  check(ticket: string, kind: CandidateKind, now: number): Check {
    const entry = this.pending.get(ticket);
    if (entry === undefined) {
      return this.retired.has(ticket) ? { _tag: "Stale" } : { _tag: "Unknown" };
    }
    return entry.kind !== kind || entry.expiresAt <= now
      ? { _tag: "Stale" }
      : {
          _tag: "Pending",
          deviceId: entry.deviceId,
          connectionId: entry.connectionId,
        };
  }

  // Spends the ticket, whether or not it was still good.
  spend(ticket: string, kind: CandidateKind, now: number): boolean {
    const entry = this.pending.get(ticket);
    if (entry === undefined || entry.kind !== kind) return false;
    this.retire(ticket);
    this.spent.set(ticket, entry);
    return entry.expiresAt > now;
  }

  // A spent ticket good again, unless its set was retired since.
  unspend(ticket: string): void {
    const entry = this.spent.get(ticket);
    if (entry === undefined) return;
    this.spent.delete(ticket);
    this.retired.delete(ticket);
    this.pending.set(ticket, entry);
  }

  // Drops the tickets of every device not in `deviceIds`.
  keepDevices(deviceIds: ReadonlySet<string>): void {
    for (const [set, entry] of this.sets) {
      if (!deviceIds.has(entry.deviceId)) this.dropSet(set);
    }
  }

  private dropSet(set: string): void {
    for (const ticket of this.sets.get(set)?.tickets ?? []) this.retire(ticket);
    this.sets.delete(set);
  }

  private forget(ticket: string): void {
    const entry = this.pending.get(ticket);
    this.retire(ticket);
    if (entry === undefined) return;
    const set = this.sets.get(entry.set);
    if (set === undefined) return;
    const left = set.tickets.filter((other) => other !== ticket);
    if (left.length === 0) this.sets.delete(entry.set);
    else this.sets.set(entry.set, { ...set, tickets: left });
  }

  private retire(ticket: string): void {
    this.pending.delete(ticket);
    this.spent.delete(ticket);
    this.retired.add(ticket);
    for (const oldest of this.retired) {
      if (this.retired.size <= RETIRED_TICKETS_KEPT) break;
      this.retired.delete(oldest);
    }
  }
}
