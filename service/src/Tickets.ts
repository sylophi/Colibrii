// An account's unused connect tickets, in its Durable Object's own
// SQLite storage. Only a ticket's random part is kept: its route part is
// what brought the socket to this object.
import * as SqliteMigrator from "@effect/sql-sqlite-do/SqliteMigrator";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as Base64Url from "@colibrii/sdk/wire/base64url";
import { CONNECT_TICKET_TTL_MS } from "@colibrii/sdk/wire/ticket";

const MAX_UNUSED_TICKETS = 64;

// Who a ticket admits, and the credential that minted it, which must
// still be the device's when the ticket is spent.
export interface TicketHolder {
  readonly deviceId: string;
  readonly connectionId: string;
  readonly credentialHash: string;
}

export class Tickets extends Context.Service<
  Tickets,
  {
    // Answers the new ticket's random part.
    readonly mint: (holder: TicketHolder) => Effect.Effect<string, SqlError>;
    // Spends the ticket, answering its holder while it is unexpired.
    readonly take: (
      random: string,
    ) => Effect.Effect<TicketHolder | null, SqlError>;
  }
>()("colibrii/service/Tickets") {}

const migrations = SqliteMigrator.fromRecord({
  "0001_tickets": Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql`CREATE TABLE tickets (
      random TEXT PRIMARY KEY,
      device_id TEXT NOT NULL,
      connection_id TEXT NOT NULL,
      credential_hash TEXT NOT NULL,
      expires_at INTEGER NOT NULL
    )`,
  ),
});

const make = Effect.gen(function* () {
  yield* SqliteMigrator.run({ loader: migrations });
  const sql = yield* SqlClient.SqlClient;

  const mint = Effect.fn("Tickets.mint")(function* (holder: TicketHolder) {
    const now = yield* Clock.currentTimeMillis;
    yield* sql`DELETE FROM tickets WHERE expires_at <= ${now}`;
    // Past the cap, the new ticket takes the place of the one soonest to
    // expire.
    yield* sql`DELETE FROM tickets WHERE random IN (
      SELECT random FROM tickets ORDER BY expires_at
      LIMIT MAX(0, (SELECT COUNT(*) FROM tickets) - ${MAX_UNUSED_TICKETS - 1})
    )`;
    const random = Base64Url.random(16);
    yield* sql`INSERT INTO tickets ${sql.insert({
      random,
      device_id: holder.deviceId,
      connection_id: holder.connectionId,
      credential_hash: holder.credentialHash,
      expires_at: now + CONNECT_TICKET_TTL_MS,
    })}`;
    return random;
  });

  const take = Effect.fn("Tickets.take")(function* (random: string) {
    const now = yield* Clock.currentTimeMillis;
    const [row] = yield* sql<{
      device_id: string;
      connection_id: string;
      credential_hash: string;
      expires_at: number;
    }>`DELETE FROM tickets WHERE random = ${random}
       RETURNING device_id, connection_id, credential_hash, expires_at`;
    if (row === undefined || row.expires_at <= now) return null;
    return {
      deviceId: row.device_id,
      connectionId: row.connection_id,
      credentialHash: row.credential_hash,
    };
  });

  return Tickets.of({ mint, take });
});

export const layer = Layer.effect(Tickets, make);
