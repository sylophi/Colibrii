// The device registry in D1 (migrations/0001_init.sql). The Worker and
// each account's Durable Object reach it, and every query lives here.
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  Connections,
  DeviceData,
  type Device,
} from "@colibrii/sdk/wire/device";
import { WorkerEnv } from "./env.ts";

export class RegistryError extends Schema.TaggedError<RegistryError>()(
  "RegistryError",
  { operation: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `The device registry failed to ${this.operation}.`;
  }
}

const DeviceRow = Schema.Struct({
  device_id: Schema.String,
  app_id: Schema.String,
  account_id: Schema.String,
  name: Schema.String,
  data: Schema.fromJsonString(DeviceData),
  connections: Connections,
  public_key: Schema.String,
  credential_hash: Schema.String,
  created_at: Schema.Int,
  last_seen_at: Schema.NullOr(Schema.Int),
});
type DeviceRow = typeof DeviceRow.Type;

export const toDevice = (row: DeviceRow, online: boolean): Device => ({
  deviceId: row.device_id,
  name: row.name,
  data: row.data,
  connections: row.connections,
  createdAt: row.created_at,
  lastSeenAt: row.last_seen_at,
  online,
});

export interface Account {
  readonly appId: string;
  readonly accountId: string;
}

export interface Enrollment extends Account {
  readonly deviceId: string;
  readonly name: string;
  readonly data: DeviceRow["data"];
  readonly connections: DeviceRow["connections"];
  readonly publicKey: string;
  readonly credentialHash: string;
  readonly createdAt: number;
}

// How long a removed device's credential reads as revoked.
const REVOKED_FOR_MS = 14 * 24 * 60 * 60 * 1000;

const COLUMNS =
  "device_id, app_id, account_id, name, data, connections, public_key, credential_hash, created_at, last_seen_at";

export class Registry extends Context.Service<
  Registry,
  {
    readonly byId: (
      deviceId: string,
    ) => Effect.Effect<DeviceRow | null, RegistryError>;
    readonly byCredentialHash: (
      credentialHash: string,
    ) => Effect.Effect<DeviceRow | null, RegistryError>;
    // Oldest first.
    readonly list: (
      account: Account,
    ) => Effect.Effect<ReadonlyArray<DeviceRow>, RegistryError>;
    // Seen longest ago first, a device never seen by when it enrolled.
    readonly stalestFirst: (
      account: Account,
    ) => Effect.Effect<ReadonlyArray<string>, RegistryError>;
    // Inserts the device, or under the same app and account replaces its
    // credential, key, name, data and connections. False when the device
    // id is another app's or account's. `evict`, a device of the account,
    // goes as `remove` would remove it, in the same write and only with
    // the new row.
    readonly enroll: (
      enrollment: Enrollment,
      evict: string | null,
    ) => Effect.Effect<boolean, RegistryError>;
    // Deletes the device and keeps its credential as revoked. False when
    // the account had no such device.
    readonly remove: (
      account: Account,
      deviceId: string,
    ) => Effect.Effect<boolean, RegistryError>;
    readonly isRevoked: (
      credentialHash: string,
    ) => Effect.Effect<boolean, RegistryError>;
    // False when the account has no such device.
    readonly update: (
      account: Account,
      deviceId: string,
      patch: { readonly name?: string; readonly data?: DeviceRow["data"] },
    ) => Effect.Effect<boolean, RegistryError>;
    readonly touchLastSeen: (
      deviceId: string,
    ) => Effect.Effect<void, RegistryError>;
  }
>()("colibrii/service/Registry") {}

const make = Effect.gen(function* () {
  const db = (yield* WorkerEnv).DB;

  const query = <A>(operation: string, run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) => new RegistryError({ operation, cause }),
    });

  const decodeRow = (operation: string, row: unknown) =>
    Schema.decodeUnknownEffect(DeviceRow)(row).pipe(
      Effect.mapError((cause) => new RegistryError({ operation, cause })),
    );

  const first = Effect.fnUntraced(function* (
    operation: string,
    statement: D1PreparedStatement,
  ) {
    const row = yield* query(operation, () => statement.first());
    return row === null ? null : yield* decodeRow(operation, row);
  });

  const byId = Effect.fn("Registry.byId")(function* (deviceId: string) {
    return yield* first(
      "read a device",
      db
        .prepare(`SELECT ${COLUMNS} FROM devices WHERE device_id = ?`)
        .bind(deviceId),
    );
  });

  const byCredentialHash = Effect.fn("Registry.byCredentialHash")(function* (
    credentialHash: string,
  ) {
    return yield* first(
      "read a credential",
      db
        .prepare(`SELECT ${COLUMNS} FROM devices WHERE credential_hash = ?`)
        .bind(credentialHash),
    );
  });

  const list = Effect.fn("Registry.list")(function* (account: Account) {
    const { results } = yield* query("list an account", () =>
      db
        .prepare(
          `SELECT ${COLUMNS} FROM devices WHERE app_id = ? AND account_id = ?
           ORDER BY created_at, device_id`,
        )
        .bind(account.appId, account.accountId)
        .all(),
    );
    return yield* Effect.forEach(results, (row) =>
      decodeRow("list an account", row),
    );
  });

  const stalestFirst = Effect.fn("Registry.stalestFirst")(function* (
    account: Account,
  ) {
    const { results } = yield* query("rank an account's devices", () =>
      db
        .prepare(
          `SELECT device_id FROM devices WHERE app_id = ? AND account_id = ?
           ORDER BY COALESCE(last_seen_at, created_at), device_id`,
        )
        .bind(account.appId, account.accountId)
        .all<{ device_id: string }>(),
    );
    return results.map((row) => row.device_id);
  });

  // Deletes the device and keeps its credential as revoked: with
  // `whileHeld`, only while a device holds that credential.
  const removal = (
    account: Account,
    deviceId: string,
    now: number,
    whileHeld?: string,
  ) => {
    const held =
      whileHeld === undefined
        ? { sql: "", values: [] }
        : {
            sql: " AND EXISTS (SELECT 1 FROM devices WHERE credential_hash = ?)",
            values: [whileHeld],
          };
    return [
      db
        .prepare(
          `INSERT OR REPLACE INTO revoked_credentials (credential_hash, revoked_at)
           SELECT credential_hash, ? FROM devices
           WHERE device_id = ? AND app_id = ? AND account_id = ?${held.sql}`,
        )
        .bind(now, deviceId, account.appId, account.accountId, ...held.values),
      db
        .prepare(
          `DELETE FROM devices
           WHERE device_id = ? AND app_id = ? AND account_id = ?${held.sql}`,
        )
        .bind(deviceId, account.appId, account.accountId, ...held.values),
    ];
  };

  const enroll = Effect.fn("Registry.enroll")(function* (
    enrollment: Enrollment,
    evict: string | null,
  ) {
    const now = yield* Clock.currentTimeMillis;
    const [result] = yield* query("enroll a device", () =>
      db.batch([
        db
          .prepare(
            `INSERT INTO devices (device_id, app_id, account_id, name, data, connections, public_key, credential_hash, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (device_id) DO UPDATE
             SET name = excluded.name,
                 data = excluded.data,
                 connections = excluded.connections,
                 public_key = excluded.public_key,
                 credential_hash = excluded.credential_hash
             WHERE app_id = excluded.app_id AND account_id = excluded.account_id`,
          )
          .bind(
            enrollment.deviceId,
            enrollment.appId,
            enrollment.accountId,
            enrollment.name,
            JSON.stringify(enrollment.data),
            enrollment.connections,
            enrollment.publicKey,
            enrollment.credentialHash,
            enrollment.createdAt,
          ),
        ...(evict === null
          ? []
          : removal(enrollment, evict, now, enrollment.credentialHash)),
      ]),
    );
    return (result?.meta.changes ?? 0) > 0;
  });

  const remove = Effect.fn("Registry.remove")(function* (
    account: Account,
    deviceId: string,
  ) {
    const now = yield* Clock.currentTimeMillis;
    const [, deleted] = yield* query("remove a device", () =>
      db.batch([
        ...removal(account, deviceId, now),
        db
          .prepare("DELETE FROM revoked_credentials WHERE revoked_at < ?")
          .bind(now - REVOKED_FOR_MS),
      ]),
    );
    return (deleted?.meta.changes ?? 0) > 0;
  });

  const isRevoked = Effect.fn("Registry.isRevoked")(function* (
    credentialHash: string,
  ) {
    const now = yield* Clock.currentTimeMillis;
    const row = yield* query("read a revoked credential", () =>
      db
        .prepare(
          "SELECT 1 FROM revoked_credentials WHERE credential_hash = ? AND revoked_at >= ?",
        )
        .bind(credentialHash, now - REVOKED_FOR_MS)
        .first(),
    );
    return row !== null;
  });

  const update = Effect.fn("Registry.update")(function* (
    account: Account,
    deviceId: string,
    patch: { readonly name?: string; readonly data?: DeviceRow["data"] },
  ) {
    const result = yield* query("change a device", () =>
      db
        .prepare(
          `UPDATE devices SET name = COALESCE(?, name), data = COALESCE(?, data)
           WHERE device_id = ? AND app_id = ? AND account_id = ?`,
        )
        .bind(
          patch.name ?? null,
          patch.data === undefined ? null : JSON.stringify(patch.data),
          deviceId,
          account.appId,
          account.accountId,
        )
        .run(),
    );
    return result.meta.changes > 0;
  });

  const touchLastSeen = Effect.fn("Registry.touchLastSeen")(function* (
    deviceId: string,
  ) {
    const now = yield* Clock.currentTimeMillis;
    yield* query("note a device seen", () =>
      db
        .prepare("UPDATE devices SET last_seen_at = ? WHERE device_id = ?")
        .bind(now, deviceId)
        .run(),
    );
  });

  return Registry.of({
    byId,
    byCredentialHash,
    list,
    stalestFirst,
    enroll,
    remove,
    isRevoked,
    update,
    touchLastSeen,
  });
});

export const layer = Layer.effect(Registry, make);
