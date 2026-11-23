-- One row per enrolled device. The credential is kept only as its
-- SHA-256 in lowercase hex. `data` is the app's device data as compact
-- JSON. Times are milliseconds since the Unix epoch.
CREATE TABLE devices (
  device_id TEXT PRIMARY KEY,
  app_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  name TEXT NOT NULL,
  data TEXT NOT NULL,
  connections TEXT NOT NULL,
  public_key TEXT NOT NULL,
  credential_hash TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL,
  last_seen_at INTEGER
);

CREATE INDEX devices_account ON devices (app_id, account_id);

-- The credentials of removed devices, so a device that was offline when
-- it was removed is told so for 14 days. Pruned on each removal.
CREATE TABLE revoked_credentials (
  credential_hash TEXT PRIMARY KEY,
  revoked_at INTEGER NOT NULL
);

CREATE INDEX revoked_credentials_revoked_at ON revoked_credentials (revoked_at);
