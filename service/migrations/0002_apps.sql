-- The apps the instance serves, which scripts/app.ts adds, replaces and
-- removes. `origins` is a JSON array of strings. `clerk_secret_name`
-- names the Worker secret that holds the app's Clerk secret key.
CREATE TABLE apps (
  id TEXT PRIMARY KEY,
  clerk_public_key TEXT NOT NULL,
  origins TEXT NOT NULL,
  oauth_client_id TEXT,
  clerk_secret_name TEXT,
  device_cap INTEGER NOT NULL
);
