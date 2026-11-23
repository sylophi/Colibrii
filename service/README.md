# The Colibrii service

A Cloudflare Worker that implements the HTTP API and the hub socket of
[the Colibrii wire](../spec/README.md). Each instance is run by one
owner for their own apps, on their own Cloudflare account and domains.
These pages use `hub.example.com` for the hub address, `example.link`
for the tunnel domain, and `inkwell` for an app, as the spec does.

## What runs where

- `src/worker.ts` serves the HTTP API, declared once in the SDK
  (`sdk/ts/src/wire/api.ts`). It checks the version header, the
  bearer, and the per-IP rate limits, and answers with open CORS.
- `src/hub.ts` is a Durable Object per app and account, named
  `<app id>/<account id>`. It holds the hub sockets of the account's
  devices, announces presence, relays, keeps the account's connect
  tickets in its own SQLite storage (`src/Tickets.ts`), and runs the
  account's enrollments and removals one at a time.
- `src/Registry.ts` is the device registry and the instance's apps in
  D1 (`migrations/`).
- `src/apps.ts` is what an app is registered with and the rules it
  keeps, which `scripts/app.ts` checks before it writes one. The Worker
  reads an app on each request that names one.
- `src/clerk.ts` checks login tokens with the app's Clerk public key,
  without a network call, and mints sign-in tokens with the app's Clerk
  secret key.
- `src/tunnel.ts` provisions and removes each device's Cloudflare
  tunnel through the Cloudflare API.
- `src/config.ts` reads the rest of the instance's configuration as
  the Worker starts. A missing or malformed value stops the Worker
  there, and every request answers 500 until it is fixed, with the
  reason in the Worker's logs.

## Setting up an instance

You need a Cloudflare account, Node 24 and pnpm, and this repository
with `pnpm install` run at its root. Every command below runs from
`service/`, with Wrangler as `pnpm exec wrangler …`, signed in with
`pnpm exec wrangler login`.

### 1. Two zones

Both on the same Cloudflare account:

- **The hub's zone**, such as `example.com`, which serves the hub
  address `hub.example.com`. Deploying creates that hostname's record
  and certificate. The zone can hold anything else you run.
- **The tunnel domain**, such as `example.link`, a zone of its own that
  holds nothing but tunnels (`spec/tunnels.md`). Every device's tunnel
  gets a hostname one label deep under it, which Cloudflare's standard
  certificate covers. Give it one standing record: type `A`, name `*`,
  IPv4 address `192.0.2.1`, proxied. A new tunnel's hostname then
  resolves to Cloudflare before its own record exists, instead of being
  cached as nonexistent.

Note the tunnel zone's **Zone ID** and your **Account ID**, both shown
on the zone's overview page in the dashboard.

### 2. The API token

In the dashboard, under your profile's API Tokens, create a custom token
with:

- Account, **Cloudflare Tunnel**, Edit. The dashboard may list it as
  **Cloudflare One Connector: cloudflared** instead. Either works.
- Zone, **DNS**, Edit, for the tunnel zone only

The service uses it for nothing but tunnels and their DNS records.
Scoped to the tunnel zone, it cannot touch any other records you have.

### 3. The database

```sh
pnpm exec wrangler d1 create colibrii
```

It prints the database's id.

### 4. The configuration

```sh
cp instance.example.jsonc instance.jsonc
```

`instance.jsonc` is ignored by git. Fill in:

| Value | What it is |
|---|---|
| `name` | The Worker's name on your account. |
| `account_id`, `vars.CLOUDFLARE_ACCOUNT_ID` | Your Account ID. |
| `routes[0].pattern` | The hub address's hostname, such as `hub.example.com`. |
| `d1_databases[0].database_id` | The id step 3 printed. |
| `vars.TUNNEL_DOMAIN` | The tunnel domain, such as `example.link`. |
| `vars.TUNNEL_ZONE_ID` | The tunnel zone's Zone ID. |

### 5. Deploy

```sh
pnpm run deploy
```

It applies the database's migrations, then deploys the Worker with
`instance.jsonc`. Run it again after any change to the configuration or
the code. Wrangler asks before it applies a migration.

### 6. The apps

Each app is a row in the instance's database, which this command
writes through Wrangler with `instance.jsonc`:

```sh
pnpm run app add inkwell --key clerk.pem \
  --origin inkwell://app --origin https://app.inkwell.example \
  --oauth-client '<client id>' --secret-name INKWELL_CLERK_SECRET_KEY
```

| Option | What it is |
|---|---|
| `<id>` | The app's id, 1 to 32 lowercase letters, digits or `-`. Its devices name it on enrollment. |
| `--key` | The path of a PEM file holding the public key of the app's Clerk instance, which checks its login tokens. |
| `--origin` | One for each origin its Clerk session tokens come from, such as its desktop window's `inkwell://app` and its web client's `https://app.inkwell.example`. `http://localhost:*` stands for `http://localhost` on any port, for a development web client whose port differs per checkout. |
| `--oauth-client` | Optional. The client id of its Clerk OAuth application, for signing in through OAuth or with a code. Without it, no OAuth access token is accepted. |
| `--secret-name` | Optional. The name of the Worker secret that holds its Clerk secret key, such as `INKWELL_CLERK_SECRET_KEY`. Without it, `POST /sign-in-token` answers `SignInTokensOff` for the app. |
| `--device-cap` | Optional. How many devices an account of the app may have, 16 unless set, and at most 1,024. |

Adding an app under an id that is taken replaces that app, which is how
an app changes. `app list` shows the apps, and `app remove <id>` removes
one: it takes no new enrollments, and its devices keep their credentials
and go on working until they are removed.

An app takes effect from the next request that names it, without a
deploy, so no connected device notices. A development build that signs
in to its own Clerk instance registers as an app of its own, such as
`inkwell-dev`. Apps may share a Clerk instance, as long as they share no
origin and no OAuth client id, since a login token names its app by
nothing else. The command refuses an app that would, and checks the
rest of the rules above, before it writes.

### 7. The secrets

```sh
openssl rand -base64 32 | pnpm exec wrangler secret put TICKET_SIGNING_KEY -c instance.jsonc
pnpm exec wrangler secret put CLOUDFLARE_API_TOKEN -c instance.jsonc
pnpm exec wrangler secret put INKWELL_CLERK_SECRET_KEY -c instance.jsonc
```

- `TICKET_SIGNING_KEY` signs connect tickets. It never leaves the
  Worker, so it goes straight from `openssl` into Wrangler. Changing it
  only voids the tickets of the last minute.
- `CLOUDFLARE_API_TOKEN` is the token from step 2.
- One for each app added with `--secret-name`, under that name: the
  app's Clerk secret key (`sk_live_…`), from the Clerk dashboard's API
  keys page. The service uses it only to mint sign-in tokens.

A secret takes effect without a deploy. Until the first two are set,
the Worker answers 500. Until an app's is set, `POST /sign-in-token`
answers 500 for that app, with the secret's name in the Worker's logs.

### 8. Check it

```sh
curl -s https://hub.example.com/devices -H 'colibrii-version: 1'
```

answers `{"_tag":"CredentialRejected"}`.

## Registering an app with its Clerk instance

Each app brings its own Clerk instance, and the service never calls it
to check a sign-in.

**The public key.** In the Clerk dashboard, on the instance's API keys
page, the JWT public key is shown as PEM. Save it to a file, such as
`clerk.pem`, and give that file's path to `--key`.

**Session tokens.** An app that embeds Clerk's components sends the
Clerk session token as its login token. Its `azp` claim is the origin of
the window that minted it, and must be one of the app's origins
(`--origin`). A
desktop window on a custom scheme has an origin such as
`inkwell://app`, which Clerk's Frontend API also needs among the
instance's allowed origins before it serves that window.

**Signing in with a code.** A device with no browser signs in with
Clerk's device authorization grant (`spec/service.md`, "Signing in with
a code"). On the Clerk dashboard's OAuth applications page, create an
application for the app that:

- is public, with no client secret,
- has the device authorization grant on,
- issues its access tokens as JWTs.

Its client id goes in `--oauth-client`. The instance's Account Portal
must be on, since it hosts the page where the code is approved. The
access token the device ends with is its login token for enrollment.
An app that also wants a Clerk session on that device is added with
`--secret-name` and has that secret set, and trades the access token
for a sign-in token at `POST /sign-in-token`.

## Limits on what a stranger can cost

Requests are budgeted per client IP inside the Worker (the
`ratelimits` bindings), so a limited request is still a Worker
invocation you pay for. Only a rule in front of the Worker stops a flood
before that: in the dashboard, under the hub's zone, Security, WAF, a
rate limiting rule by IP well above the Worker's own budgets, such as
200 requests per 10 seconds, does it.

## Local development

```sh
pnpm --filter ./service run dev
```

serves the Worker at `http://localhost:8787` with a local database,
from `wrangler.jsonc` and one app, `inkwell`, that `scripts/dev.ts`
adds to that database. That script makes an RSA key pair in `.local/`
on its first run, which git ignores, and `inkwell` checks login tokens
with its public half. A login token signed with `.local/clerk.key.pem`
signs in. `pnpm --filter ./service run app --local …` adds, lists and
removes the local database's apps. Tunnel provisioning calls
Cloudflare, so it fails locally.

## Checks

From the repository root:

```sh
pnpm typecheck
pnpm lint
pnpm format:check
pnpm test
```

The tests (`test/`) drive the real Worker in workerd through Wrangler's
test harness (`harness/`), with login tokens signed by key pairs it
makes for each run. Tunnel provisioning and minting a sign-in token call
Cloudflare and Clerk, so they go untested. A test that removes a device
starts its tunnel's removal all the same, which Cloudflare refuses for
want of a real token, after the answer and without failing the test.
