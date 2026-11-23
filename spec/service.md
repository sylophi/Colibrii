# The service's HTTP API

The service answers at its instance's hub address, written here as
`https://hub.example.com`. Its routes enroll devices, keep each app's
device registry, mint hub socket tickets, provision tunnels, and mint
Clerk sign-in tokens for the apps that ask. The hub socket itself is in
[hub-socket.md](hub-socket.md).

## Conventions

The spec's shared conventions are in [README.md](README.md). On top of
them:

- Bodies are JSON in UTF-8. A request body is read as JSON whatever its
  `content-type`, and fields the route does not name are ignored. A
  device ignores fields it does not know in an answer, and reads an
  error it does not know as one it cannot read ([README.md](README.md),
  "Growing the wire").
- Every request carries `colibrii-version: 1`. A request without it, or
  with a version below the service's floor, is refused with
  `UpdateRequired`.
- A request is checked in this order, and the first check that fails
  answers: the rate limit, the version, the device credential on the
  routes that take one, the body, and then, on the routes that take a
  login token, the app and the token.

## Errors

An error is a JSON body whose `_tag` names it, with the error's fields
beside it:

```json
{"_tag":"UpdateRequired","floor":1}
```

Three answers carry no body: 400 when the request does not decode (a
bad path parameter, query, or body), 404 when no route matches, and 500
when the service fails in a way it did not plan for.

| `_tag` | Status | Fields | Meaning |
|---|---|---|---|
| `UpdateRequired` | 403 | `floor` | The request named no version, or one below `floor`. |
| `LoginRejected` | 401 | | The login token is missing, invalid, expired, or not issued for the app named. |
| `UnknownApp` | 404 | `app` | No app is registered under that id. |
| `CredentialRejected` | 401 | | The device credential is malformed or matches no device. |
| `DeviceRevoked` | 403 | | The credential belonged to a device that was removed in the last 14 days. |
| `DeviceEnrolledElsewhere` | 409 | | The device id is enrolled under another app or account. |
| `AccountFull` | 409 | `limit` | The app's device cap is reached and every device is online. |
| `UnknownDevice` | 404 | `deviceId` | No such device in the caller's app and account. A device of another account reads the same way. |
| `TicketMalformed` | 403 | | The connect ticket is missing, malformed, or wrongly signed. |
| `UpgradeRequired` | 426 | | `GET /connect` without `Upgrade: websocket`. |
| `SignInTokensOff` | 403 | | The app registered no Clerk secret key, so the service mints no sign-in tokens for it. |
| `Unavailable` | 502 | `operation` | Storage, Cloudflare, or Clerk failed. `operation` is one of `enroll`, `credential`, `list`, `revoke`, `update`, `ticket`, `tunnel`, `sign-in-token`. |

`DeviceRevoked` and `CredentialRejected` mean different things to a
device. A revoked device was removed on purpose (from another device, or
by eviction) and should read as signed out. A rejected credential is
usually one that a newer enrollment of the same device replaced.

## Authentication

Each route uses one of three kinds of bearer. The login token and the
device credential are sent as `Authorization: Bearer <value>`, and the
connect ticket in the query string. A missing or malformed bearer reads
as an empty value and is refused like a wrong one.

- **Login token**, for enrollment and sign-in tokens: proof the person
  signed in to the app, issued by the app's Clerk instance and checked
  with that instance's public key. It is either of:
  - a Clerk session token, from an app that embeds Clerk's components.
    Its `azp` (the origin that minted it) must be one of the origins
    registered for the app, compared exactly. The one registered origin
    that is not compared exactly is `http://localhost:*`, which stands
    for `http://localhost:<port>` on any port. A token without `azp` is
    refused.
  - a Clerk OAuth access token in JWT form, from an app that signs in
    through OAuth in a browser, or with a code
    ([below](#signing-in-with-a-code)). Its header's `typ` is `at+jwt`
    or `application/at+jwt` (a session token's is `JWT`), and its
    `client_id` must be the app's registered OAuth client.

  Either way, the token's `sub`, the person's Clerk user id, is the
  account id. A Clerk instance signs both kinds with its one key.
- **Device credential**, for every other route:
  `colibrii_dc_` followed by base64url of 32 random bytes. The service
  stores only its SHA-256, in lowercase hex, so it is shown once, in the
  enrollment answer. A device treats it as opaque.
- **Connect ticket**, only on `GET /connect`, in the query string (see
  [hub-socket.md](hub-socket.md)).

## Signing in with a code

A device with no browser to sign in through, such as a headless program
on a server, signs in with Clerk's device authorization grant
([RFC 8628](https://www.rfc-editor.org/rfc/rfc8628)), at the app's Clerk
instance and under the app's OAuth client:

1. It reads the instance's endpoints from
   `https://<host>/.well-known/openid-configuration`, where the host is
   the Clerk instance's Frontend API, which the app's publishable key
   encodes. A Clerk instance with the grant off names no
   `device_authorization_endpoint`.
2. It posts its `client_id` to the `device_authorization_endpoint`, and
   shows the person the `verification_uri_complete`, or the
   `verification_uri` and the `user_code`, to approve from any device
   signed in to the app.
3. It polls the `token_endpoint` with the `device_code` every `interval`
   seconds (5 when none is given) until it gets an `access_token`. On
   `authorization_pending` it asks again, and on `slow_down` it waits 5
   seconds longer from then on. `access_denied`, `expired_token`, or
   `expires_in` seconds passing end the sign-in.

The access token is the device's login token for enrollment. An app that
also wants a Clerk session on the device, for a window that uses Clerk's
components, trades it for a sign-in token
([`POST /sign-in-token`](#post-sign-in-token)) and redeems that with
Clerk.

For this, the app's OAuth client is public, has the device grant on, and
issues its access tokens as JWTs. The Clerk instance's Account Portal is
on, since it hosts the page where the code is approved.

None of this passes through the service: the device talks to Clerk.

## Rate limits

Requests are budgeted per client IP address. `POST /devices/enroll`,
`POST /sign-in-token` and `GET /connect`, the routes that do work before
a device credential is checked, share a budget of 60 a minute, and every
other route 300 a minute. Past either, the answer is 429 with no body
and a `retry-after` header in seconds. The budgets are approximate and
only bound what a stranger can cost the service. They are not a security
boundary.

## CORS

Every answer allows any origin (`access-control-allow-origin: *`) and
exposes `retry-after`. A preflight is answered 204, allowing `GET`,
`POST`, `PATCH`, `DELETE` and the headers `authorization`,
`content-type` and `colibrii-version`, cached for a day. No route reads
a cookie or any other ambient credential, so a page on another origin
can do nothing a plain request could not.

## The device record

What the service keeps on each device, and answers with:

```json
{
  "deviceId": "6f1c2a9e-0b7d-4d0e-9a51-3c2b8e4f7a10",
  "name": "Studio MacBook",
  "data": {"icon": "laptop"},
  "connections": "single",
  "createdAt": 1793100000000,
  "lastSeenAt": 1793180000000,
  "online": true
}
```

- `name`: 1 to 256 bytes of UTF-8, set at enrollment and by anyone in
  the account through `PATCH`.
- `data`: the app's device data, a JSON object of at most 1024 bytes of
  UTF-8, as the service measures it by encoding it again compactly. The
  service stores it as it is and never reads it. It is not secret.
- `connections`: `single` or `multi`, set at enrollment.
  - `single` is one running instance per device: a desktop app, a
    headless program. A new hub socket or peer link from the device
    replaces its older ones.
  - `multi` is several at once: the tabs of one browser profile. Each
    connection id stands on its own.
- `lastSeenAt`: when the device last connected or disconnected its hub
  socket, or `null` before the first time.
- `online`: whether it holds a hub socket now.

The service keeps nothing else about a device. What kind of machine it
is, which system it runs, and which version of the app, are the app's to
put in `data` if it wants them.

## Routes

### `POST /devices/enroll`

Login token. Enrolls a device, or enrolls it again.

```json
{
  "app": "inkwell",
  "deviceId": "6f1c2a9e-0b7d-4d0e-9a51-3c2b8e4f7a10",
  "name": "Studio MacBook",
  "publicKey": "<43 characters: base64url of the X25519 public key>",
  "connections": "single",
  "data": {"icon": "laptop"}
}
```

`data` is optional and defaults to `{}`. The answer is 200:

```json
{"credential": "colibrii_dc_…", "accountId": "user_…", "device": { … }}
```

Enrollments for one app and account run one at a time, so two arriving
together cannot both pass the cap.

1. If the device id is enrolled under another app or account, the answer
   is `DeviceEnrolledElsewhere`.
2. A new device that would exceed the app's device cap (16 unless the
   app says otherwise) takes the place of the device seen longest ago
   that is offline. That device is removed as `DELETE` would remove it.
   If every device is online, the answer is `AccountFull`.
3. The device gets a new credential and keeps the public key sent, both
   replacing any earlier ones. Its `name`, `data` and `connections` take
   the values sent. `createdAt` survives an enrollment again.
4. A device enrolling again has its hub sockets closed with 4104, so
   they connect again with the new credential. The answer's `device`
   therefore always reads `"online": false`.

A device makes a new key pair for every enrollment.

### `POST /sign-in-token`

Login token, which must be an OAuth access token. Mints a Clerk sign-in
token for the token's account, which the device redeems once, within 5
minutes, for a Clerk session (Clerk's `ticket` sign-in strategy).

```json
{"app": "inkwell"}
```

The answer is 200:

```json
{"token": "…"}
```

A session token is refused with `LoginRejected`, since whoever holds one
has a session already. Past that, an app that registered no Clerk secret
key gets `SignInTokensOff`.

### `GET /devices`

Device credential. Lists the devices of the caller's app and account,
oldest first.

```json
{"devices": [ { … }, { … } ]}
```

A device leaves out an entry it cannot read.

### `PATCH /devices/:deviceId`

Device credential. Changes a device of the caller's app and account,
which can be the caller itself. The body names `name`, `data`, or both.
`data` replaces the whole object. The answer is 204, or `UnknownDevice`.

### `DELETE /devices/:deviceId`

Device credential. Removes a device of the caller's app and account,
which can be the caller itself (signing out). The answer is 204, or
`UnknownDevice`.

- Its credential stops working at once, and reads as `DeviceRevoked`
  for 14 days.
- Its hub sockets close with 4102, and it leaves the roster.
- Its tunnel and the tunnel's DNS record are deleted.

### `POST /tickets`

Device credential. Mints a connect ticket for the device's next hub
socket.

```json
{"connectionId": "3f9a0c1e5b7d4f2a8c6e0b1d3f5a7c9e"}
```

The answer is `{"ticket": "colibrii_ct_…"}`. A device mints a fresh
ticket for every attempt to open its hub socket.

### `POST /tunnel`

Device credential. Provisions the device's tunnel, or the one it already
has, pointing at the port on the device's loopback interface where its
listener is reached: its own, or the one in front of it that its LAN
candidates name.

```json
{"port": 41734}
```

The answer is 200:

```json
{"hostname": "<label>.example.link", "connectorToken": "…", "fresh": true}
```

`connectorToken` is what the device's `cloudflared` runs the tunnel
with. `fresh` is true when the hostname's DNS record was made in the
last minute, by this call or by one that failed after making it, so the
name may take longer to route. How a device uses all three is in
[tunnels.md](tunnels.md).

### `GET /connect?ticket=<ticket>`

No bearer, and no `colibrii-version` check: the ticket was minted on a
route that checked both. Opens the hub socket (see
[hub-socket.md](hub-socket.md)).

## What a device keeps

Between runs, a device keeps its device id, its credential, and the
private half of its device key. The last two are secrets. A desktop app
or a headless program keeps them where only the person's own user
account can read them: the system's keychain or credential store where
there is one, and otherwise a file only its owner can read. A web page
has only its origin's storage, so whatever can run script on the page's
origin can read them.

A device that loses its credential or key enrolls again under the same
device id, which replaces both.

A device that signs out reads as signed out at once, whether or not the
service heard. When its `DELETE` could not be delivered (the service
was unreachable, answered 429, or answered `Unavailable`), it keeps its
device id and credential for that alone, and sends it again when it
next starts, when the machine wakes, and before it enrolls. It forgets
them once the service answers, `CredentialRejected` and `DeviceRevoked`
included. A device enrolling its old device id under another account
meets `DeviceEnrolledElsewhere` until then, and enrolls again once the
`DELETE` goes through.

A device that learns it was removed, from `DeviceRevoked` or a hub
socket closed with 4102, forgets its credential and key and keeps only
that it was removed. It reads as removed from then on, in every run
that shares what it keeps, until its person signs in again or the app
signs it out. An app may sign its person in by itself while it holds a
sign-in, so a removal it had not yet acted on when it last quit would
otherwise put the device back on the account.

The tabs of one browser profile are one `multi` device and share what it
keeps. One tab enrolls at a time, and the others take up the enrollment
it stored. A tab that finds the stored credential gone, or kept only to
sign out, reads as signed out, and as removed when the device was.

## What an instance is set up with

An instance belongs to whoever runs it, and serves the apps they
registered. Nothing a request can do creates an app. The instance's
owner sets:

- its hub address.
- its tunnel domain, and the Cloudflare account and API token that
  provision tunnels under it ([tunnels.md](tunnels.md)).
- the key that signs connect tickets.
- its apps. For each:
  - the app id.
  - the public key of the Clerk instance the app signs people in with,
    which checks its login tokens.
  - the origins its Clerk session tokens may come from: its desktop
    window's origin, such as `inkwell://app`, and its web client's
    origin. A development web client served on a port that differs per
    checkout is registered as `http://localhost:*`.
  - its Clerk OAuth client id, if it signs in through OAuth or with a
    code.
  - its Clerk instance's secret key, if it has the service mint sign-in
    tokens. The service keeps it as a secret and uses it for nothing
    else.
  - its device cap, if not 16. It is at most 1,024.

An app the owner removes takes no new enrollments. Its devices keep
their credentials and go on working until they are removed.

The Clerk instance is the app's own, and apps may share one. Two apps
that share one share no origin and no OAuth client, since a login token
names its app by nothing else. A
development build that signs in to its own Clerk instance registers as
an app of its own. The service calls Clerk only for an app that
registered its secret key, and only to mint a sign-in token.
