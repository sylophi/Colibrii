# The Colibrii wire

What crosses the wire between a Colibrii service and the devices of the
apps built on it, and between two devices. An SDK in any language is
built from these pages and checked against the test vectors.

## The parts

- **An instance** is one deployment of the service, run by one person
  or organization for their own apps. It answers HTTP at its **hub
  address**, such as `https://hub.example.com`, and gives tunnels names
  under its **tunnel domain**, such as `example.link`. These pages use
  those two names wherever an instance's own would stand. A device
  talks to one instance, the one its app was built for.
- **The service** holds one websocket per connected device, the **hub
  socket**. It knows the apps, the accounts, and each app's devices,
  tells every device which of its peers are online, relays small sealed
  messages between them, issues tickets, and provisions tunnels.
- **A device** is one install of one app: a desktop app on a machine, a
  headless program, or a browser profile. It enrolls under an app and an
  account, and from then on holds a device credential and a key pair.
- **A peer link** is an encrypted websocket from one device to another,
  over the local network or through the listening device's tunnel. App
  data travels only here.

## Terms

| Term | What it is |
|---|---|
| account | A person, as the app's Clerk instance knows them. Its id is their Clerk user id. |
| app | A program the instance's owner registered under an id such as `inkwell`, with the Clerk instance it signs people in with. Each app has its own devices, and no app sees another's. |
| app id | 1 to 32 characters, each a lowercase letter, a digit, or `-`. |
| device id | A UUID the device picks when it first enrolls, and keeps. |
| device credential | The bearer secret the service gives a device at enrollment. It authenticates every request the device makes. |
| device key | An X25519 key pair the device makes at each enrollment. The service keeps the public half. |
| roster | The online devices of one app and account, each with its public key, as the service last announced them. |
| device data | A small JSON object an app keeps on each of its devices, such as an icon. Its meaning is the app's. |
| candidate | An address a listening device can be dialed at: on its local network, or through its tunnel. |
| app hello | Bytes the dialing app sends as a peer link opens, and the listening app checks. Their meaning is the app's. |

## Trust

- **The app's Clerk instance says who a person is.** The service
  enrolls a device only for someone signed in there, checked with the
  instance's public key. It holds the instance's secret key only for an
  app that has it mint sign-in tokens.
- **The service is the trust root for which devices exist and for their
  keys.** A device trusts a peer's key only as the roster gives it.
- **The service never reads app data or the messages it relays.** Each
  relayed message is sealed to the recipient's key. The service sees who
  sends to whom, and when.
- **A peer link is authenticated both ways.** Each side proves its
  roster key in a Noise handshake, and the dialer presents a single-use
  ticket the listener issued for exactly this dial.
- **Colibrii decides who may connect, and the app decides what a
  connected peer may do.** Only enrolled devices of the same app and
  account can link. Which of the app's operations a linked peer may run
  is the app's own policy.

## What belongs to an app

Colibrii carries an app's meaning in three places, and reads none of
them:

1. **Device data**, a JSON object of at most 1 KiB on each device.
   Colibrii stores it in plain text, and every device of that app and
   account can read it, so it is never secret.
2. **The app hello**, opaque bytes in the first message of every peer
   link. An app checks its own protocol version there.
3. **Everything sent over a peer link** after it opens.

## Versions

The wire has one version of its own, currently 1. Every HTTP request
names it in the `colibrii-version` header, and the service refuses a
request below its floor, so an SDK too old to talk to it reads as
needing an update. The handshake prologues name their own version
(`colibrii-relay-v1`, `colibrii-link-v1`), so a change to a handshake is
a new prologue. An app's own version never reaches the service: two
devices of an app compare theirs in the app hello.

Whoever runs an instance deploys its service before shipping an app
whose SDK needs it. A device therefore meets a service at least as new
as its SDK, and peers whose SDKs may be newer or older than its own.

## Growing the wire

What an SDK does with something it does not know is fixed once it
ships, so every reader follows these rules from version 1 on. A later
version may add what they cover without a new wire version:

| What a later version adds | What a reader that does not know it does |
|---|---|
| A field in any JSON object on the wire: an HTTP body or error, a hub socket message, a relayed ask or answer, a link's `hello`, `welcome` or `refuse` | Ignores the field. |
| A hub socket message with another `t` | Drops the message. |
| A kind in an ask's `dialableKinds` | Answers for the kinds it knows. |
| A candidate of another kind in an answer | Skips it, as it does one of a kind it did not ask for, and dials the rest. A candidate of a kind it knows must still decode, or the answer is unreadable. |
| A value in a device entry, in `presence` or `GET /devices`, that the reader cannot decode, such as another `connections` | Leaves that device out and reads the rest, as if the device were not in the roster. |
| A `nack` reason | Ends the asks pending to that device, as for `offline`. |
| A refusal code in an answer | Reads it as a refusal. |
| An ask | Answers `unknown-ask`. |
| A device credential, connect ticket or link ticket of another form | Carries it as it was given. Only its maker reads it. |
| An error `_tag`, or another value in an error's field | Reads the answer as one it cannot read, as when the service cannot be reached. |

Anything else needs a new wire version, or a new prologue for a
handshake: a change to what an existing field, message or frame means
or to its form, and a field a reader must understand to read the rest.
A link frame of a new type also waits until both devices know it, as
an older peer closes the link on a type it does not know
([link.md](link.md)). A later version has the two say what they
support in fields of `hello` and `welcome` first.

## Conventions

- Binary values in text are base64url without padding (RFC 4648,
  section 5).
- Times are integer milliseconds since the Unix epoch.
- A device id is a UUID in lowercase canonical form. A connection id is
  16 random bytes as 32 lowercase hex characters. A device makes one for
  its hub socket (one per process, or per browser tab) and keeps it
  across reconnects, and a fresh one for each dial.
- Every Noise handshake is `Noise_IK_25519_ChaChaPoly_SHA256`, Noise
  revision 34, without extensions. Every Noise message, transport
  messages included, is at most 65,535 bytes.
- JSON inside a Noise message is UTF-8. The receiver parses it, so no
  canonical form is needed.

## Pages

- [service.md](service.md): the HTTP API, sign-in and enrollment, the
  device registry, and what an instance is set up with.
- [hub-socket.md](hub-socket.md): the hub socket, presence, and the
  relay.
- [relay.md](relay.md): sealing a relayed message, and the connectInfo
  ask and answer.
- [link.md](link.md): candidates, connection tickets, the peer link's
  handshake and frames, admission, and liveness.
- [tunnels.md](tunnels.md): how a listening device gets and runs its
  tunnel.
- [vectors.md](vectors.md): the test vectors every SDK must pass.
