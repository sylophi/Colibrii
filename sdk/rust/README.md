# colibrii

The device side of Colibrii, in Rust. A device is one install of an
app. It enrolls under the person's account, keeps its hub socket up
while enrolled, follows which of the person's other devices are online,
and opens end-to-end encrypted links to them. A device that listens is
also dialed by them.

It is for desktop apps and programs on a server, on macOS, Linux and
Windows. On the wire it is the same device as one made with the
TypeScript SDK (`sdk/ts`): the two share a service, a roster and links.

A device runs on tokio, on a runtime of its own: a thread the device
starts when it is made and ends when it closes. What the app holds (the
`Device`, its account, roster and tunnel as `Watch`es, a `Link`'s
`send` and `recv`) only waits on that runtime, so it works from any
executor and from plain threads ([Without tokio](#without-tokio)). An
app that runs tokio already may hand the device its runtime instead.

The device keeps its credential and key in a `CredentialStore`.
`FileStore` keeps them in one file readable only by its owner, and an
app that keeps them elsewhere, such as in the system's keychain,
implements the trait's `read`, `write` and `remove` of one JSON
document. On Windows a file's mode does not limit who reads it, so an
app there keeps the document in the system's credential store.

`src/wire/` holds the wire itself (spec/ in the repository's root):
the service's schemas, Noise, the relay's sealing and the peer link's
messages, each checked against the vectors in `spec/vectors/`.

## Installing

The crate is on no registry. An app takes it from this repository as a
git dependency, by the tag of a release.
[The latest](https://github.com/sylophi/Colibrii/releases/latest) shows
the newest:

```toml
[dependencies]
colibrii = { git = "https://github.com/sylophi/Colibrii", tag = "v<version>" }
```

The tag is the only place a release's version is written, so cargo
lists the crate as 0.0.0 beside the tag.

It speaks TLS through rustls, with the system's root certificates, and
needs no OpenSSL.

## Making a device

```rust
use std::sync::Arc;

use colibrii::{Connections, Device, FileStore, Options};

let device = Device::new(Options::new(
    "https://hub.example.com",
    "inkwell",
    Connections::Single,
    Arc::new(FileStore::new("data/device.json")),
))
.await?;
```

A device whose store holds an enrollment connects at once. `close`
closes its hub socket, its listener, its tunnel and its links, and ends
its runtime. Dropping the device does the same without waiting.

## Enrolling

`enroll` takes a login token: a Clerk session token from the app's
sign-in, or the access token that signing in with a code ends with. It
makes a new key pair, enrolls, stores the credential and connects.
Enrolling again keeps the device id.

```rust
use colibrii::AccountState;

device.enroll(&login_token, "Laptop", None).await?;

let mut account = device.account();
while let Some(state) = account.changed().await {
    match state {
        AccountState::Enrolled { device_id, connected, .. } => {
            println!("enrolled as {device_id}, connected: {connected}")
        }
        AccountState::Stopped { reason, .. } => println!("stopped: {reason:?}"),
        AccountState::SignedOut { removed } => println!("signed out, removed: {removed}"),
    }
}
```

The account state is `SignedOut`, `Enrolled` or `Stopped`. While
enrolled, `connected` says whether the hub socket is up, and `settled`
whether the 10 seconds since it connected are over. After the service
restarts, every device reconnects at a slightly different moment, so
until then a peer missing from the roster may be one about to come
back: its links stay up, and an app holds back saying it went offline.

A device removed from the account, by another device or past the
account's cap, reads as `SignedOut { removed: true }` and forgets its
credential, until it enrolls again or the app signs it out, which is
how an app says it has acted on the removal. The store keeps that it
was removed, so a device made later on it reads the same, and an app
that quit before it acted on a removal meets it again on its next
start. A device stops when another copy of a `Single` device connects
(`Superseded`), when its credential was replaced (`CredentialRejected`)
or when the service wants a newer SDK (`UpdateRequired`). It stays
stopped until it enrolls again.

`sign_out` always works. It forgets the credential and reads as
`SignedOut` at once, then removes the device from the account. When the
hub cannot be told (it cannot be reached or gives no answer within 15
seconds, it rate limits, or it answers `Unavailable`), the store keeps
the device id and credential for that alone, and the device tries again
when one is next made, on a wake, and before it enrolls. It stops once
the service answers, a credential it no longer takes included.
`sign_out` answers whether the service has let the device go, so an
app about to wipe its local state knows whether a removal is still
owed. Called again while one is, it tries that removal again and
answers the same way. The device id stays enrolled under the old
account until then, so enrolling under another one delivers the
removal and tries again.

The device makes up its id when it first enrolls, and forgets it once
the service heard it signed out. An app whose device id names something
of its own, such as an install or a data folder, gives it to the
options as `device_id` (a UUID in lowercase canonical form) and keeps
it across sign-outs. A store holding another device's enrollment then
reads as signed out.

Signing in with a code is for a device with no browser. `CodeSignIn`
holds the app's Clerk publishable key and OAuth client: `start` gives
the code to show, `finish` waits for the person to approve it and gives
the access token, and `sign_in_token` trades that token for a Clerk
sign-in token.

```rust
use colibrii::CodeSignIn;

let sign_in = CodeSignIn::new(publishable_key, "inkwell-oauth");
let code = sign_in.start().await?;
println!("Approve {} at {}", code.user_code, code.verification_uri);
let login_token = sign_in.finish(&code).await?;
device.enroll(&login_token, "Server", None).await?;
```

## The device list

```rust
for entry in device.devices().list().await? {
    println!("{} {}", entry.name, if entry.online { "online" } else { "offline" });
}

device.devices().rename(&peer_id, "Desk").await?;
let mut data = serde_json::Map::new();
data.insert("icon".into(), "desktop".into());
device.devices().set_data(&peer_id, data).await?;
device.devices().remove(&peer_id).await?;
```

`roster` is the peers online now, with their keys, and every change.

## Dialing

`dial` asks the peer through the hub how to reach it, tries every
address it offers at once, and gives the first link its app welcomes.
The app hello is at most 16 KiB, for the peer's app to check.

```rust
let link = device.dial(&peer_id, b"hello").await?;

link.send(b"ping")?;
while let Some(message) = link.recv().await {
    println!("{} bytes", message.len());
}
let closed = link.closed().await;
```

A dial that gets no link fails with a `DialError` that says why
(`Offline`, `NoListener`, `Unavailable`, `TimedOut` and so on). One
`Refused` by the app carries the peer's `app_refusal` bytes. A link
carries app messages of up to 32 MiB each, whole and in order. It does
not push back: `send` hands a message to the socket at once, and the
other end holds what arrives until its app reads it, so an app that
sends in bulk paces itself, such as by waiting for its peer to
acknowledge. A link closes when either side closes it, when the peer
leaves the roster (once `settled`), when nothing is heard from it for
too long, or once the app drops its last clone of it, and `closed`
says which.

An app that keeps a link to a peer dials it again when it drops, on the
ladder in spec/link.md, and cuts a wait short on each of `wakes`, the
wakes the device hears. `wake` is how the app says the machine woke or
the network went or came back, so the hub socket, the links and the
tunnel check in at once. The device notices a sleep on its own, by its
clocks.

## Listening

A device that listens accepts links from the person's other devices on
every interface, on a port it keeps. Colibrii checks a dialer's ticket,
key and hello first. The app then welcomes or refuses each hello from
`incoming`, while the dialer waits (10 seconds at most). A welcome or a
refusal's bytes are at most 16 KiB, with a reason short enough to send
beside them (past either, Colibrii refuses the link itself), and a
hello the app drops is refused. A newer link from the same peer
replaces an older one.

```rust
use colibrii::Listen;

let device = Device::new(
    options.listen(
        // Browsers that may dial. A dialer that is not a browser sends no
        // origin, and is let in.
        Listen::new().origins(["https://inkwell.example"]),
    ),
)
.await?;

let incoming = device.incoming();
while let Some(hello) = incoming.next().await {
    if !knows(&hello.app_hello) {
        hello.refuse("unknown project", Some(b"open it here first".to_vec()));
        continue;
    }
    if let Some(link) = hello.welcome(b"welcome".to_vec()).await {
        while let Some(message) = link.recv().await {
            let _ = link.send(&message);
        }
    }
}
```

`welcome` gives no link when the dialer went away, or left the roster,
while the app decided.

A device whose peers reach it through something on its own machine
that passes their connections on to the listener, such as a local proxy
or a port forward, gives that port as `advertised_port`. Its LAN
candidates then name it, and its tunnel routes to it, while the
listener stays on the port it bound.

A device that does not listen tells a peer dialing it so, and the dial
fails with `NoListener`.

## Tunnels

A device that listens can also be reached from outside its network,
through a hostname under the instance's tunnel domain that Cloudflare
routes to its listener. It runs the tunnel with `cloudflared`, in a
directory of the device's own:

```rust
use colibrii::{TunnelOptions, TunnelState, fetch_cloudflared};

fetch_cloudflared("data/tunnel").await?;

let device = Device::new(options.listen(Listen::new()).tunnel(TunnelOptions::new("data/tunnel"))).await?;

let mut tunnel = device.tunnel();
while let Some(state) = tunnel.changed().await {
    match state {
        TunnelState::Up { hostname } => println!("reachable at {hostname}"),
        state => println!("{state:?}"),
    }
}
```

It uses the first `cloudflared` that answers `--version`: the one the
app names (`TunnelOptions::cloudflared`), then the copy
`fetch_cloudflared` put in the directory, then one on the `PATH`.
`fetch_cloudflared(directory)` downloads the release the SDK pins
(`CLOUDFLARED_VERSION`) for macOS on Apple silicon or Intel, Linux on
x64 or Windows on x64, checks its SHA-256 and unpacks it. It is for an
app's build step or first run, before the device is made.

The state is `Off` while the device is not enrolled or does not
listen, `NoBinary` when no `cloudflared` runs, `Starting` until the
hostname routes, `Up` once it does, and `Error` until the next start.
Only while it is `Up` does the device offer the tunnel to a peer. The
runner provisions the tunnel through the service, starts it again on a
ladder after it fails, checks it again when the machine wakes or a peer
never arrives through it, looks for a `cloudflared` again when the
machine wakes while it has none, and ends its `cloudflared` when the
device closes. It keeps the process id, when that process started, and
`cloudflared`'s log (`cloudflared.log`) in the directory, and ends a
`cloudflared` a crashed run left behind at its next start, only while
that id still names the process it started. A device that may not
start the tunnel soon, signed out or not listening, calls
`end_leftover_cloudflared(directory)` at startup for the same. The
connector token reaches `cloudflared` in its environment alone.

## Without tokio

Every call the app makes is a future that runs on the device's own
runtime and wakes the caller when it is done, so an app on another
executor, such as smol under GPUI, awaits it as it is:

```rust
smol::spawn(async move {
    let mut roster = device.roster();
    while let Some(peers) = roster.changed().await {
        println!("{} online", peers.len());
    }
})
.detach();
```

Enrolling, signing out and closing run to their end once begun, even if
the app drops the future, so the device and its store never part ways
halfway. Any other call stops when its future is dropped.

On a plain thread, `colibrii::block_on` runs one to its end:

```rust
let link = colibrii::block_on(device.dial(&peer_id, b"hello"))?;
```

An app that runs tokio already hands the device its runtime with
`Options::runtime(handle)`, and the device then starts no thread of its
own.
