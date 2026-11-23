# @colibrii/sdk

The device side of Colibrii, in TypeScript with Effect. A device is one
install of an app. It enrolls under the person's account, keeps its hub
socket up while enrolled, follows which of the person's other devices
are online, and opens end-to-end encrypted links to them. A device that
listens is also dialed by them.

`@colibrii/sdk` runs on any runtime. It reaches the runtime through a
few small services, which a binding provides:

- `WebSockets` opens sockets: to the hub, and to peers.
- `CredentialStore` keeps the device's credential and key. A store
  that several runs of the device hold at once, as a browser profile's
  tabs do, also says when another of them changed it, and lets one at
  a time enroll or sign out.
- `HttpClient` calls the service's HTTP API.
- `ListenerSockets` accepts sockets from peers, for a device that
  listens.
- `Wakes` says when the machine woke from sleep or the network went or
  came back, so the hub socket and links check in at once. Without it, nothing wakes them early.
- `Tunnel` runs the device's tunnel, for a peer outside the LAN.
  Without it, a device offers LAN addresses alone.

`@colibrii/sdk/node` is the binding for Node: `layer` holds everything
but the store and the tunnel. The store is a file (`layerFileStore`) or
a file encrypted with the app's own cipher (`layerEncryptedFileStore`),
and `layerTunnel` runs the tunnel with `cloudflared`. The file store
keeps its file readable only by its owner. On Windows a file's mode does
not limit who reads it, so an app there uses the encrypted store.

`@colibrii/sdk/browser` is the binding for a web page: `layer` holds
everything but the store, and `layerLocalStorageStore` keeps it in
localStorage ([In a browser](#in-a-browser)).

`src/wire/` holds the wire itself (spec/ in the repository's root): the
schemas the service shares, Noise, the relay's sealing and the peer
link's messages.

## Installing

The SDK is on no registry. Each release of this repository carries it
as a tarball, built to JavaScript with its declarations. An app installs
that tarball by its address, beside Effect, which the SDK takes as a
peer dependency so that the app's one copy is used. The SDK builds on
Effect's unstable HTTP modules, so it names the exact version it was
built with.

```sh
pnpm add https://github.com/sylophi/Colibrii/releases/download/v0.1.0/colibrii-sdk-0.1.0.tgz effect@4.1.4
```

In a checkout of this repository, `pnpm pack:sdk` builds the same
tarball into `out/`.

## Making a device

A device lives as long as the scope it is made in. Closing the scope
closes its hub socket, its listener and its links.

```ts
import * as Colibrii from "@colibrii/sdk";
import * as ColibriiNode from "@colibrii/sdk/node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

const runtime = Layer.merge(
  ColibriiNode.layer,
  ColibriiNode.layerFileStore("./data/device.json"),
);

const program = Effect.gen(function* () {
  const device = yield* Colibrii.make({
    hub: "https://hub.example.com",
    app: "inkwell",
    connections: "single",
  });
  // ...
});

Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(runtime)));
```

A device whose store holds an enrollment connects at once.

## Enrolling

`enroll` takes a login token: a Clerk session token from the app's
sign-in, or the access token that signing in with a code ends with. It
makes a new key pair, enrolls, stores the credential and connects.
Enrolling again keeps the device id.

```ts
yield* device.enroll({ loginToken, name: "Laptop" });

yield* device.account.pipe(
  Stream.runForEach((state) =>
    Effect.log(
      state._tag === "Enrolled"
        ? `enrolled as ${state.deviceId}, connected: ${state.connected}`
        : state._tag === "Stopped"
          ? `stopped: ${state.reason}`
          : `signed out, removed: ${state.removed}`,
    ),
  ),
  Effect.forkScoped,
);
```

The account state is `SignedOut`, `Enrolled` or `Stopped`. While
enrolled, `connected` says whether the hub socket is up, and `settled`
whether the 10 seconds since it connected are over. After the service
restarts, every device reconnects at a slightly different moment, so
until then a peer missing from the roster may be one about to come
back: its links stay up, and an app holds back saying it went offline.

A device removed from the account, by another device or past the
account's cap, reads as `SignedOut` with `removed: true` and forgets
its credential, until it enrolls again or the app signs it out, which
is how an app says it has acted on the removal. The store keeps that
it was removed, so a run made later on it reads the same, and an app
that quit before it acted on a removal meets it again on its next
start. A device stops when another copy of a `single` device connects
(`superseded`), when its credential was replaced
(`credential-rejected`) or when the service wants a newer SDK
(`update-required`). It stays stopped until it enrolls again.

`signOut` always works. It forgets the credential and reads as
`SignedOut` at once, then removes the device from the account. When the
hub cannot be told (it cannot be reached or gives no answer within 15
seconds, it rate limits, or it answers `Unavailable`), the store keeps the device id and credential for that
alone, and the device tries again when one is next made, on a wake, and
before it enrolls. It stops once the service answers, a credential it
no longer takes included. `signOut` answers whether the service has let
the device go, so an app about to wipe its local state knows whether a
removal is still owed. Called again while one is, it tries that removal
again and answers the same way. The device id stays enrolled under the
old account until then, so enrolling under another one delivers the
removal and tries again.

The device makes up its id when it first enrolls, and forgets it once
the service heard it signed out. An app whose device id names something of its own, such
as an install or a data folder, gives it to `make` as `deviceId` (a
UUID in lowercase canonical form) and keeps it across sign-outs. A
store holding another device's enrollment then reads as signed out.

Signing in with a code is for a device with no browser:
`startCodeSignIn` gives the code to show, `finishCodeSignIn` waits for
the person to approve it and gives the access token, and `signInToken`
trades that token for a Clerk sign-in token.

## The device list

```ts
const devices = yield* device.devices.list;
for (const entry of devices) {
  yield* Effect.log(`${entry.name} ${entry.online ? "online" : "offline"}`);
}

yield* device.devices.rename(peerId, "Desk");
yield* device.devices.setData(peerId, { icon: "desktop" });
yield* device.devices.remove(peerId);
```

`roster` is the peers online now, with their keys, and every change.

## Dialing

`dial` asks the peer through the hub how to reach it, tries every
address it offers at once, and gives the first link its app welcomes.
The app hello is at most 16 KiB, for the peer's app to check. The link
lives in the caller's scope.

```ts
const link = yield* device.dial(peerId, new TextEncoder().encode("hello"));

yield* link.messages.pipe(
  Stream.runForEach((message) => Effect.log(`${message.length} bytes`)),
  Effect.forkScoped,
);
yield* link.send(new TextEncoder().encode("ping"));
const { cause } = yield* link.closed;
```

A dial that gets no link fails with a `DialError` whose `reason` says
why (`offline`, `no-listener`, `unavailable`, `timed-out` and so on),
or with a `LinkRefusedError`. One refused `by: "app"` carries the
peer's `appRefusal` bytes. A link carries app messages of up to 32 MiB
each, whole and in order. It does not push back: `send` hands a message
to the socket at once, and the other end holds what arrives until its
app reads it, so an app that sends in bulk paces itself, such as by
waiting for its peer to acknowledge. A link closes when either side
closes it, when the peer leaves the roster (once `settled`), or when
nothing is heard from it for too long, and `closed` says which.

An app that keeps a link to a peer dials it again when it drops, on the
ladder in spec/link.md, and cuts a wait short on each of `wakes`, the
wakes the device hears.

## Listening

A device that listens accepts links from the person's other devices on
every interface, on a port it keeps. Colibrii checks a dialer's ticket,
key and hello first. The app's `admit` then gives the welcome or a
refusal, whose bytes are at most 16 KiB, with a reason short enough to
send beside them (past either, Colibrii refuses the link itself), and
`onLink` gets each link once welcomed. A newer
link from the same peer replaces an older one.

```ts
const device = yield* Colibrii.make({
  hub: "https://hub.example.com",
  app: "inkwell",
  connections: "single",
  listen: {
    // Browsers that may dial. A dialer that is not a browser sends no
    // origin, and is let in.
    origins: ["https://inkwell.example"],
    admit: (hello) =>
      knows(hello.appHello)
        ? Effect.succeed(new TextEncoder().encode("welcome"))
        : Effect.fail(
            new Colibrii.AppRefusal({
              reason: "unknown project",
              appRefusal: new TextEncoder().encode("open it here first"),
            }),
          ),
    onLink: (link) =>
      link.messages.pipe(Stream.runForEach((message) => link.send(message)), Effect.ignore),
  },
});
```

A device whose peers reach it through something on its own machine
that passes their connections on to the listener, such as a local proxy
or a port forward, gives that port as `advertisedPort`. Its LAN
candidates then name it, and its tunnel routes to it, while the
listener stays on the port it bound.

A device that does not listen tells a peer dialing it so, and the dial
fails with `no-listener`.

## Tunnels

A device that listens can also be reached from outside its network,
through a hostname under the instance's tunnel domain that Cloudflare
routes to its listener. In Node, `layerTunnel` runs it with
`cloudflared`, in a directory of the device's own:

```ts
const runtime = Layer.mergeAll(
  ColibriiNode.layer,
  ColibriiNode.layerFileStore("./data/device.json"),
  ColibriiNode.layerTunnel({ directory: "./data/tunnel" }),
);
```

It uses the first `cloudflared` that answers `--version`: the one the
app names (`cloudflared: "/path/to/cloudflared"`), then the copy
`fetchCloudflared` put in the directory, then one on the `PATH`.
`fetchCloudflared(directory)` downloads the release the SDK pins
(`CLOUDFLARED_VERSION`) for macOS on Apple silicon or Intel, Linux on
x64 or Windows on x64, checks its SHA-256 and unpacks it. It is for an
app's build step or first run, before the device is made.

```ts
yield* ColibriiNode.fetchCloudflared("./data/tunnel");

yield* device.tunnel.pipe(
  Stream.runForEach((state) =>
    Effect.log(
      state.status === "up" ? `reachable at ${state.hostname}` : state.status,
    ),
  ),
  Effect.forkScoped,
);
```

The state is `off` while the device is not enrolled or does not
listen, `no-binary` when no `cloudflared` runs, `starting` until the
hostname routes, `up` once it does, and `error` until the next start.
Only while it is `up` does the device offer the tunnel to a peer.
The runner provisions the tunnel through the service, starts it again
on a ladder after it fails, checks it again when the machine wakes or
a peer never arrives through it, looks for a `cloudflared` again when
the machine wakes while it has none, and ends its `cloudflared` when
the device's scope closes. It keeps the process id, when that process
started, and `cloudflared`'s log (`cloudflared.log`) in the directory,
and ends a `cloudflared` a crashed run left behind at its next start,
only while that id still names the process it started. A
device that may not start the tunnel soon, signed out or not
listening, calls `endLeftoverCloudflared(directory)` at startup for
the same. The connector token reaches `cloudflared` in its environment
alone.

## In a browser

A browser profile is one device, enrolled as `multi`: each tab is a
run of it, with a hub socket of its own under its own connection id.
The tabs share the store, one localStorage entry under the key the app
gives.

```ts
import * as Colibrii from "@colibrii/sdk";
import * as ColibriiBrowser from "@colibrii/sdk/browser";

const runtime = Layer.merge(
  ColibriiBrowser.layer,
  ColibriiBrowser.layerLocalStorageStore("inkwell.device"),
);

const program = Effect.gen(function* () {
  const device = yield* Colibrii.make({
    hub: "https://hub.example.com",
    app: "inkwell",
    connections: "multi",
  });
  // The session token from the app's Clerk sign-in.
  yield* device.enroll({ loginToken, name: "Browser" });
});
```

A tab follows the others through the `storage` event: one tab's
enrollment connects every open tab, and a sign-out or a removal signs
them all out. Each tab tries a removal still owed, one at a time, and
the first the service answers clears it for all. A removal reads as
`removed: true` in every tab, one opened after it included, until one
of them enrolls or signs out. Tabs enroll and sign out one at a time,
under a lock named after the key (`navigator.locks`), and a tab that
waited behind another's enrollment runs that one instead of enrolling
again. Any script on the page's origin can read the entry.

A page does not listen, so a peer dialing it fails with `no-listener`.
It dials only a peer's tunnel (`wss://`), since a page served over
HTTPS cannot open a `ws://` socket, and a dial to a peer with no tunnel
up fails with `unavailable`. The page coming back into view and the
network going or coming back count as wakes.

A browser hides some of what the Node binding sees. A socket that did
not open shows no reason, so a 429 at the upgrade waits on the usual
reconnect ladder and not on its `retry-after`. A close arrives only
once the browser has ended the connection, which against some servers
is a moment after the close frame. A page closes a socket only with
1000 or a code from 3000 to 4999, so a link it closes going away
(1001) or on a protocol error (1002, 1009) closes with 1000 and the
same reason.
