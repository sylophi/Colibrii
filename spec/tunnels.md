# Tunnels

Every device that listens gets a tunnel, so it can be dialed from
outside its network: a hostname under the instance's tunnel domain that
routes, through Cloudflare, to the device's listener on its loopback
interface, or to the port in front of it that its LAN candidates name
([relay.md](relay.md)). The service sets the tunnel up, and the device runs it with
`cloudflared`. Tunnel traffic never passes through the service.

## The hostname

`<label>.example.link`, where `example.link` stands for the instance's
tunnel domain and the label is the first 32 hexadecimal characters of
the SHA-256 of the UTF-8 string `<app id>:<account id>:<device id>`. The
name is one label deep, so Cloudflare's standard certificate covers it.

## The tunnel domain

The tunnel domain is a Cloudflare zone of its own, holding nothing but
tunnels. The API token that writes its records can then touch nothing
else, and the pages served from it share no site with the hub or with
anything else the instance's owner runs.

The zone carries one standing record: a proxied wildcard `A` record for
`*` pointing at `192.0.2.1`, an address nothing answers on. A new
hostname then resolves to Cloudflare before its own record exists,
instead of being cached as nonexistent.

## What the service does

`POST /tunnel` ([service.md](service.md)), for the device's listener
port:

1. Finds the Cloudflare tunnel named `colibrii-<label>`, or creates it,
   with its configuration held by Cloudflare.
2. Points its ingress at the listener: the hostname goes to
   `http://127.0.0.1:<port>`, and anything else answers 404.
3. Makes sure the hostname has a proxied `CNAME` to the tunnel.
4. Answers with the hostname, the connector token `cloudflared` runs
   with, and whether the DNS record was made in the last minute
   (`fresh`), by this call or by one that failed after making it.

The same device always gets the same tunnel: a new port only changes the
ingress, and enrolling again leaves the tunnel alone. Removing the
device deletes the tunnel and its record, before the removal is
answered unless Cloudflare takes longer than 8 seconds. The deletion
then finishes after the answer, and a device that enrolls again under
the same id meanwhile can lose the tunnel it provisioned to it, until
its runner provisions again.

## What the device does

**States.** A device's tunnel is in one of:

| State | Meaning |
|---|---|
| `off` | The device does not listen, so it has no tunnel. |
| `no-binary` | No usable `cloudflared` was found. The device looks again when the machine wakes. |
| `starting` | Finding `cloudflared`, provisioning, or waiting until the hostname routes. |
| `up` | Routing. Only now does the device offer its `tunnel` candidate. |
| `error` | It failed, and will be retried. |

**Running it.** The device runs
`cloudflared tunnel --no-autoupdate --ha-connections 1 run`, with the
connector token in the `TUNNEL_TOKEN` environment variable. To stop it,
the device asks it to end and kills it 3 seconds later if it has not
(`SIGTERM` then `SIGKILL`, or on Windows ending the process outright).
An SDK ships a pinned `cloudflared` for each system it supports, lets
the app point at another, and falls back to one on the `PATH`. Each
candidate binary is checked with `cloudflared --version` first.
Its output goes to a file or nowhere, never to a pipe: `cloudflared`
ends at a write to a pipe no one reads, so one whose device's process
died would end without closing the links it carries, and their dialers
would hear of it only once the edge does, seconds later.

**Waiting until it routes.** `cloudflared` running does not mean the
hostname routes yet. The device fetches `https://<hostname>` and counts
any status below 500 as routing, since its listener answers a plain
request with a 4xx. Each try gets 5 seconds.

- For a fresh hostname, it tries 5 seconds after starting, then every 8
  seconds.
- For one that existed, it tries after 1 second, then after 2, 4, 8, 8
  seconds and so on.
- For one that existed, a device that reads `cloudflared`'s log also
  tries as soon as it says it registered a connection, and goes on from
  there on the same ladder. A fresh hostname is not tried before its
  first wait: a lookup before its record is out is a miss that
  resolvers keep for the zone's negative TTL, half an hour on
  Cloudflare.
- A hostname the device routed before counts as one that existed,
  even when the service answers it fresh, since its record is out. The
  device keeps the last fresh hostname it routed with its tunnel's
  files, so a run that follows a crash in that minute is not held to
  the fresh waits.
- Not routing 60 seconds after starting is an error.

**Restarting.** After an error or an exit, the device starts again
after 1, 2, 4, 8, 16, then 60 seconds, and starts the ladder again from
1 after a run that stayed healthy for 30 seconds. It provisions again
only if the last run never routed, or the port changed.

**Checking again.** When the machine wakes, or at a peer's third ask in
a row that was offered the tunnel without a socket from that peer ever
arriving, the device fetches the hostname once. If it does not route, the tunnel goes back to `starting`, and is
restarted if it still does not route within 60 seconds.

**Leftovers.** A device records the process id of its `cloudflared` and
when that process started, and at its next start ends one left running
by a run that crashed, as it stops its own: it asks it to end, and kills
it 3 seconds later if it has not. Asked to end, `cloudflared` leaves the
edge's routing at once and closes the links it carries as their listener
goes, which can be a moment after the crash. Killed outright, it leaves
both to the edge, which notices seconds later. The device checks first
that the process under that id started when it recorded, so an id since
given to another program is never touched.
