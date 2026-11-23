# The peer link

A peer link is an encrypted websocket from one device, the dialer, to
another, the listener. Both prove the keys the roster gives them, the
dialer presents a ticket the listener issued for exactly this dial, and
once the listener welcomes it, the two apps exchange messages over it.

Only a device that runs a listener can be dialed: a desktop app or a
headless program. A browser only dials.

## Connection tickets

The listener issues one ticket per candidate in each connectInfo answer
([relay.md](relay.md)):

- `colibrii_lt_` followed by base64url of 16 random bytes (22
  characters).
- Bound to the dialing device, the connection id it asked with, and the
  candidate's kind (`lan` or `tunnel`).
- Valid for 60 seconds, and spent by the first `hello` that passes
  Colibrii's checks ([admission](#admission)), whether or not the app
  then welcomes it.

A new answer for the same device and connection id retires the tickets
of the previous one. A listener keeps at most 32 sets of tickets per
device, dropping the oldest, and at most 256 tickets in all, answering
`{"available":false}` past that. It remembers the last 1,024 tickets it retired, so a
late one reads as stale and not as a guess.

Only the listener that issued a ticket reads it. A dialer treats it as
opaque.

## Opening a link

1. The dialer opens a websocket to the candidate's URL: binary messages,
   no subprotocol, no compression.
2. The dialer sends the first message (below), which names the ticket
   and starts the handshake.
3. The listener checks the ticket, reads the handshake, and answers with
   the second handshake message. From here both sides send transport
   messages.
4. The dialer sends `hello`. The listener admits it, asks the app about
   the app hello, and answers `welcome` or `refuse`.

### The handshake

`Noise_IK_25519_ChaChaPoly_SHA256`, with:

- the dialer as initiator, its device key as its static key, and the
  listener's roster key as the responder's static key.
- the prologue: the UTF-8 of `colibrii-link-v1:<ticket>`.
- empty payloads in both handshake messages.

The first message, from the dialer, is one binary websocket message:

| Bytes | What |
|---|---|
| 2 | The ticket's length, an unsigned 16-bit big-endian integer |
| that many | The ticket, in UTF-8, in the clear |
| the rest | The first Noise handshake message (96 bytes) |

The second message, from the listener, is the second Noise handshake
message alone (48 bytes).

Before reading the handshake, the listener checks the ticket. Once the
first Noise message opens, the dialer's static key must equal the
roster's key for the ticket's device. Either failing closes the socket
with 4004. A listener that cannot see the roster (its hub socket is
down) closes with 1013 before reading the handshake instead, keeps the
ticket, and counts no guess.

### Transport messages

After the handshake, each websocket message is one Noise transport
message: binary, with empty associated data and each direction's own
counter, and at most 65,535 bytes including its 16-byte tag. A message
that is not binary, or does not decrypt, closes the link.

Each plaintext starts with one byte naming its type:

| Type | Name | Sent by | Body |
|---|---|---|---|
| `0x00` | data | either | The last (or only) piece of an app message |
| `0x01` | data, more | either | A piece of an app message, with more to come |
| `0x10` | hello | dialer | JSON, below |
| `0x11` | welcome | listener | JSON, below |
| `0x12` | refuse | listener | JSON, below |
| `0x20` | ping | either | Empty |
| `0x21` | pong | either | Empty |

An unknown type closes the link with 1002. A reader ignores fields it
does not know in `hello`, `welcome` and `refuse`, so a later version
can add some, such as to say what it supports before either side sends
a new type.

**App messages.** An app message is split into pieces of at most 65,518
bytes: every piece but the last is `0x01`, and the last is `0x00`. The
receiver joins them and hands the app the whole message. An app message
is at most 32 MiB, and a longer one closes the link with 1009. A side
sends one app message at a time, every piece of it before any piece of
the next. `ping` and `pong` may come between pieces, and leave the
message being joined alone.

**hello**, the dialer's first transport message:

```json
{"deviceId":"<dialer's device id>","connectionId":"<32 hex>","appHello":"<base64url>"}
```

`appHello` is whatever the dialing app passes, possibly empty. It, the
`appWelcome` and the `appRefusal` are each at most 16,384 bytes before
encoding, so each of these messages fits one transport message.

**welcome**: the link is open.

```json
{"appWelcome":"<base64url>"}
```

**refuse**: the link is refused, and the listener closes it with 4002.

```json
{"by":"colibrii","reason":"<text>"}
{"by":"app","reason":"<text>","appRefusal":"<base64url>"}
```

`by` says whether Colibrii's checks or the listening app refused.
`reason` is text for logs, like a close reason. `appRefusal` is optional
and is the app's own: a version mismatch, say, with both versions. An
app's refusal whose `reason` is too long to fit one transport message
beside it is sent as a refusal by `colibrii` instead.

Until `welcome`, the dialer sends only `hello` and the listener only
`welcome` or `refuse`. Anything else closes the link with 1002.

## Admission

**At the upgrade**, before any message:

- **Origin.** A request with no `Origin` is admitted, which covers every
  dialer that is not a browser. A browser's `Origin` must be one the
  listening app allows: its own web client. Any other is refused at the
  upgrade, so no socket opens.
- **Capacity.** A listener holds at most 64 sockets, at most 16 of them
  not yet welcomed. Past either, the socket closes with 1013.
- **Lockout.** An address that made 5 guesses in a row (a ticket never
  issued, a key other than the roster's, a first message that did not
  open) is refused with 4003 until 30 seconds after its last guess. A
  welcomed link clears its address. A ticket that is retired, expired,
  or for the other kind of arrival is not a guess.
- **Arrival kind.** A socket from the loopback interface carrying a
  `CF-Connecting-IP` header arrived through the tunnel, and its address
  is that header's value. Any other socket arrived over the LAN. A
  ticket only opens a link that arrived the way its candidate said.

**At `hello`**, in order:

1. `deviceId` and `connectionId` must be the ones the ticket was issued
   for.
2. That device must still be in the roster, with the key the handshake
   proved. When the listener cannot see the roster (its hub socket is
   down), it closes with 1013 and keeps the ticket, so the dialer can
   try again.
3. The ticket is spent. An expired one is refused.
4. The listening app checks `appHello`, and answers with `appWelcome` or
   refuses.
5. That device must still be in the roster, settled or not, with the
   key the handshake proved, as the app may have taken a while. When the
   listener cannot see the roster, it closes with 1013 and the ticket is
   good again, as at 2.
6. The links this one replaces close with 1001: every other link from
   the device when its roster entry says `single`, or the one with the
   same connection id when it says `multi`. Then the listener sends
   `welcome`.

A refusal at 1 to 3 or at 5 is `refuse` by `colibrii`, and doesn't
count as a guess. From the upgrade to `welcome`, the dialer has 10 seconds in all,
or the listener closes with 4002.

## When a peer goes

Both ends of a link follow the roster, the dialer as well as the
listener:

- When a peer leaves a live roster, its links close with 1001, welcomed
  or not, and a listener drops its tickets.
- Except for 10 seconds after the device's hub socket is admitted (its
  first `presence`): a peer missing from the roster then is left alone.
  After the service restarts, every device reconnects at a slightly
  different moment, so the first rosters lack peers about to come back.
  When the 10 seconds end, the roster as it then stands applies.
- When a live roster gives a peer another key than the one a link
  proved, as after the peer enrolled again while this device's hub
  socket was down, that link closes with 1001, within those 10 seconds
  too.
- While a device's own hub socket is down, the roster says nothing, so
  its links stay up, and no new link opens at either end until it is
  back.
- A device that signs out, switches account, or reads as removed (4102,
  or `DeviceRevoked`) closes all its links, and a listener drops all its
  tickets.

## Liveness

Once a link is welcomed, either side may send `ping`, and the other
answers `pong` straight away. Any message received counts as a sign of
life.

- The dialer pings every 15 seconds. It considers the link dead when
  nothing arrives for 40 seconds after a ping, and a second ping then
  gets nothing within 5 seconds. After the machine sleeps, and when the
  network goes or comes back, it pings at once.
- The listener closes a link it heard nothing from for 120 seconds, with
  1001.

## Dialing

1. Mint a connection id, and ask the peer for `connectInfo`
   ([relay.md](relay.md)), waiting up to 10 seconds.
2. Open a socket to every candidate at once, in the order given, and run
   the handshakes side by side.
3. Send `hello` on one socket at a time, in the order their handshakes
   finished, releasing the next only when the one before it failed. A
   `single` device's links replace each other at `hello`, so two at once
   would end each other.
4. The first `welcome` wins. Close the others with 1000. A `welcome`
   wins nothing when this device's hub socket is down, when the roster
   gives the peer another key than its handshake proved, or when a
   settled roster no longer names the peer: close it with 1001, and the
   dial fails. A roster not yet settled that lacks the peer leaves it
   alone, as in [When a peer goes](#when-a-peer-goes), since this
   device chose the peer from the roster a moment ago. Its link closes
   if the settled roster still lacks it.
5. Give each candidate 10 seconds to open and be welcomed, and the race
   as a whole 10 seconds. Keep the first refusal, and report it only
   when no candidate wins.

A device that keeps links to its peers redials one that dropped after 1,
2, 4, 8, then 16 seconds, staying at 16, and starts again from 1 after a
link that stayed up for 30 seconds. It dials a peer as soon as it enters
the roster, cuts a wait short when the machine wakes or the network
comes back, and stops dialing a peer that refused with `no-listener`
until it leaves and comes back to the roster.

## Close codes

| Code | Sent by | Meaning |
|---|---|---|
| 4004 | listener | The ticket or the handshake was refused. Dialing this candidate again with the same ticket will not work. |
| 4002 | listener | `hello` was refused, or came too late. |
| 4003 | listener | The address is locked out for now. |
| 1013 | listener | Try again later: over capacity, or the roster is unavailable. |
| 1001 | either | Going away: replaced, the peer left the roster or signed out, silence, or the side stopping. |
| 1009 | either | An app message was too big. |
| 1002 | either | A protocol error: a message that was not binary, did not decrypt, or broke the order above. |
| 1000 | dialer | Closed on purpose: a losing candidate, or the app closing the link. |

A web page can only close with 1000, so it sends 1000 where this table
says 1001, 1002 or 1009.

Close reasons are text for logs, and nothing should depend on them.

## What the app gets

An open link is the app's: an ordered, message-based, encrypted
connection, with the peer's device id and connection id, how it arrived
(`lan` or `tunnel`), and the app hello or welcome the other side sent.
What the apps send over it, and which of the app's operations a peer may
run, is the app's own.
