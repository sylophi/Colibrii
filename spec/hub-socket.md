# The hub socket

Every connected device holds one websocket to the service. Through it
the service tells the device which of its peers are online, with their
keys, and carries the small sealed messages devices relay to each other
to arrange a connection ([relay.md](relay.md)). App data never travels
here.

## Opening it

1. Mint a ticket: `POST /tickets` with the connection id
   ([service.md](service.md)).
2. Open `wss://hub.example.com/connect?ticket=<ticket>`.

A ticket opens one socket, once, within 60 seconds of being minted.
Every attempt, including every reconnect, mints a fresh one with the
same connection id.

The service first checks the request itself: one past the rate limit is
429, one without `Upgrade: websocket` is 426, and a ticket that is
malformed or wrongly signed is 403 `TicketMalformed`. Past those, the
upgrade succeeds. A socket the service then refuses is closed
at once with 4101, and an admitted one receives a `presence` message
straight away. That first `presence` is the sign the device is in: there
is no other greeting. A device with no `presence` 10 seconds after the
socket opened treats the attempt as failed.

A socket is refused when its ticket was already used or has expired,
when the device was removed, or when the device enrolled again after
the ticket was minted (the ticket is bound to the credential that
minted it).

## Connect tickets

`colibrii_ct_` followed by three parts joined by `.`:

1. base64url of the UTF-8 string `<app id>/<account id>`, which routes
   the socket to that app and account. The encoded part is at most 128
   characters.
2. base64url of 16 random bytes (22 characters).
3. base64url of an HMAC-SHA256 over the UTF-8 of `<part 1>.<part 2>`,
   keyed with the instance's ticket signing key (43 characters).

Only the service reads a ticket. A device treats it as opaque.

The service keeps at most 64 unused tickets for one app and account.
Past that, a new ticket takes the place of the one soonest to expire.

## Messages

Every message is a websocket text message holding one JSON object,
except the heartbeat. Binary messages are ignored. A message that does
not parse as one of these is dropped.

### From the device

**relay**: send a sealed frame to another device of the same app and
account.

```json
{"t":"relay","to":"<device id>","frame":"<string>"}
```

**ping**: the literal text `ping`, which the service answers with the
literal text `pong`. A device sends it every 15 seconds.

### From the service

**presence**: the whole roster, every time it changes.

```json
{"t":"presence","online":[
  {"deviceId":"<device id>","publicKey":"<base64url>","connections":"single"}
]}
```

Each online device appears once, the receiving device included, with
the public key its socket was admitted under, sorted by device id. A
device leaves an entry it cannot read out of its roster
([README.md](README.md), "Growing the wire"). A new
message replaces the last, and the same roster may arrive twice. The
roster is a device's only source of its peers' keys: a key not in the
latest roster is not trusted.

**relay**: a frame another device sent to this one, as it was sent.

```json
{"t":"relay","from":"<device id>","frame":"<string>"}
```

`from` is set by the service, so it names the device that really sent
it. A relay is delivered to every socket the target device holds.

**nack**: a relay could not be delivered because its target holds no
socket.

```json
{"t":"nack","to":"<device id>","reason":"offline"}
```

A nack with a reason the device does not know ends its asks all the
same.

## Limits

- A text message is at most 65,536 bytes of UTF-8. A longer one closes
  the socket with 1009.
- A message from the service is at most 262,144 bytes, and a device may
  close its socket on a longer one: a relay is a device's message with
  `from` in place of `to`, and a presence names at most 1,024 devices,
  the largest an app's device cap can be.
- A socket may send 300 messages in a burst, refilled at 30 a second,
  binary messages counted and `ping` not. Past that, it is closed with
  4105.
- A socket that sends nothing for 90 seconds, `ping` included, is closed
  with 4106. The service judges this whenever it runs, not on a timer,
  so a silent socket may be closed later than that.
- A device holds at most 32 sockets.

## One device, several sockets

What a new socket does to the device's others depends on how the device
enrolled ([service.md](service.md), the device record):

- `single`: the new socket closes every older socket of the device with
  4103.
- `multi`: the new socket closes older sockets with the same connection
  id with 4103. Past 32 sockets, the oldest close too.

## Close codes

All sent by the service.

| Code | Meaning | What a device does |
|---|---|---|
| 4101 | The ticket was refused. | Reconnect. |
| 4102 | The device was removed. | Stop, and read as signed out. |
| 4103 | A newer socket of this device replaced this one. | Stop until the person acts. For a `single` device this means a second copy of the app runs on the same device id. |
| 4104 | The device enrolled again. | Reconnect with the new credential. |
| 4105 | Too many messages. | Reconnect. |
| 4106 | Silent for too long. | Reconnect. |
| 1009 | A message was too big. | Reconnect. |

Any other close: reconnect.

## Reconnecting

What a device should do, so many devices behave alike:

- Wait 1, 2, 4, 8, then 16 seconds between attempts, staying at 16.
  Start again from 1 after a socket that stayed open for 30 seconds.
- A 429 from the ticket route or the upgrade waits at least its
  `retry-after`, or 60 seconds when the header is unreadable.
- Reconnect straight away when the network comes back or the machine
  wakes. A wait that a 429 set is kept all the same.
- Treat a close frame as the close, without waiting for the connection
  itself to end. A web page cannot: it hears of a close only once the
  connection ends. Nor can it read a 429 at the upgrade, so it follows
  the ladder there.
- A ticket mint answering `DeviceRevoked` reads as 4102. One answering
  `CredentialRejected` stops until the device enrolls again, and one
  answering `UpdateRequired` stops until the app is updated.
- Consider a socket dead when nothing has arrived for 40 seconds after
  a `ping`, and a second `ping` then gets nothing back within 5 seconds.
  Send that second `ping` at once when the machine wakes or the network
  goes or comes back.
