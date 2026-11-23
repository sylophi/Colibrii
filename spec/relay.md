# The relay: asking a peer how to reach it

Before dialing, a device asks the peer, through the hub socket, how it
can be reached. The ask and its answer are sealed to each device's key,
so the service carries them without reading them.

## Frames

A relayed frame is one of two strings:

```
ask:<id>:<sealed>
answer:<id>:<sealed>
```

- `id` is 1 to 16 decimal digits, which the answer repeats exactly. It
  is compared as a string, never as a number. The asker picks a random
  starting id for each hub socket connection and counts up from it.
- `sealed` is a Noise handshake message in base64url.

The asker sends `ask` frames, and the asked device answers with
`answer` frames. An answer is matched to its ask by id, and must come
from the device that was asked: any other answer is ignored.

## Sealing

Each ask is one Noise handshake, `Noise_IK_25519_ChaChaPoly_SHA256`
(Noise revision 34, without extensions):

- The asker is the initiator. Its static key is its device key, and it
  takes the asked device's static key from the roster.
- The asked device is the responder.
- The prologue is the UTF-8 of `colibrii-relay-v1:<asker>><asked>`, the
  two device ids joined by `>`. The asked device builds it from the
  relay's `from`, which the service set.
- The first handshake message carries the ask, and the second the
  answer, each as a JSON payload.

The asked device:

1. drops an ask from a device that is not in its latest roster.
2. drops an ask that does not open, or whose initiator static key
   differs from the roster's key for the sender.
3. remembers each ask it opens for 10 minutes, by its handshake hash
   after reading the first message, and drops one it has seen. The
   memory outlives reconnects of its hub socket. The service could
   replay an ask under any id, and this is what stops it.

None of these get an answer. The asker gives up on an ask after 10
seconds, at once when the asked device leaves the roster, and when the
service sends a `nack` for that device, which ends every ask pending to
it.

## Ask and answer

The first message's payload:

```json
{"ask":"connectInfo","input":{"dialableKinds":["lan","tunnel"],"connectionId":"<32 hex>"}}
```

The second message's payload is either a result or a refusal:

```json
{"ok":true,"result":{ … }}
{"ok":false,"code":"<code>"}
```

A refusal's `code` is one of:

- `no-listener`: the device never listens for peer links, such as a
  browser. The asker then stops dialing it until it leaves and comes
  back to the roster.
- `unknown-ask`: the device does not know the ask.
- `bad-input`: the ask's input does not decode.

An asker reads a code it does not know as a refusal too.

`connectInfo` is the only ask.

## connectInfo

The ask's input:

- `dialableKinds`: the kinds of candidate the asker can dial. A desktop
  app or a headless program can dial both `lan` and `tunnel`. A web page
  can only dial `tunnel`, since a page served over HTTPS cannot open an
  unencrypted `ws://` socket. The asked device answers for the kinds it
  knows, and leaves out any other.
- `connectionId`: the asker's connection id for the link it is about to
  open.

The result is either:

```json
{"available":false}
```

or a list of candidates, each with a ticket of its own:

```json
{"available":true,"candidates":[
  {"kind":"lan","url":"ws://192.168.1.20:41734","ticket":"colibrii_lt_…"},
  {"kind":"tunnel","url":"wss://<label>.example.link","ticket":"colibrii_lt_…"}
]}
```

- `lan` candidates come first. Each is `ws://<address>:<port>`, IPv6 in
  brackets, for each of the listener's addresses that is not internal,
  link-local, or zone-scoped. The port is the listener's, or the one
  the device has something in front of the listener take connections
  on, when it has. IPv4 comes before IPv6, and there are at
  most 6.
- The `tunnel` candidate comes last: `wss://<hostname>`, only while the
  device's tunnel is up ([tunnels.md](tunnels.md)).
- Only the kinds the asker can dial are offered. `available` is false
  when none can be.
- The asker skips an entry that names no kind it knows, and a
  candidate of a kind it did not ask for, and dials the rest. A candidate of a kind it knows that does
  not decode makes the whole answer unreadable.
- When an asker that can dial `tunnel` would get nothing, but the
  device's tunnel is starting, the device waits up to 6 seconds for it
  before answering.

What a ticket is, and how it is checked, is in [link.md](link.md).
