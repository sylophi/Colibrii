// A TypeScript device for the Rust SDK's interop tests. Run with node and
// the hub's address, it reads a command as a JSON line on stdin and says
// what happened as one on stdout: it enrolls with a login token, waits
// for a peer in its roster, and dials one. It welcomes every hello, and
// sends each app message back on the link it came on. It exits once stdin
// ends.
import { createInterface } from "node:readline";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Colibrii from "../src/index.ts";
import { layer } from "../src/node/index.ts";

type Command =
  | { readonly op: "enroll"; readonly loginToken: string }
  | { readonly op: "sees"; readonly deviceId: string }
  | { readonly op: "dial"; readonly deviceId: string; readonly hello: string };

const hub = process.argv[2] ?? "";
const say = (message: object) =>
  process.stdout.write(`${JSON.stringify(message)}\n`);
const base64url = (bytes: Uint8Array) =>
  Buffer.from(bytes).toString("base64url");

// The link's messages, each sent back as it comes.
const echo = (link: Colibrii.Link) =>
  Effect.gen(function* () {
    say({
      event: "linked",
      role: link.role,
      deviceId: link.deviceId,
      greeting: base64url(link.greeting),
    });
    yield* link.messages.pipe(
      Stream.runForEach((message) => link.send(message)),
      Effect.ignore,
    );
  });

const run = Effect.gen(function* () {
  let stored = Option.none<string>();
  const store = Colibrii.CredentialStore.of({
    read: Effect.sync(() => stored),
    write: (next) =>
      Effect.sync(() => {
        stored = Option.some(next);
      }),
    remove: Effect.sync(() => {
      stored = Option.none();
    }),
  });
  const device = yield* Colibrii.make({
    hub,
    app: "inkwell",
    connections: "single",
    listen: {
      admit: (hello) =>
        Effect.succeed(
          new TextEncoder().encode(
            `ts welcome ${new TextDecoder().decode(hello.appHello)}`,
          ),
        ),
      onLink: echo,
    },
  }).pipe(Effect.provideService(Colibrii.CredentialStore, store));
  const lines = createInterface({ input: process.stdin })[
    Symbol.asyncIterator
  ]();
  while (true) {
    const line = yield* Effect.promise(() => lines.next());
    if (line.done === true) return;
    const command = JSON.parse(line.value) as Command;
    switch (command.op) {
      case "enroll": {
        yield* device.enroll({ loginToken: command.loginToken, name: "TS" });
        const enrolled = yield* device.account.pipe(
          Stream.filter(
            (state) => state._tag === "Enrolled" && state.connected,
          ),
          Stream.runHead,
        );
        say({
          event: "enrolled",
          deviceId: Option.match(enrolled, {
            onNone: () => "",
            onSome: (now) => (now._tag === "SignedOut" ? "" : now.deviceId),
          }),
        });
        break;
      }
      case "sees":
        yield* device.roster.pipe(
          Stream.filter((peers) =>
            peers.some((peer) => peer.deviceId === command.deviceId),
          ),
          Stream.runHead,
        );
        say({ event: "sees" });
        break;
      case "dial": {
        const link = yield* device.dial(
          command.deviceId,
          Buffer.from(command.hello, "base64url"),
        );
        yield* Effect.forkScoped(echo(link));
        break;
      }
    }
  }
});

await Effect.runPromise(run.pipe(Effect.scoped, Effect.provide(layer)));
