// The real Worker in workerd, through wrangler's test harness, with two
// apps whose Clerk key pairs are made here for each run. The service's
// tests and the SDK's share it.
import { generateKeyPairSync, randomBytes } from "node:crypto";
import { connect } from "node:net";
import { join } from "node:path";
import { signJwt } from "@clerk/backend/jwt";
import { afterAll } from "vitest";
import { createTestHarness } from "wrangler";

const serviceDir = join(import.meta.dirname, "../../service");

const keyPair = () =>
  generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });

const keys = { a: keyPair(), b: keyPair() };

// The public half of inkwell's key pair.
export const clerkPublicKey = keys.a.publicKey;

// inkwell has a small cap, so a full account takes few devices. quill
// has a Clerk secret key, so it mints sign-in tokens, and takes a web
// client on any port of localhost.
export const INKWELL_CAP = 3;
const apps = [
  {
    id: "inkwell",
    clerkPublicKey,
    origins: ["inkwell://app", "https://inkwell.example"],
    oauthClientId: "inkwell-oauth",
    deviceCap: INKWELL_CAP,
  },
  {
    id: "quill",
    clerkPublicKey: keys.b.publicKey,
    origins: ["quill://app", "http://localhost:*"],
    oauthClientId: "quill-oauth",
    clerkSecretName: "QUILL_CLERK_SECRET_KEY",
  },
];

// It serves `served` in place of the two apps, to test the instance's
// own checks.
export const startService = async (served: ReadonlyArray<object> = apps) => {
  const server = createTestHarness({
    root: serviceDir,
    workers: [
      {
        configPath: "./wrangler.jsonc",
        vars: { APPS: served },
        secrets: {
          TICKET_SIGNING_KEY: "local-only",
          CLOUDFLARE_API_TOKEN: "local-only",
          QUILL_CLERK_SECRET_KEY: "local-only",
        },
      },
    ],
  });
  const { url } = await server.listen();
  const worker = server.getWorker();
  await worker.applyD1Migrations("DB");
  afterAll(() => server.close());
  return { url: url.href, worker };
};

// ---- Login tokens ----

interface TokenOptions {
  readonly key?: keyof typeof keys;
  readonly typ?: string;
  readonly claims?: Record<string, unknown>;
}

const sign = (claims: Record<string, unknown>, options: TokenOptions) =>
  signJwt(
    {
      iss: "https://clerk.local.example",
      exp: Math.floor(Date.now() / 1000) + 60,
      nbf: Math.floor(Date.now() / 1000) - 5,
      ...claims,
      ...options.claims,
    },
    keys[options.key ?? "a"].privateKey,
    {
      algorithm: "RS256",
      header: { typ: options.typ ?? "JWT", kid: "ins_local" },
    },
  );

export const sessionToken = (
  accountId: string,
  azp = "inkwell://app",
  options: TokenOptions = {},
) => sign({ sub: accountId, azp, sid: "sess_local" }, options);

export const oauthToken = (
  accountId: string,
  clientId = "inkwell-oauth",
  options: TokenOptions = {},
) =>
  sign(
    { sub: accountId, client_id: clientId, scope: "profile" },
    { typ: "at+jwt", ...options },
  );

// ---- Ids ----

export const newAccountId = () => `user_${crypto.randomUUID().slice(0, 18)}`;

export const newDeviceId = () => crypto.randomUUID();

export const newConnectionId = () => crypto.randomUUID().replaceAll("-", "");

export const newPublicKey = () =>
  Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64url");

// A client IP of its own for each request, so the rate limits only meet
// a test that means to reach them.
export const newIp = () =>
  Array.from(crypto.getRandomValues(new Uint8Array(4))).join(".");

// ---- HTTP ----

export interface CallOptions {
  readonly bearer?: string;
  readonly body?: unknown;
  // Sent as it is.
  readonly rawBody?: string;
  readonly headers?: Record<string, string>;
}

export const client = (base: string) => {
  const call = async (
    method: string,
    path: string,
    options: CallOptions = {},
  ) => {
    const response = await fetch(new URL(path, base), {
      method,
      headers: {
        "colibrii-version": "1",
        "cf-connecting-ip": newIp(),
        ...(options.bearer === undefined
          ? {}
          : { authorization: `Bearer ${options.bearer}` }),
        ...(options.body === undefined
          ? {}
          : { "content-type": "application/json" }),
        ...options.headers,
      },
      body:
        options.rawBody ??
        (options.body === undefined ? undefined : JSON.stringify(options.body)),
    });
    const text = await response.text();
    return {
      status: response.status,
      headers: response.headers,
      text,
      json: text === "" ? undefined : JSON.parse(text),
    };
  };

  const enroll = async (options: {
    readonly token: string;
    readonly app?: string;
    readonly deviceId?: string;
    readonly connections?: "single" | "multi";
    readonly publicKey?: string;
    readonly data?: unknown;
    readonly name?: string;
  }) => {
    const deviceId = options.deviceId ?? newDeviceId();
    const publicKey = options.publicKey ?? newPublicKey();
    const answer = await call("POST", "/devices/enroll", {
      bearer: options.token,
      body: {
        app: options.app ?? "inkwell",
        deviceId,
        name: options.name ?? "Test device",
        publicKey,
        connections: options.connections ?? "single",
        ...(options.data === undefined ? {} : { data: options.data }),
      },
    });
    return {
      ...answer,
      deviceId,
      publicKey,
      credential: answer.json?.credential as string,
    };
  };

  // An enrollment that has to go through, under a new account unless
  // one is given.
  const enrolled = async (
    options: Omit<Parameters<typeof enroll>[0], "token"> & {
      readonly account?: string;
    } = {},
  ) => {
    const account = options.account ?? newAccountId();
    const device = await enroll({
      token: await sessionToken(account),
      ...options,
    });
    if (device.status !== 200) {
      throw new Error(`enrolling answered ${device.status}: ${device.text}`);
    }
    return { ...device, account };
  };

  const ticket = async (credential: string, connectionId = newConnectionId()) =>
    (
      await call("POST", "/tickets", {
        bearer: credential,
        body: { connectionId },
      })
    ).json?.ticket as string;

  return { call, enroll, enrolled, ticket };
};

// ---- The hub socket ----

// A bare websocket client. workerd, run locally, never ends the
// connection after closing a hibernated socket, and a standard client
// reports the close code only once the connection ends. This one reports
// it as the close frame arrives.
export interface Socket {
  readonly send: (message: string | Uint8Array) => void;
  readonly close: () => void;
  // The next text message, parsed when it is JSON.
  readonly next: () => Promise<unknown>;
  // Messages until one passes, the ones before dropped.
  readonly until: (test: (message: unknown) => boolean) => Promise<unknown>;
  readonly closed: Promise<{ readonly code: number }>;
}

const frame = (opcode: number, payload: Buffer) => {
  const { length } = payload;
  const header = Buffer.alloc(length < 126 ? 2 : length < 65_536 ? 4 : 10);
  header[0] = 0x80 | opcode;
  if (length < 126) header[1] = 0x80 | length;
  else if (length < 65_536) {
    header[1] = 0x80 | 126;
    header.writeUInt16BE(length, 2);
  } else {
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }
  const mask = randomBytes(4);
  const masked = payload.map((byte, i) => byte ^ (mask[i % 4] ?? 0));
  return Buffer.concat([header, mask, masked]);
};

// `unanswering` keeps the connection after the service's close frame,
// as a device that never answers it would, until `close`.
export const openSocket = (
  base: string,
  ticket: string,
  options: { readonly unanswering?: boolean } = {},
): Promise<Socket> =>
  new Promise((resolve, reject) => {
    const { hostname, port } = new URL(base);
    const tcp = connect(Number(port), hostname);
    tcp.write(
      [
        `GET /connect?ticket=${encodeURIComponent(ticket)} HTTP/1.1`,
        `Host: ${hostname}:${port}`,
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Key: ${randomBytes(16).toString("base64")}`,
        "Sec-WebSocket-Version: 13",
        `cf-connecting-ip: ${newIp()}`,
        "",
        "",
      ].join("\r\n"),
    );
    const queue: unknown[] = [];
    const waiting: Array<(message: unknown) => void> = [];
    const closing = Promise.withResolvers<{ code: number }>();
    const deliver = (text: string) => {
      let message: unknown = text;
      try {
        message = JSON.parse(text);
      } catch {
        // Plain text, such as pong.
      }
      const waiter = waiting.shift();
      if (waiter === undefined) queue.push(message);
      else waiter(message);
    };
    const next = () =>
      new Promise<unknown>((done, fail) => {
        if (queue.length > 0) return done(queue.shift());
        const waiter = (message: unknown) => {
          clearTimeout(timer);
          done(message);
        };
        const timer = setTimeout(() => {
          waiting.splice(waiting.indexOf(waiter), 1);
          fail(new Error("no message within 5 seconds"));
        }, 5000);
        waiting.push(waiter);
      });
    const socket: Socket = {
      send: (message) =>
        tcp.write(
          frame(typeof message === "string" ? 1 : 2, Buffer.from(message)),
        ),
      close: () => {
        tcp.write(frame(8, Buffer.from([0x03, 0xe8])));
        tcp.end();
      },
      next,
      until: async (test) => {
        for (;;) {
          const message = await next();
          if (test(message)) return message;
        }
      },
      closed: closing.promise,
    };
    let buffer = Buffer.alloc(0);
    let open = false;
    tcp.on("error", reject);
    tcp.on("close", () => closing.resolve({ code: 1006 }));
    tcp.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (!open) {
        const end = buffer.indexOf("\r\n\r\n");
        if (end < 0) return;
        const status = buffer.subarray(0, end).toString().split("\r\n")[0];
        if (status?.startsWith("HTTP/1.1 101") !== true) {
          tcp.destroy();
          return reject(new Error(String(status)));
        }
        open = true;
        buffer = buffer.subarray(end + 4);
        resolve(socket);
      }
      for (;;) {
        if (buffer.length < 2) return;
        const opcode = (buffer[0] ?? 0) & 0x0f;
        const short = (buffer[1] ?? 0) & 0x7f;
        const offset = short === 126 ? 4 : short === 127 ? 10 : 2;
        if (buffer.length < offset) return;
        const length =
          short === 126
            ? buffer.readUInt16BE(2)
            : short === 127
              ? Number(buffer.readBigUInt64BE(2))
              : short;
        if (buffer.length < offset + length) return;
        const payload = buffer.subarray(offset, offset + length);
        buffer = buffer.subarray(offset + length);
        if (opcode === 1) deliver(payload.toString());
        if (opcode === 8) {
          closing.resolve({
            code: payload.length >= 2 ? payload.readUInt16BE(0) : 1005,
          });
          if (options.unanswering !== true) tcp.destroy();
          return;
        }
      }
    });
  });

export const isPresence = (message: unknown) =>
  (message as { t?: string }).t === "presence";

// The device ids in a presence message.
export const rosterOf = (message: unknown) =>
  (message as { online: Array<{ deviceId: string }> }).online.map(
    (device) => device.deviceId,
  );
