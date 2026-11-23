// The connect ticket's string (spec/hub-socket.md, "Connect tickets").
// The route part lets GET /connect find the account's object without a
// database read, and the signature keeps that unauthenticated route from
// waking objects of a caller's choosing. What makes a ticket valid is
// the random part the object stored.
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Base64Url from "@colibrii/sdk/wire/base64url";
import {
  CONNECT_TICKET_PREFIX,
  ConnectTicket,
} from "@colibrii/sdk/wire/ticket";
import type { Account } from "./Registry.ts";

const hmacKey = (signingKey: string) =>
  crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(signingKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );

export const buildTicket = async (
  signingKey: string,
  account: Account,
  random: string,
): Promise<string> => {
  const body = `${Base64Url.encode(new TextEncoder().encode(`${account.appId}/${account.accountId}`))}.${random}`;
  const signature = await crypto.subtle.sign(
    "HMAC",
    await hmacKey(signingKey),
    new TextEncoder().encode(body),
  );
  return `${CONNECT_TICKET_PREFIX}${body}.${Base64Url.encode(new Uint8Array(signature))}`;
};

const isTicket = Schema.is(ConnectTicket);

// Null for anything malformed or not signed with this key.
export const parseTicket = async (
  signingKey: string,
  ticket: string,
): Promise<{ readonly account: Account; readonly random: string } | null> => {
  if (!isTicket(ticket)) return null;
  const [route = "", random = "", signature = ""] = ticket
    .slice(CONNECT_TICKET_PREFIX.length)
    .split(".");
  const routeBytes = Base64Url.decode(route);
  const signatureBytes = Base64Url.decode(signature);
  if (Option.isNone(routeBytes) || Option.isNone(signatureBytes)) return null;
  const signed = await crypto.subtle.verify(
    "HMAC",
    await hmacKey(signingKey),
    signatureBytes.value,
    new TextEncoder().encode(`${route}.${random}`),
  );
  if (!signed) return null;
  const text = new TextDecoder().decode(routeBytes.value);
  const slash = text.indexOf("/");
  return {
    account: { appId: text.slice(0, slash), accountId: text.slice(slash + 1) },
    random,
  };
};
