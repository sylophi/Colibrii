// The service's two uses of an app's Clerk instance: checking a login
// token with the instance's public key, without a network call, and
// minting a sign-in token with its secret key.
//
// verifyJwt is given the app's key directly. Clerk's verifyToken would
// cache the key under the token's `kid`, so a token from another app's
// Clerk instance could be checked with a key cached for this one.
import { createClerkClient } from "@clerk/backend";
import { decodeJwt, verifyJwt } from "@clerk/backend/jwt";
import { originAllowed } from "./apps.ts";
import type { App } from "./config.ts";

const SESSION_TYPE = "JWT";
const OAUTH_TYPES = ["at+jwt", "application/at+jwt"];

export interface Login {
  readonly accountId: string;
  // An OAuth access token, not a session token.
  readonly oauth: boolean;
}

// Null when the token is not a valid login for the app.
export const verifyLogin = async (
  token: string,
  app: App,
): Promise<Login | null> => {
  let type: string | undefined;
  try {
    type = decodeJwt(token).header.typ;
  } catch {
    return null;
  }
  const oauth = type !== undefined && OAUTH_TYPES.includes(type);
  if (!oauth && type !== SESSION_TYPE) return null;
  let payload: Awaited<ReturnType<typeof verifyJwt>>;
  try {
    payload = await verifyJwt(token, {
      key: app.clerkPublicKey,
      headerType: oauth ? OAUTH_TYPES : SESSION_TYPE,
    });
  } catch {
    return null;
  }
  const issuedFor = oauth
    ? app.oauthClientId !== undefined &&
      payload["client_id"] === app.oauthClientId
    : typeof payload.azp === "string" &&
      originAllowed(app.origins, payload.azp);
  return issuedFor && payload.sub !== ""
    ? { accountId: payload.sub, oauth }
    : null;
};

// Redeemed once, within this long.
const SIGN_IN_TOKEN_SECONDS = 300;

export const mintSignInToken = async (
  secretKey: string,
  accountId: string,
): Promise<string> => {
  const { token } = await createClerkClient({
    secretKey,
  }).signInTokens.createSignInToken({
    userId: accountId,
    expiresInSeconds: SIGN_IN_TOKEN_SECONDS,
  });
  return token;
};
