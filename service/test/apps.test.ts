import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { clerkPublicKey as key } from "@colibrii/harness";
import {
  type App,
  clash,
  decodeApp,
  decodeAppRow,
  LIST_APPS_SQL,
  putAppSql,
  removeAppSql,
} from "../src/apps.ts";

const capped = (deviceCap: number) =>
  decodeApp({ id: "inkwell", clerkPublicKey: key, origins: [], deviceCap });

const app = (id: string, origin: string, oauthClientId: string) =>
  decodeApp({ id, clerkPublicKey: key, origins: [origin], oauthClientId });

describe("an app", () => {
  it("takes an id of 1 to 32 lowercase letters, digits or -", () => {
    for (const id of ["", "Inkwell", "ink_well", "a".repeat(33)]) {
      expect(() => app(id, "inkwell://app", "inkwell-oauth"), id).toThrow();
    }
    expect(app("a".repeat(32), "inkwell://app", "o").id).toBe("a".repeat(32));
  });

  it("takes a PEM public key", () => {
    expect(() =>
      decodeApp({ id: "inkwell", clerkPublicKey: "not a key", origins: [] }),
    ).toThrow();
  });

  it("takes a device cap from 1 to 1,024, 16 unless given", () => {
    for (const cap of [0, 1025, 1.5]) expect(() => capped(cap)).toThrow();
    expect(capped(1024).deviceCap).toBe(1024);
    expect(app("inkwell", "inkwell://app", "o").deviceCap).toBe(16);
  });

  it("refuses empty values", () => {
    for (const field of ["origins", "oauthClientId", "clerkSecretName"]) {
      expect(
        () =>
          decodeApp({
            id: "inkwell",
            clerkPublicKey: key,
            origins: [],
            [field]: field === "origins" ? [""] : "",
          }),
        field,
      ).toThrow();
    }
  });
});

describe("apps that share a Clerk instance", () => {
  const inkwell = app("inkwell", "inkwell://app", "inkwell-oauth");

  it("may join one with no origin or OAuth client in common", () => {
    expect(
      clash(app("inkwell-dev", "inkwell-dev://app", "inkwell-dev-oauth"), [
        inkwell,
      ]),
    ).toBeUndefined();
  });

  it("may not share an origin or an OAuth client with one already there", () => {
    const shared: ReadonlyArray<App> = [
      app("inkwell-dev", "inkwell://app", "inkwell-dev-oauth"),
      {
        ...app("inkwell-dev", "inkwell-dev://app", "inkwell-oauth"),
        // The same key, written differently.
        clerkPublicKey: `  ${key.trim().replaceAll("\n", "\r\n")}\n\n`,
      },
    ];
    for (const newcomer of shared) {
      expect(clash(newcomer, [inkwell])).toMatch(/inkwell-dev and inkwell/);
    }
    expect(
      clash(app("inkwell-dev", "http://localhost:*", "inkwell-dev-oauth"), [
        app("inkwell", "http://localhost:5173", "inkwell-oauth"),
      ]),
    ).toBeDefined();
  });

  it("is checked against the others alone, since one replaces its own id", () => {
    expect(
      clash(app("inkwell", "inkwell://app", "inkwell-oauth"), [inkwell]),
    ).toBeUndefined();
  });
});

// An empty database with the migrations applied.
const database = () => {
  const db = new DatabaseSync(":memory:");
  const migrations = join(import.meta.dirname, "../migrations");
  for (const file of readdirSync(migrations).toSorted()) {
    db.exec(readFileSync(join(migrations, file), "utf8"));
  }
  return db;
};

describe("the statements", () => {
  it("carry any value as it is, with nothing of it read as SQL", () => {
    const db = database();
    const hostile = "x'); DROP TABLE devices; -- \" ; \\ \n ✓";
    const written = decodeApp({
      id: "inkwell",
      clerkPublicKey: key,
      origins: [hostile, "inkwell://app"],
      oauthClientId: hostile,
      clerkSecretName: hostile,
    });
    db.exec(putAppSql(written));
    expect(db.prepare(LIST_APPS_SQL).all().map(decodeAppRow)).toEqual([
      written,
    ]);
    expect(
      db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all(),
    ).toContainEqual({ name: "devices" });
    expect(db.prepare(removeAppSql(hostile)).all()).toEqual([]);
    expect(db.prepare(removeAppSql("inkwell")).all()).toEqual([
      { id: "inkwell" },
    ]);
    expect(db.prepare(LIST_APPS_SQL).all()).toEqual([]);
  });

  it("replace an app under the same id", () => {
    const db = database();
    db.exec(putAppSql(app("inkwell", "inkwell://app", "inkwell-oauth")));
    const changed = app("inkwell", "https://inkwell.example", "other");
    db.exec(putAppSql(changed));
    expect(db.prepare(LIST_APPS_SQL).all().map(decodeAppRow)).toEqual([
      changed,
    ]);
  });
});
