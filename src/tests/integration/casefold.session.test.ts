import { createTestSession, type TestSession } from "./env.js";
import {
  CASEFOLD_ROOT,
  NOT_CASEFOLD_ROOT,
  hasCasefoldRoot,
  hasNotCasefoldRoot,
} from "../casefold.js";
import path from "node:path";
import { promises as fsp } from "node:fs";

const describeIfCasefold =
  hasCasefoldRoot && hasNotCasefoldRoot ? describe : describe.skip;

jest.setTimeout(15_000);

describeIfCasefold("casefold integration", () => {
  let session: TestSession | undefined;
  let alphaRoot: string | undefined;
  let betaRoot: string | undefined;

  beforeEach(async () => {
    alphaRoot = await fsp.mkdtemp(
      path.join(CASEFOLD_ROOT, "reflect-casefold-alpha-"),
    );
    betaRoot = await fsp.mkdtemp(
      path.join(NOT_CASEFOLD_ROOT, "reflect-casefold-beta-"),
    );
    session = await createTestSession({
      hot: false,
      full: false,
      alpha: { root: alphaRoot },
      beta: { root: betaRoot },
    });
  });

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
    if (alphaRoot) {
      await fsp.rm(alphaRoot, { recursive: true, force: true });
      alphaRoot = undefined;
    }
    if (betaRoot) {
      await fsp.rm(betaRoot, { recursive: true, force: true });
      betaRoot = undefined;
    }
  });

  it("does not overwrite conflicting filenames on case-insensitive roots", async () => {
    if (!session) throw new Error("session not initialized");

    await session.alpha.writeFile("b.txt", "alpha-original");
    await session.sync();

    await session.beta.writeFile("B.txt", "uppercase");
    await session.sync();

    await expect(session.beta.readFile("b.txt", "utf8")).resolves.toBeDefined();

    await session.alpha.writeFile("b.txt", "alpha-update");
    await session.sync();

    await expect(session.beta.readFile("b.txt", "utf8")).resolves.toBe(
      "alpha-update",
    );
    await expect(session.beta.readFile("B.txt", "utf8")).resolves.toBe(
      "uppercase",
    );
  });
});

describeIfCasefold("casefold reverse integration", () => {
  let session: TestSession | undefined;
  let alphaRoot: string | undefined;
  let betaRoot: string | undefined;

  beforeEach(async () => {
    alphaRoot = await fsp.mkdtemp(
      path.join(NOT_CASEFOLD_ROOT, "reflect-casefold-alpha-"),
    );
    betaRoot = await fsp.mkdtemp(
      path.join(CASEFOLD_ROOT, "reflect-casefold-beta-"),
    );
    session = await createTestSession({
      hot: false,
      full: false,
      alpha: { root: alphaRoot },
      beta: { root: betaRoot },
    });
  });

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
    if (alphaRoot) {
      await fsp.rm(alphaRoot, { recursive: true, force: true });
      alphaRoot = undefined;
    }
    if (betaRoot) {
      await fsp.rm(betaRoot, { recursive: true, force: true });
      betaRoot = undefined;
    }
  });

  it("preserves case variants on the sensitive side when the insensitive side deletes its winner", async () => {
    if (!session) throw new Error("session not initialized");

    await session.alpha.writeFile("c.txt", "lowercase");
    await session.alpha.writeFile("C.txt", "uppercase");
    await session.sync();

    const betaFiles = (
      await fsp.readdir(session.beta.path(""), { withFileTypes: false })
    ).filter((name) => name.endsWith(".txt"));
    expect(betaFiles.length).toBe(1);
    const preserved = betaFiles[0];

    await session.beta.rm(preserved, { recursive: false, force: true });
    await session.sync();

    const alphaFiles = (
      await fsp.readdir(session.alpha.path(""), { withFileTypes: false })
    ).filter((name) => name.endsWith(".txt"));
    expect(alphaFiles.length).toBe(1);

    const betaAfter = (
      await fsp.readdir(session.beta.path(""), { withFileTypes: false })
    ).filter((name) => name.endsWith(".txt"));
    expect(betaAfter.length).toBe(0);
    // console.log({ betaAfter, alphaFiles });

    // now that one is deleted, the other takes over and gets sync'd
    await session.sync();
    const alphaFiles2 = (
      await fsp.readdir(session.alpha.path(""), { withFileTypes: false })
    ).filter((name) => name.endsWith(".txt"));
    expect(alphaFiles2.length).toBe(1);
    const betaAfter2 = (
      await fsp.readdir(session.beta.path(""), { withFileTypes: false })
    ).filter((name) => name.endsWith(".txt"));
    expect(betaAfter2.length).toBe(1);
    expect(alphaFiles2).toEqual(betaAfter2);
    // console.log({ betaAfter2, alphaFiles2 });
  });
});
