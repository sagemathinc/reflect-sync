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

    await expect(
      session.beta.readFile("b.txt", "utf8"),
    ).resolves.toBeDefined();

    await session.alpha.writeFile("b.txt", "alpha-update");
    await session.sync();

    await expect(
      session.beta.readFile("b.txt", "utf8"),
    ).resolves.toBe("alpha-update");
    await expect(
      session.beta.readFile("B.txt", "utf8"),
    ).resolves.toBe("uppercase");
  });
});
