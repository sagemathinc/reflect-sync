import { createTestSession, type TestSession } from "./env.js";
import {
  CASEFOLD_ROOT,
  NOT_CASEFOLD_ROOT,
  hasCasefoldRoot,
  hasNotCasefoldRoot,
} from "../casefold.js";
import path from "node:path";
import { promises as fsp } from "node:fs";
import { Database } from "../../db.js";

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

  it("keeps conflicting directory trees isolated when syncing into a case-insensitive root", async () => {
    if (!session) throw new Error("session not initialized");

    await session.beta.mkdir("Project");
    await session.beta.writeFile("Project/report.txt", "project-report");
    await session.beta.mkdir("project");
    await session.beta.writeFile("project/draft.txt", "project-draft");

    await session.sync();

    const alphaDirs = await listDirectories(session.alpha.path(""));
    const alphaSnapshotBefore = await snapshotTree(session.alpha.path(""));
    const canonicalDir = alphaDirs[0] ?? "Project";
    const alphaDirFiles = await fsp.readdir(session.alpha.path(canonicalDir));
    const canonicalFile = alphaDirFiles[0];
    const canonicalContent =
      canonicalFile !== undefined
        ? (
            await session.alpha.readFile(
              `${canonicalDir}/${canonicalFile}`,
              "utf8",
            )
          ).toString()
        : null;

    // Update the conflicting subtree on beta (the one that was not mirrored).
    const conflictingDir = canonicalDir === "Project" ? "project" : "Project";
    const conflictingFile =
      conflictingDir === "Project" ? "report.txt" : "draft.txt";
    await session.beta.writeFile(
      `${conflictingDir}/${conflictingFile}`,
      "conflict-update",
    );

    await session.sync();

    // Alpha should still only have the canonical directory, untouched.
    const alphaSnapshotAfter = await snapshotTree(session.alpha.path(""));
    expect(alphaSnapshotAfter).toEqual(alphaSnapshotBefore);

    if (canonicalFile && canonicalContent !== null) {
      const canonicalContentAfter = (
        await session.alpha.readFile(`${canonicalDir}/${canonicalFile}`, "utf8")
      ).toString();
      expect(canonicalContentAfter).toBe(canonicalContent);
    }

    // Case conflict is recorded in beta's database.
    const betaRows = readCaseConflictRows(session.betaDbPath, [
      "Project",
      "project",
    ]);
    expect(betaRows.some((row) => Number(row.case_conflict ?? 0) === 1)).toBe(
      true,
    );
    expect(betaRows.some((row) => Number(row.case_conflict ?? 0) === 0)).toBe(
      true,
    );
    // Both directories still exist on beta; only the conflicting subtree changed.
    await expect(
      session.beta.readFile(`${conflictingDir}/${conflictingFile}`, "utf8"),
    ).resolves.toBe("conflict-update");
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
    const remaining = alphaFiles[0];
    const remainingContent = (
      await session.alpha.readFile(remaining, "utf8")
    ).toString();
    expect(["lowercase", "uppercase"]).toContain(remainingContent);

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
    const betaContent = (
      await session.beta.readFile(betaAfter2[0], "utf8")
    ).toString();
    expect(betaContent).toBe(remainingContent);
  });

  it("promotes the surviving directory variant when case-insensitive beta loses its canonical subtree", async () => {
    if (!session) throw new Error("session not initialized");

    await session.alpha.mkdir("Docs");
    await session.alpha.writeFile("Docs/notes.txt", "docs-notes");
    await session.alpha.mkdir("docs");
    await session.alpha.writeFile("docs/todo.txt", "docs-todo");
    await session.sync();

    const betaRows = readCaseConflictRows(session.betaDbPath, [
      "Docs",
      "docs",
    ]);
    const canonicalRow =
      betaRows.find((row) => Number(row.case_conflict ?? 0) === 0) ?? betaRows[0];
    await session.beta.rm(canonicalRow.path, {
      recursive: true,
      force: true,
    });
    await session.sync();

    const alphaDirs = await listDirectories(session.alpha.path(""));
    expect(alphaDirs.length).toBeGreaterThan(0);

    await session.sync();

    const betaFinalDirs = await listDirectories(session.beta.path(""));
    expect(betaFinalDirs.length).toBeGreaterThan(0);
    const alphaSnapshotFinal = await listDirectories(session.alpha.path(""));
    expect(betaFinalDirs.sort()).toEqual(alphaSnapshotFinal.sort());
  });
});

async function listDirectories(root: string): Promise<string[]> {
  const entries = await fsp.readdir(root, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .filter((entry) => !entry.name.startsWith(".reflect"))
    .map((entry) => entry.name)
    .sort();
}

async function snapshotTree(root: string): Promise<string[]> {
  const entries = await fsp.readdir(root, { withFileTypes: true });
  const results: string[] = [];
  for (const entry of entries) {
    if (entry.name.startsWith(".reflect")) continue;
    if (entry.isDirectory()) {
      const child = await snapshotTree(path.join(root, entry.name));
      for (const leaf of child) {
        results.push(path.join(entry.name, leaf));
      }
      if (child.length === 0) {
        results.push(entry.name + "/");
      }
    } else {
      results.push(entry.name);
    }
  }
  return results.sort();
}

function readCaseConflictRows(
  dbPath: string,
  paths: string[],
): { path: string; case_conflict: number | null }[] {
  const db = new Database(dbPath);
  try {
    const placeholders = paths.map(() => "?").join(",");
    return db
      .prepare(
        `SELECT path, case_conflict FROM nodes WHERE path IN (${placeholders})`,
      )
      .all(...paths) as { path: string; case_conflict: number | null }[];
  } finally {
    db.close();
  }
}
