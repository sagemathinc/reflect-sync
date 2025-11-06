import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";

import { makeMicroSync } from "../micro-sync";
import { getDb } from "../db";
import { ConsoleLogger } from "../logger";
import { getRecentSendSignatures } from "../recent-send";

jest.mock("../rsync.js", () => {
  const run = jest.fn().mockResolvedValue({ zero: false });
  return {
    run,
    assertRsyncOk: jest.fn(),
    ensureTempDir: jest.fn(async () => ".reflect-rsync-tmp"),
  };
});

const { run: runRsync } = jest.requireMock("../rsync.js") as {
  run: jest.MockedFunction<any>;
};

function insertFileRow(
  dbPath: string,
  pathRel: string,
  opTs: number,
  hash: string,
) {
  const db = getDb(dbPath);
  try {
    db.prepare(
      `
        INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
        VALUES(?, ?, ?, ?, ?, ?, 0, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
          size = excluded.size,
          ctime = excluded.ctime,
          mtime = excluded.mtime,
          op_ts = excluded.op_ts,
          hash = excluded.hash,
          deleted = 0,
          last_seen = excluded.last_seen,
          hashed_ctime = excluded.hashed_ctime
      `,
    ).run(pathRel, 10, opTs, opTs, opTs, hash, opTs, opTs);
  } finally {
    db.close();
  }
}

describe("micro-sync recent-send integration", () => {
  it("records alpha→beta copies and suppresses immediate beta→alpha echoes", async () => {
    const work = await fs.mkdtemp(
      path.join(os.tmpdir(), "reflect-micro-recent-"),
    );
    const alphaRoot = path.join(work, "alpha");
    const betaRoot = path.join(work, "beta");
    await fs.mkdir(alphaRoot, { recursive: true });
    await fs.mkdir(betaRoot, { recursive: true });

    const alphaDbPath = path.join(work, "alpha.db");
    const betaDbPath = path.join(work, "beta.db");

    const now = Date.now();
    const pathRel = "foo.txt";
    const hash = "hash-one";

    insertFileRow(alphaDbPath, pathRel, now, hash);

    const logger = new ConsoleLogger("error");
    const micro = makeMicroSync({
      alphaRoot,
      betaRoot,
      alphaDbPath,
      betaDbPath,
      alphaHost: "alpha-remote",
      betaHost: "beta-remote",
      prefer: "alpha",
      dryRun: true,
      log: () => {},
      logger,
      compress: "none",
      isMergeActive: () => false,
    });

    runRsync.mockClear();

    await micro([pathRel], []);

    expect(runRsync).toHaveBeenCalledTimes(1);
    const betaRecent = getRecentSendSignatures(betaDbPath, "alpha->beta", [
      pathRel,
    ]);
    expect(betaRecent.get(pathRel)).toMatchObject({
      kind: "file",
      opTs: now,
      hash,
    });

    insertFileRow(betaDbPath, pathRel, now, hash);

    await micro([], [pathRel]);

    expect(runRsync).toHaveBeenCalledTimes(1);
  });
});
