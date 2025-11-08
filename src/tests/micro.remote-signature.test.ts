import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";

import { makeMicroSync } from "../micro-sync";
import { getDb } from "../db";
import { getRecentSendSignatures, type SendSignature } from "../recent-send";
import type { SignatureEntry } from "../signature-store";

jest.mock("../rsync.js", () => {
  const run = jest.fn().mockResolvedValue({
    code: 0,
    ok: true,
    zero: true,
    stderr: "",
  });
  return {
    __esModule: true,
    run,
    assertRsyncOk: jest.fn(),
    ensureTempDir: jest.fn(async () => ".reflect-rsync-tmp"),
  };
});

const mockedRsync = jest.requireMock("../rsync.js") as {
  run: jest.MockedFunction<any>;
};

afterEach(() => {
  mockedRsync.run.mockReset();
  mockedRsync.run.mockResolvedValue({
    code: 0,
    ok: true,
    zero: true,
    stderr: "",
  });
});

const createTestLogger = () => {
  const logger: any = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };
  logger.child = jest.fn(() => logger);
  return logger;
};

function insertRecentSend(
  dbPath: string,
  direction: "alpha->beta" | "beta->alpha",
  pathRel: string,
  signature: SendSignature,
) {
  const db = getDb(dbPath);
  try {
    db.prepare(
      `
      INSERT INTO recent_send(path, direction, op_ts, signature, sent_at)
      VALUES(?, ?, ?, ?, ?)
      ON CONFLICT(path, direction) DO UPDATE SET
        op_ts = excluded.op_ts,
        signature = excluded.signature,
        sent_at = excluded.sent_at
    `,
    ).run(
      pathRel,
      direction,
      signature.opTs ?? null,
      JSON.stringify(signature),
      Date.now(),
    );
  } finally {
    db.close();
  }
}

describe("micro-sync remote signature coordination", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fs.mkdtemp(path.join(os.tmpdir(), "micro-remote-"));
  });

  afterAll(async () => {
    await fs.rm(tmp, { recursive: true, force: true });
  });

  afterEach(() => {
    mockedRsync.run.mockClear();
  });

  test("remote alpha fetches signatures with ignore=true when path was recently sent", async () => {
    const alphaRoot = path.join(tmp, "alpha");
    const betaRoot = path.join(tmp, "beta");
    await fs.mkdir(alphaRoot, { recursive: true });
    await fs.mkdir(betaRoot, { recursive: true });

    const alphaDb = path.join(tmp, "alpha.db");
    const betaDb = path.join(tmp, "beta.db");
    getDb(alphaDb).close();
    getDb(betaDb).close();

    const relPath = "cocalc/.git/objects/pack/tmp_pack_remote";
    insertRecentSend(alphaDb, "beta->alpha", relPath, {
      kind: "file",
      opTs: Date.now() - 5000,
      mtime: Date.now() - 5000,
      size: 123,
    });

    const fetchAlpha = jest.fn(
      async (_paths: string[], _opts: { ignore: boolean }) => {
        return [] as SignatureEntry[];
      },
    );

    const micro = makeMicroSync({
      alphaRoot,
      betaRoot,
      alphaDbPath: alphaDb,
      betaDbPath: betaDb,
      alphaHost: "remote-alpha",
      prefer: "alpha",
      dryRun: false,
      log: () => {},
      logger: createTestLogger(),
      fetchRemoteAlphaSignatures: fetchAlpha,
    });

    await micro([relPath], []);

    expect(fetchAlpha).toHaveBeenCalledTimes(1);
    expect(fetchAlpha.mock.calls[0]?.[0]).toEqual([relPath]);
    expect(fetchAlpha.mock.calls[0]?.[1]).toEqual({ ignore: true });

    expect(mockedRsync.run).toHaveBeenCalled();

    const betaRecent = getRecentSendSignatures(betaDb, "alpha->beta", [
      relPath,
    ]);
    expect(betaRecent.get(relPath)).not.toBeUndefined();
  });

  test("remote beta is refreshed with ignore=true before and after copy", async () => {
    const alphaRoot = path.join(tmp, "alpha2");
    const betaRoot = path.join(tmp, "beta2");
    await fs.mkdir(alphaRoot, { recursive: true });
    await fs.mkdir(betaRoot, { recursive: true });

    const alphaDb = path.join(tmp, "alpha2.db");
    const betaDb = path.join(tmp, "beta2.db");
    getDb(alphaDb).close();
    getDb(betaDb).close();

    const relPath = "cocalc/.git/objects/pack/tmp_pack_remote_beta";
    insertRecentSend(betaDb, "alpha->beta", relPath, {
      kind: "file",
      opTs: Date.now() - 5000,
      mtime: Date.now() - 5000,
      size: 456,
    });

    const fetchBeta = jest.fn(
      async (_paths: string[], _opts: { ignore: boolean }) => {
        return [] as SignatureEntry[];
      },
    );

    const micro = makeMicroSync({
      alphaRoot,
      betaRoot,
      alphaDbPath: alphaDb,
      betaDbPath: betaDb,
      betaHost: "remote-beta",
      prefer: "alpha",
      dryRun: false,
      log: () => {},
      logger: createTestLogger(),
      fetchRemoteBetaSignatures: fetchBeta,
    });

    await micro([], [relPath]);

    expect(fetchBeta).toHaveBeenCalledTimes(2);
    expect(fetchBeta.mock.calls[0]?.[0]).toEqual([relPath]);
    expect(fetchBeta.mock.calls[0]?.[1]).toEqual({ ignore: true });
    expect(fetchBeta.mock.calls[1]?.[0]).toEqual([relPath]);
    expect(fetchBeta.mock.calls[1]?.[1]).toEqual({ ignore: false });

    expect(mockedRsync.run).toHaveBeenCalled();

    const alphaRecent = getRecentSendSignatures(alphaDb, "beta->alpha", [
      relPath,
    ]);
    expect(alphaRecent.get(relPath)).not.toBeUndefined();
  });
});
``;
