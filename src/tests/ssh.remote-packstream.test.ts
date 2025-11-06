import { join } from "node:path";
import os from "node:os";
import fsp from "node:fs/promises";

import { countSchedulerCycles, waitFor } from "./util";
import { getDb } from "../db";
import {
  describeIfSsh,
  startSchedulerRemote,
  stopScheduler,
  writeLongPack,
  hashFile,
} from "./ssh.remote-test-util";

describeIfSsh("SSH remote sync – sustained pack stream", () => {
  let tmp = "";
  let alphaRoot = "";
  let betaRootRemote = "";
  let alphaDb = "";
  let betaDb = "";
  let baseDb = "";

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-pack-"));
    alphaRoot = join(tmp, "alpha");
    betaRootRemote = join(tmp, "beta");
    alphaDb = join(tmp, "alpha.db");
    betaDb = join(tmp, "beta.db");
    baseDb = join(tmp, "base.db");
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
  });

  beforeEach(async () => {
    await fsp.rm(alphaRoot, { recursive: true, force: true });
    await fsp.rm(betaRootRemote, { recursive: true, force: true });
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
    await Promise.all(
      [alphaDb, betaDb, baseDb].map((p) =>
        fsp.rm(p, { recursive: true, force: true }).catch(() => {}),
      ),
    );
  });

  afterAll(async () => {
    if (tmp) {
      await fsp.rm(tmp, { recursive: true, force: true });
    }
  });

  it(
    "sustained writes on alpha mirror to remote beta without bounce",
    async () => {
      const child = startSchedulerRemote({
        alphaRoot,
        betaRootRemote,
        alphaDb,
        betaDb,
        baseDb,
        prefer: "alpha",
      });

      const relPath = "packs/stream.pack";
      const alphaFile = join(alphaRoot, relPath);
      const betaFile = join(betaRootRemote, relPath);

      try {
        await waitFor(
          () => countSchedulerCycles(baseDb),
          (n) => n >= 1,
          20_000,
          20,
        );

        await writeLongPack(alphaFile, {
          iterations: 96,
          chunkSize: 256 * 1024,
          delayMs: 15,
        });

        await waitFor(
          async () => {
            try {
              const [statAlpha, statBeta] = await Promise.all([
                fsp.stat(alphaFile),
                fsp.stat(betaFile),
              ]);
              return statAlpha.size > 0 && statBeta.size === statAlpha.size;
            } catch {
              return false;
            }
          },
          (ok) => ok,
          30_000,
          50,
        );

        await waitFor(
          () => countSchedulerCycles(baseDb),
          (n) => n >= 3,
          20_000,
          20,
        );

        const [alphaHash, betaHash] = await Promise.all([
          hashFile(alphaFile),
          hashFile(betaFile),
        ]);
        expect(betaHash).toBe(alphaHash);

        const db = getDb(alphaDb);
        try {
          const row = db
            .prepare(
              `SELECT signature FROM recent_send WHERE direction='beta->alpha' AND path = ?`,
            )
            .get(relPath);
          expect(row).toBeUndefined();
        } finally {
          db.close();
        }
      } finally {
        await stopScheduler(child);
      }
    },
    25_000,
  );
});
