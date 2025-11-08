import { join } from "node:path";
import os from "node:os";
import fsp from "node:fs/promises";
import { randomBytes } from "node:crypto";

import { countSchedulerCycles, waitFor } from "./util";
import { getDb } from "../db";
import {
  describeIfSsh,
  startSchedulerRemote,
  stopScheduler,
  listDatasetFiles,
  hashDirectory,
} from "./ssh.remote-test-util";

describeIfSsh("SSH remote sync – bulk image dataset", () => {
  let tmp = "";
  let alphaRoot = "";
  let betaRootRemote = "";
  let alphaDb = "";
  let betaDb = "";
  let baseDb = "";

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-images-"));
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

  it("bulk image dataset sync maintains parity without echo", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    const datasetDir = "dataset";
    const alphaDataset = join(alphaRoot, datasetDir);
    const betaDataset = join(betaRootRemote, datasetDir);

    try {
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        20_000,
        20,
      );

      await fsp.mkdir(alphaDataset, { recursive: true });
      const groups = 8;
      const imagesPerGroup = 150;
      const size = 2 * 1024;
      for (let g = 0; g < groups; g++) {
        const groupDir = join(alphaDataset, `group-${g}`);
        await fsp.mkdir(groupDir, { recursive: true });
        for (let i = 0; i < imagesPerGroup; i++) {
          const file = join(groupDir, `image-${i}.bin`);
          await fsp.writeFile(file, randomBytes(size));
        }
      }

      await waitFor(
        async () => {
          try {
            const [filesAlpha, filesBeta] = await Promise.all([
              listDatasetFiles(alphaDataset),
              listDatasetFiles(betaDataset),
            ]);
            return (
              filesAlpha.length === groups * imagesPerGroup &&
              filesAlpha.length === filesBeta.length
            );
          } catch {
            return false;
          }
        },
        (ok) => ok,
        60_000,
        100,
      );

      const [hashAlpha, hashBeta] = await Promise.all([
        hashDirectory(alphaDataset),
        hashDirectory(betaDataset),
      ]);
      expect(hashBeta).toBe(hashAlpha);

      const meta = getDb(alphaDb);
      try {
        const row = meta
          .prepare(
            `SELECT COUNT(*) AS n FROM recent_send WHERE direction='beta->alpha' AND path LIKE ?`,
          )
          .get(`${datasetDir}%`) as { n: number };
        expect(row.n).toBe(0);
      } finally {
        meta.close();
      }
    } finally {
      await stopScheduler(child);
    }
  }, 60_000);
});
