/**
 * pnpm test ssh.remote-sync.test.ts
 *
 * Verifies: scheduler starts with beta as a remote side, and a file created
 * on the remote (beta) arrives locally on alpha via microSync before the next
 * full cycle.
 */

import { ChildProcess, spawn, spawnSync } from "node:child_process";
import fsp from "node:fs/promises";
import { createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import { join, normalize, resolve, dirname } from "node:path";
import os from "node:os";
import { countSchedulerCycles, dirExists, linkExists, waitFor } from "./util";
import { getDb } from "../db";
//import { wait } from "./util";

// Resolve scheduler entrypoint directly to avoid CLI multi-proc trees
const SCHED = resolve(__dirname, "../../dist/scheduler.js");

const SSH_ENABLED = (() => {
  try {
    const res = spawnSync("ssh", [
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=2",
      "localhost",
      "true",
    ]);
    return res.status === 0;
  } catch {
    return false;
  }
})();
const describeFn = SSH_ENABLED ? describe : describe.skip;

function startSchedulerRemote(opts: {
  alphaRoot: string; // local
  betaRootRemote: string; // path we treat as "remote" (on localhost)
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  prefer?: "alpha" | "beta";
}): ChildProcess {
  const args = [
    SCHED,
    "--alpha-root",
    opts.alphaRoot,
    "--beta-root",
    opts.betaRootRemote,
    "--alpha-db",
    opts.alphaDb,
    "--beta-db",
    opts.betaDb,
    "--base-db",
    opts.baseDb,
    "--prefer",
    opts.prefer ?? "alpha",
    "--disable-hot-watch",
    "--beta-host",
    "localhost", // mark beta as remote so scheduler uses ssh-based watch/scan
  ];
  // Make full cycles very FAST
  const env = {
    ...process.env,
    SCHED_MIN_MS: "100",
    SCHED_MAX_MS: "200",
    SCHED_MAX_BACKOFF_MS: "50",
    SCHED_JITTER_MS: "0",
    MICRO_DEBOUNCE_MS: "0",
    COOLDOWN_MS: "10",
    SHALLOW_DEPTH: "1",
    HOT_DEPTH: "1",
    MAX_HOT_WATCHERS: "32",
  };

  return spawn(process.execPath, args, {
    stdio: ["ignore", "ignore", "inherit"],
    env,
  });
}

async function stopScheduler(p: ChildProcess) {
  if (!p.pid) return;
  try {
    process.kill(p.pid, "SIGINT");
  } catch {}
  await Promise.race([
    new Promise<void>((resolve) => p.once("exit", () => resolve())),
    new Promise<void>((resolve) => setTimeout(resolve, 500)),
  ]);
  if (p.pid) {
    try {
      process.kill(p.pid, "SIGKILL");
    } catch {}
  }
}

describeFn("SSH remote sync", () => {
  let tmp: string = "";
  let alphaRoot: string, betaRootRemote: string;
  let alphaDb: string, betaDb: string, baseDb: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-ssh-watch-"));
    alphaRoot = join(tmp, "alpha-local");
    betaRootRemote = join(tmp, "beta-remote");
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

  const wait = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  async function writeLongPack(
    file: string,
    {
      iterations,
      chunkSize,
      delayMs,
    }: { iterations: number; chunkSize: number; delayMs: number },
  ) {
    await fsp.mkdir(normalize(dirname(file)), { recursive: true });
    const handle = await fsp.open(file, "w");
    try {
      for (let i = 0; i < iterations; i++) {
        const chunk = Buffer.alloc(chunkSize, i % 251);
        await handle.write(chunk);
        if (delayMs > 0) {
          await wait(delayMs);
        }
      }
    } finally {
      await handle.close();
    }
  }

  async function hashFile(file: string): Promise<string> {
    const hash = createHash("sha256");
    await new Promise<void>((resolve, reject) => {
      const rs = createReadStream(file);
      rs.on("data", (chunk) => hash.update(chunk));
      rs.on("error", reject);
      rs.on("end", resolve);
    });
    return hash.digest("hex");
  }

  it("sustained writes on alpha mirror to remote beta without bounce", async () => {
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

      // wait for an additional cycle to allow any pending transfers to settle
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
  }, 20_000);

  it("sqlite workload on alpha stays consistent on remote beta", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    const relPath = "databases/work.db";
    const alphaFile = join(alphaRoot, relPath);
    const betaFile = join(betaRootRemote, relPath);

    try {
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        20_000,
        20,
      );

      await fsp.mkdir(dirname(alphaFile), { recursive: true });
      const db = getDb(alphaFile);
      try {
        db.exec(`PRAGMA journal_mode = WAL;`);
        db.exec(
          `CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT);`,
        );
        const insert = db.prepare(
          `INSERT OR REPLACE INTO kv(key, value) VALUES (?, ?)`,
        );
        const tx = db.transaction(
          (rows: Array<{ key: string; value: string }>) => {
            for (const row of rows) insert.run(row.key, row.value);
          },
        );
        for (let batch = 0; batch < 40; batch++) {
          const rows = Array.from({ length: 50 }, (_, j) => ({
            key: `key-${batch}-${j}`,
            value: `value-${batch}-${j}`,
          }));
          tx(rows);
          await wait(5);
        }
        db.exec(`PRAGMA wal_checkpoint(TRUNCATE);`);
      } finally {
        db.close();
      }

      await waitFor(
        async () => {
          try {
            const [statAlpha, statBeta] = await Promise.all([
              fsp.stat(alphaFile),
              fsp.stat(betaFile),
            ]);
            return statAlpha.size === statBeta.size && statBeta.size > 0;
          } catch {
            return false;
          }
        },
        (ok) => ok,
        30_000,
        50,
      );

      const [alphaHash, betaHash] = await Promise.all([
        hashFile(alphaFile),
        hashFile(betaFile),
      ]);
      expect(betaHash).toBe(alphaHash);

      const betaDbHandle = getDb(betaFile);
      try {
        const rowCount = betaDbHandle
          .prepare(`SELECT COUNT(*) AS n FROM kv`)
          .get() as { n: number };
        expect(rowCount.n).toBeGreaterThanOrEqual(40 * 50);
      } finally {
        betaDbHandle.close();
      }

      const meta = getDb(alphaDb);
      try {
        const row = meta
          .prepare(
            `SELECT signature FROM recent_send WHERE direction='beta->alpha' AND path = ?`,
          )
          .get(relPath);
        expect(row).toBeUndefined();
      } finally {
        meta.close();
      }
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);

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

it("create directory that is target of symlink, sync, move directory, sync", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    try {
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        15_000,
        10,
      );
      await fsp.mkdir(join(alphaRoot, "x"));
      await fsp.symlink("x", join(alphaRoot, "x.link"));

      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 2,
        15_000,
        10,
      );

      await expect(linkExists(join(betaRootRemote, "x.link")));
      await expect(dirExists(join(betaRootRemote, "x")));

      // move the directory
      await fsp.rename(join(betaRootRemote, "x"), join(betaRootRemote, "x2"));

      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 3,
        15_000,
        10,
      );

      expect(await fsp.readdir(alphaRoot)).toEqual([
        ".reflect-rsync-tmp",
        "x.link",
        "x2",
      ]);
      expect(await fsp.readdir(betaRootRemote)).toEqual([
        ".reflect-rsync-tmp",
        "x.link",
        "x2",
      ]);
      await expect(linkExists(join(alphaRoot, "x.link")));
      await expect(linkExists(join(betaRootRemote, "x.link")));
    } finally {
      await stopScheduler(child);
    }
  }, 20_000);
});

async function listDatasetFiles(root: string): Promise<string[]> {
  try {
    const entries = await fsp.readdir(root, { withFileTypes: true });
    const out: string[] = [];
    for (const entry of entries) {
      const full = join(root, entry.name);
      if (entry.isDirectory()) {
        const nested = await listDatasetFiles(full);
        for (const child of nested) {
          out.push(join(entry.name, child));
        }
      } else if (entry.isFile()) {
        out.push(entry.name);
      }
    }
    return out.sort();
  } catch {
    return [];
  }
}

async function hashDirectory(root: string): Promise<string> {
  const hash = createHash("sha256");
  const entries = await fsp.readdir(root, { withFileTypes: true }).catch(
    () => [],
  );
  const sorted = entries.map((e) => e.name).sort();
  for (const name of sorted) {
    const entry = entries.find((e) => e.name === name)!;
    const full = join(root, entry.name);
    if (entry.isDirectory()) {
      const subHash = await hashDirectory(full);
      hash.update(entry.name);
      hash.update(subHash);
    } else if (entry.isFile()) {
      const data = await fsp.readFile(full);
      hash.update(entry.name);
      hash.update(data);
    }
  }
  return hash.digest("hex");
}
