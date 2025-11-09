#!/usr/bin/env node
// src/scan.ts
import { Worker } from "node:worker_threads";
import os from "node:os";
import * as walk from "@nodelib/fs.walk";
import {
  readlink,
  stat as statAsync,
  lstat as lstatAsync,
} from "node:fs/promises";
import { getDb } from "./db.js";
import { Command, Option } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import path from "node:path";
import {
  collectIgnoreOption,
  createIgnorer,
  normalizeIgnorePatterns,
  autoIgnoreForRoot,
} from "./ignore.js";
import { toRel } from "./path-rel.js";
import { isRecent } from "./hotwatch.js";
import { CLI_NAME } from "./constants.js";
import {
  normalizeHashAlg,
  modeHash,
  stringDigest,
  listSupportedHashes,
  defaultHashAlg,
} from "./hash.js";
import { ConsoleLogger, type LogLevel, type Logger } from "./logger.js";
import { getReflectSyncHome } from "./session-db.js";
import { ensureTempDir } from "./rsync.js";
import {
  collectListOption,
  dedupeRestrictedList,
  dirnameRel,
} from "./restrict.js";

declare global {
  // Set during bundle by Rollup banner.
  // Contains the *bundled* worker source as a single ESM string.
  var __REFLECT_HASH_WORKER__: string | undefined;
}

function makeHashWorker(alg: string): Worker {
  let injected = globalThis.__REFLECT_HASH_WORKER__;

  // bundle mode:
  if (process.env.REFLECT_BUNDLED === "1" && injected) {
    // Defensive: if an older build accidentally left a data: URL, strip & decode it.
    if (injected.startsWith("data:text/")) {
      const i = injected.indexOf(",");
      injected = decodeURIComponent(injected.slice(i + 1));
    }
    // SEA/bundled path — run CJS source directly
    return new Worker(injected, {
      eval: true, // IMPORTANT: give raw JS here, not a data: URL
      workerData: { alg },
    });
  }

  // NOT bundle mode:
  return new Worker(new URL("./hash-worker.js", import.meta.url), {
    workerData: { alg },
  });
}

export function configureScanCommand(
  command: Command,
  { standalone = false }: { standalone?: boolean } = {},
): Command {
  if (standalone) {
    command.name(`${CLI_NAME}-scan`);
  }
  return command
    .description("Run a local scan writing to sqlite database")
    .requiredOption("--db <file>", "path to sqlite database")
    .requiredOption("--root <path>", "directory to scan")
    .option("--emit-delta", "emit NDJSON deltas to stdout for ingest", false)
    .option(
      "--emit-since-ts <milliseconds>",
      "when used with --emit-delta, first replay all rows (files/dirs/links) with op_ts >= this timestamp",
    )
    .addOption(
      new Option("--hash <algorithm>", "content hash algorithm")
        .choices(listSupportedHashes())
        .default(defaultHashAlg()),
    )
    .option("--vacuum", "vacuum the database after doing the scan", false)
    .option(
      "--prune-ms <milliseconds>",
      "prune deleted entries at least this old *before* doing the scan",
    )
    .option(
      "--restricted-path <path>",
      "restrict scan to a relative path (repeat or comma-separated)",
      collectListOption,
      [] as string[],
    )
    .option(
      "--restricted-dir <path>",
      "restrict scan to a directory tree (repeat or comma-separated)",
      collectListOption,
      [] as string[],
    )
    .option("--numeric-ids", "include uid and gid in file hashes", false)
    .option(
      "-i, --ignore <pattern>",
      "gitignore-style ignore rule (repeat or comma-separated)",
      collectIgnoreOption,
      [] as string[],
    );
}

function buildProgram(): Command {
  return configureScanCommand(new Command(), { standalone: true });
}

type ScanOptions = {
  db: string;
  emitDelta: boolean;
  emitRecentMs?: string;
  hash: string;
  root: string;
  vacuum?: boolean;
  pruneMs?: string;
  numericIds?: boolean;
  logger?: Logger;
  logLevel?: LogLevel;
  ignoreRules?: string[];
  ignore?: string[];
  restrictedPaths?: string[];
  restrictedDirs?: string[];
  restrictedPath?: string[] | string;
  restrictedDir?: string[] | string;
};

export async function runScan(opts: ScanOptions): Promise<void> {
  const {
    root,
    db: DB_PATH,
    emitDelta,
    emitRecentMs,
    hash,
    vacuum,
    pruneMs,
    numericIds,
    logger: providedLogger,
    logLevel = "info",
    ignoreRules: ignoreRulesOpt = [],
    ignore: ignoreCliOpt = [],
    restrictedPaths: restrictedPathsOpt = [],
    restrictedDirs: restrictedDirsOpt = [],
    restrictedPath: restrictedPathOpt = [],
    restrictedDir: restrictedDirOpt = [],
  } = opts;
  const logger = providedLogger ?? new ConsoleLogger(logLevel);
  const ignoreRaw: string[] = [];
  if (Array.isArray(ignoreRulesOpt)) ignoreRaw.push(...ignoreRulesOpt);
  if (Array.isArray(ignoreCliOpt)) ignoreRaw.push(...ignoreCliOpt);
  const absRoot = path.resolve(root);
  await ensureTempDir(absRoot);
  let rootDevice: number | undefined;
  try {
    const rootStat = await statAsync(absRoot);
    rootDevice = rootStat.dev;
  } catch (err) {
    throw new Error(
      `failed to stat scan root '${absRoot}': ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  }
  const syncHome = getReflectSyncHome();
  ignoreRaw.push(...autoIgnoreForRoot(absRoot, syncHome));
  const ignoreRules = normalizeIgnorePatterns(ignoreRaw);

  const restrictedPathRaw = [
    ...(Array.isArray(restrictedPathsOpt) ? restrictedPathsOpt : []),
    ...(Array.isArray(restrictedPathOpt)
      ? restrictedPathOpt
      : restrictedPathOpt
        ? [restrictedPathOpt]
        : []),
  ];
  const restrictedDirRaw = [
    ...(Array.isArray(restrictedDirsOpt) ? restrictedDirsOpt : []),
    ...(Array.isArray(restrictedDirOpt)
      ? restrictedDirOpt
      : restrictedDirOpt
        ? [restrictedDirOpt]
        : []),
  ];

  let restrictedPathList = dedupeRestrictedList(restrictedPathRaw);
  let restrictedDirList = dedupeRestrictedList(restrictedDirRaw);
  if (
    restrictedPathList.includes("") ||
    restrictedDirList.includes("")
  ) {
    restrictedPathList = [];
    restrictedDirList = [];
  }
  const hasRestrictions =
    restrictedPathList.length > 0 || restrictedDirList.length > 0;
  const restrictedPathSet = new Set(restrictedPathList);
  const restrictedDirSet = new Set(restrictedDirList);
  const restrictedDirPrefixes = restrictedDirList
    .filter((dir) => dir.length > 0)
    .map((dir) => `${dir}/`);
  const dirTraversalAllow = new Set<string>([""]);
  const addDirAndAncestors = (rel: string) => {
    let current = rel;
    while (true) {
      if (!dirTraversalAllow.has(current)) {
        dirTraversalAllow.add(current);
      }
      const parent = dirnameRel(current);
      if (parent === current || current === "") {
        dirTraversalAllow.add("");
        break;
      }
      current = parent;
      if (current === "") {
        dirTraversalAllow.add("");
        break;
      }
    }
  };
  for (const dir of restrictedDirList) {
    addDirAndAncestors(dir);
  }
  for (const relPath of restrictedPathList) {
    addDirAndAncestors(dirnameRel(relPath));
  }
  const inRestrictedDir = (rel: string): boolean => {
    if (!restrictedDirList.length) return false;
    if (restrictedDirSet.has(rel)) return true;
    for (const prefix of restrictedDirPrefixes) {
      if (rel.startsWith(prefix)) return true;
    }
    return false;
  };
  const dirAllowsTraversal = (rel: string): boolean => {
    if (!hasRestrictions) return true;
    if (dirTraversalAllow.has(rel)) return true;
    return inRestrictedDir(rel);
  };
  const dirMatches = (rel: string): boolean => {
    if (!hasRestrictions) return true;
    if (restrictedDirSet.has(rel)) return true;
    if (restrictedPathSet.has(rel)) return true;
    return inRestrictedDir(rel);
  };
  const pathMatches = (rel: string): boolean => {
    if (!hasRestrictions) return true;
    if (restrictedPathSet.has(rel)) return true;
    return inRestrictedDir(rel);
  };

  // Rows written to DB always use rpaths now.
  type Row = {
    path: string; // rpath
    size: number;
    ctime: number;
    mtime: number;
    op_ts: number;
    hash: string | null;
    last_seen: number;
    hashed_ctime: number | null;
  };

  type HashMeta = {
    modeHash?: string;
    uid?: number;
    gid?: number;
  };

  function parseHashMeta(hashValue: string | null | undefined): HashMeta {
    if (!hashValue) return {};
    const parts = hashValue.split("|");
    if (parts.length < 2) return {};
    const meta: HashMeta = { modeHash: parts[1] || undefined };
    if (parts.length >= 3) {
      const [uidStr, gidStr] = parts[2]?.split(":") ?? [];
      const uid = Number(uidStr);
      const gid = Number(gidStr);
      if (Number.isFinite(uid)) meta.uid = uid;
      if (Number.isFinite(gid)) meta.gid = gid;
    }
    return meta;
  }

  const HASH_ALG = normalizeHashAlg(hash);
  logger.info(`scan: using hash=${HASH_ALG}`);
  logger.debug("running scan", { db: DB_PATH, root });

  if (emitDelta) {
    process.stdout.write(
      JSON.stringify({ kind: "time", remote_now_ms: Date.now() }) + "\n",
    );
  }

  const CPU_COUNT = Math.min(os.cpus().length, 8);
  const DB_BATCH_SIZE = 2000;
  const DISPATCH_BATCH = 256; // files per worker message
  const HASH_PROGRESS_INTERVAL_MS = Number(
    process.env.REFLECT_HASH_PROGRESS_MS ?? 3000,
  );

  // ----------------- SQLite setup -----------------
  const db = getDb(DB_PATH);

  const restrictedClause =
    hasRestrictions && !restrictedDirSet.has("")
      ? " AND path IN (SELECT rpath FROM restricted_paths)"
      : "";
  if (hasRestrictions && !restrictedDirSet.has("")) {
    db.exec(`
      DROP TABLE IF EXISTS restricted_paths;
      CREATE TEMP TABLE restricted_paths(
        rpath TEXT PRIMARY KEY
      ) WITHOUT ROWID;
    `);
    const insertRestricted = db.prepare(
      `INSERT OR IGNORE INTO restricted_paths(rpath) VALUES (?)`,
    );
    const insertRestrictedBatch = db.transaction((paths: string[]) => {
      for (const relPath of paths) {
        insertRestricted.run(relPath);
      }
    });
    insertRestrictedBatch([
      ...restrictedPathList,
      ...restrictedDirList,
    ]);
    if (restrictedDirList.length) {
      const tables = ["files", "dirs", "links"];
      const dirStmts = tables.map((tbl) =>
        db.prepare(
          `
            INSERT OR IGNORE INTO restricted_paths(rpath)
            SELECT path FROM ${tbl}
            WHERE path = @dir
               OR (path >= @prefix AND path < @upper)
          `,
        ),
      );
      const insertExisting = db.transaction((dirs: string[]) => {
        for (const dir of dirs) {
          const prefix = dir ? `${dir}/` : "";
          const upper = dir ? `${dir}/\uffff` : "\uffff";
          for (const stmt of dirStmts) {
            stmt.run({ dir, prefix, upper });
          }
        }
      });
      insertExisting(restrictedDirList);
    }
  }

  if (pruneMs) {
    const olderThanTs = Date.now() - Number(pruneMs);
    for (const table of ["files", "dirs", "links"]) {
      db.prepare(`DELETE FROM ${table} WHERE deleted=1 AND op_ts < ?`).run(
        olderThanTs,
      );
    }
  }

  const insTouch = db.prepare(
    `INSERT OR REPLACE INTO recent_touch(path, ts) VALUES (?, ?)`,
  );

  const touchTx = db.transaction((rows: [string, number][]) => {
    for (const [p, t] of rows) insTouch.run(p, t);
  });

  function flushTouchBatch(touchBatch: [string, number][]) {
    if (!touchBatch.length) return;
    touchTx(touchBatch);
    touchBatch.length = 0;
  }

  // for directory metadata (paths are rpaths)
  const upsertDir = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (@path, @ctime, @mtime, @op_ts, @hash, 0, @scan_id)
ON CONFLICT(path) DO UPDATE SET
  ctime     = excluded.ctime,
  mtime     = excluded.mtime,
  deleted   = 0,
  last_seen = excluded.last_seen,
  hash      = excluded.hash,
  -- Preserve current op_ts unless we are resurrecting a previously-deleted dir
  op_ts     = CASE
                WHEN dirs.deleted = 1 THEN excluded.op_ts
                ELSE dirs.op_ts
              END
`);

  type DirRow = {
    path: string; // rpath
    ctime: number;
    mtime: number;
    op_ts: number;
    scan_id: number;
    hash: string;
  };

  const applyDirBatch = db.transaction((rows: DirRow[]) => {
    for (const r of rows) upsertDir.run(r);
  });

  // Files meta (paths are rpaths)
  const upsertMeta = db.prepare(`
INSERT INTO files (path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (@path, @size, @ctime, @mtime, @op_ts, @hash, 0, @last_seen, @hashed_ctime)
ON CONFLICT(path) DO UPDATE SET
  size=excluded.size,
  ctime=excluded.ctime,
  mtime=excluded.mtime,
  op_ts=excluded.op_ts,
  last_seen=excluded.last_seen,
  deleted=0
-- NOTE: we intentionally DO NOT overwrite hash or hashed_ctime here.
`);

  const applyMetaBatch = db.transaction((rows: Row[]) => {
    for (const r of rows) upsertMeta.run(r);
  });

  // Hashes (paths are rpaths)
  const applyHashBatch = db.transaction(
    (rows: { path: string; hash: string; ctime: number }[]) => {
      const stmt = db.prepare(
        `UPDATE files
         SET hash = ?, hashed_ctime = ?, deleted = 0
         WHERE path = ?`,
      );
      for (const r of rows) stmt.run(r.hash, r.ctime, r.path);
    },
  );

  // Links (paths are rpaths)
  type LinkRow = {
    path: string; // rpath
    target: string;
    ctime: number;
    mtime: number;
    op_ts: number;
    hash: string;
    scan_id: number;
  };

  const upsertLink = db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (@path, @target, @ctime, @mtime, @op_ts, @hash, 0, @scan_id)
ON CONFLICT(path) DO UPDATE SET
  target=excluded.target,
  ctime=excluded.ctime,
  mtime=excluded.mtime,
  op_ts=excluded.op_ts,
  hash=excluded.hash,
  deleted=0,
  last_seen=excluded.last_seen
`);

  const applyLinksBatch = db.transaction((rows: LinkRow[]) => {
    for (const r of rows) upsertLink.run(r);
  });

  // ----------------- Worker pool ------------------
  // Worker accepts ABS paths for hashing; we convert the results to rpaths here.
  type Job = {
    path: string;
    size: number;
    ctime: number;
    mtime: number;
  }; // ABS path
  type Result =
    | { path: string; hash: string; ctime: number; mtime: number } // ABS path echoed back
    | { path: string; error: string };

  const workers = Array.from({ length: CPU_COUNT }, () =>
    makeHashWorker(HASH_ALG),
  );

  const freeWorkers: Worker[] = [...workers];
  const waiters: Array<() => void> = [];
  let dispatched = 0;
  let received = 0;
  let hashTotalFiles = 0;
  let hashCompletedFiles = 0;
  let hashTotalBytes = 0;
  let hashCompletedBytes = 0;
  let lastHashProgressEmit = 0;

  function nextWorker(): Promise<Worker> {
    return new Promise((resolve) => {
      const w = freeWorkers.pop();
      if (w) return resolve(w);
      waiters.push(() => resolve(freeWorkers.pop()!));
    });
  }

  function emitHashProgress(force = false) {
    if (!hashTotalFiles) return;
    const now = Date.now();
    if (
      !force &&
      HASH_PROGRESS_INTERVAL_MS > 0 &&
      now - lastHashProgressEmit < HASH_PROGRESS_INTERVAL_MS
    ) {
      return;
    }
    const percent =
      hashTotalBytes > 0
        ? Math.min(100, Math.round((hashCompletedBytes / hashTotalBytes) * 100))
        : Math.min(
            100,
            Math.round((hashCompletedFiles / hashTotalFiles) * 100),
          );
    logger.info("progress", {
      scope: "scan.hash",
      stage: "hash",
      totalFiles: hashTotalFiles,
      completedFiles: hashCompletedFiles,
      totalBytes: hashTotalBytes,
      completedBytes: hashCompletedBytes,
      percent,
    });
    lastHashProgressEmit = now;
  }

  // Buffer for hash results (rpaths) to batch DB writes
  const hashResults: { path: string; hash: string; ctime: number }[] = [];
  const touchBatch: [string, number][] = [];

  // Emit buffering (fewer stdout writes)
  const deltaBuf: string[] = [];
  const emitObj = (o: {
    kind?: "dir" | "link";
    path: string; // rpath
    op_ts: number;
    deleted: number;
    size?: number;
    ctime?: number;
    mtime?: number;
    hash?: string;
    target?: string; // for links
  }) => {
    if (!emitDelta) {
      throw Error("do not call emitObj if emitDelta isn't enabled");
    }
    deltaBuf.push(JSON.stringify(o));
    if (deltaBuf.length >= 1000) {
      flushDeltaBuf();
    }
  };

  function flushDeltaBuf() {
    if (!emitDelta || deltaBuf.length === 0) {
      return;
    }
    process.stdout.write(deltaBuf.join("\n") + "\n");
    deltaBuf.length = 0;
  }

  async function emitReplaySinceTs(dbSince: number) {
    if (!emitDelta) return;

    // Files (changed or deleted)
    const files = db
      .prepare(
        `
    SELECT path, size, ctime, mtime, op_ts, hash, deleted
    FROM files
    WHERE op_ts >= ?${restrictedClause}
    ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      size: number;
      ctime: number;
      mtime: number;
      op_ts: number;
      hash: string | null;
      deleted: number;
    }[];

    for (const r of files) {
      emitObj({
        path: r.path,
        size: r.size,
        ctime: r.ctime,
        mtime: r.mtime,
        op_ts: r.op_ts,
        hash: r.hash ?? undefined,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();

    // Dirs
    const dirs = db
      .prepare(
        `
    SELECT path, ctime, mtime, op_ts, hash, deleted
    FROM dirs
    WHERE op_ts >= ?${restrictedClause}
    ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      ctime: number;
      mtime: number;
      op_ts: number;
      hash: string;
      deleted: number;
    }[];

    for (const r of dirs) {
      emitObj({
        kind: "dir",
        path: r.path,
        ctime: r.ctime,
        mtime: r.mtime,
        op_ts: r.op_ts,
        hash: r.hash,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();

    // Links
    const links = db
      .prepare(
        `
    SELECT path, target, ctime, mtime, op_ts, hash, deleted
    FROM links
    WHERE op_ts >= ?${restrictedClause}
    ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      target: string;
      ctime: number;
      mtime: number;
      op_ts: number;
      hash: string;
      deleted: number;
    }[];

    for (const r of links) {
      emitObj({
        kind: "link",
        path: r.path,
        target: r.target,
        ctime: r.ctime,
        mtime: r.mtime,
        op_ts: r.op_ts,
        hash: r.hash,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();
  }

  // We keep meta keyed by ABS path (because worker replies with ABS),
  // then translate to rpath when emitting/applying results.
  const pendingMeta = new Map<
    string, // ABS
    { size: number; ctime: number; mtime: number }
  >();

  // Handle worker replies (batched)
  for (const w of workers) {
    w.on("message", async (msg: { done?: Result[] }) => {
      freeWorkers.push(w);
      waiters.shift()?.();

      const arr = msg.done || [];
      received += arr.length;

      for (const r of arr) {
        hashCompletedFiles += 1;
        const metaToday = pendingMeta.get(r.path);
        if (metaToday) {
          hashCompletedBytes += metaToday.size;
        }
        if ("error" in r) {
          pendingMeta.delete(r.path);
          emitHashProgress();
          continue;
        }

        const rpath = toRel(r.path, absRoot);
        hashResults.push({ path: rpath, hash: r.hash, ctime: r.ctime });
        if (await isRecent(r.path, undefined, r.mtime)) {
          touchBatch.push([rpath, Date.now()]);
        }
        if (metaToday) {
          if (emitDelta) {
            emitObj({
              path: rpath,
              size: metaToday.size,
              ctime: metaToday.ctime,
              mtime: metaToday.mtime,
              op_ts: metaToday.mtime,
              hash: r.hash,
              deleted: 0,
            });
          }
          pendingMeta.delete(r.path);
        }
        if (hashResults.length >= DB_BATCH_SIZE) {
          applyHashBatch(hashResults);
          hashResults.length = 0;
          flushTouchBatch(touchBatch);
          flushDeltaBuf();
        }
        emitHashProgress();
      }
    });
  }

  // --------------- Walk + incremental logic ---------------
  async function scan() {
    const t0 = Date.now();
    const scan_id = Date.now();

    // If requested, first replay a bounded window from the DB,
    // then proceed to the actual filesystem walk (which will emit new changes).
    if (emitDelta && emitRecentMs) {
      const ms = Number(emitRecentMs);
      if (Number.isFinite(ms) && ms > 0) {
        const since = Date.now() - ms;
        await emitReplaySinceTs(since);
      }
    }

    // Load per-root ignore matcher (gitignore semantics)
    const ig = createIgnorer(ignoreRules);

    // stream entries with stats so we avoid a second stat in main thread
    const stream = walk.walkStream(absRoot, {
      stats: true,
      followSymbolicLinks: false,
      concurrency: 128,
      // Do not descend into ignored or out-of-scope directories
      deepFilter: (e) => {
        if (!e.dirent.isDirectory()) return true;
        const r = toRel(e.path, absRoot);
        if (!dirAllowsTraversal(r)) return false;
        return !ig.ignoresDir(r);
      },
      // Do not emit ignored or out-of-scope entries
      entryFilter: (e) => {
        const st = (e as { stats?: import("fs").Stats }).stats;
        if (rootDevice !== undefined && st && st.dev !== rootDevice) {
          return false;
        }
        const r = toRel(e.path, absRoot);
        if (e.dirent.isDirectory()) {
          if (!dirAllowsTraversal(r) || !dirMatches(r)) return false;
          return !ig.ignoresDir(r);
        }
        if (!pathMatches(r)) return false;
        return !ig.ignoresFile(r);
      },
      errorFilter: () => true,
    });

    // Periodic flush so we don't hold large arrays in RAM too long
    const periodicFlush = setInterval(() => {
      if (hashResults.length) {
        applyHashBatch(hashResults);
        hashResults.length = 0;
        flushTouchBatch(touchBatch);
      }
      flushDeltaBuf();
    }, 500).unref();

    // Mini-buffers
    const metaBuf: Row[] = [];
    const dirMetaBuf: DirRow[] = [];
    const linksBuf: LinkRow[] = [];
    const hashJobs: Job[] = [];

    // Existing-meta lookup by rpath
    const getExisting = db.prepare(
      `SELECT size, ctime, mtime, hashed_ctime, hash FROM files WHERE path = ?`,
    );

    // Existing-dir lookup by rpath (to decide whether to emit NDJSON)
    const getExistingDir = db.prepare(
      `SELECT ctime, mtime, hash, deleted FROM dirs WHERE path = ?`,
    );

    // Existing-link lookup by rpath (to decide whether to emit NDJSON)
    const getExistingLink = db.prepare(
      `SELECT ctime, mtime, hash, target, deleted FROM links WHERE path = ?`,
    );

    for await (const entry of stream as AsyncIterable<{
      dirent;
      path: string; // ABS
      stats: import("fs").Stats;
    }>) {
      const abs = entry.path; // absolute on filesystem
      const rpath = toRel(abs, absRoot);
      const st =
        !numericIds && entry.stats ? entry.stats : await lstatAsync(abs);
      if (rootDevice !== undefined && st.dev !== rootDevice) {
        continue;
      }
      const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();

      if (entry.dirent.isDirectory()) {
        // directory ops don't bump mtime reliably, so we use op time.
        const op_ts = Date.now();
        let hash = modeHash(st.mode);
        if (numericIds) {
          // NOTE: it is only a good idea to use this when
          // the user is root for both alpha and beta
          hash += `|${st.uid}:${st.gid}`;
        }

        // Decide whether to emit NDJSON for this dir (delta-only)
        let dirChanged = true;
        if (emitDelta) {
          const prev = getExistingDir.get(rpath);
          dirChanged =
            !prev ||
            prev.deleted === 1 ||
            prev.mtime !== mtime ||
            prev.ctime !== ctime ||
            prev.hash !== hash;
        }

        // Always upsert to bump last_seen (so deletion detection works)
        dirMetaBuf.push({ path: rpath, ctime, mtime, hash, scan_id, op_ts });
        if (dirMetaBuf.length >= DB_BATCH_SIZE) {
          applyDirBatch(dirMetaBuf);
          dirMetaBuf.length = 0;
        }

        // Only emit if changed/new/resurrected
        if (emitDelta && dirChanged) {
          emitObj({
            kind: "dir",
            path: rpath,
            ctime,
            mtime,
            hash,
            op_ts,
            deleted: 0,
          });
        }

        continue;
      } else if (entry.dirent.isFile()) {
        const size = st.size;
        const op_ts = mtime;
        const modeHex = modeHash(st.mode);

        // Upsert *metadata only* (no hash/hashed_ctime change here)
        metaBuf.push({
          path: rpath,
          size,
          ctime,
          mtime,
          op_ts,
          hash: null,
          last_seen: scan_id,
          hashed_ctime: null,
        });

        if (metaBuf.length >= DB_BATCH_SIZE) {
          applyMetaBatch(metaBuf);
          metaBuf.length = 0;
        }

        // Decide if we need to hash: only when ctime changed (or brand new)
        const row = getExisting.get(rpath) as
          | {
              hashed_ctime: number | null;
              hash: string | null;
              ctime: number | null;
              mtime: number | null;
              size: number | null;
            }
          | undefined;
        let needsHash = !row || row.hashed_ctime !== ctime;
        if (!needsHash && row?.hash) {
          const meta = parseHashMeta(row.hash);
          if (meta.modeHash && meta.modeHash !== modeHex) {
            needsHash = true;
          } else if (
            numericIds &&
            meta.uid !== undefined &&
            meta.gid !== undefined &&
            (meta.uid !== st.uid || meta.gid !== st.gid)
          ) {
            needsHash = true;
          }
        }

        if (needsHash) {
          pendingMeta.set(abs, { size, ctime, mtime });
          hashJobs.push({ path: abs, size, ctime, mtime });
          hashTotalFiles += 1;
          hashTotalBytes += size;
        }
      } else if (entry.dirent.isSymbolicLink()) {
        let target = "";
        try {
          target = await readlink(abs);
        } catch {}
        const op_ts = mtime; // LWW uses op_ts consistently
        const hash = stringDigest(HASH_ALG, target);

        // Decide whether to emit NDJSON for this link (delta-only)
        let linkChanged = true;
        if (emitDelta) {
          const prev = getExistingLink.get(rpath);
          linkChanged =
            !prev ||
            prev.deleted === 1 ||
            prev.mtime !== mtime ||
            prev.ctime !== ctime ||
            prev.hash !== hash ||
            prev.target !== target;
        }

        // Always upsert to bump last_seen
        linksBuf.push({
          path: rpath,
          target,
          ctime,
          mtime,
          op_ts,
          hash,
          scan_id,
        });
        if (linksBuf.length >= DB_BATCH_SIZE) {
          applyLinksBatch(linksBuf);
          linksBuf.length = 0;
        }

        // Only emit if changed/new/resurrected
        if (emitDelta && linkChanged) {
          emitObj({
            kind: "link",
            path: rpath,
            ctime,
            mtime,
            op_ts,
            hash,
            target,
            deleted: 0,
          });
        }

        continue;
      }
    }

    // Flush remaining meta
    if (metaBuf.length) {
      applyMetaBatch(metaBuf);
      metaBuf.length = 0;
    }

    // Flush remaining directories
    if (dirMetaBuf.length) {
      applyDirBatch(dirMetaBuf);
      dirMetaBuf.length = 0;
    }

    // Flush remaining links
    if (linksBuf.length) {
      applyLinksBatch(linksBuf);
      linksBuf.length = 0;
    }

    clearInterval(periodicFlush);

    await processHashJobs(hashJobs);

    if (hashResults.length) {
      applyHashBatch(hashResults);
      hashResults.length = 0;
      flushTouchBatch(touchBatch);
    }
    flushDeltaBuf();

    // Compute deletions (anything not seen this pass and not already deleted)
    const toDelete = db
      .prepare(
        `SELECT path FROM files WHERE last_seen <> ? AND deleted = 0${restrictedClause}`,
      )
      .all(scan_id) as { path: string }[];

    const op_ts = Date.now();
    db.prepare(
      `UPDATE files
       SET deleted = 1, op_ts = ?
       WHERE last_seen <> ? AND deleted = 0${restrictedClause}`,
    ).run(op_ts, scan_id);

    // emit-delta: file deletions
    if (emitDelta && toDelete.length) {
      for (const r of toDelete) {
        emitObj({ path: r.path, deleted: 1, op_ts });
      }
      flushDeltaBuf();
    }

    // Mark deletions: dirs
    const toDeleteDirs = db
      .prepare(
        `SELECT path FROM dirs WHERE last_seen <> ? AND deleted = 0${restrictedClause}`,
      )
      .all(scan_id) as { path: string }[];

    const op_ts_dirs = Date.now();

    db.prepare(
      `UPDATE dirs SET deleted=1, op_ts=? WHERE last_seen <> ? AND deleted = 0${restrictedClause}`,
    ).run(op_ts_dirs, scan_id);

    // emit-delta: Emit dir deletions (use the snapshot we captured BEFORE the update)
    if (emitDelta && toDeleteDirs.length) {
      for (const r of toDeleteDirs) {
        emitObj({ kind: "dir", path: r.path, deleted: 1, op_ts: op_ts_dirs });
      }
      flushDeltaBuf();
    }

    // Mark deletions: links
    const toDeleteLinks = db
      .prepare(
        `SELECT path FROM links WHERE last_seen <> ? AND deleted = 0${restrictedClause}`,
      )
      .all(scan_id) as { path: string }[];

    const op_ts_links = Date.now();
    db.prepare(
      `UPDATE links SET deleted=1, op_ts=? WHERE last_seen <> ? AND deleted = 0${restrictedClause}`,
    ).run(op_ts_links, scan_id);

    if (emitDelta && toDeleteLinks.length) {
      for (const r of toDeleteLinks) {
        emitObj({ kind: "link", path: r.path, deleted: 1, op_ts: op_ts_links });
      }
      flushDeltaBuf();
    }

    await Promise.all(workers.map((w) => w.terminate()));

    if (vacuum) {
      db.exec("vacuum");
    }

    logger.info("scan complete", {
      hashedFiles: dispatched,
      hashedResults: received,
      durationMs: Date.now() - t0,
    });
  }

  await scan();

  async function processHashJobs(jobs: Job[]) {
    if (!jobs.length) return;

    hashCompletedFiles = 0;
    hashCompletedBytes = 0;
    lastHashProgressEmit = 0;
    emitHashProgress(true);

    for (let i = 0; i < jobs.length; i += DISPATCH_BATCH) {
      const batch = jobs.slice(i, i + DISPATCH_BATCH);
      const w = await nextWorker();
      w.postMessage({ jobs: batch, numericIds });
      dispatched += batch.length;
    }

    while (received < dispatched) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    emitHashProgress(true);
  }
}

// ---------- CLI entry (preserved) ----------
cliEntrypoint<ScanOptions>(import.meta.url, buildProgram, runScan, {
  label: "scan",
});
