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
import { createLogicalClock } from "./logical-clock.js";
import type { LogicalClock } from "./logical-clock.js";

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
      "--emit-since-age <milliseconds>",
      "with --emit-delta, first replay all nodes that are at most this old (op_ts >= now - age)",
    )
    .option(
      "--emit-all",
      "with --emit-delta, replay every node before live output",
      false,
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
    )
    .option(
      "--clock-base <db>",
      "path to an additional db used to seed the logical clock",
      collectListOption,
      [] as string[],
    )
    .option(
      "--scan-tick <number>",
      "(internal) override logical timestamp used for this scan",
    );
}

function buildProgram(): Command {
  return configureScanCommand(new Command(), { standalone: true });
}

type ScanOptions = {
  db: string;
  emitDelta: boolean;
  emitSinceAge?: string;
  emitAll?: boolean;
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
  logicalClock?: LogicalClock;
  clockBase?: string[];
  scanTick?: number;
};

export async function runScan(opts: ScanOptions): Promise<void> {
  const {
    root,
    db: DB_PATH,
    emitDelta,
    emitSinceAge,
    emitAll,
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
    logicalClock,
    scanTick: scanTickOverride,
  } = opts;
  const logger = providedLogger ?? new ConsoleLogger(logLevel);
  const clock = logicalClock;
  let standaloneClock = Date.now();
  const newClockValue = () => {
    if (clock) return clock.next();
    const now = Date.now();
    const next = Math.max(now, standaloneClock + 1);
    standaloneClock = next;
    return next;
  };
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
  if (restrictedPathList.includes("") || restrictedDirList.includes("")) {
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
    hash_pending: number;
    change_start: number | null;
    change_end: number | null;
    pending_start: number | null;
    confirmed_at: number | null;
    copy_pending: number;
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
  const metaSelectStmt = db.prepare(
    `SELECT value FROM meta WHERE key = ?`,
  );
  const metaUpsertStmt = db.prepare(
    `INSERT INTO meta(key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
  );
  const readMetaNumber = (key: string): number | null => {
    try {
      const row = metaSelectStmt.get(key) as { value: string | number | null } | undefined;
      if (!row || row.value == null) return null;
      const num = Number(row.value);
      return Number.isFinite(num) ? num : null;
    } catch {
      return null;
    }
  };
  const writeMetaNumber = (key: string, value: number) => {
    metaUpsertStmt.run({ key, value: String(value) });
  };
  const META_LAST_FULL_SCAN_START = "last_full_scan_start";
  const META_LAST_FULL_SCAN_END = "last_full_scan_end";

  const scanWindowStart = newClockValue();
  const scanTick = scanTickOverride ?? newClockValue();
  const previousFullScanStart = readMetaNumber(META_LAST_FULL_SCAN_START);
  const fallbackLowerBoundBase =
    previousFullScanStart ?? scanWindowStart;
  const lowerBoundFor = (confirmed?: number | null) =>
    confirmed != null ? confirmed : fallbackLowerBoundBase;
  if (!clock) {
    try {
      const row = db
        .prepare(`SELECT MAX(updated) AS max_updated FROM nodes`)
        .get() as { max_updated?: number };
      if (row && Number.isFinite(row.max_updated)) {
        standaloneClock = Math.max(standaloneClock, Number(row.max_updated));
      }
    } catch {
      // ignore
    }
  }
  type NodeKind = "f" | "d" | "l";
  type NodeWriteParams = {
    path: string;
    kind: NodeKind;
    hash: string;
    mtime: number;
    ctime?: number;
    hashed_ctime?: number | null;
    size: number;
    deleted: 0 | 1;
    last_seen?: number | null;
    link_target?: string | null;
    last_error?: string | null;
    updated?: number;
    hash_pending?: number;
    copy_pending?: number;
    change_start?: number | null;
    change_end?: number | null;
    confirmed_at?: number | null;
  };
  const nodeUpsertStmt = db.prepare(`
        INSERT INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, hash_pending, copy_pending, change_start, change_end, confirmed_at, last_seen, link_target, last_error)
        VALUES (@path, @kind, @hash, @mtime, @ctime, @hashed_ctime, @updated, @size, @deleted, @hash_pending, @copy_pending, @change_start, @change_end, @confirmed_at, @last_seen, @link_target, @last_error)
        ON CONFLICT(path) DO UPDATE SET
          kind=excluded.kind,
          hash=excluded.hash,
          mtime=excluded.mtime,
          ctime=excluded.ctime,
          hashed_ctime=excluded.hashed_ctime,
          updated=excluded.updated,
          size=excluded.size,
          deleted=excluded.deleted,
          hash_pending=excluded.hash_pending,
          copy_pending=excluded.copy_pending,
          change_start=excluded.change_start,
          change_end=excluded.change_end,
          confirmed_at=excluded.confirmed_at,
          last_seen=excluded.last_seen,
          link_target=excluded.link_target,
          last_error=excluded.last_error
      `);
  const writeNode = (params: NodeWriteParams) => {
    const updated = params.updated ?? newClockValue();
    const ctime = params.ctime ?? params.mtime;
    let changeStart =
      params.change_start === undefined ? null : params.change_start;
    let changeEnd =
      params.change_end === undefined ? null : params.change_end;
    if (
      changeStart != null &&
      changeEnd != null &&
      changeEnd < changeStart
    ) {
      changeStart = changeEnd;
    }
    nodeUpsertStmt.run({
      path: params.path,
      kind: params.kind,
      hash: params.hash,
      mtime: params.mtime,
      ctime,
      hashed_ctime: params.hashed_ctime ?? null,
      updated,
      size: params.size,
      deleted: params.deleted,
      hash_pending: params.hash_pending ?? 0,
      copy_pending: params.copy_pending ?? 0,
      change_start: changeStart,
      change_end: changeEnd,
      confirmed_at:
        params.confirmed_at === undefined ? null : params.confirmed_at,
      last_seen: params.last_seen ?? null,
      link_target: params.link_target ?? null,
      last_error: params.last_error === undefined ? null : params.last_error,
    });
  };

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
    insertRestrictedBatch([...restrictedPathList, ...restrictedDirList]);
    if (restrictedDirList.length) {
      const insertExisting = db.prepare(
        `
        INSERT OR IGNORE INTO restricted_paths(rpath)
        SELECT path FROM nodes
        WHERE path = @dir
           OR (path >= @prefix AND path < @upper)
      `,
      );
      const insertExistingTxn = db.transaction((dirs: string[]) => {
        for (const dir of dirs) {
          const prefix = dir ? `${dir}/` : "";
          const upper = dir ? `${dir}/\uffff` : "\uffff";
          insertExisting.run({ dir, prefix, upper });
        }
      });
      insertExistingTxn(restrictedDirList);
    }
  }

  if (pruneMs) {
    const olderThanTs = Date.now() - Number(pruneMs);
    db.prepare(`DELETE FROM nodes WHERE deleted = 1 AND updated < ?`).run(
      olderThanTs,
    );
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

  type DirRow = {
    path: string; // rpath
    ctime: number;
    mtime: number;
    op_ts: number;
    scan_id: number;
    hash: string;
    change_start: number | null;
    change_end: number | null;
    confirmed_at: number | null;
    copy_pending: number;
  };

  const applyDirBatch = db.transaction((rows: DirRow[]) => {
    for (const r of rows) {
      writeNode({
        path: r.path,
        kind: "d",
        hash: r.hash ?? "",
        mtime: r.mtime,
        ctime: r.ctime,
        size: 0,
        deleted: 0,
        last_seen: r.scan_id,
        updated: r.op_ts,
        hash_pending: 0,
        copy_pending: r.copy_pending ?? 0,
        change_start: r.change_start ?? r.op_ts,
        change_end: r.change_end ?? r.op_ts,
        confirmed_at: r.confirmed_at ?? r.op_ts,
        link_target: null,
        last_error: null,
      });
    }
  });

  // Files meta (paths are rpaths)
  const applyMetaBatch = db.transaction((rows: Row[]) => {
    for (const r of rows) {
      writeNode({
        path: r.path,
        kind: "f",
        hash: r.hash ?? "",
        mtime: r.mtime,
        ctime: r.ctime,
        hashed_ctime: r.hashed_ctime,
        size: r.size ?? 0,
        deleted: 0,
        last_seen: r.last_seen,
        updated: r.op_ts,
        hash_pending: r.hash_pending ?? 0,
        change_start: r.change_start ?? r.op_ts,
        change_end: r.change_end ?? (r.hash_pending ? null : r.op_ts),
        confirmed_at: r.confirmed_at ?? (r.hash_pending ? null : r.op_ts),
        copy_pending: r.copy_pending ?? 0,
        link_target: null,
        last_error: null,
      });
    }
  });

  // Hashes (paths are rpaths)
  const applyHashBatch = db.transaction(
    (rows: { path: string; hash: string; ctime: number }[]) => {
      for (const r of rows) {
        const meta = fileNodeMeta.get(r.path);
        const prevHash = meta?.prevHash ?? null;
        const prevUpdated = meta?.prevUpdated ?? null;
        const hashChanged = !prevHash || prevHash !== r.hash;
        const updatedValue = hashChanged
          ? (meta?.updated ?? scanTick)
          : (prevUpdated ?? meta?.updated ?? scanTick);
        const pendingStart = meta?.pending_start ?? null;
        let changeStart = meta?.change_start ?? null;
        let changeEnd = meta?.change_end ?? null;
        let confirmedAt = meta?.confirmed_at ?? null;
        if (hashChanged) {
          const fallbackForHash =
            pendingStart ?? changeStart ?? fallbackLowerBoundBase;
          changeStart = fallbackForHash;
          changeEnd = scanTick;
          confirmedAt = scanTick;
        } else {
          confirmedAt = scanTick;
        }
        writeNode({
          path: r.path,
          kind: "f",
          hash: r.hash,
          mtime: meta?.mtime ?? r.ctime ?? Date.now(),
          ctime: meta?.ctime ?? r.ctime ?? Date.now(),
          hashed_ctime: r.ctime,
          size: meta?.size ?? 0,
          deleted: 0,
          last_seen: meta?.last_seen ?? null,
          updated: updatedValue,
          change_start: changeStart,
          change_end: changeEnd,
          hash_pending: 0,
          confirmed_at: confirmedAt ?? scanTick,
          copy_pending: 0,
          link_target: null,
          last_error: null,
        });
        fileNodeMeta.delete(r.path);
      }
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
    change_start: number | null;
    change_end: number | null;
    confirmed_at: number | null;
    copy_pending: number;
  };

  const applyLinksBatch = db.transaction((rows: LinkRow[]) => {
    for (const r of rows) {
      writeNode({
        path: r.path,
        kind: "l",
        hash: r.hash ?? "",
        mtime: r.mtime,
        ctime: r.ctime,
        size: Buffer.byteLength(r.target ?? "", "utf8"),
        deleted: 0,
        last_seen: r.scan_id,
        link_target: r.target ?? "",
        updated: r.op_ts,
        change_start: r.change_start ?? r.op_ts,
        change_end: r.change_end ?? r.op_ts,
        confirmed_at: r.confirmed_at ?? r.op_ts,
        copy_pending: r.copy_pending ?? 0,
        hash_pending: 0,
        last_error: null,
      });
    }
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
    | {
        path: string;
        hash: string;
        ctime: number;
        mtime: number;
        size: number;
      } // ABS path echoed back
    | { path: string; error: string }
    | { path: string; skipped: true };

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

    const nodes = db
      .prepare(
        `
    SELECT path, kind, size, ctime, mtime, updated AS op_ts, hash, link_target, deleted
      FROM nodes
     WHERE updated >= ?${restrictedClause}
     ORDER BY op_ts ASC, path ASC
  `,
      )
      .all(dbSince) as {
      path: string;
      kind: string;
      size: number | null;
      ctime: number | null;
      mtime: number | null;
      op_ts: number;
      hash: string | null;
      link_target: string | null;
      deleted: number;
    }[];

    for (const r of nodes) {
      const kind = r.kind;
      emitObj({
        kind: kind === "d" ? "dir" : kind === "l" ? "link" : undefined,
        path: r.path,
        size: kind === "f" ? (r.size ?? 0) : undefined,
        ctime: r.ctime ?? undefined,
        mtime: r.mtime ?? undefined,
        op_ts: r.op_ts,
        hash: r.hash ?? undefined,
        target: kind === "l" ? (r.link_target ?? undefined) : undefined,
        deleted: r.deleted,
      });
    }
    flushDeltaBuf();
  }

  // We keep meta keyed by ABS path (because worker replies with ABS),
  // then translate to rpath when emitting/applying results.
  const pendingMeta = new Map<
    string, // ABS
    {
      size: number;
      ctime: number;
      mtime: number;
      updated: number;
      change_start: number | null;
      change_end: number | null;
      pending_start: number | null;
      confirmed_at: number | null;
      copy_pending: number;
    }
  >();
  const fileNodeMeta = new Map<
    string,
    {
      size: number;
      mtime: number;
      ctime: number;
      last_seen: number;
      updated: number;
      prevUpdated: number | null;
      prevHash: string | null;
      hash_pending: number;
      change_start: number | null;
      change_end: number | null;
      pending_start: number | null;
      confirmed_at: number | null;
      copy_pending: number;
    }
  >();

  const timestampsClose = (a?: number, b?: number) => {
    if (a == null || b == null) return false;
    return Math.abs(a - b) < 0.5;
  };
  let skippedUnstableHashes = 0;

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
        const rpath = toRel(r.path, absRoot);
        if ("error" in r) {
          pendingMeta.delete(r.path);
          fileNodeMeta.delete(rpath);
          emitHashProgress();
          continue;
        }
        if ("skipped" in r) {
          skippedUnstableHashes += 1;
          pendingMeta.delete(r.path);
          fileNodeMeta.delete(rpath);
          emitHashProgress();
          continue;
        }
        if (
          !metaToday ||
          metaToday.size !== r.size ||
          !timestampsClose(metaToday.mtime, r.mtime) ||
          !timestampsClose(metaToday.ctime, r.ctime)
        ) {
          skippedUnstableHashes += 1;
          pendingMeta.delete(r.path);
          fileNodeMeta.delete(rpath);
          emitHashProgress();
          continue;
        }
        hashResults.push({ path: rpath, hash: r.hash, ctime: r.ctime });
        if (await isRecent(r.path, undefined, r.mtime)) {
          touchBatch.push([rpath, Date.now()]);
        }
        if (emitDelta) {
          emitObj({
            path: rpath,
            size: metaToday.size,
            ctime: metaToday.ctime,
            mtime: metaToday.mtime,
            op_ts: metaToday.updated,
            hash: r.hash,
            deleted: 0,
          });
        }
        pendingMeta.delete(r.path);
        const meta = fileNodeMeta.get(rpath);
        if (meta) {
          meta.change_start =
            meta.change_start ?? meta.change_end ?? meta.updated;
          meta.change_end = scanTick;
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
    skippedUnstableHashes = 0;

    // If requested, first replay a bounded window from the DB,
    // then proceed to the actual filesystem walk (which will emit new changes).
    if (emitDelta) {
      if (emitAll) {
        await emitReplaySinceTs(0);
      } else if (emitSinceAge) {
        const age = Number(emitSinceAge);
        if (Number.isFinite(age) && age > 0) {
          const since = Date.now() - age;
          await emitReplaySinceTs(since);
        }
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
      `SELECT size, ctime, mtime, hashed_ctime, hash, updated, deleted, hash_pending, copy_pending, change_start, change_end, confirmed_at
         FROM nodes
        WHERE path = ? AND kind = 'f'`,
    );

    const getExistingDir = db.prepare(
      `SELECT ctime, mtime, hash, deleted, updated, copy_pending, change_start, change_end, confirmed_at
         FROM nodes
        WHERE path = ? AND kind = 'd'`,
    );

    const getExistingLink = db.prepare(
      `SELECT ctime, mtime, hash, link_target AS target, deleted, updated, copy_pending, change_start, change_end, confirmed_at
         FROM nodes
        WHERE path = ? AND kind = 'l'`,
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
        const prev = getExistingDir.get(rpath) as
          | {
              hash: string | null;
              ctime: number | null;
              mtime: number | null;
              deleted: number | null;
              updated: number | null;
              change_start: number | null;
              change_end: number | null;
              confirmed_at: number | null;
            }
          | undefined;
        let hash = modeHash(st.mode);
        if (numericIds) {
          hash += `|${st.uid}:${st.gid}`;
        }

        const dirChanged = !prev || prev.deleted === 1 || prev.hash !== hash;

        const op_ts = dirChanged
          ? scanTick
          : (prev?.updated ?? prev?.mtime ?? mtime);

        const dirBaseline = lowerBoundFor(prev?.confirmed_at ?? null);
        const dirChangeStart = dirChanged
          ? dirBaseline
          : prev?.change_start ?? (prev?.change_end ?? dirBaseline);
        const dirChangeEnd = dirChanged ? op_ts : (prev?.change_end ?? op_ts);
        const dirConfirmedAt = dirChanged
          ? op_ts
          : prev?.confirmed_at ?? op_ts;

        dirMetaBuf.push({
          path: rpath,
          ctime,
          mtime,
          hash,
          scan_id,
          op_ts,
          change_start: dirChangeStart,
          change_end: dirChangeEnd,
          confirmed_at: dirConfirmedAt,
          copy_pending: 0,
        });
        if (dirMetaBuf.length >= DB_BATCH_SIZE) {
          applyDirBatch(dirMetaBuf);
          dirMetaBuf.length = 0;
        }

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
        const modeHex = modeHash(st.mode);

        const row = getExisting.get(rpath) as
          | {
              hashed_ctime: number | null;
              hash: string | null;
              ctime: number | null;
              mtime: number | null;
              size: number | null;
              updated: number | null;
              deleted: 0 | 1;
              hash_pending: number | null;
              copy_pending: number | null;
              change_start: number | null;
              change_end: number | null;
              confirmed_at: number | null;
            }
          | undefined;

        const resurrecting = row?.deleted === 1;
        let needsHash =
          resurrecting ||
          !row ||
          row.hashed_ctime !== ctime ||
          row.size !== size ||
          row.mtime !== mtime;
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

        const op_ts =
          needsHash || !row ? scanTick : (row?.updated ?? row?.mtime ?? mtime);

        const rowConfirmed = row?.confirmed_at ?? null;
        const baseline = lowerBoundFor(rowConfirmed);
        let changeStart = row?.change_start ?? null;
        let changeEnd = row?.change_end ?? null;
        let pendingStart: number | null = null;
        if (!row) {
          changeStart = baseline;
          changeEnd = needsHash ? null : op_ts;
        } else if (needsHash) {
          pendingStart = baseline;
          changeStart = baseline;
          changeEnd = null;
        } else {
          changeStart = row.change_start ?? baseline;
          changeEnd = row.change_end ?? (row.change_start ?? baseline);
        }
        const hashPendingFlag = needsHash ? 1 : (row?.hash_pending ?? 0);

        // Upsert *metadata only* (no hash/hashed_ctime change here)
        metaBuf.push({
          path: rpath,
          size,
          ctime,
          mtime,
          op_ts,
          hash: row?.hash ?? null,
          last_seen: scan_id,
          hashed_ctime: row?.hashed_ctime ?? null,
          hash_pending: hashPendingFlag,
          change_start: changeStart,
          change_end: changeEnd,
          pending_start: pendingStart,
          confirmed_at: rowConfirmed ?? null,
          copy_pending: row?.copy_pending ?? 0,
        });

        if (metaBuf.length >= DB_BATCH_SIZE) {
          applyMetaBatch(metaBuf);
          metaBuf.length = 0;
        }

        if (needsHash) {
          pendingMeta.set(abs, {
            size,
            ctime,
            mtime,
            updated: op_ts,
            change_start: changeStart,
            change_end: changeEnd,
            pending_start: pendingStart,
            confirmed_at: rowConfirmed ?? null,
            copy_pending: row?.copy_pending ?? 0,
          });
          hashJobs.push({ path: abs, size, ctime, mtime });
          hashTotalFiles += 1;
          hashTotalBytes += size;
          fileNodeMeta.set(rpath, {
            size,
            mtime,
            ctime,
            last_seen: scan_id,
            updated: op_ts,
            prevUpdated: row?.updated ?? null,
            prevHash: row?.hash ?? null,
            hash_pending: 1,
            change_start: changeStart,
            change_end: changeEnd,
            pending_start: pendingStart,
            confirmed_at: rowConfirmed ?? null,
            copy_pending: row?.copy_pending ?? 0,
          });
        }
      } else if (entry.dirent.isSymbolicLink()) {
        let target = "";
        try {
          target = await readlink(abs);
        } catch {}
        const hash = stringDigest(HASH_ALG, target);
        const prev = getExistingLink.get(rpath) as
          | {
              ctime: number | null;
              mtime: number | null;
              hash: string | null;
              target: string | null;
              deleted: number | null;
              updated: number | null;
              change_start: number | null;
              change_end: number | null;
              confirmed_at: number | null;
            }
          | undefined;
        const linkChanged =
          !prev ||
          prev.deleted === 1 ||
          prev.mtime !== mtime ||
          prev.ctime !== ctime ||
          prev.hash !== hash ||
          prev.target !== target;
        const op_ts = linkChanged
          ? scanTick
          : (prev?.updated ?? prev?.mtime ?? mtime);

        const linkBaseline = lowerBoundFor(prev?.confirmed_at ?? null);
        const linkChangeStart = linkChanged
          ? linkBaseline
          : prev?.change_start ?? (prev?.change_end ?? linkBaseline);
        const linkChangeEnd = linkChanged ? op_ts : (prev?.change_end ?? op_ts);
        const linkConfirmedAt = linkChanged
          ? op_ts
          : prev?.confirmed_at ?? op_ts;

        linksBuf.push({
          path: rpath,
          target,
          ctime,
          mtime,
          op_ts,
          hash,
          scan_id,
          change_start: linkChangeStart,
          change_end: linkChangeEnd,
          confirmed_at: linkConfirmedAt,
          copy_pending: 0,
        });
        if (linksBuf.length >= DB_BATCH_SIZE) {
          applyLinksBatch(linksBuf);
          linksBuf.length = 0;
        }

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
    const toDeleteNodes = db
      .prepare(
        `SELECT path, kind
           FROM nodes
          WHERE last_seen <> ?
            AND deleted = 0${restrictedClause}`,
      )
      .all(scan_id) as { path: string; kind: NodeKind }[];

    if (toDeleteNodes.length) {
      const deleteTs = scanTick;
      db.prepare(
        `UPDATE nodes
            SET deleted = 1,
                updated = @ts,
                mtime = CASE
                  WHEN last_seen IS NOT NULL THEN last_seen + 1
                  WHEN updated IS NOT NULL THEN updated + 1
                  WHEN mtime IS NOT NULL THEN mtime + 1
                  ELSE @ts
                END,
                hash = '',
                hashed_ctime = NULL,
                size = 0,
                change_start = COALESCE(change_start, confirmed_at, @lower),
                change_end = @ts,
                confirmed_at = @ts,
                copy_pending = 0,
                hash_pending = 0,
                last_error = NULL
          WHERE last_seen <> @scan
            AND deleted = 0${restrictedClause}`,
      ).run({ ts: deleteTs, scan: scan_id, lower: fallbackLowerBoundBase });

      if (emitDelta) {
        for (const r of toDeleteNodes) {
          emitObj({
            kind: r.kind === "d" ? "dir" : r.kind === "l" ? "link" : undefined,
            path: r.path,
            deleted: 1,
            op_ts: deleteTs,
          });
        }
        flushDeltaBuf();
      }
    }

    await Promise.all(workers.map((w) => w.terminate()));

    if (vacuum) {
      db.exec("vacuum");
    }

    if (!hasRestrictions) {
      writeMetaNumber(META_LAST_FULL_SCAN_START, scanWindowStart);
      writeMetaNumber(META_LAST_FULL_SCAN_END, scanTick);
    }

    logger.info("scan complete", {
      hashedFiles: dispatched,
      hashedResults: received,
      durationMs: Date.now() - t0,
    });
    if (skippedUnstableHashes) {
      logger.debug("skipped unstable hashes", {
        skipped: skippedUnstableHashes,
      });
    }
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
cliEntrypoint<ScanOptions>(
  import.meta.url,
  buildProgram,
  async (options) => {
    let clock = options.logicalClock;
    if (!clock && options.clockBase && options.clockBase.length) {
      clock = await createLogicalClock([options.db, ...options.clockBase]);
    }
    const scanTickOverride =
      options.scanTick !== undefined && options.scanTick !== null
        ? Number(options.scanTick)
        : undefined;
    await runScan({
      ...options,
      logicalClock: clock,
      scanTick: scanTickOverride,
    });
  },
  {
    label: "scan",
  },
);
