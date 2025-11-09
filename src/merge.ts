#!/usr/bin/env node

/**
 * Merge strategy overview:
 *
 * 1. Build 3-way plans from alpha/beta/base (TMP tables). For every path we
 *    decide whether it should be copied alpha→beta, beta→alpha, created,
 *    or deleted. We also precompute delete plans and dir copies.
 *
 * 2. Execute rsync (or "cp --reflink" when enabled) in well defined phases:
 *    delete file conflicts, cleanup dir→file cases, copy directories,
 *    copy files/links, then delete empty directories.
 *
 * 3. After each transfer we determine exactly which paths actually completed.
 *    For files/links we rely on rsync transfer logs; for directories we use
 *    the per-dir rsync stream. We then:
 *      • mirror the authoritative metadata from the source DB into the
 *        destination DB (files/links). For directories we currently rely on
 *        noteAlpha/BetaChange because their metadata is sparse.
 *      • update the destination DB immediately so the next cycle sees the same
 *        metadata the source had when the copy completed.
 *      • update base/base_dirs only for confirmed transfers/deletions.
 *
 * 4. The next scan reconciles any concurrent edits: if the source mutated while
 *    rsync was running we will notice on the next merge because the source DB
 *    changes again, while the destination DB remains at the mirrored state.
 *
 * This keeps the invariant that single-sided activity never causes a bounce
 * in the opposite direction, while still allowing us to skip immediate re-scans
 * after every transfer.
 */

// merge.ts
import { tmpdir } from "node:os";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { lstatSync } from "node:fs";
import path from "node:path";
import { Command, Option } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import { getBaseDb, getDb } from "./db.js";
import {
  collectIgnoreOption,
  createIgnorer,
  filterIgnored,
  filterIgnoredDirs,
  normalizeIgnorePatterns,
} from "./ignore.js";
import { CLI_NAME } from "./constants.js";
import {
  rsyncCopyChunked,
  rsyncCopyDirs,
  rsyncDeleteChunked,
  ensureTempDir,
} from "./rsync.js";
import { cpReflinkFromList, sameDevice } from "./reflink.js";
import { signatureFromStamp, type SendSignature } from "./recent-send.js";
import { fetchOpStamps } from "./op-stamp.js";
import { createHash } from "node:crypto";
import { updateSession } from "./session-db.js";
import {
  ConsoleLogger,
  type Logger,
  type LogLevel,
  LOG_LEVELS,
  parseLogLevel,
} from "./logger.js";
import type { SignatureEntry } from "./signature-store.js";
import {
  collectListOption,
  dedupeRestrictedList,
} from "./restrict.js";

// if true immediately freeze if there is a plan to modify anything on alpha.
// Obviously only for very low level debugging!
const TERMINATE_ON_CHANGE_ALPHA =
  !!process.env.REFLECT_TERMINATE_ON_CHANGE_ALPHA;

// set to true for debugging
const LEAVE_TEMP_FILES = false;

function toBoolVerbose(v?: boolean | string): boolean {
  if (typeof v === "string") {
    const trimmed = v.trim().toLowerCase();
    if (
      !trimmed ||
      trimmed === "false" ||
      trimmed === "0" ||
      trimmed === "off"
    ) {
      return false;
    }
    return true;
  }
  return !!v;
}

export function configureMergeCommand(
  command: Command,
  { standalone = false }: { standalone?: boolean } = {},
): Command {
  if (standalone) {
    command.name(`${CLI_NAME}-merge`);
  }
  return command
    .description("3-way plan + rsync between alpha/beta; updates base snapshot")
    .requiredOption("--alpha-root <path>", "alpha filesystem root")
    .requiredOption("--beta-root <path>", "beta filesystem root")
    .option("--alpha-db <path>", "alpha sqlite", "alpha.db")
    .option("--beta-db <path>", "beta sqlite", "beta.db")
    .option("--base-db <path>", "base sqlite", "base.db")
    .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
    .option("--alpha-port <n>", "SSH port for alpha", (v) =>
      Number.parseInt(v, 10),
    )
    .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
    .option("--beta-port <n>", "SSH port for beta", (v) =>
      Number.parseInt(v, 10),
    )
    .addOption(
      new Option("--prefer <side>", "conflict winner")
        .choices(["alpha", "beta"])
        .default("alpha"),
    )
    .option("--dry-run", "simulate without changing files", false)
    .option(
      "--compress <algo>",
      "[auto|zstd|lz4|zlib|zlibx|none][:level]",
      "auto",
    )
    .option("--session-id <id>", "optional session id to enable heartbeats")
    .option("--session-db <path>", "path to session database")
    .option(
      "--log-level <level>",
      `log verbosity (${LOG_LEVELS.join(", ")})`,
      "info",
    )
    .option(
      "-i, --ignore <pattern>",
      "gitignore-style ignore rule (repeat or comma-separated)",
      collectIgnoreOption,
      [] as string[],
    )
    .option(
      "--restricted-path <path>",
      "restrict merge to a relative path (repeat or comma-separated)",
      collectListOption,
      [] as string[],
    )
    .option(
      "--restricted-dir <path>",
      "restrict merge to a directory tree (repeat or comma-separated)",
      collectListOption,
      [] as string[],
    );
}

export function normalizeMergeCliOptions<T extends Record<string, unknown>>(
  opts: T,
): T {
  const anyOpts = opts as Record<string, unknown>;
  if (
    Array.isArray(anyOpts.restrictedPath) &&
    !Array.isArray(anyOpts.restrictedPaths)
  ) {
    anyOpts.restrictedPaths = anyOpts.restrictedPath;
  }
  if (
    Array.isArray(anyOpts.restrictedDir) &&
    !Array.isArray(anyOpts.restrictedDirs)
  ) {
    anyOpts.restrictedDirs = anyOpts.restrictedDir;
  }
  return opts;
}

function buildProgram(): Command {
  return configureMergeCommand(new Command(), { standalone: true });
}

type MergeRsyncOptions = {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  alphaHost?: string;
  alphaPort?: number;
  betaHost?: string;
  betaPort?: number;
  prefer: "alpha" | "beta";
  dryRun: boolean | string;
  verbose?: boolean | string;
  compress?: string;
  sessionId?: number;
  sessionDb?: string;
  logger?: Logger;
  logLevel?: LogLevel | string;
  ignoreRules?: string[];
  fetchRemoteAlphaSignatures?: (
    paths: string[],
    opts: { ignore: boolean; stable?: boolean },
  ) => Promise<SignatureEntry[]> | Promise<void> | void;
  fetchRemoteBetaSignatures?: (
    paths: string[],
    opts: { ignore: boolean; stable?: boolean },
  ) => Promise<SignatureEntry[]> | Promise<void> | void;
  alphaRemoteLock?: RemoteLockHandle;
  betaRemoteLock?: RemoteLockHandle;
  restrictedPaths?: string[];
  restrictedDirs?: string[];
  enableReflink?: boolean;
};

// ---------- helpers ----------
function join0(items: string[]) {
  const filtered = items.filter(Boolean);
  return filtered.length
    ? Buffer.from(filtered.join("\0") + "\0")
    : Buffer.alloc(0);
}

type ReleaseEntry = {
  path: string;
  watermark?: number | null;
};

export type RemoteLockHandle = {
  lock: (paths: string[]) => Promise<void>;
  release: (entries: ReleaseEntry[]) => Promise<void>;
  unlock: (paths: string[]) => Promise<void>;
};

const buildReleaseEntries = (
  releasePaths: string[],
  signatures: SignatureEntry[],
): ReleaseEntry[] => {
  if (!releasePaths.length || !signatures.length) return [];
  const sigMap = new Map<string, SignatureEntry["signature"]>();
  for (const entry of signatures) {
    sigMap.set(entry.path, entry.signature);
  }
  const entries: ReleaseEntry[] = [];
  for (const path of releasePaths) {
    const sig = sigMap.get(path);
    if (!sig || sig.kind === "missing") continue;
    const watermark = sig.ctime ?? sig.mtime ?? sig.opTs ?? Date.now();
    entries.push({ path, watermark });
  }
  return entries;
};

async function finalizeRemoteLock(
  handle: RemoteLockHandle | undefined,
  lockedPaths: string[],
  releasePaths: string[],
  signatures: SignatureEntry[],
) {
  if (!handle || !lockedPaths.length) return;
  if (!releasePaths.length) {
    await handle.unlock(lockedPaths);
    return;
  }
  const releaseEntries = buildReleaseEntries(releasePaths, signatures);
  const released = new Set(releaseEntries.map((e) => e.path));
  if (releaseEntries.length) {
    await handle.release(releaseEntries);
  }
  const leftovers = lockedPaths.filter((p) => !released.has(p));
  if (leftovers.length) {
    await handle.unlock(leftovers);
  }
}

export async function runMerge({
  alphaRoot,
  betaRoot,
  alphaDb,
  betaDb,
  baseDb,
  alphaHost,
  alphaPort: rawAlphaPort,
  betaHost,
  betaPort: rawBetaPort,
  prefer,
  dryRun,
  compress,
  sessionDb,
  sessionId,
  logger: providedLogger,
  logLevel = "info",
  verbose,
  ignoreRules: rawIgnoreRules = [],
  fetchRemoteAlphaSignatures,
  fetchRemoteBetaSignatures,
  alphaRemoteLock,
  betaRemoteLock,
  restrictedPaths,
  restrictedDirs,
  enableReflink = false,
}: MergeRsyncOptions) {
  const coercePort = (value: unknown): number | undefined => {
    if (value === undefined || value === null || value === "") return undefined;
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : undefined;
  };
  const alphaPort = coercePort(rawAlphaPort);
  const betaPort = coercePort(rawBetaPort);

  const level =
    typeof logLevel === "string"
      ? parseLogLevel(logLevel, "info")
      : (logLevel ?? "info");
  const logger = providedLogger ?? new ConsoleLogger(level);
  const debug = toBoolVerbose(verbose) || level === "debug";
  const sshPort = alphaHost ? alphaPort : betaPort;
  const rsyncOpts = {
    dryRun,
    verbose: debug,
    compress,
    logger,
    logLevel: level,
    sshPort,
  };

  const ignoreRules = normalizeIgnorePatterns(rawIgnoreRules);
  const alphaIg = createIgnorer(ignoreRules);
  const betaIg = createIgnorer(ignoreRules);

  const t = (label: string) => {
    if (!debug) return () => {};
    logger.debug(`[phase] ${label}: running...`);
    const t0 = Date.now();
    return () => {
      const dt = Date.now() - t0;
      logger.debug(`[phase] ${label}: ${dt} ms`);
    };
  };

  // small helpers used by safety rails
  const asSet = (xs: string[]) => new Set(xs);
  const uniq = (xs: string[]) => Array.from(asSet(xs));
  const depth = (r: string) => (r ? r.split("/").length : 0);
  const sortDeepestFirst = (xs: string[]) =>
    xs.slice().sort((a, b) => depth(b) - depth(a));
  const nonRoot = (xs: string[]) => xs.filter((r) => r && r !== ".");
  const buildSignatureMap = (
    dbPath: string,
    paths: string[],
  ): Map<string, SendSignature | null> => {
    const unique = uniq(paths);
    if (!unique.length) return new Map();
    const stamps = fetchOpStamps(dbPath, unique);
    const map = new Map<string, SendSignature | null>();
    for (const path of unique) {
      map.set(path, signatureFromStamp(stamps.get(path)));
    }
    return map;
  };

  const buildSignatureFromFs = (
    root: string,
    relPath: string,
  ): SendSignature | null => {
    try {
      const abs = path.join(root, relPath);
      const st = lstatSync(abs);
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
      const ctime = (st as any).ctimeMs ?? st.ctime.getTime();
      if (st.isSymbolicLink()) {
        return {
          kind: "link",
          opTs: mtime,
          mtime,
          ctime,
        };
      }
      if (st.isDirectory()) {
        return {
          kind: "dir",
          opTs: mtime,
          mtime,
          ctime,
        };
      }
      if (st.isFile()) {
        return {
          kind: "file",
          opTs: mtime,
          mtime,
          ctime,
          size: st.size,
        };
      }
      return {
        kind: "file",
        opTs: mtime,
        mtime,
        ctime,
        size: st.size,
      };
    } catch (err: any) {
      if (err?.code === "ENOENT") {
        return {
          kind: "missing",
          opTs: Date.now(),
        };
      }
      return null;
    }
  };

  const collectLocalSignatureEntries = (
    root: string,
    paths: string[],
  ): SignatureEntry[] => {
    const entries: SignatureEntry[] = [];
    for (const path of uniq(paths)) {
      const sig = buildSignatureFromFs(root, path);
      if (sig) {
        entries.push({ path, signature: sig });
      }
    }
    return entries;
  };

  const normalizeRpath = (input: string): string => {
    if (!input) return input;
    let out = input;
    if (out.startsWith("./")) out = out.slice(2);
    while (out.length > 1 && out.endsWith("/")) {
      out = out.slice(0, -1);
    }
    return out;
  };

  const fetchSignaturesForTarget = async (
    target: "alpha" | "beta",
    paths: string[],
  ): Promise<SignatureEntry[]> => {
    if (!paths.length) return [];
    if (target === "alpha") {
      if (alphaHost) {
        if (!fetchRemoteAlphaSignatures) return [];
        const res =
          (await fetchRemoteAlphaSignatures(paths, {
            ignore: false,
            stable: false,
          })) ?? [];
        return res;
      }
      return collectLocalSignatureEntries(alphaRoot, paths);
    } else {
      if (betaHost) {
        if (!fetchRemoteBetaSignatures) return [];
        const res =
          (await fetchRemoteBetaSignatures(paths, {
            ignore: false,
            stable: false,
          })) ?? [];
        return res;
      }
      return collectLocalSignatureEntries(betaRoot, paths);
    }
  };

  const confirmDeletions = async (
    target: "alpha" | "beta",
    planned: string[],
    explicit: string[],
  ): Promise<string[]> => {
    if (!planned.length) return [];
    const plannedEntries = planned.map((raw) => ({
      raw,
      norm: normalizeRpath(raw),
    }));
    const canonical = new Map<string, string>();
    for (const entry of plannedEntries) {
      if (!canonical.has(entry.norm)) {
        canonical.set(entry.norm, entry.raw);
      }
    }
    const confirmed = new Set<string>();
    for (const p of explicit) {
      const norm = normalizeRpath(p);
      if (!norm) continue;
      confirmed.add(norm);
      if (!canonical.has(norm)) {
        canonical.set(norm, p);
      }
    }
    const remaining = plannedEntries
      .filter((entry) => !confirmed.has(entry.norm))
      .map((entry) => entry.raw);
    if (remaining.length) {
      const sigs = await fetchSignaturesForTarget(target, remaining);
      for (const entry of sigs) {
        if (entry.signature.kind === "missing") {
          const norm = normalizeRpath(entry.path);
          confirmed.add(norm);
          if (!canonical.has(norm)) {
            canonical.set(norm, entry.path);
          }
        }
      }
    }
    return Array.from(confirmed).map((norm) => canonical.get(norm) ?? norm);
  };

  let alphaTempDir: string | undefined;
  let betaTempDir: string | undefined;
  let alphaTempArg: string | undefined;
  let betaTempArg: string | undefined;

  const requestRemoteBetaIgnore = async (paths: string[]) => {
    if (!paths.length || !betaHost || !fetchRemoteBetaSignatures) return false;
    await fetchRemoteBetaSignatures(paths, { ignore: true });
    return true;
  };

  const requestRemoteAlphaIgnore = async (paths: string[]) => {
    if (!paths.length || !alphaHost || !fetchRemoteAlphaSignatures)
      return false;
    await fetchRemoteAlphaSignatures(paths, { ignore: true });
    return true;
  };

  const noteBetaChange = async (paths: string[]) => {
    if (!paths.length) return;
  };

  const noteAlphaChange = async (paths: string[]) => {
    if (!paths.length) return;
  };

  async function main() {
    alphaTempDir = !alphaHost ? await ensureTempDir(alphaRoot) : undefined;
    betaTempDir = !betaHost ? await ensureTempDir(betaRoot) : undefined;
    alphaTempArg = alphaHost ? ".reflect-rsync-tmp" : alphaTempDir;
    betaTempArg = betaHost ? ".reflect-rsync-tmp" : betaTempDir;
    // ---------- DB ----------
    // ensure alpha/beta exist (creates schema if missing)
    getDb(alphaDb);
    getDb(betaDb);
    const db = getBaseDb(baseDb);

    db.prepare(`ATTACH DATABASE ? AS alpha`).run(alphaDb);
    db.prepare(`ATTACH DATABASE ? AS beta`).run(betaDb);

    const restrictedPathList = dedupeRestrictedList(restrictedPaths);
    const restrictedDirList = dedupeRestrictedList(restrictedDirs);
    const hasRestrictions =
      restrictedPathList.length > 0 || restrictedDirList.length > 0;
    if (hasRestrictions) {
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
      if (restrictedPathList.length) {
        insertRestrictedBatch(restrictedPathList);
      }
      if (restrictedDirList.length) {
        const dirTables = [
          "alpha.files",
          "alpha.links",
          "alpha.dirs",
          "beta.files",
          "beta.links",
          "beta.dirs",
          "base",
          "base_dirs",
        ];
        const dirStmts = dirTables.map((tbl) =>
          db.prepare(
            `
              INSERT OR IGNORE INTO restricted_paths(rpath)
              SELECT path FROM ${tbl}
              WHERE path = @dir
                 OR (path >= @prefix AND path < @upper)
            `,
          ),
        );
        const insertDirs = db.transaction((dirs: string[]) => {
          for (const dir of dirs) {
            insertRestricted.run(dir);
            const prefix = `${dir}/`;
            const upper = `${prefix}\uffff`;
            for (const stmt of dirStmts) {
              stmt.run({ dir, prefix, upper });
            }
          }
        });
        insertDirs(restrictedDirList);
      }
    }
    const restrictEntriesSuffix = hasRestrictions
      ? " AND path IN (SELECT rpath FROM restricted_paths)"
      : "";
    const restrictWhereClause = hasRestrictions
      ? " WHERE path IN (SELECT rpath FROM restricted_paths)"
      : "";

    const chunkPaths = <T,>(items: T[], size = 200): T[][] => {
      if (items.length <= size) return [items];
      const chunks: T[][] = [];
      for (let i = 0; i < items.length; i += size) {
        chunks.push(items.slice(i, i + size));
      }
      return chunks;
    };

    type TableKind = "files" | "dirs" | "links";
    type MirrorConfig = {
      columns: string[];
      select: string;
    };

    const mirrorConfigs: Record<TableKind, MirrorConfig> = {
      files: {
        columns: [
          "path",
          "size",
          "ctime",
          "mtime",
          "op_ts",
          "hash",
          "deleted",
          "last_seen",
          "hashed_ctime",
        ],
        select:
          "path, size, ctime, mtime, op_ts, hash, 0 AS deleted, last_seen, hashed_ctime",
      },
      dirs: {
        columns: [
          "path",
          "ctime",
          "mtime",
          "op_ts",
          "hash",
          "deleted",
          "last_seen",
        ],
        select:
          "path, ctime, mtime, op_ts, hash, 0 AS deleted, last_seen",
      },
      links: {
        columns: [
          "path",
          "target",
          "ctime",
          "mtime",
          "op_ts",
          "hash",
          "deleted",
          "last_seen",
        ],
        select:
          "path, target, ctime, mtime, op_ts, hash, 0 AS deleted, last_seen",
      },
    };

    const mirrorTableEntries = (
      source: "alpha" | "beta",
      dest: "alpha" | "beta",
      table: TableKind,
      paths: string[],
    ) => {
      const unique = uniq(paths);
      if (!unique.length || source === dest) return;
      const cfg = mirrorConfigs[table];
      const assignments = cfg.columns
        .filter((col) => col !== "path")
        .map((col) => `${col} = excluded.${col}`)
        .join(", ");
      for (const chunk of chunkPaths(unique)) {
        if (!chunk.length) continue;
        const placeholders = chunk.map(() => "?").join(",");
        const sql = `
          INSERT INTO ${dest}.${table} (${cfg.columns.join(",")})
          SELECT ${cfg.select}
          FROM ${source}.${table}
          WHERE path IN (${placeholders})
          ON CONFLICT(path) DO UPDATE SET ${assignments};
        `;
        db.prepare(sql).run(...chunk);
      }
    };

    const mirrorFilesAndLinks = (
      source: "alpha" | "beta",
      dest: "alpha" | "beta",
      paths: string[],
    ) => {
      if (!paths.length) return;
      mirrorTableEntries(source, dest, "files", paths);
      mirrorTableEntries(source, dest, "links", paths);
    };

    const mirrorDirs = (
      source: "alpha" | "beta",
      dest: "alpha" | "beta",
      paths: string[],
    ) => {
      if (!paths.length) return;
      mirrorTableEntries(source, dest, "dirs", paths);
    };



    // ---------- EARLY-OUT FAST PATH ----------
    // Hash live (non-deleted) catalogs of both sides. If unchanged vs last time → skip planning/rsync.
    db.exec(`
      CREATE TABLE IF NOT EXISTS merge_meta (
        key   TEXT PRIMARY KEY,
        value TEXT
      );
    `);

    const computeSideDigest = (side: "alpha" | "beta") => {
      // Only non-deleted; include files, links, and dirs; paths are already RELATIVE.
      const stmt = db.prepare(`
        SELECT path, COALESCE(hash, '') AS hash
        FROM (
          SELECT path, hash FROM ${side}.files WHERE deleted = 0 AND hash IS NOT NULL
          UNION ALL
          SELECT path, hash FROM ${side}.links WHERE deleted = 0 AND hash IS NOT NULL
          UNION ALL
          SELECT path, hash FROM ${side}.dirs  WHERE deleted = 0 AND hash IS NOT NULL
        )
        ORDER BY path
      `);
      const h = createHash("sha256");
      for (const row of stmt.iterate()) {
        h.update(`${row.path}\x1f${row.hash}\x1f`);
      }
      return h.digest("base64");
    };

    let done = t("compute digests");
    const digestAlpha = computeSideDigest("alpha");
    const digestBeta = computeSideDigest("beta");
    if (debug) {
      logger.debug("[digest] alpha", { digest: digestAlpha });
      logger.debug("[digest] beta", { digest: digestBeta });
    }
    if (sessionDb && sessionId) {
      updateSession(sessionDb, sessionId, {
        last_digest: Date.now(),
        alpha_digest: digestAlpha,
        beta_digest: digestBeta,
      });
    }

    done();

    if (digestAlpha == digestBeta) {
      // if both sides are identical the 3-way merge isn't going to do
      // anything. We're definitely in sync.
      if (debug) {
        logger.debug(
          "[merge] no-op: both sides are equal; skipping planning/rsync",
        );
      }
      return;
    }
    // ---------- END EARLY-OUT FAST PATH ----------

    // ---------- Build rel tables (already RELATIVE paths in alpha/beta) ----------
    db.exec(
      `
      DROP VIEW IF EXISTS alpha_entries;
      DROP VIEW IF EXISTS beta_entries;
      `,
    );
    /*
    The alpha_entries/beta_entries queries below are complicated for many reasons:
     - a path may occur in both alpha.files
       and alpha.links, e.g., if it was recently a file, then replaced
       by a symlink with the same name.  Hence we dedup by taking
       the non-deleted one (say).  The paths must be unique for
       creating alpha_rel and beta_rel below, where the path is the
       primary key.
     - if a file is created on alpha and scan'd so in the database,
       then copied to beta, then quickly deleted on beta, it will not
       be in the database for beta at all (not marked as a delete).
       Thus after the scan we expand the tables for both sides
       so that the paths are the same, defaulting to a delete happening
       if there is nothing on one side (so we are obviously assuming
       that scanning fully works).
     */
    db.exec(`
      -- =========================
      -- alpha_entries (ranked)
      -- =========================
      CREATE TEMP VIEW alpha_entries AS
      WITH unioned AS (
        SELECT path, hash, deleted, op_ts, mtime FROM alpha.files
        UNION ALL
        SELECT path, hash, deleted, op_ts, mtime FROM alpha.links
      ),
      ranked AS (
        SELECT path, hash, deleted, op_ts, mtime,
               ROW_NUMBER() OVER (
                 PARTITION BY path
                 ORDER BY
                   deleted ASC,  -- prefer non-deleted
                   op_ts  DESC   -- then newest op_ts
               ) AS rn
        FROM unioned
      )
      SELECT path, hash, deleted, op_ts, mtime
      FROM ranked
      WHERE rn = 1${restrictEntriesSuffix};

      -- =========================
      -- beta_entries (ranked)
      -- =========================
      CREATE TEMP VIEW beta_entries AS
      WITH unioned AS (
        SELECT path, hash, deleted, op_ts, mtime FROM beta.files
        UNION ALL
        SELECT path, hash, deleted, op_ts, mtime FROM beta.links
      ),
      ranked AS (
        SELECT path, hash, deleted, op_ts, mtime,
               ROW_NUMBER() OVER (
                 PARTITION BY path
                 ORDER BY
                   deleted ASC,
                   op_ts  DESC
               ) AS rn
        FROM unioned
      )
      SELECT path, hash, deleted, op_ts, mtime
      FROM ranked
      WHERE rn = 1${restrictEntriesSuffix};
    `);

    db.exec(
      `
      DROP TABLE IF EXISTS alpha_rel;
      DROP TABLE IF EXISTS beta_rel;
      DROP TABLE IF EXISTS base_rel;`,
    );

    done = t("build *_rel");

    db.exec(`
      CREATE TEMP TABLE alpha_rel(
        rpath   TEXT PRIMARY KEY,
        hash    TEXT,
        deleted INTEGER,
        op_ts   INTEGER,
        mtime   INTEGER
      ) WITHOUT ROWID;

      CREATE TEMP TABLE beta_rel(
        rpath   TEXT PRIMARY KEY,
        hash    TEXT,
        deleted INTEGER,
        op_ts   INTEGER,
        mtime   INTEGER
      ) WITHOUT ROWID;

      CREATE TEMP TABLE base_rel(
        rpath   TEXT PRIMARY KEY,
        hash    TEXT,
        deleted INTEGER,
        op_ts   INTEGER,
        mtime   INTEGER
      ) WITHOUT ROWID;
    `);

    db.exec(`
      INSERT INTO alpha_rel(rpath,hash,deleted,op_ts, mtime)
      SELECT path, hash, deleted, op_ts, mtime FROM alpha_entries;

      INSERT INTO beta_rel(rpath,hash,deleted,op_ts, mtime)
      SELECT path, hash, deleted, op_ts, mtime FROM beta_entries;

      INSERT INTO base_rel(rpath,hash,deleted,op_ts)
      SELECT path, hash, deleted, op_ts FROM base${restrictWhereClause};
    `);

    // Dirs (RELATIVE)
    db.exec(`
      DROP TABLE IF EXISTS alpha_dirs_rel;
      DROP TABLE IF EXISTS beta_dirs_rel;
      DROP TABLE IF EXISTS base_dirs_rel;

      CREATE TEMP TABLE alpha_dirs_rel(
        rpath   TEXT PRIMARY KEY,
        deleted INTEGER,
        op_ts   INTEGER,
        mtime   INTEGER,
        hash    TEXT
      ) WITHOUT ROWID;

      CREATE TEMP TABLE beta_dirs_rel(
        rpath   TEXT PRIMARY KEY,
        deleted INTEGER,
        op_ts   INTEGER,
        mtime   INTEGER,
        hash    TEXT
      ) WITHOUT ROWID;

      CREATE TEMP TABLE base_dirs_rel(
        rpath   TEXT PRIMARY KEY,
        deleted INTEGER,
        op_ts   INTEGER,
        mtime   INTEGER,
        hash    TEXT
      ) WITHOUT ROWID;

      INSERT INTO alpha_dirs_rel(rpath,deleted,op_ts,mtime,hash)
      SELECT path, deleted, op_ts, mtime, COALESCE(hash,'')
      FROM alpha.dirs${restrictWhereClause};

      INSERT INTO beta_dirs_rel(rpath,deleted,op_ts,mtime,hash)
      SELECT path, deleted, op_ts, mtime, COALESCE(hash,'')
      FROM beta.dirs${restrictWhereClause};

      INSERT INTO base_dirs_rel(rpath,deleted,op_ts,hash)
      SELECT path, deleted, op_ts, COALESCE(hash,'')
      FROM base_dirs${restrictWhereClause};
    `);

    // covering indexes for (deleted,rpath) checks
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_alpha_rel_deleted_rpath ON alpha_rel(deleted, rpath);
      CREATE INDEX IF NOT EXISTS idx_beta_rel_deleted_rpath  ON beta_rel(deleted,  rpath);
      CREATE INDEX IF NOT EXISTS idx_base_rel_deleted_rpath  ON base_rel(deleted,  rpath);

      CREATE INDEX IF NOT EXISTS idx_alpha_dirs_rel_deleted_rpath ON alpha_dirs_rel(deleted, rpath);
      CREATE INDEX IF NOT EXISTS idx_beta_dirs_rel_deleted_rpath  ON beta_dirs_rel(deleted,  rpath);
      CREATE INDEX IF NOT EXISTS idx_base_dirs_rel_deleted_rpath  ON base_dirs_rel(deleted,  rpath);
    `);
    done();

    // ---------- 3-way plan: FILES ----------
    done = t("build tmp_* files");
    db.exec(`
      DROP TABLE IF EXISTS tmp_changedA;
      DROP TABLE IF EXISTS tmp_changedB;
      DROP TABLE IF EXISTS tmp_deletedA;
      DROP TABLE IF EXISTS tmp_deletedB;

      CREATE TEMP TABLE tmp_changedA(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TEMP TABLE tmp_changedB(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TEMP TABLE tmp_deletedA(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TEMP TABLE tmp_deletedB(rpath TEXT PRIMARY KEY) WITHOUT ROWID;

      INSERT OR IGNORE INTO tmp_changedA(rpath)
      SELECT a.rpath
      FROM alpha_rel a
      LEFT JOIN base_rel b USING (rpath)
      WHERE a.deleted = 0 AND (b.rpath IS NULL OR b.deleted = 1 OR a.hash <> b.hash);

      INSERT OR IGNORE INTO tmp_changedB(rpath)
      SELECT b.rpath
      FROM beta_rel b
      LEFT JOIN base_rel bb USING (rpath)
      WHERE b.deleted = 0 AND (bb.rpath IS NULL OR bb.deleted = 1 OR b.hash <> bb.hash);

      INSERT OR IGNORE INTO tmp_deletedA(rpath)
      SELECT b.rpath
      FROM base_rel b
      LEFT JOIN alpha_rel a USING (rpath)
      WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);

      INSERT OR IGNORE INTO tmp_deletedB(rpath)
      SELECT b.rpath
      FROM base_rel b
      LEFT JOIN beta_rel a USING (rpath)
      WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);
    `);
    done();

    // ---------- 3-way plan: DIRS ----------
    db.exec(`
      DROP TABLE IF EXISTS tmp_dirs_changedA;
      DROP TABLE IF EXISTS tmp_dirs_changedB;
      DROP TABLE IF EXISTS tmp_dirs_deletedA;
      DROP TABLE IF EXISTS tmp_dirs_deletedB;

      CREATE TEMP TABLE tmp_dirs_changedA(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TEMP TABLE tmp_dirs_changedB(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TEMP TABLE tmp_dirs_deletedA(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TEMP TABLE tmp_dirs_deletedB(rpath TEXT PRIMARY KEY) WITHOUT ROWID;

      INSERT OR IGNORE INTO tmp_dirs_changedA(rpath)
      SELECT a.rpath
      FROM alpha_dirs_rel a
      LEFT JOIN base_dirs_rel b USING (rpath)
      WHERE a.deleted = 0 AND (b.rpath IS NULL OR b.deleted = 1 OR a.hash <> b.hash);

      INSERT OR IGNORE INTO tmp_dirs_changedB(rpath)
      SELECT b.rpath
      FROM beta_dirs_rel b
      LEFT JOIN base_dirs_rel bb USING (rpath)
      WHERE b.deleted = 0 AND (bb.rpath IS NULL OR bb.deleted = 1 OR b.hash <> bb.hash);

      INSERT OR IGNORE INTO tmp_dirs_deletedA(rpath)
      SELECT b.rpath
      FROM base_dirs_rel b
      LEFT JOIN alpha_dirs_rel a USING (rpath)
      WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);

      INSERT OR IGNORE INTO tmp_dirs_deletedB(rpath)
      SELECT b.rpath
      FROM base_dirs_rel b
      LEFT JOIN beta_dirs_rel a USING (rpath)
      WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);
    `);

    done = t("derive plan arrays");
    const count = (tbl: string) =>
      (db.prepare(`SELECT COUNT(*) AS n FROM ${tbl}`).get() as any).n as number;
    if (debug) {
      logger.debug("planner input counts", {
        changedA: count("tmp_changedA"),
        changedB: count("tmp_changedB"),
        deletedA: count("tmp_deletedA"),
        deletedB: count("tmp_deletedB"),
        d_changedA: count("tmp_dirs_changedA"),
        d_changedB: count("tmp_dirs_changedB"),
        d_deletedA: count("tmp_dirs_deletedA"),
        d_deletedB: count("tmp_dirs_deletedB"),
      });
    }

    // ---------- Build copy/delete plans (FILES) with LWW ----------
    let toBeta = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_changedA a
        WHERE NOT EXISTS (SELECT 1 FROM tmp_changedB WHERE rpath = a.rpath)
          AND ( ? <> 'beta' OR NOT EXISTS (SELECT 1 FROM tmp_deletedB WHERE rpath = a.rpath))
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let toAlpha = db
      .prepare(
        `
        SELECT b.rpath
        FROM tmp_changedB b
        WHERE NOT EXISTS (SELECT 1 FROM tmp_changedA WHERE rpath = b.rpath)
          AND ( ? <> 'alpha' OR NOT EXISTS (SELECT 1 FROM tmp_deletedA WHERE rpath = b.rpath))
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const bothChangedToBeta = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_changedA cA
        JOIN tmp_changedB cB USING (rpath)
        JOIN alpha_rel a USING (rpath)
        JOIN beta_rel  b USING (rpath)
        WHERE a.mtime > b.mtime
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const bothChangedToAlpha = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_changedA cA
        JOIN tmp_changedB cB USING (rpath)
        JOIN alpha_rel a USING (rpath)
        JOIN beta_rel  b USING (rpath)
        WHERE b.mtime > a.mtime
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const bothChangedTie = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_changedA cA
        JOIN tmp_changedB cB USING (rpath)
        JOIN alpha_rel a USING (rpath)
        JOIN beta_rel  b USING (rpath)
        WHERE a.mtime = b.mtime
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    if (prefer === "alpha") {
      toBeta = uniq([...toBeta, ...bothChangedToBeta, ...bothChangedTie]);
      toAlpha = uniq([...toAlpha, ...bothChangedToAlpha]);
    } else {
      toBeta = uniq([...toBeta, ...bothChangedToBeta]);
      toAlpha = uniq([...toAlpha, ...bothChangedToAlpha, ...bothChangedTie]);
    }
    done();

    // ---------- deletions (files) with LWW ----------
    done = t("deletions");

    // everything deleted in A but NOT changed in
    // B and not deleted in B.  Definitely the thing
    // to do is delete it in B.
    const delInBeta_noConflict = db
      .prepare(
        `
        SELECT rpath FROM tmp_deletedA
        EXCEPT
        SELECT rpath FROM tmp_changedB
        EXCEPT
        SELECT rpath FROM tmp_deletedB
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const delInBeta_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_deletedA dA
        JOIN tmp_changedB cB USING (rpath)
        LEFT JOIN alpha_rel a USING (rpath)
        LEFT JOIN beta_rel  b USING (rpath)
        LEFT JOIN base_rel  br USING (rpath)
        WHERE COALESCE(a.op_ts, br.op_ts, 0) > COALESCE(b.op_ts, 0)
           OR (COALESCE(a.op_ts, br.op_ts, 0) = COALESCE(b.op_ts, 0) AND ? = 'alpha')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const toAlpha_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_deletedA dA
        JOIN tmp_changedB cB USING (rpath)
        LEFT JOIN alpha_rel a USING (rpath)
        LEFT JOIN beta_rel  b USING (rpath)
        LEFT JOIN base_rel  br USING (rpath)
        WHERE COALESCE(b.op_ts, 0) > COALESCE(a.op_ts, br.op_ts, 0)
           OR (COALESCE(b.op_ts, 0) = COALESCE(a.op_ts, br.op_ts, 0) AND ? = 'beta')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let delInBeta = uniq([...delInBeta_noConflict, ...delInBeta_conflict]);
    toAlpha = uniq([...toAlpha, ...toAlpha_conflict]);

    // everything that was deleted in B, but NOT changed in A
    // and also not deleted in A.
    const deletedOnlyInBeta = db
      .prepare(
        `
        SELECT rpath FROM tmp_deletedB
        EXCEPT
        SELECT rpath FROM tmp_changedA
        EXCEPT
        SELECT rpath FROM tmp_deletedA
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const delInAlpha_conflict = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_deletedB dB
        JOIN tmp_changedA cA USING (rpath)
        LEFT JOIN alpha_rel a USING (rpath)
        LEFT JOIN beta_rel  b USING (rpath)
        LEFT JOIN base_rel  br USING (rpath)
        WHERE COALESCE(b.op_ts, br.op_ts, 0) > COALESCE(a.op_ts, 0)
           OR (COALESCE(b.op_ts, br.op_ts, 0) = COALESCE(a.op_ts, 0) AND ? = 'beta')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const toBeta_conflict = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_deletedB dB
        JOIN tmp_changedA cA USING (rpath)
        LEFT JOIN alpha_rel a USING (rpath)
        LEFT JOIN beta_rel  b USING (rpath)
        LEFT JOIN base_rel  br USING (rpath)
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, br.op_ts, 0)
           OR (COALESCE(a.op_ts, 0) = COALESCE(b.op_ts, br.op_ts, 0) AND ? = 'alpha')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let delInAlpha: string[];
    const betaAddedContent = toAlpha.length > 0;
    if (prefer === "beta") {
      delInAlpha = uniq([...deletedOnlyInBeta, ...delInAlpha_conflict]);
    } else if (!betaAddedContent) {
      delInAlpha = uniq([...delInAlpha_conflict]);
      toBeta = uniq([...toBeta, ...deletedOnlyInBeta]);
    } else {
      delInAlpha = uniq([...deletedOnlyInBeta, ...delInAlpha_conflict]);
    }
    toBeta = uniq([...toBeta, ...toBeta_conflict]);
    done();

    // ---------- DIR plans with LWW (presence-based) ----------
    // create-only
    let toBetaDirs = db
      .prepare(
        `
        SELECT rpath FROM tmp_dirs_changedA
        WHERE NOT EXISTS (SELECT 1 FROM tmp_dirs_changedB WHERE rpath = tmp_dirs_changedA.rpath)
          AND ( ? <> 'beta' OR NOT EXISTS (SELECT 1 FROM tmp_dirs_deletedB WHERE rpath = tmp_dirs_changedA.rpath))
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let toAlphaDirs = db
      .prepare(
        `
        SELECT rpath FROM tmp_dirs_changedB
        WHERE NOT EXISTS (SELECT 1 FROM tmp_dirs_changedA WHERE rpath = tmp_dirs_changedB.rpath)
          AND ( ? <> 'alpha' OR NOT EXISTS (SELECT 1 FROM tmp_dirs_deletedA WHERE rpath = tmp_dirs_changedB.rpath))
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const bothDirsToBeta = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_dirs_changedA cA
        JOIN tmp_dirs_changedB cB USING (rpath)
        JOIN alpha_dirs_rel a USING (rpath)
        JOIN beta_dirs_rel  b USING (rpath)
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, 0)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const bothDirsToAlpha = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_dirs_changedA cA
        JOIN tmp_dirs_changedB cB USING (rpath)
        JOIN alpha_dirs_rel a USING (rpath)
        JOIN beta_dirs_rel  b USING (rpath)
        WHERE COALESCE(b.op_ts,0) > COALESCE(a.op_ts,0)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const bothDirsTie = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_dirs_changedA cA
        JOIN tmp_dirs_changedB cB USING (rpath)
        JOIN alpha_dirs_rel a USING (rpath)
        JOIN beta_dirs_rel  b USING (rpath)
        WHERE COALESCE(a.op_ts,0) = COALESCE(b.op_ts,0)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    if (prefer === "alpha") {
      toBetaDirs = uniq([...toBetaDirs, ...bothDirsToBeta, ...bothDirsTie]);
      toAlphaDirs = uniq([...toAlphaDirs, ...bothDirsToAlpha]);
    } else {
      toBetaDirs = uniq([...toBetaDirs, ...bothDirsToBeta]);
      toAlphaDirs = uniq([...toAlphaDirs, ...bothDirsToAlpha, ...bothDirsTie]);
    }

    // dir deletions with LWW
    done = t("dir LWW");
    const delDirsInBeta_noConflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_dirs_deletedA dA
        LEFT JOIN tmp_dirs_changedB cB USING (rpath)
        WHERE cB.rpath IS NULL
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const deletedDirsOnlyInBeta = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_dirs_deletedB dB
        LEFT JOIN tmp_dirs_changedA cA USING (rpath)
        WHERE cA.rpath IS NULL
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const delDirsInBeta_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_dirs_deletedA dA
        JOIN tmp_dirs_changedB cB USING (rpath)
        LEFT JOIN alpha_dirs_rel a USING (rpath)
        LEFT JOIN beta_dirs_rel  b USING (rpath)
        LEFT JOIN base_dirs_rel br USING (rpath)
        WHERE COALESCE(a.op_ts, br.op_ts, 0) > COALESCE(b.op_ts, 0)
           OR (COALESCE(a.op_ts, br.op_ts, 0) = COALESCE(b.op_ts, 0) AND ? = 'alpha')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const toAlphaDirs_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_dirs_deletedA dA
        JOIN tmp_dirs_changedB cB USING (rpath)
        LEFT JOIN alpha_dirs_rel a USING (rpath)
        LEFT JOIN beta_dirs_rel  b USING (rpath)
        LEFT JOIN base_dirs_rel br USING (rpath)
        WHERE COALESCE(b.op_ts, 0) > COALESCE(a.op_ts, br.op_ts, 0)
           OR (COALESCE(b.op_ts, 0) = COALESCE(a.op_ts, br.op_ts, 0) AND ? = 'beta')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const delDirsInAlpha_conflict = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_dirs_deletedB dB
        JOIN tmp_dirs_changedA cA USING (rpath)
        LEFT JOIN alpha_dirs_rel a USING (rpath)
        LEFT JOIN beta_dirs_rel  b USING (rpath)
        LEFT JOIN base_dirs_rel br USING (rpath)
        WHERE COALESCE(b.op_ts, br.op_ts, 0) > COALESCE(a.op_ts, 0)
           OR (COALESCE(b.op_ts, br.op_ts, 0) = COALESCE(a.op_ts, 0) AND ? = 'beta')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    const toBetaDirs_conflict = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_dirs_deletedB dB
        JOIN tmp_dirs_changedA cA USING (rpath)
        LEFT JOIN alpha_dirs_rel a USING (rpath)
        LEFT JOIN beta_dirs_rel  b USING (rpath)
        LEFT JOIN base_dirs_rel br USING (rpath)
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, br.op_ts, 0)
           OR (COALESCE(a.op_ts, 0) = COALESCE(b.op_ts, br.op_ts, 0) AND ? = 'alpha')
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let delDirsInBeta = uniq([
      ...delDirsInBeta_noConflict,
      ...delDirsInBeta_conflict,
    ]);
    let delDirsInAlpha: string[];
    const betaAddedDirs = toAlphaDirs.length > 0;
    if (prefer === "beta") {
      delDirsInAlpha = uniq([
        ...deletedDirsOnlyInBeta,
        ...delDirsInAlpha_conflict,
      ]);
    } else if (!betaAddedDirs) {
      delDirsInAlpha = uniq([...delDirsInAlpha_conflict]);
      toBetaDirs = uniq([...toBetaDirs, ...deletedDirsOnlyInBeta]);
    } else {
      delDirsInAlpha = uniq([
        ...deletedDirsOnlyInBeta,
        ...delDirsInAlpha_conflict,
      ]);
    }
    toAlphaDirs = uniq([...toAlphaDirs, ...toAlphaDirs_conflict]);
    toBetaDirs = uniq([...toBetaDirs, ...toBetaDirs_conflict]);
    toAlphaDirs = uniq([...toAlphaDirs, ...toAlphaDirs_conflict]);
    toBetaDirs = uniq([...toBetaDirs, ...toBetaDirs_conflict]);
    done();

    // ---------- TYPE-FLIP (file vs dir) conflicts ----------
    done = t("type flips");

    const fileConflictsInBeta = db
      .prepare(
        `
        SELECT b.rpath
        FROM beta_rel b
        JOIN alpha_dirs_rel d USING (rpath)
        WHERE b.deleted = 0 AND (d.deleted = 0 OR d.deleted IS NULL)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const fileConflictsInAlpha = db
      .prepare(
        `
        SELECT a.rpath
        FROM alpha_rel a
        JOIN beta_dirs_rel d USING (rpath)
        WHERE a.deleted = 0 AND (d.deleted = 0 OR d.deleted IS NULL)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    if (prefer === "alpha") {
      delInBeta = uniq([...delInBeta, ...fileConflictsInBeta]);
    } else {
      const keep = new Set(fileConflictsInBeta);
      delInBeta = delInBeta.filter((r) => !keep.has(r));
    }
    if (prefer === "beta") {
      delInAlpha = uniq([...delInAlpha, ...fileConflictsInAlpha]);
    } else {
      const keep = new Set(fileConflictsInAlpha);
      delInAlpha = delInAlpha.filter((r) => !keep.has(r));
    }

    // Pre-copy dir→file cleanup sets (only used if that side is preferred)
    let preDeleteDirsOnAlphaForBetaFiles: string[] = db
      .prepare(
        `
        SELECT a.rpath
        FROM alpha_dirs_rel a
        JOIN beta_rel b USING (rpath)
        WHERE a.deleted = 0 AND b.deleted = 0
      `,
      )
      .all()
      .map((r) => (r as any).rpath as string);

    let preDeleteDirsOnBetaForAlphaFiles: string[] = db
      .prepare(
        `
        SELECT b.rpath
        FROM beta_dirs_rel b
        JOIN alpha_rel a USING (rpath)
        WHERE b.deleted = 0 AND a.deleted = 0
      `,
      )
      .all()
      .map((r) => (r as any).rpath as string);

    preDeleteDirsOnAlphaForBetaFiles = sortDeepestFirst(
      nonRoot(uniq(preDeleteDirsOnAlphaForBetaFiles)),
    );
    preDeleteDirsOnBetaForAlphaFiles = sortDeepestFirst(
      nonRoot(uniq(preDeleteDirsOnBetaForAlphaFiles)),
    );
    done();

    // ---------- SAFETY RAILS ----------
    done = t("safety rails");
    toBeta = uniq(toBeta);
    toAlpha = uniq(toAlpha);
    delInBeta = uniq(delInBeta);
    delInAlpha = uniq(delInAlpha);
    toBetaDirs = uniq(toBetaDirs);
    toAlphaDirs = uniq(toAlphaDirs);
    delDirsInBeta = uniq(delDirsInBeta);
    delDirsInAlpha = uniq(delDirsInAlpha);


    // --- helpers: POSIX-y parent and normalization
    const posixParent = (p: string) => {
      if (!p) return "";
      const i = p.lastIndexOf("/");
      return i <= 0 ? "" : p.slice(0, i);
    };
    const normDir = (d: string) =>
      (d.endsWith("/") ? d.slice(0, -1) : d).replace(/^\.\/+/, "");

    /**
     * Build a fast “is under any deleted dir?” predicate.
     *  - Pre-normalizes and reduces the deleted-dirs list to a minimal cover
     *    (drops descendants when an ancestor is present)
     *  - Checks membership by walking ancestors using a Set (O(depth))
     *  - Caches results per path to amortize repeated prefixes
     */
    function makeAncestorChecker(dirs: string[]) {
      // Normalize and minimal-cover the deleted dirs
      const base = dirs.map(normDir).filter((d) => d && d !== ".");
      base.sort(); // lexicographic for ancestor test

      const minimal = [] as string[];
      const chosen = new Set<string>();
      for (const d of base) {
        // skip if any ancestor is already chosen
        let p = d;
        let covered = false;
        while (p) {
          if (chosen.has(p)) {
            covered = true;
            break;
          }
          p = posixParent(p);
        }
        if (!covered) {
          minimal.push(d);
          chosen.add(d);
        }
      }

      // Fast ancestor membership + memoization
      const del = chosen; // Set of minimal deleted dirs
      const memo = new Map<string, boolean>();
      return (p: string) => {
        let cur = normDir(p);
        // exact/ancestor cache walk
        while (true) {
          const hit = memo.get(cur);
          if (hit !== undefined) {
            memo.set(p, hit);
            return hit;
          }
          if (del.has(cur)) {
            memo.set(p, true);
            return true;
          }
          const parent = posixParent(cur);
          if (parent === cur || !parent) {
            memo.set(p, false);
            return false;
          }
          cur = parent;
        }
      };
    }

    const deletedDirsA = db
      .prepare(`SELECT rpath FROM tmp_dirs_deletedA`)
      .all()
      .map((r: any) => r.rpath as string);
    const deletedDirsB = db
      .prepare(`SELECT rpath FROM tmp_dirs_deletedB`)
      .all()
      .map((r: any) => r.rpath as string);

    if (prefer === "alpha" && deletedDirsA.length) {
      const underADeleted = makeAncestorChecker(deletedDirsA);
      toAlpha = toAlpha.filter((r) => !underADeleted(r));
    }
    if (prefer === "beta" && deletedDirsB.length) {
      const underBDeleted = makeAncestorChecker(deletedDirsB);
      toBeta = toBeta.filter((r) => !underBDeleted(r));
    }
    done();

    // ---------- APPLY IGNORES ----------
    done = t("ignores");
    const before = debug
      ? {
          toBeta: toBeta.length,
          toAlpha: toAlpha.length,
          delInBeta: delInBeta.length,
          delInAlpha: delInAlpha.length,
          toBetaDirs: toBetaDirs.length,
          toAlphaDirs: toAlphaDirs.length,
          delDirsInBeta: delDirsInBeta.length,
          delDirsInAlpha: delDirsInAlpha.length,
        }
      : undefined;

    const ignore = (x: string[]) => filterIgnored(x, alphaIg, betaIg);
    toBeta = ignore(toBeta);
    toAlpha = ignore(toAlpha);
    delInBeta = ignore(delInBeta);
    delInAlpha = ignore(delInAlpha);

    const ignoreDirs = (x: string[]) => filterIgnoredDirs(x, alphaIg, betaIg);
    toBetaDirs = ignoreDirs(toBetaDirs);
    toAlphaDirs = ignoreDirs(toAlphaDirs);
    delDirsInBeta = ignoreDirs(delDirsInBeta);
    delDirsInAlpha = ignoreDirs(delDirsInAlpha);

    if (debug && before) {
      const after = {
        toBeta: toBeta.length,
        toAlpha: toAlpha.length,
        delInBeta: delInBeta.length,
        delInAlpha: delInAlpha.length,
        toBetaDirs: toBetaDirs.length,
        toAlphaDirs: toAlphaDirs.length,
        delDirsInBeta: delDirsInBeta.length,
        delDirsInAlpha: delDirsInAlpha.length,
      };
      const delta = Object.fromEntries(
        Object.entries(after).map(([k, v]) => [k, (before as any)[k] - v]),
      );
      logger.debug("ignores filtered plan counts", { dropped: delta });
    }

    done();

    // ---------- overlaps + dir delete safety ----------
    done = t("overlaps");
    const delInBetaSet = asSet(delInBeta);
    const delInAlphaSet = asSet(delInAlpha);

    if (toBeta.length && delInBeta.length) {
      const toBetaSet = new Set(toBeta);
      const beforeN = delInBeta.length;
      delInBeta = delInBeta.filter((r) => !toBetaSet.has(r));
      if (debug && beforeN !== delInBeta.length) {
        logger.warn("planner safety: alpha→beta file overlap", {
          dropped: beforeN - delInBeta.length,
        });
      }
    }
    if (toAlpha.length && delInAlpha.length) {
      const toAlphaSet = new Set(toAlpha);
      const beforeN = delInAlpha.length;
      delInAlpha = delInAlpha.filter((r) => !toAlphaSet.has(r));
      if (debug && beforeN !== delInAlpha.length) {
        logger.warn("planner safety: beta→alpha file overlap", {
          dropped: beforeN - delInAlpha.length,
        });
      }
    }

    function normalizeDir(d: string) {
      return d.endsWith("/") ? d.slice(0, -1) : d;
    }
    function lowerBound(arr: string[], target: string) {
      let lo = 0,
        hi = arr.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (arr[mid] < target) lo = mid + 1;
        else hi = mid;
      }
      return lo;
    }
    function anyIncomingUnderDir(
      incomingSorted: string[],
      dir: string,
    ): boolean {
      const d = normalizeDir(dir);
      if (!d) return incomingSorted.length > 0;
      const j = lowerBound(incomingSorted, d);
      if (j < incomingSorted.length && incomingSorted[j] === d) return true;
      const prefix = d + "/";
      const i = lowerBound(incomingSorted, prefix);
      return i < incomingSorted.length && incomingSorted[i].startsWith(prefix);
    }

    if (prefer === "beta" && delDirsInBeta.length) {
      const outgoingFromBetaSorted = [...toAlpha, ...toAlphaDirs].sort();
      const beforeN = delDirsInBeta.length;
      delDirsInBeta = delDirsInBeta.filter(
        (d) => !anyIncomingUnderDir(outgoingFromBetaSorted, d),
      );
      if (debug && beforeN !== delDirsInBeta.length) {
        logger.warn(
          "planner safety: preserved beta dirs with outgoing descendants",
          {
            kept: beforeN - delDirsInBeta.length,
          },
        );
      }
    }
    if (prefer === "alpha" && delDirsInAlpha.length) {
      const outgoingFromAlphaSorted = [...toBeta, ...toBetaDirs].sort();
      const beforeN = delDirsInAlpha.length;
      delDirsInAlpha = delDirsInAlpha.filter(
        (d) => !anyIncomingUnderDir(outgoingFromAlphaSorted, d),
      );
      if (debug && beforeN !== delDirsInAlpha.length) {
        logger.warn(
          "planner safety: preserved alpha dirs with outgoing descendants",
          {
            kept: beforeN - delDirsInAlpha.length,
          },
        );
      }
    }

    const betaIncomingSorted = [...toBeta, ...toBetaDirs].sort();
    const alphaIncomingSorted = [...toAlpha, ...toAlphaDirs].sort();

    const beforeDelDirsInBeta = delDirsInBeta.length;
    delDirsInBeta = delDirsInBeta.filter(
      (d) => !anyIncomingUnderDir(betaIncomingSorted, d),
    );
    const droppedBetaDirs = beforeDelDirsInBeta - delDirsInBeta.length;

    const beforeDelDirsInAlpha = delDirsInAlpha.length;
    delDirsInAlpha = delDirsInAlpha.filter(
      (d) => !anyIncomingUnderDir(alphaIncomingSorted, d),
    );
    const droppedAlphaDirs = beforeDelDirsInAlpha - delDirsInAlpha.length;

    if (debug && (droppedBetaDirs || droppedAlphaDirs)) {
      logger.warn("planner safety: dir-delete clamped", {
        beta: droppedBetaDirs,
        alpha: droppedAlphaDirs,
      });
    }
    done();

    if (debug) {
      const againBeta = toBeta.filter((r) => delInBetaSet.has(r)).length;
      const againAlpha = toAlpha.filter((r) => delInAlphaSet.has(r)).length;
      if (againBeta || againAlpha) {
        logger.error(
          "planner invariant failed: copy/delete not disjoint after clamp",
          { againBeta, againAlpha },
        );
      }
    }

    const tmp = await mkdtemp(path.join(tmpdir(), "sync-plan-"));

    // helper definitions inserted later after alpha/beta constants
    try {
      done = t("files lists");
      const listToBeta = path.join(tmp, "toBeta.list");
      const listToAlpha = path.join(tmp, "toAlpha.list");
      const listDelInBeta = path.join(tmp, "delInBeta.list");
      const listDelInAlpha = path.join(tmp, "delInAlpha.list");

      const listToBetaDirs = path.join(tmp, "toBeta.dirs.list");
      const listToAlphaDirs = path.join(tmp, "toAlpha.dirs.list");
      const listDelDirsInBeta = path.join(tmp, "delInBeta.dirs.list");
      const listDelDirsInAlpha = path.join(tmp, "delInAlpha.dirs.list");

      delDirsInBeta = sortDeepestFirst(nonRoot(delDirsInBeta));
      delDirsInAlpha = sortDeepestFirst(nonRoot(delDirsInAlpha));

      const toBetaRelativeAll = makeRelative(toBeta, betaRoot);
      const toAlphaRelativeAll = makeRelative(toAlpha, alphaRoot);

      const alphaSignatureHintsFull = buildSignatureMap(
        alphaDb,
        toBetaRelativeAll,
      );
      const betaSignatureHintsFull = buildSignatureMap(
        betaDb,
        toAlphaRelativeAll,
      );

      const toBetaRelative: string[] = [];
      const alphaSignatureHints = new Map<string, SendSignature | null>();
      const extraBetaDirs: string[] = [];
      for (const rel of toBetaRelativeAll) {
        const sig = alphaSignatureHintsFull.get(rel);
        if (sig?.kind === "dir") {
          extraBetaDirs.push(rel);
          continue;
        }
        toBetaRelative.push(rel);
        alphaSignatureHints.set(rel, sig ?? null);
      }
      const toBetaRelativeSet = new Set(toBetaRelative);

      const toAlphaRelative: string[] = [];
      const betaSignatureHints = new Map<string, SendSignature | null>();
      const extraAlphaDirs: string[] = [];
      for (const rel of toAlphaRelativeAll) {
        const sig = betaSignatureHintsFull.get(rel);
        if (sig?.kind === "dir") {
          extraAlphaDirs.push(rel);
          continue;
        }
        toAlphaRelative.push(rel);
        betaSignatureHints.set(rel, sig ?? null);
      }
      const toAlphaRelativeSet = new Set(toAlphaRelative);

      toBetaDirs = uniq([...toBetaDirs, ...extraBetaDirs]);
      toAlphaDirs = uniq([...toAlphaDirs, ...extraAlphaDirs]);

      await writeFile(listToBeta, join0(toBetaRelative));
      await writeFile(listToAlpha, join0(toAlphaRelative));
      await writeFile(listDelInBeta, join0(delInBeta));
      await writeFile(listDelInAlpha, join0(delInAlpha));

      await writeFile(listToBetaDirs, join0(toBetaDirs));
      await writeFile(listToAlphaDirs, join0(toAlphaDirs));
      await writeFile(listDelDirsInBeta, join0(delDirsInBeta));
      await writeFile(listDelDirsInAlpha, join0(delDirsInAlpha));
      done();

      logger.info("merge plan", {
        copiesAlphaToBeta: toBeta.length,
        copiesBetaToAlpha: toAlpha.length,
        deletionsInBeta: delInBeta.length,
        deletionsInAlpha: delInAlpha.length,
        dirsCreateBeta: toBetaDirs.length,
        dirsCreateAlpha: toAlphaDirs.length,
        dirsDeleteBeta: delDirsInBeta.length,
        dirsDeleteAlpha: delDirsInAlpha.length,
        prefer,
        dryRun,
        debug,
        //         toBeta,
        //         toAlpha,
        //         delInBeta,
        //         delInAlpha,
      });

      if (
        TERMINATE_ON_CHANGE_ALPHA &&
        (toAlpha.length ||
          delInAlpha.length ||
          toAlphaDirs.length ||
          delDirsInAlpha.length)
      ) {
        logger.info("invalid - pausing forever", {
          toAlpha,
          delInAlpha,
          toAlphaDirs,
          delDirsInAlpha,
        });
        await new Promise(() => {});
      }

      // ---------- rsync ----------
      let copyDirsAlphaBetaOk = false;
      let copyDirsBetaAlphaOk = false;
      const completedCopyToBeta = new Set<string>();
      const completedCopyToAlpha = new Set<string>();
      const deletedFilesAlpha = new Set<string>();
      const deletedFilesBeta = new Set<string>();
      const deletedDirsAlpha = new Set<string>();
      const deletedDirsBeta = new Set<string>();

      const alpha = alphaHost ? `${alphaHost}:${alphaRoot}` : alphaRoot;
      const beta = betaHost ? `${betaHost}:${betaRoot}` : betaRoot;

      const isVanishedWarning = (stderr?: string | null) =>
        typeof stderr === "string" && stderr.toLowerCase().includes("vanished");

      const copyFilesWithGuards = async (
        direction: "alpha->beta" | "beta->alpha",
        paths: string[],
        options: Omit<
          Parameters<typeof rsyncCopyChunked>[5],
          "onChunkResult"
        > = {},
      ): Promise<{
        ok: boolean;
        signatures: SignatureEntry[];
        transferred: string[];
      }> => {
        if (!paths.length) return { ok: true, signatures: [], transferred: [] };
        if (!paths.length) {
          return { ok: true, signatures: [], transferred: [] };
        }
        const uniquePaths = uniq(paths);
        const remoteHost = direction === "alpha->beta" ? betaHost : alphaHost;
        const remoteLock =
          direction === "alpha->beta" ? betaRemoteLock : alphaRemoteLock;
        if (remoteLock && uniquePaths.length) {
          await remoteLock.lock(uniquePaths);
        }
        const partialPaths: string[] = [];
        let postSignatures: SignatureEntry[] = [];
        try {
          const { ok, transferred } = await rsyncCopyChunked(
            tmp,
            direction === "alpha->beta" ? alpha : beta,
            direction === "alpha->beta" ? beta : alpha,
            paths,
            direction === "alpha->beta" ? "alpha→beta" : "beta→alpha",
            {
              ...rsyncOpts,
              ...(options ?? {}),
              direction,
              captureTransfers: true,
              onChunkResult: (chunk, result) => {
                if (result.code === 23 && !isVanishedWarning(result.stderr)) {
                  partialPaths.push(...chunk);
                }
              },
            },
          );
          const transferredPaths = transferred ?? [];
          if (partialPaths.length) {
            if (remoteLock && uniquePaths.length) {
              await remoteLock.unlock(uniquePaths);
            }
            if (debug) {
              logger.warn("partial transfer", {
                direction,
                paths: partialPaths.length,
              });
            }
          }
          const fetchSignatures =
            direction === "alpha->beta"
              ? fetchRemoteBetaSignatures
              : fetchRemoteAlphaSignatures;
          if (
            remoteHost &&
            remoteLock &&
            uniquePaths.length &&
            fetchSignatures
          ) {
            postSignatures =
              (await fetchSignatures(uniquePaths, { ignore: false })) ?? [];
            await finalizeRemoteLock(
              remoteLock,
              uniquePaths,
              uniquePaths,
              postSignatures,
            );
          } else if (remoteLock && uniquePaths.length) {
            await remoteLock.unlock(uniquePaths);
          }
          return {
            ok,
            signatures: remoteHost ? postSignatures : [],
            transferred: transferredPaths,
          };
        } catch (err) {
          if (remoteLock && uniquePaths.length) {
            try {
              await remoteLock.unlock(uniquePaths);
            } catch {}
          }
          throw err;
        }
      };

      // 1) delete file conflicts first
      done = t("rsync: 1) delete file conflicts");

      const delAlphaRes = await rsyncDeleteChunked(
        tmp,
        beta,
        alpha,
        delInAlpha,
        "alpha deleted (files)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: alphaTempArg,
          direction: "beta->alpha",
          captureDeletes: true,
        },
      );
      const confirmedAlphaDeletes = await confirmDeletions(
        "alpha",
        delInAlpha,
        delAlphaRes.deleted,
      );
      confirmedAlphaDeletes.forEach((p) => deletedFilesAlpha.add(p));
      if (confirmedAlphaDeletes.length) {
        mirrorFilesAndLinks("beta", "alpha", confirmedAlphaDeletes);
        await noteAlphaChange(confirmedAlphaDeletes);
      }
      const delBetaRes = await rsyncDeleteChunked(
        tmp,
        alpha,
        beta,
        delInBeta,
        "beta deleted (files)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: betaTempArg,
          direction: "alpha->beta",
          captureDeletes: true,
        },
      );
      const confirmedBetaDeletes = await confirmDeletions(
        "beta",
        delInBeta,
        delBetaRes.deleted,
      );
      confirmedBetaDeletes.forEach((p) => deletedFilesBeta.add(p));
      if (confirmedBetaDeletes.length) {
        mirrorFilesAndLinks("alpha", "beta", confirmedBetaDeletes);
        await noteBetaChange(confirmedBetaDeletes);
      }
      done();

      // 1b) dir→file cleanup
      done = t("rsync: 1b) dir→file cleanup");
      if (prefer === "beta" && preDeleteDirsOnAlphaForBetaFiles.length) {
        await rsyncDeleteChunked(
          tmp,
          alpha,
          alpha,
          preDeleteDirsOnAlphaForBetaFiles,
          "cleanup dir→file on alpha",
          {
            forceEmptySource: true,
            ...rsyncOpts,
            tempDir: alphaTempArg,
            direction: "alpha->alpha",
          },
        );
        await noteAlphaChange(preDeleteDirsOnAlphaForBetaFiles);
      }
      if (prefer === "alpha" && preDeleteDirsOnBetaForAlphaFiles.length) {
        await rsyncDeleteChunked(
          tmp,
          beta,
          beta,
          preDeleteDirsOnBetaForAlphaFiles,
          "cleanup dir→file on beta",
          {
            forceEmptySource: true,
            ...rsyncOpts,
            tempDir: betaTempArg,
            direction: "beta->beta",
          },
        );
        await noteBetaChange(preDeleteDirsOnBetaForAlphaFiles);
      }
      done();

      // 2) create dirs
      done = t("rsync: 2) create dirs");
      const alphaDirRes = await rsyncCopyDirs(
        alpha,
        beta,
        listToBetaDirs,
        "alpha→beta",
        {
          ...rsyncOpts,
          tempDir: betaTempArg,
          direction: "alpha->beta",
          captureDirs: true,
        },
      );
      copyDirsAlphaBetaOk = alphaDirRes.ok;
      if (alphaDirRes.transferred.length) {
        mirrorDirs("alpha", "beta", alphaDirRes.transferred);
        await noteBetaChange(alphaDirRes.transferred);
      }

      const betaDirRes = await rsyncCopyDirs(
        beta,
        alpha,
        listToAlphaDirs,
        "beta→alpha",
        {
          ...rsyncOpts,
          tempDir: alphaTempArg,
          direction: "beta->alpha",
          captureDirs: true,
        },
      );
      copyDirsBetaAlphaOk = betaDirRes.ok;
      if (betaDirRes.transferred.length) {
        mirrorDirs("beta", "alpha", betaDirRes.transferred);
        await noteAlphaChange(betaDirRes.transferred);
      }
      done();

      const canReflink =
        enableReflink &&
        !alphaHost &&
        !betaHost &&
        (await sameDevice(alphaRoot, betaRoot));
      if (canReflink) {
        done = t("cp: 3) copy files -- using copy on write, if possible");
        const betaReflink = await cpReflinkFromList(
          alphaRoot,
          betaRoot,
          listToBeta,
        );
        const alphaReflink = await cpReflinkFromList(
          betaRoot,
          alphaRoot,
          listToAlpha,
        );
        done();

        const betaCopied = betaReflink.copied.filter((rel) =>
          toBetaRelativeSet.has(rel),
        );
        if (betaCopied.length) {
          betaCopied.forEach((rel) => completedCopyToBeta.add(rel));
          mirrorFilesAndLinks("alpha", "beta", betaCopied);
          await noteBetaChange(betaCopied);
        }

        const alphaCopied = alphaReflink.copied.filter((rel) =>
          toAlphaRelativeSet.has(rel),
        );
        if (alphaCopied.length) {
          alphaCopied.forEach((rel) => completedCopyToAlpha.add(rel));
          mirrorFilesAndLinks("beta", "alpha", alphaCopied);
          await noteAlphaChange(alphaCopied);
        }

        const reflinkFailures = [
          ...betaReflink.failed.map((f) => ({
            direction: "alpha->beta" as const,
            ...f,
          })),
          ...alphaReflink.failed.map((f) => ({
            direction: "beta->alpha" as const,
            ...f,
          })),
        ];
        if (reflinkFailures.length) {
          logger.error("reflink copy incomplete", {
            failures: reflinkFailures.slice(0, debug ? undefined : 20),
            totalFailures: reflinkFailures.length,
          });
          const err = new Error("reflink partial transfer");
          (err as any).failures = reflinkFailures;
          throw err;
        }
      } else {
        // 3) copy files
        done = t("rsync: 3) copy files");
        const alphaCopyRes = await copyFilesWithGuards(
          "alpha->beta",
          toBetaRelative,
          {
            tempDir: betaTempArg,
            progressScope: "merge.copy.alpha->beta",
            progressMeta: {
              stage: "copy",
              direction: "alpha->beta",
            },
          },
        );
        const betaCopyRes = await copyFilesWithGuards(
          "beta->alpha",
          toAlphaRelative,
          {
            tempDir: alphaTempArg,
            progressScope: "merge.copy.beta->alpha",
            progressMeta: {
              stage: "copy",
              direction: "beta->alpha",
            },
          },
        );
        if (alphaCopyRes.transferred.length) {
          alphaCopyRes.transferred.forEach((rel) =>
            completedCopyToBeta.add(rel),
          );
          mirrorFilesAndLinks("alpha", "beta", alphaCopyRes.transferred);
          await noteBetaChange(alphaCopyRes.transferred);
        }
        if (betaCopyRes.transferred.length) {
          betaCopyRes.transferred.forEach((rel) =>
            completedCopyToAlpha.add(rel),
          );
          mirrorFilesAndLinks("beta", "alpha", betaCopyRes.transferred);
          await noteAlphaChange(betaCopyRes.transferred);
        }
        done();
      }

      // 4) delete dirs last (after files removed so dirs are empty)
      done = t("rsync: 4) delete dirs");
      if (prefer === "beta" && delDirsInBeta.length) {
        const isChangedB = db.prepare(
          `SELECT 1 AS one FROM tmp_dirs_changedB WHERE rpath = ?`,
        );
        const beforeN = delDirsInBeta.length;
        delDirsInBeta = delDirsInBeta.filter((d) => !isChangedB.get(d)?.one);
        if (debug && beforeN !== delDirsInBeta.length) {
          logger.warn("clamped dir deletions in beta due to prefer=beta", {
            kept: beforeN - delDirsInBeta.length,
          });
        }
      }
      if (prefer === "alpha" && delDirsInAlpha.length) {
        const isChangedA = db.prepare(
          `SELECT 1 AS one FROM tmp_dirs_changedA WHERE rpath = ?`,
        );
        const beforeN = delDirsInAlpha.length;
        delDirsInAlpha = delDirsInAlpha.filter((d) => !isChangedA.get(d)?.one);
        if (debug && beforeN !== delDirsInAlpha.length) {
          logger.warn("clamped dir deletions in alpha due to prefer=alpha", {
            kept: beforeN - delDirsInAlpha.length,
          });
        }
      }

      const dirDelBetaRes = await rsyncDeleteChunked(
        tmp,
        alpha,
        beta,
        delDirsInBeta,
        "beta deleted (dirs)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: betaTempArg,
          direction: "alpha->beta",
          captureDeletes: true,
        },
      );
      const confirmedDirDeletesBeta = await confirmDeletions(
        "beta",
        delDirsInBeta,
        dirDelBetaRes.deleted,
      );
      confirmedDirDeletesBeta.forEach((p) => deletedDirsBeta.add(p));
      if (confirmedDirDeletesBeta.length) {
        mirrorDirs("alpha", "beta", confirmedDirDeletesBeta);
        await noteBetaChange(confirmedDirDeletesBeta);
        if (!betaHost) {
          for (const rel of confirmedDirDeletesBeta) {
            try {
              await rm(path.join(betaRoot, rel), {
                recursive: true,
                force: true,
              });
            } catch {}
          }
        }
      }
      const dirBetaRes = await rsyncDeleteChunked(
        tmp,
        beta,
        alpha,
        delDirsInAlpha,
        "alpha deleted (dirs)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: alphaTempArg,
          direction: "beta->alpha",
          captureDeletes: true,
        },
      );
      const confirmedDirDeletesAlpha = await confirmDeletions(
        "alpha",
        delDirsInAlpha,
        dirBetaRes.deleted,
      );
      confirmedDirDeletesAlpha.forEach((p) => deletedDirsAlpha.add(p));
      if (confirmedDirDeletesAlpha.length) {
        mirrorDirs("beta", "alpha", confirmedDirDeletesAlpha);
        await noteAlphaChange(confirmedDirDeletesAlpha);
        if (!alphaHost) {
          for (const rel of confirmedDirDeletesAlpha) {
            try {
              await rm(path.join(alphaRoot, rel), {
                recursive: true,
                force: true,
              });
            } catch {}
          }
        }
      }
      done();

      logger.info("rsync complete, updating base database");

      if (dryRun) {
        logger.info("(dry-run) skipping base updates");
        logger.info("Merge complete.");
        return;
      }

      const completedToBeta = Array.from(completedCopyToBeta);
      const completedToAlpha = Array.from(completedCopyToAlpha);
      const completedDelBeta = Array.from(deletedFilesBeta);
      const completedDelAlpha = Array.from(deletedFilesAlpha);
      const completedDirDelBeta = Array.from(deletedDirsBeta);
      const completedDirDelAlpha = Array.from(deletedDirsAlpha);

      // ---------- set-based base updates (fast) ----------
      done = t("post rsync database update");
      db.exec(`
        DROP TABLE IF EXISTS plan_to_beta;
        DROP TABLE IF EXISTS plan_to_alpha;
        DROP TABLE IF EXISTS plan_del_beta;
        DROP TABLE IF EXISTS plan_del_alpha;

        DROP TABLE IF EXISTS plan_dirs_to_beta;
        DROP TABLE IF EXISTS plan_dirs_to_alpha;
        DROP TABLE IF EXISTS plan_dirs_del_beta;
        DROP TABLE IF EXISTS plan_dirs_del_alpha;

        CREATE TEMP TABLE plan_to_beta  (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_to_alpha (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_del_beta (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_del_alpha(rpath TEXT PRIMARY KEY) WITHOUT ROWID;

        CREATE TEMP TABLE plan_dirs_to_beta  (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_dirs_to_alpha (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_dirs_del_beta (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_dirs_del_alpha(rpath TEXT PRIMARY KEY) WITHOUT ROWID;

        CREATE TEMP TABLE plan_to_beta_done  (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_to_alpha_done (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_del_beta_done (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_del_alpha_done(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_dirs_del_beta_done (rpath TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE plan_dirs_del_alpha_done(rpath TEXT PRIMARY KEY) WITHOUT ROWID;
      `);

      function bulkInsert(table: string, rows: string[], chunk = 5000) {
        if (!rows.length) return;
        for (let i = 0; i < rows.length; i += chunk) {
          const slice = rows.slice(i, i + chunk);
          const placeholders = slice.map(() => "(?)").join(",");
          db.prepare(
            `INSERT OR IGNORE INTO ${table}(rpath) VALUES ${placeholders}`,
          ).run(...slice);
        }
      }

      function timed(label: string, fn: () => void) {
        if (!debug) return void fn();
        const t0 = Date.now();
        fn();
        const dt = Date.now() - t0;
        if (dt > 10) logger.debug(`[plan] ${label}`, { durationMs: dt });
      }

      timed("insert plan tables", () => {
        const tx = db.transaction(() => {
          bulkInsert("plan_to_beta", toBeta);
          bulkInsert("plan_to_alpha", toAlpha);
          bulkInsert("plan_del_beta", delInBeta);
          bulkInsert("plan_del_alpha", delInAlpha);

          bulkInsert("plan_dirs_to_beta", toBetaDirs);
          bulkInsert("plan_dirs_to_alpha", toAlphaDirs);
          bulkInsert("plan_dirs_del_beta", delDirsInBeta);
          bulkInsert("plan_dirs_del_alpha", delDirsInAlpha);
        });
        tx();
      });

      timed("insert success tables", () => {
        const tx = db.transaction(() => {
          bulkInsert("plan_to_beta_done", completedToBeta);
          bulkInsert("plan_to_alpha_done", completedToAlpha);
          bulkInsert("plan_del_beta_done", completedDelBeta);
          bulkInsert("plan_del_alpha_done", completedDelAlpha);
          bulkInsert("plan_dirs_del_beta_done", completedDirDelBeta);
          bulkInsert("plan_dirs_del_alpha_done", completedDirDelAlpha);
        });
        tx();
      });

      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_plan_to_beta_rpath        ON plan_to_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_to_alpha_rpath       ON plan_to_alpha(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_del_beta_rpath       ON plan_del_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_del_alpha_rpath      ON plan_del_alpha(rpath);

        CREATE INDEX IF NOT EXISTS idx_plan_dirs_to_beta_rpath   ON plan_dirs_to_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_to_alpha_rpath  ON plan_dirs_to_alpha(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_beta_rpath  ON plan_dirs_del_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_alpha_rpath ON plan_dirs_del_alpha(rpath);

        CREATE INDEX IF NOT EXISTS idx_plan_to_beta_done_rpath   ON plan_to_beta_done(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_to_alpha_done_rpath  ON plan_to_alpha_done(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_del_beta_done_rpath  ON plan_del_beta_done(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_del_alpha_done_rpath ON plan_del_alpha_done(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_beta_done_rpath  ON plan_dirs_del_beta_done(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_alpha_done_rpath ON plan_dirs_del_alpha_done(rpath);
      `);

      if (debug) {
        const c = (t: string) =>
          (db.prepare(`SELECT COUNT(*) n FROM ${t}`).get() as any).n;
        logger.debug("plan table counts", {
          to_beta: c("plan_to_beta"),
          to_alpha: c("plan_to_alpha"),
          del_beta: c("plan_del_beta"),
          del_alpha: c("plan_del_alpha"),
          dirs_to_beta: c("plan_dirs_to_beta"),
          dirs_to_alpha: c("plan_dirs_to_alpha"),
          dirs_del_beta: c("plan_dirs_del_beta"),
          dirs_del_alpha: c("plan_dirs_del_alpha"),
          done_to_beta: c("plan_to_beta_done"),
          done_to_alpha: c("plan_to_alpha_done"),
          del_beta_done: c("plan_del_beta_done"),
          del_alpha_done: c("plan_del_alpha_done"),
          dirs_del_beta_done: c("plan_dirs_del_beta_done"),
          dirs_del_alpha_done: c("plan_dirs_del_alpha_done"),
        });
      }

      // set-based updates:
      // -- copy to base for every path that was successfully transferred, as
      //    reported by rsync.
      // -- preserve op_ts of chosen side
      db.transaction(() => {
        if (completedToBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT d.rpath, a.hash, 0, a.op_ts
            FROM plan_to_beta_done d
            JOIN alpha_rel a USING (rpath);
          `);
        }
        if (completedToAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT d.rpath, b.hash, 0, b.op_ts
            FROM plan_to_alpha_done d
            JOIN beta_rel b USING (rpath);
          `);
        }
        if (completedDelBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT d.rpath, NULL, 1, COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_beta_done d
            LEFT JOIN alpha_rel a USING (rpath)
            LEFT JOIN beta_rel  b USING (rpath)
            LEFT JOIN base_rel  br USING (rpath);
          `);
        }
        if (completedDelAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT d.rpath, NULL, 1, COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_alpha_done d
            LEFT JOIN beta_rel  b USING (rpath)
            LEFT JOIN alpha_rel a USING (rpath)
            LEFT JOIN base_rel  br USING (rpath);
          `);
        }

        if (copyDirsAlphaBetaOk && toBetaDirs.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts, hash)
            SELECT p.rpath, 0, a.op_ts, a.hash
            FROM plan_dirs_to_beta p
            JOIN alpha_dirs_rel a USING (rpath);
          `);
        }
        if (copyDirsBetaAlphaOk && toAlphaDirs.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts, hash)
            SELECT p.rpath, 0, b.op_ts, b.hash
            FROM plan_dirs_to_alpha p
            JOIN beta_dirs_rel b USING (rpath);
          `);
        }
        if (completedDirDelBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts, hash)
            SELECT d.rpath, 1, COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000), ''
            FROM plan_dirs_del_beta_done d
            LEFT JOIN alpha_dirs_rel a USING (rpath)
            LEFT JOIN beta_dirs_rel  b USING (rpath)
            LEFT JOIN base_dirs_rel  br USING (rpath);
          `);
        }
        if (completedDirDelAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts, hash)
            SELECT d.rpath, 1, COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000), ''
            FROM plan_dirs_del_alpha_done d
            LEFT JOIN beta_dirs_rel  b USING (rpath)
            LEFT JOIN alpha_dirs_rel a USING (rpath)
            LEFT JOIN base_dirs_rel  br USING (rpath);
          `);
        }
      })();
      done();

      // ---------- prune tombstones ----------
      done = t("drop tombstones in alpha and beta");
      db.exec(`
        DELETE FROM alpha.files
        WHERE deleted = 1
          AND EXISTS (SELECT 1 FROM base WHERE base.path = alpha.files.path AND base.deleted = 1);

        DELETE FROM beta.files
        WHERE deleted = 1
          AND EXISTS (SELECT 1 FROM base WHERE base.path = beta.files.path AND base.deleted = 1);

        DELETE FROM alpha.dirs
        WHERE deleted = 1
          AND EXISTS (SELECT 1 FROM base_dirs WHERE base_dirs.path = alpha.dirs.path AND base_dirs.deleted = 1);

        DELETE FROM beta.dirs
        WHERE deleted = 1
          AND EXISTS (SELECT 1 FROM base_dirs WHERE base_dirs.path = beta.dirs.path AND base_dirs.deleted = 1);

        DELETE FROM alpha.links
        WHERE deleted = 1
          AND EXISTS (SELECT 1 FROM base WHERE base.path = alpha.links.path AND base.deleted = 1);

        DELETE FROM beta.links
        WHERE deleted = 1
          AND EXISTS (SELECT 1 FROM base WHERE base.path = beta.links.path AND base.deleted = 1);
      `);
      done();

      done = t("drop paths in base in neither alpha nor beta");
      db.exec(`
        -- delete anything deleted in base that doesn't exist in either alpha or beta
        -- Materialize "live" sets without touching the ranked views
        DROP TABLE IF EXISTS _live_paths;
        CREATE TEMP TABLE _live_paths(
          path TEXT PRIMARY KEY
        ) WITHOUT ROWID;

        INSERT INTO _live_paths(path)
        SELECT path FROM alpha.files WHERE deleted=0
        UNION
        SELECT path FROM alpha.links WHERE deleted=0
        UNION
        SELECT path FROM beta.files  WHERE deleted=0
        UNION
        SELECT path FROM beta.links  WHERE deleted=0;

        DROP TABLE IF EXISTS _live_dir_paths;
        CREATE TEMP TABLE _live_dir_paths(
          path TEXT PRIMARY KEY
        ) WITHOUT ROWID;

        INSERT INTO _live_dir_paths(path)
        SELECT path FROM alpha.dirs WHERE deleted=0
        UNION
        SELECT path FROM beta.dirs  WHERE deleted=0;

        -- 4) Prune base tables using the materialized sets
        DELETE FROM base
         WHERE NOT EXISTS (SELECT 1 FROM _live_paths lp WHERE lp.path = base.path);

        DELETE FROM base_dirs
         WHERE NOT EXISTS (SELECT 1 FROM _live_dir_paths lp WHERE lp.path = base_dirs.path);
      `);
      done();

      try {
        done = t("sqlite hygiene");
        db.pragma("optimize");
        db.pragma("wal_checkpoint(TRUNCATE)");
        db.pragma("incremental_vacuum");
        db.pragma("optimize");
        done();
        // for now do a more expensive full vacuum;
        // this can save a LOT more space than incremental vacuum
        done = t("sqlite full vacuum");
        db.exec("vacuum; vacuum alpha; vacuum beta;");
        done();
      } catch (err) {
        // harmless if fails -- may happen if locked
        logger.warn("sqlite hygiene issue", { error: String(err) });
      }

      logger.info("Merge complete.");

      // Persist digests (after successful non-dry-run merge)
      const up = db.prepare(`REPLACE INTO merge_meta(key,value) VALUES(?,?)`);
      up.run("alpha_digest", digestAlpha);
      up.run("beta_digest", digestBeta);
    } finally {
      if (!LEAVE_TEMP_FILES) {
        await rm(tmp, { recursive: true, force: true });
      }
    }
  }

  // files = (now) RELATIVE rpaths; keep this to be resilient if a caller passes absolute paths later
  function makeRelative(files: string[], root: string) {
    return files.map((file) =>
      file.startsWith(root + "/") ? file.slice(root.length + 1) : file,
    );
  }

  await main();
}

cliEntrypoint<MergeRsyncOptions>(
  import.meta.url,
  buildProgram,
  async (opts) => {
    await runMerge(normalizeMergeCliOptions(opts));
  },
  {
    label: "merge",
  },
);
