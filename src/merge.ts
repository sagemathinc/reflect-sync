#!/usr/bin/env node

// merge.ts
import { tmpdir } from "node:os";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
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
import {
  getRecentSendSignatures,
  signatureEquals,
  signatureFromStamp,
} from "./recent-send.js";
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

function buildProgram(): Command {
  const program = new Command();
  return program
    .command(`${CLI_NAME}-merge`)
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
    );
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
  markAlphaToBeta?: (paths: string[]) => Promise<void> | void;
  markBetaToAlpha?: (paths: string[]) => Promise<void> | void;
};

// ---------- helpers ----------
function join0(items: string[]) {
  const filtered = items.filter(Boolean);
  return filtered.length
    ? Buffer.from(filtered.join("\0") + "\0")
    : Buffer.alloc(0);
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
  markAlphaToBeta,
  markBetaToAlpha,
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

  let alphaTempDir: string | undefined;
  let betaTempDir: string | undefined;
  let alphaTempArg: string | undefined;
  let betaTempArg: string | undefined;

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
      DROP VIEW IF EXISTS all_paths;
      DROP VIEW IF EXISTS coalesced_pairs;
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
        SELECT path, hash, deleted, op_ts FROM alpha.files
        UNION ALL
        SELECT path, hash, deleted, op_ts FROM alpha.links
      ),
      ranked AS (
        SELECT path, hash, deleted, op_ts,
               ROW_NUMBER() OVER (
                 PARTITION BY path
                 ORDER BY
                   deleted ASC,  -- prefer non-deleted
                   op_ts  DESC   -- then newest op_ts
               ) AS rn
        FROM unioned
      )
      SELECT path, hash, deleted, op_ts
      FROM ranked
      WHERE rn = 1;

      -- =========================
      -- beta_entries (ranked)
      -- =========================
      CREATE TEMP VIEW beta_entries AS
      WITH unioned AS (
        SELECT path, hash, deleted, op_ts FROM beta.files
        UNION ALL
        SELECT path, hash, deleted, op_ts FROM beta.links
      ),
      ranked AS (
        SELECT path, hash, deleted, op_ts,
               ROW_NUMBER() OVER (
                 PARTITION BY path
                 ORDER BY
                   deleted ASC,
                   op_ts  DESC
               ) AS rn
        FROM unioned
      )
      SELECT path, hash, deleted, op_ts
      FROM ranked
      WHERE rn = 1;

      -- =========================
      -- Superset of paths
      -- =========================
      CREATE TEMP VIEW all_paths AS
      SELECT path FROM alpha_entries
      UNION
      SELECT path FROM beta_entries;
    `);

    db.exec(
      `DROP TABLE IF EXISTS alpha_rel; DROP TABLE IF EXISTS beta_rel; DROP TABLE IF EXISTS base_rel;`,
    );

    done = t("build *_rel");

    db.exec(`
      CREATE TEMP TABLE alpha_rel(
        rpath   TEXT PRIMARY KEY,
        hash    TEXT,
        deleted INTEGER,
        op_ts   INTEGER
      ) WITHOUT ROWID;

      CREATE TEMP TABLE beta_rel(
        rpath   TEXT PRIMARY KEY,
        hash    TEXT,
        deleted INTEGER,
        op_ts   INTEGER
      ) WITHOUT ROWID;

      CREATE TEMP TABLE base_rel(
        rpath   TEXT PRIMARY KEY,
        hash    TEXT,
        deleted INTEGER,
        op_ts   INTEGER
      ) WITHOUT ROWID;
    `);

    db.exec(`
      INSERT INTO alpha_rel(rpath,hash,deleted,op_ts)
      SELECT path, hash, deleted, op_ts FROM alpha_entries;

      INSERT INTO beta_rel(rpath,hash,deleted,op_ts)
      SELECT path, hash, deleted, op_ts FROM beta_entries;

      INSERT INTO base_rel(rpath,hash,deleted,op_ts)
      SELECT path, hash, deleted, op_ts FROM base;
    `);

    // Dirs (RELATIVE)
    db.exec(`
      DROP TABLE IF EXISTS alpha_dirs_rel; DROP TABLE IF EXISTS beta_dirs_rel; DROP TABLE IF EXISTS base_dirs_rel;

      CREATE TEMP TABLE alpha_dirs_rel(
        rpath   TEXT PRIMARY KEY,
        deleted INTEGER,
        op_ts   INTEGER,
        hash    TEXT
      ) WITHOUT ROWID;

      CREATE TEMP TABLE beta_dirs_rel(
        rpath   TEXT PRIMARY KEY,
        deleted INTEGER,
        op_ts   INTEGER,
        hash    TEXT
      ) WITHOUT ROWID;

      CREATE TEMP TABLE base_dirs_rel(
        rpath   TEXT PRIMARY KEY,
        deleted INTEGER,
        op_ts   INTEGER,
        hash    TEXT
      ) WITHOUT ROWID;

      INSERT INTO alpha_dirs_rel(rpath,deleted,op_ts,hash)
      SELECT path, deleted, op_ts, COALESCE(hash,'') FROM alpha.dirs;

      INSERT INTO beta_dirs_rel(rpath,deleted,op_ts,hash)
      SELECT path, deleted, op_ts, COALESCE(hash,'') FROM beta.dirs;

      INSERT INTO base_dirs_rel(rpath,deleted,op_ts,hash)
      SELECT path, deleted, op_ts, COALESCE(hash,'') FROM base_dirs;
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
        WHERE a.op_ts > b.op_ts
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
        WHERE b.op_ts > a.op_ts
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
        WHERE a.op_ts = b.op_ts
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

    const delInBeta_noConflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_deletedA dA
        LEFT JOIN tmp_changedB cB USING (rpath)
        WHERE cB.rpath IS NULL
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

    const deletedOnlyInBeta = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_deletedB dB
        LEFT JOIN tmp_changedA cA USING (rpath)
        WHERE cA.rpath IS NULL
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

    const alphaBounceCandidates = uniq([
      ...toBeta,
      ...toBetaDirs,
    ]);
    const betaBounceCandidates = uniq([
      ...toAlpha,
      ...toAlphaDirs,
    ]);

    const skipAlphaToBeta = new Set<string>();
    if (alphaBounceCandidates.length) {
      const stamps = fetchOpStamps(alphaDb, alphaBounceCandidates);
      const recent = getRecentSendSignatures(
        alphaDb,
        "beta->alpha",
        alphaBounceCandidates,
      );
      for (const path of alphaBounceCandidates) {
        const sig = signatureFromStamp(stamps.get(path));
        const last = recent.get(path);
        if (sig && last && signatureEquals(sig, last)) {
          skipAlphaToBeta.add(path);
        }
      }
    }

    const skipBetaToAlpha = new Set<string>();
    if (betaBounceCandidates.length) {
      const stamps = fetchOpStamps(betaDb, betaBounceCandidates);
      const recent = getRecentSendSignatures(
        betaDb,
        "alpha->beta",
        betaBounceCandidates,
      );
      for (const path of betaBounceCandidates) {
        const sig = signatureFromStamp(stamps.get(path));
        const last = recent.get(path);
        if (sig && last && signatureEquals(sig, last)) {
          skipBetaToAlpha.add(path);
        }
      }
    }

    const filterBounce = (paths: string[], skip: Set<string>) =>
      paths.filter((p) => !skip.has(p));

    toBeta = filterBounce(toBeta, skipAlphaToBeta);
    toBetaDirs = filterBounce(toBetaDirs, skipAlphaToBeta);

    toAlpha = filterBounce(toAlpha, skipBetaToAlpha);
    toAlphaDirs = filterBounce(toAlphaDirs, skipBetaToAlpha);

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

      const toBetaRelative = makeRelative(toBeta, betaRoot);
      const toAlphaRelative = makeRelative(toAlpha, alphaRoot);

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

      // ---------- rsync ----------
      let copyAlphaBetaOk = false;
      let copyBetaAlphaOk = false;
      let copyDirsAlphaBetaOk = false;
      let copyDirsBetaAlphaOk = false;

      const alpha = alphaHost ? `${alphaHost}:${alphaRoot}` : alphaRoot;
      const beta = betaHost ? `${betaHost}:${betaRoot}` : betaRoot;

      // 1) delete file conflicts first
      done = t("rsync: 1) delete file conflicts");

      await rsyncDeleteChunked(
        tmp,
        beta,
        alpha,
        delInAlpha,
        "alpha deleted (files)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: alphaTempArg,
        },
      );
      if (delInAlpha.length && markBetaToAlpha) {
        await markBetaToAlpha(delInAlpha);
      }
      await rsyncDeleteChunked(
        tmp,
        alpha,
        beta,
        delInBeta,
        "beta deleted (files)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: betaTempArg,
        },
      );
      if (delInBeta.length && markAlphaToBeta) {
        await markAlphaToBeta(delInBeta);
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
          },
        );
        if (markBetaToAlpha) {
          await markBetaToAlpha(preDeleteDirsOnAlphaForBetaFiles);
        }
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
          },
        );
        if (markAlphaToBeta) {
          await markAlphaToBeta(preDeleteDirsOnBetaForAlphaFiles);
        }
      }
      done();

      // 2) create dirs
      done = t("rsync: 2) create dirs");
      copyDirsAlphaBetaOk = (
        await rsyncCopyDirs(
          alpha,
          beta,
          listToBetaDirs,
          "alpha→beta",
          {
            ...rsyncOpts,
            tempDir: betaTempArg,
          },
        )
      ).ok;
      copyDirsBetaAlphaOk = (
        await rsyncCopyDirs(
          beta,
          alpha,
          listToAlphaDirs,
          "beta→alpha",
          {
            ...rsyncOpts,
            tempDir: alphaTempArg,
          },
        )
      ).ok;
      if (copyDirsAlphaBetaOk && toBetaDirs.length && markAlphaToBeta) {
        await markAlphaToBeta(toBetaDirs);
      }
      if (copyDirsBetaAlphaOk && toAlphaDirs.length && markBetaToAlpha) {
        await markBetaToAlpha(toAlphaDirs);
      }
      done();

      const canReflink =
        !alphaHost && !betaHost && (await sameDevice(alphaRoot, betaRoot));
      if (canReflink) {
        done = t("cp: 3) copy files -- using copy on write, if possible");
        try {
          await cpReflinkFromList(alphaRoot, betaRoot, listToBeta);
          await cpReflinkFromList(betaRoot, alphaRoot, listToAlpha);
          copyAlphaBetaOk = toBeta.length === 0 || true;
          copyBetaAlphaOk = toAlpha.length === 0 || true;
          if (toBetaRelative.length && markAlphaToBeta) {
            await markAlphaToBeta(toBetaRelative);
          }
          if (toAlphaRelative.length && markBetaToAlpha) {
            await markBetaToAlpha(toAlphaRelative);
          }
          done();
        } catch (e) {
          // Fallback: if any cp --reflink failed (e.g., subtrees on different filesystems),
          // fall back to your current rsync path for the affected direction(s).
          if (debug) {
            logger.warn("reflink copy failed; falling back to rsync", {
              error: String(e),
            });
          }
          done();
          done = t("rsync: 3) copy files -- falling back to rsync");
          copyAlphaBetaOk = (
            await rsyncCopyChunked(
              tmp,
              alpha,
              beta,
              toBetaRelative,
              "alpha→beta",
              {
                ...rsyncOpts,
                tempDir: betaTempArg,
                progressScope: "merge.copy.alpha->beta",
                progressMeta: {
                  stage: "copy",
                  direction: "alpha->beta",
                },
              },
            )
          ).ok;
          copyBetaAlphaOk = (
            await rsyncCopyChunked(
              tmp,
              beta,
              alpha,
              toAlphaRelative,
              "beta→alpha",
              {
                ...rsyncOpts,
                tempDir: alphaTempArg,
                progressScope: "merge.copy.beta->alpha",
                progressMeta: {
                  stage: "copy",
                  direction: "beta->alpha",
                },
              },
            )
          ).ok;
          if (copyAlphaBetaOk && toBetaRelative.length && markAlphaToBeta) {
            await markAlphaToBeta(toBetaRelative);
          }
          if (copyBetaAlphaOk && toAlphaRelative.length && markBetaToAlpha) {
            await markBetaToAlpha(toAlphaRelative);
          }
        }
      } else {
        // 3) copy files
        done = t("rsync: 3) copy files");
        copyAlphaBetaOk = (
          await rsyncCopyChunked(
            tmp,
            alpha,
            beta,
            toBetaRelative,
            "alpha→beta",
            {
              ...rsyncOpts,
              tempDir: betaTempArg,
              progressScope: "merge.copy.alpha->beta",
              progressMeta: {
                stage: "copy",
                direction: "alpha->beta",
              },
            },
          )
        ).ok;
        copyBetaAlphaOk = (
          await rsyncCopyChunked(
            tmp,
            beta,
            alpha,
            toAlphaRelative,
            "beta→alpha",
            {
              ...rsyncOpts,
              tempDir: alphaTempArg,
              progressScope: "merge.copy.beta->alpha",
              progressMeta: {
                stage: "copy",
                direction: "beta->alpha",
              },
            },
          )
        ).ok;
        if (copyAlphaBetaOk && toBetaRelative.length && markAlphaToBeta) {
          await markAlphaToBeta(toBetaRelative);
        }
        if (copyBetaAlphaOk && toAlphaRelative.length && markBetaToAlpha) {
          await markBetaToAlpha(toAlphaRelative);
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

      await rsyncDeleteChunked(
        tmp,
        alpha,
        beta,
        delDirsInBeta,
        "beta deleted (dirs)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: betaTempArg,
        },
      );
      if (delDirsInBeta.length) {
        if (markAlphaToBeta) {
          await markAlphaToBeta(delDirsInBeta);
        }
      }
      await rsyncDeleteChunked(
        tmp,
        beta,
        alpha,
        delDirsInAlpha,
        "alpha deleted (dirs)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
          tempDir: alphaTempArg,
        },
      );
      if (delDirsInAlpha.length) {
        markBetaToAlpha?.(delDirsInAlpha);
      }
      done();

      logger.info("rsync complete, updating base database");

      if (dryRun) {
        logger.info("(dry-run) skipping base updates");
        logger.info("Merge complete.");
        return;
      }

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

      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_plan_to_beta_rpath        ON plan_to_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_to_alpha_rpath       ON plan_to_alpha(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_del_beta_rpath       ON plan_del_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_del_alpha_rpath      ON plan_del_alpha(rpath);

        CREATE INDEX IF NOT EXISTS idx_plan_dirs_to_beta_rpath   ON plan_dirs_to_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_to_alpha_rpath  ON plan_dirs_to_alpha(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_beta_rpath  ON plan_dirs_del_beta(rpath);
        CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_alpha_rpath ON plan_dirs_del_alpha(rpath);
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
        });
      }

      // set-based updates (preserve op_ts of chosen side); gate on rsync success
      db.transaction(() => {
        if (copyAlphaBetaOk && toBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, a.hash, 0, a.op_ts
            FROM plan_to_beta p
            JOIN alpha_rel a USING (rpath);
          `);
        }
        if (copyBetaAlphaOk && toAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, b.hash, 0, b.op_ts
            FROM plan_to_alpha p
            JOIN beta_rel b USING (rpath);
          `);
        }
        if (delInBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, NULL, 1, COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_beta p
            LEFT JOIN alpha_rel a USING (rpath)
            LEFT JOIN beta_rel  b USING (rpath)
            LEFT JOIN base_rel  br USING (rpath);
          `);
        }
        if (delInAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, NULL, 1, COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_alpha p
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
        if (delDirsInBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts, hash)
            SELECT p.rpath, 1, COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000), ''
            FROM plan_dirs_del_beta p
            LEFT JOIN alpha_dirs_rel a USING (rpath)
            LEFT JOIN beta_dirs_rel  b USING (rpath)
            LEFT JOIN base_dirs_rel  br USING (rpath);
          `);
        }
        if (delDirsInAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts, hash)
            SELECT p.rpath, 1, COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000), ''
            FROM plan_dirs_del_alpha p
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

cliEntrypoint<MergeRsyncOptions>(import.meta.url, buildProgram, runMerge, {
  label: "merge",
});
