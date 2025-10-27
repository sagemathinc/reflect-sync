#!/usr/bin/env node

// merge.ts
import { tmpdir } from "node:os";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { Command, Option } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import { getDb } from "./db.js";
import Database from "better-sqlite3";
import { loadIgnoreFile, filterIgnored, filterIgnoredDirs } from "./ignore.js";
import { IGNORE_FILE } from "./constants.js";
import { rsyncCopy, rsyncCopyDirs, rsyncDeleteChunked } from "./rsync.js";
import { xxh3 } from "@node-rs/xxhash";
import { hex128 } from "./hash.js";

// set to true for debugging
const LEAVE_TEMP_FILES = false;

function buildProgram(): Command {
  const program = new Command();
  return program
    .command("ccsync-merge-rsync")
    .description("3-way plan + rsync between alpha/beta; updates base snapshot")
    .requiredOption("--alpha-root <path>", "alpha filesystem root")
    .requiredOption("--beta-root <path>", "beta filesystem root")
    .option("--alpha-db <path>", "alpha sqlite", "alpha.db")
    .option("--beta-db <path>", "beta sqlite", "beta.db")
    .option("--base-db <path>", "base sqlite", "base.db")
    .option("--alpha-host <ssh>", "SSH host for alpha (e.g. user@host)")
    .option("--beta-host <ssh>", "SSH host for beta (e.g. user@host)")
    .addOption(
      new Option("--prefer <side>", "conflict winner")
        .choices(["alpha", "beta"])
        .default("alpha"),
    )
    .option("--lww-epsilon-ms <ms>", "LWW tie epsilon in ms", "3000")
    .option("--dry-run", "simulate without changing files", false)
    .option("--verbose", "enable verbose logging", false);
}

type MergeRsyncOptions = {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  alphaHost?: string;
  betaHost?: string;
  prefer: "alpha" | "beta";
  lwwEpsilonMs: string;
  dryRun: boolean | string;
  verbose: boolean | string;
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
  betaHost,
  prefer,
  lwwEpsilonMs,
  dryRun,
  verbose,
}: MergeRsyncOptions) {
  const EPS = Number(lwwEpsilonMs || "3000") || 3000;
  const rsyncOpts = { dryRun, verbose };

  const alphaIg = await loadIgnoreFile(alphaRoot);
  const betaIg = await loadIgnoreFile(betaRoot);

  const t = (label: string) => {
    if (!verbose) return () => {};
    console.log(`[phase] ${label}: running...`);
    const t0 = Date.now();
    return () => {
      const dt = Date.now() - t0;
      if (dt > 20) console.log(`[phase] ${label}: ${dt} ms`);
    };
  };

  // small helpers used by safety rails
  const asSet = (xs: string[]) => new Set(xs);
  const uniq = (xs: string[]) => Array.from(asSet(xs));
  const depth = (r: string) => (r ? r.split("/").length : 0);
  const sortDeepestFirst = (xs: string[]) =>
    xs.slice().sort((a, b) => depth(b) - depth(a));
  const nonRoot = (xs: string[]) => xs.filter((r) => r && r !== ".");

  async function main() {
    // ---------- DB ----------
    // ensure alpha/beta exist (creates schema if missing)
    getDb(alphaDb);
    getDb(betaDb);

    const db = new Database(baseDb);
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
    db.pragma("temp_store = MEMORY"); // keep temp tables in RAM for speed

    // base (files) and base_dirs (directories) — relative paths; include op_ts for LWW
    db.exec(`
      CREATE TABLE IF NOT EXISTS base (
        path    TEXT PRIMARY KEY,  -- RELATIVE file path
        hash    TEXT,
        deleted INTEGER DEFAULT 0,
        op_ts   INTEGER
      );
      CREATE TABLE IF NOT EXISTS base_dirs (
        path    TEXT PRIMARY KEY,  -- RELATIVE dir path
        deleted INTEGER DEFAULT 0,
        op_ts   INTEGER,
        hash    TEXT DEFAULT ''
      );
    `);

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
          SELECT path, hash FROM ${side}.files WHERE deleted = 0
          UNION ALL
          SELECT path, hash FROM ${side}.links WHERE deleted = 0
          UNION ALL
          SELECT path, hash FROM ${side}.dirs  WHERE deleted = 0
        )
        ORDER BY path
      `);
      const h = xxh3.Xxh3.withSeed();
      for (const row of stmt.iterate()) {
        h.update(`${row.path}\x1f${row.hash}\x1f`);
      }
      return hex128(h.digest());
    };

    let done = t("compute digests");
    const digestAlpha = computeSideDigest("alpha");
    const digestBeta = computeSideDigest("beta");
    if (verbose) {
      console.log("[digest] alpha:", digestAlpha);
      console.log("[digest] beta :", digestBeta);
    }
    done();

    const lastAlpha =
      (
        db
          .prepare(`SELECT value FROM merge_meta WHERE key='alpha_digest'`)
          .get() as any
      )?.value ?? null;
    const lastBeta =
      (
        db
          .prepare(`SELECT value FROM merge_meta WHERE key='beta_digest'`)
          .get() as any
      )?.value ?? null;

    if (
      lastAlpha !== null &&
      lastBeta !== null &&
      digestAlpha === lastAlpha &&
      digestBeta === lastBeta
    ) {
      if (verbose)
        console.log(
          "[merge] no-op: catalogs unchanged; skipping planning/rsync",
        );
      return;
    }
    // ---------- END EARLY-OUT FAST PATH ----------

    // ---------- Build rel tables (already RELATIVE paths in alpha/beta) ----------
    db.exec(
      `DROP VIEW IF EXISTS alpha_entries; DROP VIEW IF EXISTS beta_entries;`,
    );
    db.exec(`
      CREATE TEMP VIEW alpha_entries AS
        SELECT path, hash, deleted, op_ts FROM alpha.files
        UNION ALL
        SELECT path, hash, deleted, op_ts FROM alpha.links;
      CREATE TEMP VIEW beta_entries AS
        SELECT path, hash, deleted, op_ts FROM beta.files
        UNION ALL
        SELECT path, hash, deleted, op_ts FROM beta.links;
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
    if (verbose) {
      console.log(
        `Planner input counts: changedA=${count("tmp_changedA")} changedB=${count("tmp_changedB")} deletedA=${count("tmp_deletedA")} deletedB=${count("tmp_deletedB")}`,
      );
      console.log(
        `Planner dir counts  : d_changedA=${count("tmp_dirs_changedA")} d_changedB=${count("tmp_dirs_changedB")} d_deletedA=${count("tmp_dirs_deletedA")} d_deletedB=${count("tmp_dirs_deletedB")}`,
      );
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
        WHERE a.op_ts > b.op_ts + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothChangedToAlpha = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_changedA cA
        JOIN tmp_changedB cB USING (rpath)
        JOIN alpha_rel a USING (rpath)
        JOIN beta_rel  b USING (rpath)
        WHERE b.op_ts > a.op_ts + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothChangedTie = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_changedA cA
        JOIN tmp_changedB cB USING (rpath)
        JOIN alpha_rel a USING (rpath)
        JOIN beta_rel  b USING (rpath)
        WHERE ABS(a.op_ts - b.op_ts) <= ?
      `,
      )
      .all(EPS)
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
        WHERE COALESCE(a.op_ts, br.op_ts, 0) > COALESCE(b.op_ts, 0) + ?
           OR (ABS(COALESCE(a.op_ts, br.op_ts, 0) - COALESCE(b.op_ts, 0)) <= ? AND ? = 'alpha')
      `,
      )
      .all(EPS, EPS, prefer)
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
        WHERE COALESCE(b.op_ts, 0) > COALESCE(a.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(b.op_ts, 0) - COALESCE(a.op_ts, br.op_ts, 0)) <= ? AND ? = 'beta')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    let delInBeta = uniq([...delInBeta_noConflict, ...delInBeta_conflict]);
    toAlpha = uniq([...toAlpha, ...toAlpha_conflict]);

    const delInAlpha_noConflict = db
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
        WHERE COALESCE(b.op_ts, br.op_ts, 0) > COALESCE(a.op_ts, 0) + ?
           OR (ABS(COALESCE(b.op_ts, br.op_ts, 0) - COALESCE(a.op_ts, 0)) <= ? AND ? = 'beta')
      `,
      )
      .all(EPS, EPS, prefer)
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
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(a.op_ts, 0) - COALESCE(b.op_ts, br.op_ts, 0)) <= ? AND ? = 'alpha')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    let delInAlpha = uniq([...delInAlpha_noConflict, ...delInAlpha_conflict]);
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
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, 0) + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothDirsToAlpha = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_dirs_changedA cA
        JOIN tmp_dirs_changedB cB USING (rpath)
        JOIN alpha_dirs_rel a USING (rpath)
        JOIN beta_dirs_rel  b USING (rpath)
        WHERE COALESCE(b.op_ts,0) > COALESCE(a.op_ts,0) + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothDirsTie = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_dirs_changedA cA
        JOIN tmp_dirs_changedB cB USING (rpath)
        JOIN alpha_dirs_rel a USING (rpath)
        JOIN beta_dirs_rel  b USING (rpath)
        WHERE ABS(COALESCE(a.op_ts,0) - COALESCE(b.op_ts,0)) <= ?
      `,
      )
      .all(EPS)
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

    const delDirsInAlpha_noConflict = db
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
        WHERE COALESCE(a.op_ts, br.op_ts, 0) > COALESCE(b.op_ts, 0) + ?
           OR (ABS(COALESCE(a.op_ts, br.op_ts, 0) - COALESCE(b.op_ts, 0)) <= ? AND ? = 'alpha')
      `,
      )
      .all(EPS, EPS, prefer)
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
        WHERE COALESCE(b.op_ts, 0) > COALESCE(a.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(b.op_ts, 0) - COALESCE(a.op_ts, br.op_ts, 0)) <= ? AND ? = 'beta')
      `,
      )
      .all(EPS, EPS, prefer)
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
        WHERE COALESCE(b.op_ts, br.op_ts, 0) > COALESCE(a.op_ts, 0) + ?
           OR (ABS(COALESCE(b.op_ts, br.op_ts, 0) - COALESCE(a.op_ts, 0)) <= ? AND ? = 'beta')
      `,
      )
      .all(EPS, EPS, prefer)
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
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(a.op_ts, 0) - COALESCE(b.op_ts, br.op_ts, 0)) <= ? AND ? = 'alpha')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    let delDirsInBeta = uniq([
      ...delDirsInBeta_noConflict,
      ...delDirsInBeta_conflict,
    ]);
    let delDirsInAlpha = uniq([
      ...delDirsInAlpha_noConflict,
      ...delDirsInAlpha_conflict,
    ]);
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

    function makePrefixChecker(dirs: string[]) {
      const ps = dirs
        .map((d) => (d.endsWith("/") ? d.slice(0, -1) : d))
        .filter((d) => d && d !== ".")
        .sort((a, b) => b.length - a.length);
      return (p: string) => {
        for (const pre of ps) {
          if (p === pre || p.startsWith(pre + "/")) return true;
        }
        return false;
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
      const underADeleted = makePrefixChecker(deletedDirsA);
      toAlpha = toAlpha.filter((r) => !underADeleted(r));
    }
    if (prefer === "beta" && deletedDirsB.length) {
      const underBDeleted = makePrefixChecker(deletedDirsB);
      toBeta = toBeta.filter((r) => !underBDeleted(r));
    }
    done();

    // ---------- APPLY IGNORES ----------
    done = t("ignores");
    const before = verbose
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
      : {};

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

    if (verbose) {
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
      console.log("Ignores filtered plan counts (dropped):", delta);
    }

    const dropInternal = (xs: string[]) =>
      xs.filter((r) => r.split("/").pop() !== IGNORE_FILE);
    toBeta = dropInternal(toBeta);
    toAlpha = dropInternal(toAlpha);
    delInBeta = dropInternal(delInBeta);
    delInAlpha = dropInternal(delInAlpha);
    done();

    // ---------- overlaps + dir delete safety ----------
    done = t("overlaps");
    const delInBetaSet = asSet(delInBeta);
    const delInAlphaSet = asSet(delInAlpha);

    if (toBeta.length && delInBeta.length) {
      const toBetaSet = new Set(toBeta);
      const beforeN = delInBeta.length;
      delInBeta = delInBeta.filter((r) => !toBetaSet.has(r));
      if (verbose && beforeN !== delInBeta.length) {
        console.warn(
          `planner safety: alpha→beta file overlap dropped=${beforeN - delInBeta.length}`,
        );
      }
    }
    if (toAlpha.length && delInAlpha.length) {
      const toAlphaSet = new Set(toAlpha);
      const beforeN = delInAlpha.length;
      delInAlpha = delInAlpha.filter((r) => !toAlphaSet.has(r));
      if (verbose && beforeN !== delInAlpha.length) {
        console.warn(
          `planner safety: beta→alpha file overlap dropped=${beforeN - delInAlpha.length}`,
        );
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
      if (verbose && beforeN !== delDirsInBeta.length) {
        console.warn(
          `planner safety: kept ${beforeN - delDirsInBeta.length} beta dirs (preferred source has outgoing under them)`,
        );
      }
    }
    if (prefer === "alpha" && delDirsInAlpha.length) {
      const outgoingFromAlphaSorted = [...toBeta, ...toBetaDirs].sort();
      const beforeN = delDirsInAlpha.length;
      delDirsInAlpha = delDirsInAlpha.filter(
        (d) => !anyIncomingUnderDir(outgoingFromAlphaSorted, d),
      );
      if (verbose && beforeN !== delDirsInAlpha.length) {
        console.warn(
          `planner safety: kept ${beforeN - delDirsInAlpha.length} alpha dirs (preferred source has outgoing under them)`,
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

    if (verbose && (droppedBetaDirs || droppedAlphaDirs)) {
      console.warn(
        `planner safety: dir-delete clamped (beta=${droppedBetaDirs}, alpha=${droppedAlphaDirs})`,
      );
    }
    done();

    if (verbose) {
      const againBeta = toBeta.filter((r) => delInBetaSet.has(r)).length;
      const againAlpha = toAlpha.filter((r) => delInAlphaSet.has(r)).length;
      if (againBeta || againAlpha) {
        console.error(
          "planner invariant failed: copy/delete not disjoint after clamp",
        );
        console.error({ againBeta, againAlpha });
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

      await writeFile(listToBeta, join0(makeRelative(toBeta, betaRoot)));
      await writeFile(listToAlpha, join0(makeRelative(toAlpha, alphaRoot)));
      await writeFile(listDelInBeta, join0(delInBeta));
      await writeFile(listDelInAlpha, join0(delInAlpha));

      await writeFile(listToBetaDirs, join0(toBetaDirs));
      await writeFile(listToAlphaDirs, join0(toAlphaDirs));
      await writeFile(listDelDirsInBeta, join0(delDirsInBeta));
      await writeFile(listDelDirsInAlpha, join0(delDirsInAlpha));
      done();

      if (verbose) {
        console.log(`Plan:
  alpha→beta copies : ${toBeta.length}
  beta→alpha copies : ${toAlpha.length}
  deletions in beta : ${delInBeta.length}
  deletions in alpha: ${delInAlpha.length}
  dirs create beta  : ${toBetaDirs.length}
  dirs create alpha : ${toAlphaDirs.length}
  dirs delete beta  : ${delDirsInBeta.length}
  dirs delete alpha : ${delDirsInAlpha.length}
  prefer side       : ${prefer}
  dry-run           : ${dryRun}
  verbose           : ${verbose}
`);
      }

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
        },
      );
      await rsyncDeleteChunked(
        tmp,
        alpha,
        beta,
        delInBeta,
        "beta deleted (files)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
        },
      );
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
          { forceEmptySource: true },
        );
      }
      if (prefer === "alpha" && preDeleteDirsOnBetaForAlphaFiles.length) {
        await rsyncDeleteChunked(
          tmp,
          beta,
          beta,
          preDeleteDirsOnBetaForAlphaFiles,
          "cleanup dir→file on beta",
          { forceEmptySource: true },
        );
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
          rsyncOpts,
        )
      ).ok;
      copyDirsBetaAlphaOk = (
        await rsyncCopyDirs(
          beta,
          alpha,
          listToAlphaDirs,
          "beta→alpha",
          rsyncOpts,
        )
      ).ok;
      done();

      // 3) copy files
      done = t("rsync: 3) copy files");
      copyAlphaBetaOk = (
        await rsyncCopy(alpha, beta, listToBeta, "alpha→beta", rsyncOpts)
      ).ok;
      copyBetaAlphaOk = (
        await rsyncCopy(beta, alpha, listToAlpha, "beta→alpha", rsyncOpts)
      ).ok;
      done();

      // 4) delete dirs last (after files removed so dirs are empty)
      done = t("rsync: 4) delete dirs");
      if (prefer === "beta" && delDirsInBeta.length) {
        const isChangedB = db.prepare<[string], { one?: number }>(
          `SELECT 1 AS one FROM tmp_dirs_changedB WHERE rpath = ?`,
        );
        const beforeN = delDirsInBeta.length;
        delDirsInBeta = delDirsInBeta.filter((d) => !isChangedB.get(d)?.one);
        if (verbose && beforeN !== delDirsInBeta.length) {
          console.warn(
            `clamped ${beforeN - delDirsInBeta.length} dir deletions in beta (dir existed on beta & prefer=beta)`,
          );
        }
      }
      if (prefer === "alpha" && delDirsInAlpha.length) {
        const isChangedA = db.prepare<[string], { one?: number }>(
          `SELECT 1 AS one FROM tmp_dirs_changedA WHERE rpath = ?`,
        );
        const beforeN = delDirsInAlpha.length;
        delDirsInAlpha = delDirsInAlpha.filter((d) => !isChangedA.get(d)?.one);
        if (verbose && beforeN !== delDirsInAlpha.length) {
          console.warn(
            `clamped ${beforeN - delDirsInAlpha.length} dir deletions in alpha (dir existed on alpha & prefer=alpha)`,
          );
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
        },
      );
      await rsyncDeleteChunked(
        tmp,
        beta,
        alpha,
        delDirsInAlpha,
        "alpha deleted (dirs)",
        {
          forceEmptySource: true,
          ...rsyncOpts,
        },
      );
      done();

      if (verbose) console.log("rsync's all done, now updating database");

      if (dryRun) {
        console.log("(dry-run) skipping base updates");
        console.log("Merge complete.");
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
        if (!verbose) return void fn();
        const t0 = Date.now();
        fn();
        const dt = Date.now() - t0;
        if (dt > 10) console.log(`[plan] ${label}: ${dt} ms`);
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

      if (verbose) {
        const c = (t: string) =>
          (db.prepare(`SELECT COUNT(*) n FROM ${t}`).get() as any).n;
        console.log(
          `Plan table counts: to_beta=${c("plan_to_beta")} to_alpha=${c("plan_to_alpha")} del_beta=${c(
            "plan_del_beta",
          )} del_alpha=${c("plan_del_alpha")}`,
        );
        console.log(
          `Plan dir counts   : d_to_beta=${c("plan_dirs_to_beta")} d_to_alpha=${c(
            "plan_dirs_to_alpha",
          )} d_del_beta=${c("plan_dirs_del_beta")} d_del_alpha=${c("plan_dirs_del_alpha")}`,
        );
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

      // ---------- prune tombstones now that alpha/beta are RELATIVE ----------
      done = t("drop tombstones");
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
      `);
      done();

      done = t("sqlite hygiene");
      db.exec("PRAGMA optimize");
      db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      done();

      if (verbose) console.log("Merge complete.");

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

  main();
}

cliEntrypoint<MergeRsyncOptions>(import.meta.url, buildProgram, runMerge, {
  label: "merge",
});
