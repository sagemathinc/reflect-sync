#!/usr/bin/env node

// src/merge.ts
import { tmpdir } from "node:os";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { Command, Option } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import { getDb } from "./db.js";
import Database from "better-sqlite3";
import { loadIgnoreFile, filterIgnored, filterIgnoredDirs } from "./ignore.js";
import { IGNORE_FILE } from "./constants.js";
import {
  rsyncCopy,
  rsyncCopyDirs,
  rsyncDeleteChunked,
  rsyncFixMeta,
  rsyncFixMetaDirs,
} from "./rsync.js";

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
    if (!verbose) {
      return () => {};
    }
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

    // base (files) and base_dirs (directories) — include mode/uid/gid + op_ts
    db.exec(`
      CREATE TABLE IF NOT EXISTS base (
        path    TEXT PRIMARY KEY,  -- RELATIVE file path
        hash    TEXT,
        mode    INTEGER DEFAULT 0,
        uid     INTEGER DEFAULT 0,
        gid     INTEGER DEFAULT 0,
        deleted INTEGER DEFAULT 0,
        op_ts   INTEGER
      );
      CREATE TABLE IF NOT EXISTS base_dirs (
        path    TEXT PRIMARY KEY,  -- RELATIVE dir path
        mode    INTEGER DEFAULT 0,
        uid     INTEGER DEFAULT 0,
        gid     INTEGER DEFAULT 0,
        deleted INTEGER DEFAULT 0,
        op_ts   INTEGER
      );
    `);

    db.prepare(`ATTACH DATABASE ? AS alpha`).run(alphaDb);
    db.prepare(`ATTACH DATABASE ? AS beta`).run(betaDb);

    // ---------- EARLY-OUT FAST PATH ----------
    // We compute a light-weight "digest" of each side's catalog (files+links and dirs).
    // If both digests equal what we saved after the last merge, we skip planning/rsync entirely.
    db.exec(`
      CREATE TABLE IF NOT EXISTS merge_meta (
        key   TEXT PRIMARY KEY,
        value TEXT
      );
    `);

    const computeSideDigest = (side: "alpha" | "beta") => {
      // files + links aggregated together
      const f = db
        .prepare(
          `
        SELECT
          COUNT(*)                                AS n,
          COALESCE(SUM(op_ts), 0)                 AS s_op,
          COALESCE(MAX(op_ts), 0)                 AS m_op,
          COALESCE(SUM(mode), 0)                  AS s_mode,
          COALESCE(SUM(uid), 0)                   AS s_uid,
          COALESCE(SUM(gid), 0)                   AS s_gid,
          COALESCE(SUM(deleted), 0)               AS s_del,
          COALESCE(SUM(LENGTH(path)), 0)          AS s_plen,
          COALESCE(SUM(LENGTH(COALESCE(hash,''))), 0) AS s_hlen
        FROM (
          SELECT path, hash, deleted, mode, uid, gid, op_ts FROM ${side}.files
          UNION ALL
          SELECT path, hash, deleted, 0, 0, 0, op_ts FROM ${side}.links
        )
      `,
        )
        .get() as any;

      // dirs
      const d = db
        .prepare(
          `
        SELECT
          COUNT(*)                        AS n,
          COALESCE(SUM(op_ts), 0)         AS s_op,
          COALESCE(MAX(op_ts), 0)         AS m_op,
          COALESCE(SUM(mode), 0)          AS s_mode,
          COALESCE(SUM(uid), 0)           AS s_uid,
          COALESCE(SUM(gid), 0)           AS s_gid,
          COALESCE(SUM(deleted), 0)       AS s_del,
          COALESCE(SUM(LENGTH(path)), 0)  AS s_plen
        FROM ${side}.dirs
      `,
        )
        .get() as any;

      // A compact, stable string; collisions are extremely unlikely with these aggregates.
      return `F:${f.n},${f.s_op},${f.m_op},${f.s_mode},${f.s_uid},${f.s_gid},${f.s_del},${f.s_plen},${f.s_hlen};D:${d.n},${d.s_op},${d.m_op},${d.s_plen},${d.s_mode},${d.s_uid},${d.s_gid},${d.s_del}`;
    };

    const digestAlpha = computeSideDigest("alpha");
    const digestBeta = computeSideDigest("beta");
    if (verbose) {
      console.log("[digest] alpha:", digestAlpha);
      console.log("[digest] beta:", digestBeta);
    }

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
      if (verbose) {
        console.log(
          "[merge] no-op: catalogs unchanged; skipping planning/rsync",
        );
      }
      return; // 🔚 Early return – nothing to do
    }
    // ---------- END EARLY-OUT FAST PATH ----------

    // ---------- Build relative-path temp tables ----------
    // Temp views to normalize to a single stream of path/hash/deleted/op_ts
    db.exec(
      `DROP VIEW IF EXISTS alpha_entries; DROP VIEW IF EXISTS beta_entries;`,
    );

    // Files+links view for content/hash logic (no mode/uid/gid)
    db.exec(
      `
      CREATE TEMP VIEW alpha_entries AS
      SELECT path, hash, deleted, op_ts FROM alpha.files
      UNION ALL
      SELECT path, hash, deleted, op_ts FROM alpha.links;
    `,
    );
    db.exec(
      `
      CREATE TEMP VIEW beta_entries AS
      SELECT path, hash, deleted, op_ts FROM beta.files
      UNION ALL
      SELECT path, hash, deleted, op_ts FROM beta.links;
    `,
    );

    db.exec(
      `DROP TABLE IF EXISTS alpha_rel; DROP TABLE IF EXISTS beta_rel; DROP TABLE IF EXISTS base_rel;`,
    );

    let done = t("build *_rel");

    // --- Files (content, no mode/uid/gid) for existing logic
    // Define the *_rel tables with a primary key and WITHOUT ROWID.
    // This makes rpath unique, saves memory, and gives you an implicit PK index.
    db.prepare(
      `
     CREATE TEMP TABLE alpha_rel(
       rpath TEXT PRIMARY KEY,
       hash  TEXT,
       deleted INTEGER,
       op_ts  INTEGER
     ) WITHOUT ROWID;
    `,
    ).run();
    db.prepare(
      `
     INSERT INTO alpha_rel SELECT
        CASE
          WHEN instr(path, (:alphaRoot || '/')) = 1 THEN substr(path, length(:alphaRoot) + 2)
          WHEN path = :alphaRoot THEN '' ELSE path
        END AS rpath, hash, deleted, op_ts
      FROM alpha_entries;
    `,
    ).run({ alphaRoot });

    db.prepare(
      `
    CREATE TEMP TABLE beta_rel(
      rpath TEXT PRIMARY KEY,
      hash  TEXT,
      deleted INTEGER,
      op_ts  INTEGER
    ) WITHOUT ROWID;`,
    ).run();
    db.prepare(
      `
    INSERT INTO beta_rel SELECT
        CASE
          WHEN instr(path, (:betaRoot || '/')) = 1 THEN substr(path, length(:betaRoot) + 2)
          WHEN path = :betaRoot THEN '' ELSE path
        END AS rpath, hash, deleted, op_ts
      FROM beta_entries;
    `,
    ).run({ betaRoot });

    // --- Files (with mode/uid/gid) for meta-only detection
    db.exec(
      `DROP TABLE IF EXISTS alpha_files_rel; DROP TABLE IF EXISTS beta_files_rel;`,
    );
    db.prepare(
      `
     CREATE TEMP TABLE alpha_files_rel(
       rpath  TEXT PRIMARY KEY,
       hash   TEXT,
       deleted INTEGER,
       op_ts  INTEGER,
       mode   INTEGER,
       uid    INTEGER,
       gid    INTEGER
     ) WITHOUT ROWID;`,
    ).run();
    db.prepare(
      `
     INSERT INTO alpha_files_rel
     SELECT
        CASE
          WHEN instr(path, (:alphaRoot || '/')) = 1 THEN substr(path, length(:alphaRoot) + 2)
          WHEN path = :alphaRoot THEN '' ELSE path
        END AS rpath,
        hash, deleted, op_ts, mode, uid, gid
      FROM alpha.files;
    `,
    ).run({ alphaRoot });

    db.prepare(
      `
     CREATE TEMP TABLE beta_files_rel(
       rpath  TEXT PRIMARY KEY,
       hash   TEXT,
       deleted INTEGER,
       op_ts  INTEGER,
       mode   INTEGER,
       uid    INTEGER,
       gid    INTEGER
     ) WITHOUT ROWID;`,
    ).run();
    db.prepare(
      `
     INSERT INTO beta_files_rel
     SELECT
        CASE
          WHEN instr(path, (:betaRoot || '/')) = 1 THEN substr(path, length(:betaRoot) + 2)
          WHEN path = :betaRoot THEN '' ELSE path
        END AS rpath,
        hash, deleted, op_ts, mode, uid, gid
      FROM beta.files;
    `,
    ).run({ betaRoot });

    // --- Base (files) with mode/uid/gid
    db.prepare(
      `
      CREATE TEMP TABLE base_rel AS
      SELECT
        CASE
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          ELSE path
        END AS rpath,
        hash, deleted, op_ts, mode, uid, gid
      FROM base
    `,
    ).run(alphaRoot, alphaRoot, betaRoot, betaRoot);

    // ---------- Dirs (include mode/uid/gid) ----------
    db.exec(
      `DROP TABLE IF EXISTS alpha_dirs_rel; DROP TABLE IF EXISTS beta_dirs_rel; DROP TABLE IF EXISTS base_dirs_rel;`,
    );

    db.prepare(
      `
        CREATE TEMP TABLE alpha_dirs_rel AS
        SELECT
          CASE
            WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
            WHEN path = ? THEN '' ELSE path
          END AS rpath,
          deleted, op_ts, mode, uid, gid
        FROM alpha.dirs
      `,
    ).run(alphaRoot, alphaRoot, alphaRoot);

    db.prepare(
      `
        CREATE TEMP TABLE beta_dirs_rel AS
        SELECT
          CASE
            WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
            WHEN path = ? THEN '' ELSE path
          END AS rpath,
          deleted, op_ts, mode, uid, gid
        FROM beta.dirs
      `,
    ).run(betaRoot, betaRoot, betaRoot);

    db.prepare(
      `
      CREATE TEMP TABLE base_dirs_rel AS
      SELECT
        CASE
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          ELSE path
        END AS rpath,
        deleted, op_ts, mode, uid, gid
      FROM base_dirs
    `,
    ).run(alphaRoot, alphaRoot, betaRoot, betaRoot);

    // Index temp rel tables
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_alpha_rel_rpath       ON alpha_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_beta_rel_rpath        ON beta_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_base_rel_rpath        ON base_rel(rpath);

      CREATE INDEX IF NOT EXISTS idx_alpha_files_rel_rpath ON alpha_files_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_beta_files_rel_rpath  ON beta_files_rel(rpath);

      CREATE INDEX IF NOT EXISTS idx_alpha_dirs_rel_rpath  ON alpha_dirs_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_beta_dirs_rel_rpath   ON beta_dirs_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_base_dirs_rel_rpath   ON base_dirs_rel(rpath);
    `);

    // composite indexes to speed up queries involving deleted
    db.exec(`
    -- For files
    CREATE INDEX IF NOT EXISTS idx_alpha_rel_deleted_rpath ON alpha_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_rel_deleted_rpath  ON beta_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_base_rel_deleted_rpath  ON base_rel(deleted, rpath);

    CREATE INDEX IF NOT EXISTS idx_alpha_files_rel_deleted_rpath ON alpha_files_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_files_rel_deleted_rpath  ON beta_files_rel(deleted, rpath);

    -- For dirs
    CREATE INDEX IF NOT EXISTS idx_alpha_dirs_rel_deleted_rpath ON alpha_dirs_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_dirs_rel_deleted_rpath  ON beta_dirs_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_base_dirs_rel_deleted_rpath  ON base_dirs_rel(deleted, rpath);
    `);
    done();

    done = t("build tmp_* files");
    // ---------- 3-way plan: FILES (content change/delete sets) ----------
    db.exec(`
      DROP TABLE IF EXISTS tmp_changedA;
      DROP TABLE IF EXISTS tmp_changedB;
      DROP TABLE IF EXISTS tmp_deletedA;
      DROP TABLE IF EXISTS tmp_deletedB;

      CREATE TEMP TABLE tmp_changedA AS
        SELECT a.rpath AS rpath, a.hash
        FROM alpha_rel a
        LEFT JOIN base_rel b ON b.rpath = a.rpath
        WHERE a.deleted = 0 AND (b.rpath IS NULL OR b.deleted = 1 OR a.hash <> b.hash);

      CREATE TEMP TABLE tmp_changedB AS
        SELECT b.rpath AS rpath, b.hash
        FROM beta_rel b
        LEFT JOIN base_rel bb ON bb.rpath = b.rpath
        WHERE b.deleted = 0 AND (bb.rpath IS NULL OR bb.deleted = 1 OR b.hash <> bb.hash);

      -- deletions: base says existed (deleted=0) and side is missing or deleted
      CREATE TEMP TABLE tmp_deletedA AS
        SELECT b.rpath
        FROM base_rel b
        LEFT JOIN alpha_rel a ON a.rpath = b.rpath
        WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);

      CREATE TEMP TABLE tmp_deletedB AS
        SELECT b.rpath
        FROM base_rel b
        LEFT JOIN beta_rel a ON a.rpath = b.rpath
        WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);
    `);

    // ---------- META-ONLY (files): hash equal but mode/uid/gid differ ----------
    db.exec(`
      DROP TABLE IF EXISTS tmp_metaChangedA;
      DROP TABLE IF EXISTS tmp_metaChangedB;

      CREATE TEMP TABLE tmp_metaChangedA AS
        SELECT a.rpath
        FROM alpha_files_rel a
        JOIN base_rel b USING (rpath)
        WHERE a.deleted = 0 AND b.deleted = 0
          AND a.hash = b.hash
          AND (a.mode<>b.mode OR a.uid<>b.uid OR a.gid<>b.gid);

      CREATE TEMP TABLE tmp_metaChangedB AS
        SELECT b.rpath
        FROM beta_files_rel b
        JOIN base_rel bb USING (rpath)
        WHERE b.deleted = 0 AND bb.deleted = 0
          AND b.hash = bb.hash
          AND (b.mode<>bb.mode OR b.uid<>bb.uid OR b.gid<>bb.gid);
    `);

    // ---------- 3-way plan: DIRS (presence-only change/delete) ----------
    db.exec(`
      DROP TABLE IF EXISTS tmp_dirs_changedA;
      DROP TABLE IF EXISTS tmp_dirs_changedB;
      DROP TABLE IF EXISTS tmp_dirs_deletedA;
      DROP TABLE IF EXISTS tmp_dirs_deletedB;

      CREATE TEMP TABLE tmp_dirs_changedA AS
        SELECT a.rpath AS rpath
        FROM alpha_dirs_rel a
        LEFT JOIN base_dirs_rel b ON b.rpath = a.rpath
        WHERE a.deleted = 0 AND (b.rpath IS NULL OR b.deleted = 1);

      CREATE TEMP TABLE tmp_dirs_changedB AS
        SELECT b.rpath AS rpath
        FROM beta_dirs_rel b
        LEFT JOIN base_dirs_rel bb ON bb.rpath = b.rpath
        WHERE b.deleted = 0 AND (bb.rpath IS NULL OR bb.deleted = 1);

      CREATE TEMP TABLE tmp_dirs_deletedA AS
        SELECT b.rpath
        FROM base_dirs_rel b
        LEFT JOIN alpha_dirs_rel a ON a.rpath = b.rpath
        WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);

      CREATE TEMP TABLE tmp_dirs_deletedB AS
        SELECT b.rpath
        FROM base_dirs_rel b
        LEFT JOIN beta_dirs_rel a ON a.rpath = b.rpath
        WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);
    `);

    // ---------- DIR META-ONLY (mode/uid/gid differ) ----------
    db.exec(`
      DROP TABLE IF EXISTS tmp_dirMetaChangedA;
      DROP TABLE IF EXISTS tmp_dirMetaChangedB;

      CREATE TEMP TABLE tmp_dirMetaChangedA AS
        SELECT a.rpath
        FROM alpha_dirs_rel a
        JOIN base_dirs_rel b USING (rpath)
        WHERE a.deleted = 0 AND b.deleted = 0
          AND (a.mode<>b.mode OR a.uid<>b.uid OR a.gid<>b.gid);

      CREATE TEMP TABLE tmp_dirMetaChangedB AS
        SELECT b.rpath
        FROM beta_dirs_rel b
        JOIN base_dirs_rel bb USING (rpath)
        WHERE b.deleted = 0 AND bb.deleted = 0
          AND (b.mode<>bb.mode OR b.uid<>bb.uid OR b.gid<>bb.gid);
    `);

    // Index the tmp_* tables too
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_tmp_changedA_rpath       ON tmp_changedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_changedB_rpath       ON tmp_changedB(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_deletedA_rpath       ON tmp_deletedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_deletedB_rpath       ON tmp_deletedB(rpath);

      CREATE INDEX IF NOT EXISTS idx_tmp_metaChangedA_rpath   ON tmp_metaChangedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_metaChangedB_rpath   ON tmp_metaChangedB(rpath);

      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_changedA_rpath  ON tmp_dirs_changedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_changedB_rpath  ON tmp_dirs_changedB(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_deletedA_rpath  ON tmp_dirs_deletedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_deletedB_rpath  ON tmp_dirs_deletedB(rpath);

      CREATE INDEX IF NOT EXISTS idx_tmp_dirMetaChangedA_rpath ON tmp_dirMetaChangedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirMetaChangedB_rpath ON tmp_dirMetaChangedB(rpath);
    `);
    done();

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
      console.log(
        `Meta-only counts     : metaA=${count("tmp_metaChangedA")} metaB=${count("tmp_metaChangedB")} dmetaA=${count("tmp_dirMetaChangedA")} dmetaB=${count("tmp_dirMetaChangedB")}`,
      );
    }

    // ---------- Build copy/delete plans (FILES) with LWW ----------

    // A changed, B not changed → copy A→B (respect deletions of dirs on B)
    let toBeta = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_changedA a
        WHERE a.rpath NOT IN (SELECT rpath FROM tmp_changedB)
          AND (
            ? <> 'beta' OR (
              a.rpath NOT IN (SELECT rpath FROM tmp_deletedB)
            )
          )
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    // B changed, A not changed → copy B→A (respect deletions of dirs on A)
    let toAlpha = db
      .prepare(
        `
        SELECT b.rpath
        FROM tmp_changedB b
        WHERE b.rpath NOT IN (SELECT rpath FROM tmp_changedA)
          AND (
            ? <> 'alpha' OR (
              b.rpath NOT IN (SELECT rpath FROM tmp_deletedA)
            )
          )
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    // changed vs changed → LWW (use op_ts; tie within EPS -> prefer)
    const bothChangedToBeta = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_changedA cA
        JOIN tmp_changedB cB USING (rpath)
        JOIN alpha_rel a ON a.rpath = cA.rpath
        JOIN beta_rel  b ON b.rpath = cB.rpath
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
        JOIN alpha_rel a ON a.rpath = cA.rpath
        JOIN beta_rel  b ON b.rpath = cB.rpath
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
        JOIN alpha_rel a ON a.rpath = cA.rpath
        JOIN beta_rel  b ON b.rpath = cB.rpath
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

    // ---------- META-ONLY propagation (FILES) ----------
    // only when other side didn't content-change/delete that path
    let toBetaMeta = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_metaChangedA a
        WHERE a.rpath NOT IN (SELECT rpath FROM tmp_changedB)
          AND a.rpath NOT IN (SELECT rpath FROM tmp_deletedB)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    let toAlphaMeta = db
      .prepare(
        `
        SELECT b.rpath
        FROM tmp_metaChangedB b
        WHERE b.rpath NOT IN (SELECT rpath FROM tmp_changedA)
          AND b.rpath NOT IN (SELECT rpath FROM tmp_deletedA)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    // meta vs meta on both sides → LWW on files' op_ts (uses alpha_files_rel/beta_files_rel)
    const bothMetaToBeta = db
      .prepare(
        `
        SELECT mA.rpath
        FROM tmp_metaChangedA mA
        JOIN tmp_metaChangedB mB USING (rpath)
        JOIN alpha_files_rel a USING (rpath)
        JOIN beta_files_rel  b USING (rpath)
        WHERE a.op_ts > b.op_ts + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothMetaToAlpha = db
      .prepare(
        `
        SELECT mA.rpath
        FROM tmp_metaChangedA mA
        JOIN tmp_metaChangedB mB USING (rpath)
        JOIN alpha_files_rel a USING (rpath)
        JOIN beta_files_rel  b USING (rpath)
        WHERE b.op_ts > a.op_ts + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothMetaTie = db
      .prepare(
        `
        SELECT mA.rpath
        FROM tmp_metaChangedA mA
        JOIN tmp_metaChangedB mB USING (rpath)
        JOIN alpha_files_rel a USING (rpath)
        JOIN beta_files_rel  b USING (rpath)
        WHERE ABS(a.op_ts - b.op_ts) <= ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    if (prefer === "alpha") {
      toBetaMeta = uniq([...toBetaMeta, ...bothMetaToBeta, ...bothMetaTie]);
      toAlphaMeta = uniq([...toAlphaMeta, ...bothMetaToAlpha]);
    } else {
      toBetaMeta = uniq([...toBetaMeta, ...bothMetaToBeta]);
      toAlphaMeta = uniq([...toAlphaMeta, ...bothMetaToAlpha, ...bothMetaTie]);
    }

    done();

    done = t("deletions");

    // deletions (files) with LWW against changes on the other side

    // No-conflict delete: A deleted & B didn't change
    const delInBeta_noConflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_deletedA dA
        LEFT JOIN tmp_changedB cB ON cB.rpath = dA.rpath
        WHERE cB.rpath IS NULL
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    // Conflict: A deleted vs B changed → LWW (delete in B if A newer; tie -> prefer alpha)
    const delInBeta_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_deletedA dA
        JOIN tmp_changedB cB USING (rpath)
        LEFT JOIN alpha_rel a ON a.rpath = dA.rpath
        LEFT JOIN beta_rel  b ON b.rpath = dA.rpath
        LEFT JOIN base_rel  br ON br.rpath = dA.rpath
        WHERE COALESCE(a.op_ts, br.op_ts, 0) > COALESCE(b.op_ts, 0) + ?
           OR (ABS(COALESCE(a.op_ts, br.op_ts, 0) - COALESCE(b.op_ts, 0)) <= ? AND ? = 'alpha')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    // Conflict: A deleted vs B changed → LWW (copy B->A if B newer; tie -> prefer beta)
    const toAlpha_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_deletedA dA
        JOIN tmp_changedB cB USING (rpath)
        LEFT JOIN alpha_rel a ON a.rpath = dA.rpath
        LEFT JOIN beta_rel  b ON b.rpath = dA.rpath
        LEFT JOIN base_rel  br ON br.rpath = dA.rpath
        WHERE COALESCE(b.op_ts, 0) > COALESCE(a.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(b.op_ts, 0) - COALESCE(a.op_ts, br.op_ts, 0)) <= ? AND ? = 'beta')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    let delInBeta = uniq([...delInBeta_noConflict, ...delInBeta_conflict]);
    toAlpha = uniq([...toAlpha, ...toAlpha_conflict]);

    // Symmetric side: B deleted vs A changed

    const delInAlpha_noConflict = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_deletedB dB
        LEFT JOIN tmp_changedA cA ON cA.rpath = dB.rpath
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
        LEFT JOIN alpha_rel a ON a.rpath = dB.rpath
        LEFT JOIN beta_rel  b ON b.rpath = dB.rpath
        LEFT JOIN base_rel  br ON br.rpath = dB.rpath
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
        LEFT JOIN alpha_rel a ON a.rpath = dB.rpath
        LEFT JOIN beta_rel  b ON b.rpath = dB.rpath
        LEFT JOIN base_rel  br ON br.rpath = dB.rpath
        WHERE COALESCE(a.op_ts, 0) > COALESCE(b.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(a.op_ts, 0) - COALESCE(b.op_ts, br.op_ts, 0)) <= ? AND ? = 'alpha')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    let delInAlpha = uniq([...delInAlpha_noConflict, ...delInAlpha_conflict]);
    toBeta = uniq([...toBeta, ...toBeta_conflict]);

    // ---------- DIR plans with LWW (presence-based) ----------
    // create-only
    let toBetaDirs = db
      .prepare(
        `
        SELECT rpath FROM tmp_dirs_changedA
        WHERE rpath NOT IN (SELECT rpath FROM tmp_dirs_changedB)
          AND ( ? <> 'beta' OR rpath NOT IN (SELECT rpath FROM tmp_dirs_deletedB))
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let toAlphaDirs = db
      .prepare(
        `
        SELECT rpath FROM tmp_dirs_changedB
        WHERE rpath NOT IN (SELECT rpath FROM tmp_dirs_changedA)
          AND ( ? <> 'alpha' OR rpath NOT IN (SELECT rpath FROM tmp_dirs_deletedA))
      `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    // both created dir → LWW via op_ts (dir op_ts)
    const bothDirsToBeta = db
      .prepare(
        `
        SELECT cA.rpath
        FROM tmp_dirs_changedA cA
        JOIN tmp_dirs_changedB cB USING (rpath)
        JOIN alpha_dirs_rel a ON a.rpath = cA.rpath
        JOIN beta_dirs_rel  b ON b.rpath = cB.rpath
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
        JOIN alpha_dirs_rel a ON a.rpath = cA.rpath
        JOIN beta_dirs_rel  b ON b.rpath = cB.rpath
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
        JOIN alpha_dirs_rel a ON a.rpath = cA.rpath
        JOIN beta_dirs_rel  b ON b.rpath = cB.rpath
        WHERE ABS(COALESCE(a.op_ts,0) - COALESCE(b.op_ts,0)) <= ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);
    done();
    done = t("to alpha/beta uniq");

    if (prefer === "alpha") {
      toBetaDirs = uniq([...toBetaDirs, ...bothDirsToBeta, ...bothDirsTie]);
      toAlphaDirs = uniq([...toAlphaDirs, ...bothDirsToAlpha]);
    } else {
      toBetaDirs = uniq([...toBetaDirs, ...bothDirsToBeta]);
      toAlphaDirs = uniq([...toAlphaDirs, ...bothDirsToAlpha, ...bothDirsTie]);
    }
    done();

    // ---------- DIR META-ONLY: LWW ----------
    let toBetaDirsMeta = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_dirMetaChangedA a
        WHERE a.rpath NOT IN (SELECT rpath FROM tmp_dirs_deletedB)
          AND a.rpath NOT IN (SELECT rpath FROM tmp_dirs_changedB)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    let toAlphaDirsMeta = db
      .prepare(
        `
        SELECT b.rpath
        FROM tmp_dirMetaChangedB b
        WHERE b.rpath NOT IN (SELECT rpath FROM tmp_dirs_deletedA)
          AND b.rpath NOT IN (SELECT rpath FROM tmp_dirs_changedA)
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    const bothDirMetaToBeta = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_dirMetaChangedA a
        JOIN tmp_dirMetaChangedB b USING (rpath)
        JOIN alpha_dirs_rel da USING (rpath)
        JOIN beta_dirs_rel  db USING (rpath)
        WHERE COALESCE(da.op_ts,0) > COALESCE(db.op_ts,0) + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothDirMetaToAlpha = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_dirMetaChangedA a
        JOIN tmp_dirMetaChangedB b USING (rpath)
        JOIN alpha_dirs_rel da USING (rpath)
        JOIN beta_dirs_rel  db USING (rpath)
        WHERE COALESCE(db.op_ts,0) > COALESCE(da.op_ts,0) + ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    const bothDirMetaTie = db
      .prepare(
        `
        SELECT a.rpath
        FROM tmp_dirMetaChangedA a
        JOIN tmp_dirMetaChangedB b USING (rpath)
        JOIN alpha_dirs_rel da USING (rpath)
        JOIN beta_dirs_rel  db USING (rpath)
        WHERE ABS(COALESCE(da.op_ts,0) - COALESCE(db.op_ts,0)) <= ?
      `,
      )
      .all(EPS)
      .map((r) => r.rpath as string);

    if (prefer === "alpha") {
      toBetaDirsMeta = uniq([
        ...toBetaDirsMeta,
        ...bothDirMetaToBeta,
        ...bothDirMetaTie,
      ]);
      toAlphaDirsMeta = uniq([...toAlphaDirsMeta, ...bothDirMetaToAlpha]);
    } else {
      toBetaDirsMeta = uniq([...toBetaDirsMeta, ...bothDirMetaToBeta]);
      toAlphaDirsMeta = uniq([
        ...toAlphaDirsMeta,
        ...bothDirMetaToAlpha,
        ...bothDirMetaTie,
      ]);
    }

    // ---------- TYPE-FLIP (file vs dir) conflicts ----------
    done = t("type flips");
    const fileConflictsInBeta = db
      .prepare(
        `
        SELECT b.rpath
        FROM beta_rel b
        JOIN alpha_dirs_rel d ON d.rpath = b.rpath
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
        JOIN beta_dirs_rel d ON d.rpath = a.rpath
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

    // --- Pre-copy dir→file cleanup sets (only used if that side is preferred) ---
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

    done = t("safety rails");

    // ---------- SAFETY RAILS ----------
    toBeta = uniq(toBeta);
    toAlpha = uniq(toAlpha);
    toBetaMeta = uniq(toBetaMeta.filter((r) => !asSet(toBeta).has(r)));
    toAlphaMeta = uniq(toAlphaMeta.filter((r) => !asSet(toAlpha).has(r)));

    delInBeta = uniq(delInBeta);
    delInAlpha = uniq(delInAlpha);

    toBetaDirs = uniq(toBetaDirs);
    toAlphaDirs = uniq(toAlphaDirs);
    toBetaDirsMeta = uniq(
      toBetaDirsMeta.filter((r) => !asSet(toBetaDirs).has(r)),
    );
    toAlphaDirsMeta = uniq(
      toAlphaDirsMeta.filter((r) => !asSet(toAlphaDirs).has(r)),
    );

    let delDirsInBeta = uniq([
      ...db
        .prepare(`SELECT rpath FROM tmp_dirs_deletedA`)
        .all()
        .map((r: any) => r.rpath as string),
    ]);
    let delDirsInAlpha = uniq([
      ...db
        .prepare(`SELECT rpath FROM tmp_dirs_deletedB`)
        .all()
        .map((r: any) => r.rpath as string),
    ]);

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
      toAlphaMeta = toAlphaMeta.filter((r) => !underADeleted(r));
      toAlphaDirs = toAlphaDirs.filter((r) => !underADeleted(r));
      toAlphaDirsMeta = toAlphaDirsMeta.filter((r) => !underADeleted(r));
    }
    if (prefer === "beta" && deletedDirsB.length) {
      const underBDeleted = makePrefixChecker(deletedDirsB);
      toBeta = toBeta.filter((r) => !underBDeleted(r));
      toBetaMeta = toBetaMeta.filter((r) => !underBDeleted(r));
      toBetaDirs = toBetaDirs.filter((r) => !underBDeleted(r));
      toBetaDirsMeta = toBetaDirsMeta.filter((r) => !underBDeleted(r));
    }

    // ---------- APPLY IGNORES ----------
    // Drop any rpaths that are ignored on either side. This makes ignores local-only:
    // we do not copy *or* delete ignored paths, and we do not update base for them.
    done = t("ignores");
    const before = verbose
      ? {
          toBeta: toBeta.length,
          toAlpha: toAlpha.length,
          toBetaMeta: toBetaMeta.length,
          toAlphaMeta: toAlphaMeta.length,
          delInBeta: delInBeta.length,
          delInAlpha: delInAlpha.length,
          toBetaDirs: toBetaDirs.length,
          toAlphaDirs: toAlphaDirs.length,
          toBetaDirsMeta: toBetaDirsMeta.length,
          toAlphaDirsMeta: toAlphaDirsMeta.length,
          delDirsInBeta: delDirsInBeta.length,
          delDirsInAlpha: delDirsInAlpha.length,
        }
      : {};

    const ignore = (x: string[]) => filterIgnored(x, alphaIg, betaIg);
    toBeta = ignore(toBeta);
    toAlpha = ignore(toAlpha);
    toBetaMeta = ignore(toBetaMeta);
    toAlphaMeta = ignore(toAlphaMeta);
    delInBeta = ignore(delInBeta);
    delInAlpha = ignore(delInAlpha);

    const ignoreDirs = (x: string[]) => filterIgnoredDirs(x, alphaIg, betaIg);
    toBetaDirs = ignoreDirs(toBetaDirs);
    toAlphaDirs = ignoreDirs(toAlphaDirs);
    toBetaDirsMeta = ignoreDirs(toBetaDirsMeta);
    toAlphaDirsMeta = ignoreDirs(toAlphaDirsMeta);
    delDirsInBeta = ignoreDirs(delDirsInBeta);
    delDirsInAlpha = ignoreDirs(delDirsInAlpha);

    const dropInternal = (xs: string[]) =>
      xs.filter((r) => r.split("/").pop() !== IGNORE_FILE);

    toBeta = dropInternal(toBeta);
    toAlpha = dropInternal(toAlpha);
    toBetaMeta = dropInternal(toBetaMeta);
    toAlphaMeta = dropInternal(toAlphaMeta);
    delInBeta = dropInternal(delInBeta);
    delInAlpha = dropInternal(delInAlpha);
    toBetaDirs = dropInternal(toBetaDirs);
    toAlphaDirs = dropInternal(toAlphaDirs);
    toBetaDirsMeta = dropInternal(toBetaDirsMeta);
    toAlphaDirsMeta = dropInternal(toAlphaDirsMeta);

    if (verbose) {
      const after = {
        toBeta: toBeta.length,
        toAlpha: toAlpha.length,
        toBetaMeta: toBetaMeta.length,
        toAlphaMeta: toAlphaMeta.length,
        delInBeta: delInBeta.length,
        delInAlpha: delInAlpha.length,
        toBetaDirs: toBetaDirs.length,
        toAlphaDirs: toAlphaDirs.length,
        toBetaDirsMeta: toBetaDirsMeta.length,
        toAlphaDirsMeta: toAlphaDirsMeta.length,
        delDirsInBeta: delDirsInBeta.length,
        delDirsInAlpha: delDirsInAlpha.length,
      };
      const delta = Object.fromEntries(
        Object.entries(after).map(([k, v]) => [k, (before as any)[k] - v]),
      );
      console.log("Ignores filtered plan counts (dropped):", delta);
    }
    done();

    // ---------- overlaps + dir delete safety ----------
    done = t("overlaps");
    const delInBetaSet = asSet(delInBeta);
    const delInAlphaSet = asSet(delInAlpha);

    // 1) copy/delete overlap (files) in O(n)
    if (toBeta.length && delInBeta.length) {
      const toBetaSet = new Set(toBeta);
      delInBeta = delInBeta.filter((r) => !toBetaSet.has(r));
    }
    if (toAlpha.length && delInAlpha.length) {
      const toAlphaSet = new Set(toAlpha);
      delInAlpha = delInAlpha.filter((r) => !toAlphaSet.has(r));
    }
    if (toBetaMeta.length && delInBeta.length) {
      const set = new Set(toBetaMeta);
      delInBeta = delInBeta.filter((r) => !set.has(r));
    }
    if (toAlphaMeta.length && delInAlpha.length) {
      const set = new Set(toAlphaMeta);
      delInAlpha = delInAlpha.filter((r) => !set.has(r));
    }

    // 2) dir delete safety
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

    const betaIncomingSorted = [
      ...toBeta,
      ...toBetaMeta,
      ...toBetaDirs,
      ...toBetaDirsMeta,
    ].sort();
    const alphaIncomingSorted = [
      ...toAlpha,
      ...toAlphaMeta,
      ...toAlphaDirs,
      ...toAlphaDirsMeta,
    ].sort();

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

    // Assert (verbose only)
    if (verbose) {
      const againBeta =
        toBeta.filter((r) => delInBetaSet.has(r)).length +
        toBetaMeta.filter((r) => delInBetaSet.has(r)).length;
      const againAlpha =
        toAlpha.filter((r) => delInAlphaSet.has(r)).length +
        toAlphaMeta.filter((r) => delInAlphaSet.has(r)).length;
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
      // ---------- files-from (NUL-separated) ----------
      const listToBeta = path.join(tmp, "toBeta.list");
      const listToAlpha = path.join(tmp, "toAlpha.list");
      const listToBetaMeta = path.join(tmp, "toBeta.meta.list");
      const listToAlphaMeta = path.join(tmp, "toAlpha.meta.list");

      const listDelInBeta = path.join(tmp, "delInBeta.list");
      const listDelInAlpha = path.join(tmp, "delInAlpha.list");

      const listToBetaDirs = path.join(tmp, "toBeta.dirs.list");
      const listToAlphaDirs = path.join(tmp, "toAlpha.dirs.list");
      const listToBetaDirsMeta = path.join(tmp, "toBeta.dirs.meta.list");
      const listToAlphaDirsMeta = path.join(tmp, "toAlpha.dirs.meta.list");
      const listDelDirsInBeta = path.join(tmp, "delInBeta.dirs.list");
      const listDelDirsInAlpha = path.join(tmp, "delInAlpha.dirs.list");

      // don't try to delete non-empty folder
      delDirsInBeta = sortDeepestFirst(nonRoot(delDirsInBeta));
      delDirsInAlpha = sortDeepestFirst(nonRoot(delDirsInAlpha));

      await writeFile(listToBeta, join0(makeRelative(toBeta, betaRoot)));
      await writeFile(listToAlpha, join0(makeRelative(toAlpha, alphaRoot)));
      await writeFile(
        listToBetaMeta,
        join0(makeRelative(toBetaMeta, betaRoot)),
      );
      await writeFile(
        listToAlphaMeta,
        join0(makeRelative(toAlphaMeta, alphaRoot)),
      );
      await writeFile(listDelInBeta, join0(delInBeta));
      await writeFile(listDelInAlpha, join0(delInAlpha));

      await writeFile(listToBetaDirs, join0(toBetaDirs));
      await writeFile(listToAlphaDirs, join0(toAlphaDirs));
      await writeFile(listToBetaDirsMeta, join0(toBetaDirsMeta));
      await writeFile(listToAlphaDirsMeta, join0(toAlphaDirsMeta));
      await writeFile(listDelDirsInBeta, join0(delDirsInBeta));
      await writeFile(listDelDirsInAlpha, join0(delDirsInAlpha));
      done();

      if (verbose) {
        console.log(`Plan:
  alpha→beta copies : ${toBeta.length}
  beta→alpha copies : ${toAlpha.length}
  alpha→beta meta   : ${toBetaMeta.length}
  beta→alpha meta   : ${toAlphaMeta.length}
  deletions in beta : ${delInBeta.length}
  deletions in alpha: ${delInAlpha.length}
  dirs create beta  : ${toBetaDirs.length}
  dirs create alpha : ${toAlphaDirs.length}
  dirs meta  beta   : ${toBetaDirsMeta.length}
  dirs meta  alpha  : ${toAlphaDirsMeta.length}
  dirs delete beta  : ${delDirsInBeta.length}
  dirs delete alpha : ${delDirsInAlpha.length}
  prefer side       : ${prefer}
  dry-run           : ${dryRun}
  verbose           : ${verbose}
`);
      }

      // ---------- rsync ----------
      let didAlphaBetaFiles = false;
      let didBetaAlphaFiles = false;
      let didAlphaBetaDirs = false;
      let didBetaAlphaDirs = false;

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

      // 1b) dir→file cleanup (remove conflicting dirs on the side where files will land)
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

      // 2) create dirs (so file copies won't fail due to missing parents)
      done = t("rsync: 2) create dirs");
      const resDirsAB = await rsyncCopyDirs(
        alpha,
        beta,
        listToBetaDirs,
        "alpha→beta",
        rsyncOpts,
      );
      const resDirsBA = await rsyncCopyDirs(
        beta,
        alpha,
        listToAlphaDirs,
        "beta→alpha",
        rsyncOpts,
      );
      didAlphaBetaDirs ||= resDirsAB.zero;
      didBetaAlphaDirs ||= resDirsBA.zero;
      done();

      // 2b) fix dir metadata (no content)
      done = t("rsync: 2b) fix dir meta");
      const resDirMetaAB = await rsyncFixMetaDirs(
        alpha,
        beta,
        listToBetaDirsMeta,
        "alpha→beta",
        rsyncOpts,
      );
      const resDirMetaBA = await rsyncFixMetaDirs(
        beta,
        alpha,
        listToAlphaDirsMeta,
        "beta→alpha",
        rsyncOpts,
      );
      didAlphaBetaDirs ||= resDirMetaAB.zero;
      didBetaAlphaDirs ||= resDirMetaBA.zero;
      done();

      // 3) copy file content
      done = t("rsync: 3) copy files");
      const resFilesAB = await rsyncCopy(
        alpha,
        beta,
        listToBeta,
        "alpha→beta",
        rsyncOpts,
      );
      const resFilesBA = await rsyncCopy(
        beta,
        alpha,
        listToAlpha,
        "beta→alpha",
        rsyncOpts,
      );
      didAlphaBetaFiles ||= resFilesAB.zero;
      didBetaAlphaFiles ||= resFilesBA.zero;
      done();

      // 3b) fix file metadata (no content)
      done = t("rsync: 3b) fix file meta");
      const resMetaAB = await rsyncFixMeta(
        alpha,
        beta,
        listToBetaMeta,
        "alpha→beta",
        rsyncOpts,
      );
      const resMetaBA = await rsyncFixMeta(
        beta,
        alpha,
        listToAlphaMeta,
        "beta→alpha",
        rsyncOpts,
      );
      didAlphaBetaFiles ||= resMetaAB.zero;
      didBetaAlphaFiles ||= resMetaBA.zero;
      done();

      // 4) delete dirs last (after files removed so dirs are empty)
      done = t("rsync: 4) delete dirs");
      if (prefer === "beta" && delDirsInBeta.length) {
        const isChangedB = db.prepare<[string], { one?: number }>(
          `SELECT 1 AS one FROM tmp_dirs_changedB WHERE rpath = ?`,
        );
        const before = delDirsInBeta.length;
        delDirsInBeta = delDirsInBeta.filter((d) => !isChangedB.get(d)?.one);
        if (verbose && before !== delDirsInBeta.length) {
          console.warn(
            `clamped ${before - delDirsInBeta.length} dir deletions in beta (dir existed on beta & prefer=beta)`,
          );
        }
      }
      if (prefer === "alpha" && delDirsInAlpha.length) {
        const isChangedA = db.prepare<[string], { one?: number }>(
          `SELECT 1 AS one FROM tmp_dirs_changedA WHERE rpath = ?`,
        );
        const before = delDirsInAlpha.length;
        delDirsInAlpha = delDirsInAlpha.filter((d) => !isChangedA.get(d)?.one);
        if (verbose && before !== delDirsInAlpha.length) {
          console.warn(
            `clamped ${before - delDirsInAlpha.length} dir deletions in alpha (dir existed on alpha & prefer=alpha)`,
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

      if (verbose) {
        console.log("rsync's all done, now updating database");
      }

      if (dryRun) {
        console.log("(dry-run) skipping base updates");
        console.log("Merge complete.");
        return;
      }

      done = t("post rsync database update");
      // ---------- set-based base updates (fast) ----------
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
        if (!verbose) {
          fn();
          return;
        }
        const t0 = Date.now();
        fn();
        const dt = Date.now() - t0;
        if (dt > 10) console.log(`[plan] ${label}: ${dt} ms`);
      }

      const toBetaAll = uniq([...toBeta, ...toBetaMeta]);
      const toAlphaAll = uniq([...toAlpha, ...toAlphaMeta]);
      const toBetaDirsAll = uniq([...toBetaDirs, ...toBetaDirsMeta]);
      const toAlphaDirsAll = uniq([...toAlphaDirs, ...toAlphaDirsMeta]);

      timed("insert plan tables", () => {
        const tx = db.transaction(() => {
          bulkInsert("plan_to_beta", toBetaAll);
          bulkInsert("plan_to_alpha", toAlphaAll);
          bulkInsert("plan_del_beta", delInBeta);
          bulkInsert("plan_del_alpha", delInAlpha);

          bulkInsert("plan_dirs_to_beta", toBetaDirsAll);
          bulkInsert("plan_dirs_to_alpha", toAlphaDirsAll);
          bulkInsert("plan_dirs_del_beta", delDirsInBeta);
          bulkInsert("plan_dirs_del_alpha", delDirsInAlpha);
        });
        tx();
      });

      // Index plan tables for faster joins on big sets
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

      // Show plan table sizes in verbose mode
      if (verbose) {
        const c = (t: string) =>
          (db.prepare(`SELECT COUNT(*) n FROM ${t}`).get() as any).n;
        console.log(
          `Plan table counts: to_beta=${c("plan_to_beta")} to_alpha=${c(
            "plan_to_alpha",
          )} del_beta=${c("plan_del_beta")} del_alpha=${c("plan_del_alpha")}`,
        );
        console.log(
          `Plan dir counts   : d_to_beta=${c(
            "plan_dirs_to_beta",
          )} d_to_alpha=${c("plan_dirs_to_alpha")} d_del_beta=${c(
            "plan_dirs_del_beta",
          )} d_del_alpha=${c("plan_dirs_del_alpha")}`,
        );
      }

      // Now set-based updates in a single transaction (preserve op_ts & meta)
      db.transaction(() => {
        // files copied/metadata A->B
        if (didAlphaBetaFiles && toBetaAll.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath, a.hash, a.mode, a.uid, a.gid, 0, a.op_ts
            FROM plan_to_beta p
            JOIN alpha_files_rel a ON a.rpath = p.rpath;
          `);
        }

        // files copied/metadata B->A
        if (didBetaAlphaFiles && toAlphaAll.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath, b.hash, b.mode, b.uid, b.gid, 0, b.op_ts
            FROM plan_to_alpha p
            JOIN beta_files_rel b ON b.rpath = p.rpath;
          `);
        }

        // file deletions in beta (alpha won)
        if (delInBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath, NULL,
                   COALESCE(a.mode, b.mode, br.mode, 0),
                   COALESCE(a.uid,  b.uid,  br.uid,  0),
                   COALESCE(a.gid,  b.gid,  br.gid,  0),
                   1,
                   COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_beta p
            LEFT JOIN alpha_files_rel a ON a.rpath = p.rpath
            LEFT JOIN beta_files_rel  b ON b.rpath  = p.rpath
            LEFT JOIN base_rel        br ON br.rpath = p.rpath;
          `);
        }

        // file deletions in alpha (beta won)
        if (delInAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath, NULL,
                   COALESCE(b.mode, a.mode, br.mode, 0),
                   COALESCE(b.uid,  a.uid,  br.uid,  0),
                   COALESCE(b.gid,  a.gid,  br.gid,  0),
                   1,
                   COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_alpha p
            LEFT JOIN beta_files_rel  b ON b.rpath  = p.rpath
            LEFT JOIN alpha_files_rel a ON a.rpath   = p.rpath
            LEFT JOIN base_rel        br ON br.rpath = p.rpath;
          `);
        }

        // Directories
        if (didAlphaBetaDirs && toBetaDirsAll.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath, a.mode, a.uid, a.gid, 0, a.op_ts
            FROM plan_dirs_to_beta p
            JOIN alpha_dirs_rel a ON a.rpath = p.rpath;
          `);
        }
        if (didBetaAlphaDirs && toAlphaDirsAll.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath, b.mode, b.uid, b.gid, 0, b.op_ts
            FROM plan_dirs_to_alpha p
            JOIN beta_dirs_rel b ON b.rpath = p.rpath;
          `);
        }
        if (delDirsInBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath,
                   COALESCE(a.mode, b.mode, br.mode, 0),
                   COALESCE(a.uid,  b.uid,  br.uid,  0),
                   COALESCE(a.gid,  b.gid,  br.gid,  0),
                   1,
                   COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_dirs_del_beta p
            LEFT JOIN alpha_dirs_rel a ON a.rpath = p.rpath
            LEFT JOIN beta_dirs_rel  b ON b.rpath  = p.rpath
            LEFT JOIN base_dirs_rel  br ON br.rpath = p.rpath;
          `);
        }
        if (delDirsInAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, mode, uid, gid, deleted, op_ts)
            SELECT p.rpath,
                   COALESCE(b.mode, a.mode, br.mode, 0),
                   COALESCE(b.uid,  a.uid,  br.uid,  0),
                   COALESCE(b.gid,  a.gid,  br.gid,  0),
                   1,
                   COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_dirs_del_alpha p
            LEFT JOIN beta_dirs_rel  b ON b.rpath  = p.rpath
            LEFT JOIN alpha_dirs_rel a ON a.rpath  = p.rpath
            LEFT JOIN base_dirs_rel  br ON br.rpath = p.rpath;
          `);
        }
      })();

      done();
      // Optional hygiene on big runs
      db.exec("PRAGMA optimize");
      db.exec("PRAGMA wal_checkpoint(TRUNCATE)");

      if (verbose) {
        console.log("Merge complete.");
      }

      // Persist current side digests so we can early-out next time.
      const up = db.prepare(`REPLACE INTO merge_meta(key,value) VALUES(?,?)`);
      up.run("alpha_digest", digestAlpha);
      up.run("beta_digest", digestBeta);
    } finally {
      if (!LEAVE_TEMP_FILES) {
        await rm(tmp, { recursive: true, force: true });
      }
    }
  }

  // files = absolute paths
  // root = they should all be in root
  function makeRelative(files: string[], root: string) {
    return files.map((file) =>
      file.startsWith(root) ? file.slice(root.length + 1) : file,
    );
  }

  main();
}

cliEntrypoint<MergeRsyncOptions>(import.meta.url, buildProgram, runMerge, {
  label: "merge",
});
