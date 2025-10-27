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

    // base (files) and base_dirs (directories) — now include op_ts for LWW
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
          COALESCE(SUM(deleted), 0)               AS s_del,
          COALESCE(SUM(LENGTH(path)), 0)          AS s_plen,
          COALESCE(SUM(LENGTH(COALESCE(hash,''))), 0) AS s_hlen
        FROM (
          SELECT path, hash, deleted, op_ts FROM ${side}.files
          UNION ALL
          SELECT path, hash, deleted, op_ts FROM ${side}.links
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
          COALESCE(SUM(deleted), 0)       AS s_del,
          COALESCE(SUM(LENGTH(path)), 0)  AS s_plen
        FROM ${side}.dirs
      `,
        )
        .get() as any;

      // A compact, stable string; collisions are extremely unlikely with these aggregates.
      return `F:${f.n},${f.s_op},${f.m_op},${f.s_del},${f.s_plen},${f.s_hlen};D:${d.n},${d.s_op},${d.m_op},${d.s_plen},${d.s_del}`;
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

    db.prepare(
      `
      CREATE TEMP TABLE base_rel AS
      SELECT
        CASE
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          ELSE path
        END AS rpath,
        hash, deleted, op_ts
      FROM base
    `,
    ).run(alphaRoot, alphaRoot, betaRoot, betaRoot);

    // ---------- Dirs (alpha/beta may not have dirs table yet) ----------
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
          deleted, op_ts
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
          deleted, op_ts
        FROM beta.dirs
      `,
    ).run(betaRoot, betaRoot, betaRoot);

    // ----- base

    db.prepare(
      `
      CREATE TEMP TABLE base_dirs_rel AS
      SELECT
        CASE
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          ELSE path
        END AS rpath,
        deleted, op_ts
      FROM base_dirs
    `,
    ).run(alphaRoot, alphaRoot, betaRoot, betaRoot);

    // Index temp rel tables
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_alpha_rel_rpath       ON alpha_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_beta_rel_rpath        ON beta_rel(rpath);
      CREATE INDEX IF NOT EXISTS idx_base_rel_rpath        ON base_rel(rpath);

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

    -- For dirs
    CREATE INDEX IF NOT EXISTS idx_alpha_dirs_rel_deleted_rpath ON alpha_dirs_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_dirs_rel_deleted_rpath  ON beta_dirs_rel(deleted, rpath);
    CREATE INDEX IF NOT EXISTS idx_base_dirs_rel_deleted_rpath  ON base_dirs_rel(deleted, rpath);
    `);
    done();

    done = t("build tmp_* files");
    // ---------- 3-way plan: FILES (change/delete sets) ----------
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

    // Index the tmp_* tables too
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_tmp_changedA_rpath       ON tmp_changedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_changedB_rpath       ON tmp_changedB(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_deletedA_rpath       ON tmp_deletedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_deletedB_rpath       ON tmp_deletedB(rpath);

      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_changedA_rpath  ON tmp_dirs_changedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_changedB_rpath  ON tmp_dirs_changedB(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_deletedA_rpath  ON tmp_dirs_deletedA(rpath);
      CREATE INDEX IF NOT EXISTS idx_tmp_dirs_deletedB_rpath  ON tmp_dirs_deletedB(rpath);
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

    // dir deletions with LWW
    done = t("dir LWW");

    // no-conflict
    const delDirsInBeta_noConflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_dirs_deletedA dA
        LEFT JOIN tmp_dirs_changedB cB ON cB.rpath = dA.rpath
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
        LEFT JOIN tmp_dirs_changedA cA ON cA.rpath = dB.rpath
        WHERE cA.rpath IS NULL
      `,
      )
      .all()
      .map((r) => r.rpath as string);

    // conflict: A deleted dir vs B created dir
    const delDirsInBeta_conflict = db
      .prepare(
        `
        SELECT dA.rpath
        FROM tmp_dirs_deletedA dA
        JOIN tmp_dirs_changedB cB USING (rpath)
        LEFT JOIN alpha_dirs_rel a ON a.rpath = dA.rpath
        LEFT JOIN beta_dirs_rel  b ON b.rpath = dA.rpath
        LEFT JOIN base_dirs_rel br ON br.rpath = dA.rpath
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
        LEFT JOIN alpha_dirs_rel a ON a.rpath = dA.rpath
        LEFT JOIN beta_dirs_rel  b ON b.rpath = dA.rpath
        LEFT JOIN base_dirs_rel br ON br.rpath = dA.rpath
        WHERE COALESCE(b.op_ts, 0) > COALESCE(a.op_ts, br.op_ts, 0) + ?
           OR (ABS(COALESCE(b.op_ts, 0) - COALESCE(a.op_ts, br.op_ts, 0)) <= ? AND ? = 'beta')
      `,
      )
      .all(EPS, EPS, prefer)
      .map((r) => r.rpath as string);

    // conflict: B deleted dir vs A created dir
    const delDirsInAlpha_conflict = db
      .prepare(
        `
        SELECT dB.rpath
        FROM tmp_dirs_deletedB dB
        JOIN tmp_dirs_changedA cA USING (rpath)
        LEFT JOIN alpha_dirs_rel a ON a.rpath = dB.rpath
        LEFT JOIN beta_dirs_rel  b ON b.rpath = dB.rpath
        LEFT JOIN base_dirs_rel br ON br.rpath = dB.rpath
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
        LEFT JOIN alpha_dirs_rel a ON a.rpath = dB.rpath
        LEFT JOIN beta_dirs_rel  b ON b.rpath = dB.rpath
        LEFT JOIN base_dirs_rel br ON br.rpath = dB.rpath
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

    done = t("type flips");

    // ---------- TYPE-FLIP (file vs dir) conflicts ----------
    // If alpha has a dir and beta has a file at same rpath -> delete file in beta
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

    // If beta has a dir and alpha has a file -> delete file in alpha
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

    // If alpha has a dir and beta has a file:
    // - prefer "alpha": delete beta's file
    // - prefer "beta": keep beta's file; remove any accidental scheduling
    if (prefer === "alpha") {
      delInBeta = uniq([...delInBeta, ...fileConflictsInBeta]);
    } else {
      const keep = new Set(fileConflictsInBeta);
      delInBeta = delInBeta.filter((r) => !keep.has(r));
    }

    // If beta has a dir and alpha has a file:
    // - prefer "beta": delete alpha's file
    // - prefer "alpha": keep alpha's file; remove any accidental scheduling
    if (prefer === "beta") {
      delInAlpha = uniq([...delInAlpha, ...fileConflictsInAlpha]);
    } else {
      const keep = new Set(fileConflictsInAlpha);
      delInAlpha = delInAlpha.filter((r) => !keep.has(r));
    }

    // --- Pre-copy dir→file cleanup sets (only used if that side is preferred) ---
    // If beta has a file and alpha has a dir at the same rpath → when prefer=beta,
    // we must remove the dir on alpha before copying files to alpha.
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

    // If alpha has a file and beta has a dir → when prefer=alpha,
    // remove the dir on beta before copying files to beta.
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

    // De-dupe and deepest-first so parent dirs are removed last.
    preDeleteDirsOnAlphaForBetaFiles = sortDeepestFirst(
      nonRoot(uniq(preDeleteDirsOnAlphaForBetaFiles)),
    );
    preDeleteDirsOnBetaForAlphaFiles = sortDeepestFirst(
      nonRoot(uniq(preDeleteDirsOnBetaForAlphaFiles)),
    );

    done();

    done = t("safety rails");

    // ---------- SAFETY RAILS ----------
    // These are not "just" safety -- they are important since doing them
    // in sql is slow.
    toBeta = uniq(toBeta);
    toAlpha = uniq(toAlpha);
    delInBeta = uniq(delInBeta);
    delInAlpha = uniq(delInAlpha);
    toBetaDirs = uniq(toBetaDirs);
    toAlphaDirs = uniq(toAlphaDirs);
    delDirsInBeta = uniq(delDirsInBeta);
    delDirsInAlpha = uniq(delDirsInAlpha);

    // --- JS guard to block copies into a side that deleted a parent dir (by preference) ---
    function makePrefixChecker(dirs: string[]) {
      // Normalize and sort longest-first for early exits
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

    // We only need the raw deleted-dir *candidates* from the tmp tables:
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
      // Block beta→alpha copies that land under a dir alpha deleted
      toAlpha = toAlpha.filter((r) => !underADeleted(r));
    }
    if (prefer === "beta" && deletedDirsB.length) {
      const underBDeleted = makePrefixChecker(deletedDirsB);
      // Block alpha→beta copies that land under a dir beta deleted
      toBeta = toBeta.filter((r) => !underBDeleted(r));
    }
    done();

    // ---------- APPLY IGNORES ----------
    // Drop any rpaths that are ignored on either side. This makes ignores local-only:
    // we do not copy *or* delete ignored paths, and we do not update base for them.
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

    done = t("overlaps");
    const delInBetaSet = asSet(delInBeta);
    const delInAlphaSet = asSet(delInAlpha);
    /** -------- 1) copy/delete overlap (files) in O(n) using Sets -------- */
    if (toBeta.length && delInBeta.length) {
      const toBetaSet = new Set(toBeta);
      const before = delInBeta.length;
      delInBeta = delInBeta.filter((r) => !toBetaSet.has(r));
      const dropped = before - delInBeta.length;
      if (dropped && verbose) {
        console.warn(
          `planner safety: alpha→beta file overlap dropped=${dropped}`,
        );
      }
    }
    if (toAlpha.length && delInAlpha.length) {
      const toAlphaSet = new Set(toAlpha);
      const before = delInAlpha.length;
      delInAlpha = delInAlpha.filter((r) => !toAlphaSet.has(r));
      const dropped = before - delInAlpha.length;
      if (dropped && verbose) {
        console.warn(
          `planner safety: beta→alpha file overlap dropped=${dropped}`,
        );
      }
    }

    /** -------- 2) dir delete safety in O(m log n) via binary search -------- */
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
    /**
     * Returns true if any incoming path is the dir itself or lies under it.
     * incomingSorted must be lexicographically sorted.
     */
    function anyIncomingUnderDir(
      incomingSorted: string[],
      dir: string,
    ): boolean {
      const d = normalizeDir(dir);
      if (!d) return incomingSorted.length > 0; // conservative for root

      // 1) exact match at the same path (e.g., file replacing dir)
      const j = lowerBound(incomingSorted, d);
      if (j < incomingSorted.length && incomingSorted[j] === d) return true;

      // 2) descendant under d/
      const prefix = d + "/";
      const i = lowerBound(incomingSorted, prefix);
      return i < incomingSorted.length && incomingSorted[i].startsWith(prefix);
    }

    // Additional safety: if a side is preferred and we are COPYING OUT of a dir on that side,
    // don't delete that dir there. (Protects "prefer beta → keep & restore".)
    if (prefer === "beta" && delDirsInBeta.length) {
      const outgoingFromBetaSorted = [...toAlpha, ...toAlphaDirs].sort();
      const before = delDirsInBeta.length;
      delDirsInBeta = delDirsInBeta.filter(
        (d) => !anyIncomingUnderDir(outgoingFromBetaSorted, d),
      );
      if (verbose && before !== delDirsInBeta.length) {
        console.warn(
          `planner safety: kept ${before - delDirsInBeta.length} beta dirs (preferred source has outgoing under them)`,
        );
      }
    }
    if (prefer === "alpha" && delDirsInAlpha.length) {
      const outgoingFromAlphaSorted = [...toBeta, ...toBetaDirs].sort();
      const before = delDirsInAlpha.length;
      delDirsInAlpha = delDirsInAlpha.filter(
        (d) => !anyIncomingUnderDir(outgoingFromAlphaSorted, d),
      );
      if (verbose && before !== delDirsInAlpha.length) {
        console.warn(
          `planner safety: kept ${before - delDirsInAlpha.length} alpha dirs (preferred source has outgoing under them)`,
        );
      }
    }

    // Sort once; merging files + dirs keeps semantics you had before
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

    // Assert (verbose only)
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
      // ---------- files-from (NUL-separated) ----------
      const listToBeta = path.join(tmp, "toBeta.list");
      const listToAlpha = path.join(tmp, "toAlpha.list");
      const listDelInBeta = path.join(tmp, "delInBeta.list");
      const listDelInAlpha = path.join(tmp, "delInAlpha.list");

      const listToBetaDirs = path.join(tmp, "toBeta.dirs.list");
      const listToAlphaDirs = path.join(tmp, "toAlpha.dirs.list");
      const listDelDirsInBeta = path.join(tmp, "delInBeta.dirs.list");
      const listDelDirsInAlpha = path.join(tmp, "delInAlpha.dirs.list");

      // don't try to delete non-empty folder
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
      let copyAlphaBetaZero = false;
      let copyBetaAlphaZero = false;
      let copyDirsAlphaBetaZero = false;
      let copyDirsBetaAlphaZero = false;

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
        // Delete dirs on alpha so beta's files can be copied over
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
      copyDirsAlphaBetaZero = (
        await rsyncCopyDirs(
          alpha,
          beta,
          listToBetaDirs,
          "alpha→beta",
          rsyncOpts,
        )
      ).zero;
      copyDirsBetaAlphaZero = (
        await rsyncCopyDirs(
          beta,
          alpha,
          listToAlphaDirs,
          "beta→alpha",
          rsyncOpts,
        )
      ).zero;
      done();

      // 3) copy files
      done = t("rsync: 3) copy files");
      copyAlphaBetaZero = (
        await rsyncCopy(alpha, beta, listToBeta, "alpha→beta", rsyncOpts)
      ).zero;
      copyBetaAlphaZero = (
        await rsyncCopy(beta, alpha, listToAlpha, "beta→alpha", rsyncOpts)
      ).zero;
      done();

      // 4) delete dirs last (after files removed so dirs are empty)
      done = t("rsync: 4) delete dirs");
      // Guard: never delete a dir on a side that *also* reports it as created/changed this round
      // when that side is preferred. This protects the “prefer beta keeps & restore” case.
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

      // Insert many rpaths in one statement (chunked to avoid param limits).
      // The param limit in sqlite is by default 32766, so chunk should be less
      // than that.
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

      // (Optional) tiny timer wrapper to see wins
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

      // Now set-based updates in a single transaction (preserve op_ts)
      db.transaction(() => {
        // files copied A->B
        if (copyAlphaBetaZero && toBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, a.hash, 0, a.op_ts
            FROM plan_to_beta p
            JOIN alpha_rel a ON a.rpath = p.rpath;
          `);
        }

        // files copied B->A
        if (copyBetaAlphaZero && toAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, b.hash, 0, b.op_ts
            FROM plan_to_alpha p
            JOIN beta_rel b ON b.rpath = p.rpath;
          `);
        }

        // file deletions in beta (alpha won)
        if (delInBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, NULL, 1,
                   COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_beta p
            LEFT JOIN alpha_rel a ON a.rpath = p.rpath
            LEFT JOIN beta_rel  b ON b.rpath  = p.rpath
            LEFT JOIN base_rel  br ON br.rpath = p.rpath;
          `);
        }

        // file deletions in alpha (beta won)
        if (delInAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base(path, hash, deleted, op_ts)
            SELECT p.rpath, NULL, 1,
                   COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_del_alpha p
            LEFT JOIN beta_rel  b ON b.rpath  = p.rpath
            LEFT JOIN alpha_rel a ON a.rpath = p.rpath
            LEFT JOIN base_rel  br ON br.rpath = p.rpath;
          `);
        }

        // Directories
        if (copyDirsAlphaBetaZero && toBetaDirs.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts)
            SELECT p.rpath, 0, a.op_ts
            FROM plan_dirs_to_beta p
            JOIN alpha_dirs_rel a ON a.rpath = p.rpath;
          `);
        }
        if (copyDirsBetaAlphaZero && toAlphaDirs.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts)
            SELECT p.rpath, 0, b.op_ts
            FROM plan_dirs_to_alpha p
            JOIN beta_dirs_rel b ON b.rpath = p.rpath;
          `);
        }
        if (delDirsInBeta.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts)
            SELECT p.rpath, 1,
                   COALESCE(a.op_ts, b.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_dirs_del_beta p
            LEFT JOIN alpha_dirs_rel a ON a.rpath = p.rpath
            LEFT JOIN beta_dirs_rel  b ON b.rpath  = p.rpath
            LEFT JOIN base_dirs_rel  br ON br.rpath = p.rpath;
          `);
        }
        if (delDirsInAlpha.length) {
          db.exec(`
            INSERT OR REPLACE INTO base_dirs(path, deleted, op_ts)
            SELECT p.rpath, 1,
                   COALESCE(b.op_ts, a.op_ts, br.op_ts, CAST(strftime('%s','now') AS INTEGER)*1000)
            FROM plan_dirs_del_alpha p
            LEFT JOIN beta_dirs_rel  b ON b.rpath  = p.rpath
            LEFT JOIN alpha_dirs_rel a ON a.rpath = p.rpath
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
      // (Only after successful non-dry-run merge.)
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
