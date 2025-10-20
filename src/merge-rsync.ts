#!/usr/bin/env node
// merge-rsync.ts
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtemp, rm, writeFile, stat as fsStat } from "node:fs/promises";
import path from "node:path";
import { Command, Option } from "commander";
import { cliEntrypoint } from "./cli-util.js";
import { getDb } from "./db.js";
import Database from "better-sqlite3";

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
    .option("--dry-run", "simulate without changing files", false)
    .option("--verbose", "enable verbose logging", false);
}

type MergeRsyncOptions = {
  alphaRoot: string;
  betaRoot: string;
  alphaDb: string;
  betaDb: string;
  baseDb: string;
  alphaHost: string;
  betaHost: string;
  prefer: string;
  dryRun: string;
  verbose: string;
};

// ---------- helpers ----------
function join0(items: string[]) {
  const filtered = items.filter(Boolean);
  return filtered.length
    ? Buffer.from(filtered.join("\0") + "\0")
    : Buffer.alloc(0);
}

export async function runMergeRsync({
  alphaRoot,
  betaRoot,
  alphaDb,
  betaDb,
  baseDb,
  alphaHost,
  betaHost,
  prefer,
  dryRun,
  verbose,
}: MergeRsyncOptions) {
  function rsyncArgsBase() {
    const a = ["-a", "-I", "--relative"];
    if (dryRun) a.unshift("-n");
    if (verbose) a.push("-v");
    return a;
    // NOTE: -I disables rsync's quick-check so listed files always copy.
  }

  function rsyncArgsDirs() {
    // -d: transfer directories themselves (no recursion) — needed for empty dirs
    const a = ["-a", "-d", "--relative", "--from0"];
    if (dryRun) a.unshift("-n");
    if (verbose) a.push("-v");
    return a;
  }

  function rsyncArgsDelete() {
    const a = [
      "-a",
      "--relative",
      "--from0",
      "--ignore-missing-args",
      "--delete-missing-args",
      "--force",
    ];
    if (dryRun) a.unshift("-n");
    if (verbose) a.push("-v");
    return a;
  }

  async function fileNonEmpty(p: string) {
    try {
      return (await fsStat(p)).size > 0;
    } catch (err) {
      if (verbose) console.warn("fileNonEmpty ", p, err);
      return false;
    }
  }

  function run(
    cmd: string,
    args: string[],
    okCodes: number[] = [0],
  ): Promise<{ code: number | null; ok: boolean; zero: boolean }> {
    if (verbose)
      console.log(
        `$ ${cmd} ${args.map((a) => (/\s/.test(a) ? JSON.stringify(a) : a)).join(" ")}`,
      );
    return new Promise((resolve) => {
      const p = spawn(cmd, args, { stdio: "inherit" });
      p.on("exit", (code) => {
        const zero = code === 0;
        const ok = code !== null && okCodes.includes(code!);
        resolve({ code, ok, zero });
      });
      p.on("error", () =>
        resolve({ code: 1, ok: okCodes.includes(1), zero: false }),
      );
    });
  }

  function ensureTrailingSlash(root: string): string {
    return root.endsWith("/") ? root : root + "/";
  }

  // small helpers used by safety rails
  const asSet = (xs: string[]) => new Set(xs);
  const uniq = (xs: string[]) => Array.from(asSet(xs));
  const setMinus = (xs: string[], bad: Set<string>) =>
    xs.filter((x) => !bad.has(x));
  const isParentOf = (parent: string, child: string) =>
    parent && (child === parent || child.startsWith(parent + "/"));
  const depth = (r: string) => (r ? r.split("/").length : 0);
  const sortDeepestFirst = (xs: string[]) =>
    xs.slice().sort((a, b) => depth(b) - depth(a));
  const nonRoot = (xs: string[]) => xs.filter((r) => r && r !== ".");

  async function main() {
    // ---------- DB ----------
    // ensure that alppha/beta are setup
    getDb(alphaDb);
    getDb(betaDb);
    const db = new Database(baseDb);
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
    db.pragma("temp_store = MEMORY"); // keep temp tables in RAM for speed

    // base (files) and base_dirs (directories)
    db.exec(`
    CREATE TABLE IF NOT EXISTS base (
      path TEXT PRIMARY KEY,  -- RELATIVE file path
      hash TEXT,
      deleted INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS base_dirs (
      path TEXT PRIMARY KEY,  -- RELATIVE dir path
      deleted INTEGER DEFAULT 0
    );
  `);

    db.prepare(`ATTACH DATABASE ? AS alpha`).run(alphaDb);
    db.prepare(`ATTACH DATABASE ? AS beta`).run(betaDb);

    // ---------- Build relative-path temp tables ----------
    db.exec(
      `DROP TABLE IF EXISTS alpha_rel; DROP TABLE IF EXISTS beta_rel; DROP TABLE IF EXISTS base_rel;`,
    );

    db.prepare(
      `
    CREATE TEMP TABLE alpha_rel AS
    SELECT
      CASE
        WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
        WHEN path = ? THEN '' ELSE path
      END AS rpath,
      hash, deleted
    FROM alpha.files
  `,
    ).run(alphaRoot, alphaRoot, alphaRoot);

    db.prepare(
      `
    CREATE TEMP TABLE beta_rel AS
    SELECT
      CASE
        WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
        WHEN path = ? THEN '' ELSE path
      END AS rpath,
      hash, deleted
    FROM beta.files
  `,
    ).run(betaRoot, betaRoot, betaRoot);

    db.prepare(
      `
    CREATE TEMP TABLE base_rel AS
    SELECT
      CASE
        WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
        WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
        ELSE path
      END AS rpath,
      hash, deleted
    FROM base
  `,
    ).run(alphaRoot, alphaRoot, betaRoot, betaRoot);

    // ---------- Dirs (alpha/beta may not have dirs table yet) ----------
    db.exec(
      `DROP TABLE IF EXISTS alpha_dirs_rel; DROP TABLE IF EXISTS beta_dirs_rel; DROP TABLE IF EXISTS base_dirs_rel;`,
    );

    const hasAlphaDirs =
      (
        db
          .prepare(
            `SELECT 1 AS ok FROM alpha.sqlite_master WHERE type='table' AND name='dirs'`,
          )
          .get() as any
      )?.ok === 1;
    const hasBetaDirs =
      (
        db
          .prepare(
            `SELECT 1 AS ok FROM beta.sqlite_master WHERE type='table' AND name='dirs'`,
          )
          .get() as any
      )?.ok === 1;

    if (hasAlphaDirs) {
      db.prepare(
        `
      CREATE TEMP TABLE alpha_dirs_rel AS
      SELECT
        CASE
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          WHEN path = ? THEN '' ELSE path
        END AS rpath,
        deleted
      FROM alpha.dirs
    `,
      ).run(alphaRoot, alphaRoot, alphaRoot);
    } else {
      db.exec(`CREATE TEMP TABLE alpha_dirs_rel(rpath TEXT, deleted INTEGER);`);
    }

    if (hasBetaDirs) {
      db.prepare(
        `
      CREATE TEMP TABLE beta_dirs_rel AS
      SELECT
        CASE
          WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
          WHEN path = ? THEN '' ELSE path
        END AS rpath,
        deleted
      FROM beta.dirs
    `,
      ).run(betaRoot, betaRoot, betaRoot);
    } else {
      db.exec(`CREATE TEMP TABLE beta_dirs_rel(rpath TEXT, deleted INTEGER);`);
    }

    db.prepare(
      `
    CREATE TEMP TABLE base_dirs_rel AS
    SELECT
      CASE
        WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
        WHEN instr(path, (? || '/')) = 1 THEN substr(path, length(?) + 2)
        ELSE path
      END AS rpath,
      deleted
    FROM base_dirs
  `,
    ).run(alphaRoot, alphaRoot, betaRoot, betaRoot);

    // Index temp rel tables for fast joins/lookups
    db.exec(`
    CREATE INDEX IF NOT EXISTS idx_alpha_rel_rpath ON alpha_rel(rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_rel_rpath  ON beta_rel(rpath);
    CREATE INDEX IF NOT EXISTS idx_base_rel_rpath  ON base_rel(rpath);

    CREATE INDEX IF NOT EXISTS idx_alpha_dirs_rel_rpath ON alpha_dirs_rel(rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_dirs_rel_rpath  ON beta_dirs_rel(rpath);
    CREATE INDEX IF NOT EXISTS idx_base_dirs_rel_rpath  ON base_dirs_rel(rpath);
  `);

    // ---------- 3-way plan on FILES ----------
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

    // ---------- 3-way plan on DIRS ----------
    db.exec(`
    DROP TABLE IF EXISTS tmp_dirs_changedA;
    DROP TABLE IF EXISTS tmp_dirs_changedB;
    DROP TABLE IF EXISTS tmp_dirs_deletedA;
    DROP TABLE IF EXISTS tmp_dirs_deletedB;

    -- "changed" for dirs == exists (not deleted) and not in base or base marked deleted
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

    -- "deleted" for dirs == base shows it existed and side is missing or deleted
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

    // Index the tmp_* tables too (helps joins/NOT IN on large sets)
    db.exec(`
    CREATE INDEX IF NOT EXISTS idx_tmp_changedA_rpath      ON tmp_changedA(rpath);
    CREATE INDEX IF NOT EXISTS idx_tmp_changedB_rpath      ON tmp_changedB(rpath);
    CREATE INDEX IF NOT EXISTS idx_tmp_deletedA_rpath      ON tmp_deletedA(rpath);
    CREATE INDEX IF NOT EXISTS idx_tmp_deletedB_rpath      ON tmp_deletedB(rpath);

    CREATE INDEX IF NOT EXISTS idx_tmp_dirs_changedA_rpath ON tmp_dirs_changedA(rpath);
    CREATE INDEX IF NOT EXISTS idx_tmp_dirs_changedB_rpath ON tmp_dirs_changedB(rpath);
    CREATE INDEX IF NOT EXISTS idx_tmp_dirs_deletedA_rpath ON tmp_dirs_deletedA(rpath);
    CREATE INDEX IF NOT EXISTS idx_tmp_dirs_deletedB_rpath ON tmp_dirs_deletedB(rpath);
  `);

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

    // toBeta FILES
    let toBeta = db
      .prepare(
        `
      SELECT a.rpath
      FROM tmp_changedA a
      WHERE a.rpath NOT IN (SELECT rpath FROM tmp_changedB)
        AND ( ? <> 'beta'
            OR (
                 a.rpath NOT IN (SELECT rpath FROM tmp_deletedB)
                 AND NOT EXISTS (
                   SELECT 1 FROM tmp_dirs_deletedB d
                   WHERE a.rpath = d.rpath OR a.rpath LIKE d.rpath || '/%'
                 )
               )
          )
      UNION ALL
      SELECT cA.rpath FROM tmp_changedA cA
      INNER JOIN tmp_changedB cB USING (rpath)
      WHERE ? = 'alpha'
    `,
      )
      .all(prefer, prefer)
      .map((r) => r.rpath as string);

    // toAlpha FILES
    let toAlpha = db
      .prepare(
        `
      SELECT b.rpath
      FROM tmp_changedB b
      WHERE b.rpath NOT IN (SELECT rpath FROM tmp_changedA)
        AND ( ? <> 'alpha'
              OR (
                   -- exclude exact matches
                   b.rpath NOT IN (SELECT rpath FROM tmp_deletedA)
                   -- exclude descendants of deleted dirs in alpha
                   AND NOT EXISTS (
                     SELECT 1 FROM tmp_dirs_deletedA d
                     WHERE b.rpath = d.rpath OR b.rpath LIKE d.rpath || '/%'
                   )
                 )
            )
      UNION ALL
      SELECT cB.rpath FROM tmp_changedB cB
      INNER JOIN tmp_changedA cA USING (rpath)
      WHERE ? = 'beta'
    `,
      )
      .all(prefer, prefer)
      .map((r) => r.rpath as string);

    // deletions FILES
    let delInBeta = db
      .prepare(
        `
      SELECT dA.rpath
      FROM tmp_deletedA dA
      LEFT JOIN tmp_changedB cB ON cB.rpath = dA.rpath
      WHERE cB.rpath IS NULL
      UNION ALL
      SELECT b.rpath
      FROM base_rel b
      LEFT JOIN alpha_rel a ON a.rpath = b.rpath
      LEFT JOIN tmp_changedB cB ON cB.rpath = b.rpath
      WHERE ?='alpha' AND b.deleted=0 AND (a.rpath IS NULL OR a.deleted=1) AND cB.rpath IS NOT NULL
    `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let delInAlpha = db
      .prepare(
        `
      SELECT dB.rpath
      FROM tmp_deletedB dB
      LEFT JOIN tmp_changedA cA ON cA.rpath = dB.rpath
      WHERE cA.rpath IS NULL
      UNION ALL
      SELECT b.rpath
      FROM base_rel b
      LEFT JOIN beta_rel a ON a.rpath = b.rpath
      LEFT JOIN tmp_changedA cA ON cA.rpath = b.rpath
      WHERE ?='beta' AND b.deleted=0 AND (a.rpath IS NULL OR a.deleted=1) AND cA.rpath IS NOT NULL
    `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    // ---------- DIRS plan ----------
    // toBeta DIRS
    let toBetaDirs = db
      .prepare(
        `
      SELECT rpath FROM tmp_dirs_changedA
      WHERE rpath NOT IN (SELECT rpath FROM tmp_dirs_changedB)
        AND ( ? <> 'beta' OR rpath NOT IN (SELECT rpath FROM tmp_dirs_deletedB))
      UNION ALL
      SELECT cA.rpath FROM tmp_dirs_changedA cA
      INNER JOIN tmp_dirs_changedB cB USING (rpath)
      WHERE ? = 'alpha'
    `,
      )
      .all(prefer, prefer)
      .map((r) => r.rpath as string);

    // toAlpha DIRS
    let toAlphaDirs = db
      .prepare(
        `
      SELECT rpath FROM tmp_dirs_changedB
      WHERE rpath NOT IN (SELECT rpath FROM tmp_dirs_changedA)
        AND ( ? <> 'alpha' OR rpath NOT IN (SELECT rpath FROM tmp_dirs_deletedA))
      UNION ALL
      SELECT cB.rpath FROM tmp_dirs_changedB cB
      INNER JOIN tmp_dirs_changedA cA USING (rpath)
      WHERE ? = 'beta'
    `,
      )
      .all(prefer, prefer)
      .map((r) => r.rpath as string);

    // deletions DIRS
    let delDirsInBeta = db
      .prepare(
        `
      SELECT dA.rpath
      FROM tmp_dirs_deletedA dA
      LEFT JOIN tmp_dirs_changedB cB ON cB.rpath = dA.rpath
      WHERE cB.rpath IS NULL
      UNION ALL
      SELECT b.rpath
      FROM base_dirs_rel b
      LEFT JOIN alpha_dirs_rel a ON a.rpath = b.rpath
      LEFT JOIN tmp_dirs_changedB cB ON cB.rpath = b.rpath
      WHERE ?='alpha' AND b.deleted=0 AND (a.rpath IS NULL OR a.deleted=1) AND cB.rpath IS NOT NULL
    `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    let delDirsInAlpha = db
      .prepare(
        `
      SELECT dB.rpath
      FROM tmp_dirs_deletedB dB
      LEFT JOIN tmp_dirs_changedA cA ON cA.rpath = dB.rpath
      WHERE cA.rpath IS NULL
      UNION ALL
      SELECT b.rpath
      FROM base_dirs_rel b
      LEFT JOIN beta_dirs_rel a ON a.rpath = b.rpath
      LEFT JOIN tmp_dirs_changedA cA ON cA.rpath = b.rpath
      WHERE ?='beta' AND b.deleted=0 AND (a.rpath IS NULL OR a.deleted=1) AND cA.rpath IS NOT NULL
    `,
      )
      .all(prefer)
      .map((r) => r.rpath as string);

    // ---------- TYPE-FLIP (file vs dir) conflicts: delete blocking FILE on dest before creating DIR ----------
    // If alpha has a dir at rpath and beta has a file at same rpath -> delete file in beta
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

    delInBeta = uniq([...delInBeta, ...fileConflictsInBeta]);
    delInAlpha = uniq([...delInAlpha, ...fileConflictsInAlpha]);

    // ---------- SAFETY RAILS ----------
    // de-dupe everything
    toBeta = uniq(toBeta);
    toAlpha = uniq(toAlpha);
    delInBeta = uniq(delInBeta);
    delInAlpha = uniq(delInAlpha);
    toBetaDirs = uniq(toBetaDirs);
    toAlphaDirs = uniq(toAlphaDirs);
    delDirsInBeta = uniq(delDirsInBeta);
    delDirsInAlpha = uniq(delDirsInAlpha);

    // copy/delete overlap (files): favor copy, drop deletion
    const delInBetaSet = asSet(delInBeta);
    const delInAlphaSet = asSet(delInAlpha);
    const overlapBeta = toBeta.filter((r) => delInBetaSet.has(r));
    const overlapAlpha = toAlpha.filter((r) => delInAlphaSet.has(r));
    if (overlapBeta.length || overlapAlpha.length) {
      console.warn(
        `planner safety: copy/delete overlap detected (alpha→beta overlap=${overlapBeta.length}, beta→alpha overlap=${overlapAlpha.length}); dropping deletions for overlapped paths`,
      );
      delInBeta = setMinus(delInBeta, asSet(overlapBeta));
      delInAlpha = setMinus(delInAlpha, asSet(overlapAlpha));
    }

    // dir delete safety: don't delete a dir that's a parent of any copy/create
    const betaIncoming = [...toBeta, ...toBetaDirs];
    const alphaIncoming = [...toAlpha, ...toAlphaDirs];
    delDirsInBeta = delDirsInBeta.filter(
      (d) => !betaIncoming.some((p) => isParentOf(d, p)),
    );
    delDirsInAlpha = delDirsInAlpha.filter(
      (d) => !alphaIncoming.some((p) => isParentOf(d, p)),
    );

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

      async function rsyncCopy(
        fromRoot: string,
        toRoot: string,
        listFile: string,
        label: string,
      ) {
        if (!(await fileNonEmpty(listFile))) {
          if (verbose) console.log(`>>> rsync ${label}: nothing to do`);
          return { zero: false };
        }
        if (verbose) {
          console.log(`>>> rsync ${label} (${fromRoot} -> ${toRoot})`);
        }
        const args = [
          ...rsyncArgsBase(),
          "--from0",
          `--files-from=${listFile}`,
          ensureTrailingSlash(fromRoot),
          ensureTrailingSlash(toRoot),
        ];
        const res = await run("rsync", args, [0, 23, 24]); // accept partials
        if (verbose) {
          console.log(`>>> rsync ${label}: done (code ${res.code})`);
        }
        return { zero: res.zero };
      }

      async function rsyncCopyDirs(
        fromRoot: string,
        toRoot: string,
        listFile: string,
        label: string,
      ) {
        if (!(await fileNonEmpty(listFile))) {
          if (verbose) console.log(`>>> rsync ${label} (dirs): nothing to do`);
          return { zero: false };
        }
        if (verbose) {
          console.log(`>>> rsync ${label} (dirs) (${fromRoot} -> ${toRoot})`);
        }
        const args = [
          ...rsyncArgsDirs(),
          `--files-from=${listFile}`,
          ensureTrailingSlash(fromRoot),
          ensureTrailingSlash(toRoot),
        ];
        const res = await run("rsync", args, [0, 23, 24]);
        if (verbose) {
          console.log(`>>> rsync ${label} (dirs): done (code ${res.code})`);
        }
        return { zero: res.zero };
      }

      async function rsyncDelete(
        fromRoot: string,
        toRoot: string,
        listFile: string,
        label: string,
        opts: { forceEmptySource?: boolean } = {},
      ) {
        if (!(await fileNonEmpty(listFile))) {
          if (verbose) console.log(`>>> rsync delete ${label}: nothing to do`);
          return;
        }

        // Force all listed paths to be "missing" by using an empty temp source dir
        let sourceRoot = ensureTrailingSlash(fromRoot);
        let tmpEmptyDir: string | null = null;
        try {
          if (opts.forceEmptySource) {
            tmpEmptyDir = await mkdtemp(path.join(tmpdir(), "rsync-empty-"));
            sourceRoot = ensureTrailingSlash(tmpEmptyDir);
          }

          if (verbose) {
            console.log(
              `>>> rsync delete ${label} (missing in ${sourceRoot} => delete in ${toRoot})`,
            );
          }
          const args = [
            ...rsyncArgsDelete(), // includes --delete-missing-args --force
            `--files-from=${listFile}`,
            sourceRoot,
            ensureTrailingSlash(toRoot),
          ];
          await run("rsync", args, [0, 24]);
        } finally {
          if (tmpEmptyDir) {
            await rm(tmpEmptyDir, { recursive: true, force: true });
          }
        }
      }

      const alpha = alphaHost ? `${alphaHost}:${alphaRoot}` : alphaRoot;
      const beta = betaHost ? `${betaHost}:${betaRoot}` : betaRoot;

      // 1) delete file conflicts first
      await rsyncDelete(beta, alpha, listDelInAlpha, "alpha deleted (files)");
      await rsyncDelete(alpha, beta, listDelInBeta, "beta deleted (files)");

      // 2) create dirs (so file copies won't fail due to missing parents)
      copyDirsAlphaBetaZero = (
        await rsyncCopyDirs(alpha, beta, listToBetaDirs, "alpha→beta")
      ).zero;
      copyDirsBetaAlphaZero = (
        await rsyncCopyDirs(beta, alpha, listToAlphaDirs, "beta→alpha")
      ).zero;

      // 3) copy files
      copyAlphaBetaZero = (
        await rsyncCopy(alpha, beta, listToBeta, "alpha→beta")
      ).zero;
      copyBetaAlphaZero = (
        await rsyncCopy(beta, alpha, listToAlpha, "beta→alpha")
      ).zero;

      // 4) delete dirs last (after files removed so dirs are empty)
      await rsyncDelete(alpha, beta, listDelDirsInBeta, "beta deleted (dirs)");
      await rsyncDelete(
        beta,
        alpha,
        listDelDirsInAlpha,
        "alpha deleted (dirs)",
      );

      if (verbose) {
        console.log("rsync's all done, now updating database");
      }

      if (dryRun) {
        console.log("(dry-run) skipping base updates");
        console.log("Merge complete.");
        return;
      }

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

      CREATE TEMP TABLE plan_to_beta  (rpath TEXT PRIMARY KEY);
      CREATE TEMP TABLE plan_to_alpha (rpath TEXT PRIMARY KEY);
      CREATE TEMP TABLE plan_del_beta (rpath TEXT PRIMARY KEY);
      CREATE TEMP TABLE plan_del_alpha(rpath TEXT PRIMARY KEY);

      CREATE TEMP TABLE plan_dirs_to_beta  (rpath TEXT PRIMARY KEY);
      CREATE TEMP TABLE plan_dirs_to_alpha (rpath TEXT PRIMARY KEY);
      CREATE TEMP TABLE plan_dirs_del_beta (rpath TEXT PRIMARY KEY);
      CREATE TEMP TABLE plan_dirs_del_alpha(rpath TEXT PRIMARY KEY);
    `);

      const ins = (tbl: string) =>
        db.prepare(`INSERT OR IGNORE INTO ${tbl}(rpath) VALUES (?)`);
      const insToBeta = ins("plan_to_beta");
      const insToAlpha = ins("plan_to_alpha");
      const insDelBeta = ins("plan_del_beta");
      const insDelAlpha = ins("plan_del_alpha");
      const insDirToBeta = ins("plan_dirs_to_beta");
      const insDirToAlpha = ins("plan_dirs_to_alpha");
      const insDirDelBeta = ins("plan_dirs_del_beta");
      const insDirDelAlpha = ins("plan_dirs_del_alpha");

      db.transaction(() => {
        for (const r of toBeta) insToBeta.run(r);
        for (const r of toAlpha) insToAlpha.run(r);
        for (const r of delInBeta) insDelBeta.run(r);
        for (const r of delInAlpha) insDelAlpha.run(r);

        for (const r of toBetaDirs) insDirToBeta.run(r);
        for (const r of toAlphaDirs) insDirToAlpha.run(r);
        for (const r of delDirsInBeta) insDirDelBeta.run(r);
        for (const r of delDirsInAlpha) insDirDelAlpha.run(r);
      })();

      // Index plan tables for faster joins on big sets
      db.exec(`
      CREATE INDEX IF NOT EXISTS idx_plan_to_beta_rpath      ON plan_to_beta(rpath);
      CREATE INDEX IF NOT EXISTS idx_plan_to_alpha_rpath     ON plan_to_alpha(rpath);
      CREATE INDEX IF NOT EXISTS idx_plan_del_beta_rpath     ON plan_del_beta(rpath);
      CREATE INDEX IF NOT EXISTS idx_plan_del_alpha_rpath    ON plan_del_alpha(rpath);

      CREATE INDEX IF NOT EXISTS idx_plan_dirs_to_beta_rpath ON plan_dirs_to_beta(rpath);
      CREATE INDEX IF NOT EXISTS idx_plan_dirs_to_alpha_rpath ON plan_dirs_to_alpha(rpath);
      CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_beta_rpath ON plan_dirs_del_beta(rpath);
      CREATE INDEX IF NOT EXISTS idx_plan_dirs_del_alpha_rpath ON plan_dirs_del_alpha(rpath);
    `);

      // Show plan table sizes in verbose mode
      if (verbose) {
        console.log(
          `Plan table counts: to_beta=${count("plan_to_beta")} to_alpha=${count(
            "plan_to_alpha",
          )} del_beta=${count("plan_del_beta")} del_alpha=${count(
            "plan_del_alpha",
          )}`,
        );
        console.log(
          `Plan dir counts   : d_to_beta=${count(
            "plan_dirs_to_beta",
          )} d_to_alpha=${count("plan_dirs_to_alpha")} d_del_beta=${count(
            "plan_dirs_del_beta",
          )} d_del_alpha=${count("plan_dirs_del_alpha")}`,
        );
      }

      // Now set-based updates in a single transaction (no UPSERT; use OR REPLACE)
      db.transaction(() => {
        if (copyAlphaBetaZero && toBeta.length) {
          db.exec(`
          INSERT OR REPLACE INTO base(path, hash, deleted)
          SELECT p.rpath, a.hash, 0
          FROM plan_to_beta p
          JOIN alpha_rel a ON a.rpath = p.rpath;
        `);
        }

        if (copyBetaAlphaZero && toAlpha.length) {
          db.exec(`
          INSERT OR REPLACE INTO base(path, hash, deleted)
          SELECT p.rpath, b.hash, 0
          FROM plan_to_alpha p
          JOIN beta_rel b ON b.rpath = p.rpath;
        `);
        }

        if (delInBeta.length) {
          db.exec(`
          INSERT OR REPLACE INTO base(path, hash, deleted)
          SELECT rpath, NULL, 1
          FROM plan_del_beta;
        `);
        }

        if (delInAlpha.length) {
          db.exec(`
          INSERT OR REPLACE INTO base(path, hash, deleted)
          SELECT rpath, NULL, 1
          FROM plan_del_alpha;
        `);
        }

        // Directories
        if (copyDirsAlphaBetaZero && toBetaDirs.length) {
          db.exec(`
          INSERT OR REPLACE INTO base_dirs(path, deleted)
          SELECT rpath, 0 FROM plan_dirs_to_beta;
        `);
        }
        if (copyDirsBetaAlphaZero && toAlphaDirs.length) {
          db.exec(`
          INSERT OR REPLACE INTO base_dirs(path, deleted)
          SELECT rpath, 0 FROM plan_dirs_to_alpha;
        `);
        }
        if (delDirsInBeta.length) {
          db.exec(`
          INSERT OR REPLACE INTO base_dirs(path, deleted)
          SELECT rpath, 1 FROM plan_dirs_del_beta;
        `);
        }
        if (delDirsInAlpha.length) {
          db.exec(`
          INSERT OR REPLACE INTO base_dirs(path, deleted)
          SELECT rpath, 1 FROM plan_dirs_del_alpha;
        `);
        }
      })();

      // Optional hygiene on big runs
      db.exec("PRAGMA optimize");
      db.exec("PRAGMA wal_checkpoint(TRUNCATE)");

      if (verbose) {
        console.log("Merge complete.");
      }
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

cliEntrypoint<MergeRsyncOptions>(import.meta.url, buildProgram, runMergeRsync, {
  label: "merge-rsync",
});
