#!/usr/bin/env node
// merge-rsync.ts
import Database from "better-sqlite3";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtemp, writeFile, stat as fsStat } from "node:fs/promises";
import path from "node:path";

// ---------- tiny CLI ----------
const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) {
  const k = process.argv[i],
    v = process.argv[i + 1];
  if (!k || v === undefined) break;
  args.set(k.replace(/^--/, ""), v);
}
const alphaRoot = args.get("alpha-root")!;
const betaRoot = args.get("beta-root")!;
const alphaDb = args.get("alpha-db") ?? "alpha.db";
const betaDb = args.get("beta-db") ?? "beta.db";
const baseDb = args.get("base-db") ?? "base.db";
const alphaHost = args.get("alpha-host")?.trim();
const betaHost = args.get("beta-host")?.trim();
const prefer = (args.get("prefer") ?? "alpha").toLowerCase();
const dryRun = /^t|1|y/i.test(args.get("dry-run") ?? "");
const verbose = /^t|1|y/i.test(args.get("verbose") ?? "");

// ---------- helpers ----------
function join0(items: string[]) {
  const filtered = items.filter(Boolean);
  return filtered.length
    ? Buffer.from(filtered.join("\0") + "\0")
    : Buffer.alloc(0);
}

function rsyncArgsBase() {
  const a = ["-a", "--inplace", "--relative"];
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
  ];
  if (dryRun) a.unshift("-n");
  if (verbose) a.push("-v");
  return a;
}

async function fileNonEmpty(p: string) {
  try {
    return (await fsStat(p)).size > 0;
  } catch {
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

async function main() {
  // ---------- DB ----------
  const db = new Database(baseDb);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("temp_store = MEMORY"); // keep temp tables in RAM for speed

  db.exec(`
    CREATE TABLE IF NOT EXISTS base (
      path TEXT PRIMARY KEY,  -- stored as RELATIVE PATH
      hash TEXT,
      deleted INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_base_path ON base(path);
  `);

  db.prepare(`ATTACH DATABASE ? AS alpha`).run(alphaDb);
  db.prepare(`ATTACH DATABASE ? AS beta`).run(betaDb);

  // indexes on attached DBs (schema lives there)
  db.exec(`CREATE INDEX IF NOT EXISTS alpha.idx_files_path ON files(path);`);
  db.exec(`CREATE INDEX IF NOT EXISTS beta.idx_files_path  ON files(path);`);

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

  // Index the temp rel tables for fast joins/lookups
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_alpha_rel_rpath ON alpha_rel(rpath);
    CREATE INDEX IF NOT EXISTS idx_beta_rel_rpath  ON beta_rel(rpath);
    CREATE INDEX IF NOT EXISTS idx_base_rel_rpath  ON base_rel(rpath);
  `);

  // ---------- 3-way plan on rpath ----------
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

  const toBeta = db
    .prepare(
      `
    SELECT rpath FROM tmp_changedA
    WHERE rpath NOT IN (SELECT rpath FROM tmp_changedB)
    UNION ALL
    SELECT cA.rpath FROM tmp_changedA cA
    INNER JOIN tmp_changedB cB USING(rpath)
    WHERE ? = 'alpha'
  `,
    )
    .all(prefer)
    .map((r) => r.rpath as string);

  const toAlpha = db
    .prepare(
      `
    SELECT rpath FROM tmp_changedB
    WHERE rpath NOT IN (SELECT rpath FROM tmp_changedA)
    UNION ALL
    SELECT cB.rpath FROM tmp_changedB cB
    INNER JOIN tmp_changedA cA USING(rpath)
    WHERE ? = 'beta'
  `,
    )
    .all(prefer)
    .map((r) => r.rpath as string);

  const delInBeta = db
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

  const delInAlpha = db
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

  // ---------- files-from (NUL-separated) ----------
  const tmp = await mkdtemp(path.join(tmpdir(), "sync-plan-"));
  const listToBeta = path.join(tmp, "toBeta.list");
  const listToAlpha = path.join(tmp, "toAlpha.list");
  const listDelInBeta = path.join(tmp, "delInBeta.list");
  const listDelInAlpha = path.join(tmp, "delInAlpha.list");

  await writeFile(listToBeta, join0(makeRelative(toBeta, betaRoot)));
  await writeFile(listToAlpha, join0(makeRelative(toAlpha, alphaRoot)));
  await writeFile(listDelInBeta, join0(delInBeta));
  await writeFile(listDelInAlpha, join0(delInAlpha));

  console.log(`Plan:
  alpha→beta copies : ${toBeta.length}
  beta→alpha copies : ${toAlpha.length}
  deletions in beta : ${delInBeta.length}
  deletions in alpha: ${delInAlpha.length}
  prefer side       : ${prefer}
  dry-run           : ${dryRun}
  verbose           : ${verbose}
`);

  // ---------- rsync ----------
  let copyAlphaBetaZero = false;
  let copyBetaAlphaZero = false;

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
    const res = await run("rsync", args, [0, 23, 24]); // accept partials but only advance base on exit 0
    if (verbose) {
      console.log(`>>> rsync ${label}: done (code ${res.code})`);
    }
    return { zero: res.zero };
  }

  async function rsyncDelete(
    fromRoot: string,
    toRoot: string,
    listFile: string,
    label: string,
  ) {
    if (!(await fileNonEmpty(listFile))) {
      if (verbose) {
        console.log(`>>> rsync delete ${label}: nothing to do`);
      }
      return;
    }
    console.log(
      `>>> rsync delete ${label} (missing in ${fromRoot} => delete in ${toRoot})`,
    );
    const args = [
      ...rsyncArgsDelete(),
      `--files-from=${listFile}`,
      ensureTrailingSlash(fromRoot),
      ensureTrailingSlash(toRoot),
    ];
    await run("rsync", args, [0, 24]); // 24=vanished is fine for delete-missing-args
  }

  const alpha = alphaHost ? `${alphaHost}:${alphaRoot}` : alphaRoot;
  const beta = betaHost ? `${betaHost}:${betaRoot}` : betaRoot;
  copyAlphaBetaZero = (await rsyncCopy(alpha, beta, listToBeta, "alpha→beta"))
    .zero;
  copyBetaAlphaZero = (await rsyncCopy(beta, alpha, listToAlpha, "beta→alpha"))
    .zero;
  await rsyncDelete(alpha, beta, listDelInBeta, "alpha deleted");
  await rsyncDelete(beta, alpha, listDelInAlpha, "beta deleted");

  console.log("rsync's all done, now updating database");

  if (dryRun) {
    console.log("(dry-run) skipping base updates");
    console.log("Merge complete.");
    return;
  }

  // ---------- set-based base updates (fast) ----------
  // Build plan tables from arrays
  db.exec(`
    DROP TABLE IF EXISTS plan_to_beta;
    DROP TABLE IF EXISTS plan_to_alpha;
    DROP TABLE IF EXISTS plan_del_beta;
    DROP TABLE IF EXISTS plan_del_alpha;

    CREATE TEMP TABLE plan_to_beta  (rpath TEXT PRIMARY KEY);
    CREATE TEMP TABLE plan_to_alpha (rpath TEXT PRIMARY KEY);
    CREATE TEMP TABLE plan_del_beta (rpath TEXT PRIMARY KEY);
    CREATE TEMP TABLE plan_del_alpha(rpath TEXT PRIMARY KEY);
  `);

  const insToBeta = db.prepare(
    `INSERT OR IGNORE INTO plan_to_beta(rpath) VALUES (?)`,
  );
  const insToAlpha = db.prepare(
    `INSERT OR IGNORE INTO plan_to_alpha(rpath) VALUES (?)`,
  );
  const insDelBeta = db.prepare(
    `INSERT OR IGNORE INTO plan_del_beta(rpath) VALUES (?)`,
  );
  const insDelAlpha = db.prepare(
    `INSERT OR IGNORE INTO plan_del_alpha(rpath) VALUES (?)`,
  );

  db.transaction(() => {
    for (const r of toBeta) insToBeta.run(r);
    for (const r of toAlpha) insToAlpha.run(r);
    for (const r of delInBeta) insDelBeta.run(r);
    for (const r of delInAlpha) insDelAlpha.run(r);
  })();

  // Index plan tables for faster joins on big sets
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_plan_to_beta_rpath   ON plan_to_beta(rpath);
    CREATE INDEX IF NOT EXISTS idx_plan_to_alpha_rpath  ON plan_to_alpha(rpath);
    CREATE INDEX IF NOT EXISTS idx_plan_del_beta_rpath  ON plan_del_beta(rpath);
    CREATE INDEX IF NOT EXISTS idx_plan_del_alpha_rpath ON plan_del_alpha(rpath);
  `);

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
  })();

  // Optional hygiene on big runs
  db.exec("PRAGMA optimize");
  db.exec("PRAGMA wal_checkpoint(TRUNCATE)");

  console.log("Merge complete.");
}

// files = absolute paths
// root = they should all be in root
function makeRelative(files: string[], root: string) {
  return (
    files
      // safety filter
      .filter((file) => file.startsWith(root))
      .map((file) => file.slice(root.length+1))
  );
}

main();
