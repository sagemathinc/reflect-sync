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
function run(
  cmd: string,
  args: string[],
  okCodes: number[] = [0],
): Promise<void> {
  if (verbose)
    console.log(
      `$ ${cmd} ${args.map((a) => (/\s/.test(a) ? JSON.stringify(a) : a)).join(" ")}`,
    );
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: "inherit" });
    p.on("exit", (code) =>
      code !== null && okCodes.includes(code)
        ? resolve()
        : reject(new Error(`${cmd} exited ${code}`)),
    );
    p.on("error", reject);
  });
}
async function fileNonEmpty(p: string) {
  try {
    return (await fsStat(p)).size > 0;
  } catch {
    return false;
  }
}

async function main() {
  // ---------- DB ----------
  const db = new Database(baseDb);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
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

  // indexes on attached DBs (note schema lives there)
  db.exec(`CREATE INDEX IF NOT EXISTS alpha.idx_files_path ON files(path);`);
  db.exec(`CREATE INDEX IF NOT EXISTS beta.idx_files_path  ON files(path);`);

  // ---------- Build relative-path temp tables in SQL ----------
  db.exec(
    `DROP TABLE IF EXISTS alpha_rel; DROP TABLE IF EXISTS beta_rel; DROP TABLE IF EXISTS base_rel;`,
  );

  // alpha_rel: rpath = alpha.files.path stripped of alphaRoot + '/'
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

  // beta_rel: rpath = beta.files.path stripped of betaRoot + '/'
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

  // base_rel: rpath = base.path stripped of either alphaRoot or betaRoot (if previously stored absolute)
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

  await writeFile(listToBeta, join0(toBeta));
  await writeFile(listToAlpha, join0(toAlpha));
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
  async function rsyncCopy(
    fromRoot: string,
    toRoot: string,
    listFile: string,
    label: string,
  ) {
    if (!(await fileNonEmpty(listFile))) {
      if (verbose) {
        console.log(`>>> rsync ${label}: nothing to do`);
      }
      return;
    }
    if (verbose) {
      console.log(`>>> rsync ${label} (${fromRoot} -> ${toRoot})`);
    }
    const args = [
      ...rsyncArgsBase(),
      "--from0",
      `--files-from=${listFile}`,
      fromRoot.endsWith("/") ? fromRoot : fromRoot + "/",
      toRoot.endsWith("/") ? toRoot : toRoot + "/",
    ];
    await run("rsync", args);
    if (verbose) {
      console.log(`>>> rsync ${label}: done`);
    }
  }

  async function rsyncDelete(
    fromRoot: string,
    toRoot: string,
    listFile: string,
    label: string,
  ) {
    if (!(await fileNonEmpty(listFile))) {
      if (verbose) console.log(`>>> rsync delete ${label}: nothing to do`);
      return;
    }
    console.log(
      `>>> rsync delete ${label} (missing in ${fromRoot} => delete in ${toRoot})`,
    );
    const args = [
      ...rsyncArgsDelete(),
      `--files-from=${listFile}`,
      fromRoot.endsWith("/") ? fromRoot : fromRoot + "/",
      toRoot.endsWith("/") ? toRoot : toRoot + "/",
    ];
    // With --delete-missing-args, exit 24 is normal/ok when entries don't exist on sender.
    await run("rsync", args, [0, 24]);
  }

  await rsyncCopy(alphaRoot, betaRoot, listToBeta, "alpha→beta");
  await rsyncCopy(betaRoot, alphaRoot, listToAlpha, "beta→alpha");
  await rsyncDelete(alphaRoot, betaRoot, listDelInBeta, "alpha deleted");
  await rsyncDelete(betaRoot, alphaRoot, listDelInAlpha, "beta deleted");

  console.log("rsync's all done, now updating database");
  // ---------- update base to the MERGED result (store RELATIVE paths) ----------
  const upsertBase = db.prepare(`
  INSERT INTO base(path,hash,deleted) VALUES (?,?,?)
  ON CONFLICT(path) DO UPDATE SET hash=excluded.hash, deleted=excluded.deleted
`);
  const aHashStmt = db.prepare(`SELECT hash FROM alpha_rel WHERE rpath = ?`);
  const bHashStmt = db.prepare(`SELECT hash FROM beta_rel  WHERE rpath = ?`);

  const tx = db.transaction(() => {
    for (const r of toBeta) {
      upsertBase.run(r, aHashStmt.get(r)?.hash ?? null, 0);
    }
    for (const r of toAlpha) {
      upsertBase.run(r, bHashStmt.get(r)?.hash ?? null, 0);
    }
    for (const r of delInBeta) {
      upsertBase.run(r, null, 1);
    }
    for (const r of delInAlpha) {
      upsertBase.run(r, null, 1);
    }
  });
  console.log("commit transaction");
  tx();

  console.log("database updated!");
  console.log("Merge complete.");
}

main();
