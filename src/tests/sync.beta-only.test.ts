import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "../db.js";
import { spawn } from "node:child_process";

const DIST = path.resolve(__dirname, "../../dist");

function runDist(scriptRel: string, args: string[] = []) {
  return new Promise<void>((resolve, reject) => {
    const script = path.join(DIST, scriptRel);
    const proc = spawn(process.execPath, [script, ...args], {
      stdio: "inherit",
      env: process.env,
    });
    proc.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${scriptRel} exited ${code}`)),
    );
    proc.on("error", reject);
  });
}

describe("pending copies protect beta", () => {
  const originalTraceEnv = process.env.REFLECT_TRACE_ALL;
  let executeThreeWayMerge: typeof import("../three-way-merge.js").executeThreeWayMerge;
  let syncConfirmedCopiesToBase: typeof import("../copy-pending.js").syncConfirmedCopiesToBase;
  let createLogicalClock: typeof import("../logical-clock.js").createLogicalClock;

  beforeAll(() => {
    process.env.REFLECT_TRACE_ALL = "1";
    return (async () => {
      ({ executeThreeWayMerge } = await import("../three-way-merge.js"));
      ({ syncConfirmedCopiesToBase } = await import("../copy-pending.js"));
      ({ createLogicalClock } = await import("../logical-clock.js"));
    })();
  });
  afterAll(() => {
    if (originalTraceEnv === undefined) delete process.env.REFLECT_TRACE_ALL;
    else process.env.REFLECT_TRACE_ALL = originalTraceEnv;
  });

  test("beta-only modifications never trigger alpha->beta operations", async () => {
    const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "pending-beta-"));
    const alphaRoot = path.join(tmp, "alpha");
    const betaRoot = path.join(tmp, "beta");
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRoot, { recursive: true });
    const alphaDb = path.join(tmp, "alpha.db");
    const betaDb = path.join(tmp, "beta.db");
    const baseDb = path.join(tmp, "base.db");
    const traceDb = path.join(tmp, "trace.db");

    const fileBeta = path.join(betaRoot, "logs", "beta.log");
    await fsp.mkdir(path.dirname(fileBeta), { recursive: true });

    const logicalClock = await createLogicalClock([alphaDb, betaDb, baseDb]);

    const runCycle = async (content: string) => {
      await fsp.writeFile(fileBeta, content, "utf8");
      const scanTick = logicalClock.next();
      const scanArgs = (root: string, db: string) => [
        "--root",
        root,
        "--db",
        db,
        "--hash",
        "sha256",
        "--clock-base",
        alphaDb,
        "--clock-base",
        betaDb,
        "--clock-base",
        baseDb,
        "--scan-tick",
        String(scanTick),
      ];
      await runDist("scan.js", scanArgs(alphaRoot, alphaDb));
      await runDist("scan.js", scanArgs(betaRoot, betaDb));
      syncConfirmedCopiesToBase(alphaDb, baseDb);
      syncConfirmedCopiesToBase(betaDb, baseDb);
      await executeThreeWayMerge({
        alphaDb,
        betaDb,
        baseDb,
        alphaRoot,
        betaRoot,
        prefer: "alpha",
        strategyName: "last-write-wins",
        logicalClock,
        traceDbPath: traceDb,
      });
    };

    // Initial content plus a few rewrites on beta.
    for (let i = 0; i < 3; i += 1) {
      await runCycle(`cycle-${i}-${Date.now()}`);
    }

    const trace = new Database(traceDb);
    try {
      const rows = trace
        .prepare(
          `SELECT operation FROM trace_entries WHERE operation IN ('copy alpha->beta','delete beta')`,
        )
        .all() as { operation: string }[];
      expect(rows.length).toBe(0);
    } finally {
      trace.close();
      await fsp.rm(tmp, { recursive: true, force: true });
    }
  });
});
