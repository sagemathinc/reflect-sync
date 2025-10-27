import { mkCase, sync } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { spawn } from "node:child_process";

const SCAN = join(__dirname, "../../dist/scan.js");
const INGEST = join(__dirname, "../../dist/ingest-delta.js");

function runCapture(cmd: string, args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const p = spawn(process.execPath, [cmd, ...args], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    p.stdout!.on("data", (d) => (out += d.toString()));
    let err = "";
    p.stderr!.on("data", (d) => (err += d.toString()));
    p.on("exit", (code) =>
      code === 0
        ? resolve(out)
        : reject(new Error(`${cmd} failed: ${code}: ${err}`)),
    );
  });
}

function runIngestWithPreface(
  prefaceLines: any[],
  payload: string,
  db: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const p = spawn(process.execPath, [INGEST, "--db", db], {
      stdio: ["pipe", "inherit", "inherit"],
    });
    const preface = prefaceLines.map((o) => JSON.stringify(o) + "\n").join("");
    p.stdin!.write(preface);
    p.stdin!.write(payload);
    p.stdin!.end();
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`ingest failed: ${code}`)),
    );
  });
}

describe("LWW with clock skew (beta considered remote via ingest)", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-lww-3-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("beta clock fast by +60s: local newer change should still win after skew adjust", async () => {
    const r = await mkCase(tmp, "t-lww-skew");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    // seed base from alpha
    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    // 1) beta modifies (we'll ingest it with a fast remote clock)
    await fsp.writeFile(b, "beta-change");

    // Capture a beta scan delta (local clock)
    const delta = await runCapture(SCAN, [
      "--root",
      r.bRoot,
      "--emit-delta",
      "--db",
      r.bDb,
    ]);

    // Ingest the delta into beta.db but tell ingest the "remote now" is +60s
    const fakeRemoteNow = Date.now() + 60_000;
    await runIngestWithPreface(
      [{ kind: "time", now: fakeRemoteNow }],
      delta,
      r.bDb,
    );

    // 2) alpha modifies *after* beta, in real time
    await fsp.writeFile(a, "alpha-newer");

    // Merge: LWW should pick alpha (true newer) even if beta's raw mtime looked newer
    await sync(r, "alpha");
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-newer");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-newer");
  });
});
