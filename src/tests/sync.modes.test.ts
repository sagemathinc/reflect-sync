// tests/sync.modes.test.ts
//
// pnpm test sync.modes.test.ts
//
// File & directory mode propagation + conflict resolution (mode-only).

import { mkCase, sync, fileExists } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";

const getMode = async (p: string) => (await fsp.lstat(p)).mode & 0o777;

describe("reflex-sync: file/dir mode propagation + conflicts", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-test-modes-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("file mode: create + chmod propagate alpha→beta", async () => {
    const r = await mkCase(tmp, "t-file-mode-basic");
    const a = join(r.aRoot, "m.txt");
    const b = join(r.bRoot, "m.txt");

    await fsp.writeFile(a, "data");
    await fsp.chmod(a, 0o640);
    await sync(r, "alpha");

    expect(await fileExists(b)).toBe(true);
    expect(await getMode(b)).toBe(0o640);

    // change mode only (content unchanged)
    await fsp.chmod(a, 0o600);
    await sync(r, "alpha");

    expect(await fileExists(b)).toBe(true);
    expect(await fsp.readFile(b, "utf8")).toBe("data");
    expect(await getMode(b)).toBe(0o600);
  });

  test("directory mode: chmod on alpha propagates to beta", async () => {
    const r = await mkCase(tmp, "t-dir-mode-basic");
    const aDir = join(r.aRoot, "d");
    const bDir = join(r.bRoot, "d");
    const aFile = join(aDir, "x.txt");
    const bFile = join(bDir, "x.txt");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "x");
    await sync(r, "alpha");
    expect(await fileExists(bFile)).toBe(true);

    // change dir mode, keep content intact
    await fsp.chmod(aDir, 0o711);
    await sync(r, "alpha");

    expect(await getMode(bDir)).toBe(0o711);
    expect(await fsp.readFile(bFile, "utf8")).toBe("x");
  });

  test("mode-only conflict on file: prefer=alpha → alpha mode wins", async () => {
    const r = await mkCase(tmp, "t-file-mode-conflict-alpha");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    await fsp.writeFile(a, "seed");
    await fsp.chmod(a, 0o644);
    await sync(r, "alpha");

    // change modes on both sides (content unchanged)
    await fsp.chmod(a, 0o600); // alpha wants 600
    await fsp.chmod(b, 0o640); // beta  wants 640

    // Use a large epsilon so if op_ts is close, it's treated as a tie → prefer decides.
    await sync(r, "alpha", false, undefined);

    expect(await getMode(a)).toBe(0o600);
    expect(await getMode(b)).toBe(0o600);
    expect(await fsp.readFile(a, "utf8")).toBe("seed");
    expect(await fsp.readFile(b, "utf8")).toBe("seed");
  });

  test("mode-only conflict on file: prefer=beta → beta mode wins", async () => {
    const r = await mkCase(tmp, "t-file-mode-conflict-beta");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    await fsp.writeFile(a, "seed");
    await fsp.chmod(a, 0o600);
    await sync(r, "alpha");

    // change modes on both sides (content unchanged)
    await fsp.chmod(a, 0o640); // alpha 640
    await fsp.chmod(b, 0o644); // beta  644

    await sync(r, "beta", false, undefined);

    expect(await getMode(a)).toBe(0o644);
    expect(await getMode(b)).toBe(0o644);
    expect(await fsp.readFile(a, "utf8")).toBe("seed");
    expect(await fsp.readFile(b, "utf8")).toBe("seed");
  });

  test("directory mode conflict: prefer side wins", async () => {
    const r = await mkCase(tmp, "t-dir-mode-conflict");
    const aDir = join(r.aRoot, "cfg");
    const bDir = join(r.bRoot, "cfg");
    const aFile = join(aDir, "k.txt");
    const bFile = join(bDir, "k.txt");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "k");
    await fsp.chmod(aDir, 0o755);
    await sync(r, "alpha");

    // diverge dir modes
    await fsp.chmod(aDir, 0o700); // alpha wants 700
    await fsp.chmod(bDir, 0o711); // beta  wants 711

    // prefer alpha
    await sync(r, "alpha", false, undefined);
    expect(await getMode(aDir)).toBe(0o700);
    expect(await getMode(bDir)).toBe(0o700);
    expect(await fsp.readFile(aFile, "utf8")).toBe("k");
    expect(await fsp.readFile(bFile, "utf8")).toBe("k");

    // diverge again and prefer beta
    await fsp.chmod(aDir, 0o755);
    await fsp.chmod(bDir, 0o711);
    await sync(r, "beta", false, undefined);
    expect(await getMode(aDir)).toBe(0o711);
    expect(await getMode(bDir)).toBe(0o711);
  });
});
