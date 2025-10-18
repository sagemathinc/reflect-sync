// tests/sync.basic-2.test.ts

import { sync, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join, dirname } from "node:path";
import os from "node:os";
import crypto from "node:crypto";

describe("ccsync: scan → merge-rsync simple tests and conflicts (part 2)", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-test-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("unicode & spaces & newline in path", async () => {
    const r = await mkCase(tmp, "t-unicode-spaces");
    const a = join(r.aRoot, "s\np ace/üñîçødé.txt");
    const b = join(r.bRoot, "s\np ace/üñîçødé.txt");
    await fsp.mkdir(dirname(a), { recursive: true });
    await fsp.writeFile(a, "uni");
    await sync(r, "alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("uni");
  });

  test("binary file content", async () => {
    const r = await mkCase(tmp, "t-binary");
    const a = join(r.aRoot, "bin.dat");
    const b = join(r.bRoot, "bin.dat");
    const buf1 = crypto.randomBytes(1024);
    const buf2 = crypto.randomBytes(1024);
    await fsp.writeFile(a, buf1);
    await sync(r, "alpha");
    expect((await fsp.readFile(b)).equals(buf1)).toBe(true);
    await fsp.writeFile(a, buf2);
    await sync(r, "alpha");
    expect((await fsp.readFile(b)).equals(buf2)).toBe(true);
  });

  test("type flip at same path: file→dir", async () => {
    const r = await mkCase(tmp, "t-type-flip");
    const aFile = join(r.aRoot, "flip");
    const bFile = join(r.bRoot, "flip");
    await fsp.writeFile(aFile, "x");
    await sync(r, "alpha");
    expect(await fileExists(bFile)).toBe(true);

    // replace file with directory on alpha
    await fsp.rm(aFile);
    await fsp.mkdir(aFile);
    await fsp.writeFile(join(aFile, "inside"), "ok");

    await sync(r, "alpha");
    // destination should mirror: file gone, dir exists with file inside
    await expect(fileExists(join(r.bRoot, "flip", "inside"))).resolves.toBe(
      true,
    );
  });

  // Informational: current behavior (ignored)
  test("symlink is ignored by scanner", async () => {
    const r = await mkCase(tmp, "t-symlink-ignored");
    const a = join(r.aRoot, "ignored-link");
    try {
      await fsp.symlink("/no/such/target", a);
    } catch {
      // symlink may require privileges on some fs; skip silently
      return;
    }
    await sync(r, "alpha");
    expect(await fileExists(join(r.bRoot, "ignored-link"))).toBe(false);
  });
});
