// tests/sync.basic.test.ts

import { sync, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join, dirname } from "node:path";
import os from "node:os";

describe("scan → merge simple tests and conflicts (part 1)", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-test-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test.only("create + delete propagate alpha→beta", async () => {
    const r = await mkCase(tmp, "t-create-delete");
    const aFile = join(r.aRoot, "hello.txt");
    const bFile = join(r.bRoot, "hello.txt");
    await fsp.writeFile(aFile, "hello\n");
    await sync(r, "alpha");
    expect(await fileExists(bFile)).toBe(true);
    expect(await fsp.readFile(bFile, "utf8")).toBe("hello\n");
    await fsp.rm(aFile);
    await sync(r, "alpha");
    expect(await fileExists(bFile)).toBe(false);
  });

  test("same-size content update (v1→v2) overwrites due to -I", async () => {
    const r = await mkCase(tmp, "t-same-size");
    const a = join(r.aRoot, "foo.txt");
    const b = join(r.bRoot, "foo.txt");
    await fsp.writeFile(a, "v1");
    await sync(r, "alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("v1");
    // same byte length
    await fsp.writeFile(a, "v2");
    await sync(r, "alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("v2");
  });

  test("nested directories create/modify/delete", async () => {
    const r = await mkCase(tmp, "t-nested");
    const a = join(r.aRoot, "dir/sub/deeper/file.txt");
    const b = join(r.bRoot, "dir/sub/deeper/file.txt");
    await fsp.mkdir(dirname(a), { recursive: true });
    await fsp.writeFile(a, "one");
    await sync(r, "alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("one");
    await fsp.writeFile(a, "two");
    await sync(r, "alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("two");
    await fsp.rm(a);
    await sync(r, "alpha");
    expect(await fileExists(b)).toBe(false);
  });

  test("rename/move is delete+create: beta mirrors alpha", async () => {
    const r = await mkCase(tmp, "t-rename");
    const a1 = join(r.aRoot, "old/name.txt");
    const b1 = join(r.bRoot, "old/name.txt");
    const a2 = join(r.aRoot, "new/name.txt");
    const b2 = join(r.bRoot, "new/name.txt");
    await fsp.mkdir(dirname(a1), { recursive: true });
    await fsp.writeFile(a1, "X");
    await sync(r, "alpha");
    expect(await fileExists(b1)).toBe(true);
    await fsp.mkdir(dirname(a2), { recursive: true });
    await fsp.rename(a1, a2);
    await sync(r, "alpha");
    expect(await fileExists(b1)).toBe(false);
    expect(await fsp.readFile(b2, "utf8")).toBe("X");
  });

  test("conflict: modify both sides, prefer=alpha → alpha wins", async () => {
    const r = await mkCase(tmp, "t-conflict-alpha");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");
    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");
    await fsp.writeFile(a, "alpha");
    await fsp.writeFile(b, "beta");
    await sync(r, "alpha");
    expect(await fsp.readFile(a, "utf8")).toBe("alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha");
  });

  test("conflict: modify both sides, prefer=beta → beta wins", async () => {
    const r = await mkCase(tmp, "t-conflict-beta");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");
    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");
    await fsp.writeFile(a, "alpha");
    await fsp.writeFile(b, "beta");
    await sync(r, "beta");
    expect(await fsp.readFile(a, "utf8")).toBe("beta");
    expect(await fsp.readFile(b, "utf8")).toBe("beta");
  });

  test("delete vs modify: beta deletes while alpha modifies (prefer alpha → restore to beta)", async () => {
    const r = await mkCase(tmp, "t-del-vs-mod-alpha");
    const a = join(r.aRoot, "x.txt");
    const b = join(r.bRoot, "x.txt");
    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");
    await fsp.writeFile(a, "alpha2"); // modify alpha
    await fsp.rm(b); // delete on beta
    await sync(r, "alpha");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha2");
  });

  test("delete vs modify: beta deletes while alpha modifies (prefer beta → delete on alpha)", async () => {
    const r = await mkCase(tmp, "t-del-vs-mod-beta");
    const a = join(r.aRoot, "x.txt");
    const b = join(r.bRoot, "x.txt");
    await fsp.writeFile(a, "seed");
    await sync(r, "beta");
    await fsp.writeFile(a, "alpha2"); // modify alpha
    await fsp.rm(b); // delete on beta
    await sync(r, "beta");
    expect(await fileExists(a)).toBe(false);
    expect(await fileExists(b)).toBe(false);
  });
});
