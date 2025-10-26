// tests/sync.dirs.test.ts
//
// pnpm test sync.dirs.test.ts
//
// Directory-focused scenarios: empty dir creation/deletion, deep trees,
// delete-vs-create conflicts (prefer alpha/beta), and a skipped "dir→file"
// current limitation test.

import { sync, dirExists, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";

describe("ccsync: directory semantics", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-test-dirs-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("empty dir create propagates alpha→beta", async () => {
    const r = await mkCase(tmp, "t-empty-dir-create");
    const aDir = join(r.aRoot, "empty/dir");
    const bDir = join(r.bRoot, "empty/dir");
    await fsp.mkdir(aDir, { recursive: true });

    await sync(r, "alpha");

    await expect(dirExists(bDir)).resolves.toBe(true);
  });

  test("empty dir delete propagates alpha→beta", async () => {
    const r = await mkCase(tmp, "t-empty-dir-delete");
    const aDir = join(r.aRoot, "gone/soon");
    const bDir = join(r.bRoot, "gone/soon");

    await fsp.mkdir(aDir, { recursive: true });
    await sync(r, "alpha");
    await expect(dirExists(bDir)).resolves.toBe(true);

    // remove now-empty dir on alpha
    await fsp.rm(aDir, { recursive: true, force: true });
    await sync(r, "alpha");

    await expect(dirExists(bDir)).resolves.toBe(false);
  });

  test("dir persists when file inside is removed (empty dir remains)", async () => {
    const r = await mkCase(tmp, "t-empty-dir-persists");
    const aDir = join(r.aRoot, "keepme");
    const bDir = join(r.bRoot, "keepme");
    const aFile = join(aDir, "child.txt");
    const bFile = join(bDir, "child.txt");

    await fsp.mkdir(aDir, { recursive: true });
    await fsp.writeFile(aFile, "x");
    await sync(r, "alpha");

    await expect(fileExists(bFile)).resolves.toBe(true);
    await fsp.rm(aFile);
    await sync(r, "alpha");

    await expect(fileExists(bFile)).resolves.toBe(false);
    await expect(dirExists(bDir)).resolves.toBe(true); // empty but preserved
  });

  test("deep empty dirs: create nested alpha→beta then delete top-level", async () => {
    const r = await mkCase(tmp, "t-deep-dirs");
    const aTop = join(r.aRoot, "top");
    const aDeep = join(r.aRoot, "top/a/b/c");
    const bTop = join(r.bRoot, "top");
    const bDeep = join(r.bRoot, "top/a/b/c");

    await fsp.mkdir(aDeep, { recursive: true });
    await sync(r, "alpha");

    await expect(dirExists(bDeep)).resolves.toBe(true);

    // deleting top removes subtree
    await fsp.rm(aTop, { recursive: true, force: true });
    await sync(r, "alpha");

    await expect(dirExists(bTop)).resolves.toBe(false);
    await expect(dirExists(bDeep)).resolves.toBe(false);
  });

  test("delete dir on alpha vs add file in same dir on beta (prefer alpha → delete on beta)", async () => {
    const r = await mkCase(tmp, "t-dir-del-vs-create-alpha");
    const aDir = join(r.aRoot, "race");
    const bDir = join(r.bRoot, "race");
    const bFile = join(bDir, "note.txt");

    // seed: dir exists on both
    await fsp.mkdir(aDir, { recursive: true });
    await sync(r, "alpha");

    // alpha deletes dir; beta adds child file
    await fsp.rm(aDir, { recursive: true, force: true });
    await fsp.writeFile(bFile, "beta-has-this");

    await sync(r, "alpha"); // prefer alpha

    // expect alpha's deletion to win
    await expect(dirExists(bDir)).resolves.toBe(false);
    await expect(dirExists(aDir)).resolves.toBe(false);
  });

  test("delete dir on alpha vs add file in same dir on beta (prefer beta → keep & restore)", async () => {
    const r = await mkCase(tmp, "t-dir-del-vs-create-beta");
    const aDir = join(r.aRoot, "race2");
    const bDir = join(r.bRoot, "race2");
    const bFile = join(bDir, "note.txt");
    const aFile = join(aDir, "note.txt");

    // seed
    await fsp.mkdir(aDir, { recursive: true });
    await sync(r, "alpha");

    // alpha deletes dir; beta adds file
    await fsp.rm(aDir, { recursive: true, force: true });
    await fsp.mkdir(bDir, { recursive: true });
    await fsp.writeFile(bFile, "keep-me");

    await sync(r, "beta"); // prefer beta

    // expect beta to win: dir & file remain in beta and are restored to alpha
    await expect(dirExists(bDir)).resolves.toBe(true);
    await expect(dirExists(aDir)).resolves.toBe(true);
    await expect(fileExists(aFile)).resolves.toBe(true);
    await expect(fileExists(bFile)).resolves.toBe(true);
    await expect(fsp.readFile(aFile, "utf8")).resolves.toBe("keep-me");
  });

  // We auto delete a directory to allow a file at same path.
  test("dir→file conflict (prefer beta): currently conservative, dir on alpha remains", async () => {
    const r = await mkCase(tmp, "t-dir-to-file-limit");
    const aDir = join(r.aRoot, "clash");
    const bFile = join(r.bRoot, "clash");

    // seed with directory on alpha
    await fsp.mkdir(aDir, { recursive: true });
    await sync(r, "alpha");

    // beta replaces with a file at same path
    await fsp.rm(bFile, { recursive: true, force: true }).catch(() => {});
    await fsp.writeFile(bFile, "beta-file");

    // prefer beta; this recursively delete dirs automatically (less safe, but
    // the only reasonable automatic thing to do).
    await sync(r, "beta");

    await expect(fileExists(aDir)).resolves.toBe(true);
    await expect(fileExists(bFile)).resolves.toBe(true);
  });
});
