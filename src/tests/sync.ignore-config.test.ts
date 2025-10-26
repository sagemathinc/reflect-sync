import { sync, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { IGNORE_FILE } from "../constants";

describe("planner ignores IGNORE_FILE itself", () => {
  let tmp: string;
  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-ignore-config-"));
  });
  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test(".ccsyncignore is not copied alpha→beta", async () => {
    const r = await mkCase(tmp, "t-no-copy-config");
    const aCfg = join(r.aRoot, IGNORE_FILE);
    const bCfg = join(r.bRoot, IGNORE_FILE);

    await fsp.writeFile(aCfg, "dist/\n");
    await sync(r, "alpha");

    await expect(fileExists(bCfg)).resolves.toBe(false);
  });

  test(".ccsyncignore on beta stays even if missing on alpha (no delete)", async () => {
    const r = await mkCase(tmp, "t-no-delete-config");
    const bCfg = join(r.bRoot, IGNORE_FILE);

    await fsp.writeFile(bCfg, "build/\n");
    await sync(r, "alpha"); // prefer alpha, which lacks the file

    await expect(fileExists(bCfg)).resolves.toBe(true); // not deleted
  });
});
