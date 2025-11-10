import { join } from "node:path";
import os from "node:os";
import fsp from "node:fs/promises";

import {
  countSchedulerCycles,
  dirExists,
  linkExists,
  wait,
  waitFor,
} from "./util";
import {
  describeIfSsh,
  startSchedulerRemote,
  stopScheduler,
} from "./ssh.remote-test-util";

const KEEP_TMP = !!process.env.KEEP_SYMLINK_TMP;

describeIfSsh("SSH remote sync – symlink moves", () => {
  let tmp = "";
  let alphaRoot = "";
  let betaRootRemote = "";
  let alphaDb = "";
  let betaDb = "";
  let baseDb = "";

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-symlink-"));
    if (KEEP_TMP) {
      console.log("KEEP_TMP dir:", tmp);
    }
    alphaRoot = join(tmp, "alpha");
    betaRootRemote = join(tmp, "beta");
    alphaDb = join(tmp, "alpha.db");
    betaDb = join(tmp, "beta.db");
    baseDb = join(tmp, "base.db");
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
  });

  beforeEach(async () => {
    await fsp.rm(alphaRoot, { recursive: true, force: true });
    await fsp.rm(betaRootRemote, { recursive: true, force: true });
    await fsp.mkdir(alphaRoot, { recursive: true });
    await fsp.mkdir(betaRootRemote, { recursive: true });
    await Promise.all(
      [alphaDb, betaDb, baseDb].map((p) =>
        fsp.rm(p, { recursive: true, force: true }).catch(() => {}),
      ),
    );
  });

  afterAll(async () => {
    if (!KEEP_TMP && tmp) {
      await fsp.rm(tmp, { recursive: true, force: true });
    }
  });

  it("create directory that is target of symlink, sync, move directory, sync", async () => {
    const child = startSchedulerRemote({
      alphaRoot,
      betaRootRemote,
      alphaDb,
      betaDb,
      baseDb,
      prefer: "alpha",
    });

    try {
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= 1,
        15_000,
        10,
      );
      await fsp.mkdir(join(alphaRoot, "x"));
      await fsp.symlink("x", join(alphaRoot, "x.link"));

      let m = countSchedulerCycles(baseDb);
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= m + 1,
        15_000,
        10,
      );

      await expect(linkExists(join(betaRootRemote, "x.link")));
      await expect(dirExists(join(betaRootRemote, "x")));

      // prefer is alpha, so don't immediately do the rename:
      await wait(3000);
      await fsp.rename(join(betaRootRemote, "x"), join(betaRootRemote, "x2"));

      m = countSchedulerCycles(baseDb);
      await waitFor(
        () => countSchedulerCycles(baseDb),
        (n) => n >= m + 1,
        15_000,
        10,
      );

      const betaListing = new Set(await fsp.readdir(betaRootRemote));
      const expectedListing = betaListing;
      const alphaListing = new Set(await fsp.readdir(alphaRoot));

      expect(expectedListing).toEqual(alphaListing);

      await expect(linkExists(join(alphaRoot, "x.link")));
      await expect(linkExists(join(betaRootRemote, "x.link")));
    } finally {
      await stopScheduler(child);
    }
  }, 40_000);
});
