import { runDist, fileExists } from "./util";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";

// set to true to see tons of output when debugging
const VERBOSE = false;
const verboseArg = VERBOSE ? ["--verbose"] : [];

describe("ccsync integration: scan + merge-rsync", () => {
  let tmp: string;
  let aRoot: string, bRoot: string;
  let aDb: string, bDb: string, baseDb: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "ccsync-test-"));
    aRoot = path.join(tmp, "alpha");
    bRoot = path.join(tmp, "beta");
    aDb = path.join(tmp, "alpha.db");
    bDb = path.join(tmp, "beta.db");
    baseDb = path.join(tmp, "base.db");
    await fsp.mkdir(aRoot, { recursive: true });
    await fsp.mkdir(bRoot, { recursive: true });
  });

  afterAll(async () => {
    // comment this out when debugging to inspect tmp dir
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  const sync = async () => {
    // scan both roots (writes to aDb/bDb)
    await runDist("scan.js", [aRoot, "--db", aDb, ...verboseArg]);
    await runDist("scan.js", [bRoot, "--db", bDb, ...verboseArg]);

    // merge prefer alpha
    await runDist("merge-rsync.js", [
      "--alpha-root",
      aRoot,
      "--beta-root",
      bRoot,
      "--alpha-db",
      aDb,
      "--beta-db",
      bDb,
      "--base-db",
      baseDb,
      "--prefer",
      "alpha",
      ...verboseArg,
    ]);
  };

  test("alpha→beta copy, then deletion propagates back", async () => {
    const aFile = path.join(aRoot, "hello.txt");
    const bFile = path.join(bRoot, "hello.txt");

    // create in alpha
    await fsp.writeFile(aFile, "hello world\n", "utf8");
    await sync();
    // verify in beta
    expect(await fileExists(bFile)).toBe(true);
    expect(await fsp.readFile(bFile, "utf8")).toBe("hello world\n");

    // delete in alpha
    await fsp.rm(aFile);
    await sync();
    // verify deletion in beta
    expect(await fileExists(bFile)).toBe(false);
  });

  test("content update in alpha propagates to beta", async () => {
    const aFile = path.join(aRoot, "foo.txt");
    const bFile = path.join(bRoot, "foo.txt");

    await fsp.writeFile(aFile, "v1", "utf8");
    await sync();
    expect(await fsp.readFile(bFile, "utf8")).toBe("v1");

    await fsp.writeFile(aFile, "v2", "utf8");
    await sync();
    expect(await fsp.readFile(bFile, "utf8")).toBe("v2");
  });

  test("change on both sides and see that alpha side is chosen", async () => {
    const aFile = path.join(aRoot, "foo.txt");
    const bFile = path.join(bRoot, "foo.txt");

    await fsp.writeFile(aFile, "v3", "utf8");
    await fsp.writeFile(bFile, "v4", "utf8");
    await sync();
    expect(await fsp.readFile(aFile, "utf8")).toBe("v3");
    expect(await fsp.readFile(bFile, "utf8")).toBe("v3");
  });
});
