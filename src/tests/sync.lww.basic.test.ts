import { mkCase, sync, syncPrefer } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";

describe("LWW: basic conflicting edits", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "rfsync-lww-1-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("newer beta wins even when prefer=alpha", async () => {
    const r = await mkCase(tmp, "t-lww-newer-beta");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    // seed
    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    // modify both; make beta "newer"
    await fsp.writeFile(a, "alpha-old");
    await fsp.writeFile(b, "beta-new");

    // even with prefer=alpha, LWW should choose beta
    await sync(r, "alpha");

    expect(await fsp.readFile(a, "utf8")).toBe("beta-new");
    expect(await fsp.readFile(b, "utf8")).toBe("beta-new");

    // do it the other way
    // modify both; make alpha "newer"
    await fsp.writeFile(a, "alpha-new-2");
    await fsp.writeFile(b, "beta-old-2");

    await sync(r, "beta", undefined, undefined, {
      scanOrder: ["beta", "alpha"],
    });
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-new-2");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-new-2");
  });

  test("prefer strategy overrides default order when requested", async () => {
    const r = await mkCase(tmp, "t-lww-equal");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    await fsp.writeFile(a, "alpha-data");
    await fsp.writeFile(b, "beta-data");

    // prefer=beta should decide
    await syncPrefer(r, "beta");
    expect(await fsp.readFile(a, "utf8")).toBe("beta-data");
    expect(await fsp.readFile(b, "utf8")).toBe("beta-data");

    // do it again the other way
    await fsp.writeFile(a, "alpha-next");
    await fsp.writeFile(b, "beta-next");
    await syncPrefer(r, "alpha");
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-next");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-next");
  });
});
