import { mkCase, sync } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { setMtimeMs } from "./util";

describe("LWW: basic conflicting edits", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-lww-1-"));
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

    const now = Date.now();
    await setMtimeMs(a, now - 20_000); // alpha older
    await setMtimeMs(b, now - 5_000); // beta newer

    // even with prefer=alpha, LWW should choose beta
    await sync(r, "alpha");

    expect(await fsp.readFile(a, "utf8")).toBe("beta-new");
    expect(await fsp.readFile(b, "utf8")).toBe("beta-new");

    // do it the other way
    // modify both; make alpha "newer"
    await fsp.writeFile(a, "alpha-new-2");
    await fsp.writeFile(b, "beta-old-2");

    await setMtimeMs(a, now - 5_000); // alpha newer
    await setMtimeMs(b, now - 20_000); // beta older
    await sync(r, "beta");
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-new-2");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-new-2");
  });

  test("equal mtimes -> falls back to prefer", async () => {
    const r = await mkCase(tmp, "t-lww-equal");
    const a = join(r.aRoot, "conf.txt");
    const b = join(r.bRoot, "conf.txt");

    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    await fsp.writeFile(a, "alpha-data");
    await fsp.writeFile(b, "beta-data");

    const t = Date.now() - 10_000;
    await setMtimeMs(a, t);
    await setMtimeMs(b, t);

    // prefer=beta should decide the tie
    await sync(r, "beta");
    expect(await fsp.readFile(a, "utf8")).toBe("beta-data");
    expect(await fsp.readFile(b, "utf8")).toBe("beta-data");

    // do it again the other way
    await fsp.writeFile(a, "alpha-next");
    await fsp.writeFile(b, "beta-next");
    const t2 = Date.now() - 5_000;
    await setMtimeMs(a, t2);
    await setMtimeMs(b, t2);
    await sync(r, "alpha");
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-next");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-next");
  });
});
