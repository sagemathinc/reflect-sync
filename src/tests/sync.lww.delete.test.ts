import { mkCase, sync, fileExists } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import os from "node:os";
import { setMtimeMs } from "./util";

describe("LWW: delete vs modify", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-lww-2-"));
  });

  afterAll(async () => {
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test("delete newer than modify -> delete wins", async () => {
    const r = await mkCase(tmp, "t-lww-del-newer");
    const a = join(r.aRoot, "x.txt");
    const b = join(r.bRoot, "x.txt");

    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    // modify alpha (older op_ts)
    await fsp.writeFile(a, "alpha-old");
    await setMtimeMs(a, Date.now() - 20_000);

    // delete beta now (delete op_ts observed at scan time, i.e. ~now)
    await fsp.rm(b);

    // prefer doesn't matter; LWW by op_ts(delete) should win
    await sync(r, "alpha");
    expect(await fileExists(a)).toBe(false);
    expect(await fileExists(b)).toBe(false);
  });

  test("modify newer than prior delete -> modify wins (restores)", async () => {
    const r = await mkCase(tmp, "t-lww-mod-newer");
    const a = join(r.aRoot, "y.txt");
    const b = join(r.bRoot, "y.txt");

    await fsp.writeFile(a, "seed");
    await sync(r, "alpha");

    // delete on beta first (older delete op_ts after next scan)
    await fsp.rm(b);
    // then modify alpha later (newer op_ts)
    await fsp.writeFile(a, "alpha-newer");
    await setMtimeMs(a, Date.now() - 2_000);

    await sync(r, "beta"); // prefer irrelevant; newer op_ts should restore to beta
    expect(await fsp.readFile(a, "utf8")).toBe("alpha-newer");
    expect(await fsp.readFile(b, "utf8")).toBe("alpha-newer");
  });
});
