/*
FUTURE CAPABILITIES (skipped until implemented)

Nothing skipped here is expected to work at all yet.
*/

import { sync, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import { spawn } from "node:child_process";
import os from "node:os";

describe("rfsync: future capabilities", () => {
  let tmp: string;

  beforeAll(async () => {
    tmp = await fsp.mkdtemp(join(os.tmpdir(), "ccsync-test-"));
  });

  afterAll(async () => {
    // comment out to inspect failures
    await fsp.rm(tmp, { recursive: true, force: true });
  });

  test.skip("propagate mode-only change (chmod +x) without content change", async () => {
    // REQUIREMENTS:
    // - scanner stores file mode
    // - plan treats mode-diff as a change
    // - rsync copy uses `-a` (already) and includes the path
    const r = await mkCase(tmp, "f-mode-only");
    const a = join(r.aRoot, "tool.sh");
    const b = join(r.bRoot, "tool.sh");

    await fsp.writeFile(a, "#!/bin/sh\necho ok\n");
    await sync(r, "alpha");
    const start = (await fsp.stat(b)).mode;

    // flip execute bit only
    await fsp.chmod(a, 0o755);
    await sync(r, "alpha");

    const end = (await fsp.stat(b)).mode;
    expect((end & 0o111) !== (start & 0o111)).toBe(true);
  });

  test.skip("propagate uid/gid using numeric ids", async () => {
    // REQUIREMENTS:
    // - scanner stores uid/gid
    // - plan treats uid/gid diff as a change
    // - rsync invoked with `--numeric-ids`
    // NOTE: requires test to have permission to chown, so likely only in CI root or with user namespaces.
    const r = await mkCase(tmp, "f-ids");
    const a = join(r.aRoot, "file.txt");
    await fsp.writeFile(a, "id\n");
    await sync(r, "alpha");

    // hypothetical: change to same uid, different gid (adjust for your env)
    const st = await fsp.stat(a);
    try {
      await fsp.chown(a, st.uid, st.gid === 0 ? 1000 : 0);
    } catch {
      return; // skip on permission error
    }
    await sync(r, "alpha");

    const bst = await fsp.stat(join(r.bRoot, "file.txt"));
    expect(bst.gid).toBe((await fsp.stat(a)).gid);
  });

  test.skip("preserve hard links (two names, one inode)", async () => {
    // REQUIREMENTS:
    // - rsync invoked with `-H`
    // - (optionally) scanner stores link count/inode to plan intelligently
    const r = await mkCase(tmp, "f-hardlinks");
    const a1 = join(r.aRoot, "h1");
    const a2 = join(r.aRoot, "h2");
    const b1 = join(r.bRoot, "h1");
    const b2 = join(r.bRoot, "h2");

    await fsp.writeFile(a1, "same inode");
    // make hardlink (Linux/Unix)
    await fsp.link(a1, a2);
    await sync(r, "alpha");

    const s1 = await fsp.stat(b1);
    const s2 = await fsp.stat(b2);
    // crude inode equality check: device+ino stable enough for test boxes
    expect(`${s1.dev}:${s1.ino}`).toBe(`${s2.dev}:${s2.ino}`);
  });

  test.skip("propagate xattrs (and xattr-only change triggers resync)", async () => {
    // REQUIREMENTS:
    // - scanner must record a digest of xattrs (or the names+values)
    // - rsync with `-X`
    // This test uses setfattr/getfattr if available; otherwise skip.
    const r = await mkCase(tmp, "f-xattrs");
    const a = join(r.aRoot, "doc");
    const b = join(r.bRoot, "doc");
    await fsp.writeFile(a, "xattr");

    // set an xattr on alpha
    const set = spawn("setfattr", ["-n", "user.test", "-v", "v1", a]);
    await new Promise<void>((res, rej) =>
      set.on("exit", (c) =>
        c === 0 ? res() : rej(new Error("setfattr failed")),
      ),
    ).catch(() => {
      return;
    });

    await sync(r, "alpha");

    // update xattr only
    const set2 = spawn("setfattr", ["-n", "user.test", "-v", "v2", a]);
    await new Promise<void>((res, rej) =>
      set2.on("exit", (c) =>
        c === 0 ? res() : rej(new Error("setfattr failed")),
      ),
    ).catch(() => {
      return;
    });
    await sync(r, "alpha");

    // check dest xattr
    const get = spawn("getfattr", ["--only-values", "-n", "user.test", b]);
    let out = "";
    get.stdout?.on("data", (d) => (out += String(d)));
    await new Promise<void>((res, rej) =>
      get.on("exit", (c) =>
        c === 0 ? res() : rej(new Error("getfattr failed")),
      ),
    ).catch(() => {
      return;
    });
    expect(out.trim()).toBe("v2");
  });

  test.skip("case-only rename on case-insensitive fs behaves sensibly", async () => {
    // REQUIREMENTS:
    // - handle case-insensitive roots (macOS default) to avoid conflicts
    // - scanner may normalize or detect collisions
    const r = await mkCase(tmp, "f-case");
    const a1 = join(r.aRoot, "Name.txt");
    const a2 = join(r.aRoot, "name.txt");

    await fsp.writeFile(a1, "x");
    await sync(r, "alpha");
    await fsp.rename(a1, a2);
    await sync(r, "alpha");

    // On case-insensitive filesystems, there is only one file; ensure beta matches alpha name.
    expect(await fileExists(join(r.bRoot, "name.txt"))).toBe(true);
  });

  test.skip("sparse file stays sparse (block usage roughly preserved)", async () => {
    // REQUIREMENTS:
    // - rsync with `--sparse` (or default behavior sufficient)
    // - test environment supports sparse files
    const r = await mkCase(tmp, "f-sparse");
    const a = join(r.aRoot, "big.img");
    const b = join(r.bRoot, "big.img");

    // create sparse: seek then write
    const fd = await fsp.open(a, "w");
    try {
      await fd.truncate(200 * 1024 * 1024); // 200MB logical
      await fd.write(Buffer.from("end"), 0, 3, 200 * 1024 * 1024 - 3);
    } finally {
      await fd.close();
    }
    await sync(r, "alpha");

    // Expect rsync to keep sparseness; tricky to assert portably—placeholder:
    const st = await fsp.stat(b);
    expect(st.size).toBe(200 * 1024 * 1024);
    // FUTURE: check allocated blocks on Linux via `stat -c %b` or fs.statfs, then compare ratio.
  });
});
