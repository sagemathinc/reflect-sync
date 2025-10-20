/*
FUTURE CAPABILITIES (skipped until implemented)

Nothing skipped here is expected to work at all yet.
*/

import { sync, fileExists, mkCase } from "./util";
import fsp from "node:fs/promises";
import { join } from "node:path";
import { spawn } from "node:child_process";
import os from "node:os";

describe("ccsync: future capabilities", () => {
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

  test.skip("sync empty directories (create and delete)", async () => {
    // REQUIREMENTS:
    // - scanner must index directories (not just files)
    // - plan must create/delete empty dirs
    // - rsync create step must include dirs (--dirs or equivalent) when needed
    const r = await mkCase(tmp, "f-empty-dirs");
    const aDir = join(r.aRoot, "empty/dir");
    const bDir = join(r.bRoot, "empty/dir");

    await fsp.mkdir(aDir, { recursive: true });
    await sync(r, "alpha");

    await expect(fileExists(bDir)).resolves.toBe(true);

    // delete on alpha -> should be deleted on beta
    await fsp.rm(join(r.aRoot, "empty"), { recursive: true, force: true });
    await sync(r, "alpha");

    await expect(fileExists(bDir)).resolves.toBe(false);
  });

  test.skip("preserve and sync symlinks as links (not dereferenced)", async () => {
    // REQUIREMENTS:
    // - scanner must index symlinks with link target
    // - plan treats link target change as change
    // - rsync copy uses `-l`/`-a` (keeps links)
    const r = await mkCase(tmp, "f-symlinks");
    const target = join(r.aRoot, "data.txt");
    const linkA = join(r.aRoot, "link.ln");
    const linkB = join(r.bRoot, "link.ln");

    await fsp.writeFile(target, "hello");
    try {
      await fsp.symlink("data.txt", linkA); // relative link
    } catch {
      return; // filesystem may disallow symlinks
    }
    await sync(r, "alpha");

    // verify it's a link at dest, and points to same target
    const lst = await fsp.lstat(linkB);
    expect(lst.isSymbolicLink()).toBe(true);
    const destTarget = await fsp.readlink(linkB);
    expect(destTarget).toBe("data.txt");
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

  test.skip("prefer=alpha restores file when beta deletes and alpha keeps (no content change)", async () => {
    // Difference is: no content change between scans—only a deletion on beta should cause a restore,
    // which currently requires us to track directory entries even without content change.
    const r = await mkCase(tmp, "f-restore-on-delete");
    const p = join(r.aRoot, "keep.txt");
    await fsp.writeFile(p, "same");
    await sync(r, "alpha");
    await fsp.rm(join(r.bRoot, "keep.txt")); // delete on beta only
    await sync(r, "alpha");
    expect(await fileExists(join(r.bRoot, "keep.txt"))).toBe(true);
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

  test.skip("filters: exclude '*.log' and include 'src/**' only", async () => {
    // REQUIREMENTS:
    // - scheduler/merge accept include/exclude patterns
    // - scanner respects them, or planner filters paths before rsync
    const r = await mkCase(tmp, "f-filters");
    await fsp.mkdir(join(r.aRoot, "src"), { recursive: true });
    await fsp.mkdir(join(r.aRoot, "logs"), { recursive: true });
    await fsp.writeFile(join(r.aRoot, "src", "keep.ts"), "ok");
    await fsp.writeFile(join(r.aRoot, "logs", "app.log"), "nope");

    // hypothetical args once implemented:
    // await runDist("scan.js", ["--root", r.aRoot, "--db", r.aDb, "--include", "src/**", "--exclude", "**/*.log"]);
    // await runDist("scan.js", ["--root", r.bRoot, "--db", r.bDb, "--include", "src/**", "--exclude", "**/*.log"]);
    // await runDist("merge-rsync.js", [..., "--include", "src/**", "--exclude", "**/*.log"]);

    await sync(r, "alpha"); // currently will copy both; future: only src/**

    expect(await fileExists(join(r.bRoot, "src", "keep.ts"))).toBe(true);
    expect(await fileExists(join(r.bRoot, "logs", "app.log"))).toBe(false);
  });

  test.skip("micro-sync: single-file change pushes immediately without full scan", async () => {
    // REQUIREMENTS:
    // - scheduler micro-sync enabled and testable (max cycles, short timers)
    // - test harness starts scheduler once and watches for the copy to appear
    // Idea: write file in alpha, wait for small timeout, assert file exists in beta, then stop scheduler.
  });

  test.skip("remote scan over SSH (alpha remote) with ingest-delta", async () => {
    // REQUIREMENTS:
    // - localhost ssh server available and ccsync on PATH remotely
    // - scheduler supports --alpha-remote args (already drafted)
    // This will be flaky locally; keep as a manual/CI gated test.
  });
});
