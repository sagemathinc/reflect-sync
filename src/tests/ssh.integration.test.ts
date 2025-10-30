/* ssh.integration.test.ts

SSH integration tests for reflex-sync.

Skips unless both:
- env variable RFSYNC_SKIP_SSH_TEST is NOT set
- sshd is reachable on localhost

It:
  1) creates tmp roots + DB
  2) creates a file in alpha root
  3) runs remote scan (--emit-delta) over SSH piping to local ingest
  4) verifies the row appears in alpha.db
 */

import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";
import { Database } from "../db";
import { spawn } from "node:child_process";
import {
  canSshLocalhost,
  withTempSshKey,
  spawnSshLocal,
  shQuote,
} from "./ssh-util";

const dist = (...p: string[]) =>
  path.resolve(__dirname, "..", "..", "dist", ...p);
const nodeExe = process.execPath; // absolute node path running the tests

// Pipe remote scan stdout -> local ingest stdin, and wait
async function runScanOverSshIntoIngest(opts: {
  keyPath: string;
  remoteRoot: string;
  remoteDb: string;
  localDb: string;
}) {
  const { keyPath, remoteRoot, remoteDb, localDb } = opts;

  // Build remote command: node <dist/scan.js> <remoteRoot> --emit-delta --db <path/to/db>
  const scanJs = dist("scan.js");
  const remoteCmd = [
    shQuote(nodeExe),
    shQuote(scanJs),
    "--root",
    shQuote(remoteRoot),
    "--emit-delta",
    "--db",
    shQuote(remoteDb),
  ].join(" ");

  const ssh = spawnSshLocal(keyPath, remoteCmd, "pipe");

  // Local ingest
  const ingest = spawn(nodeExe, [dist("ingest-delta.js"), "--db", localDb], {
    stdio: ["pipe", "inherit", "inherit"],
  });

  ssh.stdout!.pipe(ingest.stdin!);

  await new Promise<void>((resolve, reject) => {
    let sshOk = false,
      ingestOk = false;
    const check = () => {
      if (sshOk && ingestOk) resolve();
    };

    ssh.on("exit", (code) => {
      sshOk = code === 0;
      if (!sshOk) reject(new Error(`ssh scan exited ${code}`));
      else check();
    });
    ssh.on("error", reject);

    ingest.on("exit", (code) => {
      ingestOk = code === 0;
      if (!ingestOk) reject(new Error(`ingest exited ${code}`));
      else check();
    });
    ingest.on("error", reject);
  });
}

const sshEnabled = process.env.RFSYNC_SKIP_SSH_TEST === undefined;

(sshEnabled ? describe : describe.skip)(
  "SSH: remote scan -> local ingest",
  () => {
    let tmp: string, aRoot: string, aDbLocal: string, aDbRemote: string;
    let keyCleanup: (() => Promise<void>) | null = null;
    let keyPath: string;

    beforeAll(async () => {
      if (!(await canSshLocalhost())) {
        throw Error("Skipping: sshd not reachable on localhost");
      }

      // temp dirs
      tmp = await fs.mkdtemp(path.join(os.tmpdir(), "rfsync-ssh-test-"));
      aRoot = path.join(tmp, "alpha");
      await fs.mkdir(aRoot, { recursive: true });

      // local and remote DB paths (same machine, different files)
      aDbLocal = path.join(tmp, "alpha.local.db");
      aDbRemote = path.join(tmp, "alpha.remote.db");

      const { keyPath: kp, cleanup } = await withTempSshKey();
      keyPath = kp;
      keyCleanup = cleanup;
    });

    afterAll(async () => {
      if (keyCleanup) await keyCleanup();
      await fs.rm(tmp, { recursive: true, force: true }).catch(() => {});
    });

    test("delta from remote scan shows up in local DB", async () => {
      // create a file remotely-visible (same FS, but path is “remote root”)
      const f = path.join(aRoot, "hello.txt");
      await fs.writeFile(f, "hello ssh\n");

      // run remote scan -> local ingest
      await runScanOverSshIntoIngest({
        keyPath,
        remoteRoot: aRoot,
        remoteDb: aDbRemote,
        localDb: aDbLocal,
      });

      // Verify local DB has the row
      const db = new Database(aDbLocal);
      const row = db
        .prepare(`SELECT path, hash, deleted FROM files WHERE path = ?`)
        .get("hello.txt");
      db.close();

      expect(row).toBeTruthy();
      expect(row?.deleted).toBe(0);
    });
  },
);
