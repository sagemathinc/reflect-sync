// ssh-util.ts

// Helper functions for unit testing ccsync over ssh.

import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";

// Ensure sshd reachable on localhost. Returns true/false (does not throw).
export async function canSshLocalhost(): Promise<boolean> {
  return new Promise((resolve) => {
    const p = spawn(
      "ssh",
      [
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "localhost",
        "true",
      ],
      { stdio: "ignore" },
    );
    p.on("exit", (code) => resolve(code === 0));
    p.on("error", () => resolve(false));
  });
}

// Create a temp ed25519 keypair, append public key to authorized_keys with markers, and return cleanup.
export async function withTempSshKey() {
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "ccsync-ssh-"));
  const keyPath = path.join(tmp, "id_ed25519");
  const comment = `ccsync-test-${crypto.randomBytes(6).toString("hex")}`;

  // Generate keypair
  await new Promise<void>((resolve, reject) => {
    const p = spawn(
      "ssh-keygen",
      ["-t", "ed25519", "-N", "", "-C", comment, "-f", keyPath],
      { stdio: "ignore" },
    );
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error("ssh-keygen failed")),
    );
    p.on("error", reject);
  });

  const pub = await fs.readFile(keyPath + ".pub", "utf8");

  // Authorized keys path
  const home = os.homedir();
  const sshDir = path.join(home, ".ssh");
  const auth = path.join(sshDir, "authorized_keys");
  await fs.mkdir(sshDir, { recursive: true, mode: 0o700 }).catch(() => {});
  try {
    await fs.chmod(sshDir, 0o700);
  } catch {}

  const begin = `# BEGIN ${comment}\n`;
  const end = `# END ${comment}\n`;
  const block = begin + pub + (pub.endsWith("\n") ? "" : "\n") + end;

  // Append block
  await fs.appendFile(auth, block, { mode: 0o600 }).catch(async (e) => {
    if ((e as any).code === "ENOENT") {
      await fs.writeFile(auth, block, { mode: 0o600 });
    } else {
      throw e;
    }
  });
  try {
    await fs.chmod(auth, 0o600);
  } catch {}

  async function cleanup() {
    try {
      const contents = await fs.readFile(auth, "utf8");
      const start = contents.indexOf(begin);
      const stop = contents.indexOf(end, start + begin.length);
      if (start !== -1 && stop !== -1) {
        const newText =
          contents.slice(0, start) + contents.slice(stop + end.length);
        await fs.writeFile(auth, newText, { mode: 0o600 });
      }
    } catch {}
    await fs.rm(tmp, { recursive: true, force: true }).catch(() => {});
  }

  return { keyPath, comment, cleanup };
}

// Spawn ssh to run a remote command on localhost with given identity file and stdio options.
export function spawnSshLocal(
  keyPath: string,
  remoteCmd: string,
  stdio: "pipe" | "inherit" | "ignore" = "pipe",
) {
  const args = [
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "LogLevel=ERROR",
    "-o",
    "IdentitiesOnly=yes",
    "-i",
    keyPath,
    "localhost",
    remoteCmd,
  ];
  // console.log(`ssh ${args.join(" ")}`);
  return spawn("ssh", args, { stdio });
}

// Very basic shell-quote for args (safe for simple ascii paths w/out single quotes).
export function shQuote(s: string) {
  if (s === "") return "''";
  return `'${s.replace(/'/g, `'\\''`)}'`;
}
