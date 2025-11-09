// reflink.ts
import { cpus } from "node:os";
import path from "node:path";
import { stat as fsStat, lstat, readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { argsJoin } from "./remote.js";

export type ReflinkCopyResult = {
  copied: string[];
  failed: Array<{ path: string; error: string }>;
};

export async function sameDevice(a: string, b: string): Promise<boolean> {
  const [sa, sb] = await Promise.all([fsStat(a), fsStat(b)]);
  return sa.dev === sb.dev;
}

function run(cmd: string, args: string[], cwd?: string): Promise<void> {
  return new Promise((res, rej) => {
    const p = spawn(cmd, args, {
      cwd,
      stdio: ["ignore", "ignore", "pipe"], // capture stderr for easier debugging
    });

    let errBuf = "";
    p.stderr.on("data", (d) => (errBuf += d.toString()));

    p.on("exit", (code) => {
      if (code === 0) return res();
      const msg = `${cmd} ${argsJoin(args)} -> ${code}${errBuf ? `\n${errBuf}` : ""}`;
      rej(new Error(msg));
    });
  });
}

/**
 * Reflink-copy each relative path from listFile (NUL-delimited) from srcRoot→dstRoot.
 * - Uses batched `cp --reflink --parents -t DST rel1 rel2 ...` commands (cp falls back to normal copy if CoW fails)
 * - Runs several batch cp's in parallel (default: up to ~8 or CPU count)
 * - Expects rel paths (as produced by your planner lists). Do not include directories here.
 * - Returns the subset that actually copied plus any failures so callers can log/throw as needed.
 */
export async function cpReflinkFromList(
  srcRoot: string,
  dstRoot: string,
  listFile: string,
  parallel = Math.max(2, Math.min(8, cpus().length)),
): Promise<ReflinkCopyResult> {
  // Fast fail if cross-device (no reflink possible):
  if (!(await sameDevice(srcRoot, dstRoot))) {
    throw new Error("reflink: src/dst are on different devices");
  }

  const buf = await readFile(listFile);
  // Deduplicate & filter trivial empties
  const rels = Array.from(
    new Set(buf.toString("utf8").split("\0").filter(Boolean)),
  );
  if (rels.length === 0) {
    return { copied: [], failed: [] };
  }

  // --- Chunking strategy ----------------------------------------------------
  // We have two constraints:
  //   1) Number of args per 'cp' (avoid huge process argv)
  //   2) Total characters in argv (stay well under ARG_MAX)
  //
  // These conservative defaults work well on Linux:
  const MAX_ARGS_PER_CP = 5000; // thousands per cp call
  const MAX_CHARS_PER_CP = 500_000; // ~0.5 MB of argv payload

  // Build chunks of rel-paths, each within both limits.
  const chunks: string[][] = [];
  {
    let cur: string[] = [];
    let charSum = 0;
    for (const r of rels) {
      const addLen = r.length + 1; // +1 for space/NUL overhead
      const wouldOverflow =
        cur.length >= MAX_ARGS_PER_CP || charSum + addLen > MAX_CHARS_PER_CP;
      if (wouldOverflow && cur.length > 0) {
        chunks.push(cur);
        cur = [];
        charSum = 0;
      }
      cur.push(r);
      charSum += addLen;
    }
    if (cur.length) chunks.push(cur);
  }

  const isRoot = process.geteuid?.() === 0;
  const successes = new Set<string>();
  const failures: Array<{ path: string; error: string }> = [];

  const buildArgs = (relChunk: string[]) => [
    "--reflink",
    "--no-dereference", // copy symlink objects as symlinks
    `--preserve=timestamps,mode${isRoot ? ",ownership" : ""}`,
    "--parents",
    "-t",
    dstRoot,
    "--",
    ...relChunk,
  ];

  const recordFailure = (rel: string, err: unknown) => {
    failures.push({
      path: rel,
      error: err instanceof Error ? err.message : String(err),
    });
  };

  const CLOCK_SLOP_MS = 50;
  async function detectCopiesAfterFailure(
    relChunk: string[],
    startedAt: number,
    err: unknown,
  ) {
    for (const rel of relChunk) {
      const dstPath = path.join(dstRoot, rel);
      try {
        const st = await lstat(dstPath);
        if (Number.isFinite(st.ctimeMs) && st.ctimeMs >= startedAt - CLOCK_SLOP_MS) {
          successes.add(rel);
          continue;
        }
      } catch {
        // fallthrough to failure record below
      }
      recordFailure(rel, err);
    }
  }

  // Worker that processes chunk indices i, i+parallel, ...
  async function worker(startIdx: number) {
    for (let i = startIdx; i < chunks.length; i += parallel) {
      const relChunk = chunks[i];
      const startedAt = Date.now();
      try {
        await run("cp", buildArgs(relChunk), srcRoot);
        relChunk.forEach((rel) => successes.add(rel));
      } catch (err) {
        await detectCopiesAfterFailure(relChunk, startedAt, err);
      }
    }
  }

  // Launch parallel batch cp processes
  const workers = Array.from(
    { length: Math.min(parallel, chunks.length) },
    (_, k) => worker(k),
  );
  await Promise.all(workers);
  return {
    copied: Array.from(successes),
    failed: failures,
  };
}
