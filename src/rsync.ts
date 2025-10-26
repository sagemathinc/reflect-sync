// rsync.ts
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtemp, rm, stat as fsStat } from "node:fs/promises";
import path from "node:path";
import { createWriteStream } from "node:fs";
import { finished } from "node:stream/promises";

// extremely verbose -- showing all output of rsync, which
// can be massive, of course.
const verbose2 = !!process.env.CCSYNC_VERBOSE2;

// ---------- helpers that used to be local to merge ----------
export function ensureTrailingSlash(root: string): string {
  return root.endsWith("/") ? root : root + "/";
}

export async function fileNonEmpty(p: string, verbose?: boolean) {
  try {
    return (await fsStat(p)).size > 0;
  } catch (err) {
    if (verbose) console.warn("fileNonEmpty ", p, err);
    return false;
  }
}

export function rsyncArgsBase(opts: {
  dryRun?: boolean | string;
  verbose?: boolean | string;
}) {
  const a = ["-a", "-I", "--relative"];
  if (opts.dryRun) a.unshift("-n");
  if (verbose2) a.push("-v");
  if (!opts.verbose) a.push("--quiet");
  return a;
  // NOTE: -I disables rsync's quick-check so listed files always copy.
}

export function rsyncArgsDirs(opts: {
  dryRun?: boolean | string;
  verbose?: boolean | string;
}) {
  // -d: transfer directories themselves (no recursion) — needed for empty dirs
  const a = ["-a", "-d", "--relative", "--from0"];
  if (opts.dryRun) a.unshift("-n");
  if (verbose2) a.push("-v");
  if (!opts.verbose) a.push("--quiet");
  return a;
}

export function rsyncArgsDelete(opts: {
  dryRun?: boolean | string;
  verbose?: boolean | string;
}) {
  const a = [
    "-a",
    "--relative",
    "--from0",
    "--ignore-missing-args",
    "--delete-missing-args",
    "--force",
  ];
  if (opts.dryRun) {
    a.unshift("-n");
  }
  if (verbose2) a.push("-v");
  if (!opts.verbose) a.push("--quiet");
  return a;
}

export function run(
  cmd: string,
  args: string[],
  okCodes: number[] = [0],
  verbose?: boolean | string,
): Promise<{ code: number | null; ok: boolean; zero: boolean }> {
  const t = Date.now();
  if (verbose)
    console.log(
      `$ ${cmd} ${args.map((a) => (/\s/.test(a) ? JSON.stringify(a) : a)).join(" ")}`,
    );
  return new Promise((resolve) => {
    // ignore is critical for stdio since we don't read the output
    // and there is a lot, so it would otherwise DEADLOCK.
    const p = spawn(cmd, args, {
      stdio: verbose2 ? "inherit" : ["ignore", "ignore", "ignore"],
    });
    p.on("exit", (code) => {
      const zero = code === 0;
      const ok = code !== null && okCodes.includes(code!);
      resolve({ code, ok, zero });
      if (verbose) {
        console.log("time:", Date.now() - t, "ms");
      }
    });
    p.on("error", () =>
      resolve({ code: 1, ok: okCodes.includes(1), zero: false }),
    );
  });
}

// write a NUL-separated list memory efficiently
export async function writeNulList(file: string, items: string[]) {
  const ws = createWriteStream(file);
  for (const it of items) {
    if (!ws.write(it + "\0")) {
      await new Promise<void>((resolve) => ws.once("drain", resolve));
    }
  }
  ws.end();
  await finished(ws);
}

// divide arr up into chunks of size at most n.
export function chunk<T>(arr: T[], n: number): T[][] {
  if (arr.length <= n) {
    return [arr];
  }
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) {
    out.push(arr.slice(i, i + n));
  }
  return out;
}

export async function rsyncCopy(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: { dryRun?: boolean | string; verbose?: boolean | string } = {},
): Promise<{ zero: boolean }> {
  if (!(await fileNonEmpty(listFile, !!opts.verbose))) {
    if (opts.verbose) console.log(`>>> rsync ${label}: nothing to do`);
    return { zero: false };
  }
  if (opts.verbose) {
    console.log(`>>> rsync ${label} (${fromRoot} -> ${toRoot})`);
  }
  const args = [
    ...rsyncArgsBase(opts),
    "--from0",
    `--files-from=${listFile}`,
    ensureTrailingSlash(fromRoot),
    ensureTrailingSlash(toRoot),
  ];
  const res = await run("rsync", args, [0, 23, 24], opts.verbose); // accept partials
  if (opts.verbose) {
    console.log(`>>> rsync ${label}: done (code ${res.code})`);
  }
  return { zero: res.zero };
}

export async function rsyncCopyDirs(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: { dryRun?: boolean | string; verbose?: boolean | string } = {},
): Promise<{ zero: boolean }> {
  if (!(await fileNonEmpty(listFile, !!opts.verbose))) {
    if (opts.verbose) console.log(`>>> rsync ${label} (dirs): nothing to do`);
    return { zero: false };
  }
  if (opts.verbose) {
    console.log(`>>> rsync ${label} (dirs) (${fromRoot} -> ${toRoot})`);
  }
  const args = [
    ...rsyncArgsDirs(opts),
    `--files-from=${listFile}`,
    ensureTrailingSlash(fromRoot),
    ensureTrailingSlash(toRoot),
  ];
  const res = await run("rsync", args, [0, 23, 24], opts.verbose);
  if (opts.verbose) {
    console.log(`>>> rsync ${label} (dirs): done (code ${res.code})`);
  }
  return { zero: res.zero };
}

export async function rsyncDelete(
  fromRoot: string,
  toRoot: string,
  listFile: string,
  label: string,
  opts: {
    forceEmptySource?: boolean;
    dryRun?: boolean | string;
    verbose?: boolean | string;
  } = {},
): Promise<void> {
  if (!(await fileNonEmpty(listFile, !!opts.verbose))) {
    if (opts.verbose) console.log(`>>> rsync delete ${label}: nothing to do`);
    return;
  }

  // Force all listed paths to be "missing" by using an empty temp source dir
  let sourceRoot = ensureTrailingSlash(fromRoot);
  let tmpEmptyDir: string | null = null;
  try {
    if (opts.forceEmptySource) {
      tmpEmptyDir = await mkdtemp(path.join(tmpdir(), "rsync-empty-"));
      sourceRoot = ensureTrailingSlash(tmpEmptyDir);
    }

    if (opts.verbose) {
      console.log(
        `>>> rsync delete ${label} (missing in ${sourceRoot} => delete in ${toRoot})`,
      );
    }
    const args = [
      ...rsyncArgsDelete(opts), // includes --delete-missing-args --force
      `--files-from=${listFile}`,
      sourceRoot,
      ensureTrailingSlash(toRoot),
    ];
    await run("rsync", args, [0, 24], opts.verbose);
  } finally {
    if (tmpEmptyDir) {
      await rm(tmpEmptyDir, { recursive: true, force: true });
    }
  }
}

export async function rsyncDeleteChunked(
  workDir: string,
  fromRoot: string,
  toRoot: string,
  rpaths: string[],
  label: string,
  opts: {
    forceEmptySource?: boolean;
    chunkSize?: number;
    dryRun?: boolean | string;
    verbose?: boolean | string;
  } = {},
) {
  if (!rpaths.length) return;
  const { chunkSize = 50000 } = opts; // good default for large waves
  const batches = chunk(rpaths, chunkSize);
  if (opts.verbose) {
    console.log(
      `>>> rsync delete ${label}: ${rpaths.length} in ${batches.length} batches of size at most ${chunkSize}`,
    );
  }

  for (let i = 0; i < batches.length; i++) {
    if (opts.verbose) {
      console.log(`>>> rsync delete ${label} [${i + 1}/${batches.length}]`);
    }
    const listFile = path.join(
      workDir,
      `${label.replace(/[\s\(\)]+/g, "-")}.${i}.list`,
    );
    await writeNulList(listFile, batches[i]);
    await rsyncDelete(
      fromRoot,
      toRoot,
      listFile,
      `${label} [${i + 1}/${batches.length}]`,
      {
        forceEmptySource: opts.forceEmptySource,
        dryRun: opts.dryRun,
        verbose: opts.verbose,
      },
    );
  }
}
