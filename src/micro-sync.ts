// src/micro-sync.ts
import path from "node:path";
import { tmpdir } from "node:os";
import {
  mkdtemp,
  rm,
  writeFile,
  lstat as fsLstat,
  stat as fsStat,
} from "node:fs/promises";
import { cpReflinkFromList, sameDevice } from "./reflink.js";
import type { SpawnOptions } from "node:child_process";
import {
  isCompressing,
  rsyncCompressionArgs,
  type RsyncCompressSpec,
} from "./rsync-compression.js";
import type { Logger } from "./logger.js";

export type MicroSyncDeps = {
  alphaRoot: string;
  betaRoot: string;
  alphaHost?: string;
  alphaPort?: number;
  betaHost?: string;
  betaPort?: number;
  prefer: "alpha" | "beta";
  dryRun: boolean;
  spawnTask: (
    cmd: string,
    args: string[],
    okCodes?: number[],
    extra?: SpawnOptions,
  ) => Promise<{
    code: number | null;
    ms: number;
    ok: boolean;
    lastZero: boolean;
  }>;
  log: (
    level: "info" | "warn" | "error",
    source: string,
    msg: string,
    details?: any,
  ) => void;
  compress?: RsyncCompressSpec;
  logger: Logger;
};

export function makeMicroSync({
  alphaRoot,
  betaRoot,
  alphaHost,
  alphaPort,
  betaHost,
  betaPort,
  prefer,
  dryRun,
  spawnTask,
  log,
  compress,
  logger,
}: MicroSyncDeps) {
  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  const microLogger = logger.child("micro");

  // Echo suppression prevents one-sided “echo” events from bouncing back
  // immediately after we ourselves copied the file to that side.
  const ECHO_SUPPRESS_MS = Number(process.env.REFLECT_MICRO_ECHO_SUPPRESS_MS ?? 2500);

  // Last time we pushed a path in a given direction.
  const lastA2B = new Map<string, number>();
  const lastB2A = new Map<string, number>();

  function rsyncRoots(
    fromRoot: string,
    fromHost: string | undefined,
    fromPort: number | undefined,
    toRoot: string,
    toHost: string | undefined,
    toPort: number | undefined,
    compression: RsyncCompressSpec | undefined,
  ) {
    const slash = (s: string) => (s.endsWith("/") ? s : s + "/");
    const from = fromHost ? `${fromHost}:${slash(fromRoot)}` : slash(fromRoot);
    const to = toHost ? `${toHost}:${slash(toRoot)}` : slash(toRoot);

    const wantDisableSSH = isCompressing(compression);
    const port = fromHost ? fromPort : toPort;
    const sshParts = ["ssh"];
    if (port != null) {
      sshParts.push("-p", String(port));
    }
    if (wantDisableSSH) {
      sshParts.push("-oCompression=no");
    }
    const transport =
      fromHost || toHost
        ? sshParts.length > 1
          ? (["-e", sshParts.join(" ")] as string[])
          : ["-e", sshParts[0]]
        : [];
    return { from, to, transport };
  }

  async function statKind(
    root: string,
    r: string,
  ): Promise<"file" | "dir" | "link" | "missing"> {
    try {
      const st = await fsLstat(path.join(root, r));
      if (st.isSymbolicLink()) return "link";
      if (st.isFile()) return "file";
      if (st.isDirectory()) return "dir";
      // treat other special files as "file" for rsync purposes
      return "file";
    } catch {
      return "missing";
    }
  }

  async function classifyLocal(
    root: string,
    rpaths: string[],
  ): Promise<{ files: string[]; others: string[]; missing: string[] }> {
    const files: string[] = [];
    const others: string[] = [];
    const missing: string[] = [];
    for (const r of rpaths) {
      const k = await statKind(root, r);
      if (k === "file") files.push(r);
      else if (k === "missing") missing.push(r);
      else others.push(r); // dir or link
    }
    return { files, others, missing };
  }

  const join0 = (items: string[]) =>
    Buffer.from(items.filter(Boolean).join("\0") + (items.length ? "\0" : ""));

  async function fileNonEmpty(p: string) {
    try {
      return (await fsStat(p)).size > 0;
    } catch {
      return false;
    }
  }

  async function doRsync(
    direction: "alpha->beta" | "beta->alpha",
    listFile: string,
    extra: string[] = [],
  ): Promise<boolean> {
    const compArgs = rsyncCompressionArgs(compress);

    if (direction === "alpha->beta") {
      const { from, to, transport } = rsyncRoots(
        alphaRoot,
        alphaHost,
        alphaPort,
        betaRoot,
        betaHost,
        betaPort,
        compress,
      );
      const res = await spawnTask(
        "rsync",
        [
          ...(dryRun ? ["-n"] : []),
          ...transport,
          "-a",
          "-I",
          "--relative",
          "--from0",
          ...compArgs,
          ...extra,
          `--files-from=${listFile}`,
          from,
          to,
        ],
        [0, 23, 24],
      );
      return !!res.lastZero;
    } else {
      const { from, to, transport } = rsyncRoots(
        betaRoot,
        betaHost,
        betaPort,
        alphaRoot,
        alphaHost,
        alphaPort,
        compress,
      );
      const res = await spawnTask(
        "rsync",
        [
          ...(dryRun ? ["-n"] : []),
          ...transport,
          "-a",
          "-I",
          "--relative",
          "--from0",
          ...compArgs,
          ...extra,
          `--files-from=${listFile}`,
          from,
          to,
        ],
        [0, 23, 24],
      );
      return !!res.lastZero;
    }
  }

  // Small helpers for specific intents
  // - Unified copy+delete (safe for remote sources): will copy present paths and
  //   delete them on the receiver if they are missing on the sender.
  async function rsyncCopyOrDelete(
    direction: "alpha->beta" | "beta->alpha",
    listFile: string,
  ) {
    // --delete-missing-args ensures a missing source path deletes the dest
    // --force allows removing non-empty directories when targeted
    return await doRsync(direction, listFile, [
      "--delete-missing-args",
      "--force",
      "--dirs",
    ]);
  }

  // - Copy only dirs/symlinks, no recursion (so “create the node”, not its tree)
  async function rsyncDirsAndLinks(
    direction: "alpha->beta" | "beta->alpha",
    listFile: string,
  ) {
    // Keep attributes like -a but suppress recursion:
    //   --no-recursive overrides -r from -a, --dirs copies directory entries,
    //   symlinks are handled via -l inside -a.
    return await doRsync(direction, listFile, ["--no-recursive", "--dirs"]);
  }

  return async function microSync(rpathsAlpha: string[], rpathsBeta: string[]) {
    const setA = new Set(rpathsAlpha);
    const setB = new Set(rpathsBeta);

    let toBeta: string[] = [];
    let toAlpha: string[] = [];

    // Decide intended direction(s)
    const touched = new Set<string>([...setA, ...setB]);
    for (const r of touched) {
      const aTouched = setA.has(r);
      const bTouched = setB.has(r);
      if (aTouched && bTouched) {
        if (prefer === "alpha") toBeta.push(r);
        else toAlpha.push(r);
      } else if (aTouched) {
        toBeta.push(r);
      } else {
        toAlpha.push(r);
      }
    }

    // Echo suppression (only when it’s a one-sided echo)
    const now = Date.now();
    toAlpha = toAlpha.filter((r) => {
      // If only B touched and we *just* pushed A→B, this is likely echo from our own write.
      if (!setA.has(r) && setB.has(r)) {
        const tA2B = lastA2B.get(r) || 0;
        if (now - tA2B < ECHO_SUPPRESS_MS) return false;
      }
      return true;
    });
    toBeta = toBeta.filter((r) => {
      // If only A touched and we *just* pushed B→A, this is likely echo from our own write.
      if (setA.has(r) && !setB.has(r)) {
        const tB2A = lastB2A.get(r) || 0;
        if (now - tB2A < ECHO_SUPPRESS_MS) return false;
      }
      return true;
    });

    // NOTE: we may also act on directories/symlinks and deletions below,
    // so don't early-return if file lists are empty.

    const tmp = await mkdtemp(path.join(tmpdir(), "micro-plan-"));
    try {
      // ===== α → β =====
      if (toBeta.length) {
        const listAllA2B = path.join(tmp, "alpha2beta.all");
        await writeFile(listAllA2B, join0(toBeta));

        if (alphaIsRemote || betaIsRemote) {
          // Remote source or dest: single unified rsync that both copies and deletes.
          log(
            "info",
            "realtime",
            `alpha→beta ${toBeta.length} paths (unified copy/delete)`,
          );
          await rsyncCopyOrDelete("alpha->beta", listAllA2B);
          const t = Date.now();
          for (const r of toBeta) lastA2B.set(r, t);
        } else {
          // Local → Local: classify so we can keep reflink for files and still
          // handle deletions + dir/symlink create/remove.
          const { files, others, missing } = await classifyLocal(
            alphaRoot,
            toBeta,
          );

          // Files: try reflink first (same device), else rsync copy for the file subset.
          const listFiles = path.join(tmp, "alpha2beta.files");
          await writeFile(listFiles, join0(files));
          if (await fileNonEmpty(listFiles)) {
            log("info", "realtime", `alpha→beta files ${files.length}`);
            let ok = false;
            if (!dryRun && (await sameDevice(alphaRoot, betaRoot))) {
              try {
                await cpReflinkFromList(alphaRoot, betaRoot, listFiles);
                ok = true;
              } catch (e: any) {
                const meta = { err: String(e?.message || e) };
                log(
                  "warn",
                  "realtime",
                  "reflink alpha→beta failed; falling back to rsync",
                  meta,
                );
                microLogger.warn(
                  "reflink alpha→beta failed; falling back to rsync",
                  meta,
                );
              }
            }
            if (!ok) {
              await doRsync("alpha->beta", listFiles);
            }
          }

          // Dirs & Symlinks: create/update without recursing trees.
          const listOther = path.join(tmp, "alpha2beta.other");
          await writeFile(listOther, join0(others));
          if (await fileNonEmpty(listOther)) {
            log(
              "info",
              "realtime",
              `alpha→beta dirs/symlinks ${others.length} (no-recursive)`,
            );
            await rsyncDirsAndLinks("alpha->beta", listOther);
          }

          // Deletions: anything missing on the source should be removed on dest.
          const listMissing = path.join(tmp, "alpha2beta.missing");
          await writeFile(listMissing, join0(missing));
          if (await fileNonEmpty(listMissing)) {
            log("info", "realtime", `alpha→beta deletes ${missing.length}`);
            await rsyncCopyOrDelete("alpha->beta", listMissing);
          }

          const t = Date.now();
          for (const r of files.concat(others, missing)) lastA2B.set(r, t);
        }
      }

      // ===== β → α =====
      if (toAlpha.length) {
        const listAllB2A = path.join(tmp, "beta2alpha.all");
        await writeFile(listAllB2A, join0(toAlpha));

        if (alphaIsRemote || betaIsRemote) {
          // Remote: single unified rsync that both copies and deletes.
          log(
            "info",
            "realtime",
            `beta→alpha ${toAlpha.length} paths (unified copy/delete)`,
          );
          await rsyncCopyOrDelete("beta->alpha", listAllB2A);
          const t = Date.now();
          for (const r of toAlpha) lastB2A.set(r, t);
        } else {
          // Local → Local classification + fast path
          const { files, others, missing } = await classifyLocal(
            betaRoot,
            toAlpha,
          );

          // Files: reflink fast-path if same device, else rsync copy
          const listFiles = path.join(tmp, "beta2alpha.files");
          await writeFile(listFiles, join0(files));
          if (await fileNonEmpty(listFiles)) {
            log("info", "realtime", `beta→alpha files ${files.length}`);
            let ok = false;
            if (!dryRun && (await sameDevice(alphaRoot, betaRoot))) {
              try {
                await cpReflinkFromList(betaRoot, alphaRoot, listFiles);
                ok = true;
              } catch (e: any) {
                const meta = { err: String(e?.message || e) };
                log(
                  "warn",
                  "realtime",
                  "reflink beta→alpha failed; falling back to rsync",
                  meta,
                );
                microLogger.warn(
                  "reflink beta→alpha failed; falling back to rsync",
                  meta,
                );
              }
            }
            if (!ok) {
              await doRsync("beta->alpha", listFiles);
            }
          }

          // Dirs & Symlinks: create/update without recursing trees
          const listOther = path.join(tmp, "beta2alpha.other");
          await writeFile(listOther, join0(others));
          if (await fileNonEmpty(listOther)) {
            log(
              "info",
              "realtime",
              `beta→alpha dirs/symlinks ${others.length} (no-recursive)`,
            );
            await rsyncDirsAndLinks("beta->alpha", listOther);
          }

          // Deletions
          const listMissing = path.join(tmp, "beta2alpha.missing");
          await writeFile(listMissing, join0(missing));
          if (await fileNonEmpty(listMissing)) {
            log("info", "realtime", `beta→alpha deletes ${missing.length}`);
            await rsyncCopyOrDelete("beta->alpha", listMissing);
          }

          const t = Date.now();
          for (const r of files.concat(others, missing)) lastB2A.set(r, t);
        }
      }
    } finally {
      await rm(tmp, { recursive: true, force: true });
    }
  };
}
