// src/micro-sync.ts
import path from "node:path";
import { tmpdir } from "node:os";
import { mkdtemp, rm, writeFile, lstat as fsLstat, stat as fsStat } from "node:fs/promises";
import { lstatSync } from "node:fs";
import { cpReflinkFromList, sameDevice } from "./reflink.js";
import {
  run as runRsync,
  type RsyncProgressEvent,
  assertRsyncOk,
  ensureTempDir,
} from "./rsync.js";
import {
  recordRecentSend,
  getRecentSendSignatures,
  signatureEquals,
  signatureFromStamp,
  type SendSignature,
  type SyncDirection,
} from "./recent-send.js";
import {
  isCompressing,
  rsyncCompressionArgs,
  type RsyncCompressSpec,
} from "./rsync-compression.js";
import type { Logger } from "./logger.js";
import { fetchOpStamps } from "./op-stamp.js";

export type MicroSyncDeps = {
  isMergeActive?: () => boolean;
  alphaRoot: string;
  betaRoot: string;
  alphaDbPath: string;
  betaDbPath: string;
  alphaHost?: string;
  alphaPort?: number;
  betaHost?: string;
  betaPort?: number;
  prefer: "alpha" | "beta";
  dryRun: boolean;
  log: (
    level: "info" | "warn" | "error",
    source: string,
    msg: string,
    details?: any,
  ) => void;
  compress?: RsyncCompressSpec;
  logger: Logger;
};

type MicroSyncFn = ((
  rpathsAlpha: string[],
  rpathsBeta: string[],
) => Promise<void>) & {
  markAlphaToBeta: (paths: string[]) => void;
  markBetaToAlpha: (paths: string[]) => void;
};

export function makeMicroSync({
  alphaRoot,
  betaRoot,
  alphaDbPath,
  betaDbPath,
  alphaHost,
  alphaPort,
  betaHost,
  betaPort,
  prefer,
  dryRun,
  log,
  compress,
  logger,
  isMergeActive,
}: MicroSyncDeps) {
  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  const microLogger = logger.child("micro");

  let alphaTempDir: string | undefined;
  let betaTempDir: string | undefined;

  const dedupe = (paths: Iterable<string>) => Array.from(new Set(paths));

  const buildSignatureFromFs = (
    root: string,
    relPath: string,
  ): SendSignature | null => {
    try {
      const abs = path.join(root, relPath);
      const st = lstatSync(abs);
      const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
      if (st.isSymbolicLink()) {
        return {
          kind: "link",
          opTs: mtime,
          mtime,
          size: st.size,
        };
      }
      if (st.isDirectory()) {
        return {
          kind: "dir",
          opTs: mtime,
          mtime,
        };
      }
      if (st.isFile()) {
        return {
          kind: "file",
          opTs: mtime,
          size: st.size,
          mtime,
        };
      }
      return {
        kind: "file",
        opTs: mtime,
        size: st.size,
        mtime,
      };
    } catch (err: any) {
      if (err?.code === "ENOENT") {
        return {
          kind: "missing",
          opTs: Date.now(),
        };
      }
      return null;
    }
  };

  const recordDirection = (
    direction: SyncDirection,
    paths: string[],
  ) => {
    if (!paths.length) return;
    const unique = dedupe(paths);
    const destDb =
      direction === "alpha->beta" ? betaDbPath : alphaDbPath;
    const destRoot =
      direction === "alpha->beta" ? betaRoot : alphaRoot;
    const destIsRemote =
      direction === "alpha->beta" ? betaIsRemote : alphaIsRemote;
    const sourceDb =
      direction === "alpha->beta" ? alphaDbPath : betaDbPath;

    const destStamps = fetchOpStamps(destDb, unique);
    const sourceStamps = fetchOpStamps(sourceDb, unique);

    const entries = unique.map((path) => {
      let signature = signatureFromStamp(destStamps.get(path));
      if (!signature && !destIsRemote) {
        signature = buildSignatureFromFs(destRoot, path);
      }
      if (!signature) {
        signature = signatureFromStamp(sourceStamps.get(path));
      }
      microLogger.debug("record recent send", {
        direction,
        path,
        signature,
        destDb,
        sourceDb,
      });
      return { path, signature };
    });
    recordRecentSend(destDb, direction, entries);
  };

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
    const progressScope =
      direction === "alpha->beta"
        ? "micro.rsync.alpha->beta"
        : "micro.rsync.beta->alpha";

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
      const res = await runRsync(
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
        {
          logger: microLogger,
          logLevel: "debug",
          tempDir: betaIsRemote ? undefined : betaTempDir,
          onProgress: (event: RsyncProgressEvent) => {
            microLogger.info("progress", {
              scope: progressScope,
              stage: "micro",
              direction,
              transferredBytes: event.transferredBytes,
              totalBytes: event.totalBytes ?? null,
              percent: event.percent,
              speed: event.speed ?? null,
              etaMilliseconds: event.etaMilliseconds ?? null,
            });
          },
        },
      );
      assertRsyncOk(`micro ${direction}`, res, { direction });
      return res.zero;
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
      const res = await runRsync(
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
        {
          logger: microLogger,
          logLevel: "debug",
          tempDir: alphaIsRemote ? undefined : alphaTempDir,
          onProgress: (event: RsyncProgressEvent) => {
            microLogger.info("progress", {
              scope: progressScope,
              stage: "micro",
              direction,
              transferredBytes: event.transferredBytes,
              totalBytes: event.totalBytes ?? null,
              percent: event.percent,
              speed: event.speed ?? null,
              etaMilliseconds: event.etaMilliseconds ?? null,
            });
          },
        },
      );
      assertRsyncOk(`micro ${direction}`, res, { direction });
      return res.zero;
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

  const microSync: MicroSyncFn = async function microSync(
    // rpathsAlpha are paths in alpha that will be copied to beta
    rpathsAlpha: string[],
    rpathsBeta: string[],
  ) {
    if (process.env.REFLECT_DISABLE_AUTOMATIC_COPY === "1") {
      return;
    }
    if (isMergeActive?.()) {
      return;
    }
    alphaTempDir = alphaIsRemote ? undefined : await ensureTempDir(alphaRoot);
    betaTempDir = betaIsRemote ? undefined : await ensureTempDir(betaRoot);
    const setA = new Set(rpathsAlpha);
    const setB = new Set(rpathsBeta);

    const touched = new Set<string>([...setA, ...setB]);
    if (!touched.size) {
      return;
    }
    const touchedList = Array.from(touched);

    const alphaStamps = fetchOpStamps(alphaDbPath, touchedList);
    const betaStamps = fetchOpStamps(betaDbPath, touchedList);

    const alphaSigMap = new Map<string, SendSignature | null>();
    const betaSigMap = new Map<string, SendSignature | null>();
    for (const path of touchedList) {
      alphaSigMap.set(path, signatureFromStamp(alphaStamps.get(path)));
      betaSigMap.set(path, signatureFromStamp(betaStamps.get(path)));
    }

    if (!alphaIsRemote) {
      for (const path of setA) {
        if (!alphaSigMap.get(path)) {
          const sig = buildSignatureFromFs(alphaRoot, path);
          if (sig) alphaSigMap.set(path, sig);
        }
      }
    }
    if (!betaIsRemote) {
      for (const path of setB) {
        if (!betaSigMap.get(path)) {
          const sig = buildSignatureFromFs(betaRoot, path);
          if (sig) betaSigMap.set(path, sig);
        }
      }
    }

    const recentFromBeta = getRecentSendSignatures(
      alphaDbPath,
      "beta->alpha",
      touchedList,
    );
    const recentFromAlpha = getRecentSendSignatures(
      betaDbPath,
      "alpha->beta",
      touchedList,
    );

    const toBetaSet = new Set<string>();
    for (const path of setA) {
      const sig = alphaSigMap.get(path) ?? null;
      const last = recentFromBeta.get(path);
      if (sig && last && signatureEquals(sig, last)) {
        microLogger.debug("skip alpha→beta bounce", { path });
        continue;
      }
      toBetaSet.add(path);
    }

    const toAlphaSet = new Set<string>();
    for (const path of setB) {
      const sig = betaSigMap.get(path) ?? null;
      const last = recentFromAlpha.get(path);
      if (sig && last && signatureEquals(sig, last)) {
        microLogger.debug("skip beta→alpha bounce", { path });
        continue;
      }
      microLogger.debug("queue beta→alpha", {
        path,
        sig,
        last,
        reason:
          sig && last
            ? "signature-mismatch"
            : sig
              ? "no-recent-record"
              : "no-signature",
      });
      toAlphaSet.add(path);
    }

    for (const path of [...toBetaSet]) {
      if (toAlphaSet.has(path)) {
        if (prefer === "alpha") {
          toAlphaSet.delete(path);
        } else {
          toBetaSet.delete(path);
        }
      }
    }

    let toBeta = Array.from(toBetaSet);
    let toAlpha = Array.from(toAlphaSet);

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
        }

        recordDirection("alpha->beta", toBeta);
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
        }

        recordDirection("beta->alpha", toAlpha);
      }
    } finally {
      await rm(tmp, { recursive: true, force: true });
    }
  };

  microSync.markAlphaToBeta = (paths: string[]) => {
    //log("info", "realtime", `markAlphaToBeta: ${JSON.stringify(paths)}`);
    log("info", "realtime", `markAlphaToBeta: ${paths.length} paths`);
    recordDirection("alpha->beta", paths);
  };

  microSync.markBetaToAlpha = (paths: string[]) => {
    //log("info", "realtime", `markBetaToAlpha: ${JSON.stringify(paths)}`);
    log("info", "realtime", `markBetaToAlpha: ${paths.length} paths`);
    recordDirection("beta->alpha", paths);
  };

  return microSync;
}
