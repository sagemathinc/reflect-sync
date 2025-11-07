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
import {
  applySignaturesToDb,
  type SignatureEntry,
} from "./signature-store.js";

export class PartialTransferError extends Error {
  alphaPaths?: string[];
  betaPaths?: string[];

  constructor(
    message: string,
    data: { alphaPaths?: string[]; betaPaths?: string[] } = {},
  ) {
    super(message);
    this.name = "PartialTransferError";
    if (data.alphaPaths?.length) {
      this.alphaPaths = Array.from(new Set(data.alphaPaths.filter(Boolean)));
    }
    if (data.betaPaths?.length) {
      this.betaPaths = Array.from(new Set(data.betaPaths.filter(Boolean)));
    }
  }
}

type ReleaseEntry = {
  path: string;
  watermark?: number | null;
};

type RemoteLockHandle = {
  lock: (paths: string[]) => Promise<void>;
  release: (entries: ReleaseEntry[]) => Promise<void>;
  unlock: (paths: string[]) => Promise<void>;
};

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
  fetchRemoteAlphaSignatures?: (
    paths: string[],
    opts: { ignore: boolean },
  ) => Promise<SignatureEntry[]>;
  fetchRemoteBetaSignatures?: (
    paths: string[],
    opts: { ignore: boolean },
  ) => Promise<SignatureEntry[]>;
  numericIds?: boolean;
  alphaRemoteLock?: RemoteLockHandle;
  betaRemoteLock?: RemoteLockHandle;
};

export type MarkDirectionOptions = {
  remoteIgnoreHandled?: boolean;
};

type MicroSyncFn = ((
  rpathsAlpha: string[],
  rpathsBeta: string[],
) => Promise<void>) & {
  markAlphaToBeta: (
    paths: string[],
    opts?: MarkDirectionOptions,
  ) => Promise<void>;
  markBetaToAlpha: (
    paths: string[],
    opts?: MarkDirectionOptions,
  ) => Promise<void>;
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
  fetchRemoteAlphaSignatures,
  fetchRemoteBetaSignatures,
  numericIds = false,
  alphaRemoteLock,
  betaRemoteLock,
}: MicroSyncDeps) {
  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  const microLogger = logger.child("micro");

  let alphaTempDir: string | undefined;
  let betaTempDir: string | undefined;

  const dedupe = (paths: Iterable<string>) => Array.from(new Set(paths));

  const logPartial = (
    direction: "alpha->beta" | "beta->alpha",
    paths: string[],
    res: RsyncResult,
  ) => {
    microLogger.warn("partial transfer detected", {
      direction,
      paths: paths.length,
      code: res.code,
      stderr: res.stderr ? res.stderr.slice(0, 200) : undefined,
    });
  };

  const ensureNoPartial = (
    direction: "alpha->beta" | "beta->alpha",
    paths: string[],
    res: RsyncResult,
  ) => {
    if (res.code !== 23) return;
    const unique = dedupe(paths);
    if (!unique.length) return;
    logPartial(direction, unique, res);
    if (direction === "alpha->beta") {
      throw new PartialTransferError("alpha→beta transfer incomplete", {
        alphaPaths: unique,
      });
    } else {
      throw new PartialTransferError("beta→alpha transfer incomplete", {
        betaPaths: unique,
      });
    }
  };

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

    const applyEntries: SignatureEntry[] = [];
    const recentEntries = unique.map((path) => {
      const destStamp = destStamps.get(path);
      const sourceStamp = sourceStamps.get(path);

      let signature = signatureFromStamp(destStamp);
      if (!signature && !destIsRemote) {
        signature = buildSignatureFromFs(destRoot, path);
      }
      if (!signature) {
        signature = signatureFromStamp(sourceStamp);
      }

      if (!signature) {
        signature = {
          kind: "missing",
          opTs: Date.now(),
        };
      }

      const target =
        sourceStamp?.target ??
        destStamp?.target ??
        undefined;

      if (destIsRemote && signature.kind === "file") {
        applyEntries.push({ path, signature, target });
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
    recordRecentSend(destDb, direction, recentEntries);
    if (destIsRemote && applyEntries.length) {
      applySignaturesToDb(destDb, applyEntries, { numericIds });
    }
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

  const categorizeForRelease = (
    paths: string[],
    sigMap: Map<string, SendSignature | null>,
  ) => {
    const copies: string[] = [];
    const deletions: string[] = [];
    for (const path of paths) {
      const sig = sigMap.get(path);
      if (sig && sig.kind === "missing") {
        deletions.push(path);
      } else {
        copies.push(path);
      }
    }
    return { copies, deletions };
  };

  const buildReleaseEntries = (
    targets: string[],
    signatures: SignatureEntry[],
  ): ReleaseEntry[] => {
    if (!targets.length || !signatures.length) return [];
    const sigMap = new Map<string, SendSignature>();
    for (const entry of signatures) {
      sigMap.set(entry.path, entry.signature);
    }
    const releases: ReleaseEntry[] = [];
    for (const path of targets) {
      const sig = sigMap.get(path);
      if (!sig || sig.kind === "missing") continue;
      const watermark =
        sig.ctime ?? sig.mtime ?? sig.opTs ?? Date.now();
      releases.push({ path, watermark });
    }
    return releases;
  };

  async function finalizeRemoteLock(
    handle: RemoteLockHandle,
    lockedPaths: string[],
    copyTargets: string[],
    deleteTargets: string[],
    signatures: SignatureEntry[],
  ) {
    if (!lockedPaths.length) return;
    try {
      const releaseEntries = buildReleaseEntries(copyTargets, signatures);
      const released = new Set(releaseEntries.map((e) => e.path));
      if (releaseEntries.length) {
        await handle.release(releaseEntries);
      }
      const fallbackUnlock = [
        ...deleteTargets,
        ...copyTargets.filter((p) => !released.has(p)),
      ];
      if (fallbackUnlock.length) {
        await handle.unlock(fallbackUnlock);
      }
    } catch (err) {
      await handle.unlock(lockedPaths);
      throw err;
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

  type RsyncResult = {
    code: number | null;
    zero: boolean;
    ok: boolean;
    stderr?: string;
  };

  async function doRsync(
    direction: "alpha->beta" | "beta->alpha",
    listFile: string,
    extra: string[] = [],
  ): Promise<RsyncResult> {
    const compArgs = rsyncCompressionArgs(compress);
    const progressScope =
      direction === "alpha->beta"
        ? "micro.rsync.alpha->beta"
        : "micro.rsync.beta->alpha";

    if (direction === "alpha->beta") {
      const tempDirArg = betaIsRemote ? ".reflect-rsync-tmp" : betaTempDir;
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
          tempDir: tempDirArg,
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
      return res;
    } else {
      const tempDirArg = alphaIsRemote ? ".reflect-rsync-tmp" : alphaTempDir;
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
          tempDir: tempDirArg,
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
      return res;
    }
  }

  // Small helpers for specific intents
  // - Unified copy+delete (safe for remote sources): will copy present paths and
  //   delete them on the receiver if they are missing on the sender.
  async function rsyncCopyOrDelete(
    direction: "alpha->beta" | "beta->alpha",
    listFile: string,
  ): Promise<RsyncResult> {
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
  ): Promise<RsyncResult> {
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
    const setAList = Array.from(setA);
    const setBList = Array.from(setB);

    const recentFromBetaPre = alphaIsRemote
      ? getRecentSendSignatures(alphaDbPath, "beta->alpha", setAList)
      : new Map<string, SendSignature | null>();
    const recentFromAlphaPre = betaIsRemote
      ? getRecentSendSignatures(betaDbPath, "alpha->beta", setBList)
      : new Map<string, SendSignature | null>();

    if (alphaIsRemote && fetchRemoteAlphaSignatures && setAList.length) {
      const sentByBeta = setAList.filter((p) => recentFromBetaPre.has(p));
      const others = setAList.filter((p) => !recentFromBetaPre.has(p));
      if (sentByBeta.length) {
        await fetchRemoteAlphaSignatures(sentByBeta, { ignore: true });
      }
      if (others.length) {
        await fetchRemoteAlphaSignatures(others, { ignore: false });
      }
    }
    if (betaIsRemote && fetchRemoteBetaSignatures && setBList.length) {
      const sentByAlpha = setBList.filter((p) => recentFromAlphaPre.has(p));
      const others = setBList.filter((p) => !recentFromAlphaPre.has(p));
      if (sentByAlpha.length) {
        await fetchRemoteBetaSignatures(sentByAlpha, { ignore: true });
      }
      if (others.length) {
        await fetchRemoteBetaSignatures(others, { ignore: false });
      }
    }

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
        const betaReleasePlan = categorizeForRelease(
          toBeta,
          alphaSigMap,
        );
        const needBetaLock = betaIsRemote && !!betaRemoteLock;
        const betaLockedPaths = needBetaLock ? [...toBeta] : [];
        if (needBetaLock && betaLockedPaths.length) {
          await betaRemoteLock!.lock(betaLockedPaths);
        }
        let betaPostSignatures: SignatureEntry[] = [];
        try {
          if (alphaIsRemote || betaIsRemote) {
            log(
              "info",
              "realtime",
              `alpha→beta ${toBeta.length} paths (unified copy/delete)`,
            );
            const res = await rsyncCopyOrDelete("alpha->beta", listAllA2B);
            if (needBetaLock && betaLockedPaths.length && res.code === 23) {
              await betaRemoteLock!.unlock(betaLockedPaths);
            }
            ensureNoPartial("alpha->beta", toBeta, res);
          } else {
            const { files, others, missing } = await classifyLocal(
              alphaRoot,
              toBeta,
            );
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
                const res = await doRsync("alpha->beta", listFiles);
                ensureNoPartial("alpha->beta", files, res);
              }
            }
            const listOther = path.join(tmp, "alpha2beta.other");
            await writeFile(listOther, join0(others));
            if (await fileNonEmpty(listOther)) {
              log(
                "info",
                "realtime",
                `alpha→beta dirs/symlinks ${others.length} (no-recursive)`,
              );
              const res = await rsyncDirsAndLinks("alpha->beta", listOther);
              ensureNoPartial("alpha->beta", others, res);
            }
            const listMissing = path.join(tmp, "alpha2beta.missing");
            await writeFile(listMissing, join0(missing));
            if (await fileNonEmpty(listMissing)) {
              log("info", "realtime", `alpha→beta deletes ${missing.length}`);
              const res = await rsyncCopyOrDelete("alpha->beta", listMissing);
              ensureNoPartial("alpha->beta", missing, res);
            }
          }

          if (betaIsRemote && fetchRemoteBetaSignatures) {
            betaPostSignatures = await fetchRemoteBetaSignatures(toBeta, {
              ignore: false,
            });
          }
          if (needBetaLock && betaLockedPaths.length) {
            await finalizeRemoteLock(
              betaRemoteLock!,
              betaLockedPaths,
              betaReleasePlan.copies,
              betaReleasePlan.deletions,
              betaPostSignatures,
            );
          }
          recordDirection("alpha->beta", toBeta);
        } catch (err) {
          if (needBetaLock && betaLockedPaths.length) {
            try {
              await betaRemoteLock!.unlock(betaLockedPaths);
            } catch {}
          }
          throw err;
        }
      }

      // ===== β → α =====
      if (toAlpha.length) {
        const listAllB2A = path.join(tmp, "beta2alpha.all");
        await writeFile(listAllB2A, join0(toAlpha));
        const alphaReleasePlan = categorizeForRelease(
          toAlpha,
          betaSigMap,
        );
        const needAlphaLock = alphaIsRemote && !!alphaRemoteLock;
        const alphaLockedPaths = needAlphaLock ? [...toAlpha] : [];
        if (needAlphaLock && alphaLockedPaths.length) {
          await alphaRemoteLock!.lock(alphaLockedPaths);
        }
        let alphaPostSignatures: SignatureEntry[] = [];
        try {
          if (alphaIsRemote || betaIsRemote) {
            log(
              "info",
              "realtime",
              `beta→alpha ${toAlpha.length} paths (unified copy/delete)`,
            );
            const res = await rsyncCopyOrDelete("beta->alpha", listAllB2A);
            if (needAlphaLock && alphaLockedPaths.length && res.code === 23) {
              await alphaRemoteLock!.unlock(alphaLockedPaths);
            }
            ensureNoPartial("beta->alpha", toAlpha, res);
          } else {
            const { files, others, missing } = await classifyLocal(
              betaRoot,
              toAlpha,
            );
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
                const res = await doRsync("beta->alpha", listFiles);
                ensureNoPartial("beta->alpha", files, res);
              }
            }
            const listOther = path.join(tmp, "beta2alpha.other");
            await writeFile(listOther, join0(others));
            if (await fileNonEmpty(listOther)) {
              log(
                "info",
                "realtime",
                `beta→alpha dirs/symlinks ${others.length} (no-recursive)`,
              );
              const res = await rsyncDirsAndLinks("beta->alpha", listOther);
              ensureNoPartial("beta->alpha", others, res);
            }
            const listMissing = path.join(tmp, "beta2alpha.missing");
            await writeFile(listMissing, join0(missing));
            if (await fileNonEmpty(listMissing)) {
              log("info", "realtime", `beta→alpha deletes ${missing.length}`);
              const res = await rsyncCopyOrDelete("beta->alpha", listMissing);
              ensureNoPartial("beta->alpha", missing, res);
            }
          }

          if (alphaIsRemote && fetchRemoteAlphaSignatures) {
            alphaPostSignatures = await fetchRemoteAlphaSignatures(toAlpha, {
              ignore: false,
            });
          }
          if (betaIsRemote && fetchRemoteBetaSignatures) {
            await fetchRemoteBetaSignatures(toAlpha, { ignore: false });
          }
          if (needAlphaLock && alphaLockedPaths.length) {
            await finalizeRemoteLock(
              alphaRemoteLock!,
              alphaLockedPaths,
              alphaReleasePlan.copies,
              alphaReleasePlan.deletions,
              alphaPostSignatures,
            );
          }
          recordDirection("beta->alpha", toAlpha);
        } catch (err) {
          if (needAlphaLock && alphaLockedPaths.length) {
            try {
              await alphaRemoteLock!.unlock(alphaLockedPaths);
            } catch {}
          }
          throw err;
        }
      }
    } finally {
      await rm(tmp, { recursive: true, force: true });
    }
  };

  microSync.markAlphaToBeta = async (
    paths: string[],
    opts?: MarkDirectionOptions,
  ) => {
    //log("info", "realtime", `markAlphaToBeta: ${JSON.stringify(paths)}`);
    log("info", "realtime", `markAlphaToBeta: ${paths.length} paths`);
    const skipRemoteIgnore = !!opts?.remoteIgnoreHandled;
    if (!skipRemoteIgnore && betaIsRemote && fetchRemoteBetaSignatures && paths.length) {
      await fetchRemoteBetaSignatures(paths, { ignore: true });
    }
    recordDirection("alpha->beta", paths);
  };

  microSync.markBetaToAlpha = async (
    paths: string[],
    opts?: MarkDirectionOptions,
  ) => {
    //log("info", "realtime", `markBetaToAlpha: ${JSON.stringify(paths)}`);
    log("info", "realtime", `markBetaToAlpha: ${paths.length} paths`);
    const skipRemoteIgnore = !!opts?.remoteIgnoreHandled;
    if (!skipRemoteIgnore && alphaIsRemote && fetchRemoteAlphaSignatures && paths.length) {
      await fetchRemoteAlphaSignatures(paths, { ignore: true });
    }
    recordDirection("beta->alpha", paths);
  };

  return microSync;
}
