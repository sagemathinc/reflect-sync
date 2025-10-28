// micro-sync.ts
import path from "node:path";
import { tmpdir } from "node:os";
import {
  mkdtemp,
  rm,
  writeFile,
  lstat as fsLstat,
  stat as fsStat,
} from "node:fs/promises";
import type { SpawnOptions } from "node:child_process";
import { cpReflinkFromList, sameDevice } from "./reflink.js";

export type MicroSyncDeps = {
  alphaRoot: string;
  betaRoot: string;
  alphaHost?: string;
  betaHost?: string;
  prefer: "alpha" | "beta";
  dryRun: boolean;
  verbose?: boolean;
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
};

export function makeMicroSync(deps: MicroSyncDeps) {
  const {
    alphaRoot,
    betaRoot,
    alphaHost,
    betaHost,
    prefer,
    dryRun,
    verbose,
    spawnTask,
    log,
  } = deps;

  const alphaIsRemote = !!alphaHost;
  const betaIsRemote = !!betaHost;

  // Cooldown prevents rapid re-pushing of the *same* path in any direction.
  const MICRO_COOLDOWN_MS = Number(process.env.COOLDOWN_MS ?? 300);

  // Echo suppression prevents one-sided “echo” events from bouncing back
  // immediately after we ourselves copied the file to that side.
  const ECHO_SUPPRESS_MS = Number(process.env.MICRO_ECHO_SUPPRESS_MS ?? 2500);

  // Last time we pushed a path in a given direction.
  const lastA2B = new Map<string, number>();
  const lastB2A = new Map<string, number>();
  // Generic cooldown (direction-agnostic).
  const lastPush = new Map<string, number>();

  function keepFresh(rpaths: string[]) {
    const now = Date.now();
    const out: string[] = [];
    for (const r of rpaths) {
      const t = lastPush.get(r) || 0;
      if (now - t >= MICRO_COOLDOWN_MS) {
        out.push(r);
        lastPush.set(r, now);
      }
    }
    return out;
  }

  async function keepFilesLocal(root: string, rpaths: string[]) {
    const out: string[] = [];
    for (const r of rpaths) {
      try {
        const st = await fsLstat(path.join(root, r));
        if (st.isFile()) out.push(r);
      } catch {
        /* file may have vanished */
      }
    }
    return out;
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

  function rsyncRoots(
    fromRoot: string,
    fromHost: string | undefined,
    toRoot: string,
    toHost: string | undefined,
  ) {
    const slash = (s: string) => (s.endsWith("/") ? s : s + "/");
    const from = fromHost ? `${fromHost}:${slash(fromRoot)}` : slash(fromRoot);
    const to = toHost ? `${toHost}:${slash(toRoot)}` : slash(toRoot);
    const transport = fromHost || toHost ? (["-e", "ssh"] as string[]) : [];
    return { from, to, transport };
  }

  async function doRsync(
    direction: "alpha->beta" | "beta->alpha",
    listFile: string,
  ): Promise<boolean> {
    if (direction === "alpha->beta") {
      const { from, to, transport } = rsyncRoots(
        alphaRoot,
        alphaHost,
        betaRoot,
        betaHost,
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
        alphaRoot,
        alphaHost,
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
          `--files-from=${listFile}`,
          from,
          to,
        ],
        [0, 23, 24],
      );
      return !!res.lastZero;
    }
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

    // Only push actual files on local endpoints; rsync handles all types remotely.
    const toBetaFiles = alphaIsRemote
      ? keepFresh(toBeta)
      : keepFresh(await keepFilesLocal(alphaRoot, toBeta));
    const toAlphaFiles = betaIsRemote
      ? keepFresh(toAlpha)
      : keepFresh(await keepFilesLocal(betaRoot, toAlpha));

    if (toBetaFiles.length === 0 && toAlphaFiles.length === 0) return;

    const tmp = await mkdtemp(path.join(tmpdir(), "micro-plan-"));
    try {
      const listToBeta = path.join(tmp, "toBeta.list");
      const listToAlpha = path.join(tmp, "toAlpha.list");
      await writeFile(listToBeta, join0(toBetaFiles));
      await writeFile(listToAlpha, join0(toAlphaFiles));

      // --- α → β ---
      if (await fileNonEmpty(listToBeta)) {
        log("info", "realtime", `alpha→beta ${toBetaFiles.length} paths`);
        const localLocal = !alphaIsRemote && !betaIsRemote;
        let ok = false;

        if (!dryRun && localLocal) {
          try {
            if (await sameDevice(alphaRoot, betaRoot)) {
              await cpReflinkFromList(alphaRoot, betaRoot, listToBeta);
              ok = true;
            }
          } catch (e: any) {
            if (verbose)
              log(
                "warn",
                "realtime",
                "reflink alpha→beta failed; falling back to rsync",
                { err: String(e?.message || e) },
              );
            ok = false;
          }
        }
        if (!ok) {
          ok = await doRsync("alpha->beta", listToBeta);
        }
        if (ok) {
          const t = Date.now();
          for (const r of toBetaFiles) {
            lastA2B.set(r, t);
          }
        }
      }

      // --- β → α ---
      if (await fileNonEmpty(listToAlpha)) {
        log("info", "realtime", `beta→alpha ${toAlphaFiles.length} paths`);
        const localLocal = !alphaIsRemote && !betaIsRemote;
        let ok = false;

        if (!dryRun && localLocal) {
          try {
            if (await sameDevice(alphaRoot, betaRoot)) {
              await cpReflinkFromList(betaRoot, alphaRoot, listToAlpha);
              ok = true;
            }
          } catch (e: any) {
            if (verbose)
              log(
                "warn",
                "realtime",
                "reflink beta→alpha failed; falling back to rsync",
                { err: String(e?.message || e) },
              );
            ok = false;
          }
        }
        if (!ok) {
          ok = await doRsync("beta->alpha", listToAlpha);
        }
        if (ok) {
          const t = Date.now();
          for (const r of toAlphaFiles) {
            lastB2A.set(r, t);
          }
        }
      }
    } finally {
      await rm(tmp, { recursive: true, force: true });
    }
  };
}
