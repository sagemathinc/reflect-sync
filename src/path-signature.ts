import { lstat, readlink } from "node:fs/promises";
import path from "node:path";
import { fileDigest, modeHash, stringDigest } from "./hash.js";
import type { SendSignature } from "./recent-send.js";
import type { Stats } from "node:fs";

export type PathSignatureOptions = {
  hashAlg: string;
  numericIds?: boolean;
};

export type PathSignatureResult = {
  signature: SendSignature;
  target?: string | null;
};

function appendIds(hash: string, st: { uid?: number; gid?: number } | null) {
  if (!st) return hash;
  const uid = Number.isFinite(st.uid ?? NaN) ? Number(st.uid) : 0;
  const gid = Number.isFinite(st.gid ?? NaN) ? Number(st.gid) : 0;
  return `${hash}|${uid}:${gid}`;
}

export async function computePathSignature(
  absPath: string,
  opts: PathSignatureOptions,
  existingStats?: Stats,
): Promise<PathSignatureResult> {
  const now = Date.now();
  try {
    const st = existingStats ?? (await lstat(absPath));
    const mtime = (st as any).mtimeMs ?? st.mtime.getTime();
    const ctime = (st as any).ctimeMs ?? st.ctime.getTime();

    if (st.isSymbolicLink()) {
      const target = await readlink(absPath);
      return {
        signature: {
          kind: "link",
          opTs: mtime,
          mtime,
          ctime,
          hash: stringDigest(opts.hashAlg, target),
        },
        target,
      };
    }

    if (st.isDirectory()) {
      let hash = modeHash(st.mode);
      if (opts.numericIds) {
        hash = appendIds(hash, st);
      }
      return {
        signature: {
          kind: "dir",
          opTs: now,
          mtime,
          ctime,
          hash,
        },
      };
    }

    if (st.isFile()) {
      const digest = await fileDigest(opts.hashAlg, absPath, st.size);
      let hash = `${digest}|${modeHash(st.mode)}`;
      if (opts.numericIds) {
        hash = appendIds(hash, st);
      }
      return {
        signature: {
          kind: "file",
          opTs: mtime,
          mtime,
          ctime,
          size: st.size,
          hash,
          mode: st.mode,
          uid: opts.numericIds ? st.uid ?? undefined : undefined,
          gid: opts.numericIds ? st.gid ?? undefined : undefined,
        },
      };
    }

    // Fallback: treat unknown types as files without hashes.
    return {
      signature: {
        kind: "file",
        opTs: mtime,
        mtime,
        ctime,
        size: st.size ?? 0,
      },
    };
  } catch (err: any) {
    if (err?.code === "ENOENT") {
      return {
        signature: {
          kind: "missing",
          opTs: now,
        },
      };
    }
    throw err;
  }
}

export async function computeSignatureEntry(
  root: string,
  relPath: string,
  opts: PathSignatureOptions,
): Promise<PathSignatureResult & { path: string }> {
  const abs = path.join(root, relPath);
  const result = await computePathSignature(abs, opts);
  return { path: relPath, ...result };
}
