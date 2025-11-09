import type { OpStamp, EntryKind } from "./op-stamp.js";

export type SendSignature = {
  kind: EntryKind;
  opTs: number | null;
  size?: number | null;
  mtime?: number | null;
  ctime?: number | null;
  hash?: string | null;
  mode?: number | null;
  uid?: number | null;
  gid?: number | null;
};

function compactSignature(sig: SendSignature | null): SendSignature | null {
  if (!sig) return null;
  const normalized: SendSignature = {
    kind: sig.kind,
    opTs: sig.opTs ?? null,
  };
  if (sig.size != null) normalized.size = sig.size;
  if (sig.mtime != null) normalized.mtime = sig.mtime;
  if (sig.ctime != null) normalized.ctime = sig.ctime;
  if (sig.hash != null) normalized.hash = sig.hash;
  if (sig.mode != null) normalized.mode = sig.mode;
  if (sig.uid != null) normalized.uid = sig.uid;
  if (sig.gid != null) normalized.gid = sig.gid;
  return normalized;
}

export function signatureFromStamp(stamp?: OpStamp): SendSignature | null {
  if (!stamp) return null;
  const kind: EntryKind = stamp.deleted ? "missing" : stamp.kind;
  return compactSignature({
    kind,
    opTs: stamp.opTs ?? null,
    size: stamp.size ?? null,
    mtime: stamp.mtime ?? null,
    ctime: stamp.ctime ?? null,
    hash: stamp.hash ?? null,
  });
}

export function signatureEquals(
  a?: SendSignature | null,
  b?: SendSignature | null,
): boolean {
  if (!a && !b) return true;
  if (!a || !b) return false;
  if (a.kind === "missing" && b.kind === "missing") {
    return true;
  }
  if (a.ctime && b.ctime) {
    return a.kind === b.kind && a.ctime === b.ctime;
  }
  if (a.hash && b.hash) {
    return a.kind === b.kind && a.hash === b.hash;
  }
  const same = (x: number | string | null | undefined, y: typeof x) => {
    if (x == null || y == null) return true;
    return x === y;
  };
  return (
    a.kind === b.kind &&
    same(a.size, b.size) &&
    same(a.mtime, b.mtime) &&
    same(a.ctime, b.ctime) &&
    same(a.mode, b.mode) &&
    same(a.uid, b.uid) &&
    same(a.gid, b.gid)
  );
}
