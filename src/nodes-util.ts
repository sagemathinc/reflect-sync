export type NodeKind = "f" | "d" | "l";

export type EntryKind = "file" | "dir" | "link" | "missing";

export function nodeKindToEntry(kind: string): EntryKind {
  switch (kind) {
    case "d":
      return "dir";
    case "l":
      return "link";
    default:
      return "file";
  }
}

export function deletionMtimeFromMeta(
  meta: { last_seen?: number | null; updated?: number | null; mtime?: number | null },
  fallback: number,
): number {
  const candidates = [
    meta.last_seen,
    meta.updated,
    meta.mtime,
  ].filter((v): v is number => Number.isFinite(v as number));
  if (candidates.length) {
    return candidates[0]! + 1;
  }
  return fallback;
}
