// src/path-rel.ts
import path from "node:path";

// posix only (ccsync is posix); if you want robust, use path.posix explicitly.
export function toRel(abs: string, root: string): string {
  if (abs === root) return ""; // canonical root row in dirs
  if (abs.startsWith(root + "/")) return abs.slice(root.length + 1);
  // Fallback: resolve and slice, so callers don’t explode on odd inputs
  const r = path.posix.resolve(root);
  const a = path.posix.resolve(abs);
  return a.startsWith(r + "/") ? a.slice(r.length + 1) : a;
}

export function toAbs(rel: string, root: string): string {
  return rel ? `${root}/${rel}` : root;
}
