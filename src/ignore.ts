import ignore from "ignore";
import path from "node:path";

export type Ignorer = {
  ignoresFile: (r: string) => boolean; // file/symlink path
  ignoresDir: (r: string) => boolean; // directory path
  debug?: string[];
};

export function normalizeR(r: string): string {
  // rpath normalization; keep empty "" for root-safe callers
  return r.replace(/\\/g, "/").replace(/^\/+/, "");
}

function toDirPattern(pattern: string): string {
  return pattern.endsWith("/") ? pattern : `${pattern}/`;
}

function cleanPattern(pattern: string): string | null {
  const trimmed = pattern.trim();
  if (!trimmed) return null;
  return trimmed.replace(/\\/g, "/");
}

export function normalizeIgnorePatterns(patterns: string[]): string[] {
  const out = new Set<string>();
  for (const raw of patterns ?? []) {
    if (typeof raw !== "string") continue;
    const cleaned = cleanPattern(raw);
    if (cleaned) out.add(cleaned);
  }
  return Array.from(out);
}

export function serializeIgnoreRules(patterns: string[]): string | null {
  const cleaned = normalizeIgnorePatterns(patterns);
  return cleaned.length ? JSON.stringify(cleaned) : null;
}

export function deserializeIgnoreRules(raw?: string | null): string[] {
  if (!raw) return [];
  let lines;
  try {
    lines = JSON.parse(raw);
  } catch {
    console.warn("invalid ignore rules", { raw });
    return [];
  }
  return normalizeIgnorePatterns(lines);
}

export function collectIgnoreOption(value: string, acc: string[]): string[] {
  if (typeof value !== "string") return acc;
  const parts = value
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
  acc.push(...parts);
  return acc;
}

export function createIgnorer(patterns: string[] = []): Ignorer {
  const ig = ignore();
  const cleaned = normalizeIgnorePatterns(patterns);
  if (cleaned.length) {
    ig.add(cleaned);
  }
  return {
    ignoresFile: (r) => ig.ignores(normalizeR(r)),
    ignoresDir: (r) => ig.ignores(toDirPattern(normalizeR(r))),
  };
}

export function autoIgnoreForRoot(
  root: string,
  syncHome: string,
): string[] {
  if (!root || !syncHome) return [];
  const rootAbs = path.resolve(root);
  const homeAbs = path.resolve(syncHome);
  const rel = path.relative(rootAbs, homeAbs);
  if (!rel || rel.startsWith("..") || path.isAbsolute(rel)) {
    return [];
  }
  const posix = rel.split(path.sep).join("/");
  const dirPattern = posix.endsWith("/") ? posix : `${posix}/`;
  return normalizeIgnorePatterns([dirPattern]);
}

function ignoredByEitherFile(r: string, aIg: Ignorer, bIg: Ignorer): boolean {
  // If r or any of its parents are ignored by either side, skip it.
  // The 'ignore' lib handles dir/** patterns and trailing '/'
  return aIg.ignoresFile(r) || bIg.ignoresFile(r);
}

export function filterIgnored(
  rpaths: string[],
  aIg: Ignorer,
  bIg: Ignorer,
): string[] {
  if (!rpaths.length) return rpaths;
  return rpaths.filter((r) => !ignoredByEitherFile(r, aIg, bIg));
}

function ignoredByEitherDir(r: string, aIg: Ignorer, bIg: Ignorer): boolean {
  // If r or any of its parents are ignored by either side, skip it.
  // The 'ignore' lib handles dir/** patterns and trailing '/'
  return aIg.ignoresDir(r) || bIg.ignoresDir(r);
}

export function filterIgnoredDirs(
  rpaths: string[],
  aIg: Ignorer,
  bIg: Ignorer,
) {
  if (!rpaths.length) return rpaths;
  return rpaths.filter((r) => !ignoredByEitherDir(r, aIg, bIg));
}
