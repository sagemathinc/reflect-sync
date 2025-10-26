import ignore from "ignore";
import path from "node:path";
import { readFile, access } from "node:fs/promises";

export type Ignorer = {
  ignoresFile: (r: string) => boolean; // file/symlink path
  ignoresDir: (r: string) => boolean; // directory path
  debug?: string[];
};

export async function loadIgnoreFile(root: string): Promise<Ignorer> {
  const ig = ignore();
  const file = path.join(root, ".ccsyncignore");
  try {
    await access(file);
    const raw = await readFile(file, "utf8");
    ig.add(raw.replace(/\r\n/g, "\n"));
  } catch {}
  // File check = r as-is; Dir check = r + "/" (gitignore semantics)
  return {
    ignoresFile: (r) => ig.ignores(normalizeR(r)),
    ignoresDir: (r) => ig.ignores(normalizeR(r).replace(/\/?$/, "/")),
  };
}

export function normalizeR(r: string): string {
  // rpath normalization; keep empty "" for root-safe callers
  return r.replace(/\\/g, "/").replace(/^\/+/, "");
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
