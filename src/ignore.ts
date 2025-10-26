import ignore from "ignore";
import { access, readFile } from "node:fs/promises";
import { join } from "node:path";

export type Ignorer = { ignores: (rpath: string) => boolean };

export async function loadIgnoreFile(root: string): Promise<Ignorer> {
  const ig = ignore();
  const f = join(root, ".ccsyncignore");
  try {
    await access(f);
    const raw = await readFile(f, "utf8");
    // Normalize Windows newlines and add patterns
    ig.add(raw.replace(/\r\n/g, "\n"));
  } catch {
    // no ignore file -> empty matcher
  }
  // We match on rpaths (POSIX-ish) that are already slash-separated in planner
  return ig;
}

function ignoredByEither(r: string, aIg: Ignorer, bIg: Ignorer): boolean {
  // If r or any of its parents are ignored by either side, skip it.
  // The 'ignore' lib handles dir/** patterns and trailing '/'
  return aIg.ignores(r) || bIg.ignores(r);
}

export function filterIgnored(
  rpaths: string[],
  aIg: Ignorer,
  bIg: Ignorer,
): string[] {
  if (!rpaths.length) return rpaths;
  return rpaths.filter((r) => !ignoredByEither(r, aIg, bIg));
}

export function filterIgnoredDirs(
  rpaths: string[],
  alphaIg: Ignorer,
  betaIg: Ignorer,
) {
  return rpaths.filter((r) => {
    const d = r.endsWith("/") ? r : r + "/";
    return !(alphaIg.ignores(d) || betaIg.ignores(d));
  });
}
