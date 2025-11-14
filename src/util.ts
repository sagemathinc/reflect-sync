import { rm as fsRm, rmdir as fsRmdir } from "node:fs/promises";
import path from "node:path";
import { sortChildFirst } from "./rsync.js";

export function wait(ms: number) {
  return ms > 0
    ? new Promise((resolve) => setTimeout(resolve, ms))
    : Promise.resolve();
}

export async function deleteRelativePaths(
  root: string,
  relPaths: readonly string[],
  opts: {
    logError?: (relPath: string, err: Error) => void;
  } = {},
): Promise<string[]> {
  if (!relPaths.length) return [];
  const unique = Array.from(new Set(relPaths.filter(Boolean)));
  sortChildFirst(unique);
  const deleted: string[] = [];
  for (const rel of unique) {
    const abs = path.join(root, rel);
    try {
      await fsRm(abs, { recursive: false, force: false });
      deleted.push(rel);
    } catch (err: any) {
      const code = err?.code;
      if (code === "ENOENT") {
        deleted.push(rel);
        continue;
      }
      if (code === "ERR_FS_EISDIR") {
        try {
          await fsRmdir(abs);
          deleted.push(rel);
          continue;
        } catch (dirErr: any) {
          if (dirErr?.code === "ENOENT") {
            deleted.push(rel);
            continue;
          }
          if (opts.logError) {
            opts.logError(
              rel,
              dirErr instanceof Error ? dirErr : new Error(String(dirErr)),
            );
          }
          continue;
        }
      }
      if (opts.logError) {
        opts.logError(rel, err instanceof Error ? err : new Error(String(err)));
      }
    }
  }
  return deleted;
}
