import fs from "node:fs";
import path from "node:path";

function resolveEntryPath(): string {
  const candidates: Array<string | undefined> = [
    process.env.REFLECT_ENTRY,
    process.argv[1],
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      if (fs.existsSync(candidate)) {
        return path.resolve(candidate);
      }
    } catch {
      // Ignore filesystem errors and continue.
    }
  }
  return "";
}

export function resolveSelfLaunch(): { command: string; args: string[] } {
  const entry = resolveEntryPath();
  const bundled = process.env.REFLECT_BUNDLED === "1";

  if (bundled && entry && entry !== process.execPath) {
    // Running out of a bundled single-file script: execute that script directly.
    return { command: entry, args: [] };
  }

  if (!entry) {
    // In SEA or other contexts without a discoverable entry script; fall back to execPath.
    return { command: process.execPath, args: [] };
  }

  // Standard Node.js execution: node <entry> ...
  return { command: process.execPath, args: [entry] };
}
