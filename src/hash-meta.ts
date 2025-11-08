import type { Stats } from "node:fs";
import { modeHash } from "./hash.js";

type MetaSource =
  | Pick<Stats, "mode" | "uid" | "gid">
  | {
      mode: number;
      uid?: number | null;
      gid?: number | null;
    };

function stringifyIds(uid?: number | null, gid?: number | null): string {
  const u = Number.isFinite(uid ?? NaN) ? Number(uid) : 0;
  const g = Number.isFinite(gid ?? NaN) ? Number(gid) : 0;
  return `${u}:${g}`;
}

/**
 * Given an existing hash string ("<digest>|<mode>[|uid:gid]") and the latest
 * stat data, rebuild the metadata suffix so we can reflect chmod/chown without
 * recomputing the content digest.
 */
export function withUpdatedMetadataHash(
  existing: string | null | undefined,
  st: MetaSource,
  numericIds: boolean,
): string | null {
  if (!existing) return null;
  const parts = existing.split("|");
  if (parts.length < 2 || !parts[0]) {
    return null;
  }
  const digest = parts[0];
  const updated: string[] = [digest, modeHash(st.mode)];
  if (numericIds) {
    updated.push(stringifyIds(st.uid, st.gid));
  } else if (parts.length >= 3) {
    updated.push(parts[2]);
  }
  return updated.join("|");
}
