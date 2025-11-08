import { getDb } from "./db.js";
import type { OpStamp, EntryKind } from "./op-stamp.js";

export type SyncDirection = "alpha->beta" | "beta->alpha";

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

const SQLITE_MAX_VARIABLE_NUMBER = 999;

function chunkArray<T>(items: T[], chunkSize: number): T[][] {
  if (items.length <= chunkSize) return [items];
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    out.push(items.slice(i, i + chunkSize));
  }
  return out;
}

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

export function recordRecentSend(
  dbPath: string,
  direction: SyncDirection,
  entries: Array<{ path: string; signature: SendSignature | null }>,
): void {
  if (!entries.length) return;
  const db = getDb(dbPath);
  try {
    const stmt = db.prepare(`
      INSERT INTO recent_send(path, direction, op_ts, signature, sent_at)
      VALUES(?, ?, ?, ?, ?)
      ON CONFLICT(path, direction)
        DO UPDATE SET op_ts = excluded.op_ts,
                      signature = excluded.signature,
                      sent_at = excluded.sent_at
    `);
    const now = Date.now();
    const tx = db.transaction((items: typeof entries) => {
      for (const item of items) {
        const sig = compactSignature(item.signature);
        stmt.run(
          item.path,
          direction,
          sig?.opTs ?? null,
          sig ? JSON.stringify(sig) : null,
          now,
        );
      }
    });
    tx(entries);
  } finally {
    db.close();
  }
}

export function getRecentSendSignatures(
  dbPath: string,
  direction: SyncDirection,
  paths: string[],
): Map<string, SendSignature | null> {
  const result = new Map<string, SendSignature | null>();
  if (!paths.length) return result;
  const db = getDb(dbPath);
  try {
    const batches = chunkArray(paths, SQLITE_MAX_VARIABLE_NUMBER - 1);
    for (const batch of batches) {
      const placeholders = batch.map(() => "?").join(",");
      const stmt = db.prepare(
        `SELECT path, signature FROM recent_send WHERE direction = ? AND path IN (${placeholders})`,
      );
      for (const row of stmt.iterate(direction, ...batch)) {
        const raw = row.signature as string | null;
        result.set(row.path as string, raw ? JSON.parse(raw) : null);
      }
    }
  } finally {
    db.close();
  }
  return result;
}

export function purgeRecentSend(
  dbPath: string,
  direction: SyncDirection,
  paths: string[],
): void {
  if (!paths.length) return;
  const db = getDb(dbPath);
  try {
    const stmt = db.prepare(
      `DELETE FROM recent_send WHERE direction = ? AND path = ?`,
    );
    const tx = db.transaction((items: string[]) => {
      for (const p of items) {
        stmt.run(direction, p);
      }
    });
    const batches = chunkArray(paths, SQLITE_MAX_VARIABLE_NUMBER - 1);
    for (const batch of batches) {
      tx(batch);
    }
  } finally {
    db.close();
  }
}
