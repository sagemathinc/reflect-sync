import { getDb } from "./db.js";
import type { SendSignature } from "./recent-send.js";
import { withUpdatedMetadataHash } from "./hash-meta.js";

export interface SignatureEntry {
  path: string;
  signature: SendSignature;
  target?: string | null;
}

/**
 * Apply remote signatures to the local mirror database so later scans/micro-sync
 * see the fresh metadata. Mirrors the behaviour of ingest-delta but with a much
 * smaller surface.
 */
export function applySignaturesToDb(
  dbPath: string,
  entries: SignatureEntry[],
  opts: { numericIds?: boolean } = {},
): void {
  if (!entries.length) return;
  const db = getDb(dbPath);
  try {
    const now = Date.now();
    const lookupHash = db.prepare(`SELECT hash FROM files WHERE path = ?`);
    const upsertFile = db.prepare(`
INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (?, ?, NULL, ?, ?, ?, 0, ?, NULL)
ON CONFLICT(path) DO UPDATE SET
  size       = COALESCE(excluded.size, files.size),
  mtime      = COALESCE(excluded.mtime, files.mtime),
  op_ts      = CASE WHEN excluded.op_ts >= files.op_ts THEN excluded.op_ts ELSE files.op_ts END,
  hash       = COALESCE(excluded.hash, files.hash),
  deleted    = 0,
  last_seen  = CASE WHEN excluded.last_seen > files.last_seen THEN excluded.last_seen ELSE files.last_seen END
`);

    const upsertDir = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, NULL, ?, ?, ?, 0, ?)
ON CONFLICT(path) DO UPDATE SET
  mtime     = COALESCE(excluded.mtime, dirs.mtime),
  op_ts     = CASE WHEN excluded.op_ts >= dirs.op_ts THEN excluded.op_ts ELSE dirs.op_ts END,
  hash      = COALESCE(excluded.hash, dirs.hash),
  deleted   = 0,
  last_seen = CASE WHEN excluded.last_seen > dirs.last_seen THEN excluded.last_seen ELSE dirs.last_seen END
`);

    const upsertLink = db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, ?, NULL, ?, ?, ?, 0, ?)
ON CONFLICT(path) DO UPDATE SET
  target    = COALESCE(excluded.target, links.target),
  mtime     = COALESCE(excluded.mtime, links.mtime),
  op_ts     = CASE WHEN excluded.op_ts >= links.op_ts THEN excluded.op_ts ELSE links.op_ts END,
  hash      = COALESCE(excluded.hash, links.hash),
  deleted   = 0,
  last_seen = CASE WHEN excluded.last_seen > links.last_seen THEN excluded.last_seen ELSE links.last_seen END
`);

    const markFileDeleted = db.prepare(`
INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (?, NULL, NULL, NULL, ?, NULL, 1, ?, NULL)
ON CONFLICT(path) DO UPDATE SET
  deleted   = 1,
  op_ts     = CASE WHEN excluded.op_ts >= files.op_ts THEN excluded.op_ts ELSE files.op_ts END,
  last_seen = CASE WHEN excluded.last_seen > files.last_seen THEN excluded.last_seen ELSE files.last_seen END
`);

    const markDirDeleted = db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, NULL, NULL, ?, NULL, 1, ?)
ON CONFLICT(path) DO UPDATE SET
  deleted   = 1,
  op_ts     = CASE WHEN excluded.op_ts >= dirs.op_ts THEN excluded.op_ts ELSE dirs.op_ts END,
  last_seen = CASE WHEN excluded.last_seen > dirs.last_seen THEN excluded.last_seen ELSE dirs.last_seen END
`);

    const markLinkDeleted = db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, NULL, NULL, NULL, ?, NULL, 1, ?)
ON CONFLICT(path) DO UPDATE SET
  deleted   = 1,
  op_ts     = CASE WHEN excluded.op_ts >= links.op_ts THEN excluded.op_ts ELSE links.op_ts END,
  last_seen = CASE WHEN excluded.last_seen > links.last_seen THEN excluded.last_seen ELSE links.last_seen END
`);

    const tx = db.transaction((rows: SignatureEntry[]) => {
      for (const { path, signature, target } of rows) {
        const opTs = signature.opTs ?? now;
        switch (signature.kind) {
          case "file": {
            let hashValue = signature.hash ?? null;
            if (!hashValue && (signature.mode != null || opts.numericIds)) {
              try {
                const row = lookupHash.get(path) as { hash?: string | null };
                hashValue = row?.hash ?? null;
              } catch {}
            }
            if (hashValue && (signature.mode != null || opts.numericIds)) {
              const updated = withUpdatedMetadataHash(
                hashValue,
                {
                  mode: signature.mode ?? 0,
                  uid: signature.uid ?? null,
                  gid: signature.gid ?? null,
                },
                !!opts.numericIds,
              );
              if (updated) {
                hashValue = updated;
              }
            }
            upsertFile.run(
              path,
              signature.size ?? null,
              signature.mtime ?? null,
              opTs,
              hashValue,
              now,
            );
            break;
          }
          case "dir":
            upsertDir.run(
              path,
              signature.mtime ?? null,
              opTs,
              signature.hash ?? null,
              now,
            );
            break;
          case "link":
            upsertLink.run(
              path,
              target ?? null,
              signature.mtime ?? null,
              opTs,
              signature.hash ?? null,
              now,
            );
            break;
          case "missing":
          default:
            markFileDeleted.run(path, opTs, now);
            markDirDeleted.run(path, opTs, now);
            markLinkDeleted.run(path, opTs, now);
            break;
        }
      }
    });

    tx(entries);
  } finally {
    db.close();
  }
}
