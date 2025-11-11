import { getDb } from "./db.js";
import type { SendSignature } from "./recent-send.js";
import { withUpdatedMetadataHash } from "./hash-meta.js";

export interface SignatureEntry {
  path: string;
  signature: SendSignature;
  target?: string | null;
}

/**
 * Apply remote signatures to the local mirror database so later restricted cycles
 * see the fresh metadata. Mirrors the behaviour of ingest-delta but with a much
 * smaller surface.
 */
export function applySignaturesToDb(
  dbPath: string,
  entries: SignatureEntry[],
  opts: { numericIds?: boolean; writer?: "legacy" | "nodes" | "both" } = {},
): void {
  if (!entries.length) return;
  const db = getDb(dbPath);
  try {
    const now = Date.now();
    const writerMode = opts.writer ?? "legacy";
    const useNodes = writerMode === "nodes" || writerMode === "both";
    const useLegacy = writerMode === "legacy" || writerMode === "both";
    const lookupHash = useLegacy
      ? db.prepare(`SELECT hash FROM files WHERE path = ?`)
      : null;
    const upsertFile = useLegacy
      ? db.prepare(`
INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (?, ?, NULL, ?, ?, ?, 0, ?, NULL)
ON CONFLICT(path) DO UPDATE SET
  size       = COALESCE(excluded.size, files.size),
  mtime      = COALESCE(excluded.mtime, files.mtime),
  op_ts      = CASE WHEN excluded.op_ts >= files.op_ts THEN excluded.op_ts ELSE files.op_ts END,
  hash       = COALESCE(excluded.hash, files.hash),
  deleted    = 0,
  last_seen  = CASE WHEN excluded.last_seen > files.last_seen THEN excluded.last_seen ELSE files.last_seen END
`)
      : null;

    const upsertDir = useLegacy
      ? db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, NULL, ?, ?, ?, 0, ?)
ON CONFLICT(path) DO UPDATE SET
  mtime     = COALESCE(excluded.mtime, dirs.mtime),
  op_ts     = CASE WHEN excluded.op_ts >= dirs.op_ts THEN excluded.op_ts ELSE dirs.op_ts END,
  hash      = COALESCE(excluded.hash, dirs.hash),
  deleted   = 0,
  last_seen = CASE WHEN excluded.last_seen > dirs.last_seen THEN excluded.last_seen ELSE dirs.last_seen END
`)
      : null;

    const upsertLink = useLegacy
      ? db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, ?, NULL, ?, ?, ?, 0, ?)
ON CONFLICT(path) DO UPDATE SET
  target    = COALESCE(excluded.target, links.target),
  mtime     = COALESCE(excluded.mtime, links.mtime),
  op_ts     = CASE WHEN excluded.op_ts >= links.op_ts THEN excluded.op_ts ELSE links.op_ts END,
  hash      = COALESCE(excluded.hash, links.hash),
  deleted   = 0,
  last_seen = CASE WHEN excluded.last_seen > links.last_seen THEN excluded.last_seen ELSE links.last_seen END
`)
      : null;

    const markFileDeleted = useLegacy
      ? db.prepare(`
INSERT INTO files(path, size, ctime, mtime, op_ts, hash, deleted, last_seen, hashed_ctime)
VALUES (?, NULL, NULL, NULL, ?, NULL, 1, ?, NULL)
ON CONFLICT(path) DO UPDATE SET
  deleted   = 1,
  op_ts     = CASE WHEN excluded.op_ts >= files.op_ts THEN excluded.op_ts ELSE files.op_ts END,
  last_seen = CASE WHEN excluded.last_seen > files.last_seen THEN excluded.last_seen ELSE files.last_seen END
`)
      : null;

    const markDirDeleted = useLegacy
      ? db.prepare(`
INSERT INTO dirs(path, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, NULL, NULL, ?, NULL, 1, ?)
ON CONFLICT(path) DO UPDATE SET
  deleted   = 1,
  op_ts     = CASE WHEN excluded.op_ts >= dirs.op_ts THEN excluded.op_ts ELSE dirs.op_ts END,
  last_seen = CASE WHEN excluded.last_seen > dirs.last_seen THEN excluded.last_seen ELSE dirs.last_seen END
`)
      : null;

    const markLinkDeleted = useLegacy
      ? db.prepare(`
INSERT INTO links(path, target, ctime, mtime, op_ts, hash, deleted, last_seen)
VALUES (?, NULL, NULL, NULL, ?, NULL, 1, ?)
ON CONFLICT(path) DO UPDATE SET
  deleted   = 1,
  op_ts     = CASE WHEN excluded.op_ts >= links.op_ts THEN excluded.op_ts ELSE links.op_ts END,
  last_seen = CASE WHEN excluded.last_seen > links.last_seen THEN excluded.last_seen ELSE links.last_seen END
`)
      : null;

    const upsertNode = useNodes
      ? db.prepare(`
        INSERT INTO nodes(path, kind, hash, mtime, updated, size, deleted, last_error)
        VALUES (@path, @kind, @hash, @mtime, @updated, @size, @deleted, NULL)
        ON CONFLICT(path) DO UPDATE SET
          kind=excluded.kind,
          hash=excluded.hash,
          mtime=excluded.mtime,
          updated=excluded.updated,
          size=excluded.size,
          deleted=excluded.deleted,
          last_error=excluded.last_error
      `)
      : null;
    const markNodeDeleted = useNodes
      ? db.prepare(`
        INSERT INTO nodes(path, kind, hash, mtime, updated, size, deleted, last_error)
        VALUES (@path, 'f', '', @mtime, @updated, 0, 1, NULL)
        ON CONFLICT(path) DO UPDATE SET
          deleted=1,
          mtime=excluded.mtime,
          updated=excluded.updated,
          last_error=NULL
      `)
      : null;

    const tx = db.transaction((rows: SignatureEntry[]) => {
      for (const { path, signature, target } of rows) {
        const opTs = signature.opTs ?? now;
        switch (signature.kind) {
          case "file": {
            let hashValue = signature.hash ?? null;
            if (
              !hashValue &&
              useLegacy &&
              (signature.mode != null || opts.numericIds)
            ) {
              try {
                const row = lookupHash!.get(path) as {
                  hash?: string | null;
                };
                hashValue = row?.hash ?? null;
              } catch {}
            }
            if (
              hashValue &&
              useLegacy &&
              (signature.mode != null || opts.numericIds)
            ) {
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
            if (useLegacy) {
              upsertFile!.run(
                path,
                signature.size ?? null,
                signature.mtime ?? null,
                opTs,
                hashValue,
                now,
              );
            }
            const finalHash = hashValue ?? "";
            if (useNodes) {
              upsertNode!.run({
                path,
                kind: "f",
                hash: finalHash,
                mtime: signature.mtime ?? opTs,
                updated: now,
                size: signature.size ?? 0,
                deleted: 0,
              });
            }
            break;
          }
          case "dir":
            if (useLegacy) {
              upsertDir!.run(
                path,
                signature.mtime ?? null,
                opTs,
                signature.hash ?? null,
                now,
              );
            }
            if (useNodes) {
              upsertNode!.run({
                path,
                kind: "d",
                hash: signature.hash ?? "",
                mtime: signature.mtime ?? opTs,
                updated: now,
                size: 0,
                deleted: 0,
              });
            }
            break;
          case "link":
            if (useLegacy) {
              upsertLink!.run(
                path,
                target ?? null,
                signature.mtime ?? null,
                opTs,
                signature.hash ?? null,
                now,
              );
            }
            if (useNodes) {
              upsertNode!.run({
                path,
                kind: "l",
                hash: target ?? "",
                mtime: signature.mtime ?? opTs,
                updated: now,
                size: (target ?? "").length,
                deleted: 0,
              });
            }
            break;
          case "missing":
          default:
            if (useLegacy) {
              markFileDeleted!.run(path, opTs, now);
              markDirDeleted!.run(path, opTs, now);
              markLinkDeleted!.run(path, opTs, now);
            }
            if (useNodes) {
              const observed = now;
              markNodeDeleted!.run({
                path,
                mtime: observed,
                updated: observed,
              });
            }
            break;
        }
      }
    });

    tx(entries);
  } finally {
    db.close();
  }
}
