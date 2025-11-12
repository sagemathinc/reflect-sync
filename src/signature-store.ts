import { getDb } from "./db.js";
import type { SendSignature } from "./recent-send.js";
import { withUpdatedMetadataHash } from "./hash-meta.js";
import { deletionMtimeFromMeta } from "./nodes-util.js";
import type { LogicalClock } from "./logical-clock.js";

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
  opts: { numericIds?: boolean; logicalClock?: LogicalClock } = {},
): void {
  if (!entries.length) return;
  const db = getDb(dbPath);
  try {
    const clock = opts.logicalClock;
    const now = Date.now();
    let standaloneClock = now;
    if (!clock) {
      try {
        const row = db
          .prepare(`SELECT MAX(updated) AS max_updated FROM nodes`)
          .get() as { max_updated?: number };
        if (row && Number.isFinite(row.max_updated)) {
          standaloneClock = Math.max(standaloneClock, Number(row.max_updated));
        }
      } catch {
        // ignore
      }
    }
    const nextClock = () => {
      if (clock) return clock.next();
      const tick = Date.now();
      const next = Math.max(tick, standaloneClock + 1);
      standaloneClock = next;
      return next;
    };
    type NodeKind = "f" | "d" | "l";
    type NodeRow = {
      kind: NodeKind;
      hash: string | null;
      size: number | null;
      ctime: number | null;
      hashed_ctime: number | null;
      link_target: string | null;
      last_seen: number | null;
      updated: number | null;
      mtime: number | null;
      hash_pending?: number | null;
      change_start?: number | null;
      change_end?: number | null;
    };
    type NodeWriteParams = {
      path: string;
      kind: NodeKind;
      hash: string;
      mtime: number;
      ctime?: number;
      hashed_ctime?: number | null;
      size: number;
      deleted: 0 | 1;
      updated?: number;
      last_seen?: number | null;
      link_target?: string | null;
      last_error?: string | null;
      hash_pending?: number;
      change_start?: number | null;
      change_end?: number | null;
    };

    const selectNodeStmt = db.prepare(
      `SELECT kind, hash, size, ctime, hashed_ctime, link_target, last_seen, updated, mtime, hash_pending, change_start, change_end FROM nodes WHERE path = ?`,
    );
    const nodeUpsertStmt = db.prepare(`
        INSERT INTO nodes(path, kind, hash, mtime, ctime, hashed_ctime, updated, size, deleted, hash_pending, change_start, change_end, last_seen, link_target, last_error)
        VALUES (@path, @kind, @hash, @mtime, @ctime, @hashed_ctime, @updated, @size, @deleted, @hash_pending, @change_start, @change_end, @last_seen, @link_target, @last_error)
        ON CONFLICT(path) DO UPDATE SET
          kind=excluded.kind,
          hash=excluded.hash,
          mtime=excluded.mtime,
          ctime=excluded.ctime,
          hashed_ctime=excluded.hashed_ctime,
          updated=excluded.updated,
          size=excluded.size,
          deleted=excluded.deleted,
          hash_pending=excluded.hash_pending,
          change_start=excluded.change_start,
          change_end=excluded.change_end,
          last_seen=excluded.last_seen,
          link_target=excluded.link_target,
          last_error=excluded.last_error
      `);
    const writeNode = (params: NodeWriteParams) => {
      const updated = params.updated ?? nextClock();
      const ctime = params.ctime ?? params.mtime;
      nodeUpsertStmt.run({
        path: params.path,
        kind: params.kind,
        hash: params.hash,
        mtime: params.mtime,
        ctime,
        hashed_ctime: params.hashed_ctime ?? null,
        updated,
        size: params.size,
        deleted: params.deleted,
        hash_pending: params.hash_pending ?? 0,
        change_start:
          params.change_start === undefined ? null : params.change_start,
        change_end: params.change_end === undefined ? null : params.change_end,
        last_seen: params.last_seen ?? null,
        link_target: params.link_target ?? null,
        last_error: params.last_error ?? null,
      });
    };
    const markDeleted = (path: string, kindHint?: NodeKind) => {
      const existing = selectNodeStmt.get(path) as NodeRow | undefined;
      const observed = Date.now();
      const deleteMtime = deletionMtimeFromMeta(existing ?? {}, observed);
      const updatedTick = nextClock();
      writeNode({
        path,
        kind: existing?.kind ?? kindHint ?? "f",
        hash: "",
        mtime: deleteMtime,
        ctime: deleteMtime,
        hashed_ctime: null,
        size: 0,
        deleted: 1,
        updated: updatedTick,
        last_seen: existing?.last_seen ?? null,
        link_target: null,
        change_start:
          existing?.change_start ?? existing?.updated ?? deleteMtime,
        change_end: null,
        hash_pending: 0,
      });
    };

    const tx = db.transaction((rows: SignatureEntry[]) => {
      for (const { path, signature, target } of rows) {
        const opTs = signature.opTs ?? now;
        switch (signature.kind) {
          case "file": {
            let hashValue = signature.hash ?? null;
            if (!hashValue && (signature.mode != null || opts.numericIds)) {
              try {
                const existing = selectNodeStmt.get(path) as
                  | NodeRow
                  | undefined;
                hashValue = existing?.hash ?? null;
              } catch {}
            }
            if (hashValue && (signature.mode != null || opts.numericIds)) {
              const updatedHash = withUpdatedMetadataHash(
                hashValue,
                {
                  mode: signature.mode ?? 0,
                  uid: signature.uid ?? null,
                  gid: signature.gid ?? null,
                },
                !!opts.numericIds,
              );
              if (updatedHash) {
                hashValue = updatedHash;
              }
            }
            const updatedTick = nextClock();
            writeNode({
              path,
              kind: "f",
              hash: hashValue ?? "",
              mtime: signature.mtime ?? opTs,
              ctime: signature.ctime ?? signature.mtime ?? opTs,
              hashed_ctime: signature.ctime ?? null,
              size: signature.size ?? 0,
              deleted: 0,
              updated: updatedTick,
              last_seen: null,
              link_target: null,
              hash_pending: 0,
              change_start: opTs,
              change_end: updatedTick,
            });
            break;
          }
          case "dir": {
            const updatedTick = nextClock();
            writeNode({
              path,
              kind: "d",
              hash: signature.hash ?? "",
              mtime: signature.mtime ?? opTs,
              ctime: signature.ctime ?? signature.mtime ?? opTs,
              size: 0,
              deleted: 0,
              updated: updatedTick,
              last_seen: null,
              link_target: null,
              hash_pending: 0,
              change_start: opTs,
              change_end: updatedTick,
            });
            break;
          }
          case "link": {
            const linkTarget = target ?? "";
            const updatedTick = nextClock();
            writeNode({
              path,
              kind: "l",
              hash: signature.hash ?? "",
              mtime: signature.mtime ?? opTs,
              ctime: signature.ctime ?? signature.mtime ?? opTs,
              size: linkTarget.length,
              deleted: 0,
              updated: updatedTick,
              last_seen: null,
              link_target: linkTarget,
              hash_pending: 0,
              change_start: opTs,
              change_end: updatedTick,
            });
            break;
          }
          default:
            markDeleted(path);
            break;
        }
      }
    });

    tx(entries);
  } finally {
    db.close();
  }
}
