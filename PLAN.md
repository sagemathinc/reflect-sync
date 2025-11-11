## Rewrite Plan

We now understand the problem space well enough to aim for a clean, obviously-correct implementation. The goals:

- A merge algorithm that always converges, even if scans/deltas are delayed or the scheduler restarts mid-cycle.
- A single source of truth per endpoint (alpha/beta/base) so the planner can decide every action (copy/delete) in one indexed query.
- Hot-syncs are just restricted full merges: watchers write precise metadata into the DB, and the same planner handles the rest.

## Node-based schema

Each SQLite database (alpha, beta, base) exposes one table covering every filesystem entry:

| column    | type   | notes |
|-----------|--------|-------|
| `path`    | TEXT PRIMARY KEY | relative path |
| `kind`    | TEXT NOT NULL    | `'f'` (file), `'d'` (dir), `'l'` (symlink) |
| `hash`    | TEXT NOT NULL    | sha256 (+uid/gid/mode when rooted), symlink target, or `''` for dirs |
| `mtime`   | REAL NOT NULL    | filesystem mtime |
| `updated` | REAL NOT NULL    | logical timestamp we control (e.g., scan time) |
| `size`    | INTEGER NOT NULL | informational only |
| `deleted` | INTEGER NOT NULL | 0/1 tombstone |
| `last_error` | TEXT | JSON blob describing the last failed merge op (null on success) |

No `ctime` column—we can’t reproduce it faithfully after copy, so we rely on `updated` instead. Base uses the same schema, so mirroring rows between DBs is trivial.

## Merge query

A single `LEFT JOIN` + anti-join surfaces all paths requiring work. This runs in ~1 s per million nodes in local tests.

```sql
WITH
pairs AS (
  SELECT
    a.path,
    a.kind    AS a_kind,
    a.hash    AS a_hash,
    a.mtime   AS a_mtime,
    a.updated AS a_updated,
    a.size    AS a_size,
    a.deleted AS a_deleted,
    a.last_error AS a_error,
    b.kind    AS b_kind,
    b.hash    AS b_hash,
    b.mtime   AS b_mtime,
    b.updated AS b_updated,
    b.size    AS b_size,
    b.deleted AS b_deleted
    b.last_error AS b_error
  FROM alpha.nodes a
  LEFT JOIN beta.nodes b ON b.path = a.path
  WHERE b.path IS NULL              -- only on alpha
     OR a.kind    <> b.kind
     OR a.hash    <> b.hash
     OR a.deleted <> b.deleted
),

beta_only AS (
  SELECT
    b.path,
    NULL      AS a_kind,
    NULL      AS a_hash,
    NULL      AS a_mtime,
    NULL      AS a_updated,
    NULL      AS a_size,
    1         AS a_deleted,
    b.kind    AS b_kind,
    b.hash    AS b_hash,
    b.mtime   AS b_mtime,
    b.updated AS b_updated,
    b.size    AS b_size,
    b.deleted AS b_deleted
  FROM beta.nodes b
  LEFT JOIN alpha.nodes a ON a.path = b.path
  WHERE a.path IS NULL
),

diff AS (
  SELECT * FROM pairs
  UNION ALL
  SELECT * FROM beta_only
)

SELECT
  diff.path,
  diff.a_kind,    diff.a_hash,    diff.a_mtime,
  diff.a_updated, diff.a_size,    diff.a_deleted,
  diff.a_error,
  diff.b_kind,    diff.b_hash,    diff.b_mtime,
  diff.b_updated, diff.b_size,    diff.b_deleted,
  diff.b_error,
  base.kind       AS base_kind,
  base.hash       AS base_hash,
  base.mtime      AS base_mtime,
  base.updated    AS base_updated,
  base.deleted    AS base_deleted,
  base.last_error AS base_error
FROM diff
LEFT JOIN base.nodes AS base ON base.path = diff.path
ORDER BY diff.path;
```

Hot-sync windows reuse the same query with filters (`AND a.updated >= :floor`, etc.) or explicit path temp tables.

## Operational strategy

1. **Scanning**  
   - Full scans walk the filesystem, compute hashes (including uid/gid/mode for root copies), and upsert into `nodes`, bumping `updated`.  
   - Watchers do the same for their touched paths (no special “signature” tables anymore).

2. **Planning**  
   - Run the query above (optionally restricted) to obtain every path where alpha/beta/base disagree.  
   - Feed the rows into a simple resolver that decides: copy alpha→beta, copy beta→alpha, delete alpha, delete beta. Policies (prefer alpha/beta, LWW on `mtime` vs `updated`, etc.) live here.

3. **Executing**  
   - Convert the plan into rsync/reflink operations. Each copy/delete is atomic for its path (files, dirs, symlinks).  
   - No rsync optimizations that infer child nodes; every listed path is explicitly tracked so we always know what changed.

4. **Mirroring DB state**  
   - After a successful copy, insert the exact source row into the destination `nodes` table and update base to match.  
   - After a delete, mark the destination row deleted and propagate the tombstone to base.  
   - Because we mirror immediately, the next scan sees the same data whether or not the filesystem changed mid-copy; any new edits simply show up as fresh rows on the next merge.

5. **Hot-sync**  
   - Watchers enqueue paths, immediately update `nodes`, and trigger a restricted merge using the same pipeline.  
   - If hot sync is disabled, nothing special happens; the next full cycle converges via the same planner.

6. **Self-heal**  
   - Even if remote delta ingestion drops events or the scheduler restarts before finishing a cycle, the next merge re-runs the query and produces operations until both sides equal base. There’s no separate “signature” bookkeeping to get out of sync.

## Work items

1. \(done\) Implement the new `nodes` schema in alpha/beta/base \(drop legacy tables once stable\).
2. (done) Update scan/watch code to write into `nodes` \(hashes, kinds, `updated` timestamps\).
3. (done) Replace the merge planner with the query \+ resolver described above.
4. (done) Reuse existing rsync/reflink helpers to execute copy/delete operations \(one path per entry, no implicit recursion\).
5. (done) Mirror DB state after each confirmed operation \(source row → destination \+ base\).
6. (done) Wire hot\-sync to use restricted versions of the same scan/merge pipeline.
7. Rewrite "reflect sync" to instead use the output of the merge plan -- done = all files transfer successfully so database is same on both sides. Before reflect sync depended on the digests, which we are no longer computing.  Also add a nice green checkbox to the "reflect list" when the last known full scan showed that that the sync roots are the same.  Do not store and compute the digest as part of a normal sync cycle, since it's just as expensive to compute as doing the whole merge strategy, and it's only useful to compare the two sides, which we already do directly.
8. Remove old signature/recent\-send logic and simplify docs/tests around the new model.
9. Implement a tracing table for debugging.  Enable with an env variable being set, e.g., REFLECT_TRACE_ALL=1.   When set, we log a lot of information to a new db called trace.db, to help with debugging.  It will explain exactly what the diffs were during each 3-way merge, both with full scans and hotsync.  Basically when set the 3-way merge function should record:
   - all the paths that are different with the metadata (exactly the output of that huge sql query, plus a timestamp)
   - what the merge strategy decided to do: the operations, and for each operation, whether or not it succeeded.
With this in hand, we should be able to look at a given problematic file and see every time it out of sync, what the plan was for it, and what happened. Probably the trace.db table should thus have columns and path should be indexed:

  time | path | alpha state... | beta state... | planned operation | did op work

This should make it very easy to see "why is path this way now?"  What have we done to it.  It will also be very useful for unit testing, since we can just read this table and verify what we think should happen actually did happen.


With this structure, every sync cycle is deterministic: the planner surfaces all discrepancies, the executor performs concrete operations, and the databases converge immediately if those operations succeed.


## Big Scans and file transfers versus Realtime Updates

We need to structure things so that a single large file being
transferred or a large scan doesn't block the realtime
transfer if actively edited files.  Some ideas:

- make sure that when we divide files up into chunks to 
  send via rsync that we bound the total *size* of those
  files.  We know the size ahead of time of all files.
  that is better than batching by number of files.
- I think it should be possible to do operations concurrently
  with files x and y in many cases. I can't think of any
  problem at all when x!=y except e.g., if x is a directory
  and y is contained in x, and the operation is "delete x".
- when we start carrying out operations we store in memory
  what operations are queued
- when deciding what operations to do as part of a 3-way merge,
  we discard any that are not allowed due to currently queued or
  running operations, then start/queue up the rest.

With this approach, we could have a full scan *and* several hot watch
transfers safely happening all at once.   The real limit is the
impact on cpu/bandwidth.  

## Other questions and thoughts about merge plans

Question: could we completely eliminate the --prefer option entirely and instead make
  that a part of the merge-strategy name?   For mirror-to-alpha and mirror-to-beta,
  it's not meaningful.  For lww-mtime we could have:
 
  - 'lww-mtime-prefer-alpha'
  - 'lww-mtime-prefer-beta'
  - 'lww-mtime-prefer-updated'
  
## Memory usage

It seems really large when doing stress tests, e.g, during scan and merge planning. This is not surprising given we grab all the diff out of the db in one query, and that could blow up into a lot of used memory (it hit maybe 6GB?)

```
2808677 wstein    20   0   13.7g   3.6g  62988 R 637.2  11.9   3:29.88 MainThread 
```

In contrast, the on disk database usage is under control:

```
wstein@lite:~/build/reflect-sync/src$ !ls
ls -lht ~/.local/share/reflect-sync/sessions/7/*.db
-rw-r--r-- 1 wstein wstein 91M Nov 11 10:38 /home/wstein/.local/share/reflect-sync/sessions/7/alpha.db
-rw-r--r-- 1 wstein wstein 87M Nov 11 10:38 /home/wstein/.local/share/reflect-sync/sessions/7/beta.db
-rw-r--r-- 1 wstein wstein 83M Nov 11 10:37 /home/wstein/.local/share/reflect-sync/sessions/7/base.db
```



