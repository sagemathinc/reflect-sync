# Home stretch

  1. Remote binary version check – totally agree. We already rely on invoking reflect remotely for scans/watch/fscap probes, so it’s easy to run reflect --version (or reflect
     info) once per session startup and ensure it matches the local pkg.version. If it mismatches, fail fast with a clear error so users don’t end up with mismatched schema/
     features.
  2. Forced scan support – makes sense as a guardrail when metadata can’t be trusted (e.g., btrfs snapshot diff, external tools mutating files). Architecturally I’d add two
     pieces:
      - CLI knob (“full” flag or reflect sync --force-scan) that tells the scheduler to run runScan({ forced: true }), which walks every path regardless of ctime/hash heuristic.
      - Optional file-list mode where we feed a NUL-separated list into scan (or a companion command) so advanced users can leverage snapshot-diff or rsync “–files-from” output.
        That’d reuse the existing worker pool, just seed the stream with the supplied paths.
  3. Mixed privilege copies – good catch. Today we always pass rsync -a/-o/-g, which only works when both sides can set ownership. We need a per-session knob (or automatic
     detection) that switches to “preserve mode/mtime but skip owner/group” when one side runs as root and the other doesn’t. Implementation-wise:
      - Detect at session creation whether alpha/beta roots are owned by the current user; if they differ, mark the session as “ownership mismatch”.
      - Teach the rsync helper to drop --owner/--group (and avoid chown/cloned metadata) when that flag is set. For local copies we may need to call cp without
        --preserve=ownership, and for rsync we can still preserve permissions/mtimes but let the destination pick the owning user.




# (Done) Case Sensitivity versus Insensitivity

## Goal

Let mixed case-sensitive (Linux/btrfs) ↔ case-insensitive or Unicode-normalizing (macOS/NTFS) sessions continue syncing even when one side contains `foo.txt` + `FOO.txt` or `école` + `école`. Instead of inventing synthetic filenames, we normalize the names we see on lossy filesystems, skip extra variants, record that decision, and surface it to the user so they can resolve the conflict upstream.

## Implementation Plan

1. **\(done\) Detect filesystem behavior**
   - During session bootstrap \(same place we detect remote vs local roots\), probe each endpoint with a throwaway file to determine whether the filesystem is case\-insensitive and/or auto\-normalizes Unicode \(e.g., macOS NFD\). Cache this in the session state so scans/merges know which side needs folding/canonicalization.

2. **\(done\) Schema update**
   - Add a `case_conflict INTEGER NOT NULL DEFAULT 0` column to `nodes` in alpha, beta, and base DBs. Include it in the indexes we already rely on \(`nodes_deleted_path_idx`, etc.\).
   - No need to worry about migrations \-\- project isn't released yet.

3. **\(done\) Scan/ingest changes for insensitive or Unicode\-normalizing roots**
   - While scanning \(or ingesting remote watch deltas\) on a lossy root, maintain a map of `canonical(path) -> canonicalRow`, where `canonical()` lowercase\-folds and converts to NFC before comparison. For each directory:
     - If we encounter `p` and `canonical(p)` is unused, insert it into the map and proceed as today \(write row with `case_conflict = 0`\).
     - If `canonical(p)` already has a different canonical path, pick a winner \(e.g., the newest `mtime`, or deterministically the lexicographically smallest; for a directory, consider the mtimes of everything under that directory\). Mark every loser in that conflict set with `case_conflict = 1`. Their metadata stays accurate so the Linux side still knows about them, but future merges targeting the insensitive side will skip them; for directories we also mark the entire subtree so children behave like an ignored path.
   - When a conflict disappears \(e.g., user deletes/renames a file so the canonical key is unique again\), the next scan clears the flag by writing `case_conflict = 0`.
   - Emit a logger warning once per conflict set so users get notified immediately. Include the canonical path, the skipped variants, and which root is lossy \(case\-folding, Unicode normalization, or both\).

4. **Merge planner/executor awareness**
   - When we build the diff query for a destination that is case\-insensitive/normalizing, add `AND case_conflict = 0` to the relevant side. This prevents the executor from scheduling rsync copies or deletes for rows that would be lossy on that filesystem, and because directories propagate the flag the whole subtree is skipped \(exactly like an ignore rule\).
   - When copying from the insensitive side back to the sensitive side, we still consider every row \(including ones flagged as conflicts\), because the Linux tree should remain complete.
   - If a conflict row would otherwise plan an operation \(e.g., delete on Linux towards macOS\), record a trace entry noting that it was skipped due to `case_conflict`.

5. **CLI surface area**
   - Teach `reflect diff` \(and any other inspection commands\) to include the `case_conflict` flag in its output. A simple column like `CASE?` or an asterisk next to the path is enough so users can list unresolved conflicts.
   - Optionally add `reflect list` / `reflect status` summary counts \(“2 files skipped on beta due to case conflicts”\) to improve visibility.

6. **Testing**
   - Unit tests: simulate a case\-insensitive scan by forcing the folding logic in `scan.ts` and assert that `case_conflict` rows are written as expected.
   - Integration tests: run an end\-to\-end sync where alpha contains both `foo` and `FOO`; verify beta only receives one file, logs a warning, and `reflect diff` shows the conflict.

7. **Future improvements \(optional\)**
   - Provide a `reflect resolve-case foo.txt` helper that renames/archives the conflicting variants on the Linux side.
   - Allow advanced users to choose the conflict resolution strategy \(newest wins, alpha always wins, etc.\), but keep “newest wins \+ warning” as the default.

This approach confines the complexity to scan/ingest + planner filters; rsync execution stays untouched, and we never create synthetic filenames on disk.

# New Algorithm -- Rewrite Plan

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
7. (done) Rewrite "reflect sync" to instead use the output of the merge plan -- done = all files transfer successfully so database is same on both sides. 
8. (done) Remove old signature/recent\-send logic and simplify docs/tests around the new model.
9. (done) "reflect sync" doesn't work, due to no longer having digests. Fix it. Before reflect sync depended on the digests, which we are no longer computing.  Also add a nice green checkbox to the "reflect list" when the last known full scan showed that that the sync roots are the same.  Do not store and compute the digest as part of a normal sync cycle, since it's just as expensive to compute as doing the whole merge strategy, and it's only useful to compare the two sides, which we already do directly.
10. (done) Implement a tracing table for debugging.  Enable with an env variable being set, e.g., REFLECT_TRACE_ALL=1.   When set, we log a lot of information to a new db called trace.db, to help with debugging.  It will explain exactly what the diffs were during each 3-way merge, both with full scans and hotsync.  Basically when set the 3-way merge function should record:

   - all the paths that are different with the metadata (exactly the output of that huge sql query, plus a timestamp)
   - what the merge strategy decided to do: the operations, and for each operation, whether or not it succeeded.
With this in hand, we should be able to look at a given problematic file and see every time it out of sync, what the plan was for it, and what happened. Probably the trace.db table should thus have columns and path should be indexed:

  time | path | alpha state... | beta state... | planned operation | did op work

This should make it very easy to see "why is path this way now?"  What have we done to it.  It will also be very useful for unit testing, since we can just read this table and verify what we think should happen actually did happen.

With this structure, every sync cycle is deterministic: the planner surfaces all discrepancies, the executor performs concrete operations, and the databases converge immediately if those operations succeed.

## (done) Big Scans and file transfers versus Realtime Updates

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

### \(done\) My overlayfs apt\-get stress test fails

See src/scripts/overlay/  

The test fails every time I've tried it, with files getting corrupted, 
which means something likely got copied back in the wrong direction,
due to a mistake.  "reflect diff" also outputs all this, but I looked at one of these files and it is deleted on both sides, so I think
`reflect diff` is wrong.

Since both alpha and beta have this as deleted, it shouldn't be in the merge plan at all, right?

### \(done\) A thought related to "last write wins" and deletes.

Right now we set the mtime of a delete to be the time we observe it,
which is I think the most dangerous choice.  I better option
would be to set the delete to 1ms after the last time we observed
that it was NOT deleted.  That is the most conservative choice,
which for deletes seems reasonable.   We know the last time we
observed it not deleted, since we update a field about times
each time we do a scan.  Anyway, with this choice, if between
full cycles a file is both modified and deleted, then it'll get
preserved, because the mod happened later.

## (not done) Memory usage

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

## \(done\) Use a socket and a single ssh session

There's a way to run a single persistent ssh session that all these
remote commands (e.g., scans, rsync copies, etc.) all reuse, which addresses that connecting over ssh may be expensive.  We should
just use this if possible as it could be a massive win (often ssh
takes 1s or more and can break in general).

## \(done\) robustness and self healing

If there is an error doing a copy operation, do something so that 
file gets scanned again during the next cycle (or do a targeted
scan).  Why?  Because if somehow the file was deleted but this isn't
known in the database (e.g., something got dropped with ingest-delta
during a session restart), we get stuck always thinking the file is
not deleted even though it is.  I think this is an issue only for
a remote file, since the remote database is definitely correct; it's
only the local one that is wrong.

Another option could be to request the latest state from the remote
database for a particular list of paths (all the ones with copy failures), 
then update local accordingly.

I did see this edge case when stress testing.

## \(done\) Remote agent feasibility

\(not worth it!\)

Long-term we may want a lightweight remote watcher/scan agent that replaces the SEA bundle on hosts where installing Node is painful. The required features boil down to SQLite access, a directory walker, a file watcher, and SHA-256 hashing; both Go and Rust ecosystems cover these pieces with portable libraries.

- **Timing**: defer until the wire protocol and merge semantics are locked down; re-implementing while reflect-sync is still shifting would create thrash.
- **Go vs Rust**: Go is the likely win because static binaries, cross-compilation, and standard tooling make it trivial to ship one binary per target. Rust would yield tighter control over memory footprint and potentially smaller binaries, but static builds with SQLite and watcher crates require more CI plumbing.
- **Portability targets**: Linux + macOS on x86/ARM. Go handles this out of the box; Rust can as well but expects more manual setup for glibc vs musl builds.
- **Protocol fidelity**: whichever language we choose must exactly mirror today’s scan/watch behavior—ignore rules, hashing, NDJSON framing, lock/unlock semantics—so this effort should only start once those behaviors are fully specified and proven stable.
- **Performance**: both languages can saturate disks for full scans and deliver low-latency watcher events via native backends (inotify/kqueue/FSEvents). The challenge is less about raw speed and more about faithfully reproducing our existing throttling and batching logic.

