# Design Details

## Motivation & relationship to [Mutagen](https://mutagen.io/)

`ccsync` borrows the **algorithmic shape** of Mutagen’s two-way sync (periodic full scans + watching + a 3-way merge over a snapshot base) but makes a few big, pragmatic pivots:

- **Use SQLite for metadata**, not in-memory graphs. That makes RAM use predictable, lets us handle millions of files, and gives us query power (recent changes, “du”-like stats, search).
- **Use rsync for transport**, not a reimplementation. Rsync is battle-hardened, has great flags for exactly the workflows we need, and is easy to reason about/debug in production.
- **Keep the core in Node/TypeScript** for maintainability and approachability while still being fast (streaming IO, worker pool hashing, batched DB writes).

Why? In large deployments we’ve seen in-memory file indexes ramp to multiple GB per session, and custom transfer engines are harder to tune/inspect than rsync. With SQLite-on-disk metadata and rsync, we get performance and visibility, and the system becomes easy to extend. Also, rsync fully supports xattrs/ACL's, preserving numeric ID's, timestamps, etc., whereas Mutagen doesn't.

---

## Architecture at a glance

- **Databases**: `alpha.db`, `beta.db` (one per root), and `base.db` (3-way merge base).
- **scan**: walks a tree, stores/refreshes `files(path, size, ctime, mtime, hash, deleted, last_seen, hashed_ctime)` and **only rehashes when ctime changed**.
- **merge**: builds **relative-path** temp views of alpha/beta/base, computes the 3-way plan (changed A, changed B, deletions), resolves conflicts by `--prefer`, and then feeds rsync with NUL-separated `--files-from` lists. On success, it updates `base.db` to the merged state.
- **scheduler**: adaptive loop that (1) runs a full scan/merge at a dynamic interval, (2) runs **micro-sync** immediately for hot files, and (3) uses shallow + bounded deep watchers to avoid inotify/fsevents explosions.
- **SSH mode**: remote scan streams NDJSON deltas (`--emit-delta`) over stdout; a local **ingest** process mirrors them into a local SQLite DB for planning.

---

## Scanning pipeline (fast & incremental)

- **Walker**: `@nodelib/fs.walk` streaming API with `stats: true` and tuned concurrency. This avoids allocating a giant list; we stream entries as they arrive.
- **Hashing**: a worker pool (up to 8 threads by default). Each worker streams file content (large `highWaterMark`) and returns `{path, hash, ctime}` results in batches.
- **Rehash gating**: we only hash when `files.hashed_ctime !== current ctime`. This turns full rescans into mostly metadata refreshes for unchanged trees.
- **DB writes**: metadata upserts and hash updates are **batched** inside SQLite transactions (`better-sqlite3`) to minimize journaling overhead. `WAL` + `synchronous=NORMAL` hit a great perf/safety balance.
- **Deletions**: each scan sets `last_seen = scan_id`; anything not seen this pass is marked `deleted=1`.

This yields scan times that largely scale with **“bytes changed”**, not “files present”.

---

## SQLite schema & “recent touch”

Core table:

```sql
CREATE TABLE files (
  path TEXT PRIMARY KEY,
  size INTEGER,
  ctime INTEGER,
  mtime INTEGER,
  hash TEXT,
  deleted INTEGER DEFAULT 0,
  last_seen INTEGER,
  hashed_ctime INTEGER
);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
```

For realtime/watcher seeding:

```sql
CREATE TABLE IF NOT EXISTS recent_touch (
  path TEXT PRIMARY KEY,
  ts   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_recent_touch_ts ON recent_touch(ts);
```

Workers add to `recent_touch` when a fresh hash completes, and scans can seed “hot” watchers from the most-recently touched paths.

---

## Three-way merge planner (SQL)

Three\-way merge is accomplished by simply doing SQL queries.   We attach alpha/beta DBs and build **temporary** relative\-path tables:

- `alpha_rel(rpath, hash, deleted)`
- `beta_rel(rpath, hash, deleted)`
- `base_rel(rpath, hash, deleted)`

Changed sets (vs base):

```sql
-- Changed in A
CREATE TEMP TABLE tmp_changedA AS
  SELECT a.rpath, a.hash
  FROM alpha_rel a
  LEFT JOIN base_rel b ON b.rpath = a.rpath
  WHERE a.deleted = 0 AND (b.rpath IS NULL OR b.deleted = 1 OR a.hash <> b.hash);

-- Changed in B
CREATE TEMP TABLE tmp_changedB AS
  SELECT b.rpath, b.hash
  FROM beta_rel b
  LEFT JOIN base_rel bb ON bb.rpath = b.rpath
  WHERE b.deleted = 0 AND (bb.rpath IS NULL OR bb.deleted = 1 OR b.hash <> bb.hash);
```

Deletions (missing vs base):

```sql
CREATE TEMP TABLE tmp_deletedA AS
  SELECT b.rpath
  FROM base_rel b
  LEFT JOIN alpha_rel a ON a.rpath = b.rpath
  WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);

CREATE TEMP TABLE tmp_deletedB AS
  SELECT b.rpath
  FROM base_rel b
  LEFT JOIN beta_rel a ON a.rpath = b.rpath
  WHERE b.deleted = 0 AND (a.rpath IS NULL OR a.deleted = 1);
```

Then compute `toBeta`, `toAlpha`, `delInBeta`, `delInAlpha`, resolving same-rpath conflicts by `--prefer`.

---

## Rsync transport

- Copies: `rsync -a --inplace --relative --from0 --files-from=/tmp/list …`
- Deletes: `rsync -a --relative --from0 --ignore-missing-args --delete-missing-args …`
- NUL\-separated lists prevent path edge cases \(filenames with newlines in them\).
- We **allow exit codes 23/24** in micro\-sync \(files vanishing while edited\) to avoid breaking the loop; the next full cycle settles consistency.

On completion, we upsert `base.db` to the merged result with **relative** paths only. That keeps `base.db` portable and small.

---

## Realtime micro-sync (hot files)

When watchers report a handful of touched files, we:

1. decide direction per rpath (conflicts → `--prefer` side),
2. filter to **regular files** (`lstat.isFile()`),
3. write a tiny NUL-separated list and **rsync just those paths** immediately,
4. accept partial rsync codes (23/24) and debounce repeats.

No base updates here—**the next full cycle verifies and updates base**. This keeps micro-sync simple and robust in the face of edits-in-flight.

---

## Watching strategy (bounded & adaptive)

Global deep watchers explode on large trees (inotify limit). We use:

- **Shallow root watchers** (`depth = 0/1` by default, but configurable) to cheaply detect _where_ activity occurs.
- A bounded **HotWatchManager** that adds **depth-limited** deep watchers only on **hot** subtrees (with a TTL + LRU) and **escalates deeper** when it sees events at the frontier.
- After each scan, we **seed hot watchers from `recent_touch`**—keeps focus on directories users actually touch.

This yields “realtime-enough” behavior without OS watcher blowups.

---

## Scheduler (adaptive loop + backoff)

- Full cycle target interval is ~**2× the last cycle time**, clamped (e.g. 7.5s..60s).
- Any watcher activity schedules a sooner run.
- Rsync errors cause **backoff**:
  - 23/24 → small delay (edits in flight),
  - 28 (ENOSPC) → exponential backoff and loud logs (avoid egress loops).
- Logs are written to an `events` table in `base.db` for observability.

---

## SSH mode & star topologies

- **Remote scans** run over SSH and print NDJSON \(`--emit-delta`\), which we pipe into **ingest** locally. The coordinator always plans locally \(it has `alpha.db`, `beta.db`, `base.db`\).
- Both sides can be remote by running two scan pipes into the same coordinator; the **merge/rsync** still runs where you want the authoritative operation to happen.
- This trivially supports star topologies \(many clients ↔ one server\) without extra overhead. The current implementation supports **prefer the server** when there are conflicts.

---

## Performance characteristics

- **Scanning** is IO-bound and scales with “bytes changed” thanks to ctime gating. Worker hashing and 8–16 MB stream buffers keep CPU saturated on fast SSDs.
- **DB writes** are batched (2k rows by default) and committed inside transactions; WAL reduces fsync pressure.
- **Rsync** does the heavy transfer lifting; using `--from0 --relative` lets us ship precise changes and deletions without building giant command lines.

Typical large-tree outcomes we’ve seen:

- Millions of files, ~50 GB: scan in tens of seconds to ~1–2 min with low steady RAM; rsync in minutes; DB sizes in the 100–500 MB range \(cheap disk, great introspection\).
- Compared to “spawn `sha256sum` on every file”, the worker pool \+ batching approach is often **faster** while being memory\-predictable and not requiring external dependencies.

(Tune with `SCAN_CONCURRENCY`, `SCAN_DISPATCH_BATCH`, `SCAN_DB_BATCH` if needed.)

---

## Disk-usage & “recent files” reporting (bonus)

Because metadata lives in SQLite, **du-like** and “recent changes” features become **simple queries**:

```sql
-- Bytes under a subtree (fast if path is normalized):
SELECT SUM(size) AS bytes
FROM files
WHERE deleted=0
  AND path >= '/srv/alpha/project/'
  AND path <  '/srv/alpha/project0';

-- Recently changed:
SELECT path, mtime
FROM files
WHERE deleted=0
ORDER BY mtime DESC
LIMIT 200;

-- Search-as-you-type (prefix):
SELECT path FROM files
WHERE deleted=0
  AND path >= '/srv/alpha/proj/sub/'
  AND path <  '/srv/alpha/proj/sub0'
ORDER BY path
LIMIT 200;
```

You can materialize helpers (e.g., a `path_norm` column with `/` separators, or store precomputed parent directory columns) to avoid slow `LIKE '%…%'` scans.

---

## Tradeoffs & known limitations (current)

- **Dirs/links/attrs**: micro\-sync currently focuses on regular files. Full rsync handles dirs/permissions per `-a`, but we plan explicit handling for directories, symlinks, xattrs/ACLs, and **numeric IDs** where appropriate.
- **Base updates & verification**: we update `base.db` after rsync; the next full cycle **verifies** the result end\-to\-end. In high\-churn cases you may see rsync 23/24 warnings—by design.
- **ctime reliance**: ctime is an excellent gate for rehashing but can vary across FS types or be coarser than you like; you can switch to an `(mtime, size)` policy if desired \(configurable in future\).
- **Watcher locality**: only the coordinator watches deeply. Remote sides rely on their periodic scans to report deltas.

---

## Roadmap (high-impact next steps)

- **Full metadata parity**: xattrs/ACLs, uid/gid numeric IDs, symlink strategies, and precise permission diffs.
- **Robust micro\-sync for dirs** \(create/remove/move at small scale\) with conflict\-safe base updates.
- **Policy knobs**: mtime/size hashing gates; ignore globs; include\-only “whitelists” for star topologies.
- **Out\-of\-space guardrails**: coordinated backoff across scheduler \+ rsync \+ telemetry to prevent expensive retry loops.
- **Programmatic API**: expose planner and scan/ingest as functions for embedding in higher\-level orchestrators.
- **SEA/Docker/systemd** packs for easier deployment.

---

This design aims to keep the _algorithm_ you like from Mutagen, but shift its heavy lifting to **SQLite** and **rsync**, which are fast, observable, and easy to operate at scale—while keeping the codebase small and pleasant to evolve.

