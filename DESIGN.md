# ReflectSync Design Details

## Motivation & relationship to [Mutagen](https://mutagen.io/)

**ReflectSync** borrows the **algorithmic shape** of Mutagen’s two-way sync (periodic full scans + watching + a 3-way merge over a snapshot base) but makes a few big, pragmatic pivots:

- **Use SQLite for metadata**, not in-memory graphs. That makes RAM use predictable, lets us handle millions of files, and gives us query power (recent changes, “du”-like stats, search).
- **Use rsync for transport**, not a reimplementation. Rsync is battle-hardened, has great flags for exactly the workflows we need, and is easy to reason about/debug in production.
- **Keep the core in Node/TypeScript** for maintainability and approachability while still being fast (streaming IO, worker pool hashing, batched DB writes).

Why? In large deployments we’ve seen in-memory file indexes ramp to multiple GB per session, and custom transfer engines are harder to tune/inspect than rsync. With SQLite-on-disk metadata and rsync, we get performance and visibility, and the system becomes easy to extend. Also, rsync fully supports xattrs/ACL's, preserving numeric ID's, timestamps, etc., whereas Mutagen doesn't.

---

## Architecture at a glance

- **Databases**: `alpha.db`, `beta.db` \(one per root\), and `base.db` \(3\-way merge base\).
- **scan**: walks a tree, stores/refreshes `files(relative path, size, ctime, mtime, hash, deleted, last_seen, hashed_ctime, ...)` and **only rehashes when ctime changed**.
- **merge**: builds **relative\-path** temp views of alpha/beta/base, computes the 3\-way plan \(changed A, changed B, deletions\), resolves conflicts by `--prefer`, and then feeds rsync with NUL\-separated `--files-from` lists. On success, it updates `base.db` to the merged state.
- **scheduler**: adaptive loop that \(1\) runs a full scan/merge at a dynamic interval, \(2\) runs **restricted scan/merge cycles** immediately for hot files, and \(3\) uses shallow \+ bounded deep watchers to avoid inotify/fsevents explosions.
- **SSH mode**: remote scan streams NDJSON deltas \(`--emit-delta`\) over stdout; a local **ingest** process mirrors them into a local SQLite DB for planning.

---

## Scanning pipeline (fast & incremental)

- **Walker**: `@nodelib/fs.walk` streaming API with `stats: true` and tuned concurrency. This avoids allocating a giant list; we stream entries as they arrive.
- **Hashing**: a worker pool (up to 8 threads by default). Each worker streams file content (large `highWaterMark`) and returns `{path, hash, ctime}` results in batches.
- **Rehash gating**: we only hash when `files.hashed_ctime !== current ctime`. This turns full rescans into mostly metadata refreshes for unchanged trees.
- **DB writes**: metadata upserts and hash updates are **batched** inside SQLite transactions [node:sqlite](https://nodejs.org/docs/latest-v24.x/api/sqlite.html) to minimize journaling overhead. `WAL` + `synchronous=NORMAL` hit a great perf/safety balance.
- **Deletions**: each scan sets `last_seen = scan_id`; anything not seen this pass is marked `deleted=1`.

This yields scan times that largely scale with **“bytes changed”**, not “files present”.

---

## Three-way merge planner (SQL)

Three\-way merge is accomplished by simply doing SQL queries. This is nice because it's just declarative SQL, hence easy to read and understand. The queries deal with merge conflicts using **last write wins**, with tie breaking via a preference.

---

## Rsync transport

- Copies: `rsync -a --relative --from0 --files-from=/tmp/list …`
- Deletes: `rsync -a --relative --from0 --ignore-missing-args --delete-missing-args …`
- NUL\-separated lists prevent path edge cases \(filenames with newlines in them\).
- We **allow exit codes 23/24** in restricted cycles \(files vanishing while edited\) to avoid breaking the loop; the next full cycle settles consistency.

On completion, we upsert `base.db` to the merged result with **relative** paths only. That keeps `base.db` portable and small.

---

## Realtime restricted cycles (hot files)

When watchers report a handful of touched files, we:

1. debounce/dedupe the rpaths reported by root + hot watchers,
2. wait for them to become stable (mtime settled, file closed),
3. run `scan` with `--restricted-path/dir` so only those entries refresh,
4. run `merge` with the same restrictions, which feeds rsync the tiny subset.

It’s the exact same planner pipeline, just scoped. Failures simply drop the paths back into the hot queue; the next pass (restricted or full) reconciles them.

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

## SSH mode

- **Remote scans** run over SSH and print NDJSON \(`--emit-delta`\), which we pipe into **ingest** locally. The coordinator always plans locally \(it has `alpha.db`, `beta.db`, `base.db`\).

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

## Tradeoffs & known limitations (current)

- **Dirs/links**: restricted cycles currently focus on regular files. Full rsync handles dirs/permissions per `-a`, but may add explicit handling for directories, symlinks, xattrs/ACLs, and **numeric IDs** where appropriate. Directories and symlinks are sync'd, just not immediately.
- **Base updates & verification**: we update `base.db` after rsync; the next full cycle **verifies** the result end\-to\-end. In high\-churn cases you may see rsync 23/24 warnings—by design.
- **ctime reliance**: ctime is an excellent gate for rehashing but can vary across FS types or be coarser than you like; you can switch to an `(mtime, size)` policy if desired \(configurable in future\).

---

## Roadmap

- **Full metadata parity**: xattrs/ACLs, uid/gid numeric IDs, symlink strategies, and precise permission diffs.
- **SEA** \(single executable binary\) packs for easier deployment.
- Non\-Posix: Native Windows Support

---

This design aims to keep the _algorithm_ you like from Mutagen, but shift its heavy lifting to **SQLite** and **rsync**, which are fast, observable, and easy to operate at scale—while keeping the codebase small and pleasant to evolve.

---

## Change Interval Semantics

Every row in `nodes` stores `change_start` and `change_end`. Together they mean:

> “The most recent transition for this path occurred **after** `change_start` and **no later than** `change_end`.”

These intervals give last-writer-wins a concrete model to reason about ordering, even when filesystem clocks lie or watchers miss events. We treat the bounds as half-open `(start, end]`; in storage we allow `change_start = change_end` to represent the degenerate case where the best information we have is a single logical instant.

Separately, we track a **confirmation tick** (call it `confirmed_at`) that records the most recent time we verified the *current* state had not changed. This does **not** alter the existing interval—it simply becomes the lower bound we will use the next time a change is detected. Double stats, watcher stability checks, or any other “still matches” validations update `confirmed_at`.

### Sources of intervals

- **Full scans \(discoveries vs confirmations\)**
  - If we perform back\-to\-back observations and the file is unchanged \(double stat after hashing, finishing a restricted scan that sees identical metadata, etc.\), we set `confirmed_at` to the later observation. The stored interval `(change_start, change_end]` for the most recent change stays the same, but now we have a tighter lower bound ready for the next change.
  - When a scan detects an actual change \(new file, modified contents, or resurrected entry\) and we have no such `confirmed_at` confirmation, the best lower bound we have is the **start** of the previous full scan. As soon as that scan began we stopped observing the old state, so the change could have occurred moments later. Therefore a change noticed during scan N is known to have happened sometime after scan N‑1 started and no later than the tick when scan N recorded it \(unless `confirmed_at` gives us a later lower bound\).

- **Hash completion**
  - Metadata upserts \(size/mtime\) tentatively extend the interval but leave `change_end = null` while hashing is in flight. Once the worker reports a stable hash \(and we re\-stat to ensure metadata still matches\), we clamp `change_end` to the hash tick and set `change_start` to the best lower bound we have _before_ the change happened—namely the previous `confirmed_at` \(falling back to the start of the prior full scan if no confirmation exists\). We also set `confirmed_at = hash_tick` because we now know the current contents were valid at that instant. This keeps the interval aligned with when the change could have occurred, not merely when we noticed it.

- **Hot watcher updates / restricted scans**
  - Watchers only shrink certainty—they can never shift events earlier than our last confirmation. The lower bound for a watcher\-driven refresh is therefore `confirmed_at` \(falling back to the previous scan start if no confirmation exists\), while the upper bound is the tick when the restricted scan finished. Restricted scans behave the same way: their short window gives us a more recent upper bound than waiting for the next full pass.

- **Deletions**
  - When a scan fails to find a path that existed previously, the change interval is `(last_confirmed_presence, scan_tick]`, i.e., the deletion occurred after the last time we definitely saw it and no later than the tick when we concluded it was gone. Any subsequent resurrection reuses the same logic as a new discovery.

- **Remote ingest \(SSH delta stream\)**
  - Each delta includes the remote clock tick when it was observed. We convert it into our logical clock and apply the same rules: lower bound is whichever confirmation we have locally, upper bound is the adjusted remote tick \(kept monotonic per path to handle out\-of\-order lines\). If the remote side confirms stability \(e.g., double stat before hashing\), we can adopt that tighter lower bound locally too. When the remote scanner provides extra context—such as how long after the scan started the hash was confirmed—we use that delta to reconstruct a tighter `confirmed_at` on the receiving side.

- **Merge applications**
  - After rsync successfully copies or deletes a path, the scheduler records the resulting state in the destination DB and `base.db`. The interval becomes `(previous base change_end, merge_tick]`, because rsync is the definitive last writer at that moment, and we set `confirmed_at = merge_tick` on both destination and base rows. A follow\-up verification \(double stat or restricted rescan\) can further tighten the lower bound before the next change.

### How the planner uses intervals

1. If both sides’ intervals are disjoint, the planner trusts the later upper bound.
2. If they overlap, we fall back to additional signals in order: filesystem mtimes, “which hash diverged from base,” then ctimes, and finally the configured `prefer` side.
3. Intervals also guard against partial knowledge: if either side has `change_end = null` (hash still pending), the planner issues a `noop` and waits for the interval to close before making a copy decision.

Documenting the semantics up front lets us audit each pipeline (scan, ingest, merge) to ensure they respect the same invariant: **every observation narrows—but never contradicts—the window in which a file changed**. Once every writer follows the same rule, last‑write‑wins has a solid footing regardless of filesystem quirks or watcher drops.

---

## Practical Assumptions & Verification

Reflect’s incremental scans assume that any **observable** change to a file will manifest through metadata differences we can see via `stat`: `ctime`, `mtime`, size, mode, or ownership. In most workflows and filesystems that is true, but it is *not* an absolute guarantee:

- Filesystems expose metadata at finite resolution (1 ns–1 s).  Two quick edits inside the same tick may report identical `ctime`/`mtime`/`size`.
- Privileged software can explicitly set timestamps back to old values (`utimensat`, `touch -r`, overlay copies that preserve metadata).
- Some network/overlay layers forward metadata from a lower layer, so byte content can change without a corresponding timestamp bump.

Our scan pipeline therefore treats “two identical stat snapshots” as evidence that nothing changed between them, but it remains a **heuristic**. If applications rapidly rewrite a file and intentionally preserve timestamps, incremental scans can miss those edits until some other observable attribute differs.

To counter that, we expose an optional **full-verify scan** mode (planned for the CLI) that rehashes every file regardless of metadata. Users can run it on demand or schedule it (e.g., nightly) to ensure there are no silent divergences. Full scans are slower, but they provide cryptographic certainty and complement the fast incremental watches.

Longer term, some filesystems (e.g., btrfs snapshots, ZFS events) offer native change tracking between checkpoints. Reflect could eventually integrate with those hooks to obtain ground-truth deltas directly from the filesystem, further reducing reliance on POSIX metadata heuristics.
