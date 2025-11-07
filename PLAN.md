## Reflect Sync Reliability Plan

Goal: eliminate echo-induced corruption when beta pushes heavy writes to a remote
alpha. We want guarantees that rsync never causes alpha to re-send the same
paths back or ingest partial files. No TTL hacks—everything must be ordered and
deterministic.  We also must ensure that reflect sync never places a file
that is corrupted due to it changing during the transfer.

### Pillar 1 — Remote watcher lock/ack protocol

1. **Lock request.** Before rsyncing a chunk, scheduler sends
   `{op:"lock", requestId, paths:[…]}` over the watch control channel.
2. Remote `watch` records each path in a forced-ignore map and replies
   `{op:"lockAck", requestId}` once it is ready.
3. Scheduler waits for every ack before starting rsync. This guarantees the
   watcher will ignore those paths until we explicitly release them.
4. **Release.** After rsync finishes, scheduler probes (stat) the destination to
   learn the new ctime/mtime, then sends `{op:"release", paths:[…],
   watermark:{path:ctime,…}}`. The watcher discards any future events whose
   ctime ≤ watermark so late-arriving write notifications don’t echo.
5. **Unlock / failure cleanup.** If rsync fails or we abort, send
   `{op:"unlock", paths:[…]}` so the watcher drops forced ignores. Nothing
   remains suppressed indefinitely.

### Pillar 2 — Stable inputs before copy

- Extend the existing hot watcher debounce so a file must be quiescent \(no size
  or mtime change\) for ~250 ms immediately before we enqueue it for rsync.
- When building a micro\-sync batch, stat each candidate; if it changed in that
  window, skip it for this cycle and let the watcher requeue it later. This keeps
  signatures meaningful and avoids copying mid\-write data, which would result in creating corrupted files.

### Pillar 3 — Detect rsync partial transfers

- Keep using temp dirs \(no `--inplace`\) so partial copies never clobber live
  files.
- Treat rsync exit code 23 \(and “file has vanished” lines\) as “retry needed”:
  requeue those paths and send `unlock` so locks drop even on failure.
- Do not count a batch as successful unless all paths finished without partial
  errors. This ensures alpha never installs a truncated pack file.
- Double check that in case of exit code 23, rsync does NOT move the file into place anyways.

### Why signatures stay

Locks/ctime gating only protects the realtime watcher. Full scans still rely on
hash signatures to know whether work is required, so we keep the signature
pipeline intact: lock → (optionally) refresh remote signatures →
rsync → release with ctime → scans verify hashes.

### Implementation order

1. **Control channel enhancements:** add lock/release/unlock RPCs + ACK handling
   in `watch.ts` and scheduler’s remote stream.
2. **Watcher forced-ignore map:** ctime-based suppression keyed by path so any
   event with ctime ≤ watermark is dropped immediately.
3. **Stability window:** ensure micro-sync only copies files that have been idle
   for a short duration; reschedule hot files instead of racing them.
4. **Rsync partial detection:** handle exit code 23 / vanished warnings by
   retrying and cleaning up locks.

### Validation

* Unit + integration tests (`pnpm test`, ssh remote suite).
* Stress scenarios: dpkg install, concurrent `git clone`, overlayfs upperdir
  churn. Only ship once those converge with zero corruption.

