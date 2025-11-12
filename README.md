# WARNING:

November 3, 2025 Status:  **THIS IS NOT READY FOR PRODUCTION USE. IT MAY DELETE YOUR FILES!   WE WOULD LIKE FEEDBACK.**

# ReflectSync

Fast, rsync-powered two-way file sync with SQLite metadata and optional SSH. Designed for very large trees, low RAM, and observability.

- **Rsync transport** \(latest rsync recommended\)
- **SQLite indexes** per side \(alpha / beta\) \+ base snapshot for true 3\-way merges
- **Incremental scans** with content hashes only when needed \(based on ctime\)
- **Realtime restricted cycles** for hot files \(debounced, safe on partial edits\)
- **SSH\-friendly**: stream remote deltas over stdin
- **Copy on Write (optional):** when both roots are local you can opt into `cp --reflink` copies for fast COW transfers (`reflect create … --enable-reflink`); the flag is off by default so nothing attempts reflinks automatically.
- **Very Lightweight:** The reflect-sync npm package is under 100KB compressed (about 300KB uncompressed), including all dependencies.

> Requires **Node.js 22+**.

## LICENSE

Open source under the MIT License.

## STATUS: **NOT** SAFE

That said, all the planned functionality is implemented and there is a test suite.  Also, this is a relatively lightweight program built on sqlite, rsync and nodejs, and those three components rock solid.   **Do NOT trust valuable data to reflect without extensive backups.**

Some differences compared to Mutagen, and todos:

- the command line options are different:  it's inspired by Mutagen, not a drop in replacement
- conflicts are resolved using **last write wins** \(clocks are sync'd\), with close ties resolved by one chosen side always wins. There is no manual resolution mode.
- we use Sha\-256 by default, but also support "sha1", "sha1", "sha512", "blake2b512", "blake2s256", "sha3\\-256", and "sha3\\-512".
- only supports macos and linux \(currently\)
- timestamps and permissions are fully preserved, whereas Mutagen mostly ignores them
- edit sessions after you create them
- query for total bytes under a directory, recent files, etc.

See [Design Details](./DESIGN.md).

### Consistency guarantees & tradeoffs

Reflect’s realtime path treats each rsync as a small transaction: before we copy a path from one side to the other we acquire a watcher lock on the destination, rsync the path, and only release the lock after re-sampling the destination signature (hash + metadata). This gives a practical guarantee: **if a subtree is only modified on one side, Reflect will never push older bytes back over it.** In other words, corruption can only occur if both sides are actively editing the same path (a true conflict). The default merge strategy is **`last-write-wins`**, which compares our logical observation time (`updated`) on each side. This means a freshly-deleted or recreated file always beats an old mtime-preserved copy even if the filesystem timestamp didn’t change. If you truly want to compare raw mtimes, you can opt into the legacy `lww-mtime` strategy.

Tradeoffs:

- We still rely on rsync’s “copy to temp, rename into place” model. If a file mutates mid-transfer, rsync exits with code 23, we keep the lock, and the scheduler retries once the file settles.
- We trust the database snapshots we just wrote: after each copy/delete we mirror alpha’s metadata into beta’s DB (and vice versa) so the next cycle sees the same state without having to rescan immediately.
- We do **not** try to solve the general case of simultaneous edits on both sides; those devolve to last-write-wins with `--prefer` as the tie breaker. The guarantee above simply ensures that editing only one side cannot regress the other.

### Delete semantics

Deletions are tracked just like edits: every scan/watch event updates the `nodes` table and the merge planner only reasons about that metadata. When a path disappears we set its `mtime` to **one millisecond after the last time we saw it alive** (we keep that timestamp in `last_seen`). Why?

- We use `mtime` for last\-write\-wins. If a file was modified and then deleted before the next cycle, that “delete” should win because it happened _after_ the last known write. Using `last_seen + 1 ms` encodes exactly that ordering even if the host clock jitters.
- If two copies both delete the same path they now share the same tombstone timestamp and we drop the path from the plan entirely, so nothing “resurrects” it later.
- Remote deltas, watcher\-driven updates, and rsync\-driven deletes all follow this rule, so every subsystem reports the same ordering information without doing a fresh stat on the remote filesystem.
- If during a single cycle one side edits the file and the other side deletes it, the with last write wins and our conservative choice of mtime, the edit will be preserved.

Practically this means Reflect will only resurrect a deleted file if the other side really does have a newer, surviving edit, which is the safest interpretation for automated sync.

---

## Install

Project (local):

```bash
pnpm add reflect-sync
# or: npm i reflect-sync
```

Global (handy for ops boxes):

```bash
pnpm add -g reflect-sync
# then: reflect help
```

Build from source (dev):

```bash
pnpm install
pnpm build
pnpm test
```

The package exposes the `reflect-sync` CLI (and aliases `reflect`) via its `bin` map.

---

## Quick start

### 1) One machine (both roots local)

```bash
reflect --help
reflect create /path/to/alpha /path/to/beta
reflect list
reflect status 1
reflect logs 1
reflect create /path/to/alpha /path/to/beta --ignore=node_modules --ignore="*.log"
```

- The scheduler runs a scan on each side, computes a 3-way plan, runs rsync, and repeats on an adaptive interval.
- File changes are pushed in near-realtime by running a restricted scan+merge cycle while the main loop periodically verifies and updates the base snapshot for last write wins 3-way merge semantics.
- Ignore rules can be provided at creation time with repeated `--ignore` flags (comma-separated or repeated). They’re stored in the session database and applied symmetrically to both sides (scans, merge planning, local/remote watchers, and rsync stages).

### 2) One side over SSH

Two ways to do it:

```bash
reflect create user@alpha.example.com:/tmp/alpha /tmp/beta
# or specify a non-default SSH port
reflect create /tmp/beta alpha.example.com:2222:/srv/alpha
```

or

```bash
reflect create /tmp/beta user@alpha.example.com:/tmp/alpha
# default port 22 can also be written with a double colon
reflect create /tmp/beta alpha.example.com::/srv/alpha
```

The scheduler will SSH to `alpha-host`, run a remote scan that streams NDJSON deltas, and ingest them locally.

> Remote paths must begin with `/` or `~/`. The optional `:<port>` segment lives between the host and the path (`host:2222:/path`). A bare single colon or a double colon (`host:/path`, `host::/path`) uses the default port 22.

> Advanced plumbing commands (`scan`, `watch`, `scheduler`, …) are hidden from the default help to keep the interface ergonomic. Run `reflect --help --advanced` if you need to see or invoke them directly.

### Copy-on-write reflink copies (optional)

Reflect never attempts reflink copies automatically. If **both roots are local and on the same filesystem**, pass `--enable-reflink` to `reflect create` (or `reflect edit … --enable-reflink`) to force `cp --reflink=always --parents …` for alpha↔beta copies. The scheduler CLI also accepts `--enable-reflink` when you launch it manually. Remote sessions must leave the flag unset; the CLI errors if either side is remote.

With the flag enabled Reflect fails fast if the filesystem refuses a reflink copy—there is no silent fallback to rsync—so only opt in when you know the workload supports CoW clones (e.g., btrfs, XFS with reflinks). `reflect list` and `reflect status` annotate sessions with `reflink` so you can confirm the setting later.

### Running as a daemon

The `reflect daemon` command keeps schedulers alive in the background:

```bash
reflect daemon start   # fire-and-forget supervisor (writes ~/.local/share/reflect-sync/daemon/reflect.pid)
reflect daemon status  # quick health check
reflect daemon stop    # terminate the supervisor
reflect daemon logs [--follow] [--json]  # tail the supervisor logs stored in sessions.db
```

Use `reflect daemon install` to install a user service (systemd on Linux, LaunchAgent on macOS) that launches the daemon on login. The service runs `reflect daemon run` in the foreground, so you can also wire it into your own service manager. All daemon output is persisted in `sessions.db`, so you can inspect historic activity with `reflect daemon logs` even if the process is no longer running.

### Common CLI commands

```bash
reflect create <alpha> <beta>         # start a new session
reflect list                          # list sessions
reflect status <id-or-name>                   # show heartbeat / merge status
reflect logs <id-or-name> [--follow]          # stream recent structured logs
reflect logs <id-or-name> --message progress  # tail only progress events (scan/hash/rsync/etc.)
reflect stop <id-or-name...>                  # stop one or more sessions
reflect start <id-or-name...>                 # start (and auto-start scheduler if needed)
reflect terminate <id-or-name...>             # stop and remove session state
reflect daemon logs [--follow]                # inspect supervisor logs persisted in sessions.db
reflect daemon start                          # ensure the background supervisor is running
reflect daemon stop                           # stop the supervisor (removes the PID file)
```

All commands honor `--session-db <path>` if you want to keep session metadata outside the default location.

### Session queries

The sync databases already track file metadata, so you can pull summaries without re-scanning the filesystem:

```bash
reflect query size 3                      # total logical bytes on alpha side
reflect query size --side=beta 3          # beta side
reflect query size --path=work 3          # only files under “work/”
reflect query recent --max=20 3           # 20 most recently touched files
reflect query recent --path=logs --json 3 # JSON output for scripting
```

Paths must live inside the session root (after ignore rules); for remote sessions the command resolves `~/` via SSH just like the scheduler.

### SSH port forwards

Reflect can keep long-lived SSH tunnels alive via the daemon supervisor:

```bash
reflect forward create localhost:8443 user@host:443    # local -> remote
reflect forward create user@host:2222:22 :2022         # remote -> local
reflect forward list                                   # ASCII table with live PIDs & ssh args
reflect forward terminate <id-or-name>
```

Each forward row stores its `ssh` invocation. The daemon \(`reflect daemon start`\) keeps the background `ssh` process alive and restarts it if necessary. `reflect forward list` surfaces the recorded PID and command, and marks forwards as `error` when the underlying process has disappeared.

### Monitoring changes in real time

The `reflect monitor` command streams change events (hot watcher hits and scan-driven deltas) for any session. It’s useful for dashboards and editors:

```bash
reflect monitor 7                     # human-readable stream for both sides
reflect monitor 7 --json --alpha-only # machine-friendly JSON for alpha only
reflect monitor project-a --since=60000  # replay the last minute of activity, then keep streaming
```

Each event includes the side, path, source (`hotwatch`, `scan`, `remote-watch`), timestamps, and hashes when available. Use `--beta-only`, `--poll-interval`, or `--hot-interval` to tune the feed.

The monitor command allows you to watch a potentially huge nested directory with hundreds of thousands of files on Linux, without using too many inotify handles.

---

## How Reflect works \(short\)

- **Scan** writes metadata \(`path, size, ctime, mtime, hash, deleted, last_seen, hashed_ctime`\) to SQLite. Hashing is streamed and parallelized; we only rehash when `ctime` changed since the last hash.  _Scanning never crosses filesytem boundaries._
- **Merge** builds temp tables with _relative_ paths, computes the 3\-way plan \(changed A, changed B, deleted A/B, resolve conflicts by `--prefer`\), then feeds rsync with NUL\-separated `--files-from` lists.
- **Scheduler**:
  - shallow root watchers \+ bounded deep “hot” watchers \(recently touched subtrees\),
  - **restricted cycles** run the same scan/merge pipeline on the hot set immediately,
  - periodically run a full scan \+ merge; interval adapts to prior cycle time and recent rsync errors.
  - **Progress logging:** hashing and rsync stages emit logs tagged with `message="progress"` plus scopes in the metadata. Use `reflect logs <id> --message progress -f` to observe in real time.

---

## Environment & scripts

- **Node.js**: `>= 22` (set in `engines`).
- **Build**: `pnpm build` (TypeScript → `dist/`), `rootDir=src`, `outDir=dist`.
- **Dev helpers**: `dev:scan`, `dev:ingest`, `dev:merge`, `dev:scheduler` run the TS sources via `tsx`.

---

## Typical file layout

You’ll end up with three DBs alongside your process (or wherever you point them):

- `alpha.db` — metadata for alpha root
- `beta.db` — metadata for beta root
- `base.db` — 3-way merge base (relative paths)

This separation makes it easy to relocate/rotate databases, inspect state, and compute user-facing reports (e.g. “what changed recently”, “top space users”).

---

## Troubleshooting

- **Inotify/FSEvents limits \(Linux/macOS\)**: scheduler uses shallow \+ bounded hot watchers. If you still hit limits, tune:
  - `MAX_HOT_WATCHERS` — cap number of deep watchers
  - `SHALLOW_DEPTH` — 0 or 1 recommended
  - `HOT_DEPTH` — typical 1–2

- **DB size**: large trees create large but inexpensive DBs. Use WAL mode \(default\) and SSDs for best throughput.

---

## Development

```bash
pnpm install
pnpm build
pnpm test
pnpm link -g .
reflect -h
```

TypeScript compiler outputs to `dist/.`

---

## Notes

- Executables are provided via `bin` and linked to the compiled files in `dist/`. If you’re hacking locally in this repo, either run `node dist/cli.js …` or `pnpm link --global .` to get `reflect` on your PATH.
- For SSH use, ensure the remote has Node 22\+ and `reflect` on PATH \(installed or included in your SEA image\). Then `reflect scan … --emit-delta | reflect ingest …` is all you need. Also, make sure you have ssh keys setup for passwordless login.

---

## Why ReflectSync?

**reflect-sync** is a two-way file sync tool written in Typescript with **deterministic Last-Write-Wins (LWW)** semantics, built on **rsync** for transfer and **SQLite** for state. It aims to be predictable, debuggable, and fast for the common case of **two roots** (e.g., laptop ↔ server, container bind-mount ↔ host, staging ↔ prod).

### Key properties

- **Deterministic LWW:** Changes are decided by content timestamps with a small configurable epsilon; ties break toward the **preferred side** you choose.
- **Hash-driven change detection:** Content hashes, not “mtime only,” determine whether a file actually changed—keeps plans stable and reduces churn.
- **First-class symlinks:** Links are scanned via `lstat`, targets are stored and hashed as **`link:<target>`**, and rsync preserves them.
- **Simple & inspectable:** Uses SQLite tables and NDJSON deltas—easy to debug, test, and reason about.
- **MIT-licensed:** Very permissive for both open-source and commercial use.

---

## When to use ReflectSync

When you want:

- **Two endpoints** with **predictable outcomes** (no “conflict copies”).
- Great performance on **large files** or **incremental edits** (thanks to rsync).
- **Transparent plans** and state you can audit (SQLite + file lists).
- **Symlink-accurate** behavior across platforms that support them.

Not a perfect fit if you need:

- A **multi\-node mesh** with discovery/relays \(see Syncthing/Resilio\).
- Built\-in **version history** or a cloud UI \(see Nextcloud/Dropbox\).
- **Interactive** conflict resolution UX \(see Unison\); with ReflectSync there is **never** conflict resolution.

---

## How it compares

| Tool            | License                             | Sync model                   | Conflict policy                   | Notes                                         |
| --------------- | ----------------------------------- | ---------------------------- | --------------------------------- | --------------------------------------------- |
| **ReflectSync** | **MIT**                             | Two-way between two roots    | **LWW** (+ preferred side on tie) | rsync transport; SQLite state; symlink-aware  |
| **Unison**      | GPL-3                               | Two-way                      | Interactive or policy-driven      | Mature, formal; heavier UX for headless flows |
| **Syncthing**   | MPL-2.0                             | Continuous P2P mesh          | **Conflict copies** on diverge    | Great for many devices; background indexer    |
| **Mutagen**     | Source-available (see project docs) | Dev-focused low-latency sync | Modes incl. “prefer side”         | Very fast for dev trees; custom protocol      |
| **lsyncd**      | GPL-2.0+                            | One-way (event → rsync)      | N/A                               | Simple near-real-time mirroring               |

> Philosophy difference: **ReflectSync** favors _determinism without duplicates or "conflict" files_ \(LWW \+ preference\). Tools like Syncthing/Dropbox prefer _never lose data_ \(create conflict files\), which is ideal for less controlled, multi\-party edits.

---

## Semantics (brief)

- **Change detection:** By **content hash, permissions and \(for root\) uid/gid**. Pure mtime or ctime changes do NOT count as edits. However, when there is an edit, the file is transfered with the mtime properly synced.
- **Last write wins \(LWW\) resolution:** The newer op timestamp wins, or preferred side in case of a tie.
- **Type changes:** File ↔ symlink ↔ dir follow the same LWW rule \(so type can change if the winner differs\).
- **Deletes:** Deletions are first\-class operations and replicate per LWW.
- **Symlinks:** Stored with target string and hashed as `<target>`; preserved by rsync.
- **Permission modes:** are always fully sync'd on posix systems.
- **UID/GID:** sync'd as numeric ids when the user is root; ignored otherwise.

---

## Performance notes

- **Large files / small edits:** Excellent even for high latency links \(rsync rolling checksums\).
- **Many small files:** Competitive when watch\-driven; initial cold scans take longer than always\-on indexers.
- **Observability:** Plans are explicit \(`toAlpha`, `toBeta`, delete lists\) and replayable. State is visible in sqlite tables. After each scan a hash of each directory tree is computed, so you can be certain the state has converged.

---

## Platform support

- **Linux / macOS:** Fully supported \(Node.js 22\+\). Uses `lutimes` where available for precise symlink mtimes.
- **Windows:** Works best via **WSL** or an rsync port. Symlink behavior depends on platform capabilities and permissions. Not yet tested for native Windows, but planned.

---

## Open Source

The MIT license is maximally permissive: embed, modify, and redistribute with minimal friction. This makes **ReflectSync** easy to adopt in both open\-source stacks and commercial tooling.

---

## ReflectSync vs. X — choose-by-scenario

| Scenario                                                                | Recommended                  | Why                                                                                  | Notes                                                                            |
| ----------------------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| **Two endpoints; predictable outcome; no conflict copies wanted**       | **ReflectSync**              | Deterministic **LWW** with explicit tie-preference; symlink-aware; transparent plans | Great for laptop↔server, container bind-mounts, staging↔prod                   |
| **One-way near-real-time mirroring** (e.g., deploy artifacts → webroot) | **lsyncd**                   | Event→batch→rsync is simple and robust                                               | If you still want ReflectSync, just run one side as authoritative (prefer-alpha) |
| **Dev loop; tons of small files; low latency**                          | **Mutagen**                  | Purpose-built for fast dev sync; very low overhead on edits                          | License differs; protocol/agent required                                         |
| **Many devices; peer-to-peer mesh; zero central server**                | **Syncthing**                | Discovery, relay, NAT traversal, continuous                                          | Creates conflict copies on diverge (safer for multi-writer)                      |
| **Non-technical users; desktop + mobile; web UI; version history**      | **Nextcloud** or **Dropbox** | Turnkey clients + history + sharing                                                  | Heavier footprint; server (Nextcloud) or cloud (Dropbox)                         |
| **CI/CD cache or artifacts between two machines**                       | **ReflectSync**              | Deterministic, debuggable, rsync-efficient on large binaries                         | Keep file lists tight; parallelize rsync if needed                               |
| **Large binary files with small edits over LAN**                        | **ReflectSync**              | rsync rolling checksum excels                                                        | Consider `--inplace` only if types won’t change and perms allow                  |
| **Interactive conflict resolution preferred**                           | **Unison**                   | Mature interactive/tunable policy engine                                             | More friction in headless automation                                             |
| **Multi-writer folder; avoid any silent overwrite**                     | **Syncthing**                | Uses conflict files rather than overwrite                                            | Safer for less-controlled edits; not deterministic                               |
| **Windows-first environment**                                           | **Syncthing** / **Dropbox**  | Native UX; no rsync/WSL needed                                                       | **ReflectSync** works best via **WSL** (document this path)                      |
| **Air-gapped / restricted SSH only**                                    | **ReflectSync**              | rsync over SSH; explicit file lists; easy to audit                                   | Works well in regulated environments                                             |
| **Exact promotion between environments (e.g., staging → prod)**         | **ReflectSync**              | Precise deletes; type changes honored; no conflict files                             | Keep backups if human edits happen in prod                                       |
| **One-way ingest to object storage (S3, etc.)**                         | **rclone** (adjacent tool)   | Direct backends; checksumming; retries                                               | Different problem space; can be combined with ReflectSync locally                |

**Legend:**

- **LWW** = Last-Write-Wins. In ReflectSync, ties break toward your **preferred side** (alpha/beta).
- “Conflict copies” = tools that create duplicate files when both sides changed (e.g., `filename (conflict copy).txt`).

**Rule of thumb**

- Want **determinism** between **two roots** → pick **ReflectSync**.
- Want a **mesh** or **never lose data** via conflict files → pick **Syncthing** (or cloud sync).
- Want **dev-loop speed** → pick **Mutagen**.
- Want **one-way mirroring** → pick **lsyncd**.
- Want **history + sharing** → pick **Nextcloud/Dropbox**.

