# ccsync

Fast, rsync-powered two-way file sync with SQLite metadata and optional SSH. Designed for very large trees, low RAM, and observability.

- **Rsync transport** (latest rsync recommended)
- **SQLite indexes** per side (alpha / beta) + base snapshot for true 3-way merges
- **Incremental scans** with content hashes only when needed (based on ctime)
- **Realtime micro-sync** for hot files (debounced, safe on partial edits)
- **SSH-friendly**: stream remote deltas over stdin; no bespoke daemon required
- **Clean CLIs**: `ccsync scan /root`, `ccsync merge …`, `ccsync scheduler …` (plus aliases)

> Requires **Node.js 24+**.

## LICENSE

Open source under the MIT License.

## Status: NOT READY FOR PRODUCTION

**ccsync is NOT READY FOR PRODUCTION USE YET!** There is still a significant todo list of subtle edge cases to handle, features to implement, tests to put in place, etc. Do not use this. Our plan is to finish this quickly, then put this into major production on https://cocalc.com, and then I'll update this README when it is much safer to trust ccsync with your files.

Some limitations and todos:

- conflicts are always resolved by "alpha wins" (like two-way-resolved in mutagen),
- we will likely change the hash from sha256 to something faster
- directory deletes are not handles (only files)
- when syncing over ssh, only the local side has watchers to support realtime file copies.
- only supports macos and linux.

For far more details, see [Design Details](./DESIGN.md).

---

## Install

Project (local):

```bash
pnpm add ccsync
# or: npm i ccsync
```

Global (handy for ops boxes):

```bash
pnpm add -g ccsync
# then: ccsync help
```

Build from source (dev):

```bash
pnpm install
pnpm build
```

The package exposes the `ccsync` CLI (and aliases `ccsync-scan`, `ccsync-ingest`, `ccsync-merge`, `ccsync-scheduler`) via its `bin` map.

---

## Quick start

### 1) One machine (both roots local)

```bash
# First full scan & sync loop
ccsync scheduler \
  --alpha-root /srv/alpha \
  --beta-root  /srv/beta  \
  --alpha-db   alpha.db   \
  --beta-db    beta.db    \
  --base-db    base.db    \
  --prefer     alpha      \
  --verbose    true
```

- The scheduler runs a scan on each side, computes a 3-way plan, runs rsync, and repeats on an adaptive interval.
- File changes are pushed quickly via a micro-sync path while the main loop verifies and updates the base snapshot.

### 2) One side over SSH

Two ways to do it:

**a) Let the scheduler do the remote scan over SSH**

```bash
ccsync scheduler \
  --alpha-root /srv/alpha --alpha-host user@alpha.example.com \
  --beta-root  /srv/beta \
  --alpha-db   alpha.db   --beta-db beta.db --base-db base.db \
  --prefer alpha --verbose true
```

The scheduler will SSH to `alpha-host`, run a remote scan that streams NDJSON deltas, and ingest them locally.

**b) Manual pipe (useful to sanity-check)**

```bash
ssh user@alpha 'env DB_PATH=~/.cache/ccsync/alpha.db ccsync scan /srv/alpha --emit-delta' \
  | ccsync ingest --db alpha.db --root /srv/alpha
```

---

## Commands

### `ccsync scan <root> [--emit-delta]`

Walks `<root>`, updates (or creates) the local SQLite DB at `DB_PATH` (env), hashing only when ctime changed.
With `--emit-delta`, prints NDJSON events to stdout so a remote host can mirror into its DB.

- Env: `DB_PATH=/path/to/alpha.db` (or `beta.db`), default `./alpha.db`/`./beta.db`.

### `ccsync ingest --db <dbfile> --root <root>`

Reads NDJSON deltas from stdin and mirrors them into the given `files` table. Intended to pair with a remote `scan --emit-delta`.

### `ccsync merge ...`

3-way plan + rsync, then updates `base.db` to the merged state (relative paths). Typical flags:

```
--alpha-root /srv/alpha --beta-root /srv/beta
--alpha-db alpha.db --beta-db beta.db --base-db base.db
--prefer alpha|beta
[--dry-run true] [--verbose true]
```

### `ccsync scheduler ...`

Adaptive watcher/scan/merge loop that ties everything together, including optional SSH for one side:

```
--alpha-root /srv/alpha [--alpha-host user@host]
--beta-root  /srv/beta  [--beta-host  user@host]
--alpha-db alpha.db --beta-db beta.db --base-db base.db
--prefer alpha|beta
[--dry-run true] [--verbose true]
```

> The CLI shims for these commands are published in the `bin` field, so you can run them directly after install.

---

## How it works (short)

- **Scan** writes metadata (`path, size, ctime, mtime, hash, deleted, last_seen, hashed_ctime`) to SQLite. Hashing is streamed and parallelized; we only rehash when `ctime` changed since the last hash.
- **Merge** builds temp tables with _relative_ paths, computes the 3-way plan (changed A, changed B, deleted A/B, resolve conflicts by `--prefer`), then feeds rsync with NUL-separated `--files-from` lists.
- **Scheduler**:
  - shallow root watchers + bounded deep “hot” watchers (recently touched subtrees),
  - **micro-sync** a small list of hot files immediately,
  - periodically run a full scan + merge; interval adapts to prior cycle time and recent rsync errors.

---

## Environment & scripts

- **Node.js**: `>= 24` (set in `engines`).
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

## Examples

**Run a full cycle once (no scheduler):**

```bash
ccsync scan /srv/alpha   --db-path alpha.db
ccsync scan /srv/beta    --db-path beta.db
ccsync merge \
  --alpha-root /srv/alpha --beta-root /srv/beta \
  --alpha-db alpha.db --beta-db beta.db --base-db base.db \
  --prefer alpha --verbose true
```

**Realtime development project (local):**

```bash
ccsync scheduler \
  --alpha-root ~/src/project \
  --beta-root  /mnt/build-cache/project \
  --alpha-db alpha.db --beta-db beta.db --base-db base.db \
  --prefer alpha --verbose true
```

---

## Troubleshooting

- **`rsync` exit 23/24** (vanished files): normal if files are being edited; scheduler backs off briefly and the next cycle will settle.

- **Inotify/FSEvents limits (Linux/macOS)**: scheduler uses shallow + bounded hot watchers. If you still hit limits, tune:
  - `MAX_HOT_WATCHERS` — cap number of deep watchers
  - `SHALLOW_DEPTH` — 0 or 1 recommended
  - `HOT_DEPTH` — typical 1–2

- **DB size**: large trees create large but inexpensive DBs. Use WAL mode (default) and SSDs for best throughput.

---

## Development

```bash
pnpm i
pnpm build
node dist/cli.js help      # or: pnpm exec ccsync help (after link/install)
```

TypeScript compiler outputs to `dist/` mirroring `src/` (see `tsconfig.json`).

---

## Notes

- Executables are provided via `bin` and linked to the compiled files in `dist/`. If you’re hacking locally in this repo, either run `node dist/cli.js …` or `pnpm link --global` to get `ccsync` on your PATH.
- For SSH use, ensure the remote has Node 24\+ and `ccsync` on PATH \(installed or included in your SEA image\). Then `ccsync scan … --emit-delta | ccsync ingest …` is all you need. Also, make sure you have ssh keys setup for passwordless login.

