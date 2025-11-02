// session-db.ts
import { Database } from "./db.js";
import fs from "node:fs";
import { join } from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { CLI_NAME } from "./constants.js";
import { defaultHashAlg } from "./hash.js";
import { serializeIgnoreRules } from "./ignore.js";

// Paths & Home

export function getReflectSyncHome(): string {
  const explicit = process.env.REFLECT_HOME?.trim();
  if (explicit) {
    return ensureDir(expandHome(explicit));
  }

  // XDG first
  const xdg = process.env.XDG_DATA_HOME;
  if (xdg && xdg.trim()) {
    return ensureDir(join(expandHome(xdg), CLI_NAME));
  }

  // Platform defaults
  const home = os.homedir();
  if (process.platform === "darwin") {
    return ensureDir(join(home, "Library", "Application Support", CLI_NAME));
  }
  if (process.platform === "win32") {
    const appData = process.env.APPDATA || join(home, "AppData", "Roaming");
    return ensureDir(join(appData, CLI_NAME));
  }
  // Linux/other
  return ensureDir(join(home, ".local", "share", CLI_NAME));
}

// A persistent local identifier for this CLI_NAME "origin"/installation.
export function getOrCreateEngineId(home = getReflectSyncHome()): string {
  const p = join(home, "engine.id");
  try {
    const s = fs.readFileSync(p, "utf8").trim();
    if (s) return s;
  } catch {}
  // 128-bit random, base64url without '=' padding
  const raw = crypto
    .randomBytes(16)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  fs.mkdirSync(home, { recursive: true });
  fs.writeFileSync(p, raw + "\n");
  return raw;
}

export function getSessionDbPath(home = getReflectSyncHome()): string {
  return join(home, "sessions.db");
}

export function sessionDir(id: number, home = getReflectSyncHome()): string {
  return join(home, "sessions", String(id));
}

export function deriveSessionPaths(id: number, home = getReflectSyncHome()) {
  const dir = sessionDir(id, home);
  return {
    dir,
    base_db: join(dir, "base.db"),
    alpha_db: join(dir, "alpha.db"),
    beta_db: join(dir, "beta.db"),
    events_db: join(dir, "events.db"),
  };
}

function ensureDir(p: string): string {
  fs.mkdirSync(p, { recursive: true });
  return p;
}

function expandHome(p: string): string {
  if (!p) {
    return p;
  }
  if (p.startsWith("~")) {
    return join(os.homedir(), p.slice(1));
  }
  return p;
}

//   Types
export type Side = "alpha" | "beta";
export type DesiredState = "running" | "paused";
export type ActualState = "running" | "starting" | "paused" | "error";
export type ForwardDesiredState = "running" | "stopped";
export type ForwardActualState = "running" | "error" | "stopped";

export interface SessionCreateInput {
  name?: string;
  alpha_root: string;
  beta_root: string;
  prefer: Side;
  alpha_host?: string | null;
  alpha_port?: number | null;
  beta_host?: string | null;
  beta_port?: number | null;
  alpha_remote_db?: string | null;
  beta_remote_db?: string | null;
  remote_scan_cmd?: string | null;
  remote_watch_cmd?: string | null;
  hash_alg: string;
  compress?: string | null;
  ignore?: string[];
}

export interface SessionPatch {
  name?: string | null;
  alpha_root?: string;
  beta_root?: string;
  prefer?: Side;
  alpha_host?: string | null;
  alpha_port?: number | null;
  beta_host?: string | null;
  beta_port?: number | null;
  alpha_remote_db?: string | null;
  beta_remote_db?: string | null;
  remote_scan_cmd?: string | null;
  remote_watch_cmd?: string | null;
  base_db?: string | null;
  alpha_db?: string | null;
  beta_db?: string | null;
  events_db?: string | null;
  desired_state?: DesiredState;
  actual_state?: ActualState;
  scheduler_pid?: number | null;
  last_heartbeat?: number | null;
  last_digest?: number | null;
  alpha_digest?: string | null;
  beta_digest?: string | null;
  compress?: string | null;
  hash_alg?: string | null;
  ignore_rules?: string | null;
}

export interface SessionRow {
  id: number;
  created_at: number;
  updated_at: number;
  name: string | null;
  alpha_root: string;
  beta_root: string;
  prefer: Side;
  alpha_host: string | null;
  alpha_port: number | null;
  beta_host: string | null;
  beta_port: number | null;
  alpha_remote_db: string | null;
  beta_remote_db: string | null;
  remote_scan_cmd: string | null;
  remote_watch_cmd: string | null;
  base_db: string | null;
  alpha_db: string | null;
  beta_db: string | null;
  events_db: string | null;
  hash_alg?: string | null;
  desired_state: DesiredState;
  actual_state: ActualState;
  last_heartbeat: number | null;
  scheduler_pid: number | null;
  last_digest?: number | null;
  alpha_digest?: string | null;
  beta_digest?: string | null;
  compress?: string | null;
  ignore_rules?: string | null;
}

export interface ForwardCreateInput {
  name?: string | null;
  direction: "local_to_remote" | "remote_to_local";
  ssh_host: string;
  ssh_port?: number | null;
  ssh_compress?: boolean;
  ssh_args?: string | null;
  local_host: string;
  local_port: number;
  remote_host: string;
  remote_port: number;
  desired_state?: ForwardDesiredState;
  actual_state?: ForwardActualState;
}

export interface ForwardPatch {
  name?: string | null;
  ssh_port?: number | null;
  ssh_compress?: boolean;
  ssh_args?: string | null;
  local_host?: string;
  local_port?: number;
  remote_host?: string;
  remote_port?: number;
  desired_state?: ForwardDesiredState;
  actual_state?: ForwardActualState;
  monitor_pid?: number | null;
  last_error?: string | null;
}

export interface ForwardRow {
  id: number;
  created_at: number;
  updated_at: number;
  name: string | null;
  direction: "local_to_remote" | "remote_to_local";
  ssh_host: string;
  ssh_port: number | null;
  ssh_compress: number;
  ssh_args: string | null;
  local_host: string;
  local_port: number;
  remote_host: string;
  remote_port: number;
  desired_state: ForwardDesiredState;
  actual_state: ForwardActualState;
  monitor_pid: number | null;
  last_error: string | null;
}

// DB init

export function ensureSessionDb(sessionDbPath = getSessionDbPath()): Database {
  const db = new Database(sessionDbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("foreign_keys = ON");

  db.exec(`
      CREATE TABLE IF NOT EXISTS sessions(
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at       INTEGER NOT NULL,
        updated_at       INTEGER NOT NULL,
        name             TEXT UNIQUE,

        alpha_root       TEXT NOT NULL,
        beta_root        TEXT NOT NULL,

        prefer           TEXT NOT NULL CHECK (prefer IN ('alpha','beta')),
        alpha_host       TEXT,
        alpha_port       INTEGER,
        beta_host        TEXT,
        beta_port        INTEGER,
        alpha_remote_db  TEXT,
        beta_remote_db   TEXT,
        remote_scan_cmd  TEXT DEFAULT '${CLI_NAME} scan',
        remote_watch_cmd TEXT DEFAULT '${CLI_NAME} watch',

        base_db          TEXT,
        alpha_db         TEXT,
        beta_db          TEXT,
        events_db        TEXT,

        hash_alg         TEXT NOT NULL DEFAULT '${defaultHashAlg()}',
        compress         TEXT,
        ignore_rules     TEXT,  -- JSON.stringify(string[])

        desired_state    TEXT NOT NULL DEFAULT 'paused',
        actual_state     TEXT NOT NULL DEFAULT 'paused',
        last_heartbeat   INTEGER,

        last_digest      INTEGER,
        alpha_digest     TEXT,
        beta_digest      TEXT,

        scheduler_pid    INTEGER
      );

      CREATE INDEX IF NOT EXISTS idx_sessions_state ON sessions(desired_state, actual_state);

      CREATE TABLE IF NOT EXISTS session_labels(
        session_id INTEGER NOT NULL,
        k TEXT NOT NULL,
        v TEXT NOT NULL,
        PRIMARY KEY(session_id, k),
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_labels_kv ON session_labels(k, v);

      CREATE TABLE IF NOT EXISTS session_state(
        session_id     INTEGER PRIMARY KEY,
        pid            INTEGER,
        status         TEXT,        -- starting|running|paused|error
        started_at     INTEGER,
        paused_at     INTEGER,
        last_heartbeat INTEGER,
        running        INTEGER,     -- boolean
        pending        INTEGER,     -- boolean
        last_cycle_ms  INTEGER,
        backoff_ms     INTEGER,
        cycles         INTEGER DEFAULT 0,
        errors         INTEGER DEFAULT 0,
        last_error     TEXT,
        version        TEXT,
        host           TEXT,
        flushing          INTEGER DEFAULT 0,     -- 1 while a flush is in progress
        flush_started_at  INTEGER,               -- ms epoch
        last_flush_started_at INTEGER,           -- ms epoch
        flush_finished_at INTEGER,               -- ms epoch
        last_flush_ok     INTEGER,               -- 1=success, 0=failure, NULL=never
        last_flush_error  TEXT,                  -- error string if last_flush_ok=0
        last_flush_ms     INTEGER                -- duration of last flush in ms
      );

      CREATE TABLE IF NOT EXISTS session_heartbeats(
        session_id     INTEGER,
        ts             INTEGER,
        running        INTEGER,
        pending        INTEGER,
        last_cycle_ms  INTEGER,
        backoff_ms     INTEGER,
        PRIMARY KEY(session_id, ts)
      );
      CREATE INDEX IF NOT EXISTS idx_session_heartbeats_sid_ts
        ON session_heartbeats(session_id, ts);

      -- commands such as "flush":
      CREATE TABLE IF NOT EXISTS session_commands (
          id          INTEGER PRIMARY KEY,
          session_id  INTEGER NOT NULL,
          ts          INTEGER NOT NULL,     -- Date.now() when enqueued
          cmd         TEXT NOT NULL,        -- e.g. 'flush'
          payload     TEXT,                 -- JSON blob (optional)
          acked       INTEGER NOT NULL DEFAULT 0,  -- 0=pending, 1=done (success or failure)
          acked_at    INTEGER               -- when scheduler marked it handled
        );
        CREATE INDEX IF NOT EXISTS idx_session_commands_pending
          ON session_commands(session_id, acked, ts);

      CREATE TABLE IF NOT EXISTS session_logs (
          id          INTEGER PRIMARY KEY,
          session_id  INTEGER NOT NULL,
          ts          INTEGER NOT NULL,
          level       TEXT NOT NULL,
          scope       TEXT,
          message     TEXT NOT NULL,
          meta        TEXT,
          FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS idx_session_logs_sid_ts
        ON session_logs(session_id, ts);
      CREATE INDEX IF NOT EXISTS idx_session_logs_sid_id
        ON session_logs(session_id, id);

      CREATE TABLE IF NOT EXISTS ssh_sessions (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at     INTEGER NOT NULL,
        updated_at     INTEGER NOT NULL,
        name           TEXT UNIQUE,
        direction      TEXT NOT NULL CHECK(direction IN ('local_to_remote','remote_to_local')),
        ssh_host       TEXT NOT NULL,
        ssh_port       INTEGER,
        ssh_compress   INTEGER NOT NULL DEFAULT 0,
        ssh_args       TEXT,
        local_host     TEXT NOT NULL,
        local_port     INTEGER NOT NULL,
        remote_host    TEXT NOT NULL,
        remote_port    INTEGER NOT NULL,
        desired_state  TEXT NOT NULL DEFAULT 'running',
        actual_state   TEXT NOT NULL DEFAULT 'running',
        monitor_pid    INTEGER,
        last_error     TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_ssh_sessions_state ON ssh_sessions(desired_state, actual_state);
    `);

  const ensureColumn = (table: string, column: string, defSql: string) => {
    const existing = db.prepare(`PRAGMA table_info(${table})`).all() as {
      name: string;
    }[];
    if (!existing.some((row) => row.name === column)) {
      db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${defSql}`);
    }
  };

  ensureColumn("sessions", "alpha_port", "INTEGER");
  ensureColumn("sessions", "beta_port", "INTEGER");
  ensureColumn("sessions", "ignore_rules", "TEXT");
  ensureColumn("ssh_sessions", "ssh_args", "TEXT");

  return db;
}

// Core helpers (open/close per call to keep usage simple)

function open(sessionDbPath = getSessionDbPath()): Database {
  const db = new Database(sessionDbPath);
  db.pragma("foreign_keys = ON");
  return db;
}

export function createSession(
  sessionDbPath: string,
  input: SessionCreateInput,
  labels?: Record<string, string>,
): number {
  const db = ensureSessionDb(sessionDbPath);
  try {
    const now = Date.now();
    const stmt = db.prepare(`
      INSERT INTO sessions(
        created_at, updated_at, name,
        alpha_root, beta_root, prefer,
        alpha_host, alpha_port,
        beta_host, beta_port,
        alpha_remote_db, beta_remote_db,
        remote_scan_cmd, remote_watch_cmd,
        hash_alg, compress, ignore_rules
      )
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    const info = stmt.run(
      now,
      now,
      input.name ?? null,
      input.alpha_root,
      input.beta_root,
      input.prefer,
      input.alpha_host ?? null,
      input.alpha_port ?? null,
      input.beta_host ?? null,
      input.beta_port ?? null,
      input.alpha_remote_db ?? null,
      input.beta_remote_db ?? null,
      input.remote_scan_cmd ?? `${CLI_NAME} scan`,
      input.remote_watch_cmd ?? `${CLI_NAME} watch`,
      input.hash_alg ?? defaultHashAlg(),
      input.compress ?? null,
      input.ignore ? serializeIgnoreRules(input.ignore) : null,
    );
    const id = Number(info.lastInsertRowid);

    if (labels && Object.keys(labels).length) {
      const lab = db.prepare(
        `INSERT OR REPLACE INTO session_labels(session_id,k,v) VALUES (?,?,?)`,
      );
      const tx = db.transaction(() => {
        for (const [k, v] of Object.entries(labels)) lab.run(id, k, v);
      });
      tx();
    }
    return id;
  } finally {
    db.close();
  }
}

export function createForwardSession(
  sessionDbPath: string,
  input: ForwardCreateInput,
): number {
  const db = ensureSessionDb(sessionDbPath);
  try {
    const now = Date.now();
    const stmt = db.prepare(`
      INSERT INTO ssh_sessions(
        created_at, updated_at, name,
        direction, ssh_host, ssh_port, ssh_compress, ssh_args,
        local_host, local_port, remote_host, remote_port,
        desired_state, actual_state
      )
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    const info = stmt.run(
      now,
      now,
      input.name ?? null,
      input.direction,
      input.ssh_host,
      input.ssh_port ?? null,
      input.ssh_compress ? 1 : 0,
      input.ssh_args ?? null,
      input.local_host,
      input.local_port,
      input.remote_host,
      input.remote_port,
      input.desired_state ?? "running",
      input.actual_state ?? "running",
    );
    return Number(info.lastInsertRowid);
  } finally {
    db.close();
  }
}

function mapForwardRow(row: any): ForwardRow {
  return {
    ...row,
    ssh_compress: Number(row.ssh_compress ?? 0),
  } as ForwardRow;
}

export function loadForwardById(
  sessionDbPath: string,
  id: number,
): ForwardRow | undefined {
  const db = open(sessionDbPath);
  try {
    const row = db
      .prepare(`SELECT * FROM ssh_sessions WHERE id = ?`)
      .get(id) as any;
    if (!row) return undefined;
    return mapForwardRow(row);
  } finally {
    db.close();
  }
}

export function loadForwardByName(
  sessionDbPath: string,
  name: string,
): ForwardRow | undefined {
  const db = open(sessionDbPath);
  try {
    const row = db
      .prepare(`SELECT * FROM ssh_sessions WHERE name = ?`)
      .get(name) as any;
    if (!row) return undefined;
    return mapForwardRow(row);
  } finally {
    db.close();
  }
}

export function selectForwardSessions(
  sessionDbPath: string,
): ForwardRow[] {
  const db = open(sessionDbPath);
  try {
    const rows = db
      .prepare(`SELECT * FROM ssh_sessions ORDER BY id ASC`)
      .all() as any[];
    return rows.map((row) => mapForwardRow(row)) as ForwardRow[];
  } finally {
    db.close();
  }
}

export function updateForwardSession(
  sessionDbPath: string,
  id: number,
  patch: ForwardPatch,
): void {
  if (!patch || !Object.keys(patch).length) return;
  const db = open(sessionDbPath);
  try {
    const sets: string[] = [];
    const vals: any[] = [];
    for (const [key, value] of Object.entries(patch)) {
      if (key === "ssh_compress") {
        sets.push(`${key} = ?`);
        vals.push(value ? 1 : 0);
      } else {
        sets.push(`${key} = ?`);
        vals.push(value);
      }
    }
    sets.push(`updated_at = ?`);
    vals.push(Date.now());
    vals.push(id);
    const sql = `UPDATE ssh_sessions SET ${sets.join(", ")} WHERE id = ?`;
    db.prepare(sql).run(...vals);
  } finally {
    db.close();
  }
}

export function deleteForwardSession(
  sessionDbPath: string,
  id: number,
): void {
  const db = open(sessionDbPath);
  try {
    db.prepare(`DELETE FROM ssh_sessions WHERE id = ?`).run(id);
  } finally {
    db.close();
  }
}

export function updateSession(
  sessionDbPath: string,
  id: number,
  patch: SessionPatch,
): void {
  const db = open(sessionDbPath);
  try {
    const sets: string[] = [];
    const vals: any[] = [];
    for (const [k, v] of Object.entries(patch)) {
      sets.push(`${k} = ?`);
      vals.push(v);
    }
    sets.push(`updated_at = ?`);
    vals.push(Date.now());
    vals.push(id);

    const sql = `UPDATE sessions SET ${sets.join(", ")} WHERE id = ?`;
    db.prepare(sql).run(...vals);
  } finally {
    db.close();
  }
}

export function upsertLabels(
  sessionDbPath: string,
  id: number,
  labels: Record<string, string>,
): void {
  if (!labels || !Object.keys(labels).length) return;
  const db = open(sessionDbPath);
  try {
    const stmt = db.prepare(
      `INSERT OR REPLACE INTO session_labels(session_id,k,v) VALUES (?,?,?)`,
    );
    const tx = db.transaction(() => {
      for (const [k, v] of Object.entries(labels)) stmt.run(id, k, v);
    });
    tx();
  } finally {
    db.close();
  }
}

export function unsetLabels(
  sessionDbPath: string,
  id: number,
  keys: string[],
): void {
  if (!keys?.length) return;
  const db = open(sessionDbPath);
  try {
    const stmt = db.prepare(
      `DELETE FROM session_labels WHERE session_id=? AND k=?`,
    );
    const tx = db.transaction(() => {
      for (const k of keys) stmt.run(id, k);
    });
    tx();
  } finally {
    db.close();
  }
}

export function clearSessionRuntime(sessionDbPath: string, id: number): void {
  const db = open(sessionDbPath);
  try {
    const tx = db.transaction(() => {
      db.prepare(`DELETE FROM session_state WHERE session_id = ?`).run(id);
      db.prepare(`DELETE FROM session_heartbeats WHERE session_id = ?`).run(id);
      db.prepare(`DELETE FROM session_commands WHERE session_id = ?`).run(id);
      db.prepare(`DELETE FROM session_logs WHERE session_id = ?`).run(id);
    });
    tx();
  } finally {
    db.close();
  }
}

export function deleteSessionById(sessionDbPath: string, id: number) {
  const db = open(sessionDbPath);
  try {
    db.prepare(`DELETE FROM sessions WHERE id = ?`).run(id);
  } finally {
    db.close();
  }
}

export function loadSessionById(
  sessionDbPath: string,
  id: number,
): SessionRow | undefined {
  const db = open(sessionDbPath);
  try {
    const row = db.prepare(`SELECT * FROM sessions WHERE id = ?`).get(id) as
      | SessionRow
      | undefined;
    return row;
  } finally {
    db.close();
  }
}

export function loadSessionByName(
  sessionDbPath: string,
  name: string,
): SessionRow | undefined {
  const db = open(sessionDbPath);
  try {
    const row = db
      .prepare(`SELECT * FROM sessions WHERE name = ?`)
      .get(name) as SessionRow | undefined;
    return row;
  } finally {
    db.close();
  }
}

function isNumericString(value: string): boolean {
  return /^\d+$/.test(value);
}

export function resolveSessionRow(
  sessionDbPath: string,
  ref: string,
): SessionRow | undefined {
  const trimmed = ref.trim();
  if (!trimmed) return undefined;
  if (isNumericString(trimmed)) {
    return loadSessionById(sessionDbPath, Number(trimmed));
  }
  return loadSessionByName(sessionDbPath, trimmed);
}

export function resolveForwardRow(
  sessionDbPath: string,
  ref: string,
): ForwardRow | undefined {
  const trimmed = ref.trim();
  if (!trimmed) return undefined;
  if (isNumericString(trimmed)) {
    return loadForwardById(sessionDbPath, Number(trimmed));
  }
  return loadForwardByName(sessionDbPath, trimmed);
}

/* ============================================================================
   Heartbeat & state
============================================================================ */

export function recordHeartbeat(
  sessionDbPath: string,
  id: number,
  actual_state: ActualState = "running",
  scheduler_pid?: number,
): void {
  const db = open(sessionDbPath);
  try {
    const now = Date.now();
    const stmt = db.prepare(
      `UPDATE sessions
       SET last_heartbeat=?, actual_state=?, updated_at=?, scheduler_pid=COALESCE(?, scheduler_pid)
       WHERE id=?`,
    );
    stmt.run(now, actual_state, now, scheduler_pid ?? null, id);
  } finally {
    db.close();
  }
}

export function setDesiredState(
  sessionDbPath: string,
  id: number,
  desired: DesiredState,
): void {
  const db = open(sessionDbPath);
  try {
    db.prepare(
      `UPDATE sessions SET desired_state=?, updated_at=? WHERE id=?`,
    ).run(desired, Date.now(), id);
  } finally {
    db.close();
  }
}

export function setActualState(
  sessionDbPath: string,
  id: number,
  actual: ActualState,
): void {
  const db = open(sessionDbPath);
  try {
    db.prepare(
      `UPDATE sessions SET actual_state=?, updated_at=? WHERE id=?`,
    ).run(actual, Date.now(), id);
  } finally {
    db.close();
  }
}

//   Listing with simple label selectors
//   Supports: k=v, k!=v, k, !k  (AND across multiple selectors)

export type LabelSelector =
  | { type: "eq"; k: string; v: string }
  | { type: "neq"; k: string; v: string }
  | { type: "exists"; k: string }
  | { type: "notExists"; k: string };

export function parseSelectorTokens(tokens: string[]): LabelSelector[] {
  const out: LabelSelector[] = [];
  for (const t of tokens || []) {
    const s = t.trim();
    if (!s) continue;
    if (s.startsWith("!")) {
      out.push({ type: "notExists", k: s.slice(1) });
    } else if (s.includes("!=")) {
      const [k, v] = s.split("!=");
      out.push({ type: "neq", k, v });
    } else if (s.includes("=")) {
      const [k, v] = s.split("=");
      out.push({ type: "eq", k, v });
    } else {
      out.push({ type: "exists", k: s });
    }
  }
  return out;
}

export function selectSessions(
  sessionDbPath: string,
  selectors: LabelSelector[] = [],
): SessionRow[] {
  const db = open(sessionDbPath);
  try {
    let where = "1=1";
    const joins: string[] = [];
    const params: any[] = [];
    let aliasIdx = 0;

    for (const sel of selectors) {
      const alias = `l${aliasIdx++}`;
      if (sel.type === "eq") {
        joins.push(
          `JOIN session_labels ${alias}
             ON ${alias}.session_id = s.id AND ${alias}.k = ? AND ${alias}.v = ?`,
        );
        params.push(sel.k, sel.v);
      } else if (sel.type === "neq") {
        joins.push(
          `LEFT JOIN session_labels ${alias}
             ON ${alias}.session_id = s.id AND ${alias}.k = ?`,
        );
        where += ` AND (${alias}.rowid IS NULL OR ${alias}.v <> ?)`;
        params.push(sel.k, sel.v);
      } else if (sel.type === "exists") {
        joins.push(
          `JOIN session_labels ${alias}
             ON ${alias}.session_id = s.id AND ${alias}.k = ?`,
        );
        params.push(sel.k);
      } else if (sel.type === "notExists") {
        joins.push(
          `LEFT JOIN session_labels ${alias}
             ON ${alias}.session_id = s.id AND ${alias}.k = ?`,
        );
        where += ` AND ${alias}.rowid IS NULL`;
        params.push(sel.k);
      }
    }

    const sql = `
      SELECT s.*
      FROM sessions s
      ${joins.join("\n")}
      WHERE ${where}
      ORDER BY s.id ASC
    `;
    const rows = db.prepare(sql).all(...params);
    return rows as any[];
  } finally {
    db.close();
  }
}

//   Convenience: create per-session dirs and update DB paths

export function materializeSessionPaths(
  id: number,
  home = getReflectSyncHome(),
): { base_db: string; alpha_db: string; beta_db: string; events_db: string } {
  const derived = deriveSessionPaths(id, home);
  ensureDir(derived.dir);
  return {
    base_db: derived.base_db,
    alpha_db: derived.alpha_db,
    beta_db: derived.beta_db,
    events_db: derived.events_db,
  };
}

// SessionWriter below is used by the scheduler to report
// on its health periodically.

export interface CycleMetrics {
  lastCycleMs: number;
  scanAlphaMs: number;
  scanBetaMs: number;
  mergeMs: number;
  backoffMs: number;
}

export class SessionWriter {
  private upsertState;
  private updateHeartbeatStamp;
  private insertHeartbeat;
  private bumpCycles;
  private setErrorStmt;
  private stopStmt;

  constructor(
    private db: Database,
    private id: number,
  ) {
    this.upsertState = this.db.prepare(`
    INSERT INTO session_state(session_id, pid, status, started_at, last_heartbeat, running, pending, version, host)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(session_id) DO UPDATE SET
      pid=excluded.pid,
      status=excluded.status,
      last_heartbeat=excluded.last_heartbeat,
      running=excluded.running,
      pending=excluded.pending,
      version=excluded.version,
      host=excluded.host
  `);

    this.updateHeartbeatStamp = this.db.prepare(`
    UPDATE session_state SET last_heartbeat=? WHERE session_id=?
  `);

    this.insertHeartbeat = this.db.prepare(`
    INSERT OR REPLACE INTO session_heartbeats(session_id, ts, running, pending, last_cycle_ms, backoff_ms)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

    this.bumpCycles = this.db.prepare(`
    UPDATE session_state SET
      cycles       = COALESCE(cycles,0) + 1,
      last_cycle_ms = ?,
      backoff_ms    = ?
    WHERE session_id = ?
  `);

    this.setErrorStmt = this.db.prepare(`
    UPDATE session_state SET
      errors = COALESCE(errors,0) + 1,
      last_error = ?,
      status     = 'error'
    WHERE session_id = ?
  `);

    this.stopStmt = this.db.prepare(`
    UPDATE session_state SET
      status='paused',
      paused_at=?,
      last_heartbeat=?
    WHERE session_id = ?
  `);
  }

  static open(dbPath: string, sessionId: number) {
    return new SessionWriter(ensureSessionDb(dbPath), sessionId);
  }

  start(version?: string) {
    const now = Date.now();
    this.upsertState.run(
      this.id,
      process.pid,
      "running",
      now,
      now,
      1,
      0,
      version ?? "",
      os.hostname(),
    );
  }

  heartbeat(
    running: boolean,
    pending: boolean,
    lastCycleMs: number,
    backoffMs: number,
  ) {
    const ts = Date.now();
    this.insertHeartbeat.run(
      this.id,
      ts,
      running ? 1 : 0,
      pending ? 1 : 0,
      Math.max(0, lastCycleMs | 0),
      Math.max(0, backoffMs | 0),
    );
    this.updateHeartbeatStamp.run(ts, this.id);
    this.pruneHeartbeats();
  }

  cycleDone(m: CycleMetrics) {
    this.bumpCycles.run(
      Math.max(0, m.lastCycleMs | 0),
      Math.max(0, m.backoffMs | 0),
      this.id,
    );
    // Clear last_error on success:
    this.db
      .prepare(`UPDATE session_state SET last_error=NULL WHERE session_id=?`)
      .run(this.id);
  }

  error(msg: string) {
    this.setErrorStmt.run(String(msg).slice(0, 4096), this.id);
  }

  stop() {
    const ts = Date.now();
    this.stopStmt.run(ts, ts, this.id);
  }

  /**
   * Prune old heartbeats to keep the table bounded (don't waste disk).
   * Controls:
   *   REFLECT_HEARTBEAT_KEEP_MS    (default: 0 => disabled)
   *   REFLECT_HEARTBEAT_KEEP_ROWS  (default: 7200 rows, ~4h if 2s interval)
   */
  private pruneHeartbeats() {
    const keepMs = Number(process.env.REFLECT_HEARTBEAT_KEEP_MS ?? 0);
    const keepRows = Number(process.env.REFLECT_HEARTBEAT_KEEP_ROWS ?? 7200);

    if (keepMs > 0) {
      const cutoff = Date.now() - keepMs;
      this.db
        .prepare(
          `DELETE FROM session_heartbeats
             WHERE session_id = ?
               AND ts < ?`,
        )
        .run(this.id, cutoff);
    }

    if (keepRows > 0) {
      // Delete anything older than the Nth newest row
      this.db
        .prepare(
          `
          DELETE FROM session_heartbeats
           WHERE session_id = ?
             AND ts < COALESCE((
               SELECT ts FROM session_heartbeats
                WHERE session_id = ?
                ORDER BY ts DESC
                LIMIT 1 OFFSET ?
             ), -1)
        `,
        )
        .run(this.id, this.id, keepRows);
    }
  }
}
