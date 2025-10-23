// session-db.ts
import Database from "better-sqlite3";
import fs from "node:fs";
import { join } from "node:path";
import os from "node:os";

// Paths & Home

export function getCcsyncHome(): string {
  const explicit = process.env.CCSYNC_HOME?.trim();
  if (explicit) {
    return ensureDir(expandHome(explicit));
  }

  // XDG first
  const xdg = process.env.XDG_DATA_HOME;
  if (xdg && xdg.trim()) {
    return ensureDir(join(expandHome(xdg), "ccsync"));
  }

  // Platform defaults
  const home = os.homedir();
  if (process.platform === "darwin") {
    return ensureDir(join(home, "Library", "Application Support", "ccsync"));
  }
  if (process.platform === "win32") {
    const appData = process.env.APPDATA || join(home, "AppData", "Roaming");
    return ensureDir(join(appData, "ccsync"));
  }
  // Linux/other
  return ensureDir(join(home, ".local", "share", "ccsync"));
}

export function getSessionDbPath(home = getCcsyncHome()): string {
  return join(home, "sessions.db");
}

export function sessionDir(id: number, home = getCcsyncHome()): string {
  return join(home, "sessions", String(id));
}

export function deriveSessionPaths(id: number, home = getCcsyncHome()) {
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
export type DesiredState = "running" | "stopped" | "paused";
export type ActualState =
  | "running"
  | "starting"
  | "stopped"
  | "paused"
  | "error";

export interface SessionCreateInput {
  name?: string;
  alpha_root: string;
  beta_root: string;
  prefer: Side;
  alpha_host?: string | null;
  beta_host?: string | null;
  alpha_remote_db?: string | null;
  beta_remote_db?: string | null;
  remote_scan_cmd?: string | null;
  remote_watch_cmd?: string | null;
}

export interface SessionPatch {
  name?: string | null;
  alpha_root?: string;
  beta_root?: string;
  prefer?: Side;
  alpha_host?: string | null;
  beta_host?: string | null;
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
  beta_host: string | null;
  alpha_remote_db: string | null;
  beta_remote_db: string | null;
  remote_scan_cmd: string | null;
  remote_watch_cmd: string | null;
  base_db: string | null;
  alpha_db: string | null;
  beta_db: string | null;
  events_db: string | null;
  desired_state: DesiredState;
  actual_state: ActualState;
  last_heartbeat: number | null;
  scheduler_pid: number | null;
}

// DB init

export function ensureSessionDb(sessionDbPath = getSessionDbPath()): void {
  const db = new Database(sessionDbPath);
  try {
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
    db.pragma("foreign_keys = ON");

    db.exec(`
      CREATE TABLE IF NOT EXISTS sessions(
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at       INTEGER NOT NULL,
        updated_at       INTEGER NOT NULL,
        name             TEXT,

        alpha_root       TEXT NOT NULL,
        beta_root        TEXT NOT NULL,

        prefer           TEXT NOT NULL CHECK (prefer IN ('alpha','beta')),
        alpha_host       TEXT,
        beta_host        TEXT,
        alpha_remote_db  TEXT,
        beta_remote_db   TEXT,
        remote_scan_cmd  TEXT DEFAULT 'ccsync scan',
        remote_watch_cmd TEXT DEFAULT 'ccsync watch',

        base_db          TEXT,
        alpha_db         TEXT,
        beta_db          TEXT,
        events_db        TEXT,

        desired_state    TEXT NOT NULL DEFAULT 'stopped',
        actual_state     TEXT NOT NULL DEFAULT 'stopped',
        last_heartbeat   INTEGER,

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
    `);
  } finally {
    db.close();
  }
}

// Core helpers (open/close per call to keep usage simple)

function open(sessionDbPath = getSessionDbPath()): Database.Database {
  const db = new Database(sessionDbPath);
  db.pragma("foreign_keys = ON");
  return db;
}

export function createSession(
  sessionDbPath: string,
  input: SessionCreateInput,
  labels?: Record<string, string>,
): number {
  ensureSessionDb(sessionDbPath);
  const db = open(sessionDbPath);
  try {
    const now = Date.now();
    const stmt = db.prepare(`
      INSERT INTO sessions(
        created_at, updated_at, name,
        alpha_root, beta_root, prefer,
        alpha_host, beta_host, alpha_remote_db, beta_remote_db,
        remote_scan_cmd, remote_watch_cmd
      )
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    const info = stmt.run(
      now,
      now,
      input.name ?? null,
      expandHome(input.alpha_root),
      expandHome(input.beta_root),
      input.prefer,
      input.alpha_host ?? null,
      input.beta_host ?? null,
      input.alpha_remote_db ?? null,
      input.beta_remote_db ?? null,
      input.remote_scan_cmd ?? "ccsync scan",
      input.remote_watch_cmd ?? "ccsync watch",
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
    const rows = db.prepare(sql).all(...params) as SessionRow[];
    return rows;
  } finally {
    db.close();
  }
}

//   Convenience: create per-session dirs and update DB paths

export function materializeSessionPaths(
  sessionDbPath: string,
  id: number,
  home = getCcsyncHome(),
): { base_db: string; alpha_db: string; beta_db: string; events_db: string } {
  const derived = deriveSessionPaths(id, home);
  ensureDir(derived.dir);
  updateSession(sessionDbPath, id, {
    base_db: derived.base_db,
    alpha_db: derived.alpha_db,
    beta_db: derived.beta_db,
    events_db: derived.events_db,
  });
  return {
    base_db: derived.base_db,
    alpha_db: derived.alpha_db,
    beta_db: derived.beta_db,
    events_db: derived.events_db,
  };
}
