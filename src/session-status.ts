// src/session-status.ts
import { Command, Option } from "commander";
import { ensureSessionDb, getSessionDbPath } from "./session-db.js";
import { type Database } from "./db.js";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";

type AnyRow = Record<string, any>;

function fmtMs(ms?: number | null): string {
  if (!ms || ms < 0) return "-";
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)} s`;
  const m = Math.floor(s / 60);
  const rs = (s % 60).toFixed(0);
  return `${m}m ${rs}s`;
}

function fmtAgo(ts?: number | null): string {
  if (!ts) return "-";
  const d = Date.now() - ts;
  if (d < 0) return "0s";
  if (d < 1000) return `${d} ms`;
  const s = Math.floor(d / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function tryGetLabels(db: Database, sessionId: number): Record<string, string> {
  try {
    const rows = db
      .prepare(
        `SELECT k, v FROM session_labels WHERE session_id = ? ORDER BY k`,
      )
      .all(sessionId) as { k: string; v: string }[];
    const out: Record<string, string> = {};
    for (const r of rows) out[r.k] = r.v;
    return out;
  } catch {
    return {};
  }
}

function getSession(db: Database, sessionId: number): AnyRow | null {
  try {
    const row = db
      .prepare(`SELECT * FROM sessions WHERE id = ?`)
      .get(sessionId) as AnyRow | undefined;
    return row ?? null;
  } catch {
    return null;
  }
}

function getState(db: Database, sessionId: number): AnyRow | null {
  try {
    const row = db
      .prepare(`SELECT * FROM session_state WHERE session_id = ?`)
      .get(sessionId) as AnyRow | undefined;
    return row ?? null;
  } catch {
    return null;
  }
}

function getLastHeartbeat(db: Database, sessionId: number): AnyRow | null {
  try {
    const row = db
      .prepare(
        `SELECT * FROM session_heartbeats WHERE session_id = ? ORDER BY ts DESC LIMIT 1`,
      )
      .get(sessionId) as AnyRow | undefined;
    return row ?? null;
  } catch {
    return null;
  }
}

function computeHealth(
  state: AnyRow | null,
  lastHb: AnyRow | null,
): {
  health: "healthy" | "stale" | "stopped" | "unknown";
  reason?: string;
} {
  if (!state) return { health: "unknown" };
  if (state.status === "stopped" || state.running === 0) {
    return { health: "stopped" };
  }
  const staleMs = Number(process.env.SESSION_STALE_MS ?? 15_000);
  const last = (state.last_heartbeat as number) || (lastHb?.ts as number) || 0;
  if (!last) return { health: "unknown", reason: "no heartbeat yet" };
  const ago = Date.now() - last;
  if (ago > staleMs)
    return { health: "stale", reason: `last heartbeat ${fmtAgo(last)} ago` };
  return { health: "healthy" };
}

function tableOutput(
  sessionId: number,
  sess: AnyRow | null,
  labels: Record<string, string>,
  state: AnyRow | null,
  lastHb: AnyRow | null,
): string {
  const table = new AsciiTable3(
    `Session ${sessionId}${sess?.name ? ": " + sess.name : ""}`,
  )
    .setHeading("Field", "Value")
    .setStyle("unicode-round");

  let n = 0;
  const add = (k: string, v?: any) => {
    table.setAlign(n++, AlignmentEnum.LEFT);
    const s =
      v === undefined || v === null || v === ""
        ? "-"
        : typeof v === "string"
          ? v
          : String(v);
    table.addRow(k, s);
  };

  // Name & labels
  if (sess?.name) add("name", sess.name);
  if (Object.keys(labels).length) {
    add(
      "labels",
      Object.entries(labels)
        .map(([k, v]) => `${k}=${v}`)
        .join(", "),
    );
  }

  // Config
  if (sess) {
    if (sess.alpha_root || sess.alpha_host) {
      add(
        "alpha",
        `${sess.alpha_host ? `${sess.alpha_host}:` : ""}${sess.alpha_root ?? ""}`,
      );
    }
    if (sess.beta_root || sess.beta_host) {
      add(
        "beta",
        `${sess.beta_host ? `${sess.beta_host}:` : ""}${sess.beta_root ?? ""}`,
      );
    }
    if (sess.prefer) add("prefer", sess.prefer);
    if (sess.base_db) add("base db", sess.base_db);
    if (sess.alpha_db) add("alpha db", sess.alpha_db);
    if (sess.beta_db) add("beta db", sess.beta_db);
    if (sess.alpha_remote_db)
      add("alpha remote", `${sess.alpha_host}:${sess.alpha_remote_db}`);
    if (sess.beta_remote_db)
      add("beta remote", `${sess.beta_host}:${sess.beta_remote_db}`);
    if (sess.created_at)
      add("created", new Date(sess.created_at).toISOString());
  }

  // Runtime
  const hbAgo = state?.last_heartbeat ? fmtAgo(state.last_heartbeat) : "-";
  add("status", state?.status ?? "-");
  if (state?.pid) add("pid", state.pid);
  if (state?.host) add("host", state.host);
  add("running", state?.running ? "yes" : "no");
  add("pending", state?.pending ? "yes" : "no");
  if (state?.cycles != null) add("cycles", state.cycles);
  if (state?.errors != null) add("errors", state.errors);
  add("last heartbeat", hbAgo);
  if (state?.last_cycle_ms != null)
    add("last cycle", fmtMs(state.last_cycle_ms));
  if (state?.backoff_ms != null) add("backoff", fmtMs(state.backoff_ms));
  if (state?.started_at)
    add("started", new Date(state.started_at).toISOString());
  if (state?.stopped_at)
    add("stopped", new Date(state.stopped_at).toISOString());
  if (state?.last_error) add("last error", state.last_error);

  // Health
  const { health, reason } = computeHealth(state, lastHb);
  add("health", reason ? `${health} (${reason})` : health);

  return table.toString();
}

export function registerSessionStatus(sessionCmd: Command) {
  sessionCmd
    .command("status")
    .description("show runtime status of a sync session")
    .argument("<id>", "session id (integer)")
    .addOption(
      new Option("--session-db <file>", "path to sessions database").default(
        getSessionDbPath(),
      ),
    )
    .option("--json", "output JSON instead of human text", false)
    .action((idArg: string, opts: { sessionDb: string; json?: boolean }) => {
      const id = Number(idArg);
      if (!Number.isFinite(id)) {
        console.error("session status: <id> must be a number");
        process.exit(2);
      }
      const db = ensureSessionDb(opts.sessionDb);
      try {
        const sess = getSession(db, id);
        if (!sess) {
          console.error(`session status: session ${id} not found`);
          process.exit(1);
        }
        const labels = tryGetLabels(db, id);
        const state = getState(db, id);
        const lastHb = getLastHeartbeat(db, id);

        if (opts.json) {
          const { health, reason } = computeHealth(state, lastHb);
          const out = {
            session: {
              id,
              name: sess.name ?? null,
              labels,
              config: {
                alpha: {
                  host: sess.alpha_host ?? null,
                  root: sess.alpha_root ?? null,
                  db: sess.alpha_db ?? null,
                  alpha_remote_db: sess.alpha_remote_db,
                },
                beta: {
                  host: sess.beta_host ?? null,
                  root: sess.beta_root ?? null,
                  db: sess.beta_db ?? null,
                  beta_remote_db: sess.beta_remote_db,
                },
                baseDb: sess.base_db ?? null,
                prefer: sess.prefer ?? null,
                createdAt: sess.created_at ?? null,
              },
            },
            state: state ?? null,
            lastHeartbeat: lastHb ?? null,
            health: { status: health, reason },
          };
          console.log(JSON.stringify(out, null, 2));
        } else {
          console.log(tableOutput(id, sess, labels, state, lastHb));
        }
      } finally {
        db.close();
      }
    });
}
