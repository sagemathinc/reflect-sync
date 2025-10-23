// src/session-status.ts
import { Command, Option } from "commander";
import { ensureSessionDb, getSessionDbPath } from "./session-db.js";
import type Database from "better-sqlite3";

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

function tryGetLabels(
  db: Database.Database,
  sessionId: number,
): Record<string, string> {
  try {
    // optional table; ignore if missing
    const rows = db
      .prepare(
        `SELECT k, v FROM session_labels WHERE session_id = ? ORDER BY k`,
      )
      .all(sessionId) as { k: string; v: string }[];
    const out: Record<string, string> = {};
    for (const r of rows) out[r.k] = r.v;
    return out;
  } catch (err) {
    console.warn(err);
    return {};
  }
}

function getSession(db: Database.Database, sessionId: number): AnyRow | null {
  try {
    const row = db
      .prepare(`SELECT * FROM sessions WHERE id = ?`)
      .get(sessionId) as AnyRow | undefined;
    return row ?? null;
  } catch (err) {
    console.warn(err);
    return null;
  }
}

function getState(db: Database.Database, sessionId: number): AnyRow | null {
  try {
    const row = db
      .prepare(`SELECT * FROM session_state WHERE session_id = ?`)
      .get(sessionId) as AnyRow | undefined;
    return row ?? null;
  } catch (err) {
    console.warn(err);
    return null;
  }
}

function getLastHeartbeat(
  db: Database.Database,
  sessionId: number,
): AnyRow | null {
  try {
    const row = db
      .prepare(
        `SELECT * FROM session_heartbeats WHERE session_id = ? ORDER BY ts DESC LIMIT 1`,
      )
      .get(sessionId) as AnyRow | undefined;
    return row ?? null;
  } catch (err) {
    console.warn(err);
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

function textOutput(
  sessionId: number,
  sess: AnyRow | null,
  labels: Record<string, string>,
  state: AnyRow | null,
  lastHb: AnyRow | null,
): string {
  const lines: string[] = [];
  lines.push(`Session ${sessionId}`);
  if (sess?.name) lines.push(`  name:             ${sess.name}`);
  if (Object.keys(labels).length) {
    lines.push(
      `  labels:           ${Object.entries(labels)
        .map(([k, v]) => `${k}=${v}`)
        .join(", ")}`,
    );
  }

  // config (if present)
  if (sess) {
    if (sess.alpha_root || sess.alpha_host) {
      lines.push(
        `  alpha:            ${sess.alpha_host ? `${sess.alpha_host}:` : ""}${sess.alpha_root ?? ""}`,
      );
    }
    if (sess.beta_root || sess.beta_host) {
      lines.push(
        `  beta:             ${sess.beta_host ? `${sess.beta_host}:` : ""}${sess.beta_root ?? ""}`,
      );
    }
    if (sess.prefer) lines.push(`  prefer:           ${sess.prefer}`);
    if (sess.base_db) lines.push(`  base db:          ${sess.base_db}`);
    if (sess.alpha_db) lines.push(`  alpha db:         ${sess.alpha_db}`);
    if (sess.beta_db) lines.push(`  beta db:          ${sess.beta_db}`);
    if (sess.created_at)
      lines.push(
        `  created:          ${new Date(sess.created_at).toISOString()}`,
      );
  }

  // runtime
  const hbAgo = state?.last_heartbeat ? fmtAgo(state.last_heartbeat) : "-";
  lines.push(`  status:           ${state?.status ?? "-"}`);
  if (state?.pid) lines.push(`  pid:              ${state.pid}`);
  if (state?.host) lines.push(`  host:             ${state.host}`);
  lines.push(`  running:          ${state?.running ? "yes" : "no"}`);
  lines.push(`  pending:          ${state?.pending ? "yes" : "no"}`);
  if (state?.cycles != null) lines.push(`  cycles:           ${state.cycles}`);
  if (state?.errors != null) lines.push(`  errors:           ${state.errors}`);
  lines.push(`  last heartbeat:   ${hbAgo}`);
  if (state?.last_cycle_ms != null)
    lines.push(`  last cycle:       ${fmtMs(state.last_cycle_ms)}`);
  if (state?.backoff_ms != null)
    lines.push(`  backoff:          ${fmtMs(state.backoff_ms)}`);
  if (state?.started_at)
    lines.push(
      `  started:          ${new Date(state.started_at).toISOString()}`,
    );
  if (state?.stopped_at)
    lines.push(
      `  stopped:          ${new Date(state.stopped_at).toISOString()}`,
    );
  if (state?.last_error) lines.push(`  last error:       ${state.last_error}`);

  // health
  const { health, reason } = computeHealth(state, lastHb);
  lines.push(`  health:           ${health}${reason ? ` (${reason})` : ""}`);

  return lines.join("\n");
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
                },
                beta: {
                  host: sess.beta_host ?? null,
                  root: sess.beta_root ?? null,
                  db: sess.beta_db ?? null,
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
          console.log(textOutput(id, sess, labels, state, lastHb));
        }
      } finally {
        db.close();
      }
    });
}
