// src/session-status.ts
import { Command, Option } from "commander";
import {
  ensureSessionDb,
  getSessionDbPath,
  resolveSessionRow,
} from "./session-db.js";
import { type Database } from "./db.js";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";
import { deserializeIgnoreRules } from "./ignore.js";

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

const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

export function fmtAgo(
  input: Date | number | string,
  now = Date.now(),
): string {
  const t = typeof input === "number" ? input : new Date(input).getTime();
  const diff = t - now; // negative for past, positive for future
  const units: [Intl.RelativeTimeFormatUnit, number][] = [
    ["year", 365 * 24 * 60 * 60 * 1000],
    ["month", 30 * 24 * 60 * 60 * 1000],
    ["week", 7 * 24 * 60 * 60 * 1000],
    ["day", 24 * 60 * 60 * 1000],
    ["hour", 60 * 60 * 1000],
    ["minute", 60 * 1000],
    ["second", 1000],
  ];

  for (const [unit, ms] of units) {
    const val = Math.trunc(diff / ms);
    if (Math.abs(val) >= 1) return rtf.format(val, unit);
  }
  return rtf.format(0, "second"); // "now"
}

export function fmtLocalPath(path: string) {
  const home = process.env.HOME;
  if (!home) return path;
  if (path.startsWith(home)) {
    return `~${path.slice(home.length)}`;
  }
  return path;
}

function fmtCleanMarker(ts?: number | null): string {
  if (!ts) return "-";
  return `✓ ${fmtAgo(ts)}`;
}

function hostWithPort(
  host?: string | null,
  port?: number | null,
): string | null {
  if (!host) return null;
  return port != null ? `${host}:${port}` : host;
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
  const staleMs = Number(process.env.REFLECT_SESSION_STALE_MS ?? 15_000);
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
      const hostPart = hostWithPort(sess.alpha_host, sess.alpha_port);
      add("alpha", `${hostPart ? `${hostPart}:` : ""}${sess.alpha_root ?? ""}`);
    }
    if (sess.beta_root || sess.beta_host) {
      const hostPart = hostWithPort(sess.beta_host, sess.beta_port);
      add("beta", `${hostPart ? `${hostPart}:` : ""}${sess.beta_root ?? ""}`);
    }
    if (sess.prefer) add("prefer", sess.prefer);
    if (sess.hash_alg) add("hash", sess.hash_alg);
    if (sess.compress) add("compress", sess.compress);
    const ignoreRules = deserializeIgnoreRules(sess.ignore_rules);
    if (ignoreRules.length) {
      add("ignore rules", ignoreRules.join(", "));
    }
    add("hot-sync", sess.disable_hot_sync ? "disabled" : "enabled");
    add("reflink", sess.enable_reflink ? "enabled" : "disabled");
    add("full cycle", sess.disable_full_cycle ? "disabled" : "enabled");
    add("last clean sync", fmtCleanMarker(sess.last_clean_sync_at));
    if (sess.base_db) add("base db", fmtLocalPath(sess.base_db));
    if (sess.alpha_db) add("alpha db", fmtLocalPath(sess.alpha_db));
    if (sess.beta_db) add("beta db", fmtLocalPath(sess.beta_db));
    if (sess.alpha_remote_db) {
      const hostPart = hostWithPort(sess.alpha_host, sess.alpha_port);
      add(
        "alpha remote db",
        hostPart ? `${hostPart}:${sess.alpha_remote_db}` : sess.alpha_remote_db,
      );
    }
    if (sess.beta_remote_db) {
      const hostPart = hostWithPort(sess.beta_host, sess.beta_port);
      add(
        "beta remote db",
        hostPart ? `${hostPart}:${sess.beta_remote_db}` : sess.beta_remote_db,
      );
    }
    if (sess.created_at)
      add(
        "created",
        `${fmtAgo(sess.created_at)} on ${new Date(sess.created_at)}`,
      );
    if (sess.actual_state && sess.desired_state)
      add(
        "State/Desired",
        `${sess.actual_state}/${sess.desired_state}`,
      );
  }

  // Runtime
  const hbAgo = state?.last_heartbeat ? fmtAgo(state.last_heartbeat) : "-";
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
  if (state?.stopped_at && state?.status != "running")
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
    .argument("<id-or-name>", "session id or name")
    .addOption(
      new Option("--session-db <file>", "path to sessions database").default(
        getSessionDbPath(),
      ),
    )
    .option("--json", "output JSON instead of human text", false)
    .action((idArg: string, opts: { sessionDb: string; json?: boolean }) => {
      const ref = idArg.trim();
      const row = resolveSessionRow(opts.sessionDb, ref);
      if (!row) {
        console.error(`session status: session '${ref}' not found`);
        process.exit(1);
      }
      const id = row.id;
      const db = ensureSessionDb(opts.sessionDb);
      try {
        const sess = getSession(db, id);
        if (!sess) {
          console.error(`session status: session '${ref}' not found`);
          process.exit(1);
        }
        const labels = tryGetLabels(db, id);
        const state = getState(db, id);
        const lastHb = getLastHeartbeat(db, id);
        const ignoreRules = deserializeIgnoreRules(sess.ignore_rules);

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
                  port: sess.alpha_port ?? null,
                  root: sess.alpha_root ?? null,
                  db: sess.alpha_db ?? null,
                  alpha_remote_db: sess.alpha_remote_db,
                },
                beta: {
                  host: sess.beta_host ?? null,
                  port: sess.beta_port ?? null,
                  root: sess.beta_root ?? null,
                  db: sess.beta_db ?? null,
                  beta_remote_db: sess.beta_remote_db,
                },
                baseDb: sess.base_db ?? null,
                prefer: sess.prefer ?? null,
                createdAt: sess.created_at ?? null,
                ignoreRules,
              },
              sync: {
                lastCleanAt: sess.last_clean_sync_at ?? null,
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
