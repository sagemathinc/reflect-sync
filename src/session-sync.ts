// src/session-sync.ts

import { Command } from "commander";
import {
  ensureSessionDb,
  getSessionDbPath,
  resolveSessionRow,
  materializeSessionPaths,
} from "./session-db.js";
import type { Database } from "./db.js";
import { planThreeWayMerge } from "./three-way-merge.js";
import { fetchSessionLogs, type SessionLogRow } from "./session-logs.js";
import { collectListOption } from "./restrict.js";
import { wait } from "./util.js";

const POLL_INTERVAL_MS = 200;
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_MAX_CYCLES = 3;
const PROGRESS_MESSAGE_FILTER = "progress";

interface ProgressState {
  enabled: boolean;
  json: boolean;
  sessionDbPath: string;
  sessionId: number;
  lastLogId: number;
}

function parsePositiveInt(
  raw: string | number | undefined,
  fallback: number,
): number {
  if (raw === undefined) return fallback;
  const value =
    typeof raw === "number"
      ? Number.isFinite(raw)
        ? Math.trunc(raw)
        : NaN
      : Number.parseInt(raw, 10);
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return value;
}

function checkSessionRunning(
  db: Database,
  sessionId: number,
): { ok: boolean; reason?: string } {
  const session = db
    .prepare(`SELECT actual_state FROM sessions WHERE id = ?`)
    .get(sessionId) as { actual_state?: string } | undefined;
  if (!session) {
    return { ok: false, reason: "session row missing" };
  }
  if (session.actual_state !== "running") {
    return {
      ok: false,
      reason: `actual_state=${session.actual_state ?? "unknown"}`,
    };
  }
  const state = db
    .prepare(
      `SELECT running, status
         FROM session_state
        WHERE session_id = ?`,
    )
    .get(sessionId) as { running?: number; status?: string } | undefined;
  if (!state || state.running !== 1) {
    return {
      ok: false,
      reason: state
        ? `status=${state.status ?? "unknown"}`
        : "scheduler state missing",
    };
  }
  return { ok: true };
}

function renderProgressRow(row: SessionLogRow, asJson: boolean): void {
  if (asJson) {
    // Match the JSON emitted by `reflect logs --json`
    const payload = {
      id: row.id,
      session_id: row.session_id,
      ts: row.ts,
      level: row.level,
      scope: row.scope,
      message: row.message,
      meta: row.meta ?? null,
    };
    console.log(JSON.stringify(payload));
    return;
  }

  const timestamp = new Date(row.ts).toISOString();
  const scope = row.scope ? ` [${row.scope}]` : "";
  const meta =
    row.meta && Object.keys(row.meta).length
      ? ` ${JSON.stringify(row.meta)}`
      : "";
  console.log(
    `${timestamp} ${row.level.toUpperCase()}${scope} ${row.message}${meta}`,
  );
}

function drainProgressLogs(state: ProgressState): void {
  if (!state.enabled) return;
  const rows = fetchSessionLogs(state.sessionDbPath, state.sessionId, {
    afterId: state.lastLogId,
    message: PROGRESS_MESSAGE_FILTER,
    order: "asc",
  });
  if (!rows.length) return;
  for (const row of rows) {
    renderProgressRow(row, state.json);
    state.lastLogId = Math.max(state.lastLogId, row.id);
  }
}

function getPlanTargets(sessionRow: any) {
  const derived = materializeSessionPaths(sessionRow.id);
  return {
    alphaDb: sessionRow.alpha_db ?? derived.alpha_db,
    betaDb: sessionRow.beta_db ?? derived.beta_db,
    baseDb: sessionRow.base_db ?? derived.base_db,
    prefer: (sessionRow.prefer ?? "alpha") as "alpha" | "beta",
    strategy: sessionRow.merge_strategy ?? "last-write-wins",
  };
}

function enqueueSyncCommand(
  db: Database,
  sessionId: number,
  attempt: number,
  paths: string[] = [],
): number {
  const ts = Date.now();
  const payload = JSON.stringify({ requested_at: ts, attempt, paths });
  const info = db
    .prepare(
      `INSERT INTO session_commands(session_id, ts, cmd, payload, acked)
       VALUES (?, ?, 'sync', ?, 0)`,
    )
    .run(sessionId, ts, payload);
  return Number(info.lastInsertRowid);
}

async function waitForCommandAck(
  db: Database,
  sessionId: number,
  commandId: number,
  timeoutMs: number,
  progress?: ProgressState,
): Promise<void> {
  const started = Date.now();
  while (true) {
    const row = db
      .prepare(`SELECT acked FROM session_commands WHERE id = ?`)
      .get(commandId) as { acked?: number } | undefined;
    if (!row) {
      throw new Error("sync command disappeared before acknowledgement");
    }
    if (row.acked === 1) return;

    if (progress) {
      drainProgressLogs(progress);
    }

    const running = checkSessionRunning(db, sessionId);
    if (!running.ok) {
      throw new Error(
        `session stopped while waiting for sync (reason: ${running.reason ?? "unknown"})`,
      );
    }

    if (timeoutMs > 0 && Date.now() - started > timeoutMs) {
      throw new Error("timed out waiting for scheduler to run sync cycle");
    }

    await wait(POLL_INTERVAL_MS);
  }
}

async function runSyncForSession(
  db: Database,
  sessionDbPath: string,
  ref: string,
  maxCycles: number,
  timeoutMs: number,
  progress: { enabled: boolean; json: boolean } | undefined,
  paths: string[],
): Promise<number> {
  const sessionRow = resolveSessionRow(sessionDbPath, ref);
  if (!sessionRow) {
    throw new Error(`session '${ref}' not found`);
  }
  const label = sessionRow.name ?? String(sessionRow.id);

  const running = checkSessionRunning(db, sessionRow.id);
  if (!running.ok) {
    throw new Error(
      `session ${label} is not running (${running.reason ?? "unknown"})`,
    );
  }

  const progressState: ProgressState | undefined = progress?.enabled
    ? {
        enabled: true,
        json: !!progress.json,
        sessionDbPath,
        sessionId: sessionRow.id,
        lastLogId: 0,
      }
    : undefined;

  for (let attempt = 1; attempt <= maxCycles; attempt += 1) {
    console.log(
      `session ${label}: starting sync attempt ${attempt}/${maxCycles}`,
    );
    const cmdId = enqueueSyncCommand(db, sessionRow.id, attempt, paths);
    await waitForCommandAck(db, sessionRow.id, cmdId, timeoutMs, progressState);
    if (progressState) {
      drainProgressLogs(progressState);
    }

    const planTargets = getPlanTargets(sessionRow);
    const plan = planThreeWayMerge({
      alphaDb: planTargets.alphaDb,
      betaDb: planTargets.betaDb,
      baseDb: planTargets.baseDb,
      prefer: planTargets.prefer,
      strategyName: planTargets.strategy,
      restrictedPaths: paths,
    });

    if (plan.diffs.length === 0 && plan.operations.length === 0) {
      console.log(
        `session ${label} synchronized after ${attempt} ${
          attempt === 1 ? "cycle" : "cycles"
        }`,
      );
      return 0;
    }

    const sample = plan.diffs.slice(0, 5).map((row) => row.path);
    console.log(
      `session ${label}: ${plan.diffs.length} paths still differ (examples: ${
        sample.join(", ") || "n/a"
      })`,
    );

    if (attempt === maxCycles) {
      throw new Error(
        `session ${label}: still has ${plan.diffs.length} differing paths after ${maxCycles} cycles`,
      );
    }
  }

  // Should never reach here.
  return 1;
}

export function registerSessionSync(sessionCmd: Command) {
  sessionCmd
    .command("sync")
    .description(
      "trigger immediate sync cycle(s) and verify no differences remain",
    )
    .argument("<id-or-name...>", "session id(s) or name(s)")
    .option(
      "--session-db <file>",
      "path to sessions database",
      getSessionDbPath(),
    )
    .option(
      "--max-cycles <n>",
      "maximum number of full cycles before failing",
      String(DEFAULT_MAX_CYCLES),
    )
    .option(
      "--timeout <milliseconds>",
      "maximum time to wait for each cycle",
      String(DEFAULT_TIMEOUT_MS),
    )
    .option(
      "--path <path>",
      "restrict sync to a relative path (repeat or comma-separated)",
      collectListOption,
      [] as string[],
    )
    .option("--progress", "stream progress logs while syncing", false)
    .option("--json", "emit progress logs as JSON", false)
    .action(
      async (
        refs: string[],
        opts: {
          sessionDb: string;
          maxCycles?: string;
          timeout?: string;
          progress?: boolean;
          json?: boolean;
          path?: string[];
        },
      ) => {
        const maxCycles = parsePositiveInt(opts.maxCycles, DEFAULT_MAX_CYCLES);
        const timeoutMs = parsePositiveInt(opts.timeout, DEFAULT_TIMEOUT_MS);
        const sessionDbPath = opts.sessionDb ?? getSessionDbPath();
        const progressConfig = opts.progress
          ? { enabled: true, json: !!opts.json }
          : undefined;

        const db = ensureSessionDb(sessionDbPath);
        try {
          for (const ref of refs) {
            const trimmed = ref.trim();
            if (!trimmed) continue;
            try {
              await runSyncForSession(
                db,
                sessionDbPath,
                trimmed,
                maxCycles,
                timeoutMs,
                progressConfig,
                opts.path ?? [],
              );
            } catch (err) {
              const message =
                err instanceof Error
                  ? err.message
                  : String(err ?? "unknown error");
              console.error(message);
              process.exitCode = 1;
              return;
            }
          }
        } finally {
          db.close();
        }
      },
    );
}
