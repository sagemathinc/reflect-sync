// src/session-sync.ts

import { Command } from "commander";
import { ensureSessionDb, getSessionDbPath, resolveSessionRow } from "./session-db.js";
import type { Database } from "./db.js";

const POLL_INTERVAL_MS = 200;
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_MAX_CYCLES = 3;

type DigestSnapshot = {
  timestamp: number | null;
  alpha: string | null;
  beta: string | null;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function parsePositiveInt(raw: string | number | undefined, fallback: number): number {
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

function getDigestSnapshot(db: Database, sessionId: number): DigestSnapshot {
  const row = db
    .prepare(
      `SELECT last_digest AS timestamp, alpha_digest AS alpha, beta_digest AS beta
         FROM sessions WHERE id = ?`,
    )
    .get(sessionId) as DigestSnapshot | undefined;
  return (
    row ?? {
      timestamp: null,
      alpha: null,
      beta: null,
    }
  );
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

function enqueueSyncCommand(db: Database, sessionId: number, attempt: number): number {
  const ts = Date.now();
  const payload = JSON.stringify({ requested_at: ts, attempt });
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

    const running = checkSessionRunning(db, sessionId);
    if (!running.ok) {
      throw new Error(
        `session stopped while waiting for sync (reason: ${running.reason ?? "unknown"})`,
      );
    }

    if (timeoutMs > 0 && Date.now() - started > timeoutMs) {
      throw new Error("timed out waiting for scheduler to run sync cycle");
    }

    await sleep(POLL_INTERVAL_MS);
  }
}

function formatDigestLabel(alpha: string | null, beta: string | null): string {
  const truncate = (value: string | null) =>
    value ? value.slice(0, 12) + (value.length > 12 ? "..." : "") : "null";
  return `alpha=${truncate(alpha)} beta=${truncate(beta)}`;
}

async function runSyncForSession(
  db: Database,
  sessionDbPath: string,
  ref: string,
  maxCycles: number,
  timeoutMs: number,
): Promise<number> {
  const sessionRow = resolveSessionRow(sessionDbPath, ref);
  if (!sessionRow) {
    throw new Error(`session '${ref}' not found`);
  }
  const label = sessionRow.name ?? String(sessionRow.id);

  const running = checkSessionRunning(db, sessionRow.id);
  if (!running.ok) {
    throw new Error(`session ${label} is not running (${running.reason ?? "unknown"})`);
  }

  let previousDigests: DigestSnapshot | null = null;

  for (let attempt = 1; attempt <= maxCycles; attempt += 1) {
    console.log(
      `session ${label}: starting sync attempt ${attempt}/${maxCycles}`,
    );
    const cmdId = enqueueSyncCommand(db, sessionRow.id, attempt);
    await waitForCommandAck(db, sessionRow.id, cmdId, timeoutMs);

    const snapshot = getDigestSnapshot(db, sessionRow.id);
    const { alpha, beta } = snapshot;
    if (alpha == null || beta == null) {
      throw new Error(
        `session ${label}: missing digest data after attempt ${attempt}`,
      );
    }

    if (alpha === beta) {
      console.log(
        `session ${label} synchronized after ${attempt} ${
          attempt === 1 ? "cycle" : "cycles"
        }`,
      );
      return 0;
    }

    if (
      previousDigests &&
      previousDigests.alpha === alpha &&
      previousDigests.beta === beta
    ) {
      throw new Error(
        `session ${label}: digests unchanged after attempt ${attempt} (${formatDigestLabel(alpha, beta)})`,
      );
    }

    if (attempt === maxCycles) {
      throw new Error(
        `session ${label}: digests still differ after ${maxCycles} cycles (${formatDigestLabel(alpha, beta)})`,
      );
    }

    console.log(
      `session ${label}: digests differ (${formatDigestLabel(alpha, beta)}); retrying`,
    );
    previousDigests = snapshot;
  }

  // Should never reach here.
  return 1;
}

export function registerSessionSync(sessionCmd: Command) {
  sessionCmd
    .command("sync")
    .description(
      "trigger immediate sync cycle(s) and verify digests converge",
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
    .action(
      async (
        refs: string[],
        opts: { sessionDb: string; maxCycles?: string; timeout?: string },
      ) => {
        const maxCycles = parsePositiveInt(opts.maxCycles, DEFAULT_MAX_CYCLES);
        const timeoutMs = parsePositiveInt(opts.timeout, DEFAULT_TIMEOUT_MS);
        const sessionDbPath = opts.sessionDb ?? getSessionDbPath();

        const db = ensureSessionDb(sessionDbPath);
        try {
          for (const ref of refs) {
            const trimmed = ref.trim();
            if (!trimmed) continue;
            try {
              await runSyncForSession(db, sessionDbPath, trimmed, maxCycles, timeoutMs);
            } catch (err) {
              const message =
                err instanceof Error ? err.message : String(err ?? "unknown error");
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
