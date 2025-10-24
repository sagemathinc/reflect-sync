// session-flush.ts
import { Command } from "commander";
import { ensureSessionDb, getSessionDbPath } from "./session-db.js";

export function registerSessionFlush(sessionCmd: Command) {
  sessionCmd
    .command("flush")
    .description(
      "force a session to converge (drain micro events + full cycles)",
    )
    .argument("<id>", "session id (integer)")
    .option(
      "--session-db <file>",
      "path to sessions database",
      getSessionDbPath(),
    )
    .option("--no-wait", "do not wait for flush to complete", false)
    .option("--timeout <ms>", "max time to wait", "30000")
    .action(
      async (
        idArg: string,
        opts: { sessionDb: string; noWait: boolean; timeout: string },
      ) => {
        const id = Number(idArg);
        if (!Number.isFinite(id)) {
          console.error("session flush: <id> must be a number");
          process.exit(2);
        }
        const timeoutMs = Math.max(0, Number(opts.timeout ?? "30000")) || 30000;

        const db = ensureSessionDb(opts.sessionDb);
        try {
          // ensure session exists
          const ses = db
            .prepare("SELECT id FROM sessions WHERE id = ?")
            .get(id);
          if (!ses) {
            console.error(`session flush: session ${id} not found`);
            process.exit(1);
          }

          const now = Date.now();
          const payload = JSON.stringify({ requested_at: now });

          // push command
          db.prepare(
            `
          INSERT INTO session_commands(session_id, ts, cmd, payload, acked)
          VALUES (?, ?, 'flush', ?, 0)
        `,
          ).run(id, now, payload);

          if (opts.noWait) {
            console.log(`flush requested for session ${id}`);
            return;
          }

          // wait loop: watch session_state for completion
          const t0 = Date.now();
          let printedStart = false;

          while (true) {
            const st = db
              .prepare(
                `
            SELECT
              running,
              flushing,
              last_flush_started_at,
              last_flush_ok,
              last_flush_error,
              last_heartbeat,
              last_cycle_ms,
              backoff_ms
            FROM session_state WHERE session_id = ?
          `,
              )
              .get(id) as any;

            if (st?.last_flush_started_at && !printedStart) {
              printedStart = true;
              console.log("flush started…");
            }

            if (st?.last_flush_ok) {
              console.log("flush complete ✔");
              break;
            }
            if (st?.last_flush_error) {
              console.error("flush failed:", st.last_flush_error);
              process.exit(1);
            }

            if (Date.now() - t0 > timeoutMs) {
              console.error("flush timed out waiting for scheduler");
              process.exit(1);
            }

            await new Promise((r) => setTimeout(r, 200));
          }
        } finally {
          db.close();
        }
      },
    );
}
