// session-monitor.ts
import { Command } from "commander";
import { ensureSessionDb, getSessionDbPath, resolveSessionRow } from "./session-db.js";

export function registerSessionMonitor(sessionCmd: Command) {
  sessionCmd
    .command("monitor")
    .description("monitor a sync session")
    .argument("<id-or-name>", "session id or name")
    .option(
      "--session-db <file>",
      "path to sessions database",
      getSessionDbPath(),
    )
    .option("--json", "output JSON instead of human text", false)
    .action(
      async (ref: string, opts: { sessionDb: string; json: boolean }) => {
        const row = resolveSessionRow(opts.sessionDb, ref.trim());
        if (!row) {
          console.error(`session monitor: session '${ref}' not found`);
          process.exit(1);
        }
        const id = row.id;
        const db = ensureSessionDb(opts.sessionDb);
        try {
          // ensure session exists
          const ses = db
            .prepare("SELECT id FROM sessions WHERE id = ?")
            .get(id);
          if (!ses) {
            console.error(`session monitor: session ${id} not found`);
            process.exit(1);
          }
          console.log("session monitor -- TODO");
        } finally {
          db.close();
        }
      },
    );
}
