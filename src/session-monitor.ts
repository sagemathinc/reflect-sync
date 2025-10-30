// session-monitor.ts
import { Command } from "commander";
import { ensureSessionDb, getSessionDbPath } from "./session-db.js";

export function registerSessionMonitor(sessionCmd: Command) {
  sessionCmd
    .command("monitor")
    .description("monitor a sync session")
    .argument("<id>", "session id (integer)")
    .option(
      "--session-db <file>",
      "path to sessions database",
      getSessionDbPath(),
    )
    .option("--json", "output JSON instead of human text", false)
    .action(
      async (idArg: string, opts: { sessionDb: string; json: boolean }) => {
        const id = Number(idArg);
        if (!Number.isFinite(id)) {
          console.error("session monitor: <id> must be a number");
          process.exit(2);
        }
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
