import { Command } from "commander";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";
import { ensureSessionDb, getSessionDbPath, loadForwardById } from "./session-db.js";
import { createForward, listForwards, terminateForward } from "./forward-manage.js";
import { ConsoleLogger } from "./logger.js";

function resolveSessionDb(command: Command, opts: { sessionDb?: string }): string {
  const ensure = (path: string) => {
    const db = ensureSessionDb(path);
    db.close();
    return path;
  };
  if (opts.sessionDb) return ensure(opts.sessionDb);
  const globals = command.optsWithGlobals() as { sessionDb?: string };
  if (globals.sessionDb) return ensure(globals.sessionDb);
  return ensure(getSessionDbPath());
}

export function registerForwardCommands(program: Command) {
  const forward = program
    .command("forward")
    .description("Manage SSH port forwards");

  forward
    .command("create")
    .description("Create an SSH port forward")
    .argument("<left>", "left endpoint (host:port or :port)")
    .argument("<right>", "right endpoint (user@host[:sshPort]:port or host:port)")
    .option("-n, --name <name>", "friendly name")
    .option("--compress", "enable SSH compression", false)
    .option("--session-db <file>", "override path to sessions.db", getSessionDbPath())
    .action(async (left: string, right: string, opts: any, command: Command) => {
      const sessionDb = resolveSessionDb(command, opts);
      const root = command.parent?.parent ?? command.parent ?? command;
      const level = (root.optsWithGlobals?.() as any)?.logLevel ?? "info";
      const logger = new ConsoleLogger(level);
      try {
        const id = createForward({
          sessionDb,
          name: opts.name,
          left,
          right,
          compress: !!opts.compress,
          logger,
        });
        console.log(`created forward ${id}${opts.name ? ` (${opts.name})` : ""}`);
      } catch (err) {
        console.error(`failed to create forward: ${(err as Error).message}`);
        process.exitCode = 1;
      }
    });

  forward
    .command("list")
    .description("List SSH port forwards")
    .option("--session-db <file>", "override path to sessions.db", getSessionDbPath())
    .action((opts: any, command: Command) => {
      const sessionDb = resolveSessionDb(command, opts);
      const rows = listForwards(sessionDb);
      if (!rows.length) {
        console.log("no forwards");
        return;
      }
      const table = new AsciiTable3("Forwards")
        .setHeading("ID", "Name", "Direction", "Local", "Remote", "SSH", "PID", "State", "Command")
        .setStyle("unicode-round");
      [0, 1, 2, 3, 4, 5, 6, 7, 8].forEach((idx) =>
        table.setAlign(idx, AlignmentEnum.LEFT),
      );
      for (const row of rows) {
        const local = `${row.local_host}:${row.local_port}`;
        const ssh = `${row.ssh_host}${row.ssh_port ? `:${row.ssh_port}` : ""}`;
        const remote =
          row.direction === "local_to_remote"
            ? `${ssh}:${row.remote_port}`
            : `${ssh}:${row.remote_port}`;
        table.addRow(
          String(row.id),
          row.name ?? "-",
          row.direction === "local_to_remote" ? "local->remote" : "remote->local",
          local,
          remote,
          ssh,
          row.monitor_pid ? String(row.monitor_pid) : "-",
          row.actual_state,
          row.ssh_args ?? "-",
        );
      }
      console.log(table.toString());
    });

  forward
    .command("terminate")
    .description("Terminate a forward")
    .argument("<id>", "forward id")
    .option("--session-db <file>", "override path to sessions.db", getSessionDbPath())
    .action((idArg: string, opts: any, command: Command) => {
      const sessionDb = resolveSessionDb(command, opts);
      const id = Number(idArg);
      if (!Number.isInteger(id) || id <= 0) {
        console.error("forward terminate: id must be an integer");
        process.exit(1);
      }
      const row = loadForwardById(sessionDb, id);
      if (!row) {
        console.error(`forward '${id}' not found`);
        process.exit(1);
      }
      terminateForward(sessionDb, id);
      console.log(`terminated forward ${row.name ?? id}`);
    });
}
