import { Command } from "commander";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";
import {
  ensureSessionDb,
  getSessionDbPath,
  resolveForwardRow,
  type ForwardRow,
} from "./session-db.js";
import {
  createForward,
  listForwards,
  stopForward,
  startForward,
  removeForward,
} from "./forward-manage.js";
import { batch, output } from "./cli-output.js";
import { isProcessAlive } from "./process-lifecycle.js";
import { inheritedOption } from "./cli-options.js";
import { withResourceLock } from "./resource-lock.js";
import { ConsoleLogger } from "./logger.js";
import { ensureDaemonRunning } from "./session-daemon.js";

function resolveSessionDb(
  command: Command,
  opts: { sessionDb?: string },
): string {
  const ensure = (path: string) => {
    const db = ensureSessionDb(path);
    db.close();
    return path;
  };
  if (opts.sessionDb) return ensure(opts.sessionDb);
  return ensure(inheritedOption(command, "sessionDb", getSessionDbPath()));
}

export function registerForwardCommands(program: Command) {
  const forward = program
    .command("forward")
    .description("Manage SSH port forwards")
    .option("--session-db <file>", "override path to sessions.db")
    .option("--log-level <level>", "log verbosity");

  forward
    .command("create")
    .option("--json", "emit JSON instead of human text")
    .option("--stopped", "create without starting", false)
    .description("Create an SSH port forward")
    .argument("<left>", "left endpoint (host:port or :port)")
    .argument(
      "<right>",
      "right endpoint (user@host[:sshPort]:port or host:port)",
    )
    .option("-n, --name <name>", "friendly name")
    .option("--compress", "enable SSH compression", false)
    .option("--session-db <file>", "override path to sessions.db")
    .action(
      async (left: string, right: string, opts: any, command: Command) => {
        const sessionDb = resolveSessionDb(command, opts);
        const level = inheritedOption(command, "logLevel", "info");
        const logger = new ConsoleLogger(level);
        try {
          const id = await createForward({
            sessionDb,
            name: opts.name,
            left,
            right,
            compress: !!opts.compress,
            stopped: !!opts.stopped,
            logger,
          });
          ensureDaemonRunning(sessionDb, logger.child("daemon"));
          output(
            {
              id,
              name: opts.name ?? null,
              desired_state: opts.stopped ? "stopped" : "running",
            },
            opts.json,
            "Forward Created",
          );
        } catch (err) {
          console.error(`failed to create forward: ${(err as Error).message}`);
          process.exitCode = 1;
        }
      },
    );

  forward
    .command("list")
    .description("List SSH port forwards")
    .argument("[id-or-name...]", "forward id(s) or name(s) to list")
    .option("--session-db <file>", "override path to sessions.db")
    .option("--json", "emit JSON instead of a table", false)
    .action((refs: string[], opts: any, command: Command) => {
      const sessionDb = resolveSessionDb(command, opts);
      const explicitRefs = Array.isArray(refs) ? refs.filter(Boolean) : [];

      let rows: ForwardRow[];
      if (explicitRefs.length) {
        const seen = new Set<string>();
        const selected: ForwardRow[] = [];
        let hadError = false;
        for (const ref of explicitRefs) {
          if (!ref || seen.has(ref)) continue;
          seen.add(ref);
          const row = resolveForwardRow(sessionDb, ref);
          if (!row) {
            console.error(`forward '${ref}' not found`);
            process.exitCode = 1;
            hadError = true;
            continue;
          }
          selected.push(row);
        }
        if (hadError && !selected.length) return;
        rows = selected;
      } else {
        rows = listForwards(sessionDb);
      }

      rows = rows.map((row) => {
        if (row.actual_state === "running" && row.monitor_pid) {
          if (!isProcessAlive(row.monitor_pid)) {
            return {
              ...row,
              actual_state: "error",
            };
          }
        }
        return row;
      });

      if (!rows.length) {
        if (opts.json) {
          console.log("[]");
        } else {
          console.log("no forwards");
        }
        return;
      }
      if (opts.json) {
        console.log(JSON.stringify(rows, null, 2));
        return;
      }
      const table = new AsciiTable3("Forwards")
        .setHeading(
          "ID",
          "Name",
          "Direction",
          "Local",
          "Remote",
          "SSH",
          "PID",
          "State",
          "Command",
        )
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
          row.direction === "local_to_remote"
            ? "local->remote"
            : "remote->local",
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
    .command("status")
    .argument("<id-or-name>", "forward ID or name")
    .option("--session-db <file>", "override path to sessions.db")
    .option("--json", "emit JSON instead of human text")
    .action((ref: string, opts, command: Command) => {
      const row = resolveForwardRow(resolveSessionDb(command, opts), ref);
      if (!row) throw Error(`Forward '${ref}' not found`);
      output(
        {
          ...row,
          actual_state: isProcessAlive(row.monitor_pid)
            ? "running"
            : row.desired_state === "stopped"
              ? "stopped"
              : "error",
        },
        opts.json,
        "Forward Status",
      );
    });
  for (const operation of ["start", "stop", "restart", "remove"] as const) {
    const cmd = forward
      .command(operation)
      .argument("<id-or-name...>", "forward IDs or names")
      .option("--session-db <file>", "override path to sessions.db")
      .option("--json", "emit JSON instead of human text");
    if (operation === "remove")
      cmd.option("--stop", "stop active work before removal");
    cmd.action(async (refs: string[], opts, command: Command) => {
      const sessionDb = resolveSessionDb(command, opts);
      await batch(refs, opts.json, async (ref) => {
        const selected = resolveForwardRow(sessionDb, ref);
        if (!selected) throw Error(`Forward '${ref}' not found`);
        return await withResourceLock(
          sessionDb,
          "forward",
          selected.id,
          async () => {
            const row = resolveForwardRow(sessionDb, String(selected.id));
            if (!row) throw Error(`Forward '${ref}' not found`);
            if (operation === "remove") {
              await removeForward(sessionDb, row.id, opts.stop);
              return { id: row.id, removed: true };
            }
            if (operation === "stop" || operation === "restart")
              await stopForward(sessionDb, row.id);
            if (operation === "start" || operation === "restart") {
              await startForward(sessionDb, row.id);
              ensureDaemonRunning(
                sessionDb,
                new ConsoleLogger(inheritedOption(command, "logLevel", "info")),
              );
            }
            const current = resolveForwardRow(sessionDb, String(row.id))!;
            return {
              id: row.id,
              state: isProcessAlive(current.monitor_pid)
                ? "running"
                : current.desired_state === "stopped"
                  ? "stopped"
                  : "error",
              pid: current.monitor_pid,
            };
          },
        );
      });
    });
  }
}
