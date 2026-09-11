import type { Command } from "commander";
import { AsciiTable3, AlignmentEnum } from "ascii-table3";
import {
  defaultLauncherArgv,
  jupyterSessionCommand,
  launchJupyter,
  listJupyterEnvironments,
  listJupyterTargets,
  listJupyterSessions,
  removeJupyterTarget,
  prepareJupyter,
  registerJupyterTarget,
  probeJupyter,
  existingJupyterKernel,
  assertJupyterNameAvailable,
} from "./jupyter.js";
import { jupyterSshAliases } from "./jupyter-ssh.js";

function table(title: string, headings: string[], rows: string[][]): string {
  if (!rows.length) return `No ${title.toLowerCase()}.`;
  const result = new AsciiTable3(title)
    .setHeading(...headings)
    .setStyle("unicode-round");
  headings.forEach((_, index) => result.setAlign(index, AlignmentEnum.LEFT));
  for (const row of rows) result.addRow(...row);
  return result.toString();
}

function fields(value: unknown, prefix = ""): string[][] {
  if (value !== null && typeof value === "object") {
    return Object.entries(value).flatMap(([key, item]) =>
      fields(item, prefix ? `${prefix}.${key}` : key),
    );
  }
  return [[prefix, value == null ? "-" : String(value)]];
}

function output(value: unknown, json: boolean, title: string): void {
  process.stdout.write(
    (json
      ? JSON.stringify(value, null, 2)
      : table(title, ["Field", "Value"], fields(value))) + "\n",
  );
}

export function registerJupyterCommands(program: Command): void {
  const jupyter = program
    .command("jupyter")
    .description("Manage standard Jupyter kernels running over SSH");
  jupyter
    .command("ssh-targets")
    .option("--json", "emit JSON instead of a table")
    .action(async (opts) => {
      const result = await jupyterSshAliases();
      if (opts.json) output(result, true, "SSH Targets");
      else {
        process.stdout.write(
          table(
            "SSH Targets",
            ["Alias"],
            result.aliases.map((alias) => [alias]),
          ) + "\n",
        );
        for (const warning of result.warnings)
          process.stderr.write(warning + "\n");
      }
    });
  jupyter
    .command("probe")
    .requiredOption("--host <host>")
    .option("--json", "emit JSON instead of a table")
    .option(
      "--trust-new-host",
      "trust a previously unknown SSH key; reject changed keys",
    )
    .option(
      "--search-path <paths...>",
      "additional remote kernelspec directories",
    )
    .action(async (opts) => {
      const result = await probeJupyter(
        opts.host,
        opts.searchPath,
        opts.trustNewHost,
      );
      if (opts.json) output(result, true, "Remote Discovery");
      else {
        output(
          {
            platform: result.platform,
            gpu: result.gpu,
            suggested_name: result.suggested_name,
          },
          false,
          "Remote Discovery",
        );
        process.stdout.write(
          table(
            "Remote Kernels",
            ["Name", "Language", "Kernelspec"],
            result.kernels.map((kernel) => [
              kernel.display_name,
              kernel.language,
              kernel.id,
            ]),
          ) + "\n",
        );
        for (const warning of result.warnings)
          process.stderr.write(warning + "\n");
      }
    });
  jupyter
    .command("targets")
    .option("--json", "emit JSON instead of a table")
    .action(async (opts) => {
      const result = await listJupyterTargets();
      if (opts.json) output(result, true, "Jupyter Targets");
      else
        process.stdout.write(
          table(
            "Jupyter Targets",
            ["Name", "Host", "Environment", "State"],
            result.map((target) => [
              target.name,
              target.host,
              target.environment,
              target.disabled ? "disabled" : "enabled",
            ]),
          ) + "\n",
        );
    });
  jupyter
    .command("list")
    .alias("sessions")
    .description(
      "List recorded kernel sessions (use status to check remote state)",
    )
    .option("--json", "emit JSON instead of a table")
    .action(async (opts) => {
      const result = await listJupyterSessions();
      if (opts.json) output(result, true, "Jupyter Sessions");
      else
        process.stdout.write(
          table(
            "Jupyter Sessions",
            ["ID", "Target", "Host", "State"],
            result.map((session) => [
              session.session,
              session.target,
              session.host,
              session.stopped ? "stopped" : "unverified",
            ]),
          ) + "\n",
        );
    });
  jupyter
    .command("remove")
    .option("--json", "emit JSON instead of a table")
    .requiredOption("--target <name>")
    .action(async (opts) => {
      await removeJupyterTarget(opts.target);
      output({ removed: opts.target }, opts.json, "Removed Jupyter Target");
    });
  jupyter
    .command("setup")
    .option("--json", "emit JSON instead of a table")
    .requiredOption("--target <name>")
    .requiredOption("--host <host>")
    .option("--environment <name>", "managed environment", "teaching")
    .option("--python <path>", "existing remote interpreter (no installation)")
    .option("--kernel <path>", "existing remote kernel.json (any language)")
    .option("--uv <path>", "local bootstrap executable override")
    .option("--recipe <recipe>", "python or pytorch-cu128", "python")
    .action(async (opts) => {
      if (opts.python && opts.kernel)
        throw Error("Choose a Python interpreter or a kernelspec, not both");
      await assertJupyterNameAvailable(opts.target);
      const target = opts.kernel
        ? await existingJupyterKernel(opts.host, opts.kernel)
        : opts.python
          ? {
              host: opts.host,
              python: opts.python,
              environment: opts.environment,
            }
          : await prepareJupyter(
              opts.host,
              opts.environment,
              opts.uv,
              opts.recipe,
            );
      const path = await registerJupyterTarget(
        opts.target,
        target,
        defaultLauncherArgv(),
      );
      output(
        {
          kernel: `reflect-${opts.target}`,
          path,
          host: target.host,
          environment: target.environment,
        },
        opts.json,
        "Jupyter Kernel Ready",
      );
    });
  jupyter
    .command("prepare")
    .option("--json", "emit JSON instead of a table")
    .requiredOption("--host <host>", "SSH destination or alias")
    .requiredOption("--environment <name>", "isolated Python environment name")
    .option(
      "--uv <path>",
      "local uv executable matching the remote Linux architecture",
    )
    .option("--recipe <recipe>", "python or pytorch-cu128", "python")
    .action(async (opts) => {
      output(
        await prepareJupyter(opts.host, opts.environment, opts.uv, opts.recipe),
        opts.json,
        "Jupyter Environment Ready",
      );
    });
  jupyter
    .command("kernels")
    .option("--json", "emit JSON instead of a table")
    .requiredOption("--host <host>")
    .action(async (opts) => {
      output(
        await listJupyterEnvironments(opts.host),
        opts.json,
        "Managed Jupyter Environments",
      );
    });
  jupyter
    .command("register")
    .option("--json", "emit JSON instead of a table")
    .requiredOption("--target <name>")
    .requiredOption("--host <host>")
    .requiredOption("--environment <name>")
    .requiredOption("--python <path>", "absolute remote Python interpreter")
    .action(async (opts) => {
      output(
        {
          path: await registerJupyterTarget(
            opts.target,
            {
              host: opts.host,
              environment: opts.environment,
              python: opts.python,
            },
            defaultLauncherArgv(),
          ),
        },
        opts.json,
        "Registered Jupyter Target",
      );
    });
  jupyter
    .command("launch")
    .requiredOption("--target <name>")
    .requiredOption("--connection-file <path>")
    .option(
      "--lease-seconds <seconds>",
      "remote orphan cleanup grace period",
      "60",
    )
    .action(async (opts) => {
      await launchJupyter(
        opts.target,
        opts.connectionFile,
        Number(opts.leaseSeconds),
      );
    });
  for (const operation of ["status", "interrupt", "stop"] as const) {
    jupyter
      .command(operation)
      .option("--json", "emit JSON instead of a table")
      .argument("<session>")
      .action(async (session, opts) => {
        output(
          await jupyterSessionCommand(session, operation),
          opts.json,
          "Jupyter Session",
        );
      });
  }
}
