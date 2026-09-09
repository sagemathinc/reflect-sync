import type { Command } from "commander";
import {
  defaultLauncherArgv,
  jupyterSessionCommand,
  launchJupyter,
  listJupyterEnvironments,
  listJupyterTargets,
  listJupyterSessions,
  prepareJupyter,
  registerJupyterTarget,
} from "./jupyter.js";

export function registerJupyterCommands(program: Command): void {
  const jupyter = program
    .command("jupyter")
    .description("Manage standard Jupyter kernels running over SSH");
  jupyter.command("targets").action(async () => {
    process.stdout.write(JSON.stringify(await listJupyterTargets()) + "\n");
  });
  jupyter.command("sessions").action(async () => {
    process.stdout.write(JSON.stringify(await listJupyterSessions()) + "\n");
  });
  jupyter
    .command("setup")
    .requiredOption("--target <name>")
    .requiredOption("--host <host>")
    .option("--environment <name>", "managed environment", "teaching")
    .option("--python <path>", "existing remote interpreter (no installation)")
    .option("--uv <path>", "local bootstrap executable override")
    .option("--recipe <recipe>", "python or pytorch-cu128", "python")
    .action(async (opts) => {
      const target = opts.python
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
      process.stdout.write(
        JSON.stringify({ kernel: `reflect-${opts.target}`, path, ...target }) +
          "\n",
      );
    });
  jupyter
    .command("prepare")
    .requiredOption("--host <host>", "SSH destination or alias")
    .requiredOption("--environment <name>", "isolated Python environment name")
    .option(
      "--uv <path>",
      "local uv executable matching the remote Linux architecture",
    )
    .option("--recipe <recipe>", "python or pytorch-cu128", "python")
    .action(async (opts) => {
      process.stdout.write(
        JSON.stringify(
          await prepareJupyter(
            opts.host,
            opts.environment,
            opts.uv,
            opts.recipe,
          ),
        ) + "\n",
      );
    });
  jupyter
    .command("kernels")
    .requiredOption("--host <host>")
    .action(async (opts) => {
      process.stdout.write(
        JSON.stringify(await listJupyterEnvironments(opts.host)) + "\n",
      );
    });
  jupyter
    .command("register")
    .requiredOption("--target <name>")
    .requiredOption("--host <host>")
    .requiredOption("--environment <name>")
    .requiredOption("--python <path>", "absolute remote Python interpreter")
    .action(async (opts) => {
      process.stdout.write(
        (await registerJupyterTarget(
          opts.target,
          {
            host: opts.host,
            environment: opts.environment,
            python: opts.python,
          },
          defaultLauncherArgv(),
        )) + "\n",
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
      .argument("<session>")
      .action(async (session) => {
        process.stdout.write(
          JSON.stringify(await jupyterSessionCommand(session, operation)) +
            "\n",
        );
      });
  }
}
