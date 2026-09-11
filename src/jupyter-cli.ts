import type { Command } from "commander";
import { table, output, batch } from "./cli-output.js";
import {
  defaultLauncherArgv,
  jupyterSessionCommand,
  launchJupyter,
  listJupyterEnvironments,
  listJupyterTargets,
  listJupyterSessions,
  removeJupyterTarget,
  removeJupyterSession,
  prepareJupyter,
  registerJupyterTarget,
  probeJupyter,
  existingJupyterKernel,
  assertJupyterNameAvailable,
} from "./jupyter.js";
import { jupyterSshAliases } from "./jupyter-ssh.js";

export function registerJupyterCommands(program: Command): void {
  const jupyter = program
    .command("jupyter")
    .description("Manage standard Jupyter kernels running over SSH");
  const targets = jupyter
    .command("target")
    .description("Manage reusable kernel registrations");
  const environments = jupyter
    .command("environment")
    .description("Manage remote Python environments");
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
    .command("discover")
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
  targets
    .command("list")
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
    .argument("[id...]", "local IDs or remote UUIDs")
    .description(
      "List recorded kernel sessions (use status to check remote state)",
    )
    .option("--json", "emit JSON instead of a table")
    .action(async (refs: string[], opts) => {
      const all = await listJupyterSessions();
      for (const ref of refs)
        if (!all.some((row) => String(row.id) === ref || row.session === ref))
          throw Error("Jupyter session not found: " + ref);
      const result = refs.length
        ? all.filter(
            (row) =>
              refs.includes(String(row.id)) || refs.includes(row.session),
          )
        : all;
      if (opts.json) output(result, true, "Jupyter Sessions");
      else
        process.stdout.write(
          table(
            "Jupyter Sessions",
            ["ID", "Target", "Host", "State"],
            result.map((session) => [
              String(session.id),
              session.target,
              session.host,
              session.stopped ? "stopped" : "unverified",
            ]),
          ) + "\n",
        );
    });
  targets
    .command("remove")
    .option("--json", "emit JSON instead of a table")
    .argument("<name>", "target name")
    .option("--stop", "stop active kernels before removing registration")
    .action(async (name: string, opts) => {
      await removeJupyterTarget(name, opts.stop);
      output({ removed: name }, opts.json, "Removed Jupyter Target");
    });
  targets
    .command("add")
    .option("--json", "emit JSON instead of a table")
    .argument("<name>", "target name")
    .requiredOption("--host <host>")
    .option("--environment <name>", "managed environment", "teaching")
    .option("--python <path>", "existing remote interpreter (no installation)")
    .option("--kernel <path>", "existing remote kernel.json (any language)")
    .option("--uv <path>", "local bootstrap executable override")
    .option("--recipe <recipe>", "python or pytorch-cu128", "python")
    .action(async (name: string, opts) => {
      opts.target = name;
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
  environments
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
  environments
    .command("list")
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
  jupyter
    .command("remove")
    .argument("<id...>", "local IDs or remote UUIDs")
    .option("--stop", "stop active kernels before removing their records")
    .option(
      "--force",
      "forget local records without contacting the remote; kernels may remain running",
    )
    .option("--json", "emit JSON instead of human text")
    .action(async (refs: string[], opts) => {
      await batch(refs, opts.json, async (ref) => {
        await removeJupyterSession(ref, opts.stop, opts.force);
        return opts.force
          ? {
              removed: ref,
              warning:
                "Local record forgotten; remote kernel shutdown was not confirmed",
            }
          : { removed: ref };
      });
    });
}
