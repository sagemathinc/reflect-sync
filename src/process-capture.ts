import { spawn } from "node:child_process";

export type CommandResult = {
  command: string;
  args: string[];
  code: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  error?: string;
};

export type CommandRunner = (
  command: string,
  args: string[],
  options?: { timeoutMs?: number; env?: NodeJS.ProcessEnv },
) => Promise<CommandResult>;

export const captureCommand: CommandRunner = async (
  command,
  args,
  options = {},
) => {
  const timeoutMs = options.timeoutMs ?? 5_000;
  return await new Promise<CommandResult>((resolve) => {
    let stdout = "";
    let stderr = "";
    let settled = false;
    let timedOut = false;
    const child = spawn(command, args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: options.env ?? process.env,
      windowsHide: true,
    });
    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");
    child.stdout?.on("data", (chunk) => (stdout += chunk));
    child.stderr?.on("data", (chunk) => (stderr += chunk));

    const finish = (
      code: number | null,
      signal: NodeJS.Signals | null,
      error?: unknown,
    ) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        command,
        args: [...args],
        code,
        signal,
        stdout,
        stderr,
        timedOut,
        error:
          error instanceof Error
            ? error.message
            : error
              ? String(error)
              : undefined,
      });
    };

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);
    timer.unref();

    child.once("error", (error) => finish(null, null, error));
    child.once("close", (code, signal) => finish(code, signal));
  });
};
