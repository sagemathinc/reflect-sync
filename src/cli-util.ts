// src/cli-util.ts
import { Command, type OptionValues } from "commander";
import { fileURLToPath } from "node:url";
import path from "node:path";

function samePath(a: string, b: string) {
  const A = path.resolve(a);
  const B = path.resolve(b);
  // Windows is case-insensitive
  return process.platform === "win32"
    ? A.toLowerCase() === B.toLowerCase()
    : A === B;
}

export function isDirectRun(
  metaUrl: string,
  argv1 = process.argv[1] ?? "",
): boolean {
  if (process.env.REFLECT_BUNDLED) {
    return false;
  }
  try {
    if (!argv1) return false;
    const me = fileURLToPath(metaUrl);
    // allow either raw path or file:// URL in argv[1] (some loaders)
    const inv = argv1.startsWith("file:") ? fileURLToPath(argv1) : argv1;
    return samePath(me, inv);
  } catch {
    return false;
  }
}

/**
 * Minimal CLI bootstrap:
 * - If this module is run directly, parse argv and call `run(opts)`
 * - If imported, do nothing (so caller can call run() directly)
 *
 * Usage:
 *   export function buildProgram(): Command { ... }
 *   export async function runX(opts: XOpts) { ... }
 *   cliEntrypoint(import.meta.url, buildProgram, runX, {label: "scan"});
 */
export function cliEntrypoint<T extends OptionValues>(
  metaUrl: string,
  buildProgram: () => Command,
  run: (opts: T, program: Command) => Promise<void | number>,
  opts?: { label?: string; onError?: (err: unknown) => void },
): void {
  if (!isDirectRun(metaUrl)) return;

  (async () => {
    const program = buildProgram();
    const parsed = program.parse(process.argv);
    const options = parsed.opts<T>();

    try {
      const code = await run(options, program);
      if (typeof code === "number") process.exit(code);
    } catch (err: any) {
      const label = opts?.label || program.name() || "command";
      // Prefer program.error if available (prints usage + exits 1)
      try {
        // Show stack when NODE_DEBUG-style env is on, else concise
        const msg = err?.stack || String(err);
        program.error(`${label} fatal:\n${msg}`);
      } catch {
        console.error(`${label} fatal:`, err?.stack || err);
        if (opts?.onError) opts.onError(err);
        process.exit(1);
      }
    }
  })();
}

/** Handy for tests: run a command with custom argv without process.exit */
export async function parseAndRun<T extends OptionValues>(
  buildProgram: () => Command,
  run: (opts: T, program: Command) => Promise<void | number>,
  argv: string[],
): Promise<number | void> {
  const program = buildProgram();
  const parsed = program.parse(argv, { from: "user" });
  const options = parsed.opts<T>();
  return run(options, program);
}
