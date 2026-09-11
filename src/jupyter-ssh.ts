import { glob, readFile, realpath } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import SSHConfig, { LineType, type Line } from "ssh-config";

// Enumerate names only. OpenSSH, not this parser, evaluates Host/Match settings
// when connecting. In particular, enumeration must not execute Match exec.
export async function jupyterSshAliases(
  files = [join(homedir(), ".ssh/config"), "/etc/ssh/ssh_config"],
): Promise<{ aliases: string[]; warnings: string[] }> {
  const aliases = new Set<string>();
  const warnings: string[] = [];
  const seen = new Set<string>();
  async function visit(file: string, base: string, depth = 0) {
    if (depth > 16 || seen.size >= 256) {
      warnings.push("SSH Include limit reached");
      return;
    }
    try {
      const canonical = await realpath(file);
      if (seen.has(canonical)) return;
      seen.add(canonical);
      async function walk(lines: Line[]) {
        for (const line of lines) {
          if (line.type !== LineType.DIRECTIVE) continue;
          const values =
            typeof line.value === "string"
              ? [line.value]
              : line.value.map((x) => x.val);
          if (line.param.toLowerCase() === "host") {
            for (const value of values) {
              if (value && !/[*!?\[\]\s]/.test(value) && !value.startsWith("-"))
                aliases.add(value);
            }
          } else if (line.param.toLowerCase() === "include") {
            for (let pattern of values) {
              if (pattern.startsWith("~/"))
                pattern = join(homedir(), pattern.slice(2));
              if (!isAbsolute(pattern)) pattern = join(base, pattern);
              const matches: string[] = [];
              for await (const match of glob(pattern)) matches.push(match);
              for (const match of matches.sort())
                await visit(match, base, depth + 1);
            }
          }
          if ("config" in line) await walk(line.config as Line[]);
        }
      }
      await walk(SSHConfig.parse(await readFile(file, "utf8")));
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT")
        warnings.push(`${file}: ${String(err)}`);
    }
  }
  for (const file of files) await visit(file, dirname(file));
  return { aliases: [...aliases].sort(), warnings };
}
