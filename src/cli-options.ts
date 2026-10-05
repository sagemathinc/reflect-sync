import type { Command } from "commander";

// Nearest explicit value wins; a root default must not mask a child option.
export function inheritedOption<T>(
  command: Command,
  key: string,
  fallback: T,
): T {
  for (
    let current: Command | null = command;
    current;
    current = current.parent
  ) {
    const value = current.getOptionValue(key);
    if (value !== undefined && current.getOptionValueSource(key) !== "default")
      return value as T;
  }
  return fallback;
}
