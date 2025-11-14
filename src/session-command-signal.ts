import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import type { Logger } from "./logger.js";
import { wait } from "./util.js";

export const COMMAND_SIGNAL_FILE = "command.signal";

export function getCommandSignalPath(baseDb: string): string {
  return path.join(path.dirname(baseDb), COMMAND_SIGNAL_FILE);
}

let touchWarningEmitted = false;
export async function touchCommandSignal(
  baseDb: string,
  logger?: Logger,
): Promise<void> {
  try {
    const signalPath = getCommandSignalPath(baseDb);
    await fsp.mkdir(path.dirname(signalPath), { recursive: true });
    await fsp.writeFile(signalPath, `${Date.now()}`, { flag: "w" });
  } catch (err) {
    if (touchWarningEmitted) return;
    touchWarningEmitted = true;
    const message =
      err instanceof Error ? err.message : String(err ?? "unknown error");
    if (logger?.warn) {
      logger.warn(
        "touchCommandSignal could not notify scheduler (falling back to polling)",
        { error: message, baseDb },
      );
    } else {
      console.warn(
        `touchCommandSignal warning: failed to touch ${baseDb}: ${message}`,
      );
    }
  }
}

export interface CommandSignalHandle {
  wait: (ms: number) => Promise<boolean>;
  close: () => void;
}

export function createCommandSignalWatcher({
  baseDb,
  logger,
}: {
  baseDb: string;
  logger?: Logger;
}): CommandSignalHandle {
  const signalPath = getCommandSignalPath(baseDb);
  let pending = false;
  let resolver: (() => void) | null = null;
  let watcher: fs.FSWatcher | null = null;

  try {
    fs.mkdirSync(path.dirname(signalPath), { recursive: true });
    fs.writeFileSync(signalPath, "", { flag: "a" });
    watcher = fs.watch(
      path.dirname(signalPath),
      { encoding: "utf8" },
      (_event, filename) => {
        if (!filename) return;
        if (filename !== path.basename(signalPath)) return;
        pending = true;
        if (resolver) {
          const resolve = resolver;
          resolver = null;
          resolve();
        }
      },
    );
  } catch (err) {
    if (logger?.warn) {
      logger.warn("command signal watcher disabled", {
        error: err instanceof Error ? err.message : String(err),
        path: signalPath,
      });
    }
    watcher = null;
  }

  async function waitForSignal(ms: number): Promise<boolean> {
    if (!watcher) {
      if (ms > 0) await wait(ms);
      return false;
    }
    if (pending) {
      pending = false;
      return true;
    }
    if (ms <= 0) return false;
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (resolver === onSignal) {
          resolver = null;
        }
        resolve(false);
      }, ms);
      const onSignal = () => {
        clearTimeout(timer);
        if (resolver === onSignal) {
          resolver = null;
        }
        pending = false;
        resolve(true);
      };
      resolver = onSignal;
    });
  }

  function close() {
    if (watcher) {
      try {
        watcher.close();
      } catch {}
      watcher = null;
    }
    if (resolver) {
      resolver();
      resolver = null;
    }
  }

  return { wait: waitForSignal, close };
}
