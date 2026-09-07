import fs from "node:fs";
import os from "node:os";
import { join } from "node:path";
import { CLI_NAME } from "./constants.js";

function ensureDir(directory: string): string {
  fs.mkdirSync(directory, { recursive: true });
  return directory;
}

function expandHome(value: string): string {
  if (!value) return value;
  if (value.startsWith("~")) {
    return join(os.homedir(), value.slice(1));
  }
  return value;
}

export function getReflectSyncHome(): string {
  const explicit = process.env.REFLECT_HOME?.trim();
  if (explicit) {
    return ensureDir(expandHome(explicit));
  }

  const xdg = process.env.XDG_DATA_HOME;
  if (xdg?.trim()) {
    return ensureDir(join(expandHome(xdg), CLI_NAME));
  }

  const home = os.homedir();
  if (process.platform === "darwin") {
    return ensureDir(join(home, "Library", "Application Support", CLI_NAME));
  }
  if (process.platform === "win32") {
    const appData = process.env.APPDATA || join(home, "AppData", "Roaming");
    return ensureDir(join(appData, CLI_NAME));
  }
  return ensureDir(join(home, ".local", "share", CLI_NAME));
}
