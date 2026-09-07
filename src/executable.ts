import fs from "node:fs";
import path from "node:path";

export function findExecutable(
  name: string,
  env: NodeJS.ProcessEnv = process.env,
): string | null {
  if (name.includes("/") || name.includes("\\")) {
    return isExecutable(name) ? path.resolve(name) : null;
  }
  const directories = (env.PATH ?? "").split(path.delimiter).filter(Boolean);
  const extensions =
    process.platform === "win32"
      ? (env.PATHEXT ?? ".EXE;.CMD;.BAT;.COM").split(";")
      : [""];
  for (const directory of directories) {
    for (const extension of extensions) {
      const candidate = path.join(directory, `${name}${extension}`);
      if (isExecutable(candidate)) return candidate;
    }
  }
  return null;
}

function isExecutable(file: string): boolean {
  try {
    fs.accessSync(file, fs.constants.X_OK);
    return fs.statSync(file).isFile();
  } catch {
    return false;
  }
}
