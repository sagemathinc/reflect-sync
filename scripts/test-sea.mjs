#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
let executable = process.argv[2];
if (!executable) {
  const directory = path.join(root, "dist", "sea");
  const candidate = (await readdir(directory)).find(
    (file) => file.startsWith("reflect-sync-") && !file.endsWith(".json"),
  );
  if (!candidate) throw new Error("no SEA executable found under dist/sea");
  executable = path.join(directory, candidate);
}
executable = path.resolve(executable);
const temporary = await mkdtemp(path.join(os.tmpdir(), "reflect-sea-smoke-"));
const environment = {
  ...process.env,
  REFLECT_HOME: path.join(temporary, "state"),
};

function run(args, allowed = [0]) {
  const result = spawnSync(executable, args, {
    encoding: "utf8",
    env: environment,
  });
  if (result.error) throw result.error;
  if (!allowed.includes(result.status)) {
    throw new Error(
      `${executable} ${args.join(" ")} exited ${result.status}\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result;
}

try {
  const packageMetadata = JSON.parse(
    await readFile(path.join(root, "package.json"), "utf8"),
  );
  const version = run(["--version"]);
  if (version.stdout.trim() !== packageMetadata.version) {
    throw new Error(`SEA version mismatch: ${version.stdout.trim()}`);
  }
  const help = run(["--help"]);
  for (const command of ["doctor", "session", "forward"]) {
    if (!help.stdout.includes(command))
      throw new Error(`SEA help omits ${command}`);
  }
  const doctor = run(["doctor", "--json"], [0, 1]);
  const report = JSON.parse(doctor.stdout);
  if (report.reflectSync?.version !== packageMetadata.version) {
    throw new Error("SEA doctor report has the wrong ReflectSync version");
  }
  if (!report.reflectSync?.bundled) {
    throw new Error("SEA doctor report does not identify a bundled executable");
  }

  const scanRoot = path.join(temporary, "scan-root");
  const scanDatabase = path.join(temporary, "scan.db");
  await mkdir(scanRoot);
  await writeFile(
    path.join(scanRoot, "sea-smoke.txt"),
    "SEA functional smoke\n",
  );
  run(["sync", "scan", "--root", scanRoot, "--db", scanDatabase]);
  if ((await stat(scanDatabase)).size === 0) {
    throw new Error("SEA scan did not create a database");
  }

  const identity = spawnSync("file", [executable], { encoding: "utf8" });
  if (identity.status !== 0) throw new Error(`file failed: ${identity.stderr}`);
  if (process.platform === "linux") {
    if (!/ELF 64-bit/u.test(identity.stdout)) {
      throw new Error(`unexpected Linux SEA identity: ${identity.stdout}`);
    }
    const links = spawnSync("ldd", [executable], { encoding: "utf8" });
    if (links.status !== 0 || /not found/u.test(links.stdout)) {
      throw new Error(`Linux SEA has unresolved libraries: ${links.stdout}`);
    }
  } else if (process.platform === "darwin" && !/arm64/u.test(identity.stdout)) {
    throw new Error(`unexpected macOS SEA identity: ${identity.stdout}`);
  }
  console.log(`SEA smoke test passed: ${executable}`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}
