#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const temp = await mkdtemp(path.join(os.tmpdir(), "reflect-package-test-"));
const command = process.platform === "win32" ? "pnpm.cmd" : "pnpm";

function run(executable, args, cwd = root) {
  const result = spawnSync(executable, args, {
    cwd,
    encoding: "utf8",
    env: { ...process.env, npm_config_ignore_scripts: "true" },
  });
  if (result.status !== 0) {
    throw new Error(
      `${executable} ${args.join(" ")} failed\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result.stdout;
}

try {
  run(command, ["pack", "--pack-destination", temp]);
  const archive = (await readdir(temp)).find((file) => file.endsWith(".tgz"));
  if (!archive) throw new Error("pnpm pack did not produce a tarball");
  const tarball = path.join(temp, archive);
  const listing = run("tar", ["-tzf", tarball]).trim().split("\n");
  const required = [
    "package/bin/reflect-sync.mjs",
    "package/dist/bundle.mjs",
    "package/dist/index.js",
    "package/dist/index.d.ts",
    "package/LICENSE.txt",
    "package/README.md",
    "package/docs/cli-contract.md",
    "package/docs/jupyter.md",
    "package/CHANGELOG.md",
    "package/RELEASING.md",
  ];
  for (const file of required) {
    if (!listing.includes(file)) throw new Error(`package is missing ${file}`);
  }
  const forbidden = listing.find(
    (file) =>
      file.includes("node_modules/") ||
      file.endsWith("reflect-sync.blob") ||
      file.endsWith("reflect-sync.xz") ||
      file === "package/dist/reflect-sync",
  );
  if (forbidden)
    throw new Error(`package contains generated artifact ${forbidden}`);

  await writeFile(
    path.join(temp, "package.json"),
    JSON.stringify({ private: true, type: "module" }),
  );
  run(command, ["add", "--ignore-scripts", tarball], temp);
  await writeFile(
    path.join(temp, "consumer.mjs"),
    "import * as api from 'reflect-sync';\nif (!api || typeof api !== 'object') throw new Error('ESM import failed');\n",
  );
  await writeFile(
    path.join(temp, "consumer.ts"),
    "import * as api from 'reflect-sync';\nvoid api;\n",
  );
  await writeFile(
    path.join(temp, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        module: "NodeNext",
        moduleResolution: "NodeNext",
        strict: true,
        noEmit: true,
        types: ["node"],
        typeRoots: [path.join(root, "node_modules/@types")],
      },
      files: ["consumer.ts"],
    }),
  );
  run(process.execPath, [path.join(temp, "consumer.mjs")], temp);
  run(command, ["exec", "tsc", "-p", path.join(temp, "tsconfig.json")], root);

  const binDirectory = path.join(temp, "node_modules/.bin");
  const binSuffix = process.platform === "win32" ? ".cmd" : "";
  for (const bin of ["reflect", "reflect-sync"]) {
    const version = run(
      path.join(binDirectory, `${bin}${binSuffix}`),
      ["--version"],
      temp,
    );
    if (!version.trim()) throw new Error(`${bin} --version returned no output`);
  }

  const metadata = JSON.parse(
    await readFile(
      path.join(temp, "node_modules/reflect-sync/package.json"),
      "utf8",
    ),
  );
  if (metadata.name !== "reflect-sync")
    throw new Error("installed wrong package");
} finally {
  await rm(temp, { recursive: true, force: true });
}
