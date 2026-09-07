#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { appendFile, readFile } from "node:fs/promises";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith("--") || value === undefined) {
      throw new Error(
        `invalid release validation argument: ${key ?? "<missing>"}`,
      );
    }
    result[key.slice(2)] = value;
  }
  return result;
}

function git(args) {
  const result = spawnSync("git", args, { cwd: root, encoding: "utf8" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
  }
  return result.stdout.trim();
}

const args = parseArgs(process.argv.slice(2));
const packageMetadata = JSON.parse(
  await readFile(path.join(root, "package.json"), "utf8"),
);
if (
  !/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$/u.test(
    packageMetadata.version,
  )
) {
  throw new Error(
    `package version is not valid semver: ${packageMetadata.version}`,
  );
}
const commit = git(["rev-parse", "HEAD"]);
if (!/^[a-f0-9]{40}$/u.test(commit)) {
  throw new Error("release checkout did not resolve to a full commit SHA");
}
const status = git(["status", "--porcelain"]);
if (status) throw new Error(`release checkout is dirty:\n${status}`);

const tag = args.tag || "";
const createDraft = args["create-draft"] === "true";
if (createDraft && !tag) {
  throw new Error("draft release creation requires --tag");
}
if (tag) {
  const expected = `v${packageMetadata.version}`;
  if (tag !== expected) {
    throw new Error(`release tag ${tag} does not match ${expected}`);
  }
  const tagCommit = git(["rev-list", "-n", "1", tag]);
  if (tagCommit !== commit) {
    throw new Error(`release tag ${tag} points at ${tagCommit}, not ${commit}`);
  }
}

const changelog = await readFile(path.join(root, "CHANGELOG.md"), "utf8");
const escapedVersion = packageMetadata.version.replace(/\./gu, "\\.");
if (
  !new RegExp(`^## \\[${escapedVersion}\\] - \\d{4}-\\d{2}-\\d{2}$`, "mu").test(
    changelog,
  )
) {
  throw new Error(`CHANGELOG.md has no dated ${packageMetadata.version} entry`);
}
const created = new Date(
  git(["show", "-s", "--format=%cI", commit]),
).toISOString();

if (args["github-output"]) {
  await appendFile(
    args["github-output"],
    [
      `version=${packageMetadata.version}`,
      `commit=${commit}`,
      `tag=${tag}`,
      `created=${created}`,
      `create_draft=${createDraft}`,
      "",
    ].join("\n"),
  );
}
console.log(
  JSON.stringify(
    {
      version: packageMetadata.version,
      commit,
      tag: tag || null,
      created,
      createDraft,
    },
    null,
    2,
  ),
);
