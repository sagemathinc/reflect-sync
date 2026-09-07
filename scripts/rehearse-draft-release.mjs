#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { verifyReleaseDirectory } from "./release-verification.mjs";

const root = path.resolve(import.meta.dirname, "..");

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith("--") || value === undefined) {
      throw new Error(`invalid draft-release argument: ${key ?? "<missing>"}`);
    }
    result[key.slice(2)] = value;
  }
  return result;
}

function gh(args, { allowFailure = false } = {}) {
  const result = spawnSync("gh", args, {
    cwd: root,
    encoding: "utf8",
    env: process.env,
  });
  if (result.error) throw result.error;
  if (result.status !== 0 && !allowFailure) {
    throw new Error(`gh ${args.join(" ")} failed: ${result.stderr}`);
  }
  return result;
}

function releaseNotes(changelog, version) {
  const heading = `## [${version}]`;
  const start = changelog.indexOf(heading);
  if (start < 0) throw new Error(`CHANGELOG.md omits ${heading}`);
  const next = changelog.indexOf("\n## [", start + heading.length);
  return `${changelog.slice(start, next < 0 ? undefined : next).trim()}\n`;
}

const args = parseArgs(process.argv.slice(2));
if (!args.tag || !args.assets) {
  throw new Error(
    "usage: rehearse-draft-release.mjs --tag <vX.Y.Z> --assets <directory>",
  );
}
const packageMetadata = JSON.parse(
  await readFile(path.join(root, "package.json"), "utf8"),
);
const runtimeConfig = JSON.parse(
  await readFile(path.join(root, "runtime", "rsync", "runtime.json"), "utf8"),
);
if (args.tag !== `v${packageMetadata.version}`) {
  throw new Error(`tag ${args.tag} does not match v${packageMetadata.version}`);
}
const assetsDirectory = path.resolve(args.assets);
const assetEntries = await readdir(assetsDirectory, { withFileTypes: true });
if (assetEntries.some((entry) => !entry.isFile())) {
  throw new Error("draft release assets directory must contain only files");
}
const assetNames = assetEntries.map((entry) => entry.name).sort();
if (assetNames.length === 0) throw new Error("draft release has no assets");
await verifyReleaseDirectory(assetsDirectory, {
  version: packageMetadata.version,
  runtimeVersion: runtimeConfig.runtimeVersion,
});

const temporary = await mkdtemp(
  path.join(os.tmpdir(), "reflect-draft-release-"),
);
try {
  const view = gh(
    ["release", "view", args.tag, "--json", "isDraft,isPrerelease,assets"],
    { allowFailure: true },
  );
  let release;
  if (view.status === 0) {
    release = JSON.parse(view.stdout);
    if (!release.isDraft) {
      throw new Error(
        `release ${args.tag} already exists and is not a mutable draft`,
      );
    }
  } else {
    const notes = path.join(temporary, "release-notes.md");
    await writeFile(
      notes,
      releaseNotes(
        await readFile(path.join(root, "CHANGELOG.md"), "utf8"),
        packageMetadata.version,
      ),
    );
    gh([
      "release",
      "create",
      args.tag,
      "--draft",
      "--prerelease",
      "--verify-tag",
      "--title",
      `ReflectSync ${packageMetadata.version} release rehearsal`,
      "--notes-file",
      notes,
    ]);
    release = { isDraft: true, isPrerelease: true, assets: [] };
  }

  for (const existing of release.assets ?? []) {
    if (!assetNames.includes(existing.name)) {
      gh(["release", "delete-asset", args.tag, existing.name, "--yes"]);
    }
  }
  gh([
    "release",
    "upload",
    args.tag,
    ...assetNames.map((name) => path.join(assetsDirectory, name)),
    "--clobber",
  ]);
  gh(["release", "edit", args.tag, "--prerelease"]);

  const finalRelease = JSON.parse(
    gh(["release", "view", args.tag, "--json", "isDraft,isPrerelease,assets"])
      .stdout,
  );
  if (!finalRelease.isDraft || !finalRelease.isPrerelease) {
    throw new Error("release rehearsal escaped draft/prerelease state");
  }
  const remoteNames = finalRelease.assets.map((asset) => asset.name).sort();
  if (JSON.stringify(remoteNames) !== JSON.stringify(assetNames)) {
    throw new Error(
      `draft release asset mismatch: ${remoteNames} versus ${assetNames}`,
    );
  }

  const downloaded = path.join(temporary, "downloaded");
  gh(["release", "download", args.tag, "--dir", downloaded, "--clobber"]);
  await verifyReleaseDirectory(downloaded, {
    version: packageMetadata.version,
    runtimeVersion: runtimeConfig.runtimeVersion,
  });
  console.log(
    `draft release rehearsal verified ${assetNames.length} assets for ${args.tag}`,
  );
} finally {
  await rm(temporary, { recursive: true, force: true });
}
