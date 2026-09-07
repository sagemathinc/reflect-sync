import { spawnSync } from "node:child_process";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

function runPnpm(root) {
  const pnpm = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
  const result = spawnSync(pnpm, ["licenses", "list", "--prod", "--json"], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`pnpm licenses exited ${result.status}: ${result.stderr}`);
  }
  return JSON.parse(result.stdout);
}

export function productionLicenseEntries(root) {
  return Object.values(runPnpm(root))
    .flat()
    .sort((left, right) => {
      const leftId = `${left.name}@${left.versions.join(",")}`;
      const rightId = `${right.name}@${right.versions.join(",")}`;
      return leftId.localeCompare(rightId);
    });
}

export async function thirdPartyNotices(root) {
  const sections = [
    "ReflectSync bundled JavaScript dependency notices",
    "Generated from the frozen production dependency graph.",
  ];

  for (const dependency of productionLicenseEntries(root)) {
    const directory = dependency.paths[0];
    const candidates = (await readdir(directory))
      .filter((name) => /^(license|copying|unlicense)/iu.test(name))
      .sort();
    if (candidates.length === 0) {
      throw new Error(`no license file found for ${dependency.name}`);
    }
    const licenseText = await readFile(
      path.join(directory, candidates[0]),
      "utf8",
    );
    sections.push(
      "",
      "=".repeat(78),
      `${dependency.name}@${dependency.versions.join(", ")} (${dependency.license})`,
      dependency.homepage ?? "",
      "=".repeat(78),
      licenseText.trim(),
    );
  }
  return `${sections.join("\n")}\n`;
}
