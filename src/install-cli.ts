import { Command } from "commander";
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs/promises";

const EXECUTABLE_NAME = "reflect-sync";
const SYMLINK_NAME = "reflect";
const BUNDLE_NAME = "bundle.mjs";

async function pathExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function copyBundle(
  destDir: string,
  { force }: { force?: boolean },
): Promise<{ bundle: string; symlink?: string }> {
  const resolvedDir = path.resolve(destDir);
  await fs.mkdir(resolvedDir, { recursive: true });

  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  const bundlePath = path.join(currentDir, BUNDLE_NAME);
  if (!(await pathExists(bundlePath))) {
    throw new Error(
      `bundle file not found at ${bundlePath}. Run "pnpm bundle" before using install.`,
    );
  }

  const target = path.join(resolvedDir, EXECUTABLE_NAME);
  if (await pathExists(target)) {
    if (!force) {
      throw new Error(
        `${target} already exists. Re-run with --force to overwrite.`,
      );
    }
    await fs.rm(target, { force: true });
  }

  await fs.copyFile(bundlePath, target);
  await fs.chmod(target, 0o755);

  let linkPath: string | undefined;
  if (process.platform !== "win32") {
    linkPath = path.join(resolvedDir, SYMLINK_NAME);
    if (await pathExists(linkPath)) {
      if (!force) {
        linkPath = undefined;
      } else {
        await fs.rm(linkPath, { force: true });
      }
    }
    if (linkPath) {
      await fs.symlink(EXECUTABLE_NAME, linkPath);
    }
  }

  return { bundle: target, symlink: linkPath };
}

export function registerInstallCommand(program: Command) {
  program
    .command("install")
    .description(
      "Copy the bundled single-file CLI into a directory (along with a reflect symlink on POSIX).",
    )
    .argument("<directory>", "destination directory for the executable")
    .option("--force", "overwrite existing files/symlinks", false)
    .action(async (directory: string, opts: { force?: boolean }) => {
      try {
        const result = await copyBundle(directory, opts);
        console.log(`installed ${result.bundle}`);
        if (result.symlink) {
          console.log(`created symlink ${result.symlink}`);
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`install failed: ${msg}`);
        process.exitCode = 1;
      }
    });
}
