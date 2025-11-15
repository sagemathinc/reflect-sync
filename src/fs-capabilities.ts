/*
Detect basic filesystem behaviors that affect path equality. Some platforms
(e.g., macOS) will lowercase or decompose Unicode characters automatically.
We treat a filesystem as case-insensitive if it considers different letter
casings equivalent, and `normalizesUnicode` if it rewrites composed characters
to another normalized form (typically NFD). The latter lets us detect pairs
like "école" and "école" that map to the same filename.

wstein@macos:~$ node
Welcome to Node.js v24.11.0.
Type ".help" for more information.
> a="école"; b="école"; a == b
false
> require('fs').writeFileSync(a,'hi'); require('fs').writeFileSync(b,'hi')
undefined
>
wstein@macos:~$ ls -lht *cole*
-rw-r--r--  1 wstein  staff     2B Nov 15 07:19 ??cole
*/


import { randomBytes } from "node:crypto";
import { promises as fsp } from "node:fs";
import path from "node:path";
import type { Logger } from "./logger.js";

export type FilesystemCapabilities = {
  caseInsensitive: boolean;
  normalizesUnicode: boolean;
};

export const DEFAULT_FILESYSTEM_CAPABILITIES: FilesystemCapabilities = {
  caseInsensitive: false,
  normalizesUnicode: false,
};

type DetectOptions = {
  logger?: Logger;
};

function randomSuffix(): string {
  return randomBytes(6).toString("hex");
}

async function writeProbeFile(fullPath: string): Promise<void> {
  await fsp.writeFile(fullPath, "");
}

async function safeRm(fullPath: string): Promise<void> {
  try {
    await fsp.rm(fullPath, { force: true });
  } catch {}
}

export function canonicalizeComponent(
  component: string,
  caps: FilesystemCapabilities,
): string {
  let result = component;
  if (caps.normalizesUnicode) {
    result = result.normalize("NFC");
  }
  if (caps.caseInsensitive) {
    result = result.toLowerCase();
  }
  return result;
}

export function canonicalizePath(
  relPath: string,
  caps: FilesystemCapabilities,
): string {
  if (!relPath) return relPath;
  return relPath
    .split("/")
    .map((segment) => canonicalizeComponent(segment, caps))
    .join("/");
}

export async function detectFilesystemCapabilities(
  root: string,
  opts: DetectOptions = {},
): Promise<FilesystemCapabilities> {
  const { logger } = opts;
  const suffix = randomSuffix();
  const probePrefix = `.reflect-fs-probe-${suffix}`;
  const probeCase = `${probePrefix}-Case`;
  const probeCaseVariant = `${probePrefix}-case`;
  const unicodeBase = `${probePrefix}-école`;
  const unicodeVariant = unicodeBase.normalize("NFD");

  const casePath = path.join(root, probeCase);
  const caseVariantPath = path.join(root, probeCaseVariant);
  const unicodePath = path.join(root, unicodeBase);
  const unicodeVariantPath = path.join(root, unicodeVariant);
  const recorded: string[] = [];
  const caps = { ...DEFAULT_FILESYSTEM_CAPABILITIES };

  try {
    await writeProbeFile(casePath);
    recorded.push(casePath, caseVariantPath);
    try {
      await fsp.stat(caseVariantPath);
      caps.caseInsensitive = true;
    } catch {}

    if (unicodeBase !== unicodeVariant) {
      await writeProbeFile(unicodePath);
      recorded.push(unicodePath, unicodeVariantPath);
      try {
        await fsp.stat(unicodeVariantPath);
        caps.normalizesUnicode = true;
      } catch {}
    }
  } catch (err) {
    logger?.warn?.("filesystem capability probe failed", {
      root,
      error: err instanceof Error ? err.message : String(err),
    });
    return { ...DEFAULT_FILESYSTEM_CAPABILITIES };
  } finally {
    await Promise.all(recorded.map((entry) => safeRm(entry)));
  }

  return caps;
}
