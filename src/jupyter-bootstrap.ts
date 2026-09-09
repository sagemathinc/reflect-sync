import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Official release checksums, pinned independently of the downloaded archive.
const UV = {
  x86_64: "741ff1f5742c5a4a25d2f829e8395355e43f7a5ae2ebc6368e9ae2df0efb69cf",
  aarch64: "726b72a137fda33565143325f7d31c42cd30ff9ccdf067e00d124d37b4081cb2",
} as const;
export const UV_VERSION = "0.8.22";

export function verifyBootstrap(bytes: Uint8Array, expected: string): void {
  if (createHash("sha256").update(bytes).digest("hex") !== expected)
    throw Error("uv bootstrap checksum mismatch");
}

export async function downloadJupyterUv(arch: string): Promise<Buffer> {
  if (!(arch in UV)) throw Error(`Unsupported remote architecture: ${arch}`);
  const target = `uv-${arch}-unknown-linux-gnu`;
  const response = await fetch(
    `https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/${target}.tar.gz`,
    { signal: AbortSignal.timeout(120000) },
  );
  if (!response.ok) throw Error(`uv download failed: ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  verifyBootstrap(bytes, UV[arch as keyof typeof UV]);
  const dir = await mkdtemp(join(tmpdir(), "reflect-uv-"));
  try {
    await writeFile(join(dir, "uv.tar.gz"), bytes, { mode: 0o600 });
    await promisify(execFile)("tar", [
      "-xzf",
      join(dir, "uv.tar.gz"),
      "-C",
      dir,
      `${target}/uv`,
    ]);
    return await readFile(join(dir, target, "uv"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
