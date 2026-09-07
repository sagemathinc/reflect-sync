import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";

export async function sha256File(file) {
  return createHash("sha256")
    .update(await readFile(file))
    .digest("hex");
}

export function parseChecksumText(text) {
  const entries = new Map();
  for (const line of text.split(/\r?\n/u)) {
    if (!line) continue;
    const match = /^([a-f0-9]{64}) {2}([^/\\][^\r\n]*)$/u.exec(line);
    if (!match) throw new Error(`invalid SHA256SUMS line: ${line}`);
    if (entries.has(match[2])) {
      throw new Error(`duplicate SHA256SUMS entry: ${match[2]}`);
    }
    entries.set(match[2], match[1]);
  }
  return entries;
}

export async function readExpectedChecksum(file) {
  try {
    return (await readFile(`${file}.sha256`, "utf8")).split(/\s+/u)[0];
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const checksums = parseChecksumText(
    await readFile(path.join(path.dirname(file), "SHA256SUMS"), "utf8"),
  );
  const expected = checksums.get(path.basename(file));
  if (!expected) throw new Error(`SHA256SUMS omits ${path.basename(file)}`);
  return expected;
}

export async function verifyChecksum(file) {
  const expected = await readExpectedChecksum(file);
  const actual = await sha256File(file);
  if (actual !== expected) {
    throw new Error(
      `digest mismatch for ${path.basename(file)}: expected ${expected}, got ${actual}`,
    );
  }
  return actual;
}
