import { readdir, utimes } from "node:fs/promises";
import path from "node:path";

// ZIP's DOS timestamp cannot represent the Unix epoch. Use its earliest
// portable instant and force UTC when invoking the archiver.
const archiveEpoch = new Date("1980-01-01T00:00:00.000Z");

export async function normalizeArchiveTree(directory) {
  const entries = (await readdir(directory, { withFileTypes: true })).sort(
    (left, right) => left.name.localeCompare(right.name),
  );
  for (const entry of entries) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      await normalizeArchiveTree(absolute);
    } else if (!entry.isFile()) {
      throw new Error(
        `release staging contains unsupported entry: ${absolute}`,
      );
    }
    await utimes(absolute, archiveEpoch, archiveEpoch);
  }
  await utimes(directory, archiveEpoch, archiveEpoch);
}

export async function archiveFileEntries(directory, relativeRoot) {
  const result = [];

  async function visit(absolute, relative) {
    const entries = (await readdir(absolute, { withFileTypes: true })).sort(
      (left, right) => left.name.localeCompare(right.name),
    );
    for (const entry of entries) {
      const childAbsolute = path.join(absolute, entry.name);
      const childRelative = path.posix.join(relative, entry.name);
      if (entry.isDirectory()) {
        await visit(childAbsolute, childRelative);
      } else if (entry.isFile()) {
        result.push(childRelative);
      } else {
        throw new Error(
          `release staging contains unsupported entry: ${childAbsolute}`,
        );
      }
    }
  }

  await visit(directory, relativeRoot);
  return result;
}
