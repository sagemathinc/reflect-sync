import fsp from "node:fs/promises";
import path from "node:path";
import { createTestSession } from "./env.js";
import type { TestSession } from "./env.js";

const BATCH_SIZE = 2_000;
const TOTAL_FILES = 6_000;

jest.setTimeout(15_000);

describe("stress sync (local)", () => {
  let session: TestSession | undefined;

  afterEach(async () => {
    if (session) {
      await session.dispose();
      session = undefined;
    }
  });

  it(`syncs ${TOTAL_FILES} file create/delete in a single cycle`, async () => {
    session = await createTestSession({
      hot: false,
      full: false,
    });

    const alphaBulk = session.alpha.path("bulk");
    const betaBulk = session.beta.path("bulk");

    await populateFiles(alphaBulk, TOTAL_FILES);
    await session.sync();

    await expect(countFiles(betaBulk)).resolves.toBe(TOTAL_FILES);
    await expect(verifyFileContents(betaBulk, TOTAL_FILES)).resolves.toBe(true);

    await fsp.rm(alphaBulk, { recursive: true, force: true });
    await session.sync();

    await expect(countFiles(alphaBulk)).resolves.toBe(0);
    await expect(countFiles(betaBulk)).resolves.toBe(0);
  });
});

async function populateFiles(dir: string, total: number): Promise<void> {
  await fsp.mkdir(dir, { recursive: true });
  const pending: Array<Promise<unknown>> = [];
  for (let i = 0; i < total; i += 1) {
    const file = path.join(dir, `file-${i}.txt`);
    pending.push(fsp.writeFile(file, `data-${i}`));
    if (pending.length >= BATCH_SIZE) {
      await Promise.all(pending.splice(0, pending.length));
    }
  }
  if (pending.length) {
    await Promise.all(pending);
  }
}

async function verifyFileContents(
  dir: string,
  expected: number,
): Promise<boolean> {
  const chunks = Math.ceil(expected / BATCH_SIZE);
  let seen = 0;
  for (let chunk = 0; chunk < chunks; chunk += 1) {
    const start = chunk * BATCH_SIZE;
    const end = Math.min(expected, start + BATCH_SIZE);
    const batch: Array<Promise<void>> = [];
    for (let i = start; i < end; i += 1) {
      const file = path.join(dir, `file-${i}.txt`);
      batch.push(
        fsp.readFile(file, "utf8").then((contents) => {
          if (contents !== `data-${i}`) {
            throw new Error(`mismatch for ${file}`);
          }
          seen += 1;
        }),
      );
    }
    await Promise.all(batch);
  }
  return seen === expected;
}

async function countFiles(root: string): Promise<number> {
  let count = 0;
  const stack = [root];
  while (stack.length) {
    const dir = stack.pop();
    if (!dir) continue;
    let entries: Array<import("node:fs").Dirent>;
    try {
      entries = await fsp.readdir(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
      } else if (entry.isFile()) {
        count += 1;
      }
    }
  }
  return count;
}
