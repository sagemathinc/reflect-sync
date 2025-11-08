import os from "node:os";
import path from "node:path";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import type { Stats } from "node:fs";
import { DeviceBoundary } from "../device-boundary.js";

describe("DeviceBoundary", () => {
  let rootDir: string;
  let guard: DeviceBoundary;

  beforeAll(async () => {
    rootDir = await mkdtemp(path.join(os.tmpdir(), "reflect-device-"));
    guard = await DeviceBoundary.create(rootDir);
  });

  afterAll(async () => {
    await rm(rootDir, { recursive: true, force: true });
  });

  test("children under the root share its device id", async () => {
    const child = path.join(rootDir, "child.txt");
    await writeFile(child, "alpha");
    await expect(guard.isOnRootDevice(child, { isDir: false })).resolves.toBe(
      true,
    );
  });

  test("explicit stats with a different dev flag cross-device entries", async () => {
    const fakeDev =
      (typeof guard.deviceId === "bigint"
        ? Number(guard.deviceId)
        : guard.deviceId) + 123;
    const stats = { dev: fakeDev } as unknown as Stats;
    const other = path.join(rootDir, "other");
    await expect(
      guard.isOnRootDevice(other, { isDir: true, stats }),
    ).resolves.toBe(false);
  });

  test("entries removed before stat fall back to parent device", async () => {
    const ephemeral = path.join(rootDir, "ephemeral");
    await writeFile(ephemeral, "temp");
    await rm(ephemeral);
    await expect(
      guard.isOnRootDevice(ephemeral, { isDir: false }),
    ).resolves.toBe(true);
  });
});
