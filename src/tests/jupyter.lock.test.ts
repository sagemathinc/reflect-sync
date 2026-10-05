import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { withJupyterTargetLock } from "../jupyter-lock.js";

describe("Jupyter target admission lock", () => {
  it("serializes the same target and releases after errors", async () => {
    const root = await mkdtemp(join(tmpdir(), "reflect-lock-"));
    try {
      let release!: () => void;
      let entered!: () => void;
      const ready = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const hold = new Promise<void>((resolve) => {
        release = resolve;
      });
      const events: string[] = [];
      const first = withJupyterTargetLock(root, "gpu", async () => {
        events.push("launch");
        entered();
        await hold;
        events.push("launched");
      });
      await ready;
      const second = withJupyterTargetLock(root, "gpu", async () => {
        events.push("remove");
      });
      await withJupyterTargetLock(root, "other", async () => {
        events.push("independent");
      });
      expect(events).toEqual(["launch", "independent"]);
      release();
      await first;
      await second;
      expect(events).toEqual(["launch", "independent", "launched", "remove"]);
      await expect(
        withJupyterTargetLock(root, "gpu", async () => {
          throw Error("installation failed");
        }),
      ).rejects.toThrow("installation failed");
      await withJupyterTargetLock(root, "gpu", async () => undefined);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
