// rsync.delete-chunked.test.ts
import * as rs from "../rsync";
import { EventEmitter } from "node:events";
import fs from "node:fs/promises";

jest.mock("node:child_process", () => ({
  spawn: jest.fn(() => {
    const ee = new EventEmitter() as any;
    process.nextTick(() => ee.emit("exit", 0)); // success
    return ee;
  }),
}));

test("chunked delete calls rsync per batch", async () => {
  const work = await fs.mkdtemp("/tmp/rsync-test-");
  const items = Array.from({ length: 120_000 }, (_, i) => `p/${i}`);
  await rs.rsyncDeleteChunked(work, "/src", "/dst", items, "label", {
    chunkSize: 50_000,
    forceEmptySource: true,
    dryRun: false,
    verbose: false,
  });
  // 120k with chunk=50k => 3 batches
  const { spawn } = await import("node:child_process");
  expect((spawn as any).mock.calls.length).toBe(3);
});
