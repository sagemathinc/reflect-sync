import { describe, it, expect } from "vitest";
import { createHash } from "node:crypto";
import { verifyBootstrap, downloadJupyterUv } from "../jupyter-bootstrap.js";

describe("Jupyter bootstrap integrity", () => {
  it("rejects a modified artifact", () => {
    const bytes = Buffer.from("verified build");
    const hash = createHash("sha256").update(bytes).digest("hex");
    expect(() => verifyBootstrap(bytes, hash)).not.toThrow();
    expect(() => verifyBootstrap(Buffer.from("different build"), hash)).toThrow(
      /checksum/,
    );
  });
  it("rejects an unsupported platform without downloading", async () => {
    await expect(downloadJupyterUv("windows")).rejects.toThrow(/Unsupported/);
  });
});
