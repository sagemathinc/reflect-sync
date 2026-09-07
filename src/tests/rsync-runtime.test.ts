import { createHash } from "node:crypto";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  installManagedRsyncRuntime,
  managedRsyncPath,
  resolveRsyncExecutable,
} from "../rsync-runtime.js";

describe("managed rsync runtime", () => {
  let temporary = "";

  beforeEach(async () => {
    temporary = await fsp.mkdtemp(
      path.join(os.tmpdir(), "reflect-rsync-runtime-"),
    );
  });

  afterEach(async () => {
    await fsp.rm(temporary, { recursive: true, force: true });
  });

  it("installs atomically and resolves the verified managed executable", async () => {
    const source = path.join(temporary, "source-rsync");
    const bytes = "portable rsync fixture\n";
    await fsp.writeFile(source, bytes, { mode: 0o755 });
    const digest = createHash("sha256").update(bytes).digest("hex");
    const home = path.join(temporary, "home");
    const installed = await installManagedRsyncRuntime({
      sourceExecutable: source,
      expectedSha256: digest,
      home,
      target: "linux-x64",
      runtimeVersion: "test.1",
    });
    expect(installed.path).toBe(
      managedRsyncPath({ home, target: "linux-x64", runtimeVersion: "test.1" }),
    );
    expect(await fsp.readFile(installed.path, "utf8")).toBe(bytes);
    expect(
      resolveRsyncExecutable({
        home,
        target: "linux-x64",
        runtimeVersion: "test.1",
        env: { PATH: "" },
      }),
    ).toMatchObject({ source: "managed" });
  });

  it("rejects a digest mismatch without leaving the final runtime", async () => {
    const source = path.join(temporary, "source-rsync");
    await fsp.writeFile(source, "bad bytes", { mode: 0o755 });
    const home = path.join(temporary, "home");
    await expect(
      installManagedRsyncRuntime({
        sourceExecutable: source,
        expectedSha256: "0".repeat(64),
        home,
        target: "linux-x64",
        runtimeVersion: "test.2",
      }),
    ).rejects.toThrow("digest mismatch");
    await expect(
      fsp.access(
        managedRsyncPath({
          home,
          target: "linux-x64",
          runtimeVersion: "test.2",
        }),
      ),
    ).rejects.toBeTruthy();
  });

  it("allows concurrent installers to converge on one verified runtime", async () => {
    const source = path.join(temporary, "source-rsync");
    const bytes = "concurrent portable rsync fixture\n";
    await fsp.writeFile(source, bytes, { mode: 0o755 });
    const digest = createHash("sha256").update(bytes).digest("hex");
    const home = path.join(temporary, "home");
    const installs = await Promise.all(
      Array.from({ length: 4 }, () =>
        installManagedRsyncRuntime({
          sourceExecutable: source,
          expectedSha256: digest,
          home,
          target: "linux-x64",
          runtimeVersion: "test.concurrent",
        }),
      ),
    );
    expect(
      new Set(installs.map(({ path: installedPath }) => installedPath)).size,
    ).toBe(1);
    expect(await fsp.readFile(installs[0].path, "utf8")).toBe(bytes);
  });

  it("refuses to replace a corrupt existing runtime directory", async () => {
    const source = path.join(temporary, "source-rsync");
    const bytes = "expected runtime\n";
    await fsp.writeFile(source, bytes, { mode: 0o755 });
    const digest = createHash("sha256").update(bytes).digest("hex");
    const home = path.join(temporary, "home");
    const destination = managedRsyncPath({
      home,
      target: "linux-x64",
      runtimeVersion: "test.corrupt",
    });
    await fsp.mkdir(path.dirname(destination), { recursive: true });
    await fsp.writeFile(destination, "corrupt", { mode: 0o755 });
    await expect(
      installManagedRsyncRuntime({
        sourceExecutable: source,
        expectedSha256: digest,
        home,
        target: "linux-x64",
        runtimeVersion: "test.corrupt",
      }),
    ).rejects.toThrow("exists but failed verification");
    expect(await fsp.readFile(destination, "utf8")).toBe("corrupt");
  });

  it("uses the target rather than the host to select the executable name", () => {
    expect(
      managedRsyncPath({
        home: temporary,
        target: "windows-x64",
        runtimeVersion: "test.windows",
      }),
    ).toMatch(/rsync\.exe$/u);
  });
});
