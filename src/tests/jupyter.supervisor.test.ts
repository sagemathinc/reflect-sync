import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { JUPYTER_SUPERVISOR } from "../jupyter-supervisor.js";

// Exercise the real Python state machine in an isolated home, not a mocked RPC.
describe.runIf(process.platform === "linux")(
  "remote supervisor failures",
  () => {
    let root: string;
    let helper: string;
    beforeEach(async () => {
      root = await mkdtemp(join(tmpdir(), "reflect-supervisor-"));
      helper = join(root, "supervisor.py");
      await writeFile(helper, JUPYTER_SUPERVISOR);
    });
    afterEach(async () => {
      await rm(root, { recursive: true, force: true });
    });
    function rpc(request: object) {
      return JSON.parse(
        execFileSync("python3", [helper], {
          env: { ...process.env, HOME: root },
          input: JSON.stringify(request),
          timeout: 5000,
          encoding: "utf8",
        }),
      );
    }
    const request = {
      operation: "start",
      session: "test",
      python: "/nonexistent/python",
      leaseSeconds: 15,
      connection: {
        transport: "tcp",
        ip: "127.0.0.1",
        key: "synthetic-secret",
        signature_scheme: "hmac-sha256",
      },
    };
    it("leaves a cancellation tombstone even before an uncertain start arrives", () => {
      expect(rpc({ operation: "stop", session: "test" }).status).toBe(
        "stopped",
      );
      expect(rpc(request).error).toContain("different request");
    });
    it("makes retries idempotent, rejects changed requests, and cleans failed startup", async () => {
      expect(rpc(request).status).toBe("starting");
      expect(rpc({ ...request, python: "/another/python" }).error).toContain(
        "different request",
      );
      let state;
      for (let i = 0; i < 20; i++) {
        state = rpc(request);
        if (state.status === "failed") break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(state.status).toBe("failed");
      expect(rpc({ operation: "stop", session: "test" }).status).toBe("failed");
      const session = join(root, ".local/share/reflect/jupyter/sessions/test");
      await expect(
        readFile(join(session, "request.json")),
      ).rejects.toMatchObject({ code: "ENOENT" });
      await expect(
        readFile(join(session, "kernel.json")),
      ).rejects.toMatchObject({ code: "ENOENT" });
    });
    it("rejects a stale boot identity without signaling anything", async () => {
      rpc({ operation: "stop", session: "test" });
      const state = join(
        root,
        ".local/share/reflect/jupyter/sessions/test/state.json",
      );
      await writeFile(
        state,
        JSON.stringify({ boot: "previous-boot", status: "ready" }),
      );
      expect(rpc({ operation: "renew", session: "test" })).toEqual({
        status: "lost",
        reason: "VM rebooted",
      });
    });
    it("does not publish failed installations and permits a clean retry", async () => {
      const uv = join(root, "failing-uv");
      await writeFile(
        uv,
        '#!/bin/sh\nif [ "$1" = venv ]; then mkdir -p "$5/bin"; fi\nexit 1\n',
        { mode: 0o700 },
      );
      for (let attempt = 0; attempt < 2; attempt++) {
        expect(
          rpc({ operation: "prepare", environment: "test", uv }).error,
        ).toBeTruthy();
        expect(rpc({ operation: "kernels" })).toEqual([]);
        expect(
          await readdir(
            join(root, ".local/share/reflect/jupyter/environment-versions"),
          ),
        ).toEqual([]);
      }
    });
  },
);
