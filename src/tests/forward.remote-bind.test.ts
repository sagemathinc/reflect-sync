import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createForward, normalizeRemoteBind } from "../forward-manage.js";
import { buildSshArgs } from "../forward-runner.js";
import { ensureSessionDb, loadForwardById } from "../session-db.js";

describe("forward --remote-bind", () => {
  let tmp: string;
  let sessionDb: string;
  let previousDisableForward: string | undefined;

  beforeEach(async () => {
    previousDisableForward = process.env.REFLECT_DISABLE_FORWARD;
    process.env.REFLECT_DISABLE_FORWARD = "1";
    tmp = await fs.mkdtemp(join(tmpdir(), "reflect-remote-bind-"));
    sessionDb = join(tmp, "sessions.db");
    ensureSessionDb(sessionDb).close();
  });

  afterEach(async () => {
    if (previousDisableForward === undefined) {
      delete process.env.REFLECT_DISABLE_FORWARD;
    } else {
      process.env.REFLECT_DISABLE_FORWARD = previousDisableForward;
    }
    await fs.rm(tmp, { recursive: true, force: true });
  });

  it("accepts addresses, hostnames and the wildcard", () => {
    expect(normalizeRemoteBind("127.0.0.1")).toBe("127.0.0.1");
    expect(normalizeRemoteBind(" 0.0.0.0 ")).toBe("0.0.0.0");
    expect(normalizeRemoteBind("localhost")).toBe("localhost");
    expect(normalizeRemoteBind("bind-host.example.com")).toBe(
      "bind-host.example.com",
    );
    expect(normalizeRemoteBind("*")).toBe("*");
  });

  it("brackets IPv6 literals for the -R specification", () => {
    expect(normalizeRemoteBind("::1")).toBe("[::1]");
    expect(normalizeRemoteBind("[::1]")).toBe("[::1]");
    expect(normalizeRemoteBind("fd00::2")).toBe("[fd00::2]");
  });

  it("rejects values that could change the -R specification", () => {
    for (const bad of [
      "",
      "   ",
      "127.0.0.1:2222",
      "[127.0.0.1]",
      "[::1",
      "::1]",
      "a b",
      "host\n",
      "host\u0000",
      "-oProxyCommand=x",
      "fe80::1%eth0",
      "host_name",
    ]) {
      expect(() => normalizeRemoteBind(bad)).toThrow(/remote-bind/);
    }
  });

  it("stores the normalized bind for remote -> local forwards", async () => {
    const id = await createForward({
      sessionDb,
      left: "user@example.com:9222",
      right: ":9333",
      remoteBind: "::1",
    });
    const row = loadForwardById(sessionDb, id)!;
    expect(row.direction).toBe("remote_to_local");
    expect(row.remote_host).toBe("[::1]");
    expect(buildSshArgs(row)).toContain("[::1]:9222:127.0.0.1:9333");
  });

  it("rejects --remote-bind for local -> remote forwards, even blank", async () => {
    for (const remoteBind of ["127.0.0.1", ""]) {
      await expect(
        createForward({
          sessionDb,
          left: ":5000",
          right: "user@example.com:6000",
          remoteBind,
        }),
      ).rejects.toThrow(/only applies to remote -> local/);
    }
  });

  it("rejects an explicitly blank bind for remote -> local forwards", async () => {
    await expect(
      createForward({
        sessionDb,
        left: "user@example.com:9222",
        right: ":9333",
        remoteBind: " ",
      }),
    ).rejects.toThrow(/must not be empty/);
  });
});
