import { execFile, spawn, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { beforeEach, afterEach, expect, it } from "vitest";
import { updateForwardSession } from "../session-db.js";

const exec = promisify(execFile);
const cli = resolve(process.env.REFLECT_TEST_CLI ?? "dist/cli.js");
let root: string;
let env: NodeJS.ProcessEnv;
const children: ChildProcess[] = [];
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "reflect-contract-"));
  env = { ...process.env, REFLECT_HOME: root, REFLECT_DISABLE_DAEMON: "1" };
});
afterEach(async () => {
  for (const child of children.splice(0)) child.kill("SIGKILL");
  await rm(root, { recursive: true, force: true });
});
async function run(...args: string[]) {
  return await exec(process.execPath, [cli, ...args], { env, timeout: 20000 });
}
async function json(...args: string[]) {
  return JSON.parse((await run(...args, "--json")).stdout);
}

it("exposes domains, not top-level sync commands or obsolete Jupyter aliases", async () => {
  const help = (await run("--help")).stdout;
  expect(help).toContain("sync");
  expect(help).toContain("forward");
  expect(help).toContain("jupyter");
  for (const command of [
    "create",
    "list",
    "stop",
    "start",
    "terminate",
    "status",
    "scan",
    "scheduler",
  ])
    await expect(run(command)).rejects.toBeTruthy();
  for (const command of ["setup", "probe", "register", "targets", "sessions"])
    await expect(run("jupyter", command)).rejects.toBeTruthy();
});

it.each(["before", "domain", "command"])(
  "honors --session-db %s the command",
  async (position) => {
    const db = join(root, "custom", "sessions.db");
    const args = [
      "sync",
      "create",
      join(root, "a"),
      join(root, "b"),
      "--stopped",
      "--name",
      "course",
    ];
    await mkdir(join(root, "a"));
    await mkdir(join(root, "b"));
    args.splice(
      position === "before" ? 0 : position === "domain" ? 1 : args.length,
      0,
      "--session-db",
      db,
    );
    const created = await json(...args);
    expect(created.id).toBe(1);
    expect(await json("sync", "list")).toEqual([]);
    expect((await json("--session-db", db, "sync", "list"))[0].name).toBe(
      "course",
    );
    expect(
      (await json("sync", "--session-db", db, "status", "course")).id,
    ).toBe(1);
    expect(
      (await json("sync", "remove", "course", "--session-db", db))[0].ok,
    ).toBe(true);
  },
);

it("refuses active sync removal, reports batch failures, and never deletes synced files", async () => {
  const a = join(root, "a"),
    b = join(root, "b");
  await mkdir(a);
  await mkdir(b);
  await writeFile(join(a, "keep"), "data");
  const created = await json("sync", "create", a, b);
  await expect(
    run("sync", "remove", String(created.id), "--json"),
  ).rejects.toMatchObject({
    code: 1,
    stderr: expect.stringContaining("--stop"),
  });
  expect(await json("sync", "list")).toHaveLength(1);
  await expect(
    run("sync", "remove", "missing", String(created.id), "--stop", "--json"),
  ).rejects.toMatchObject({
    code: 1,
    stdout: expect.stringContaining('"ok": false'),
  });
  expect(await json("sync", "list")).toEqual([]);
  const { readFile } = await import("node:fs/promises");
  expect(await readFile(join(a, "keep"), "utf8")).toBe("data");
  expect((await json("sync", "create", a, b, "--stopped")).id).toBeGreaterThan(
    created.id,
  );
});

it("starts/stops/restarts forwards without deleting configuration and requires explicit removal", async () => {
  const bin = join(root, "bin");
  await mkdir(bin);
  await writeFile(
    join(bin, "ssh"),
    `#!${process.execPath}\nsetInterval(() => {}, 1000);\n`,
    { mode: 0o700 },
  );
  env.PATH = bin + ":" + process.env.PATH;
  const created = await json(
    "forward",
    "create",
    ":12345",
    "example:12345",
    "--name",
    "notebook",
    "--stopped",
  );
  expect(created.id).toBe(1);
  try {
    await json("forward", "start", "notebook");
    const first = (await json("forward", "list"))[0].monitor_pid;
    await json("forward", "start", "1");
    expect((await json("forward", "list"))[0].monitor_pid).toBe(first);
    await expect(run("forward", "remove", "1")).rejects.toMatchObject({
      code: 1,
    });
    await json("forward", "restart", "1");
    expect((await json("forward", "list"))[0].monitor_pid).not.toBe(first);
    await json("forward", "stop", "1");
    expect((await json("forward", "list"))[0]).toMatchObject({
      desired_state: "stopped",
      monitor_pid: null,
    });
    await json("forward", "remove", "1");
    expect(await json("forward", "list")).toEqual([]);
  } finally {
    await run("forward", "remove", "1", "--stop").catch(() => undefined);
  }
}, 30000);

it("retains configuration and reports failure when a process refuses to stop", async () => {
  const child = spawn(
    process.execPath,
    [
      "-e",
      'process.on("SIGTERM", () => {}); process.stdout.write("ready"); setInterval(() => {}, 1000)',
    ],
    { stdio: ["ignore", "pipe", "ignore"] },
  );
  children.push(child);
  await new Promise((resolve) => child.stdout!.once("data", resolve));
  await json("forward", "create", ":12345", "example:12345", "--stopped");
  updateForwardSession(join(root, "sessions.db"), 1, {
    monitor_pid: child.pid!,
    actual_state: "running",
  });
  await expect(
    run("forward", "remove", "1", "--stop", "--json"),
  ).rejects.toMatchObject({
    code: 1,
    stderr: expect.stringContaining("has not stopped"),
  });
  expect(await json("forward", "list")).toHaveLength(1);
}, 20000);
