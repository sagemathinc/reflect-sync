import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { beforeEach, afterEach, expect, it, vi } from "vitest";
import {
  jupyterSessionId,
  listJupyterSessions,
  jupyterSessionCommand,
  removeJupyterSession,
  removeJupyterTarget,
} from "../jupyter.js";

let root: string;
const uid = "11223344-1122-3344-5566-112233445566";
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "reflect-jupyter-sessions-"));
  vi.stubEnv("REFLECT_JUPYTER_HOME", root);
  vi.stubEnv("JUPYTER_DATA_DIR", join(root, "data"));
  await mkdir(join(root, "sessions"));
  await mkdir(join(root, "targets"));
  await writeFile(
    join(root, "sessions", uid + ".json"),
    JSON.stringify({
      host: "gpu",
      script: "/helper.py",
      targetName: "teaching",
    }),
  );
  await writeFile(
    join(root, "targets/teaching.json"),
    JSON.stringify({ host: "gpu", environment: "teaching" }),
  );
  await writeFile(join(root, "state"), "ready");
  await mkdir(join(root, "bin"));
  await writeFile(
    join(root, "bin/ssh"),
    `#!${process.execPath}
const fs = require("node:fs");
const state = ${JSON.stringify(join(root, "state"))};
let input = "";
process.stdin.on("data", data => input += data);
process.stdin.on("end", () => {
  if (fs.readFileSync(state, "utf8") === "unreachable") { process.stderr.write("Connection refused"); process.exitCode = 255; return; }
  const request = JSON.parse(input);
  if (request.operation === "stop") fs.writeFileSync(state, "stopped");
  process.stdout.write(JSON.stringify({ status: fs.readFileSync(state, "utf8") }));
});
`,
    { mode: 0o700 },
  );
  vi.stubEnv("PATH", join(root, "bin"));
});
afterEach(async () => {
  vi.unstubAllEnvs();
  await rm(root, { recursive: true, force: true });
});

it("allocates persistent unique local IDs and resolves both integer and UUID selectors", async () => {
  const id = await jupyterSessionId(uid);
  expect(id).toBe(1);
  const ids = await Promise.all(
    Array.from({ length: 5 }, (_, n) => jupyterSessionId(`other-${n}`)),
  );
  expect(new Set(ids).size).toBe(5);
  expect(await jupyterSessionId(uid)).toBe(id);
  const again = execFileSync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import {jupyterSessionId} from './dist/jupyter.js'; console.log(await jupyterSessionId('${uid}'));`,
    ],
    { encoding: "utf8", env: process.env },
  );
  expect(Number(again.trim())).toBe(id);
  expect(await jupyterSessionCommand(String(id), "status")).toMatchObject({
    id,
    session: uid,
    status: "ready",
  });
  expect(await jupyterSessionCommand(uid, "status")).toMatchObject({ id });
  await expect(jupyterSessionCommand("99999", "status")).rejects.toThrow(
    "not found",
  );
});

it("refuses active removal, stops explicitly, and does not recycle the handle", async () => {
  const id = (await listJupyterSessions())[0].id;
  await expect(removeJupyterSession(String(id))).rejects.toThrow("--stop");
  expect(await listJupyterSessions()).toHaveLength(1);
  await removeJupyterSession(String(id), true);
  expect(await listJupyterSessions()).toEqual([]);
  expect(await jupyterSessionId("next-session")).toBeGreaterThan(id);
});

it("does not discard unreachable remote executions", async () => {
  await writeFile(join(root, "state"), "unreachable");
  await expect(removeJupyterSession(uid, true)).rejects.toThrow(
    "Connection refused",
  );
  expect(await listJupyterSessions()).toHaveLength(1);
});

it("target removal requires explicit permission to stop active kernels", async () => {
  await expect(removeJupyterTarget("teaching")).rejects.toThrow("--stop");
  expect(
    JSON.parse(await readFile(join(root, "targets/teaching.json"), "utf8"))
      .disabled,
  ).toBeUndefined();
  await removeJupyterTarget("teaching", true);
  await expect(
    readFile(join(root, "targets/teaching.json")),
  ).rejects.toMatchObject({ code: "ENOENT" });
  expect((await listJupyterSessions())[0].stopped).toBe(true);
});

it("force forgets an unreachable session without SSH, preserving its target and ID history", async () => {
  const id = (await listJupyterSessions())[0].id;
  await rm(join(root, "bin/ssh"));
  await expect(removeJupyterSession(uid, true, true)).rejects.toThrow(
    "not both",
  );
  expect(await listJupyterSessions()).toHaveLength(1);
  const result = JSON.parse(
    execFileSync(
      process.execPath,
      ["dist/cli.js", "jupyter", "remove", String(id), "--force", "--json"],
      { encoding: "utf8", env: process.env },
    ),
  );
  expect(result[0]).toMatchObject({
    ok: true,
    result: {
      removed: String(id),
      warning: expect.stringContaining("not confirmed"),
    },
  });
  expect(await listJupyterSessions()).toEqual([]);
  expect(await readFile(join(root, "targets/teaching.json"), "utf8")).toContain(
    "gpu",
  );
  expect(await readFile(join(root, "state"), "utf8")).toBe("ready");
  expect(await jupyterSessionId("next-session")).toBeGreaterThan(id);
});
