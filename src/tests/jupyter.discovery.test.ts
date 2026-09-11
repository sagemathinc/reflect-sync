import { execFileSync } from "node:child_process";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { jupyterSshAliases } from "../jupyter-ssh.js";
import { JUPYTER_DISCOVERY } from "../jupyter-discovery.js";
import { JUPYTER_SUPERVISOR } from "../jupyter-supervisor.js";
import {
  availableJupyterName,
  jupyterName,
  registerJupyterTarget,
} from "../jupyter.js";

let root: string;
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "reflect-discovery-"));
});
afterEach(async () => {
  vi.unstubAllEnvs();
  await rm(root, { recursive: true, force: true });
});

it("avoids local kernelspec names and admits only one concurrent registration", async () => {
  vi.stubEnv("REFLECT_JUPYTER_HOME", join(root, "reflect"));
  vi.stubEnv("JUPYTER_DATA_DIR", join(root, "jupyter"));
  await mkdir(join(root, "jupyter/kernels/classroom"), { recursive: true });
  expect(await availableJupyterName("classroom")).toBe("classroom-2");
  const target = {
    host: "example",
    environment: "existing",
    kernel: {
      argv: ["/bin/false", "{connection_file}"],
      display_name: "Example",
      language: "javascript",
      interrupt_mode: "message",
    },
  };
  const results = await Promise.allSettled([
    registerJupyterTarget("classroom-2", target, ["reflect"]),
    registerJupyterTarget("classroom-2", target, ["reflect"]),
  ]);
  expect(
    results.filter((result) => result.status === "fulfilled"),
  ).toHaveLength(1);
  expect(results.filter((result) => result.status === "rejected")).toHaveLength(
    1,
  );
  expect(
    JSON.parse(
      await readFile(
        join(root, "jupyter/kernels/reflect-classroom-2/kernel.json"),
        "utf8",
      ),
    ),
  ).toMatchObject({ language: "javascript", interrupt_mode: "message" });
});

it("enumerates concrete aliases across Includes without evaluating Match exec", async () => {
  await mkdir(join(root, "config.d"));
  await writeFile(
    join(root, "config"),
    'Host cpu gpu\n HostName example.com\nInclude "config.d/*"\nHost * !excluded ?pattern\n User user\nMatch exec "touch /never-run-this"\n User ignored\n',
  );
  await writeFile(
    join(root, "config.d/a"),
    'Host "quoted-host"\n HostName other\nInclude config\n',
  );
  expect(await jupyterSshAliases([join(root, "config")])).toEqual({
    aliases: ["cpu", "gpu", "quoted-host"],
    warnings: [],
  });
});

it("derives stable safe names", () => {
  expect(jupyterName("student@GPU.example.com")).toBe(
    "student-gpu-example-com",
  );
});

it.each([
  ["printf 'NVIDIA L40S, 580.173.02\\n'", "available"],
  ["exit 1", "unavailable"],
  ["exit 0", "unknown"],
])("distinguishes GPU probe outcomes: %s", async (script, status) => {
  const python = execFileSync(
    "python3",
    ["-c", "import sys; print(sys.executable)"],
    { encoding: "utf8" },
  ).trim();
  await mkdir(join(root, "bin"));
  await writeFile(join(root, "bin/nvidia-smi"), `#!/bin/sh\n${script}\n`, {
    mode: 0o700,
  });
  const result = JSON.parse(
    execFileSync(python, ["-", "[]"], {
      input: JUPYTER_DISCOVERY,
      encoding: "utf8",
      env: { ...process.env, HOME: root, PATH: join(root, "bin") },
      timeout: 10000,
    }),
  );
  expect(result.gpu.status).toBe(status);
});

it("discovers non-Python specs and reports broken catalogs without installing a helper", async () => {
  const kernel = join(root, "kernels/bash");
  await mkdir(kernel, { recursive: true });
  await writeFile(
    join(kernel, "kernel.json"),
    JSON.stringify({
      argv: ["/bin/bash", "{resource_dir}/launch", "{connection_file}"],
      display_name: "Bash",
      language: "bash",
    }),
  );
  const result = JSON.parse(
    execFileSync("python3", ["-", JSON.stringify([join(root, "kernels")])], {
      input: JUPYTER_DISCOVERY,
      encoding: "utf8",
      env: { ...process.env, HOME: root },
      timeout: 30000,
    }),
  );
  expect(result.kernels).toContainEqual(
    expect.objectContaining({
      display_name: "Bash",
      language: "bash",
      id: join(kernel, "kernel.json"),
    }),
  );
  await expect(
    readFile(join(root, ".local/share/reflect/jupyter/state.json")),
  ).rejects.toMatchObject({ code: "ENOENT" });
});

it("resolves a non-Python spec with arguments, env and message interruption intact", async () => {
  const path = join(root, "kernel.json");
  await writeFile(
    path,
    JSON.stringify({
      argv: ["/bin/bash", "{resource_dir}/launch", "{connection_file}"],
      env: { EXAMPLE: "${HOME}/data" },
      display_name: "Bash",
      language: "bash",
      interrupt_mode: "message",
    }),
  );
  const helper = join(root, "helper.py");
  await writeFile(helper, JUPYTER_SUPERVISOR);
  const result = JSON.parse(
    execFileSync("python3", [helper], {
      input: JSON.stringify({ operation: "resolve_kernel", path }),
      encoding: "utf8",
      env: { ...process.env, HOME: root },
      timeout: 5000,
    }),
  );
  expect(result).toMatchObject({
    argv: ["/bin/bash", "{resource_dir}/launch", "{connection_file}"],
    env: { EXAMPLE: "${HOME}/data" },
    language: "bash",
    interrupt_mode: "message",
    resource_dir: root,
  });
});

it("resolves packaged ipykernel argv relative to its virtual environment", async () => {
  const env = join(root, "venv");
  const path = join(env, "share/jupyter/kernels/python3/kernel.json");
  await mkdir(join(env, "bin"), { recursive: true });
  await symlink("/bin/true", join(env, "bin/python"));
  await mkdir(join(path, ".."), { recursive: true });
  await writeFile(
    path,
    JSON.stringify({
      argv: ["python", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
      language: "python",
    }),
  );
  const helper = join(root, "helper.py");
  await writeFile(helper, JUPYTER_SUPERVISOR);
  const result = JSON.parse(
    execFileSync("python3", [helper], {
      input: JSON.stringify({ operation: "resolve_kernel", path }),
      encoding: "utf8",
      env: { ...process.env, HOME: root },
      timeout: 5000,
    }),
  );
  expect(result.argv[0]).toBe(join(env, "bin/python"));
});
