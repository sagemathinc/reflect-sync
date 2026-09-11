import { execFile, spawn, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { createHash, randomUUID } from "node:crypto";
import {
  mkdir,
  readFile,
  writeFile,
  rename,
  readdir,
  rm,
} from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { argsJoin } from "./shell-args.js";
import { JUPYTER_SUPERVISOR } from "./jupyter-supervisor.js";
import { downloadJupyterUv } from "./jupyter-bootstrap.js";
import { withJupyterTargetLock } from "./jupyter-lock.js";
import { JUPYTER_DISCOVERY } from "./jupyter-discovery.js";

export const CHANNEL_PORTS = [
  "shell_port",
  "iopub_port",
  "stdin_port",
  "control_port",
  "hb_port",
] as const;
export type ConnectionInfo = {
  transport: "tcp";
  ip: string;
  key: string;
  signature_scheme: string;
} & Record<(typeof CHANNEL_PORTS)[number], number>;
export interface JupyterTarget {
  host: string;
  python?: string;
  environment: string;
  kernel?: {
    argv: string[];
    env?: Record<string, string>;
    display_name: string;
    language: string;
    interrupt_mode?: string;
    resource_dir?: string;
  };
  disabled?: boolean;
}
interface SessionSummary {
  session: string;
  target: string;
  host: string;
  stopped?: boolean;
}
interface RemoteState {
  status: string;
  reason?: string;
  connection?: ConnectionInfo;
  boot?: string;
  error?: string;
}

export function validateConnection(value: unknown): ConnectionInfo {
  const c = value as ConnectionInfo;
  if (
    !c ||
    c.transport !== "tcp" ||
    c.ip !== "127.0.0.1" ||
    typeof c.key !== "string" ||
    c.key.length < 1 ||
    !/^hmac-sha(256|512)$/.test(c.signature_scheme)
  ) {
    throw Error("A signed loopback TCP Jupyter connection file is required");
  }
  const ports = CHANNEL_PORTS.map((p) => c[p]);
  if (
    ports.some((p) => !Number.isInteger(p) || p < 1024 || p > 65535) ||
    new Set(ports).size !== 5
  ) {
    throw Error("Five distinct unprivileged Jupyter ports are required");
  }
  return c;
}

export function validateTargetName(name: string): string {
  if (!/^[a-zA-Z0-9_-]{1,100}$/.test(name))
    throw Error("Invalid target or environment name");
  return name;
}

function home(): string {
  return (
    process.env.REFLECT_JUPYTER_HOME ||
    join(homedir(), ".local/share/reflect/jupyter")
  );
}

export function sshArgs(host: string, trustNewHost = false): string[] {
  if (!host || host.startsWith("-") || /[\s\x00-\x1f]/.test(host))
    throw Error("Invalid SSH host");
  return [
    "-T",
    "-o",
    "BatchMode=yes",
    "-o",
    `StrictHostKeyChecking=${trustNewHost ? "accept-new" : "yes"}`,
    "-o",
    "ConnectTimeout=10",
    "-o",
    "ServerAliveInterval=5",
    "-o",
    "ServerAliveCountMax=2",
    "-o",
    "ExitOnForwardFailure=yes",
    "-o",
    "ForwardAgent=no",
    host,
  ];
}

async function command(
  host: string,
  argv: string[],
  input: string | Uint8Array = "",
  timeout = 20000,
  trustNewHost = false,
): Promise<string> {
  return await new Promise((resolveResult, reject) => {
    const child = spawn(
      "ssh",
      [...sshArgs(host, trustNewHost), argsJoin(argv)],
      {
        detached: true,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    let output = "",
      error = "";
    let expired = false;
    const timer = setTimeout(() => {
      expired = true;
      child.kill("SIGKILL");
    }, timeout);
    child.stdout.on("data", (d) => {
      output += d;
      if (output.length > 1024 * 1024) child.kill("SIGKILL");
    });
    child.stderr.on("data", (d) => {
      error = (error + d).slice(-8000);
    });
    child.stdin.on("error", () => undefined);
    child.stdin.end(input);
    child.once("error", (e) => {
      clearTimeout(timer);
      reject(e);
    });
    child.once("close", (code) => {
      clearTimeout(timer);
      if (code === 0) resolveResult(output);
      else
        reject(
          Error(
            expired
              ? "SSH operation timed out"
              : `SSH operation failed (${code}): ${error.trim() || "remote helper failed"}`,
          ),
        );
    });
  });
}

// Installation is content-addressed, verified remotely, and never relies on a
// remote shell startup file. The supplied bytes come from this Reflect build.
async function installFile(
  host: string,
  content: Uint8Array,
  suffix: string,
  executable = false,
): Promise<string> {
  const hash = createHash("sha256").update(content).digest("hex");
  return (
    await command(
      host,
      [
        "sh",
        "-c",
        String.raw`
set -eu
umask 077
root="$HOME/.local/share/reflect/jupyter/helpers"
mkdir -p "$root"
tmp=$(mktemp "$root/install.XXXXXXXX")
trap 'rm -f "$tmp"' EXIT HUP INT TERM
cat > "$tmp"
printf '%s  %s\n' "$1" "$tmp" | sha256sum -c - >&2
chmod "$3" "$tmp"
path="$root/$1$2"
mv -f "$tmp" "$path"
printf '%s\n' "$path"
`,
        "reflect-install",
        hash,
        suffix,
        executable ? "700" : "600",
      ],
      content,
      120000,
    )
  ).trim();
}

async function helper(host: string): Promise<string> {
  const python = (
    await command(host, [
      "sh",
      "-c",
      'if [ -x "$HOME/.local/share/reflect/jupyter/runtime/bin/python" ]; then printf "%s\\n" "$HOME/.local/share/reflect/jupyter/runtime/bin/python"; else command -v python3; fi',
    ])
  ).trim();
  if (!python.startsWith("/") || /[\r\n]/.test(python))
    throw Error("Remote Python unavailable; run reflect jupyter prepare first");
  return await installFile(
    host,
    Buffer.from(`#!${python}\n${JUPYTER_SUPERVISOR}`),
    ".py",
    true,
  );
}

async function rpc<T = RemoteState>(
  host: string,
  script: string,
  request: unknown,
  timeout?: number,
): Promise<T> {
  const output = await command(
    host,
    [script],
    JSON.stringify(request),
    timeout,
  );
  const result = JSON.parse(output);
  if (result.error) throw Error(result.error);
  return result;
}

export async function prepareJupyter(
  host: string,
  environment: string,
  uvPath?: string,
  recipe = "python",
): Promise<JupyterTarget> {
  validateTargetName(environment);
  if (!["python", "pytorch-cu128"].includes(recipe))
    throw Error("Unknown kernel recipe");
  const platform = (await command(host, ["uname", "-sm"])).trim();
  if (!/^Linux (x86_64|aarch64)$/.test(platform))
    throw Error(`Unsupported remote platform: ${platform}`);
  const uv = await installFile(
    host,
    uvPath
      ? await readFile(uvPath)
      : await downloadJupyterUv(platform.split(" ")[1]),
    "-uv",
    true,
  );
  await command(
    host,
    [
      "sh",
      "-c",
      String.raw`
set -eu
umask 077
command -v python3 >/dev/null && exit 0
root="$HOME/.local/share/reflect/jupyter"
mkdir -p "$root"
exec 9>"$root/runtime.lock"
flock -w 170 9
[ -x "$root/runtime/bin/python" ] && exit 0
version=$(mktemp -d "$root/runtime.XXXXXXXX")
trap 'rm -rf "$version"' EXIT HUP INT TERM
"$1" venv --python 3.12.11 "$version" >&2
"$version/bin/python" -c 'import json, fcntl, ssl' >&2
ln -s "$version" "$root/runtime"
trap - EXIT HUP INT TERM
`,
      "reflect-python",
      uv,
    ],
    "",
    180000,
  );
  const script = await helper(host);
  const result = await rpc<{ python: string }>(
    host,
    script,
    { operation: "prepare", environment, uv, recipe },
    600000,
  );
  return { host, environment, python: result.python };
}

export async function listJupyterEnvironments(host: string): Promise<unknown> {
  return await rpc(host, await helper(host), { operation: "kernels" });
}

export interface JupyterProbe {
  platform: string;
  gpu: {
    status: "available" | "absent" | "unknown" | "unavailable" | "unsupported";
    reason?: string;
    description?: string;
  };
  kernels: {
    id: string;
    name: string;
    display_name: string;
    language: string;
    interrupt_mode: string;
  }[];
  environments: { name: string; recipe: string | null }[];
  warnings: string[];
  search_paths: string[];
  suggested_name?: string;
}

export async function probeJupyter(
  host: string,
  paths: string[] = [],
  trustNewHost = false,
): Promise<JupyterProbe> {
  // Explicit first-use enrollment only. Later commands remain strict, and
  // accept-new itself refuses a changed key already recorded by OpenSSH.
  if (trustNewHost) await command(host, ["true"], "", 20000, true);
  // This first command fails immediately and distinctly for SSH/auth errors.
  const python = (
    await command(host, [
      "sh",
      "-c",
      'command -v python3 || { p="$HOME/.local/share/reflect/jupyter/runtime/bin/python"; if [ -x "$p" ]; then printf "%s" "$p"; fi; }',
    ])
  ).trim();
  let result: JupyterProbe;
  if (!python) {
    result = {
      platform: (await command(host, ["uname", "-sm"])).trim(),
      gpu: {
        status: "unknown",
        reason:
          "Python is unavailable; hardware and kernel discovery require a Python 3 interpreter",
      },
      kernels: [],
      environments: [],
      search_paths: [],
      warnings: [
        "No Python 3 available for discovery. Explicit Python preparation is still available.",
      ],
    };
  } else {
    if (!python.startsWith("/") || /[\r\n]/.test(python))
      throw Error("Invalid remote Python path");
    result = JSON.parse(
      await command(
        host,
        [python, "-", JSON.stringify(paths)],
        JUPYTER_DISCOVERY,
        45000,
      ),
    );
  }
  result.suggested_name = await availableJupyterName(host);
  return result;
}

export function jupyterName(host: string): string {
  const normalized = host.replace(/[^a-zA-Z0-9_-]+/g, "-");
  let start = 0;
  let end = normalized.length;
  while (start < end && normalized[start] === "-") start++;
  while (end > start && normalized[end - 1] === "-") end--;
  return (
    normalized.slice(start, Math.min(end, start + 80)).toLowerCase() || "remote"
  );
}

async function occupiedNames(): Promise<Set<string>> {
  const dirs = [
    process.env.JUPYTER_DATA_DIR ||
      join(
        process.env.XDG_DATA_HOME || join(homedir(), ".local/share"),
        "jupyter",
      ),
    ...(process.env.JUPYTER_PATH || "").split(":"),
    "/usr/local/share/jupyter",
    "/usr/share/jupyter",
  ];
  const names = new Set<string>();
  try {
    const { stdout } = await promisify(execFile)(
      "jupyter",
      ["kernelspec", "list", "--json"],
      { timeout: 15000, maxBuffer: 1024 * 1024 },
    );
    for (const name of Object.keys(JSON.parse(stdout).kernelspecs || {}))
      names.add(name.toLowerCase());
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT")
      throw Error(`Local kernelspec discovery failed: ${String(err)}`);
  }
  for (const dir of dirs.filter(Boolean)) {
    try {
      for (const name of await readdir(join(dir, "kernels")))
        names.add(name.toLowerCase());
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    }
  }
  try {
    for (const file of await readdir(join(home(), "targets")))
      if (file.endsWith(".json"))
        names.add(`reflect-${file.slice(0, -5).toLowerCase()}`);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  }
  return names;
}

export async function assertJupyterNameAvailable(name: string): Promise<void> {
  validateTargetName(name);
  const names = await occupiedNames();
  if (
    names.has(name.toLowerCase()) ||
    names.has(`reflect-${name.toLowerCase()}`)
  )
    throw Error("Kernel name already exists; choose another name");
}

export async function availableJupyterName(host: string): Promise<string> {
  const names = await occupiedNames();
  const base = jupyterName(host);
  for (let n = 1; ; n++) {
    const name = n === 1 ? base : `${base}-${n}`;
    if (!names.has(name) && !names.has(`reflect-${name}`)) return name;
  }
}

export async function existingJupyterKernel(
  host: string,
  path: string,
): Promise<JupyterTarget> {
  if (!path.startsWith("/"))
    throw Error("Remote kernelspec requires an absolute path");
  const kernel = await rpc<NonNullable<JupyterTarget["kernel"]>>(
    host,
    await helper(host),
    { operation: "resolve_kernel", path },
  );
  return { host, environment: "existing", kernel };
}

export async function listJupyterTargets(): Promise<
  {
    name: string;
    host: string;
    environment: string;
    python?: string;
    disabled?: boolean;
  }[]
> {
  const dir = join(home(), "targets");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const result: Awaited<ReturnType<typeof listJupyterTargets>> = [];
  for (const file of (await readdir(dir)).filter((x) => x.endsWith(".json"))) {
    const target: JupyterTarget = JSON.parse(
      await readFile(join(dir, file), "utf8"),
    );
    result.push({
      name: file.slice(0, -5),
      host: target.host,
      environment: target.environment,
      python: target.python,
      disabled: target.disabled,
    });
  }
  return result;
}

export async function listJupyterSessions(): Promise<SessionSummary[]> {
  const dir = join(home(), "sessions");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const result: SessionSummary[] = [];
  for (const file of (await readdir(dir)).filter((x) => x.endsWith(".json"))) {
    const id = file.slice(0, -5);
    const record = JSON.parse(await readFile(join(dir, file), "utf8"));
    result.push({
      session: id,
      target: record.targetName,
      host: record.host,
      stopped: record.stopped,
    });
  }
  return result;
}

async function savePrivate(path: string, value: unknown): Promise<void> {
  await mkdir(join(path, ".."), { recursive: true, mode: 0o700 });
  const tmp = `${path}.${randomUUID()}`;
  await writeFile(tmp, JSON.stringify(value, null, 2), { mode: 0o600 });
  await rename(tmp, path);
}

export async function registerJupyterTarget(
  name: string,
  target: JupyterTarget,
  launcherArgv: string[],
): Promise<string> {
  validateTargetName(name);
  sshArgs(target.host);
  if (!target.kernel && !target.python?.startsWith("/"))
    throw Error("Remote interpreter must be an absolute path");
  validateTargetName(target.environment);
  if (!target.kernel)
    await rpc(target.host, await helper(target.host), {
      operation: "validate",
      python: target.python,
    });
  return await withJupyterTargetLock(
    join(home(), "registry"),
    "registration",
    () =>
      withJupyterTargetLock(home(), name, async () => {
        await assertJupyterNameAvailable(name);
        const targetPath = join(home(), "targets", `${name}.json`);
        const dataHome =
          process.env.JUPYTER_DATA_DIR ||
          join(
            process.env.XDG_DATA_HOME || join(homedir(), ".local/share"),
            "jupyter",
          );
        const spec = join(
          dataHome,
          "kernels",
          `reflect-${name}`,
          "kernel.json",
        );
        const specification = {
          argv: [
            ...launcherArgv,
            "jupyter",
            "launch",
            "--target",
            name,
            "--connection-file",
            "{connection_file}",
          ],
          display_name: `${target.kernel?.display_name || "Python"} - ${name}`,
          language: target.kernel?.language || "python",
          interrupt_mode: target.kernel?.interrupt_mode || "signal",
          metadata: { reflect: { remote: true, protocol: 1 } },
        };
        // Reserve the directory exclusively, including against other kernelspec installers.
        await mkdir(join(dataHome, "kernels"), { recursive: true });
        await mkdir(join(spec, ".."), { mode: 0o700 });
        let createdTarget = false;
        try {
          await mkdir(join(targetPath, ".."), { recursive: true, mode: 0o700 });
          await writeFile(targetPath, JSON.stringify(target, null, 2), {
            mode: 0o600,
            flag: "wx",
          });
          createdTarget = true;
          await savePrivate(spec, specification);
        } catch (err) {
          if (createdTarget) await rm(targetPath, { force: true });
          await rm(join(spec, ".."), { recursive: true, force: true });
          throw err;
        }
        return spec;
      }),
  );
}

async function stopRemote(
  host: string,
  script: string,
  session: string,
): Promise<void> {
  await rpc(host, script, { operation: "stop", session });
  const deadline = Date.now() + 15000;
  while (true) {
    const state = await rpc(host, script, { operation: "status", session });
    if (
      ["stopped", "failed"].includes(state.status) ||
      state.reason === "VM rebooted"
    )
      return;
    if (state.status === "lost" || Date.now() > deadline)
      throw Error(
        "Remote termination is unconfirmed; retry after the lease expires",
      );
    await delay(200);
  }
}

export async function removeJupyterTarget(name: string): Promise<void> {
  validateTargetName(name);
  await withJupyterTargetLock(home(), name, async () => {
    const path = join(home(), "targets", `${name}.json`);
    const target = JSON.parse(await readFile(path, "utf8"));
    await savePrivate(path, { ...target, disabled: true });
    for (const session of await listJupyterSessions()) {
      if (session.target !== name || session.stopped) continue;
      const record = JSON.parse(
        await readFile(
          join(home(), "sessions", `${session.session}.json`),
          "utf8",
        ),
      );
      await stopRemote(record.host, record.script, session.session);
      await savePrivate(join(home(), "sessions", `${session.session}.json`), {
        ...record,
        stopped: true,
      });
    }
    const dataHome =
      process.env.JUPYTER_DATA_DIR ||
      join(
        process.env.XDG_DATA_HOME || join(homedir(), ".local/share"),
        "jupyter",
      );
    await rm(join(dataHome, "kernels", `reflect-${name}`), {
      recursive: true,
      force: true,
    });
    await rm(path);
  });
}

export async function jupyterSessionCommand(
  id: string,
  operation: "status" | "interrupt" | "stop",
): Promise<unknown> {
  validateTargetName(id);
  const record = JSON.parse(
    await readFile(join(home(), "sessions", `${id}.json`), "utf8"),
  );
  if (operation === "stop") {
    await stopRemote(record.host, record.script, id);
    await savePrivate(join(home(), "sessions", `${id}.json`), {
      ...record,
      stopped: true,
    });
  }
  const state = await rpc(record.host, record.script, {
    operation,
    session: id,
  });
  const { connection: _connection, ...publicState } = state;
  return publicState;
}

export async function launchJupyter(
  targetName: string,
  connectionFile: string,
  leaseSeconds = 60,
): Promise<void> {
  validateTargetName(targetName);
  if (
    !Number.isInteger(leaseSeconds) ||
    leaseSeconds < 15 ||
    leaseSeconds > 300
  )
    throw Error("Lease must be 15-300 seconds");
  const connection = validateConnection(
    JSON.parse(await readFile(connectionFile, "utf8")),
  );
  const target: JupyterTarget = JSON.parse(
    await readFile(join(home(), "targets", `${targetName}.json`), "utf8"),
  );
  if (target.disabled) throw Error("Remote kernel target is being removed");
  const session = randomUUID();
  let script: string | undefined;
  let tunnel: ChildProcess | undefined;
  let stopping = false;
  let interruption = false;
  let started = false;
  let boot: string | undefined;
  let disconnectedAt: number | undefined;
  let tunnelFailures = 0;
  const onStop = () => {
    stopping = true;
  };
  const onInterrupt = () => {
    interruption = true;
  };
  process.on("SIGTERM", onStop);
  process.on("SIGINT", onInterrupt);
  try {
    script = await helper(target.host);
    if (stopping) return;
    const scriptPath = script;
    await withJupyterTargetLock(home(), targetName, async () => {
      const currentTarget = JSON.parse(
        await readFile(join(home(), "targets", `${targetName}.json`), "utf8"),
      );
      if (JSON.stringify(currentTarget) !== JSON.stringify(target))
        throw Error("Target changed during launch; select it again");
      if (stopping) return;
      await savePrivate(join(home(), "sessions", `${session}.json`), {
        host: target.host,
        script,
        targetName,
        launcherPid: process.pid,
      });
      const request = {
        operation: "start",
        session,
        connection,
        python: target.python,
        kernel: target.kernel,
        leaseSeconds,
      };
      // An ambiguous SSH response must not create a second remote kernel.
      started = true;
      try {
        await rpc(target.host, scriptPath, request);
      } catch {
        if (!stopping) await rpc(target.host, scriptPath, request);
      }
    });
    process.stderr.write(`Reflect kernel session ${session}\n`);
    const deadline = Date.now() + 40000;
    while (!stopping) {
      let state: RemoteState;
      try {
        state = await rpc(target.host, script, { operation: "renew", session });
        disconnectedAt = undefined;
      } catch (error) {
        disconnectedAt ??= Date.now();
        if (Date.now() - disconnectedAt > leaseSeconds * 1000) throw error;
        process.stderr.write(
          "Reflect: SSH disconnected; execution state may be uncertain. Retrying.\n",
        );
        await delay(1000);
        continue;
      }
      if (stopping) break;
      if (boot && boot !== state.boot)
        throw Error("Remote kernel incarnation changed");
      boot = state.boot;
      if (state.status === "stopped") break;
      if (state.status === "lost" || state.status === "failed")
        throw Error(state.reason || `Remote kernel ${state.status}`);
      if (state.status === "starting" && Date.now() > deadline)
        throw Error("Remote kernel startup timed out");
      if (interruption) {
        interruption = false;
        await rpc(target.host, script, { operation: "interrupt", session });
      }
      if (state.status === "ready" && !tunnel) {
        if (tunnelFailures >= 3)
          throw Error(
            "SSH forwarding repeatedly failed; check local port conflicts and SSH forwarding policy",
          );
        const remote = state.connection!;
        const args = sshArgs(target.host);
        const host = args.pop()!;
        for (const port of CHANNEL_PORTS)
          args.push(
            "-L",
            `127.0.0.1:${connection[port]}:127.0.0.1:${remote[port]}`,
          );
        // EOF from the launcher closes this remote command and thus SSH. Unlike
        // detached ssh -N, the tunnel cannot outlive a SIGKILLed launcher forever.
        const child = spawn(
          "ssh",
          [...args, host, argsJoin(["sh", "-c", "cat >/dev/null"])],
          { detached: true, stdio: ["pipe", "ignore", "pipe"] },
        );
        const opened = Date.now();
        tunnel = child;
        // Detached SSH children do not receive the kernel launcher's SIGINT.
        let lastDiagnostic = 0;
        child.stderr?.on("data", (data) => {
          if (Date.now() - lastDiagnostic > 1000) {
            lastDiagnostic = Date.now();
            process.stderr.write(data.toString().slice(-2000));
          }
        });
        child.on("error", () => {
          if (tunnel === child) tunnel = undefined;
        });
        child.on("exit", () => {
          tunnelFailures = Date.now() - opened < 2000 ? tunnelFailures + 1 : 0;
          if (tunnel === child) tunnel = undefined;
          if (!stopping)
            process.stderr.write(
              "Reflect: kernel tunnel disconnected; in-flight output may be lost. Reconnecting without replay.\n",
            );
        });
      }
      await delay(state.status === "starting" ? 200 : 1000);
    }
  } finally {
    tunnel?.kill("SIGTERM");
    if (script && started) {
      try {
        await stopRemote(target.host, script, session);
        const path = join(home(), "sessions", `${session}.json`);
        const record = JSON.parse(await readFile(path, "utf8"));
        await savePrivate(path, { ...record, stopped: true });
      } catch {
        process.stderr.write(
          "Reflect: remote stop not confirmed; lease expiry will clean up the kernel.\n",
        );
        // Do not let a standard client restart while the previous lease may
        // still be alive. No renewals are performed during this grace period.
        await delay((leaseSeconds + 2) * 1000);
      }
    }
    process.off("SIGTERM", onStop);
    process.off("SIGINT", onInterrupt);
  }
}

export function defaultLauncherArgv(): string[] {
  return [
    process.execPath,
    resolve(process.env.REFLECT_ENTRY || process.argv[1]),
  ];
}
