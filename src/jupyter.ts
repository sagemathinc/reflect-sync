import { spawn, type ChildProcess } from "node:child_process";
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
  python: string;
  environment: string;
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

export function sshArgs(host: string): string[] {
  if (!host || host.startsWith("-") || /[\s\x00-\x1f]/.test(host))
    throw Error("Invalid SSH host");
  return [
    "-T",
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=yes",
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
): Promise<string> {
  return await new Promise((resolveResult, reject) => {
    const child = spawn("ssh", [...sshArgs(host), argsJoin(argv)], {
      detached: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
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

export async function listJupyterTargets(): Promise<unknown[]> {
  const dir = join(home(), "targets");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const result: unknown[] = [];
  for (const file of (await readdir(dir)).filter((x) => x.endsWith(".json"))) {
    result.push({
      name: file.slice(0, -5),
      ...JSON.parse(await readFile(join(dir, file), "utf8")),
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
  if (!target.python.startsWith("/"))
    throw Error("Remote interpreter must be an absolute path");
  validateTargetName(target.environment);
  await rpc(target.host, await helper(target.host), {
    operation: "validate",
    python: target.python,
  });
  return await withJupyterTargetLock(home(), name, async () => {
    const targetPath = join(home(), "targets", `${name}.json`);
    try {
      if (JSON.parse(await readFile(targetPath, "utf8")).disabled) {
        throw Error(
          "Target removal is incomplete; retry remove before registering again",
        );
      }
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    }
    await savePrivate(targetPath, target);
    const dataHome =
      process.env.JUPYTER_DATA_DIR ||
      join(
        process.env.XDG_DATA_HOME || join(homedir(), ".local/share"),
        "jupyter",
      );
    const spec = join(dataHome, "kernels", `reflect-${name}`, "kernel.json");
    await savePrivate(spec, {
      argv: [
        ...launcherArgv,
        "jupyter",
        "launch",
        "--target",
        name,
        "--connection-file",
        "{connection_file}",
      ],
      display_name: `Python - ${name}`,
      language: "python",
      interrupt_mode: "signal",
      metadata: { reflect: { remote: true, protocol: 1 } },
    });
    return spec;
  });
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
