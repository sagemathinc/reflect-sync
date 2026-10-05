// Shipped with Reflect and installed by content hash. Only Python's standard
// library is needed by the supervisor; the selected interpreter runs the kernel.
export const JUPYTER_SUPERVISOR = String.raw`
import fcntl, hashlib, json, os, pathlib, re, selectors, shutil, signal, socket, string, subprocess, sys, time, uuid

os.umask(0o077)
ROOT = pathlib.Path.home() / ".local/share/reflect/jupyter"
ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
BOOT = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()
PORTS = ["shell_port", "iopub_port", "stdin_port", "control_port", "hb_port"]

def name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", value):
        raise ValueError("Invalid identifier")
    return value

def write(path, value):
    tmp = path.with_name(path.name + ".tmp-" + str(os.getpid()))
    tmp.write_text(json.dumps(value))
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)

def read(path):
    return json.loads(path.read_text())

def locked(path):
    lock = open(path, "a+")
    fcntl.flock(lock, fcntl.LOCK_EX)
    return lock

def resolve_kernel(path):
    path = pathlib.Path(path)
    if not path.is_absolute(): raise ValueError("Absolute kernelspec path required")
    spec = read(path)
    argv = spec.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(x, str) and "\x00" not in x for x in argv):
        raise ValueError("Invalid kernelspec argv")
    if not any("{connection_file}" in x for x in argv): raise ValueError("Kernelspec must accept {connection_file}")
    if spec.get("interrupt_mode", "signal") not in ("signal", "message"): raise ValueError("Unsupported interrupt mode")
    env = spec.get("env", {})
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()): raise ValueError("Invalid kernelspec environment")
    spec["resource_dir"] = str(path.parent)
    # ipykernel's packaged kernelspec uses 'python'; resolve it in its own
    # environment rather than accidentally selecting the system interpreter.
    prefix = path.parent.parent.parent.parent.parent
    if argv[0] in ("python", "python3") and (prefix / "bin/python").is_file():
        argv[0] = str(prefix / "bin/python")
    executable = argv[0].replace("{resource_dir}", str(path.parent))
    effective = dict(os.environ)
    effective.update({k: string.Template(v).safe_substitute(os.environ) for k, v in env.items()})
    if not shutil.which(executable, path=effective.get("PATH")): raise ValueError("Kernel executable unavailable: " + executable)
    return {"argv": argv, "env": env, "language": spec.get("language", ""), "display_name": spec.get("display_name", path.parent.name), "interrupt_mode": spec.get("interrupt_mode", "signal"), "resource_dir": str(path.parent)}

def status(directory):
    state = read(directory / "state.json")
    if state["boot"] != BOOT:
        return {"status": "lost", "reason": "VM rebooted"}
    # A hung/dead supervisor must not appear ready forever.
    if state["status"] in ("starting", "ready") and time.time() - state["updated"] > 10:
        guard = directory / "guard.json"
        if guard.exists():
            identity = read(guard)
            try:
                current = pathlib.Path("/proc/%s/stat" % identity["pid"]).read_text().rsplit(")", 1)[1].split()
                gone = current[0] == "Z" or current[19] != identity["start"]
            except FileNotFoundError:
                gone = True
            if gone:
                (directory / "request.json").unlink(missing_ok=True)
                (directory / "kernel.json").unlink(missing_ok=True)
                return {"status": "failed", "reason": "Supervisor exited; kernel process group stopped"}
        return {"status": "lost", "reason": "Supervisor heartbeat expired"}
    return state

def guard_kernel(directory):
    # This process is the group leader until every owned process is killed.
    # A lifetime pipe and independent lease check also cover supervisor death.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    identity = pathlib.Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
    write(directory / "guard.json", {"pid": os.getpid(), "start": identity[19]})
    request = read(directory / "request.json")
    selector = selectors.DefaultSelector()
    selector.register(sys.stdin, selectors.EVENT_READ)
    try:
        def child_signals():
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
        spec = request.get("kernel")
        env = dict(os.environ)
        if spec:
            argv = [x.replace("{connection_file}", str(directory / "kernel.json")).replace("{resource_dir}", spec.get("resource_dir", "")) for x in spec["argv"]]
            env.update({k: string.Template(v).safe_substitute(os.environ) for k, v in spec.get("env", {}).items()})
        else: argv = [request["python"], "-m", "ipykernel_launcher", "-f", str(directory / "kernel.json")]
        process = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, preexec_fn=child_signals)
        while True:
            code = process.poll()
            if code is not None:
                write(directory / "kernel-exit.json", {"code": code})
                return
            if selector.select(timeout=0.2) or time.time() > read(directory / "lease.json")["until"]:
                return
    except Exception as error:
        write(directory / "kernel-exit.json", {"code": 1, "reason": str(error)})
    finally:
        # Killing this group also kills us; the supervisor observes termination.
        os.killpg(os.getpid(), signal.SIGKILL)

def supervise(directory):
    request = read(directory / "request.json")
    connection = dict(request["connection"])
    connection["ip"] = "127.0.0.1"
    for port in PORTS:
        connection[port] = 0
    if request.get("kernel"):
        # General kernels expect assigned ports, unlike ipykernel which can
        # rewrite zero ports. Hold all reservations until allocation is complete.
        reserved = []
        try:
            for port in PORTS:
                s = socket.socket()
                s.bind(("127.0.0.1", 0))
                reserved.append(s)
                connection[port] = s.getsockname()[1]
        finally:
            for s in reserved: s.close()
    file = directory / "kernel.json"
    write(file, connection)
    process = None
    state = {"status": "starting", "boot": BOOT, "updated": time.time()}
    stop = False
    def stopping(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, stopping)
    signal.signal(signal.SIGINT, stopping)
    try:
        with open(directory / "kernel.log", "ab") as log:
            process = subprocess.Popen([sys.executable, __file__, "guard", str(directory)],
                stdin=subprocess.PIPE, stdout=log, stderr=log, start_new_session=True)
        started = time.monotonic()
        while True:
            if process.poll() is not None:
                result = directory / "kernel-exit.json"
                details = read(result) if result.exists() else {}
                code = details.get("code", process.returncode)
                expired = time.time() > read(directory / "lease.json")["until"]
                state = {"status": "stopped" if code == 0 or expired else "failed", "exitCode": code}
                if expired:
                    state["reason"] = "lease expired"
                elif details.get("reason"):
                    state["reason"] = details["reason"]
                break
            with locked(directory / "lock"):
                lease = read(directory / "lease.json")
                command = directory / "interrupt"
                if command.exists():
                    command.unlink()
                    os.killpg(process.pid, signal.SIGINT)
                stop = stop or (directory / "stop").exists()
            if stop or time.time() > lease["until"]:
                state = {"status": "stopped", "reason": "stopped" if stop else "lease expired"}
                break
            if state["status"] == "starting":
                try:
                    info = read(file)
                    if all(isinstance(info.get(p), int) and info[p] > 0 for p in PORTS):
                        if request.get("kernel"):
                            for p in PORTS:
                                with socket.create_connection(("127.0.0.1", info[p]), timeout=0.1): pass
                        state = {"status": "ready", "connection": info}
                except (ValueError, OSError):
                    pass
                if time.monotonic() - started > 30:
                    raise RuntimeError("Kernel did not bind its ports within 30 seconds")
            state.update(boot=BOOT, updated=time.time(), supervisorPid=os.getpid())
            write(directory / "state.json", state)
            time.sleep(0.2)
    except Exception as error:
        state = {"status": "failed", "reason": str(error)}
    finally:
        if process is not None:
            # Kill the entire owned process group, including background children.
            try:
                os.killpg(process.pid, signal.SIGTERM)
                time.sleep(0.5)
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            process.stdin.close()
        state.update(boot=BOOT, updated=time.time())
        write(directory / "state.json", state)
        file.unlink(missing_ok=True)
        (directory / "request.json").unlink(missing_ok=True)

def main(request):
    operation = request["operation"]
    if operation == "resolve_kernel": return resolve_kernel(request["path"])
    if operation == "validate":
        # A real handshake, not just an import, before publishing a kernelspec.
        code = '''
import sys
from jupyter_client import KernelManager
k = KernelManager()
k.kernel_spec.argv = [sys.executable, '-m', 'ipykernel_launcher', '-f', '{connection_file}']
k.start_kernel()
c = k.client()
c.start_channels()
try:
    c.wait_for_ready(timeout=20)
finally:
    k.shutdown_kernel(now=True)
    c.stop_channels()
'''
        subprocess.run([request["python"], "-c", code], check=True, timeout=30, stdout=sys.stderr)
        return {"ready": True}
    if operation == "prepare":
        # Publish a symlink only after validation. A failed install is never
        # selectable, and venv scripts keep their original absolute prefix.
        environment = ROOT / "environments" / name(request["environment"])
        environment.parent.mkdir(exist_ok=True, mode=0o700)
        with locked(environment.parent / (environment.name + ".lock")):
            python = environment / "bin/python"
            uv = request["uv"]
            recipe = request.get("recipe", "python")
            if recipe not in ("python", "pytorch-cu128"):
                raise ValueError("Unknown kernel recipe")
            if python.exists():
                marker = environment / "reflect-ready.json"
                previous = read(marker).get("recipe") if marker.exists() else None
                if previous != recipe:
                    raise ValueError("Environment already exists with another recipe; choose a new environment name")
                main({"operation": "validate", "python": str(python)})
                return {"python": str(python), "environment": environment.name}
            if os.path.lexists(environment):
                raise ValueError("Environment name already exists but is not ready; choose a new environment name")
            versions = ROOT / "environment-versions"
            versions.mkdir(exist_ok=True, mode=0o700)
            version = versions / (environment.name + "-" + str(uuid.uuid4()))
            try:
                subprocess.run([uv, "venv", "--python", sys.executable, str(version)], check=True, stdout=sys.stderr)
                python = version / "bin/python"
                subprocess.run([uv, "pip", "install", "--python", str(python), "ipykernel==6.30.1", "ipywidgets==8.1.7"], check=True, stdout=sys.stderr)
                if recipe == "pytorch-cu128":
                    subprocess.run(["nvidia-smi"], check=True, timeout=20, stdout=sys.stderr)
                    subprocess.run([uv, "pip", "install", "--python", str(python), "numpy==2.2.6"], check=True, stdout=sys.stderr)
                    subprocess.run([uv, "pip", "install", "--python", str(python), "torch==2.8.0", "--index-url", "https://download.pytorch.org/whl/cu128"], check=True, stdout=sys.stderr)
                    subprocess.run([str(python), "-c", "import torch; assert torch.cuda.is_available(); x=torch.ones((32,32),device='cuda'); assert (x@x).sum().item()==32768; torch.cuda.synchronize()"], check=True, timeout=60, stdout=sys.stderr)
                main({"operation": "validate", "python": str(python)})
                write(version / "reflect-ready.json", {"recipe": recipe})
                environment.symlink_to(version, target_is_directory=True)
            except BaseException:
                shutil.rmtree(version, ignore_errors=True)
                raise
            python = environment / "bin/python"
        return {"python": str(python), "environment": environment.name}
    if operation == "kernels":
        root = ROOT / "environments"
        return [{"environment": x.name, "python": str(x / "bin/python")} for x in root.glob("*") if (x / "bin/python").exists()]
    directory = ROOT / "sessions" / name(request["session"])
    if operation == "stop":
        directory.parent.mkdir(exist_ok=True, mode=0o700)
        # Serialize cancellation with admission, including an uncertain start
        # whose SSH command has not reached the supervisor yet.
        with locked(directory.parent / (directory.name + ".lock")):
            if not directory.exists():
                directory.mkdir(mode=0o700)
                (directory / "request.sha256").write_text("cancelled-before-start")
                write(directory / "state.json", {"status": "stopped", "boot": BOOT, "updated": time.time()})
                return status(directory)
    if operation == "start":
        directory.parent.mkdir(exist_ok=True, mode=0o700)
        # Retries of this launch must reuse its existing supervisor.
        with locked(directory.parent / (directory.name + ".lock")):
            fingerprint = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
            if directory.exists():
                existing = (directory / "request.sha256").read_text()
                if existing != fingerprint:
                    raise ValueError("Session identifier belongs to a different request")
                return status(directory)
            directory.mkdir(mode=0o700)
            (directory / "request.sha256").write_text(fingerprint)
            write(directory / "request.json", request)
            write(directory / "lease.json", {"until": time.time() + request["leaseSeconds"]})
            write(directory / "state.json", {"status": "starting", "boot": BOOT, "updated": time.time()})
            with open(directory / "supervisor.log", "ab") as log:
                subprocess.Popen([sys.executable, __file__, "supervise", str(directory)],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True, close_fds=True)
        return status(directory)
    with locked(directory / "lock"):
        state = status(directory)
        if state["status"] not in ("starting", "ready"):
            return state
        if operation == "renew":
            lease = read(directory / "lease.json")
            if lease["until"] < time.time():
                return {"status": "lost", "reason": "Lease expired"}
            seconds = read(directory / "request.json")["leaseSeconds"]
            write(directory / "lease.json", {"until": time.time() + seconds})
        elif operation in ("stop", "interrupt"):
            (directory / operation).touch(mode=0o600)
        elif operation != "status":
            raise ValueError("Unknown operation")
        return state

if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "supervise":
        supervise(pathlib.Path(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == "guard":
        guard_kernel(pathlib.Path(sys.argv[2]))
    else:
        try:
            print(json.dumps(main(json.load(sys.stdin))))
        except Exception as error:
            print(json.dumps({"error": str(error)}))
`;
