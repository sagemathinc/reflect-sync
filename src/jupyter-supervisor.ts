// Shipped with Reflect and installed by content hash. Only Python's standard
// library is needed by the supervisor; the selected interpreter runs the kernel.
export const JUPYTER_SUPERVISOR = String.raw`
import fcntl, hashlib, json, os, pathlib, re, signal, subprocess, sys, time, uuid

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

def status(directory):
    state = read(directory / "state.json")
    if state["boot"] != BOOT:
        return {"status": "lost", "reason": "VM rebooted"}
    # A hung/dead supervisor must not appear ready forever.
    if state["status"] in ("starting", "ready") and time.time() - state["updated"] > 10:
        return {"status": "lost", "reason": "Supervisor heartbeat expired"}
    return state

def supervise(directory):
    request = read(directory / "request.json")
    connection = dict(request["connection"])
    connection["ip"] = "127.0.0.1"
    for port in PORTS:
        connection[port] = 0
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
            process = subprocess.Popen([request["python"], "-m", "ipykernel_launcher", "-f", str(file)],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        started = time.monotonic()
        while True:
            if process.poll() is not None:
                state = {"status": "stopped" if process.returncode == 0 else "failed", "exitCode": process.returncode}
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
                        state = {"status": "ready", "connection": info}
                except (ValueError, OSError):
                    pass
                if time.monotonic() - started > 30:
                    raise RuntimeError("Kernel did not bind its ports within 30 seconds")
            state.update(boot=BOOT, updated=time.time())
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
        state.update(boot=BOOT, updated=time.time())
        write(directory / "state.json", state)
        file.unlink(missing_ok=True)
        (directory / "request.json").unlink(missing_ok=True)

def main(request):
    operation = request["operation"]
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
                previous = read(marker).get("recipe") if marker.exists() else "python"
                if previous != recipe:
                    raise ValueError("Environment already exists with another recipe; choose a new environment name")
                main({"operation": "validate", "python": str(python)})
                return {"python": str(python), "environment": environment.name}
            versions = ROOT / "environment-versions"
            versions.mkdir(exist_ok=True, mode=0o700)
            version = versions / (environment.name + "-" + str(uuid.uuid4()))
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
            python = environment / "bin/python"
        return {"python": str(python), "environment": environment.name}
    if operation == "kernels":
        root = ROOT / "environments"
        return [{"environment": x.name, "python": str(x / "bin/python")} for x in root.glob("*") if (x / "bin/python").exists()]
    directory = ROOT / "sessions" / name(request["session"])
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
    else:
        try:
            print(json.dumps(main(json.load(sys.stdin))))
        except Exception as error:
            print(json.dumps({"error": str(error)}))
`;
