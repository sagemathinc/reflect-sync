// Read-only remote probe: no helper installation, environment changes or kernels
// are launched. Remote command failures remain distinct from an empty catalog.
export const JUPYTER_DISCOVERY = String.raw`
import json, os, pathlib, shutil, subprocess, sys

def spec_at(path):
    data = json.loads(path.read_text())
    argv = data.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
        raise ValueError("invalid kernelspec argv")
    return {"id": str(path.resolve()), "name": path.parent.name,
            "display_name": data.get("display_name", path.parent.name),
            "language": data.get("language", ""),
            "interrupt_mode": data.get("interrupt_mode", "signal")}

def probe(extra):
    home = pathlib.Path.home()
    warnings, roots, kernels = [], [], {}
    def add(root):
        path = pathlib.Path(root).expanduser()
        if path not in roots: roots.append(path)
    for root in extra: add(root)
    for root in os.environ.get("JUPYTER_PATH", "").split(os.pathsep):
        if root: add(pathlib.Path(root) / "kernels")
    add(pathlib.Path(os.environ.get("JUPYTER_DATA_DIR", str(home / ".local/share/jupyter"))) / "kernels")
    add(pathlib.Path(sys.prefix) / "share/jupyter/kernels")
    add("/usr/local/share/jupyter/kernels")
    add("/usr/share/jupyter/kernels")
    jupyter = shutil.which("jupyter")
    if jupyter:
        try:
            result = subprocess.run([jupyter, "kernelspec", "list", "--json"], capture_output=True, text=True, timeout=15, check=True)
            for item in json.loads(result.stdout).get("kernelspecs", {}).values():
                path = pathlib.Path(item["resource_dir"]) / "kernel.json"
                kernels[str(path.resolve())] = spec_at(path)
        except Exception as error:
            warnings.append("Jupyter catalog command failed: " + str(error))
    for root in roots:
        try:
            for path in root.glob("*/kernel.json"):
                try: kernels.setdefault(str(path.resolve()), spec_at(path))
                except Exception as error: warnings.append(str(path) + ": " + str(error))
        except OSError as error: warnings.append(str(error))
    environments = []
    managed = home / ".local/share/reflect/jupyter/environments"
    for path in managed.glob("*"):
        if not path.is_dir(): continue
        marker = path / "reflect-ready.json"
        recipe = None
        try: recipe = json.loads(marker.read_text()).get("recipe")
        except (OSError, ValueError): pass
        environments.append({"name": path.name, "recipe": recipe})
        for spec in (path / "share/jupyter/kernels").glob("*/kernel.json"):
            try:
                item = spec_at(spec)
                item["display_name"] += " - " + path.name
                kernels.setdefault(str(spec.resolve()), item)
            except Exception as error: warnings.append(str(error))
    gpu = {"status": "unknown", "reason": "GPU hardware could not be determined"}
    smi = shutil.which("nvidia-smi")
    if smi:
        try:
            result = subprocess.run([smi, "--query-gpu=name,driver_version", "--format=csv,noheader"], capture_output=True, text=True, timeout=10, check=True)
            lines = [x.strip() for x in result.stdout.splitlines() if x.strip()]
            if lines:
                drivers = [int(x.rsplit(",", 1)[1].strip().split(".")[0]) for x in lines]
                gpu = {"status": "available" if min(drivers) >= 570 else "unsupported", "description": "; ".join(lines), "reason": "CUDA 12.8 recipe requires NVIDIA driver 570 or newer" if min(drivers) < 570 else ""}
            else: gpu = {"status": "unknown", "reason": "nvidia-smi returned no devices"}
        except Exception as error: gpu = {"status": "unavailable", "reason": "NVIDIA driver probe failed: " + str(error)}
    else:
        pci = pathlib.Path("/sys/bus/pci/devices")
        if pci.is_dir():
            try:
                devices = list(pci.iterdir())
                graphics = [p for p in devices if (p / "class").read_text().strip().startswith("0x03")]
                gpu = {"status": "unavailable", "reason": "Graphics hardware detected but nvidia-smi is unavailable"} if graphics else {"status": "absent"}
            except OSError as error: gpu = {"status": "unknown", "reason": str(error)}
    return {"platform": os.uname().sysname + " " + os.uname().machine,
            "gpu": gpu, "kernels": list(kernels.values()), "environments": environments,
            "warnings": warnings, "search_paths": [str(p) for p in roots]}

print(json.dumps(probe(json.loads(sys.argv[1]))))
`;
