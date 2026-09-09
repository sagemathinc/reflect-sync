"""Remove a disposable target while its standard-client kernel is running."""
import json
import pathlib
import subprocess
import sys
import uuid
from jupyter_client import KernelManager

root = pathlib.Path(__file__).resolve().parents[1]
cli = ["node", str(root / "dist/cli.js"), "jupyter"]
target = "remove-test-" + uuid.uuid4().hex[:8]
source = json.loads(subprocess.check_output(cli + ["targets"]))
template = next(x for x in source if x["name"] == sys.argv[1])
subprocess.check_call(cli + ["setup", "--target", target, "--host", template["host"],
                            "--python", template["python"], "--environment", template["environment"]])
manager = KernelManager(kernel_name="reflect-" + target)
try:
    manager.start_kernel()
    client = manager.client()
    client.start_channels()
    client.wait_for_ready(timeout=60)
    subprocess.check_call(cli + ["remove", "--target", target], timeout=60)
    assert not any(x["name"] == target for x in json.loads(subprocess.check_output(cli + ["targets"])))
    import time
    for _ in range(100):
        if not manager.is_alive():
            break
        time.sleep(0.1)
    assert not manager.is_alive(), "removed target launcher stayed alive"
    print("PASS: removal stops the running kernel and removes the kernelspec")
finally:
    manager.shutdown_kernel(now=True)
    client.stop_channels()
