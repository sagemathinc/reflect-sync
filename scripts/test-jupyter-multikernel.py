"""Live tests for isolation, binary comms, widgets, and transient tunnel loss."""
import os
import signal
import subprocess
import sys
import time
from jupyter_client import KernelManager
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path

spec = spec_from_file_location("client_test", Path(__file__).with_name("test-jupyter-client.py"))
module = module_from_spec(spec)
spec.loader.exec_module(module)
execute = module.execute

managers = []
clients = []
try:
    for i in range(2):
        manager = KernelManager(kernel_name=sys.argv[1])
        managers.append(manager)
        manager.start_kernel()
        client = manager.client()
        clients.append(client)
        client.start_channels()
        client.wait_for_ready(timeout=60)
        execute(client, "isolation = " + str(i))
    assert managers[0].get_connection_info()["key"] != managers[1].get_connection_info()["key"]
    assert managers[0].get_connection_info()["shell_port"] != managers[1].get_connection_info()["shell_port"]
    for i, client in enumerate(clients):
        outputs = execute(client, "print(isolation)")
        assert any(m["content"].get("text", "").strip() == str(i) for m in outputs), outputs
    outputs = execute(clients[0], '''
from comm import create_comm
c = create_comm(target_name='reflect-test', data={'test': True}, buffers=[b'\\x00\\xffreflect'])
c.close()
import ipywidgets as w
from IPython.display import display
display(w.IntSlider(value=42))
''')
    assert any(m["msg_type"] == "comm_open" and any(bytes(b) == b'\x00\xffreflect' for b in m.get("buffers", [])) for m in outputs), outputs
    assert any("application/vnd.jupyter.widget-view+json" in m["content"].get("data", {}) for m in outputs), outputs
    pid = managers[0].provisioner.process.pid
    rows = subprocess.check_output(["ps", "--ppid", str(pid), "-o", "pid=,args="], text=True)
    tunnels = [int(row.strip().split()[0]) for row in rows.splitlines() if " -L " in row]
    assert len(tunnels) == 1, rows
    os.kill(tunnels[0], signal.SIGKILL)
    time.sleep(5)
    clients[0].wait_for_ready(timeout=60)
    # No replay: submit new work only after reconnect, and check the old state.
    outputs = execute(clients[0], "print('SURVIVED', isolation)")
    assert any("SURVIVED 0" in m["content"].get("text", "") for m in outputs), outputs
    print("PASS independent kernels, binary comms, widget initialization, tunnel reconnect")
finally:
    for manager in managers:
        if manager.has_kernel:
            manager.shutdown_kernel(now=False)
    for client in clients:
        client.stop_channels()
