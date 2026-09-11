"""Launch an owned test kernel, SIGKILL its local launcher, verify lease cleanup."""
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
from jupyter_client import BlockingKernelClient
from jupyter_client.connect import write_connection_file

cli = str(pathlib.Path(__file__).resolve().parent.parent / "dist/cli.js")
failure = sys.argv[2] if len(sys.argv) > 2 else "launcher"
with tempfile.TemporaryDirectory() as directory:
    connection = str(pathlib.Path(directory) / "connection.json")
    write_connection_file(connection, key=b"synthetic-test-key")
    with open(pathlib.Path(directory) / "launcher.log", "w+") as log:
        launcher = subprocess.Popen(["node", cli, "jupyter", "launch", "--target", sys.argv[1],
            "--connection-file", connection, "--lease-seconds", "15"], stderr=log, start_new_session=True)
        client = BlockingKernelClient(connection_file=connection)
        session = None
        try:
            client.load_connection_file()
            client.start_channels()
            # A standalone client has no KernelManager's process-liveness check;
            # allow the launcher to bind the heartbeat forward before readiness.
            deadline = time.monotonic() + 45
            # is_beating starts optimistically true before the first ping has
            # completed. Wait for Reflect transport setup before consulting it.
            while session is None:
                log.seek(0)
                session = next((line.split()[-1] for line in log if line.startswith("Reflect kernel session ")), None)
                assert launcher.poll() is None, "Launcher exited during startup"
                assert time.monotonic() < deadline, "Supervisor never started"
                time.sleep(0.2)
            time.sleep(5)
            while not client.hb_channel.is_beating():
                assert launcher.poll() is None, "Launcher exited during startup"
                assert time.monotonic() < deadline, "Heartbeat never became ready"
                time.sleep(0.2)
            client.wait_for_ready(timeout=60)
            log.seek(0)
            session = next(line.split()[-1] for line in log if line.startswith("Reflect kernel session "))
            if failure == "supervisor":
                state = json.loads(subprocess.check_output(["node", cli, "jupyter", "status", session, "--json"]))
                pid = int(state["supervisorPid"])
                assert pid > 1
                targets = json.loads(subprocess.check_output(["node", cli, "jupyter", "target", "list", "--json"]))
                host = next(t["host"] for t in targets if t["name"] == sys.argv[1])
                subprocess.check_call(["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", host, "kill -KILL %s" % pid])
            elif failure == "kernel":
                client.execute("import os; os._exit(7)")
            else:
                os.killpg(launcher.pid, signal.SIGKILL)
                launcher.wait(timeout=5)
            deadline = time.monotonic() + 30
            while True:
                state = json.loads(subprocess.check_output(["node", cli, "jupyter", "status", session, "--json"]))
                if state["status"] in ("stopped", "failed"):
                    if failure == "launcher":
                        assert state["reason"] == "lease expired", state
                    else:
                        assert state["status"] == "failed", state
                        launcher.wait(timeout=30)
                    print(json.dumps(state))
                    break
                assert time.monotonic() < deadline, state
                time.sleep(1)
            assert pathlib.Path(connection).exists(), "Client-owned connection file was deleted"
        finally:
            client.stop_channels()
            if launcher.poll() is None:
                launcher.terminate()
                launcher.wait(timeout=30)
            if session:
                subprocess.run(["node", cli, "jupyter", "stop", session], check=True, stdout=subprocess.DEVNULL)
