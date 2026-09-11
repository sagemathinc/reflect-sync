"""Bounded startup failures using isolated local state and owned remote sessions."""
import hashlib
import json
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
from jupyter_client.connect import write_connection_file

cli = ["node", str(pathlib.Path(__file__).resolve().parents[1] / "dist/cli.js"), "jupyter"]
target = next(t for t in json.loads(subprocess.check_output(cli + ["targets", "--json"])) if t["name"] == sys.argv[1])
for failure in ("occupied-port", "missing-interpreter"):
    with tempfile.TemporaryDirectory() as root, socket.socket() as occupied:
        home = pathlib.Path(root)
        (home / "targets").mkdir()
        config = {k: target[k] for k in ("host", "python", "environment")}
        if failure == "missing-interpreter":
            config["python"] = "/nonexistent/reflect-test-python"
        (home / "targets/test.json").write_text(json.dumps(config))
        connection = str(home / "connection.json")
        _, info = write_connection_file(connection, key=b"synthetic-test-key")
        before = pathlib.Path(connection).read_bytes()
        if failure == "occupied-port":
            occupied.bind(("127.0.0.1", info["shell_port"]))
            occupied.listen()
        env = {**os.environ, "REFLECT_JUPYTER_HOME": root}
        result = subprocess.run(cli + ["launch", "--target", "test", "--connection-file", connection],
                                env=env, capture_output=True, text=True, timeout=60)
        assert result.returncode != 0, failure
        assert "synthetic-test-key" not in result.stderr
        assert pathlib.Path(connection).read_bytes() == before
        sessions = json.loads(subprocess.check_output(cli + ["list", "--json"], env=env))
        assert len(sessions) == 1
        status = json.loads(subprocess.check_output(cli + ["status", sessions[0]["session"], "--json"], env=env))
        assert status["status"] in ("stopped", "failed"), status
        print("PASS:", failure, "unwinds session and preserves client connection file")
