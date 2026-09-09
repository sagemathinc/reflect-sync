"""Live SSH test: register a Reflect kernelspec first, then pass its name.

Runs only synthetic code in a new owned kernel. Always shuts down that kernel.
"""
import json
import sys
from jupyter_client import KernelManager


def execute(client, code):
    message_id = client.execute(code)
    outputs = []
    while True:
        message = client.get_iopub_msg(timeout=30)
        if message["parent_header"].get("msg_id") != message_id:
            continue
        if message["msg_type"] == "status" and message["content"]["execution_state"] == "idle":
            return outputs
        outputs.append(message)


def main():
    manager = KernelManager(kernel_name=sys.argv[1])
    client = None
    try:
        manager.start_kernel()
        client = manager.client()
        client.start_channels()
        client.wait_for_ready(timeout=60)
        outputs = execute(client, "import socket; print('REMOTE_HOST=' + socket.gethostname())")
        text = "".join(m["content"].get("text", "") for m in outputs)
        assert "REMOTE_HOST=" in text, outputs
        print(text.strip())
        completion = client.complete("socket.gethost")
        while True:
            reply = client.get_shell_msg(timeout=10)
            if reply["parent_header"].get("msg_id") == completion:
                assert any(match.endswith("gethostname") for match in reply["content"]["matches"]), reply
                break
        inspection = client.inspect("socket.gethostname")
        while True:
            reply = client.get_shell_msg(timeout=10)
            if reply["parent_header"].get("msg_id") == inspection:
                assert reply["content"]["found"], reply
                break
        outputs = execute(client, "from IPython.display import display, HTML; display(HTML('<b>remote</b>')); 1 / 0")
        assert any("text/html" in m["content"].get("data", {}) for m in outputs), outputs
        assert any(m["content"].get("ename") == "ZeroDivisionError" for m in outputs), outputs
        input_id = client.execute("print('INPUT=' + input('Value: '))", allow_stdin=True)
        prompt = client.get_stdin_msg(timeout=10)
        assert prompt["parent_header"]["msg_id"] == input_id
        client.input("from-client")
        while True:
            output = client.get_iopub_msg(timeout=10)
            if output["parent_header"].get("msg_id") == input_id and output["msg_type"] == "stream":
                assert "INPUT=from-client" in output["content"]["text"]
                break
        message_id = client.execute("while True: pass")
        while True:
            m = client.get_iopub_msg(timeout=10)
            if m["parent_header"].get("msg_id") == message_id and m["msg_type"] == "status" and m["content"]["execution_state"] == "busy":
                break
        manager.interrupt_kernel()
        interrupted = False
        while True:
            m = client.get_iopub_msg(timeout=20)
            if m["parent_header"].get("msg_id") != message_id:
                continue
            if m["msg_type"] == "error":
                interrupted |= m["content"]["ename"] == "KeyboardInterrupt"
            if m["msg_type"] == "status" and m["content"]["execution_state"] == "idle":
                break
        assert interrupted, "Remote execution was not interrupted"
        outputs = execute(client, "print(6 * 7)")
        assert any("42" in m["content"].get("text", "") for m in outputs), outputs
        manager.restart_kernel(now=True)
        client.wait_for_ready(timeout=60)
        outputs = execute(client, "print('RESTARTED')")
        assert any("RESTARTED" in m["content"].get("text", "") for m in outputs), outputs
        print(json.dumps({"execution": True, "interrupt": True, "restart": True,
            "completion": True, "inspection": True, "richOutput": True, "errors": True, "stdin": True}))
    finally:
        if manager.has_kernel:
            manager.shutdown_kernel(now=False)
        if client:
            client.stop_channels()


if __name__ == "__main__":
    main()
