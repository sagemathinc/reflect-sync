"""Opt-in generic kernelspec test. Pass a registered remote Bash kernel name."""
import sys
import time
from jupyter_client import KernelManager


def main():
    manager = KernelManager(kernel_name=sys.argv[1])
    client = None
    try:
        manager.start_kernel()
        client = manager.client()
        client.start_channels()
        client.wait_for_ready(timeout=60)

        def execute(code):
            message_id = client.execute(code)
            output = ""
            while True:
                message = client.get_iopub_msg(timeout=30)
                if message["parent_header"].get("msg_id") != message_id:
                    continue
                if message["msg_type"] == "status" and message["content"]["execution_state"] == "idle":
                    return output
                output += message["content"].get("text", "")

        assert "42" in execute("echo $((6*7))")
        message_id = client.execute("sleep 120")
        while True:
            message = client.get_iopub_msg(timeout=15)
            if message["parent_header"].get("msg_id") == message_id and message["msg_type"] == "status" and message["content"]["execution_state"] == "busy":
                break
        time.sleep(1)
        manager.interrupt_kernel()
        while True:
            message = client.get_iopub_msg(timeout=20)
            if message["parent_header"].get("msg_id") == message_id and message["msg_type"] == "status" and message["content"]["execution_state"] == "idle":
                break
        assert "RESUMED" in execute("echo RESUMED")
        manager.restart_kernel(now=True)
        client.wait_for_ready(timeout=60)
        assert "RESTARTED" in execute("echo RESTARTED")
        print("Bash execution, interrupt, restart and reuse passed")
    finally:
        if manager.has_kernel:
            manager.shutdown_kernel(now=False)
        if client:
            client.stop_channels()


if __name__ == "__main__":
    main()
