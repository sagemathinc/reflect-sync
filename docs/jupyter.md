# Remote Jupyter Kernels

Reflect can register a standard local Jupyter kernelspec backed by a Python
kernel on a remote Linux machine. Only SSH is exposed; there is no remote
Jupyter web server, Node runtime, or CoCalc runtime.

## Setup

Passwordless SSH and verified host keys must already work from the machine
running Jupyter. Use an SSH alias to configure the hostname, account, key, port,
and jump hosts. A changing VM IP should be addressed through its stable DNS name.
Reflect never disables host-key checking or forwards your SSH agent.

```sh
reflect jupyter setup --host my-vm --target gpu --environment teaching
jupyter console --kernel reflect-gpu
```

`setup` downloads pinned uv 0.8.22 with a compiled-in SHA256, uploads it using a
second content hash check, and prepares an isolated user-owned environment with
ipykernel 6.30.1 and ipywidgets 8.1.7. Linux x86_64 and aarch64 bootstrap artifacts
are provided; the live validation environment is Ubuntu 24.04 x86_64.
No sudo or system package modifications occur. Python 3 is used if present;
otherwise uv installs a private Python 3.12.11 runtime. Preparation requires HTTPS
access to GitHub and the Python package index. A local trusted uv binary for the
remote architecture may be supplied with `--uv /absolute/path`.

To retain an existing framework/CUDA environment without modifying it:

```sh
reflect jupyter setup --host my-vm --target gpu \
  --python /home/user/gpu/bin/python
```

The selected Python must already contain ipykernel and jupyter_client. Setup
performs a real local-on-VM Jupyter handshake before registering it. GPU drivers
and frameworks are separate from kernel preparation. An ordinary Ubuntu CPU
image plus ipykernel is not a GPU software installation.

On a VM with working NVIDIA drivers, prepare and validate the supported PyTorch
recipe explicitly (a multi-GB download):

```sh
reflect jupyter setup --host my-vm --target gpu \
  --environment pytorch --recipe pytorch-cu128
```

This installs PyTorch 2.8.0 with CUDA 12.8 wheels and NumPy 2.2.6. It performs
a CUDA matrix multiplication before publishing the environment. Use a new
environment name when changing recipes; existing environments are not silently
upgraded. See [PyTorch's release instructions](https://pytorch.org/get-started/previous-versions/#v280).

`prepare` and `register` are also separate commands. `kernels --host my-vm` lists
Reflect-managed environments; arbitrary existing interpreters can be registered
by absolute path. `targets` and `sessions` emit JSON. Setup emits the installed
kernel name, target and paths as JSON.

## Lifecycle

The kernelspec launches `reflect jupyter launch` in the foreground. It honors the
caller's signed loopback TCP connection file and forwards all five Jupyter ports.
The original descriptor is never replaced or removed. Each launch creates a
separate session with independent remote ports and signing key. Reflect does not
parse Jupyter messages, so comms and binary buffers use the same wire protocol.

- SIGINT interrupts the remote kernel process group without killing SSH.
- SIGTERM requests remote termination and waits for confirmation.
- Protocol shutdown is handled by the kernel, whose exit terminates the launcher.
- SIGKILL cannot be intercepted. The kernel lease expires after 60 seconds by
  default, even if other Reflect processes remain alive. For forced client-manager
  restarts that use SIGKILL, the old kernel may remain until this lease expires.
- SSH is retried for a bounded period; a live remote incarnation is reused, never
  a new kernel. Check client readiness before submitting new work after reconnect.
  In-flight work/output may be lost or uncertain; it is never automatically replayed.
- A VM reboot loses kernel memory and requires an explicit new kernel.
- Stopping a kernel does not stop or change billing for its VM.

Session IDs appear on launcher stderr. Diagnostics do not print connection keys.
Use `reflect jupyter status ID`, `interrupt ID`, and `stop ID` from the same local
account. Status strips connection credentials. Private local session records live
under `~/.local/share/reflect/jupyter` (override with `REFLECT_JUPYTER_HOME`), and
the supervisor/environment/session files are under the same path on the VM.

## Tests

The following explicitly opt-in scripts use a registered kernelspec and only
create synthetic kernels, cleaning up their own sessions:

```sh
python3 scripts/test-jupyter-client.py reflect-gpu
python3 scripts/test-jupyter-multikernel.py reflect-gpu
python3 scripts/test-jupyter-lease.py gpu
```

These cover execution, completions, inspection, stdin, errors/rich output,
interrupt, restart, independent kernels, binary comms, widget initialization,
tunnel reconnect, and abrupt launcher death. They do not constitute validation
of every notebook frontend, GPU framework, VM provider, or file-sharing workflow.
File synchronization remains independent of kernel transport.
