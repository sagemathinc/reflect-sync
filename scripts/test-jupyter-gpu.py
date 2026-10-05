"""Opt-in GPU computation/restart test through the ordinary kernelspec."""
import sys
from jupyter_client import KernelManager
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path

spec = spec_from_file_location("client_test", Path(__file__).with_name("test-jupyter-client.py"))
module = module_from_spec(spec)
spec.loader.exec_module(module)
k = KernelManager(kernel_name=sys.argv[1])
c = None
try:
    k.start_kernel()
    c = k.client()
    c.start_channels()
    for attempt in range(2):
        c.wait_for_ready(timeout=60)
        outputs = module.execute(c, '''
import torch, socket
assert torch.cuda.is_available()
x = torch.ones((1024, 1024), device='cuda')
y = x @ x
torch.cuda.synchronize()
assert y[0, 0].item() == 1024
print('GPU_OK', socket.gethostname(), torch.cuda.get_device_name(), torch.__version__)
''')
        text = ''.join(m['content'].get('text', '') for m in outputs)
        assert 'GPU_OK' in text, outputs
        print(text.strip(), flush=True)
        if attempt == 0:
            k.restart_kernel(now=False)
finally:
    if k.has_kernel:
        k.shutdown_kernel(now=False)
    if c:
        c.stop_channels()
