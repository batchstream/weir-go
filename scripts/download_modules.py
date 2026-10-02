#!/usr/bin/env python3
"""Warm the complete module graph without adding unused ZIP sums to the SDK."""
import os
from pathlib import Path
import subprocess
import tempfile


def download_modules(root):
    inputs = {name: (root / name).read_bytes() for name in ('go.mod', 'go.sum')}
    env = dict(os.environ, GOWORK='off')
    with tempfile.TemporaryDirectory(prefix='weir-sdk-modules-') as directory:
        work = Path(directory)
        for name, raw in inputs.items():
            (work / name).write_bytes(raw)
        subprocess.run(['go', 'mod', 'download', 'all'], cwd=work, env=env, check=True, timeout=300)
    if inputs != {name: (root / name).read_bytes() for name in inputs}:
        raise ValueError('module preparation changed fixed go.mod/go.sum')


if __name__ == '__main__':
    download_modules(Path(__file__).resolve().parent.parent)
