#!/usr/bin/env python3
"""Reject server internals and generated schemas in SDK production packages."""
import json
from pathlib import Path
import subprocess


def packages(raw):
    decoder = json.JSONDecoder()
    result = []
    while raw.strip():
        package, end = decoder.raw_decode(raw.lstrip())
        result.append(package)
        raw = raw.lstrip()[end:]
    return result


def check_dependencies(items):
    for package in items:
        name = package['ImportPath']
        if name.startswith('github.com/batchstream/weir/') and not name.startswith('github.com/batchstream/weir/api/'):
            raise ValueError('SDK production imports server internals: ' + name)


def main():
    root = Path(__file__).resolve().parent.parent
    schemas = list(root.rglob('*.proto')) + list(root.rglob('*.pb.go'))
    if schemas:
        raise ValueError('SDK must consume upstream public schemas: ' + ', '.join(str(path.relative_to(root)) for path in schemas))
    result = subprocess.check_output(['go', 'list', '-deps', '-json', './...'], cwd=root, text=True, timeout=120)
    check_dependencies(packages(result))
    print('SDK production dependency boundary passed')


if __name__ == '__main__':
    main()
