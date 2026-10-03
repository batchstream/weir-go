#!/usr/bin/env python3
"""Keep the SDK and public protocol independent from the Weir server module."""
import json
import os
from pathlib import Path
import re
import subprocess


SDK = 'github.com/batchstream/weir-go'
PROTOCOL = 'github.com/batchstream/weir-protocol'
SERVER = 'github.com/batchstream/weir'


def objects(raw):
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
        if name == SERVER or name.startswith(SERVER + '/'):
            raise ValueError('SDK imports the server module: ' + name)
        if name.startswith(PROTOCOL + '/') and not name.startswith(PROTOCOL + '/api/'):
            raise ValueError('SDK imports a nonpublic protocol package: ' + name)


def check_modules(items):
    protocol_found = False
    for module in items:
        if module.get('Replace'):
            raise ValueError('SDK dependency graph contains a module replacement: ' + module['Path'])
        if module['Path'] == SERVER:
            raise ValueError('SDK module graph contains the server module')
        if module['Path'] == PROTOCOL:
            protocol_found = True
            if not re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', module.get('Version', '')):
                raise ValueError('SDK requires a canonical stable protocol version')
    if not protocol_found:
        raise ValueError('SDK requires the published protocol module')


def check_module_graph(raw):
    edges = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        pair = line.split()
        if len(pair) != 2:
            raise ValueError('invalid go mod graph edge: ' + line)
        source, target = (name.split('@', 1)[0] for name in pair)
        if SERVER in (source, target):
            raise ValueError('SDK module graph contains the server module: ' + line)
        if source == target and source in (SDK, PROTOCOL):
            raise ValueError('project module depends on itself: ' + line)
        for node in pair:
            path, _, version = node.partition('@')
            if path == PROTOCOL and not re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
                raise ValueError('SDK requires a canonical stable protocol version: ' + node)
        edges.setdefault(source, set()).add(target)
    pending = [PROTOCOL]
    visited = set()
    while pending:
        source = pending.pop()
        if source in visited:
            continue
        if source == SDK:
            raise ValueError('protocol module depends back on the SDK')
        visited.add(source)
        targets = edges.get(source, ())
        if PROTOCOL in targets:
            raise ValueError('protocol module depends back on itself')
        pending.extend(targets)


def main():
    root = Path(__file__).resolve().parent.parent
    schemas = list(root.rglob('*.proto')) + list(root.rglob('*.pb.go'))
    if schemas:
        raise ValueError('SDK must consume upstream public schemas: ' + ', '.join(str(path.relative_to(root)) for path in schemas))
    # A workspace must not hide the pinned module graph or replacements.
    env = dict(os.environ, GOWORK='off')
    graph = subprocess.check_output(['go', 'mod', 'graph'], cwd=root, env=env, text=True, timeout=120)
    check_module_graph(graph)
    modules = subprocess.check_output(['go', 'list', '-m', '-json', 'all'], cwd=root, env=env, text=True, timeout=120)
    check_modules(objects(modules))
    for tags in ([], ['-tags=integration']):
        command = ['go', 'list', '-deps', '-test', '-json'] + tags + ['./...']
        result = subprocess.check_output(command, cwd=root, env=env, text=True, timeout=120)
        check_dependencies(objects(result))
    print('SDK protocol module and package dependency boundaries passed')


if __name__ == '__main__':
    main()
