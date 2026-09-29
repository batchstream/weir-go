#!/usr/bin/env python3
"""Release only tested main; refuse existing tags pointing at another commit."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def run(args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=180, **kwargs).strip()


def main():
    version = os.environ['RELEASE_VERSION']
    if not re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError('expected canonical stable version')
    if os.environ.get('GITHUB_REPOSITORY') != 'batchstream/weir-go' or os.environ.get('GITHUB_REF') != 'refs/heads/main':
        raise ValueError('release requires batchstream/weir-go main')
    sha = run(['git', 'rev-parse', 'HEAD'])
    if sha != os.environ['GITHUB_SHA']:
        raise ValueError('checkout differs from tested commit')
    upstream = json.loads(run(['go', 'list', '-m', '-json', 'github.com/batchstream/weir']))
    if not re.fullmatch(r'v\d+\.\d+\.\d+', upstream['Version']) or upstream.get('Replace'):
        raise ValueError('release requires a stable upstream protocol version and no replace')
    tags = run(['git', 'ls-remote', '--tags', 'origin', 'refs/tags/' + version, 'refs/tags/' + version + '^{}'])
    if tags:
        refs = dict(line.split()[::-1] for line in tags.splitlines())
        target = refs.get('refs/tags/' + version + '^{}', refs.get('refs/tags/' + version))
        if target != sha:
            raise ValueError('existing version belongs to another commit; refusing overwrite')
    existing = subprocess.run(['gh', 'release', 'view', version, '--repo', 'batchstream/weir-go', '--json', 'tagName,isDraft'], capture_output=True, text=True, timeout=30)
    if existing.returncode == 0:
        release = json.loads(existing.stdout)
        if release['tagName'] != version or release['isDraft']:
            raise ValueError('existing release identity or publication state differs')
    else:
        if 'release not found' not in existing.stderr.lower() and '404' not in existing.stderr:
            raise RuntimeError('could not determine existing release state')
        with tempfile.TemporaryDirectory(prefix='weir-sdk-release-') as directory:
            notes = Path(directory) / 'notes.md'
            notes.write_text(f'Go SDK for all five Weir RPCs, with bounded calls, strict stream completion, partial result evidence, and no automatic replay.\n\nSource: `{sha}`. Protocol: `{upstream["Version"]}`. Default race tests and vet passed before publication. Install with `go get github.com/batchstream/weir-go@{version}`. See the README for deployment isolation and integration qualification.\n')
            run(['gh', 'release', 'create', version, '--repo', 'batchstream/weir-go', '--target', sha, '--title', version, '--notes-file', str(notes)])
    # Use a clean consumer module and cache: no workspace replace or existing SDK
    # checkout can mask a release-resolution problem. Retry read-only propagation.
    with tempfile.TemporaryDirectory(prefix='weir-sdk-consumer-') as directory:
        root = Path(directory)
        env = dict(os.environ, GOWORK='off', GOPROXY='https://proxy.golang.org,direct', GOMODCACHE=str(root / 'cache'), GOBIN=str(root / 'bin'))
        run(['go', 'mod', 'init', 'example.com/weir-release-check'], cwd=root, env=env)
        source = 'package main\nimport ("fmt"; weir "github.com/batchstream/weir-go")\nfunc main() { value, err := weir.Resource("mongo", "db", "records", "s:check"); if err != nil { panic(err) }; fmt.Println(value) }\n'
        (root / 'main.go').write_text(source)
        for attempt in range(6):
            result = subprocess.run(['go', 'get', 'github.com/batchstream/weir-go@' + version], cwd=root, env=env, timeout=180)
            if result.returncode == 0:
                break
            if attempt == 5:
                raise RuntimeError('published module could not be installed')
            time.sleep(5)
        if run(['go', 'run', '.'], cwd=root, env=env) != 'weir://mongo/db/records/s:check':
            raise ValueError('external consumer returned unexpected result')
        resolved = json.loads(run(['go', 'list', '-m', '-json', 'github.com/batchstream/weir-go'], cwd=root, env=env))
        if resolved['Version'] != version or resolved.get('Replace'):
            raise ValueError('external consumer did not use the published module')
        for example in ('read', 'soak'):
            run(['go', 'install', f'github.com/batchstream/weir-go/examples/{example}@{version}'], cwd=root, env=env)
        print('Verified external module and example installation:', version, sha)


if __name__ == '__main__':
    main()
