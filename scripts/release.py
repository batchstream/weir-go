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
    upstream_env = dict(os.environ, GOWORK='off')
    upstream = json.loads(run(['go', 'list', '-m', '-json', 'github.com/batchstream/weir-protocol'], env=upstream_env))
    if not re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', upstream.get('Version', '')) or upstream.get('Replace'):
        raise ValueError('release requires a stable upstream protocol version from weir-protocol and no replace')
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
            notes.write_text(f'Go SDK with typed Read/Create/Put/Replace/Delete/AtomicTransform/Scan/Native operations, ResolveStore initialization, bounded bidirectional Execute streams, incremental ReadStream/MutateStream producers and consumers, indexed acknowledgements and Scan/Native responses, with no automatic business replay.\n\nSource: `{sha}`. Protocol: `{upstream["Version"]}`. Default race tests and vet passed before publication. Install with `go get github.com/batchstream/weir-go@{version}`. See the README for usage and validation.\n')
            run(['gh', 'release', 'create', version, '--repo', 'batchstream/weir-go', '--target', sha, '--title', version, '--notes-file', str(notes)])
    # Use a clean consumer module and cache: no workspace replace or existing SDK
    # checkout can mask a release-resolution problem. Retry read-only propagation.
    with tempfile.TemporaryDirectory(prefix='weir-sdk-consumer-') as directory:
        root = Path(directory)
        env = dict(os.environ, GOWORK='off', GOPROXY='https://proxy.golang.org,direct', GOMODCACHE=str(root / 'cache'), GOBIN=str(root / 'bin'))
        run(['go', 'mod', 'init', 'example.com/weir-release-check'], cwd=root, env=env)
        source = """package main
import (
    "context"
    "fmt"
    "io"
    weir "github.com/batchstream/weir-go"
)
// Compile the ordinary API using only SDK-owned request and result names.
func typedAPI(ctx context.Context, client *weir.Client) {
    read := &weir.ReadRequest{Resource: "records/s:key"}
    readOptions := weir.ReadOneOptions{StoreName: "records", Request: read}
    _, _ = client.ReadOne(ctx, readOptions)
    reads := weir.ReadOptions{StoreName: "records", Requests: []*weir.ReadRequest{read, read}}
    _, _ = client.Read(ctx, reads)
    document := &weir.Document{ContentType: "application/json", Data: []byte(`{}`)}
    mutation := &weir.MutateRequest{Resource: read.Resource, Action: weir.MutationPut, Document: document}
    mutations := weir.MutateOptions{StoreName: "records", Requests: []*weir.MutateRequest{mutation}}
    _, _ = client.Mutate(ctx, mutations)
    streamedReads := weir.ReadStreamOptions{StoreName: "records"}
    streamedReads.Next = func(context.Context) (*weir.ReadRequest, error) { return nil, io.EOF }
    streamedReads.Consume = func(context.Context, uint64, *weir.ReadResult) error { return nil }
    _ = client.ReadStream(ctx, streamedReads)
    streamedMutations := weir.MutateStreamOptions{StoreName: "records"}
    streamedMutations.Next = func(context.Context) (*weir.MutateRequest, error) { return nil, io.EOF }
    streamedMutations.Consume = func(context.Context, uint64, *weir.MutationResult) error { return nil }
    _ = client.MutateStream(ctx, streamedMutations)
    write := &weir.WriteRequest{Resource: read.Resource, Document: document}
    writeOptions := weir.WriteOptions{StoreName: "records", Request: write}
    _, _ = client.Create(ctx, writeOptions)
    _, _ = client.Put(ctx, writeOptions)
    _, _ = client.Replace(ctx, writeOptions)
    remove := &weir.DeleteRequest{Resource: read.Resource}
    deleteOptions := weir.DeleteOptions{StoreName: "records", Request: remove}
    _, _ = client.Delete(ctx, deleteOptions)
    lua := &weir.LuaTransform{Source: []byte("return weir.keep()"), Input: document}
    transform := &weir.AtomicTransformRequest{Resource: read.Resource, Lua: lua}
    transformOptions := weir.AtomicTransformOptions{StoreName: "records", Request: transform}
    _, _ = client.AtomicTransform(ctx, transformOptions)
    filter := &weir.Document{ContentType: "application/json", Data: []byte(`{"match_all":{}}`)}
    projection := &weir.Projection{Mode: weir.ProjectionInclude, Fields: []string{"n"}}
    scan := &weir.ScanRequest{Resource: "records", Filter: filter, Projection: projection, PageSize: 1}
    scanOptions := weir.ScanOptions{StoreName: "records", Request: scan}
    scanOptions.Consume = func(context.Context, *weir.Document) error { return nil }
    _, _ = client.Scan(ctx, scanOptions)
    http := &weir.SearchHTTPRequest{Method: "GET", Path: "/_doc/key"}
    native := &weir.NativeRequest{Resource: "records", SearchHTTP: http}
    nativeOptions := weir.NativeOptions{StoreName: "records", Request: native}
    nativeOptions.Consume = func(context.Context, *weir.NativeResponse, []byte) error { return nil }
    result, _ := client.Native(ctx, nativeOptions)
    if result != nil {
        _ = result.Response.GetHttp()
        _ = result.Completion == weir.NativeCompletionUnconfirmed
        _ = result.Failure.GetCode() == weir.FailureTargetNotFound
    }

}
func main() {
    connection, err := weir.Dial("127.0.0.1:7447")
    if err != nil { panic(err) }
    defer connection.Close()
    fmt.Println(weir.EncodeSegment("s:check/path"))
}
"""
        (root / 'main.go').write_text(source)
        for attempt in range(6):
            result = subprocess.run(['go', 'get', 'github.com/batchstream/weir-go@' + version], cwd=root, env=env, timeout=180)
            if result.returncode == 0:
                break
            if attempt == 5:
                raise RuntimeError('published module could not be installed')
            time.sleep(5)
        if run(['go', 'run', '.'], cwd=root, env=env) != 's:check%2Fpath':
            raise ValueError('external consumer returned unexpected result')
        resolved = json.loads(run(['go', 'list', '-m', '-json', 'github.com/batchstream/weir-go'], cwd=root, env=env))
        if resolved['Version'] != version or resolved.get('Replace'):
            raise ValueError('external consumer did not use the published module')
        for example in ('read', 'basic', 'scan', 'native'):
            run(['go', 'install', f'github.com/batchstream/weir-go/examples/{example}@{version}'], cwd=root, env=env)
        print('Verified external module and example installation:', version, sha)


if __name__ == '__main__':
    main()
