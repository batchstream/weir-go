"""Offline dependency boundary tests; no module resolution or external I/O."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import check_dependencies


class DependencyTests(unittest.TestCase):
    def test_public_helpers_and_protobuf_are_allowed(self):
        names = ['context', 'google.golang.org/grpc', 'github.com/batchstream/weir-go',
                 'github.com/batchstream/weir-protocol/api/protocol', 'github.com/batchstream/weir-protocol/api/netlimit',
                 'github.com/batchstream/weir-protocol/api/weir/v1', 'github.com/batchstream/weir-protocol/api/netlimit']
        items = [{'ImportPath': name} for name in names]
        check_dependencies.check_dependencies(items)

    def test_server_public_and_internal_packages_are_rejected(self):
        for path in ('api/protocol', 'api/weir/v1', 'internal/directory', 'internal/api/peer/v1', 'internal/server', 'internal/backend/mongodb'):
            item = {'ImportPath': 'github.com/batchstream/weir/' + path}
            with self.assertRaisesRegex(ValueError, 'server module'):
                check_dependencies.check_dependencies([item])

    def test_unused_or_test_server_requirements_are_rejected(self):
        for source in (check_dependencies.SDK, check_dependencies.PROTOCOL + '@v0.1.0', 'example.com/test-fixture@v1.0.0'):
            graph = f'{source} github.com/batchstream/weir@v0.1.0\n'
            with self.assertRaisesRegex(ValueError, 'module graph.*server module'):
                check_dependencies.check_module_graph(graph)

    def test_protocol_cannot_depend_on_sdk_directly_or_transitively(self):
        for dependency in (check_dependencies.SDK, 'example.com/unused'):
            graph = f'{check_dependencies.SDK} {check_dependencies.PROTOCOL}@v0.1.0\n'
            graph += f'{check_dependencies.PROTOCOL}@v0.1.0 {dependency}@v0.1.0\n'
            if dependency != check_dependencies.SDK:
                graph += f'{dependency}@v0.1.0 {check_dependencies.SDK}@v0.1.0\n'
            with self.assertRaisesRegex(ValueError, 'protocol module depends back on the SDK'):
                check_dependencies.check_module_graph(graph)

    def test_project_self_requirements_are_rejected(self):
        for module in (check_dependencies.SDK, check_dependencies.PROTOCOL):
            graph = f'{module} {module}@v0.1.0\n'
            with self.assertRaisesRegex(ValueError, 'depends on itself'):
                check_dependencies.check_module_graph(graph)

    def test_protocol_cannot_depend_back_on_itself_through_an_unused_module(self):
        graph = f'{check_dependencies.SDK} {check_dependencies.PROTOCOL}@v0.1.0\n'
        graph += f'{check_dependencies.PROTOCOL}@v0.1.0 example.com/unused@v0.1.0\n'
        graph += f'example.com/unused@v0.1.0 {check_dependencies.PROTOCOL}@v0.0.1\n'
        with self.assertRaisesRegex(ValueError, 'protocol module depends back on itself'):
            check_dependencies.check_module_graph(graph)

    def test_public_protocol_graph_is_allowed(self):
        graph = f'{check_dependencies.SDK} {check_dependencies.PROTOCOL}@v0.1.0\n'
        graph += f'{check_dependencies.PROTOCOL}@v0.1.0 google.golang.org/grpc@v1.83.2\n'
        graph += 'google.golang.org/grpc@v1.83.2 golang.org/x/net@v0.58.0\n'
        check_dependencies.check_module_graph(graph)

    def test_replacements_are_rejected_even_for_unused_modules(self):
        modules = [{'Path': check_dependencies.SDK, 'Main': True},
                   {'Path': 'example.com/unused', 'Replace': {'Path': '/local'}}]
        with self.assertRaisesRegex(ValueError, 'module replacement'):
            check_dependencies.check_modules(modules)

    def test_protocol_module_must_be_published_and_stable(self):
        for version in ('', 'main', 'v0.1.0-rc.1', 'v01.0.0', 'v0.0.0-20261002231331-9eb76acccdcf'):
            module = {'Path': check_dependencies.PROTOCOL, 'Version': version}
            with self.assertRaisesRegex(ValueError, 'canonical stable protocol'):
                check_dependencies.check_modules([module])
            graph = f'{check_dependencies.SDK} {check_dependencies.PROTOCOL}@{version}\n'
            with self.assertRaisesRegex(ValueError, 'canonical stable protocol'):
                check_dependencies.check_module_graph(graph)
        with self.assertRaisesRegex(ValueError, 'published protocol module'):
            check_dependencies.check_modules([{'Path': check_dependencies.SDK, 'Main': True}])
        check_dependencies.check_modules([{'Path': check_dependencies.PROTOCOL, 'Version': 'v0.1.0'}])

    def test_main_checks_modules_before_imports_and_ignores_workspace(self):
        graph = f'{check_dependencies.SDK} {check_dependencies.PROTOCOL}@v0.1.0\n'
        modules = '\n'.join(json.dumps(item) for item in (
            {'Path': check_dependencies.SDK, 'Main': True},
            {'Path': check_dependencies.PROTOCOL, 'Version': 'v0.1.0'}))
        packages = json.dumps({'ImportPath': 'context'})
        with patch.dict(os.environ, {'GOWORK': '/local/go.work'}), patch.object(Path, 'rglob', return_value=[]), patch.object(check_dependencies.subprocess, 'check_output', side_effect=[graph, modules, packages, packages]) as command:
            check_dependencies.main()
        self.assertEqual([call.args[0] for call in command.call_args_list], [
            ['go', 'mod', 'graph'], ['go', 'list', '-m', '-json', 'all'],
            ['go', 'list', '-deps', '-test', '-json', './...'],
            ['go', 'list', '-deps', '-test', '-json', '-tags=integration', './...']])
        for call in command.call_args_list:
            self.assertEqual(call.kwargs['env']['GOWORK'], 'off')

    def test_sdk_schema_source_or_generated_files_prevent_all_go_commands(self):
        root = Path(check_dependencies.__file__).resolve().parent.parent
        for suffix in ('store.proto', 'store.pb.go'):
            generated = root / 'api' / suffix
            with patch.object(Path, 'rglob', return_value=[generated]), patch.object(check_dependencies.subprocess, 'check_output') as command:
                with self.assertRaisesRegex(ValueError, 'must consume upstream public schemas'):
                    check_dependencies.main()
                command.assert_not_called()

    def test_go_list_objects_decode_without_a_wrapper_array(self):
        expected = [{'ImportPath': 'context'}, {'ImportPath': 'github.com/batchstream/weir-protocol/api/protocol'}]
        raw = '\n'.join(json.dumps(item) for item in expected) + '\n'
        self.assertEqual(check_dependencies.objects(raw), expected)


if __name__ == '__main__':
    unittest.main()
