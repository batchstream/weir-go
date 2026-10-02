"""Offline dependency boundary tests; no module resolution or external I/O."""
import json
import unittest

import check_dependencies


class DependencyTests(unittest.TestCase):
    def test_public_helpers_and_protobuf_are_allowed(self):
        names = ['context', 'google.golang.org/grpc', 'github.com/batchstream/weir-go',
                 'github.com/batchstream/weir/api/protocol', 'github.com/batchstream/weir/api/netlimit',
                 'github.com/batchstream/weir/api/weir/v1', 'github.com/batchstream/weir/api/weir/search/v1']
        items = [{'ImportPath': name} for name in names]
        check_dependencies.check_dependencies(items)

    def test_each_internal_boundary_is_rejected(self):
        for path in ('internal/directory', 'internal/api/peer/v1', 'internal/server', 'internal/backend/mongodb'):
            item = {'ImportPath': 'github.com/batchstream/weir/' + path}
            with self.assertRaisesRegex(ValueError, 'server internals'):
                check_dependencies.check_dependencies([item])

    def test_go_list_objects_decode_without_a_wrapper_array(self):
        expected = [{'ImportPath': 'context'}, {'ImportPath': 'github.com/batchstream/weir/api/protocol'}]
        raw = '\n'.join(json.dumps(item) for item in expected) + '\n'
        self.assertEqual(check_dependencies.packages(raw), expected)


if __name__ == '__main__':
    unittest.main()
