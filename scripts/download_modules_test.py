"""Offline cache preparation tests; no module downloads or external I/O."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import download_modules


class DownloadModulesTests(unittest.TestCase):
    def test_unused_sums_and_workspace_cannot_change_the_checkout(self):
        with tempfile.TemporaryDirectory(prefix='sdk-module-test-') as directory:
            root = Path(directory)
            inputs = {'go.mod': b'module example.com/sdk\n', 'go.sum': b'fixed checksums\n'}
            for name, raw in inputs.items():
                (root / name).write_bytes(raw)

            def download(args, **kwargs):
                self.assertEqual(args, ['go', 'mod', 'download', 'all'])
                work = kwargs['cwd']
                self.assertNotEqual(work, root)
                self.assertEqual(kwargs['env']['GOWORK'], 'off')
                self.assertTrue(kwargs['check'])
                for name, raw in inputs.items():
                    self.assertEqual((work / name).read_bytes(), raw)
                (work / 'go.sum').write_bytes(b'unused ZIP checksums\n')

            with patch.dict(os.environ, {'GOWORK': '/local/go.work'}), patch.object(download_modules.subprocess, 'run', side_effect=download) as command:
                download_modules.download_modules(root)
            command.assert_called_once()
            self.assertFalse(command.call_args.kwargs['cwd'].exists())
            self.assertEqual(inputs, {name: (root / name).read_bytes() for name in inputs})

    def test_download_failure_is_reported_without_changing_fixed_inputs(self):
        with tempfile.TemporaryDirectory(prefix='sdk-module-test-') as directory:
            root = Path(directory)
            inputs = {'go.mod': b'module example.com/sdk\n', 'go.sum': b'fixed checksums\n'}
            for name, raw in inputs.items():
                (root / name).write_bytes(raw)
            failure = subprocess.CalledProcessError(1, ['go', 'mod', 'download', 'all'])
            with patch.object(download_modules.subprocess, 'run', side_effect=failure):
                with self.assertRaises(subprocess.CalledProcessError):
                    download_modules.download_modules(root)
            self.assertEqual(inputs, {name: (root / name).read_bytes() for name in inputs})


if __name__ == '__main__':
    unittest.main()
