"""Offline release preflight tests; no GitHub, registry or module requests."""
import json
import os
import subprocess
import unittest
from unittest.mock import patch

import release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.environment = dict(RELEASE_VERSION='v0.2.0', GITHUB_REPOSITORY='batchstream/weir-go',
                                GITHUB_REF='refs/heads/main', GITHUB_SHA='a' * 40)

    def test_invalid_version_does_no_io(self):
        for version in ('v01.0.0', 'v0.1.0-rc.1', '../v1.0.0', 'v1.0.0\n', ''):
            self.environment['RELEASE_VERSION'] = version
            with patch.dict(os.environ, self.environment), patch.object(release.subprocess, 'check_output') as command:
                with self.assertRaisesRegex(ValueError, 'canonical stable'):
                    release.main()
                command.assert_not_called()

    def test_feature_branch_does_no_io(self):
        self.environment['GITHUB_REF'] = 'refs/heads/feature'
        with patch.dict(os.environ, self.environment), patch.object(release.subprocess, 'check_output') as command:
            with self.assertRaisesRegex(ValueError, 'requires.*main'):
                release.main()
            command.assert_not_called()

    def test_protocol_must_be_a_stable_unreplaced_release(self):
        for upstream in (dict(Version='v0.0.0-20260929003705-40e7a13c3c8d'), dict(Version='v0.1.0-rc.1'), dict(Version='v00.1.0'), dict(Version='v0.1.0', Replace=dict(Dir='/local'))):
            responses = ['a' * 40, json.dumps(upstream)]
            with patch.dict(os.environ, self.environment), patch.object(release.subprocess, 'check_output', side_effect=responses) as command, patch.object(release.subprocess, 'run') as mutation:
                with self.assertRaisesRegex(ValueError, 'stable upstream'):
                    release.main()
                mutation.assert_not_called()
                self.assertEqual(command.call_args_list[1].args[0], ['go', 'list', '-m', '-json', 'github.com/batchstream/weir-protocol'])

    def test_workspace_cannot_hide_the_pinned_protocol_pseudo_version(self):
        self.environment['GOWORK'] = '/local/go.work'
        upstream = dict(Version='v0.0.0-20261003000000-123456789abc')
        responses = ['a' * 40, json.dumps(upstream)]
        with patch.dict(os.environ, self.environment), patch.object(release.subprocess, 'check_output', side_effect=responses) as command, patch.object(release.subprocess, 'run') as mutation:
            with self.assertRaisesRegex(ValueError, 'stable upstream'):
                release.main()
            self.assertEqual(command.call_args_list[1].kwargs['env']['GOWORK'], 'off')
            mutation.assert_not_called()

    def test_existing_tag_is_never_retargeted(self):
        for references in ('b' * 40 + '\trefs/tags/v0.2.0\n', 'c' * 40 + '\trefs/tags/v0.2.0\n' + 'b' * 40 + '\trefs/tags/v0.2.0^{}\n'):
            responses = ['a' * 40, json.dumps(dict(Version='v0.1.0')), references]
            with patch.dict(os.environ, self.environment), patch.object(release.subprocess, 'check_output', side_effect=responses), patch.object(release.subprocess, 'run') as mutation:
                with self.assertRaisesRegex(ValueError, 'refusing overwrite'):
                    release.main()
                mutation.assert_not_called()

    def test_release_access_failure_is_not_absence(self):
        responses = ['a' * 40, json.dumps(dict(Version='v0.1.0')), '']
        denied = subprocess.CompletedProcess([], 1, '', 'HTTP 403 denied')
        with patch.dict(os.environ, self.environment), patch.object(release.subprocess, 'check_output', side_effect=responses), patch.object(release.subprocess, 'run', return_value=denied) as command:
            with self.assertRaisesRegex(RuntimeError, 'existing release state'):
                release.main()
            self.assertEqual(command.call_count, 1)


if __name__ == '__main__':
    unittest.main()
