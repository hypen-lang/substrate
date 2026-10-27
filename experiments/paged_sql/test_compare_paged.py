"""Comparison orchestration contracts; subprocesses are mocked, no timing runs."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('compare_paged', Path(__file__).with_name('compare_paged.py'))
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class ComparisonTests(unittest.TestCase):
    def execute(self, extra=(), actual_format='LPG3', control=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        binary = root / 'binary'
        binary.write_bytes(b'not executed')
        output = root / 'output'
        commands = []

        def fake_run(argv, **kwargs):
            commands.append(argv)
            destination = Path(argv[argv.index('--output') + 1])
            destination.mkdir()
            raw = {'people': 500000, 'total_rows': 1000000, 'queries': [],
                   'load_seconds': 1.0, 'paged_diagnostics': {'storage': {'format': actual_format}},
                   'client_resources': {'peak_rss_bytes': 1024, 'user_cpu_seconds': 1.0,
                                        'system_cpu_seconds': 0.0}}
            (destination / 'results.json').write_text(json.dumps([raw]))

        arguments = ['compare_paged', '--before', str(binary), '--after', str(binary),
                     '--output', str(output), *extra]
        if control: arguments += ['--control', str(binary)]
        with patch('sys.argv', arguments), patch.object(compare.subprocess, 'run', side_effect=fake_run), \
                patch.object(compare.runner, 'fixture_identity', return_value=('fixed', None)), \
                patch.object(compare.runner, 'validate_result', return_value=tuple(range(11))):
            compare.main()
        return json.loads((output / 'plan.json').read_text()), commands

    def test_default_compares_dense_to_dense_three_rotated_rounds(self):
        plan, commands = self.execute()
        self.assertEqual(len(commands), 12)
        self.assertEqual(plan['expected_formats'], {'before': 'LPG3', 'after': 'LPG3'})
        self.assertEqual(plan['orders'][1], ['after', 'sqlite', 'postgres', 'before'])
        self.assertEqual(plan['total_rows'], 1000000)

    def test_io_has_only_separate_paged_runs(self):
        plan, commands = self.execute(['--io-diagnostics'])
        self.assertEqual(plan['orders'], [['before', 'after']])
        self.assertEqual(plan['timing_class'], 'instrumented-diagnostic')
        self.assertTrue(all('--io-diagnostics' in command for command in commands))
        self.assertTrue(all(command[command.index('--modes')+1] == 'paged' for command in commands))

    def test_optional_control_has_separate_identity_and_rotating_runs(self):
        plan, commands = self.execute(control=True)
        self.assertEqual(len(commands), 15)
        self.assertEqual(plan['orders'][0], ['before', 'control', 'after', 'sqlite', 'postgres'])
        self.assertEqual(plan['orders'][1], ['control', 'after', 'sqlite', 'postgres', 'before'])
        self.assertEqual(plan['expected_formats']['control'], 'LPG3')
        self.assertEqual(set(plan['binaries']), {'before', 'control', 'after'})
        plan, commands = self.execute(['--io-diagnostics'], control=True)
        self.assertEqual(plan['orders'], [['before', 'control', 'after']])
        self.assertTrue(all(command[command.index('--modes')+1] == 'paged' for command in commands))

    def test_round_count_and_reference_order_are_propagated(self):
        plan, commands = self.execute(['--rounds', '4', '--reference-order', 'reversed'])
        self.assertEqual(len(commands), 16)
        self.assertEqual(plan['orders'][3], ['postgres', 'before', 'after', 'sqlite'])
        self.assertTrue(all(command[command.index('--reference-order')+1] == 'reversed' for command in commands))

    def test_wide_suite_and_robust_hot_profile_are_propagated(self):
        plan, commands = self.execute(['--suite', 'aggregates-wide', '--hot-profile', 'robust'])
        self.assertEqual(plan['suite'], 'aggregates-wide')
        self.assertEqual(plan['hot_profile'], 'robust')
        self.assertTrue(all(command[command.index('--suite')+1] == 'aggregates-wide' for command in commands))
        self.assertTrue(all(command[command.index('--hot-profile')+1] == 'robust' for command in commands))

    def test_wrong_storage_format_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'before binary must measure LPG3'):
            self.execute(actual_format='LPG2')

    def test_sanitization_only_changes_saved_values(self):
        original = {'command': [str(compare.ROOT / 'target/bin'), str(Path.home() / 'private')],
                    'nested': ['/Users/Example Person/my project', {'path': '/home/Test User/my file'}], 'measurement': 1.25}
        copied = compare.runner.sanitize(original)
        self.assertEqual(copied['command'], ['<workspace>/target/bin', '<home>/private'])
        self.assertEqual(copied['nested'], ['<home>/my project', {'path': '<home>/my file'}])
        self.assertEqual(copied['measurement'], 1.25)
        self.assertEqual(original['command'][0], str(compare.ROOT / 'target/bin'))


    def test_client_artifacts_are_sanitized_after_failure_without_changing_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            stem = Path(temporary) / 'client'
            binary = compare.ROOT / 'target' / 'test binary'
            argv = ['paged', '100']
            def fail(actual_binary, actual_argv, actual_stem, timeout, environment):
                self.assertEqual(actual_binary, binary)
                self.assertEqual(actual_argv, argv)
                actual_stem.with_suffix('.json').write_text(json.dumps({'path': str(binary)}))
                actual_stem.with_suffix('.stderr').write_text('/Users/Example Person/private file')
                raise RuntimeError('benchmark failed')
            with patch.object(compare.runner.guard, 'measured', side_effect=fail):
                with self.assertRaisesRegex(RuntimeError, 'benchmark failed'):
                    compare.runner.measure_client(binary, argv, stem, 1, {})
            self.assertEqual(json.loads(stem.with_suffix('.json').read_text()),
                             {'path': '<workspace>/target/test binary'})
            self.assertEqual(stem.with_suffix('.stderr').read_text(), '<home>/private file')

    def test_saved_nested_plan_remains_valid_json_and_keeps_numbers(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'plan.json'
            compare.save(path, {'nested': {'argv': [str(Path.home() / 'Project with spaces' / 'engine')]},
                                'elapsed': 3.25, 'verified': True})
            value = json.loads(path.read_text())
            self.assertEqual(value['nested']['argv'], ['<home>/Project with spaces/engine'])
            self.assertEqual(value['elapsed'], 3.25)
            self.assertIs(value['verified'], True)


if __name__ == '__main__':
    unittest.main()
