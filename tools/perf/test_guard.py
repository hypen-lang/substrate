import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import guard


class GuardTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        os.environ.pop('GITHUB_STEP_SUMMARY', None)
        self.policy = guard.read_json(guard.HERE / 'policy.json')
        self.contract = {'work': {'budget': .15, 'direction': 'lower', 'unit': 'microseconds'}}

    def pairs(self, base, candidate):
        def sample(value):
            return {'metrics': {'work': {'value': value}}}
        return [{'base': sample(b), 'candidate': sample(c)} for b, c in zip(base, candidate)]

    def decision(self, candidate, direction='lower', base=None):
        self.contract['work']['direction'] = direction
        return guard.compare(self.pairs(base or [100] * 6, candidate), self.contract, self.policy)['work']

    def test_identical_and_improved_pass(self):
        self.assertEqual(self.decision([100] * 6)['status'], 'pass')
        self.assertEqual(self.decision([70] * 6)['status'], 'pass')

    def test_repeated_latency_regression_fails(self):
        result = self.decision([125, 124, 123, 122, 121, 99])
        self.assertEqual(result['status'], 'regression')
        self.assertEqual(result['bad_pairs'], 5)

    def test_throughput_direction_is_not_inverted(self):
        self.assertEqual(self.decision([80] * 6, 'higher')['status'], 'regression')
        self.assertEqual(self.decision([120] * 6, 'higher')['status'], 'pass')

    def test_one_outlier_does_not_fail(self):
        self.assertEqual(self.decision([100] * 5 + [500])['status'], 'pass')

    def test_uncertain_regression_fails_closed(self):
        self.assertEqual(self.decision([130] * 4 + [95] * 2)['status'], 'inconclusive')

    def test_noisy_pairs_are_not_green(self):
        self.assertEqual(self.decision([60, 60, 60, 140, 140, 140])['status'], 'inconclusive')

    def test_balanced_host_drift_cancels_in_pairs(self):
        self.assertEqual(self.decision([100, 200, 300, 400, 500, 600], base=[100, 200, 300, 400, 500, 600])['status'], 'pass')

    def test_absolute_floor_suppresses_tiny_timer_differences(self):
        self.contract['work']['absolute_floor'] = .1
        self.assertEqual(self.decision([.12] * 6, base=[.1] * 6)['status'], 'pass')

    def test_incomplete_pairs_are_not_green(self):
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            guard.compare(self.pairs([100], [100]), self.contract, self.policy)

    def kernel(self):
        p = self.policy['suites']['kernel']
        return {'protocol': 1, 'suite': 'kernel', 'rows': p['rows'],
                'correctness': copy.deepcopy(p['correctness']),
                'metrics': {k: {'value': 10, 'unit': v['unit'], 'direction': v['direction']}
                            for k, v in p['metrics'].items() if k != 'peak_rss_bytes'}}

    def normalize_kernel(self, raw):
        return guard.normalize('kernel', raw, {'peak_rss_bytes': 1000}, self.policy)

    def test_complete_kernel_contract(self):
        result = self.normalize_kernel(self.kernel())
        self.assertEqual(set(result['metrics']), set(self.policy['suites']['kernel']['metrics']))

    def test_missing_extra_duplicate_and_nonfinite_data_rejected(self):
        for mutation in ['missing', 'extra', 'nan', 'infinite', 'zero', 'negative', 'boolean', 'units', 'direction', 'correctness', 'rows']:
            with self.subTest(mutation=mutation):
                raw = self.kernel()
                m = raw['metrics']['memory_load_seconds']
                if mutation == 'missing': del raw['metrics']['memory_load_seconds']
                elif mutation == 'extra': raw['metrics']['unreviewed'] = m
                elif mutation == 'units': m['unit'] = 'milliseconds'
                elif mutation == 'direction': m['direction'] = 'higher'
                elif mutation == 'correctness': raw['correctness']['private_denial'] = False
                elif mutation == 'rows': raw['rows'] = 1
                else: m['value'] = {'nan': float('nan'), 'infinite': float('inf'), 'zero': 0, 'negative': -1, 'boolean': True}[mutation]
                with self.assertRaises(ValueError): self.normalize_kernel(raw)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'invalid.json'
            for text in ['{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}']:
                path.write_text(text)
                with self.assertRaises(ValueError): guard.read_json(path)

    def sql(self):
        p = self.policy['suites']['sql']
        return {'people': p['people'], 'postgres_measured': False,
                'samples': 7, 'query_iterations': copy.deepcopy(p['query_iterations']),
                'measurements': [{'name': name, 'repeat_verified': True, 'sha256': '0' * 64,
                                  'rows': 1, 'sql': 'SELECT 1', 'substrate_admin': {'median_us': 1, 'iterations': p['query_iterations'][name]},
                                  'substrate_reader': {'median_us': 2, 'iterations': p['query_iterations'][name]}} for name in p['queries']]}

    def test_sql_cases_and_permissions_must_be_present(self):
        raw = self.sql()
        valid = guard.normalize('sql', raw, {'peak_rss_bytes': 100}, self.policy)
        self.assertEqual(len(valid['metrics']), 19)
        for mutation in ['duplicate', 'missing', 'unverified', 'external_database', 'sampling', 'iterations']:
            x = copy.deepcopy(raw)
            if mutation == 'duplicate': x['measurements'].append(x['measurements'][0])
            if mutation == 'missing': x['measurements'].pop()
            if mutation == 'unverified': x['measurements'][0]['repeat_verified'] = False
            if mutation == 'external_database': x['postgres_measured'] = True
            if mutation == 'sampling': x['query_iterations'].pop(next(iter(x['query_iterations'])))
            if mutation == 'iterations': x['measurements'][0]['substrate_reader']['iterations'] = 1
            with self.assertRaises(ValueError): guard.normalize('sql', x, {'peak_rss_bytes': 100}, self.policy)

    def test_process_failure_and_timeout_do_not_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'probe'
            with self.assertRaises(RuntimeError):
                guard.measured(Path(sys.executable), ['-c', 'raise SystemExit(9)'], path, 2, os.environ.copy())
            with self.assertRaises(TimeoutError):
                guard.measured(Path(sys.executable), ['-c', 'import time;time.sleep(30)'], path, .05, os.environ.copy())
            self.assertIn('error', guard.read_json(path.with_suffix('.resources.json')))
            raw, resources = guard.measured(Path(sys.executable), ['-c', 'print("{\\\"ok\\\":true}")'], path, 2, os.environ.copy())
            self.assertEqual(raw, {'ok': True})
            self.assertGreater(resources['peak_rss_bytes'], 0)

    def test_descendants_cannot_contaminate_later_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            for code in [0, 9]:
                marker = Path(directory) / f'leaked-{code}'
                child = f'import time;from pathlib import Path;time.sleep(.2);Path({str(marker)!r}).touch()'
                parent = f'import subprocess,sys;subprocess.Popen([sys.executable,"-c",{child!r}]);print("{{}}");sys.exit({code})'
                try:
                    guard.measured(Path(sys.executable), ['-c', parent], Path(directory) / 'parent', 2, os.environ.copy())
                except RuntimeError:
                    self.assertEqual(code, 9)
                time.sleep(.3)
                self.assertFalse(marker.exists(), 'descendant survived the sample')

    def test_kernel_rss_uses_fixed_work_process(self):
        import argparse
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for label in ['base', 'candidate']: (root / label).write_text('fake executable')
            def fake(binary, argv, destination, timeout, env):
                if argv[0] == '--memory':
                    return {'protocol': 1, 'suite': 'kernel-memory', 'rows': 10000, 'verified': True}, {'peak_rss_bytes': 123}
                return self.kernel(), {'peak_rss_bytes': 99999999}
            args = argparse.Namespace(suite='kernel', binaries=root, output=root / 'results')
            with patch.object(guard, 'measured', fake): guard.run(args)
            result = guard.read_json(args.output / 'comparison.json')
            self.assertEqual(result['peak_rss_bytes']['candidate_median'], 123)

    def test_run_rejects_changed_correctness_and_retains_failure(self):
        import argparse
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for label in ['base', 'candidate']: (root / label).write_text('fake executable')
            calls = []
            def fake(binary, argv, destination, timeout, env):
                calls.append(binary.name)
                raw = self.sql()
                if len(calls) == 2: raw['measurements'][0]['sha256'] = '1' * 64
                return raw, {'peak_rss_bytes': 100}
            args = argparse.Namespace(suite='sql', binaries=root, output=root / 'results')
            with patch.object(guard, 'measured', fake):
                with self.assertRaisesRegex(ValueError, 'correctness differs'): guard.run(args)
            self.assertTrue((args.output / 'failure.json').exists())
            self.assertFalse((args.output / 'comparison.json').exists())

    def test_runner_balances_order_discards_warmup_and_reports_pass(self):
        import argparse
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for label in ['base', 'candidate']: (root / label).write_text('fake executable')
            calls = []
            def fake(binary, argv, destination, timeout, env):
                calls.append(binary.name)
                self.assertNotIn('SUBSTRATE_BENCH_PG_DSN', env)
                self.assertEqual(json.loads(env['SUBSTRATE_BENCH_QUERY_ITERATIONS']), self.policy['suites']['sql']['query_iterations'])
                return self.sql(), {'peak_rss_bytes': 100}
            args = argparse.Namespace(suite='sql', binaries=root, output=root / 'results')
            with patch.object(guard, 'measured', fake), patch.dict(os.environ, {'SUBSTRATE_BENCH_PG_DSN': 'must-not-use'}, clear=False):
                guard.run(args)
            self.assertEqual(len(calls), 14)
            self.assertEqual(calls[2:], ['base','candidate','candidate','base'] * 3)
            self.assertEqual(len(guard.read_json(args.output / 'pairs.json')), 6)
            self.assertTrue(all(x['status'] == 'pass' for x in guard.read_json(args.output / 'comparison.json').values()))


    def test_saved_metadata_redacts_nested_paths_with_spaces(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'metadata.json'
            original = {'argv': ['/Users/Example Person/project/bin', '/home/Test User/data'],
                        'nested': {'path': str(Path.home() / 'Private Project')}, 'seconds': 3.5}
            guard.save(path, original)
            saved = guard.read_json(path)
            self.assertEqual(saved['argv'], ['<home>/project/bin', '<home>/data'])
            self.assertEqual(saved['nested']['path'], '<home>/Private Project')
            self.assertEqual(saved['seconds'], 3.5)
            self.assertEqual(original['argv'][0], '/Users/Example Person/project/bin')

    def test_build_redacts_logs_only_after_copying_original_executable_path(self):
        import argparse
        with tempfile.TemporaryDirectory(prefix='build with spaces ') as directory:
            root = Path(directory)
            harness = root / 'harness'
            (harness / 'examples').mkdir(parents=True)
            (harness / 'examples/ci_kernel.rs').write_text('fixture')
            for label in ['base', 'candidate']:
                (root / label).mkdir()
                (root / label / 'Cargo.lock').write_text('fixture lock')
            executable = root / 'compiled executable'
            executable.write_bytes(b'original executable')
            args = argparse.Namespace(suite='kernel', output=root / 'output', harness_root=harness,
                                      target=root / 'target', base=root / 'base',
                                      candidate=root / 'candidate', offline=True)
            def fake_run(argv, **kwargs):
                if argv[1] == 'build':
                    kwargs['stdout'].write(json.dumps({'reason': 'compiler-artifact',
                        'target': {'name': 'ci_kernel'}, 'executable': str(executable)}) + '\n')
            def fake_command(argv):
                return 'rustc 1.94.0 fixture' if argv[0] == 'rustc' else 'fixture'
            with patch.object(guard.subprocess, 'run', side_effect=fake_run), \
                    patch.object(guard, 'command', side_effect=fake_command), \
                    patch.object(Path, 'home', return_value=root):
                guard.build(args)
            for label in ['base', 'candidate']:
                self.assertEqual((args.output / label).read_bytes(), b'original executable')
                saved = json.loads((args.output / f'{label}-build.jsonl').read_text())
                self.assertEqual(saved['executable'], '<home>/compiled executable')

    def test_failed_child_saved_stdout_and_stderr_are_sanitized(self):
        with tempfile.TemporaryDirectory() as directory:
            stem = Path(directory) / 'failed'
            code = ('import sys;print(\'{"path":"/Users/Example Person/output"}\');'
                    'print("/home/Test User/private",file=sys.stderr);sys.exit(9)')
            with self.assertRaisesRegex(RuntimeError, 'exited 9'):
                guard.measured(Path(sys.executable), ['-c', code], stem, 2, os.environ.copy())
            self.assertEqual(guard.read_json(stem.with_suffix('.json')), {'path': '<home>/output'})
            self.assertEqual(stem.with_suffix('.stderr').read_text().strip(), '<home>/private')


    def test_experiment_runners_refuse_existing_outputs_before_starting_work(self):
        import subprocess
        root = guard.HERE.parents[1]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'existing.json'
            output.write_text('previous results')
            for script in ['experiments/collections/run.py', 'experiments/collections/run_sql.py']:
                result = subprocess.run([sys.executable, str(root / script), '/unused', str(output)],
                                        text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('output already exists', result.stderr)
                self.assertEqual(output.read_text(), 'previous results')
            result = subprocess.run(['bash', str(root / 'experiments/analytics/benchmark.sh'), directory],
                                    text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('output already exists', result.stderr)


if __name__ == '__main__':
    unittest.main()
