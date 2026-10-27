"""Pure runner contract tests; never launches a database or benchmarks."""
import importlib.util
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch, Mock

spec = importlib.util.spec_from_file_location('paged_sql_runner', Path(__file__).with_name('run.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_robust_profile_requires_matching_work_and_preserves_cold_sequences(self):
        for suite in ['reads', 'aggregates', 'aggregates-wide']:
            base = dict(runner.expected_native_cases(500000, suite))
            robust = dict(runner.expected_native_cases(500000, suite, 'robust'))
            for case in base:
                self.assertEqual(robust[case], base[case] * (100 if case in runner.HOT_CASES else 1))
                self.assertEqual(runner.expected_warmups(case, 'robust'), 1000 if case in runner.HOT_CASES else 1)
            self.assertEqual(robust['changing_parameters'], 2000)
            self.assertEqual(robust['changing_parameters_warm'], 2000)
        raw = self.result() | {'mode': 'paged', 'interface': 'sql', 'storage': 'paged',
                              'hot_profile': 'robust', 'suite': 'aggregates-wide'}
        raw['queries'] = [dict(case=case, iterations=count, prepare_us=1.0, query_us=1.0,
                               warmup_iterations=runner.expected_warmups(case, 'robust'))
                          for case, count in runner.expected_native_cases(100, 'aggregates-wide', 'robust')]
        runner.validate_result(raw, 'paged', 100, suite='aggregates-wide', hot_profile='robust')
        with self.assertRaisesRegex(RuntimeError, 'hot profile mismatch'):
            runner.validate_result(raw, 'paged', 100, suite='aggregates-wide')
        with self.assertRaisesRegex(RuntimeError, 'hot profile mismatch'):
            runner.validate_result(self.result(), 'postgres', 100, hot_profile='robust')
        bad = raw | {'queries': [raw['queries'][0] | {'iterations': 5000}] + raw['queries'][1:]}
        with self.assertRaisesRegex(RuntimeError, 'case matrix mismatch'):
            runner.validate_result(bad, 'paged', 100, suite='aggregates-wide', hot_profile='robust')
        bad = raw | {'queries': [raw['queries'][0] | {'warmup_iterations': 1}] + raw['queries'][1:]}
        with self.assertRaisesRegex(RuntimeError, 'warmup mismatch'):
            runner.validate_result(bad, 'paged', 100, suite='aggregates-wide', hot_profile='robust')
        with self.assertRaisesRegex(RuntimeError, 'mixed hot profiles'):
            runner.summarize([self.result(), raw])

    def test_io_diagnostics_require_paged_modes_and_cannot_mix_timing_classes(self):
        runner.require_io_modes(['paged', 'native-paged'], True)
        runner.require_io_modes(runner.SQL_MODES, False)
        for modes in [[], ['resident'], ['sqlite'], ['postgres'], ['paged', 'postgres']]:
            with self.assertRaisesRegex(ValueError, 'requires only paged/native-paged'):
                runner.require_io_modes(modes, True)
        counts = dict.fromkeys(['read_calls', 'read_requested_bytes', 'write_calls', 'write_requested_bytes',
                                'sync_calls', 'resize_calls', 'length_calls', 'close_calls', 'errors'], 0)
        result = self.result() | {'mode': 'paged', 'interface': 'sql', 'storage': 'paged',
                                  'timing_class': 'instrumented-diagnostic'}
        result['queries'] = [query | {'backend_io_counts': counts} for query in result['queries']]
        runner.validate_result(result, 'paged', 100, io_diagnostics=True)
        with self.assertRaisesRegex(RuntimeError, 'timing class mismatch'):
            runner.validate_result(result, 'paged', 100)
        with self.assertRaisesRegex(RuntimeError, 'timing class mismatch'):
            runner.validate_result(self.result(), 'postgres', 100, io_diagnostics=True)
        invalid = result | {'queries': [result['queries'][0] | {'backend_io_counts': None}]}
        with self.assertRaisesRegex(RuntimeError, 'I/O counters'):
            runner.validate_result(invalid, 'paged', 100, io_diagnostics=True)
        invalid = result | {'timing_class': 'headline'}
        with self.assertRaisesRegex(RuntimeError, 'headline results must not enable'):
            runner.validate_result(invalid, 'paged', 100)
        with self.assertRaisesRegex(RuntimeError, 'mixed headline and instrumented'):
            runner.summarize([self.result(), result])

    def test_native_matrix_requires_api_storage_labels_and_every_case(self):
        for mode in ['native-resident', 'native-paged']:
            result = self.result() | {'mode': mode, 'interface': 'native',
                                    'storage': mode.removeprefix('native-'),
                                    'suite': 'aggregates',
                                    'native_source_materialization': 'explicit public API materialization'}
            result['queries'] = [dict(case=case, iterations=count, prepare_us=0.0, query_us=1.0,
                                      execution_path='native public scan + client fold')
                                 for case, count in runner.expected_native_cases(100, 'aggregates')]
            self.assertEqual(len(runner.validate_result(result, mode, 100, suite='aggregates')), 11)
            for field in ['interface', 'storage']:
                invalid = dict(result)
                del invalid[field]
                with self.assertRaisesRegex(RuntimeError, 'interface/storage mismatch'):
                    runner.validate_result(invalid, mode, 100, suite='aggregates')
            invalid = result | {'queries': result['queries'][:-1]}
            with self.assertRaisesRegex(RuntimeError, 'native case matrix mismatch'):
                runner.validate_result(invalid, mode, 100, suite='aggregates')
            invalid = result | {'native_source_materialization': None}
            with self.assertRaisesRegex(RuntimeError, 'materialization label'):
                runner.validate_result(invalid, mode, 100, suite='aggregates')
            invalid = result | {'queries': [query | {'execution_path': 'SQL'} for query in result['queries']]}
            with self.assertRaisesRegex(RuntimeError, 'execution path label'):
                runner.validate_result(invalid, mode, 100, suite='aggregates')
        self.assertEqual(len(runner.expected_native_cases(100, 'reads')), 6)
        self.assertEqual(dict(runner.expected_native_cases(1_000_000, 'aggregates'))['aggregate'], 1)

    def test_sql_modes_keep_legacy_compatibility_but_reject_wrong_labels(self):
        result = self.result()
        runner.validate_result(result, 'postgres', 100)
        with self.assertRaisesRegex(RuntimeError, 'interface/storage mismatch'):
            runner.validate_result(result | {'interface': 'native'}, 'postgres', 100)
        with self.assertRaisesRegex(RuntimeError, 'unknown benchmark mode'):
            runner.mode_metadata('native-sqlite')
        self.assertEqual(runner.SQL_MODES, ['resident', 'paged', 'sqlite', 'postgres'])
        self.assertIn('native-paged', runner.MODES)

    def test_reference_order_validation_rejects_old_or_wrong_fixture(self):
        result = self.result()
        runner.validate_result(result, 'postgres', 100, 'ordered')
        with self.assertRaisesRegex(RuntimeError, 'reference-order mismatch'):
            runner.validate_result(result, 'postgres', 100, 'reversed')
        result['reference_order'] = 'reversed'
        runner.validate_result(result, 'postgres', 100, 'reversed')
        with self.assertRaisesRegex(RuntimeError, 'reference-order mismatch'):
            runner.validate_result(result, 'postgres', 100, 'ordered')
        result['reference_order'] = 'invalid'
        with self.assertRaisesRegex(RuntimeError, 'reference-order mismatch'):
            runner.validate_result(result, 'postgres', 100, 'ordered')

    def test_fixed_schema_manifest_identity_and_legacy_markers(self):
        result = self.result()
        self.assertEqual(runner.fixture_identity(result), ('legacy-random-schema-ids', None))
        result['fixture_version'] = 'fixed-schema-ids-v1'
        with self.assertRaisesRegex(RuntimeError, 'invalid fixed schema manifest'):
            runner.fixture_identity(result)
        result['schema_manifest'] = [
            {'relations': [{'id': '01000000-0000-0000-0000-000000000001'}]},
            {'relations': [{'id': '02000000-0000-0000-0000-000000000002'}]},
        ]
        self.assertEqual(runner.fixture_identity(result)[0], 'fixed-schema-ids-v1')
        result['schema_manifest'][0]['relations'][0]['id'] = 'random'
        with self.assertRaisesRegex(RuntimeError, 'fixed schema identity mismatch'):
            runner.fixture_identity(result)
        result['fixture_version'] = 'unknown'
        with self.assertRaisesRegex(RuntimeError, 'unsupported fixture identity version'):
            runner.fixture_identity(result)

    def test_suite_validation_prevents_old_binary_skipping_aggregate_cases(self):
        result = self.result()
        runner.validate_result(result, 'postgres', 100, suite='reads')
        with self.assertRaisesRegex(RuntimeError, 'suite mismatch'):
            runner.validate_result(result, 'postgres', 100, suite='aggregates')
        result['suite'] = 'aggregates'
        runner.validate_result(result, 'postgres', 100, suite='aggregates')
        with self.assertRaisesRegex(RuntimeError, 'suite mismatch'):
            runner.validate_result(result, 'postgres', 100, suite='reads')

    def test_wide_suite_cannot_be_reported_as_small_group_fixture(self):
        result = self.result() | {'suite': 'aggregates-wide'}
        runner.validate_result(result, 'postgres', 100, suite='aggregates-wide')
        with self.assertRaisesRegex(RuntimeError, 'suite mismatch'):
            runner.validate_result(result, 'postgres', 100, suite='aggregates')

    def test_low_disk_space_rejected_before_launch(self):
        with patch.object(runner.shutil, 'disk_usage', return_value=SimpleNamespace(free=4 * 1024 ** 3)):
            with self.assertRaisesRegex(RuntimeError, 'preflight margin'):
                runner.require_disk_space(Path('/tmp'), 5)

    def test_client_tempfiles_removed_on_interruption(self):
        roots = []
        def interrupted(binary, argv, stem, timeout, env):
            root = Path(env['TMPDIR'])
            roots.append(root)
            (root / 'db').write_text('partial database')
            raise KeyboardInterrupt
        with patch.object(runner.guard, 'measured', side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                runner.measure_client('binary', [], Path('/tmp/unused'), 1, {})
        self.assertEqual(len(roots), 1)
        self.assertFalse(roots[0].exists())

    def result(self):
        return {'verified': True, 'people': 100, 'mode': 'postgres',
                'relations': 2, 'namespaces': 2, 'total_rows': 200,
                'load_seconds': 3, 'client_resources': {'peak_rss_bytes': 4096},
                'queries': [{'case': 'point', 'iterations': 2000,
                             'prepare_us': 2.0, 'query_us': 1.0}]}

    def test_interrupted_wait_kills_and_reaps_owned_process_group(self):
        process = SimpleNamespace(pid=123, returncode=None)
        with patch.object(runner.os, 'wait4', side_effect=[KeyboardInterrupt, (123, 0, None)]) as wait, \
                patch.object(runner.os, 'killpg') as kill:
            with self.assertRaises(KeyboardInterrupt):
                runner.reap(process, 10)
            kill.assert_called_once_with(123, runner.signal.SIGKILL)
            self.assertEqual(wait.call_count, 2)
            self.assertEqual(process.returncode, 0)

    def test_sampler_failure_still_stops_server_and_removes_cluster(self):
        cluster = runner.PgCluster({}, Path('/not-used'), {})
        cluster.process = SimpleNamespace(pid=123)
        cluster.sampler = Mock()
        cluster.sampler.stop.side_effect = RuntimeError('telemetry failed')
        temporary = Mock()
        cluster.temporary = temporary
        cluster.start = runner.time.monotonic()
        with patch.object(runner.os, 'kill') as kill, \
                patch.object(runner, 'reap', return_value={'returncode': 0}), \
                patch.object(runner.guard, 'save'):
            with self.assertRaisesRegex(RuntimeError, 'telemetry failed'):
                cluster.close()
            kill.assert_called_once_with(123, runner.signal.SIGINT)
            temporary.cleanup.assert_called_once()
            self.assertIsNone(cluster.process)
            self.assertIsNone(cluster.temporary)

    def test_ps_time_formats(self):
        self.assertEqual(runner.cpu_seconds('01:02.50'), 62.5)
        self.assertEqual(runner.cpu_seconds('2-01:02:03'), 176523)
        self.assertEqual(runner.cpu_seconds('00:00:03'), 3)
        with self.assertRaises(ValueError):
            runner.cpu_seconds('3')

    def test_sampler_excludes_unrelated_processes(self):
        sample = '1 0 999 00:00\n10 1 20 00:01\n20 10 30 00:02\n30 20 40 00:03\n40 1 900 00:04\n'
        tree = runner.process_tree(sample, 10)
        self.assertEqual([p['pid'] for p in tree], [10, 20, 30])
        self.assertEqual(sum(p['rss_bytes'] for p in tree), 90 * 1024)
        self.assertEqual(runner.process_tree(sample, 55), [])

    def test_ambient_connection_overrides_removed(self):
        with patch.dict(runner.os.environ, {'PGHOST': '/existing', 'PGOPTIONS': '-c work_mem=1GB',
                                           'SUBSTRATE_PAGED_PG_DSN': 'existing',
                                           'SUBSTRATE_PAGED_LIBPQ': 'wrong', 'SUBSTRATE_PAGED_REFERENCE_ORDER': 'reversed',
                                           'SUBSTRATE_PAGED_SQL_SUITE': 'aggregates',
                                           'SUBSTRATE_PAGED_IO_DIAGNOSTICS': '1', 'PATH': '/bin'}, clear=True):
            self.assertEqual(runner.clean_environment(), {'PATH': '/bin'})

    def test_validation_rejects_incorrect_or_nonfinite_results(self):
        self.assertEqual(runner.validate_result(self.result(), 'postgres', 100), [('point', 2000)])
        for field, value in [('verified', False), ('mode', 'sqlite'), ('people', 101),
                             ('relations', 3), ('namespaces', 1), ('total_rows', 201)]:
            result = self.result()
            result[field] = value
            with self.assertRaises(RuntimeError):
                runner.validate_result(result, 'postgres', 100)
        for field, value in [('query_us', float('nan')), ('prepare_us', -1), ('iterations', 0)]:
            result = self.result()
            result['queries'][0][field] = value
            with self.assertRaises(RuntimeError):
                runner.validate_result(result, 'postgres', 100)

    def test_median_and_separate_server_accounting(self):
        results = [self.result() for _ in range(3)]
        for result, latency in zip(results, [10, 1, 4]):
            result['queries'][0]['query_us'] = latency
            result['server_resources'] = {'peak_rss_bytes': 10000000}
        summary = runner.summarize(results)[0]
        self.assertEqual(summary['rounds'], 3)
        self.assertEqual(summary['queries'][0]['query_us_median'], 4)
        self.assertEqual(summary['client_peak_rss_bytes_max'], 4096)

    def test_client_cpu_is_optional_validated_and_summarized_separately(self):
        results = [self.result() for _ in range(3)]
        for result, cpu in zip(results, [20, 10, 15]):
            result['queries'][0]['client_cpu_us'] = cpu
            runner.validate_result(result, 'postgres', 100)
        self.assertEqual(runner.summarize(results)[0]['queries'][0]['client_cpu_us_median'], 15)
        results[0]['queries'][0]['client_cpu_us'] = None
        self.assertNotIn('client_cpu_us_median', runner.summarize(results)[0]['queries'][0])
        for cpu in [float('nan'), -1]:
            results[0]['queries'][0]['client_cpu_us'] = cpu
            with self.assertRaisesRegex(RuntimeError, 'invalid client CPU'):
                runner.validate_result(results[0], 'postgres', 100)

    def test_sampler_summary_labels_inexact_memory_and_cpu(self):
        sampler = runner.ServerSampler(10)
        sampler.samples = [
            {'sampling_wall_seconds': .01, 'processes': [
                {'pid': 10, 'cpu_seconds': 1, 'rss_bytes': 100},
                {'pid': 20, 'cpu_seconds': 2, 'rss_bytes': 200}]},
            {'sampling_wall_seconds': .02, 'processes': [
                {'pid': 10, 'cpu_seconds': 3, 'rss_bytes': 120}]}]
        summary = sampler.summary()
        self.assertEqual(summary['observed_process_cpu_seconds_lower_bound'], 5)
        self.assertEqual(summary['sampled_peak_sum_rss_bytes'], 300)
        self.assertIn('double-counts', summary['rss_note'])
        self.assertIn('lower bound', summary['cpu_note'])


if __name__ == '__main__':
    unittest.main()
