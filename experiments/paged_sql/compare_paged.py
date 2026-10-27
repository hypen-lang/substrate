#!/usr/bin/env python3
"""Run isolated same-runner before/after/SQLite/PostgreSQL comparisons.
No builds/tests may overlap these runs. Output must not already exist.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'experiments/paged_sql/run.py'
spec = importlib.util.spec_from_file_location('paged_runner', RUNNER)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

def save(path, data):
    path.write_text(json.dumps(runner.sanitize(data), indent=2, allow_nan=False) + '\n')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def summary(results):
    out = []
    for label in dict.fromkeys(r['comparison'] for r in results):
        runs = [r for r in results if r['comparison'] == label]
        entry = {'comparison': label, 'rounds': len(runs), 'hot_profile': runs[0].get('hot_profile', 'default'), 'people': runs[0]['people'],
                 'total_rows': runs[0]['total_rows'], 'queries': [],
                 'load_seconds_median': statistics.median(r['load_seconds'] for r in runs),
                 'client_peak_rss_bytes_max': max(r['client_resources']['peak_rss_bytes'] for r in runs),
                 'client_user_cpu_seconds_median': statistics.median(r['client_resources']['user_cpu_seconds'] for r in runs),
                 'client_system_cpu_seconds_median': statistics.median(r['client_resources']['system_cpu_seconds'] for r in runs)}
        footprints = [r.get('file_footprint', {}) for r in runs]
        for name in ('total_allocated_bytes', 'total_logical_bytes'):
            values = [f.get(name) for f in footprints]
            if all(isinstance(v, int) for v in values):
                entry[name + '_median'] = statistics.median(values)
        for q in runs[0]['queries']:
            samples = [next(x for x in r['queries'] if x['case'] == q['case']) for r in runs]
            values = [x['query_us'] for x in samples]
            item = {'case': q['case'], 'iterations_per_round': q['iterations'], 'warmup_iterations': q.get('warmup_iterations', 1),
                    'query_us_median': statistics.median(values),
                    'query_us_min': min(values), 'query_us_max': max(values)}
            cpu = [x.get('client_cpu_us') for x in samples]
            if all(isinstance(v, (int, float)) for v in cpu):
                item['client_cpu_us_median'] = statistics.median(cpu)
            entry['queries'].append(item)
        out.append(entry)
    return out

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--before', type=Path, required=True)
    p.add_argument('--after', type=Path, required=True)
    p.add_argument('--control', type=Path, help='optional intermediate paged binary using the after storage format')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--before-format', choices=('LPG1', 'LPG2', 'LPG3'), default='LPG3')
    p.add_argument('--after-format', choices=('LPG1', 'LPG2', 'LPG3'), default='LPG3')
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--suite', choices=('aggregates', 'aggregates-wide'), default='aggregates')
    p.add_argument('--hot-profile', choices=('default', 'robust'), default='default')
    p.add_argument('--reference-order', choices=('ordered', 'reversed'), default='ordered')
    p.add_argument('--rows', type=int, default=500000)
    p.add_argument('--pg-config', default='pg_config')
    p.add_argument('--io-diagnostics', action='store_true')
    a = p.parse_args()
    a.before = a.before.resolve(); a.after = a.after.resolve(); a.output = a.output.resolve()
    if a.control is not None: a.control = a.control.resolve()
    if not 100 <= a.rows <= 1000000:
        p.error('--rows must be between 100 and 1000000 (people; total records are twice this)')
    if a.rounds < 1:
        p.error('--rounds must be positive')
    if not a.before.is_file() or not a.after.is_file():
        p.error('both benchmark binaries must exist')
    if a.control is not None and not a.control.is_file():
        p.error('control benchmark binary must exist')
    binaries = {'before': a.before, 'after': a.after}
    formats = {'before': a.before_format, 'after': a.after_format}
    paged_labels = ['before', 'after']
    if a.control is not None:
        binaries['control'] = a.control
        formats['control'] = a.after_format
        paged_labels = ['before', 'control', 'after']
    a.output.mkdir(parents=True, exist_ok=False)
    if a.io_diagnostics:
        orders = [paged_labels]
    else:
        labels = paged_labels + ['sqlite', 'postgres']
        orders = [labels[i % len(labels):] + labels[:i % len(labels)] for i in range(a.rounds)]
    plan = {'description': 'Sequential same-runner comparison; no overlapping builds or tests. People plus the same number of scores. Medians are round means, not latency percentiles.',
            'people': a.rows, 'total_rows': a.rows * 2, 'reference_order': a.reference_order,
            'suite': a.suite, 'hot_profile': a.hot_profile,
            'expected_formats': formats,
            'comparison_runner_sha256': sha(Path(__file__)),
            'binaries': {label: {'path': str(path), 'sha256': sha(path)} for label,path in binaries.items()},
            'orders': orders, 'invocations': [], 'runner_sha256': sha(RUNNER),
            'timing_class': 'instrumented-diagnostic' if a.io_diagnostics else 'headline'}
    save(a.output / 'plan.json', plan)
    results = []; expected_fixture = None; expected_cases = None
    for ri, order in enumerate(orders, 1):
        for oi, label in enumerate(order):
            mode = 'paged' if label in binaries else label
            binary = binaries.get(label, a.after)
            destination = a.output / f'round-{ri}-{label}'
            argv = [sys.executable, str(RUNNER), '--suite', a.suite, '--hot-profile', a.hot_profile, '--rows', str(a.rows),
                    '--rounds', '1', '--modes', mode, '--binary', str(binary),
                    '--pg-config', a.pg_config, '--output', str(destination),
                    '--reference-order', a.reference_order]
            if a.io_diagnostics: argv.append('--io-diagnostics')
            plan['invocations'].append(argv); save(a.output / 'plan.json', plan)
            subprocess.run(argv, cwd=ROOT, check=True)
            raw = json.loads((destination / 'results.json').read_text())
            if len(raw) != 1: raise RuntimeError('expected exactly one isolated run')
            raw = raw[0]
            fixture = runner.fixture_identity(raw)
            cases = runner.validate_result(raw, mode, a.rows, a.reference_order, a.suite, a.io_diagnostics, a.hot_profile)
            if label in binaries:
                expected_format = formats[label]
                if raw.get('paged_diagnostics', {}).get('storage', {}).get('format') != expected_format:
                    raise RuntimeError(f'{label} binary must measure {expected_format} storage')
            if len(cases) != 11: raise RuntimeError('expected all 11 query cases')
            if expected_fixture is not None and expected_fixture != fixture: raise RuntimeError('fixture mismatch')
            if expected_cases is not None and expected_cases != cases: raise RuntimeError('query/iteration mismatch')
            expected_fixture, expected_cases = fixture, cases
            raw.update(comparison=label, round=ri, order_index=oi, artifact_directory=str(destination.relative_to(ROOT)) if destination.is_relative_to(ROOT) else str(destination))
            results.append(raw)
            save(a.output / 'results.json', results)
            save(a.output / 'summary.json', summary(results))
    print('Complete:', a.output, flush=True)

if __name__ == '__main__': main()
