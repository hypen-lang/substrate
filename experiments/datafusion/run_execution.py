"""Compare current SQL with both preserved adapters; serialize all timings."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--range-baseline', type=Path, required=True)
parser.add_argument('--eager-baseline', type=Path, required=True)
parser.add_argument('--rows', nargs='+', type=int, default=[2000, 20000])
parser.add_argument('--repeats', type=int, default=3)
parser.add_argument('--output', type=Path, default=Path('docs/benchmarks'))
args = parser.parse_args()
if args.repeats < 3:
    parser.error('at least three repetitions are required')
if len(set(args.rows)) != len(args.rows) or any(n < 10 or n > 100000 for n in args.rows):
    parser.error('row counts must be distinct and in 10..100000')
binaries = {
    'execution': args.binary.resolve(),
    'execution-range-baseline': args.range_baseline.resolve(),
    'execution-eager-baseline': args.eager_baseline.resolve(),
}
fingerprints = {kind: hashlib.sha256(binary.read_bytes()).hexdigest()
                for kind, binary in binaries.items()}
if len(set(fingerprints.values())) != 3:
    parser.error('expected three distinct executable builds')
args.output.mkdir(parents=True, exist_ok=True)
# Refuse to overwrite measurements, including files produced by an interrupted run.
for people in args.rows:
    for repetition in range(1, args.repeats + 1):
        for kind in binaries:
            path = args.output / f'datafusion-{kind}-{people}-{repetition}.json'
            if path.exists():
                parser.error(f'output already exists: {path}; use a fresh output directory')

digests = {}
env = dict(os.environ)
env.pop('SUBSTRATE_BENCH_PG_DSN', None)
for people in args.rows:
    env['SUBSTRATE_BENCH_ROWS'] = str(people)
    for repetition in range(1, args.repeats + 1):
        # Every adapter occupies each timing-order position once per three repeats.
        names = list(binaries)
        rotation = (repetition - 1) % len(names)
        order = names[rotation:] + names[:rotation]
        for kind in order:
            binary = binaries[kind]
            started_at = datetime.now(timezone.utc).isoformat()
            started = time.perf_counter()
            try:
                result = subprocess.run([str(binary)], env=env, check=True,
                                        text=True, capture_output=True)
            except subprocess.CalledProcessError as error:
                print(error.stderr, file=sys.stderr)
                raise
            data = json.loads(result.stdout)
            if data['postgres_measured'] or data['people'] != people:
                raise AssertionError('unexpected database access or fixture size')
            extended = kind != 'execution-eager-baseline'
            rows = data['measurements'] + (data['parameter_rebinding'] if extended else [])
            for row in rows:
                if extended and ('sqlite_compat_admin' not in row
                                 or 'sqlite_compat_reader' not in row):
                    raise AssertionError('missing SQLite compatibility comparison')
                if not row['repeat_verified']:
                    raise AssertionError('missing repeated-query correctness check')
                key = (people, row['name'])
                if key in digests and digests[key] != row['sha256']:
                    raise AssertionError(f'changed query output: {key}')
                digests[key] = row['sha256']
            if kind == 'execution':
                diagnostics = data['execution_diagnostics']
                if {row['name'] for row in diagnostics} != {
                        '03_range_top_100', '04_relationship_lookup', '05_cross_app_five_way',
                        '09_full_scan_aggregate'}:
                    raise AssertionError('missing fresh execution diagnostics')
                unseen = data['unseen_parameters']
                if len(unseen) != 3 or len({row['name'] for row in unseen}) != 3:
                    raise AssertionError('missing unseen-parameter workloads')
                for row in unseen:
                    if not row['repeat_verified'] or not row['first_binding_execution_verified']:
                        raise AssertionError('unseen bindings were not verified')
                    if row['distinct_parameter_sets'] != data['samples'] * data['iterations']:
                        raise AssertionError('unexpected distinct binding count')
                    key = (people, row['name'])
                    if key in digests and digests[key] != row['sha256']:
                        raise AssertionError(f'changed unseen output: {key}')
                    digests[key] = row['sha256']
                covering = data['sqlite_covering_index']
                controls = covering['measurements'] + covering['unseen_parameters']
                if len(controls) != 3:
                    raise AssertionError('missing covering-index controls')
                for row in controls:
                    if not row['repeat_verified'] or not row['plan']:
                        raise AssertionError('covering-index correctness or plan missing')
                    key = (people, row['name'])
                    if digests.get(key) != row['sha256']:
                        raise AssertionError(f'changed covering-index output: {key}')
            if extended:
                for row in data['projection']['measurements']:
                    if not row['repeat_verified']:
                        raise AssertionError('projection correctness was not verified')
                    for principal in ['admin', 'reader']:
                        key = (people, row['name'], principal)
                        digest = row[f'{principal}_sha256']
                        if key in digests and digests[key] != digest:
                            raise AssertionError(f'changed projection output: {key}')
                        digests[key] = digest
            data['benchmark_started_at_utc'] = started_at
            data['benchmark_process_elapsed_seconds'] = time.perf_counter() - started
            data['benchmark_adapter'] = kind
            data['benchmark_binary_sha256'] = fingerprints[kind]
            data['benchmark_binary_bytes'] = binary.stat().st_size
            data['paired_repetition'] = repetition
            data['paired_execution_order'] = order
            path = args.output / f'datafusion-{kind}-{people}-{repetition}.json'
            with path.open('x') as output:
                output.write(json.dumps(data, indent=2) + '\n')
            print(path, flush=True)
