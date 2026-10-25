"""Serial paired comparison of compiled tiny joins with the prior adapter."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--rows', nargs='+', type=int, default=[2000, 20000])
parser.add_argument('--repeats', type=int, default=3)
args = parser.parse_args()
if args.repeats < 3 or len(set(args.rows)) != len(args.rows) or any(n < 10 or n > 100000 for n in args.rows):
    parser.error('use at least three repeats and distinct row counts in10..100000')
binaries = {'current': args.binary.resolve(), 'baseline': args.baseline.resolve()}
fingerprints = {k: hashlib.sha256(p.read_bytes()).hexdigest() for k,p in binaries.items()}
if len(set(fingerprints.values())) != 2:
    parser.error('expected distinct builds')
args.output.mkdir(parents=True, exist_ok=True)
for n in args.rows:
    for repeat in range(1, args.repeats + 1):
        for kind in binaries:
            p = args.output / f'datafusion-tiny-{kind}-{n}-{repeat}.json'
            if p.exists():
                parser.error(f'output exists: {p}; use a fresh directory')
env = dict(os.environ)
env.pop('SUBSTRATE_BENCH_PG_DSN', None)
digests = {}
for n in args.rows:
    env['SUBSTRATE_BENCH_ROWS'] = str(n)
    for repeat in range(1, args.repeats + 1):
        order = list(binaries) if repeat % 2 else list(reversed(binaries))
        for kind in order:
            started_at = datetime.now(timezone.utc).isoformat()
            started = time.perf_counter()
            run = subprocess.run([str(binaries[kind])], env=env, text=True, capture_output=True)
            if run.returncode:
                raise RuntimeError(run.stderr)
            data = json.loads(run.stdout)
            assert data['people'] == n and not data['postgres_measured']
            for group in ['measurements', 'parameter_rebinding', 'unseen_parameters']:
                for row in data[group]:
                    assert row['repeat_verified']
                    assert 'sqlite_compat_admin' in row and 'sqlite_compat_reader' in row
                    if group == 'unseen_parameters':
                        assert row['first_binding_execution_verified']
                        assert row['distinct_parameter_sets'] == data['samples'] * data['iterations']
                    key = (n, group, row['name'])
                    if key in digests:
                        assert digests[key] == row['sha256'], key
                    digests[key] = row['sha256']
            assert len(data['measurements']) == 9
            if kind == 'current':
                assert {row['name'] for row in data['unseen_parameters']} == {
                    'unseen_id_values', 'unseen_numeric_equalities', 'unseen_numeric_ranges',
                    'unseen_relationship_joins', 'unseen_five_way_joins'}
            for row in data['projection']['measurements']:
                assert row['repeat_verified']
                for principal in ['admin', 'reader']:
                    key = (n, 'projection', row['name'], principal)
                    digest = row[f'{principal}_sha256']
                    if key in digests:
                        assert digests[key] == digest, key
                    digests[key] = digest
            for row in data['sqlite_covering_index']['measurements'] + data['sqlite_covering_index']['unseen_parameters']:
                assert row['repeat_verified'] and row['plan']
                group = 'unseen_parameters' if row['name'].startswith('unseen_') else 'measurements'
                assert digests[(n, group, row['name'])] == row['sha256']
            data.update(benchmark_started_at_utc=started_at,
                        benchmark_process_elapsed_seconds=time.perf_counter() - started,
                        benchmark_adapter=kind, benchmark_binary_sha256=fingerprints[kind],
                        benchmark_binary_bytes=binaries[kind].stat().st_size,
                        paired_repetition=repeat, paired_execution_order=order)
            path = args.output / f'datafusion-tiny-{kind}-{n}-{repeat}.json'
            path.write_text(json.dumps(data, indent=2) + '\n')
            print(path, flush=True)
