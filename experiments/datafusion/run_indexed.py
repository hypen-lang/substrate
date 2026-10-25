"""Run paired indexed/eager SQL benchmarks, with no concurrent builds."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--baseline-binary', type=Path)
parser.add_argument('--rows', nargs='+', type=int, default=[2000, 20000])
parser.add_argument('--repeats', type=int, default=3)
parser.add_argument('--output', type=Path, default=Path('docs/benchmarks'))
args = parser.parse_args()
if args.repeats < 3:
    parser.error('at least three repetitions are required')
args.output.mkdir(parents=True, exist_ok=True)
binaries = {'indexed': args.binary.resolve()}
if args.baseline_binary:
    binaries['eager-baseline'] = args.baseline_binary.resolve()
fingerprints = {kind: hashlib.sha256(binary.read_bytes()).hexdigest()
                for kind, binary in binaries.items()}
digests = {}
env = dict(os.environ)
# The fixtures are local, ephemeral in-memory databases only.
env.pop('SUBSTRATE_BENCH_PG_DSN', None)
for people in args.rows:
    env['SUBSTRATE_BENCH_ROWS'] = str(people)
    for repetition in range(1, args.repeats + 1):
        # Alternate old/new ordering to avoid always timing one build first.
        order = list(binaries)
        if repetition % 2:
            order.reverse()
        for kind in order:
            binary = binaries[kind]
            try:
                result = subprocess.run([str(binary)], env=env, check=True,
                                        text=True, capture_output=True)
            except subprocess.CalledProcessError as error:
                print(error.stderr, file=sys.stderr)
                raise
            data = json.loads(result.stdout)
            if data['postgres_measured']:
                raise AssertionError('unexpected external database access')
            rows = data['measurements']
            if kind == 'indexed':
                rows = rows + data['parameter_rebinding']
            for row in rows:
                if kind == 'indexed' and ('sqlite_compat_admin' not in row
                                          or 'sqlite_compat_reader' not in row):
                    raise AssertionError('missing old SQLite adapter comparison')
                key = (people, row['name'])
                if not row['repeat_verified']:
                    raise AssertionError('missing repeated-query correctness check')
                if key in digests and digests[key] != row['sha256']:
                    raise AssertionError(f'changed query output: {key}')
                digests[key] = row['sha256']
            if kind == 'indexed':
                for row in data['projection']['measurements']:
                    if not row['repeat_verified']:
                        raise AssertionError('projection correctness was not verified')
                    for principal in ['admin', 'reader']:
                        key = (people, row['name'], principal)
                        digest = row[f'{principal}_sha256']
                        if key in digests and digests[key] != digest:
                            raise AssertionError(f'changed projection output: {key}')
                        digests[key] = digest
            data['benchmark_adapter'] = kind
            data['benchmark_binary_sha256'] = fingerprints[kind]
            data['benchmark_binary_bytes'] = binary.stat().st_size
            data['paired_repetition'] = repetition
            data['paired_execution_order'] = order
            path = args.output / f'datafusion-{kind}-{people}-{repetition}.json'
            path.write_text(json.dumps(data, indent=2) + '\n')
            print(path, flush=True)
