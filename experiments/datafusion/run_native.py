"""Run the already-built production SQL benchmark without concurrent builds."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--rows', nargs='+', type=int, default=[2000, 20000])
parser.add_argument('--repeats', type=int, default=3)
parser.add_argument('--output', type=Path, default=Path('docs/benchmarks'))
args = parser.parse_args()
if args.repeats < 3:
    parser.error('at least three repetitions are required')
args.output.mkdir(parents=True, exist_ok=True)
binary = args.binary.resolve()
fingerprint = hashlib.sha256(binary.read_bytes()).hexdigest()
digests = {}
env = dict(os.environ)
# This benchmark only exercises local, ephemeral in-memory databases.
env.pop('SUBSTRATE_BENCH_PG_DSN', None)
for people in args.rows:
    env['SUBSTRATE_BENCH_ROWS'] = str(people)
    for repetition in range(1, args.repeats + 1):
        try:
            result = subprocess.run([str(binary)], env=env, check=True, text=True, capture_output=True)
        except subprocess.CalledProcessError as error:
            print(error.stderr, file=sys.stderr)
            raise
        data = json.loads(result.stdout)
        if data['postgres_measured']:
            raise AssertionError('unexpected external database access')
        for row in data['measurements']:
            key = (people, row['name'])
            if not row['repeat_verified']:
                raise AssertionError('missing repeated-query correctness check')
            if key in digests and digests[key] != row['sha256']:
                raise AssertionError(f'changed query output: {key}')
            digests[key] = row['sha256']
        data['benchmark_binary_sha256'] = fingerprint
        data['benchmark_binary_bytes'] = binary.stat().st_size
        path = args.output / f'datafusion-native-{people}-{repetition}.json'
        path.write_text(json.dumps(data, indent=2) + '\n')
        print(path, flush=True)
