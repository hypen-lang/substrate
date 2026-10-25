"""Serialized native overhead comparison with frozen binaries and no overwrites."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--current', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
binaries = {'before': args.baseline.resolve(), 'after': args.current.resolve()}
hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
if len(set(hashes.values())) != 2:
    parser.error('baseline and current must be distinct frozen builds')
args.output.mkdir(parents=True, exist_ok=True)
for repeat in range(1, 4):
    for name in binaries:
        path = args.output / f'native-overheads-{name}-{repeat}.json'
        if path.exists():
            parser.error(f'refusing to overwrite {path}; use a fresh output directory')
workloads = None
for repeat in range(1, 4):
    order = ['before', 'after'] if repeat % 2 else ['after', 'before']
    for name in order:
        started = datetime.now(timezone.utc).isoformat()
        start = time.perf_counter()
        completed = subprocess.run([str(binaries[name])], check=True, capture_output=True, text=True)
        result = json.loads(completed.stdout)
        if len(result['writes']) != 9 or not result['projection']['verified'] or result['projection']['rows'] != 256:
            raise AssertionError('benchmark verification or fixture mismatch')
        workload = [{key: value for key, value in row.items() if key != 'result'} for row in result['writes']]
        if workloads is not None and workloads != workload:
            raise AssertionError('write workload changed across builds')
        workloads = workload
        result['measurement'] = {'started_at_utc': started, 'elapsed_seconds': time.perf_counter() - start,
                                 'binary_sha256': hashes[name], 'paired_repetition': repeat,
                                 'execution_order': order, 'adapter': name}
        path = args.output / f'native-overheads-{name}-{repeat}.json'
        path.write_text(json.dumps(result, indent=2) + '\n')
        print(path, flush=True)
