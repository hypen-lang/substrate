#!/usr/bin/env python3
"""Compare saved and current paged executables, serial fresh processes/files."""
import argparse
import importlib.util
import os
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('guard', ROOT / 'tools/perf/guard.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--before', type=Path, required=True)
p.add_argument('--after', type=Path, default=ROOT / 'target/release/examples/paged_capacity')
p.add_argument('--output', type=Path, required=True)
p.add_argument('--rows', type=int, nargs='+', default=[10000, 100000, 1000000])
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=False)
binaries = {'before': a.before.resolve(), 'after': a.after.resolve()}
guard.save(a.output / 'environment.json', {
    'platform': platform.platform(), 'cpu_count': os.cpu_count(), 'time_unix': time.time(),
    'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
    'binary_sha256': {k: guard.sha(v) for k,v in binaries.items()},
    'method': 'One diagnostic pair per size, order alternates. Fresh process/file per executable. Identical 128-byte payload, 256-row Immediate batches, 8MiB redb +1MiB decoded cache. No simultaneous build/test/timing.'
})
results = []
for i, rows in enumerate(a.rows):
    for label in (['before','after'] if i % 2 == 0 else ['after','before']):
        raw, resource = guard.measured(binaries[label], ['paged',str(rows),'128'],a.output / f'{label}-{rows}',1200,os.environ.copy())
        if raw.get('verified') is not True or raw.get('rows') != rows or raw.get('mode') != 'paged':
            raise RuntimeError('capacity fixture mismatch')
        results.append(raw | {'revision':label, 'resources':resource})
        guard.save(a.output / 'results.json', results)
        print(f'{label} {rows}: load {raw["load_seconds"]:.2f}s, equality {raw["reads"]["equality_page_us"]:.3f}us, peak {resource["peak_rss_bytes"]/2**20:.1f}MiB',flush=True)
