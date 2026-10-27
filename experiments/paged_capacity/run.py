#!/usr/bin/env python3
"""Run each capacity fixture in its own process; retain exact child CPU and RSS."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('perf_guard', ROOT / 'tools/perf/guard.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/examples/paged_capacity')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--rows', type=int, nargs='+', default=[10000, 100000, 1000000])
parser.add_argument('--payload', type=int, default=128)
parser.add_argument('--modes', nargs='+', choices=['paged', 'resident'], default=['resident', 'paged'])
parser.add_argument('--timeout', type=int, default=1200)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
guard.save(args.output / 'environment.json', {
    'platform': platform.platform(), 'cpu_count': os.cpu_count(), 'time_unix': time.time(),
    'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
    'binary_sha256': guard.sha(args.binary), 'rows': args.rows, 'payload': args.payload,
    'method': 'One fixed-work fresh process and fresh file per size/mode; diagnostic capacity comparison, not a statistical regression gate.'
})
results = []
for rows in args.rows:
    for mode in args.modes:
        stem = args.output / f'{mode}-{rows}'
        raw, resources = guard.measured(args.binary.resolve(), [mode, str(rows), str(args.payload)], stem, args.timeout, os.environ.copy())
        if raw.get('verified') is not True or raw.get('rows') != rows or raw.get('mode') != mode:
            raise RuntimeError('capacity correctness/fixture mismatch')
        results.append(raw | {'resources': resources})
        guard.save(args.output / 'results.json', results)
        print(f'{mode} {rows}: load {raw["load_seconds"]:.2f}s, reopen {raw["reopen_seconds"]:.3f}s, peak RSS {resources["peak_rss_bytes"] / 2**20:.1f} MiB', flush=True)
