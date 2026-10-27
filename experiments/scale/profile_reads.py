#!/usr/bin/env python3
"""Sample an isolated native read workload; sampled TPS is not a benchmark result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time

from run import Monitor, ROOT, measured


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=1000000)
    parser.add_argument('--clients', type=int, default=32)
    parser.add_argument('--point-api', choices=['snapshot', 'session'], default='snapshot')
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    os.environ["SCALE_POINT_API"] = args.point_api
    args.max_memory_gib, args.min_free_gib, args.max_seconds = 5, 3, 120
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {'binary': str(args.binary),
                'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                'rows': args.rows, 'clients': args.clients, 'point_api': args.point_api,
                'note': 'Native sample adds overhead. Do not use sampled TPS as an unsampled comparison.'}
    (args.output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    with tempfile.TemporaryDirectory(prefix='substrate-read-profile-') as directory:
        database = str(Path(directory) / 'database')
        command = [str(args.binary), 'load', 'substrate', database, str(args.rows)]
        result = measured(command, args.output / 'load', 'profile-load', args)
        if not result['success']:
            raise RuntimeError('profile fixture did not finish')
        command[1] = 'run'
        command.extend(['15', str(args.clients), 'read'])
        with (args.output / 'workload.jsonl').open('x') as output, \
             (args.output / 'stderr.txt').open('x') as stderr:
            process = subprocess.Popen(command, cwd=ROOT, text=True,
                                       stdout=subprocess.PIPE, stderr=stderr)
            ready = threading.Event()

            def consume():
                for line in process.stdout:
                    output.write(line)
                    output.flush()
                    item = json.loads(line)
                    if item.get('event') == 'phase' and item.get('phase') == 'concurrent':
                        ready.set()

            reader = threading.Thread(target=consume)
            reader.start()
            monitor, sampler, peak = Monitor(), None, 0
            started = time.monotonic()
            try:
                while process.poll() is None:
                    metrics = monitor.sample([process.pid])
                    peak = max(peak, metrics['physical_footprint_sum_bytes'])
                    if peak > 5 * 1024**3 or time.monotonic() - started > 120:
                        raise RuntimeError('profile resource guard exceeded')
                    if ready.is_set() and sampler is None:
                        sampler = subprocess.Popen(
                            ['/usr/bin/sample', str(process.pid), '5', '1', '-file',
                             str((args.output / 'stacks.txt').resolve())],
                            stdout=stderr, stderr=stderr)
                    time.sleep(.2)
                if process.returncode:
                    raise RuntimeError(f'workload failed: {process.returncode}')
                if sampler is None or sampler.wait(timeout=15):
                    raise RuntimeError('native sampling failed; workload output retained')
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                reader.join()
                if sampler is not None and sampler.poll() is None:
                    sampler.terminate()
                    try:
                        sampler.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        sampler.kill()
                        sampler.wait()
            (args.output / 'summary.json').write_text(json.dumps({
                'success': True, 'sample_seconds': 5, 'sample_interval_ms': 1,
                'peak_footprint_bytes': peak, 'sampled_run': True,
            }, indent=2) + '\n')


if __name__ == '__main__':
    main()
