#!/usr/bin/env python3
"""Paired, same-host regression gate. Standard library only; Linux and macOS."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent


def read_json(path):
    def invalid(value):
        raise ValueError(f'non-finite JSON number: {value}')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'duplicate JSON key: {key}')
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), parse_constant=invalid, object_pairs_hook=unique)


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def build(args):
    """Copy the selected common harness, build both engines, preserve executables."""
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = 'examples/ci_kernel.rs' if args.suite == 'kernel' else 'benches/comparison.rs'
    name = 'ci_kernel' if args.suite == 'kernel' else 'comparison'
    kind = '--example' if args.suite == 'kernel' else '--bench'
    harness = args.harness_root.resolve() / source
    env = os.environ | {'CARGO_TARGET_DIR': str(args.target.resolve()), 'CARGO_INCREMENTAL': '0'}
    info = {'suite': args.suite, 'rustc': command(['rustc', '-Vv']),
            'cargo': command(['cargo', '-V']), 'harness_sha256': sha(harness),
            'policy_sha256': sha(HERE / 'policy.json'), 'revisions': {},
            'build_environment': {key: env.get(key) for key in
                                  ['CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'RUSTFLAGS']}}
    if not info['rustc'].startswith('rustc 1.94.0 '):
        raise ValueError('CI requires the pinned Rust 1.94.0 toolchain for both builds')
    save(output / 'build.json', info)
    for label, root in [('base', args.base), ('candidate', args.candidate)]:
        root = root.resolve()
        destination = root / source
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination != harness:
            shutil.copyfile(harness, destination)
        manifest = root / 'Cargo.toml'
        # Reuse dependency artifacts only; never accidentally benchmark a stale engine.
        subprocess.run(['cargo', 'clean', '--release', '-p', 'substrate-liquid',
                        '--manifest-path', str(manifest)], env=env, check=True)
        invocation = ['cargo', 'build', '--locked', '--release', kind, name,
                      '--manifest-path', str(manifest), '--message-format=json-render-diagnostics']
        if args.offline:
            invocation.append('--offline')
        with (output / f'{label}-build.jsonl').open('w') as stdout:
            subprocess.run(invocation, env=env, stdout=stdout, check=True)
        artifacts = [json.loads(line) for line in (output / f'{label}-build.jsonl').read_text().splitlines()]
        executable = next(x['executable'] for x in reversed(artifacts)
                          if x.get('reason') == 'compiler-artifact' and x['target']['name'] == name
                          and x.get('executable'))
        shutil.copy2(executable, output / label)
        info['revisions'][label] = {'sha': command(['git', '-C', str(root), 'rev-parse', 'HEAD']),
                                    'lock_sha256': sha(root / 'Cargo.lock'),
                                    'binary_sha256': sha(output / label)}
        save(output / 'build.json', info)


def measured(binary, argv, destination, timeout, env):
    """Fresh process, output files and exact child CPU/RSS; kill descendants on error."""
    with destination.with_suffix('.json').open('w') as stdout, destination.with_suffix('.stderr').open('w') as stderr:
        start = time.monotonic()
        process = subprocess.Popen([str(binary), *argv], stdout=stdout, stderr=stderr,
                                   env=env, start_new_session=True)
        failure = None
        try:
            while True:
                pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.monotonic() - start > timeout:
                    raise TimeoutError(f'{destination.name} exceeded {timeout}s')
                time.sleep(0.02)
        except BaseException as error:
            failure = error
        finally:
            # A leader can exit while descendants keep consuming CPU or holding
            # the output files. Every sample owns a fresh process group.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.returncode is None:
                _, status, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
        resources = {'wall_seconds': time.monotonic() - start,
                     'user_cpu_seconds': usage.ru_utime, 'system_cpu_seconds': usage.ru_stime,
                     'peak_rss_bytes': usage.ru_maxrss * (1 if sys.platform == 'darwin' else 1024),
                     'returncode': process.returncode}
        if failure:
            resources['error'] = str(failure)
        save(destination.with_suffix('.resources.json'), resources)
        if failure:
            raise failure
        if process.returncode:
            raise RuntimeError(f'{destination.name} exited {process.returncode}; see its stderr artifact')
    return read_json(destination.with_suffix('.json')), resources


def normalize(suite, raw, resources, policy):
    expected = policy['suites'][suite]
    if suite == 'kernel':
        if raw.get('protocol') != 1 or raw.get('suite') != suite or raw.get('rows') != expected['rows']:
            raise ValueError('kernel protocol/fixture mismatch')
        metrics = dict(raw['metrics'])
        correctness = raw['correctness']
        if correctness != expected['correctness']:
            raise ValueError('kernel correctness evidence missing or changed')
    else:
        if raw.get('people') != expected['people'] or raw.get('postgres_measured') is not False:
            raise ValueError('SQL fixture mismatch or unexpected external PostgreSQL')
        if raw.get('query_iterations') != expected['query_iterations'] or raw.get('samples') != 7:
            raise ValueError('SQL sampling contract mismatch')
        metrics, correctness = {}, {}
        queries = raw['measurements']
        names = [q['name'] for q in queries]
        if len(set(names)) != len(names) or sorted(names) != sorted(expected['queries']):
            raise ValueError('SQL case missing/duplicated/changed')
        for q in queries:
            if q.get('repeat_verified') is not True or len(q['sha256']) != 64 or q['rows'] < 0:
                raise ValueError('SQL result not verified')
            correctness[q['name']] = {'rows': q['rows'], 'sha256': q['sha256'], 'sql': q['sql']}
            for access in ['admin', 'reader']:
                timing = q[f'substrate_{access}']
                if timing.get('iterations') != expected['query_iterations'][q['name']]:
                    raise ValueError('SQL iteration count mismatch')
                metrics[f"{q['name']}.{access}_us"] = {
                    'value': q[f'substrate_{access}']['median_us'],
                    'direction': 'lower', 'unit': 'microseconds'}
    metrics['peak_rss_bytes'] = {'value': resources['peak_rss_bytes'], 'unit': 'bytes', 'direction': 'lower'}
    if set(metrics) != set(expected['metrics']):
        raise ValueError(f'{suite}: missing/unexpected metrics: {set(metrics) ^ set(expected["metrics"])}')
    for name, metric in metrics.items():
        contract = expected['metrics'][name]
        value = metric['value']
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f'{name}: value must be positive and finite')
        if metric['unit'] != contract['unit'] or metric['direction'] != contract['direction']:
            raise ValueError(f'{name}: changed units or direction')
    return {'metrics': metrics, 'correctness': correctness}


def compare(pairs, contract, policy):
    if len(pairs) != policy['pairs']:
        raise ValueError('incomplete pair count')
    results = {}
    for name, rule in contract.items():
        base = [p['base']['metrics'][name]['value'] for p in pairs]
        candidate = [p['candidate']['metrics'][name]['value'] for p in pairs]
        changes = [(c / b - 1) * (1 if rule['direction'] == 'lower' else -1)
                   for b, c in zip(base, candidate)]
        # Both a relative budget and an absolute floor must be exceeded. Tiny SQL
        # timings otherwise fail on timer/allocator noise below a tenth of a us.
        bad = [delta > rule['budget'] and abs(c - b) > rule.get('absolute_floor', 0)
               for b, c, delta in zip(base, candidate, changes)]
        median = statistics.median(changes)
        mad = statistics.median(abs(x - median) for x in changes)
        too_large = median > rule['budget'] and statistics.median(abs(c-b) for b,c in zip(base,candidate)) > rule.get('absolute_floor',0)
        status = 'pass'
        if sum(bad) >= policy['minimum_bad_pairs'] and too_large:
            status = 'regression'
        elif too_large or mad > policy['max_paired_mad']:
            status = 'inconclusive'
        results[name] = {'status': status, 'median_change': median, 'paired_mad': mad,
                         'bad_pairs': sum(bad), 'budget': rule['budget'],
                         'base_median': statistics.median(base), 'candidate_median': statistics.median(candidate),
                         'unit': rule['unit'], 'paired_changes': changes}
    return results


def markdown(suite, results):
    lines = [f'## Performance: {suite}', '',
             'Positive change means slower, lower throughput, or more memory. Inconclusive measurements fail closed; rerun the complete paired job.', '',
             'Worse by is the median of paired relative changes, not the ratio of the two independently displayed medians.', '',
             '| Metric | Base median | PR median | Worse by | Budget | Result |',
             '| --- | ---: | ---: | ---: | ---: | --- |']
    for name, r in results.items():
        lines.append(f'| {name} ({r["unit"]}) | {r["base_median"]:.4g} | {r["candidate_median"]:.4g} | {r["median_change"]:+.1%} | {r["budget"]:.0%} | {r["status"]} |')
    return '\n'.join(lines) + '\n'


def run(args):
    policy = read_json(HERE / 'policy.json')
    expected = policy['suites'][args.suite]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binaries = {label: (args.binaries / label).resolve() for label in ['base', 'candidate']}
    metadata = {'protocol': 1, 'suite': args.suite, 'platform': platform.platform(),
                'cpu_count': os.cpu_count(), 'time_unix': time.time(), 'policy': policy,
                'binary_sha256': {k: sha(v) for k, v in binaries.items()},
                'runner_image': os.environ.get('ImageVersion'),
                'cpu_info': Path('/proc/cpuinfo').read_text() if Path('/proc/cpuinfo').exists() else platform.processor()}
    save(output / 'metadata.json', metadata)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('SUBSTRATE_BENCH_', 'SCALE_', 'PG'))}
    env['LC_ALL'] = 'C'
    env['SUBSTRATE_BENCH_ROWS'] = str(policy['suites']['sql']['people'])
    if args.suite == 'sql':
        env['SUBSTRATE_BENCH_QUERY_ITERATIONS'] = json.dumps(expected['query_iterations'])
    argv = [str(expected['rows']), str(expected['seconds'])] if args.suite == 'kernel' else []
    pairs, correctness = [], None
    try:
        # Discard one complete warm-up pair. Alternate AB/BA for balanced order.
        for round_ in range(-1, policy['pairs']):
            pair = {}
            order = ['base', 'candidate'] if round_ % 2 == 0 else ['candidate', 'base']
            for label in order:
                destination = output / f'round-{round_+1}-{label}'
                raw, usage = measured(binaries[label], argv, destination, expected['timeout_seconds'], env)
                if args.suite == 'kernel':
                    memory, memory_usage = measured(
                        binaries[label], ['--memory', str(expected['rows'])],
                        output / f'round-{round_+1}-{label}-memory', expected['timeout_seconds'], env)
                    if memory != {'protocol': 1, 'suite': 'kernel-memory', 'rows': expected['rows'], 'verified': True}:
                        raise ValueError('fixed-work memory fixture failed verification')
                    # Timed writers do different amounts of work at different
                    # speeds. Their peak RSS is diagnostic, never a memory gate.
                    usage = memory_usage
                normalized = normalize(args.suite, raw, usage, policy)
                if correctness is None:
                    correctness = normalized['correctness']
                if normalized['correctness'] != correctness:
                    raise ValueError(f'{destination.name}: correctness differs across revisions or repetitions')
                pair[label] = normalized
            if round_ >= 0:
                pairs.append(pair)
                save(output / 'pairs.json', pairs)
                print(f'{args.suite}: completed pair {len(pairs)}/{policy["pairs"]}', flush=True)
        results = compare(pairs, expected['metrics'], policy)
        save(output / 'comparison.json', results)
        report = markdown(args.suite, results)
        (output / 'summary.md').write_text(report)
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
                summary.write(report)
        if any(r['status'] != 'pass' for r in results.values()):
            raise RuntimeError('performance gate failed; see summary.md and comparison.json')
    except Exception as error:
        save(output / 'failure.json', {'error': str(error), 'completed_pairs': len(pairs)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    b = sub.add_parser('build')
    for name in ['base', 'candidate', 'harness-root', 'target', 'output']:
        b.add_argument('--' + name, type=Path, required=True)
    b.add_argument('--offline', action='store_true')
    r = sub.add_parser('run')
    r.add_argument('--binaries', type=Path, required=True)
    r.add_argument('--output', type=Path, required=True)
    for cmd in [b, r]:
        cmd.add_argument('--suite', choices=['kernel', 'sql'], required=True)
    args = parser.parse_args()
    (build if args.action == 'build' else run)(args)


if __name__ == '__main__':
    main()
