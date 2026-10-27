#!/usr/bin/env python3
"""Sequential same-host SQL comparison, with a disposable PostgreSQL 18 server.

Every mode/size/round uses a fresh client process and database. PostgreSQL uses
only its own private Unix socket, never TCP or an existing cluster. Client and
server resources are reported separately; sampled server RSS counts shared pages
in each process and is not unique physical memory.
"""
import argparse
from collections import defaultdict
import importlib.util
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import statistics
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('guard', ROOT / 'tools/perf/guard.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
SQL_MODES = ['resident', 'paged', 'sqlite', 'postgres']
MODES = SQL_MODES + ['native-resident', 'native-paged']


sanitize = guard.sanitize
sanitize_artifact = guard.sanitize_artifact


def save(path, value):
    guard.save(path, value)


def mode_metadata(mode):
    if mode not in MODES:
        raise RuntimeError(f'unknown benchmark mode: {mode}')
    return ('native', mode.removeprefix('native-')) if mode.startswith('native-') else ('sql', mode)


def require_io_modes(modes, enabled):
    if enabled and (not modes or any(mode not in ('paged', 'native-paged') for mode in modes)):
        raise ValueError('--io-diagnostics requires only paged/native-paged modes')


HOT_CASES = {'point', 'equality', 'selective_join', 'aggregate_equality'}


def expected_warmups(case, hot_profile):
    return 1000 if hot_profile == 'robust' and case in HOT_CASES else 1


def expected_native_cases(rows, suite, hot_profile='default'):
    result = [('point', 5000), ('equality', 2000), ('changing_parameters', 2000),
              ('changing_parameters_warm', 2000), ('selective_join', 2000),
              ('aggregate', 3 if rows <= 100_000 else 1)]
    if suite != 'reads':
        result += [('aggregate_equality', 1000), ('aggregate_range', 20),
                   ('aggregate_broad', 3), ('aggregate_grouped', 3),
                   ('aggregate_filtered_grouped', 3)]
    if hot_profile == 'robust':
        result = [(case, count * 100 if case in HOT_CASES else count) for case, count in result]
    return result


def require_disk_space(output, minimum_gib):
    # A preflight margin, not a prediction or quota. Check both temporary storage
    # locations because PostgreSQL uses /tmp and Rust uses the OS temp directory.
    for path in (output, Path('/tmp'), Path(tempfile.gettempdir())):
        free = shutil.disk_usage(path).free
        if free < minimum_gib * 1024 ** 3:
            raise RuntimeError(f'{path}: only {free / 1024 ** 3:.2f} GiB free; '
                               f'benchmark requires a {minimum_gib:g} GiB preflight margin')


def measure_client(binary, argv, stem, timeout, environment):
    # Own the Rust tempfile parent so timeout/interrupt cleanup does not depend
    # on Rust destructors running after guard.measured kills the child process.
    with tempfile.TemporaryDirectory(prefix='substrate-sql-client-') as root:
        try:
            return guard.measured(binary, argv, stem, timeout,
                                  environment | {'TMPDIR': root, 'TMP': root, 'TEMP': root})
        finally:
            for suffix in ('.json', '.stderr', '.resources.json'):
                sanitize_artifact(stem.with_suffix(suffix))


def clean_environment():
    # Ambient libpq services/options must not redirect the isolated connection or
    # silently alter session settings. Every PostgreSQL connection is explicit.
    return {k: v for k, v in os.environ.items()
            if not k.startswith('PG') and k not in
            ('SUBSTRATE_PAGED_PG_DSN', 'SUBSTRATE_PAGED_LIBPQ', 'SUBSTRATE_PAGED_REFERENCE_ORDER', 'SUBSTRATE_PAGED_SQL_SUITE', 'SUBSTRATE_PAGED_IO_DIAGNOSTICS', 'SUBSTRATE_PAGED_SQL_HOT_PROFILE')}


def text_command(argv):
    return subprocess.check_output(argv, text=True, timeout=30).strip()


def postgres_tools(pg_config):
    config = shutil.which(str(pg_config))
    if config is None:
        raise RuntimeError('PostgreSQL 18 pg_config is required; use --pg-config PATH')
    bindir = Path(text_command([config, '--bindir']))
    libdir = Path(text_command([config, '--libdir']))
    tools = {name: str(bindir / name) for name in ('postgres', 'initdb', 'pg_isready')}
    version = text_command([tools['postgres'], '--version'])
    if not re.search(r'PostgreSQL\) 18(?:\.|\s|$)', version):
        raise RuntimeError(f'PostgreSQL 18 required, found {version!r}')
    libraries = [p for p in (libdir / 'libpq.dylib', libdir / 'libpq.so') if p.is_file()]
    if not libraries:
        raise RuntimeError(f'libpq shared library not found under {libdir}')
    return tools | {
        'pg_config': config, 'pg_config_version': text_command([config, '--version']),
        'server_version': version, 'libpq': str(libraries[0].resolve()),
        'libpq_sha256': guard.sha(libraries[0]),
        'postgres_sha256': guard.sha(Path(tools['postgres'])),
    }


def resource_record(usage, status, elapsed):
    return {
        'wall_seconds': elapsed, 'user_cpu_seconds': usage.ru_utime,
        'system_cpu_seconds': usage.ru_stime,
        'peak_rss_bytes': usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
        'returncode': os.waitstatus_to_exitcode(status),
    }


def reap(process, timeout):
    """Wait exactly once, preserving OS wait4 resources even on timeout."""
    start = time.monotonic()
    timed_out = False
    try:
        while True:
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                break
            if time.monotonic() - start > timeout:
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                _, status, usage = os.wait4(process.pid, 0)
                break
            time.sleep(0.02)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            _, status, _ = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
        except ChildProcessError:
            pass
        raise
    process.returncode = os.waitstatus_to_exitcode(status)
    resources = resource_record(usage, status, time.monotonic() - start)
    if timed_out:
        resources['error'] = f'timeout after {timeout}s; isolated process group killed'
    return resources


def setup_command(argv, destination, environment):
    try:
        with destination.with_suffix('.stdout').open('w') as stdout, \
                destination.with_suffix('.stderr').open('w') as stderr:
            process = subprocess.Popen(argv, stdout=stdout, stderr=stderr,
                                       env=environment, start_new_session=True)
            resources = reap(process, 60)
    finally:
        for suffix in ('.stdout', '.stderr'):
            sanitize_artifact(destination.with_suffix(suffix))
    save(destination.with_suffix('.resources.json'), resources | {'argv': argv})
    if resources['returncode']:
        raise RuntimeError(f'{argv[0]} failed; see {destination}.stderr')
    return resources


def cpu_seconds(value):
    """Parse ps TIME on macOS/Linux, including day and fractional-second forms."""
    days, clock = (value.split('-', 1) if '-' in value else ('0', value))
    fields = clock.split(':')
    if len(fields) not in (2, 3):
        raise ValueError(f'unrecognized ps time: {value}')
    seconds = 0.0
    for part in fields:
        seconds = seconds * 60 + float(part)
    return int(days) * 86400 + seconds


def process_tree(text, root):
    records = {}
    for line in text.splitlines():
        values = line.split()
        if len(values) != 4:
            continue
        pid, parent, rss = map(int, values[:3])
        records[pid] = {'pid': pid, 'parent': parent, 'rss_bytes': rss * 1024,
                        'cpu_seconds': cpu_seconds(values[3])}
    selected = {root} if root in records else set()
    while True:
        children = {pid for pid, item in records.items() if item['parent'] in selected}
        enlarged = selected | children
        if enlarged == selected:
            return [records[pid] for pid in sorted(selected)]
        selected = enlarged


class ServerSampler:
    """Low-frequency read-only process-tree samples; never a client RSS proxy."""
    def __init__(self, pid):
        self.pid = pid
        self.stop_event = threading.Event()
        self.samples = []
        self.errors = []
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop_event.is_set():
            start = time.monotonic()
            try:
                output = subprocess.check_output(
                    ['ps', '-axo', 'pid=,ppid=,rss=,time='], text=True, timeout=2)
                self.samples.append({'elapsed_seconds': start - self.started,
                                     'sampling_wall_seconds': time.monotonic() - start,
                                     'processes': process_tree(output, self.pid)})
            except (OSError, subprocess.SubprocessError, ValueError) as error:
                self.errors.append(str(error))
                # Do not repeatedly spawn denied telemetry commands.
                break
            self.stop_event.wait(0.25)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            self.errors.append('sampler did not stop within three seconds')
        return self.summary()

    def summary(self):
        maxima = {}
        for sample in self.samples:
            for item in sample['processes']:
                maxima[item['pid']] = max(maxima.get(item['pid'], 0), item['cpu_seconds'])
        return {
            'method': 'ps process-tree samples every 250 ms during client load/queries only',
            'cpu_note': 'Sum of maximum observed lifetime CPU TIME per PID, including startup CPU; '
                        'a lower bound because short-lived processes and work after the final sample '
                        'may be missed. TIME has OS-specific precision.',
            'rss_note': 'Sum of per-process resident pages double-counts shared PostgreSQL pages; '
                        'this is neither unique physical memory nor a hard memory budget.',
            'observed_process_cpu_seconds_lower_bound': sum(maxima.values()),
            'sampled_peak_sum_rss_bytes': max((sum(p['rss_bytes'] for p in s['processes'])
                                             for s in self.samples), default=None),
            'sampled_peak_single_process_rss_bytes': max((p['rss_bytes'] for s in self.samples
                                                        for p in s['processes']), default=None),
            'sampling_wall_seconds': sum(s['sampling_wall_seconds'] for s in self.samples),
            'sample_count': len(self.samples), 'errors': self.errors,
        }


class PgCluster:
    def __init__(self, tools, artifacts, environment):
        self.tools, self.artifacts, self.environment = tools, artifacts, environment
        self.temporary = None
        self.process = None
        self.log = None
        self.sampler = None
        self.resources = {}

    def __enter__(self):
        self.artifacts.mkdir()
        # Keep Unix socket paths below platform limits, independently of where
        # benchmark artifacts live. TemporaryDirectory is created mode 0700.
        self.temporary = tempfile.TemporaryDirectory(prefix='substrate-pg-', dir='/tmp')
        root = Path(self.temporary.name)
        self.data = root / 'data'
        self.socket = root / 'socket'
        self.socket.mkdir(mode=0o700)
        try:
            self.resources['initdb'] = setup_command(
                [self.tools['initdb'], '-D', str(self.data), '--no-locale', '--encoding=UTF8',
                 '--auth-local=trust', '--auth-host=reject', '--username=benchmark'],
                self.artifacts / 'initdb', self.environment)
            settings = {
                'listen_addresses': '', 'unix_socket_directories': str(self.socket),
                'unix_socket_permissions': '0700', 'port': '5432',
                'shared_buffers': '8MB', 'work_mem': '4MB', 'max_connections': '16',
                'fsync': 'on', 'synchronous_commit': 'on', 'full_page_writes': 'on',
                'max_parallel_workers_per_gather': '0', 'max_parallel_workers': '0',
                'wal_sync_method': 'fsync_writethrough' if platform.system() == 'Darwin' else 'fdatasync',
            }
            command = [self.tools['postgres'], '-D', str(self.data)]
            for key, value in settings.items():
                command.extend(['-c', f'{key}={value}'])
            self.resources['requested_settings'] = settings
            self.resources['server_argv'] = command
            self.log = (self.artifacts / 'server.log').open('w')
            self.start = time.monotonic()
            self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT,
                                            env=self.environment, start_new_session=True)
            while True:
                ready = subprocess.run(
                    [self.tools['pg_isready'], '-h', str(self.socket), '-p', '5432',
                     '-U', 'benchmark', '-d', 'postgres', '-t', '1'],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=3, env=self.environment)
                if ready.returncode == 0:
                    break
                if time.monotonic() - self.start > 30:
                    raise RuntimeError('isolated PostgreSQL did not become ready; see server.log')
                time.sleep(0.05)
            self.resources['startup_wall_seconds'] = time.monotonic() - self.start
            self.resources['postmaster_pid'] = self.process.pid
            self.sampler = ServerSampler(self.process.pid)
            self.sampler.start()
            # The random /tmp path cannot contain a quote. Explicit empty options
            # also prevents ambient session option injection through libpq.
            self.dsn = f"host='{self.socket}' port=5432 user=benchmark dbname=postgres connect_timeout=5 options=''"
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        try:
            if self.sampler:
                self.resources['client_phase_sampling'] = self.sampler.stop()
                save(self.artifacts / 'server-samples.json', self.sampler.samples)
                self.sampler = None
        finally:
            try:
                if self.process is not None:
                    # os.kill avoids Popen.poll reaping an already exited server
                    # before wait4 can retain its resource accounting.
                    try:
                        os.kill(self.process.pid, signal.SIGINT)
                    except ProcessLookupError:
                        pass
                    resources = reap(self.process, 30)
                    resources['wall_seconds'] = time.monotonic() - self.start
                    resources['scope'] = ('Postmaster wait4 over startup, client benchmark and shutdown; '
                                          'CPU includes children only as accounted by OS; '
                                          'peak RSS is not summed server memory.')
                    self.resources['postmaster_wait4'] = resources
                    self.process = None
            finally:
                if self.log:
                    self.log.close()
                    self.log = None
                    sanitize_artifact(self.artifacts / 'server.log')
                try:
                    save(self.artifacts / 'server-resources.json', self.resources)
                finally:
                    if self.temporary:
                        self.temporary.cleanup()
                        self.temporary = None

    def __exit__(self, *_):
        self.close()


def fixture_identity(raw):
    version = raw.get('fixture_version', 'legacy-random-schema-ids')
    manifest = raw.get('schema_manifest')
    if version == 'fixed-schema-ids-v1':
        try:
            identities = [module['relations'][0]['id'] for module in manifest]
        except (KeyError, IndexError, TypeError) as error:
            raise RuntimeError('invalid fixed schema manifest') from error
        if identities != ['01000000-0000-0000-0000-000000000001',
                          '02000000-0000-0000-0000-000000000002']:
            raise RuntimeError('fixed schema identity mismatch')
    elif version != 'legacy-random-schema-ids' or manifest is not None:
        raise RuntimeError('unsupported fixture identity version')
    return version, manifest


def validate_result(raw, mode, rows, reference_order='ordered', suite='reads', io_diagnostics=False, hot_profile='default'):
    fixture_identity(raw)
    if hot_profile not in ('default', 'robust') or raw.get('hot_profile', 'default') != hot_profile:
        raise RuntimeError('hot profile mismatch; legacy binaries support default only')
    interface, storage = mode_metadata(mode)
    timing_class = 'instrumented-diagnostic' if io_diagnostics else 'headline'
    if raw.get('timing_class', 'headline') != timing_class:
        raise RuntimeError('timing class mismatch; never mix instrumented diagnostics with headline timings')
    if io_diagnostics and storage != 'paged':
        raise RuntimeError('I/O diagnostics require paged storage')
    # Existing SQL binary results remain readable. Native modes are new and
    # must identify their API/storage explicitly, never silently run SQL.
    if raw.get('interface', None if interface == 'native' else interface) != interface or \
            raw.get('storage', None if interface == 'native' else storage) != storage:
        raise RuntimeError('interface/storage mismatch')
    # Older saved binaries only implement the ordered fixture. They may omit the
    # field for ordered comparisons, but must never masquerade as reversed.
    if raw.get('suite', 'reads') != suite:
        raise RuntimeError('suite mismatch (old binaries support reads only)')
    if raw.get('reference_order', 'ordered') != reference_order:
        raise RuntimeError('reference-order mismatch (old binaries support ordered only)')
    if raw.get('verified') is not True or raw.get('people') != rows or raw.get('mode') != mode:
        raise RuntimeError('SQL correctness/fixture mismatch')
    if raw.get('relations') != 2 or raw.get('namespaces') != 2 or raw.get('total_rows') != 2 * rows:
        raise RuntimeError('SQL shape mismatch: expected two relations across two namespaces')
    cases = [(q['case'], q['iterations']) for q in raw['queries']]
    if (interface == 'native' or 'hot_profile' in raw) and cases != expected_native_cases(rows, suite, hot_profile):
        raise RuntimeError('native case matrix mismatch' if interface == 'native' else 'hot-profile case matrix mismatch')
    if not cases or len(set(name for name, _ in cases)) != len(cases):
        raise RuntimeError('missing or duplicate query cases')
    if interface == 'native' and not isinstance(raw.get('native_source_materialization'), str):
        raise RuntimeError('missing native source materialization label')
    for query in raw['queries']:
        if 'hot_profile' in raw or hot_profile != 'default':
            if type(query.get('warmup_iterations')) is not int or query['warmup_iterations'] != expected_warmups(query['case'], hot_profile):
                raise RuntimeError('hot-profile warmup mismatch')
        elif 'warmup_iterations' in query and query['warmup_iterations'] != 1:
            raise RuntimeError('legacy warmup mismatch')
        if io_diagnostics:
            counts = query.get('backend_io_counts')
            required = {'read_calls', 'read_requested_bytes', 'write_calls', 'write_requested_bytes',
                        'sync_calls', 'resize_calls', 'length_calls', 'close_calls', 'errors'}
            if not isinstance(counts, dict) or set(counts) != required or \
                    any(type(value) is not int or value < 0 for value in counts.values()):
                raise RuntimeError('missing or invalid backend I/O counters')
        elif query.get('backend_io_counts') is not None:
            raise RuntimeError('headline results must not enable backend I/O counters')
        if interface == 'native' and not query.get('execution_path', '').startswith('native public '):
            raise RuntimeError('missing native execution path label')
        if query['iterations'] < 1 or any(not math.isfinite(query[k]) or query[k] < 0
                                         for k in ('prepare_us', 'query_us')):
            raise RuntimeError('invalid query measurement')
        cpu = query.get('client_cpu_us')
        if cpu is not None and (not math.isfinite(cpu) or cpu < 0):
            raise RuntimeError('invalid client CPU measurement')
    return cases


def summarize(results):
    timing_classes = {result.get('timing_class', 'headline') for result in results}
    if len(timing_classes) > 1:
        raise RuntimeError('cannot summarize mixed headline and instrumented timing classes')
    if len({result.get('hot_profile', 'default') for result in results}) > 1:
        raise RuntimeError('cannot summarize mixed hot profiles')
    grouped = defaultdict(list)
    for result in results:
        grouped[(result['people'], result['mode'])].append(result)
    summary = []
    for (rows, mode), runs in sorted(grouped.items()):
        queries = []
        for case in runs[0]['queries']:
            values = [next(q['query_us'] for q in r['queries'] if q['case'] == case['case']) for r in runs]
            queries.append({'case': case['case'], 'iterations_per_round': case['iterations'],
                            'warmup_iterations': case.get('warmup_iterations', 1),
                            'query_us_median': statistics.median(values),
                            'query_us_min': min(values), 'query_us_max': max(values)})
            if 'execution_path' in case:
                queries[-1]['execution_path'] = case['execution_path']
            cpu = [next(q.get('client_cpu_us') for q in r['queries'] if q['case'] == case['case']) for r in runs]
            if all(value is not None for value in cpu):
                queries[-1]['client_cpu_us_median'] = statistics.median(cpu)
        summary.append({'people': rows, 'mode': mode,
                        'interface': mode_metadata(mode)[0], 'storage': mode_metadata(mode)[1],
                        'timing_class': runs[0].get('timing_class', 'headline'), 'reference_order': runs[0].get('reference_order', 'ordered'),
                        'suite': runs[0].get('suite', 'reads'),
                        'hot_profile': runs[0].get('hot_profile', 'default'),
                        'native_source_materialization': runs[0].get('native_source_materialization'),
                        'fixture_version': fixture_identity(runs[0])[0],
                        'schema_manifest': fixture_identity(runs[0])[1],
                        'rounds': len(runs), 'queries': queries,
                        'load_seconds_median': statistics.median(r['load_seconds'] for r in runs),
                        'client_peak_rss_bytes_max': max(r['client_resources']['peak_rss_bytes'] for r in runs)})
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, nargs='+', default=[10000, 100000])
    parser.add_argument('--modes', nargs='+', choices=MODES, default=SQL_MODES)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--io-diagnostics', action='store_true',
                        help='separate instrumented paged-only run; never headline performance')
    parser.add_argument('--suite', choices=['reads', 'aggregates', 'aggregates-wide'], default='reads')
    parser.add_argument('--hot-profile', choices=['default', 'robust'], default='default',
                        help='robust: 100x fixed hot-query iterations and 1000 warmups; changing parameters/scans unchanged')
    parser.add_argument('--reference-order', choices=['ordered', 'reversed'], default='ordered')
    parser.add_argument('--timeout', type=int, default=1200)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/examples/paged_sql_bench')
    parser.add_argument('--pg-config', default='pg_config')
    parser.add_argument('--min-free-gib', type=float, default=5.0)
    args = parser.parse_args()
    if args.rounds < 1 or args.timeout < 1 or any(n < 100 or n > 1000000 for n in args.rows):
        parser.error('rounds/timeout must be positive; rows must be between 100 and 1000000')
    if len(set(args.rows)) != len(args.rows) or len(set(args.modes)) != len(args.modes):
        parser.error('duplicate sizes or modes are not allowed')
    if not math.isfinite(args.min_free_gib) or args.min_free_gib <= 0:
        parser.error('min-free-gib must be finite and positive')
    try:
        require_io_modes(args.modes, args.io_diagnostics)
    except ValueError as error:
        parser.error(str(error))
    binary = args.binary.resolve()
    environment = clean_environment() | {'SUBSTRATE_PAGED_REFERENCE_ORDER': args.reference_order,
                                       'SUBSTRATE_PAGED_SQL_SUITE': args.suite,
                                       'SUBSTRATE_PAGED_SQL_HOT_PROFILE': args.hot_profile,
                                       'SUBSTRATE_PAGED_IO_DIAGNOSTICS': '1' if args.io_diagnostics else '0'}
    tools = postgres_tools(args.pg_config) if 'postgres' in args.modes else None
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / 'environment.json', {
        'platform': platform.platform(), 'cpu_count': os.cpu_count(), 'time_unix': time.time(),
        'rustc': text_command(['rustc', '-Vv']), 'binary_sha256': guard.sha(binary),
        'runner_sha256': guard.sha(Path(__file__)), 'rows': args.rows,
        'rounds': args.rounds, 'modes': args.modes, 'postgres_tools': tools,
        'reference_order': args.reference_order, 'suite': args.suite, 'hot_profile': args.hot_profile,
        'io_diagnostics': args.io_diagnostics,
        'timing_class': 'instrumented-diagnostic' if args.io_diagnostics else 'headline',
        'min_free_gib': args.min_free_gib,
        'fixture_identity_policy': 'Schema manifests and fixture versions must match across engines and rounds. '
                                   'Legacy binaries without fixed IDs remain diagnostic only.',
        'method': 'Sequential fresh process/database per mode/size/round, mode order rotated across '
                  'rounds and sizes. Default profile: one warmup per query. Robust profile: 1000 warmups and '
                  '100x fixed iterations only for point/equality/selective_join/aggregate_equality; '
                  'changing-parameter sequences and scans unchanged. Identical per-profile work and '
                  'checked results. Local retained-snapshot authorized reads; SQLite/PostgreSQL '
                  'have no equivalent row authorization. OS caches are not dropped. PostgreSQL '
                  'uses per-row prepared protocol calls over a local Unix socket (including IPC '
                  'and decoding), not a bulk loader. Each case has a fresh PG18 cluster; setup is outside '
                  'load timing and recorded separately. Client resources include loading, and '
                  'exclude the PostgreSQL server. Server resources are separately labeled; '
                  '250 ms process sampling adds monitoring overhead to PostgreSQL cases only. Native modes use '
                  'public point/lookup/page APIs; aggregate operations are explicit client folds, not '
                  'engine aggregate plans. Resident public scans eagerly copy rows; paged scans use '
                  '256-row pages. Native range folds scan and filter; SQL can use indexed ranges. '
                  'Untimed diagnostic reruns/storage accounting are included in whole-process '
                  'resources but excluded from per-query measurements.'})
    results = []
    expected_cases = {}
    expected_fixture = None
    for round_index in range(args.rounds):
        for size_index, rows in enumerate(args.rows):
            offset = (round_index + size_index) % len(args.modes)
            modes = args.modes[offset:] + args.modes[:offset]
            for order_index, mode in enumerate(modes):
                require_disk_space(args.output, args.min_free_gib)
                stem = args.output / (f'round-{round_index + 1}-{mode}-{rows}' +
                                      ('-io-diagnostic' if args.io_diagnostics else ''))
                server = None
                if mode == 'postgres':
                    with PgCluster(tools, stem.with_name(stem.name + '-server'), environment) as cluster:
                        pg_env = environment | {'SUBSTRATE_PAGED_PG_DSN': cluster.dsn,
                                                'SUBSTRATE_PAGED_LIBPQ': tools['libpq']}
                        raw, resource = measure_client(binary, [mode, str(rows)], stem,
                                                       args.timeout, pg_env)
                    server = cluster.resources
                    if server.get('postmaster_wait4', {}).get('returncode') != 0:
                        raise RuntimeError('PostgreSQL server did not shut down cleanly; see server artifacts')
                else:
                    raw, resource = measure_client(binary, [mode, str(rows)], stem,
                                                   args.timeout, environment)
                cases = validate_result(raw, mode, rows, args.reference_order, args.suite, args.io_diagnostics, args.hot_profile)
                fixture = fixture_identity(raw)
                if expected_fixture is not None and expected_fixture != fixture:
                    raise RuntimeError('schema fixture differs across engines or rounds')
                expected_fixture = fixture
                if rows in expected_cases and expected_cases[rows] != cases:
                    raise RuntimeError('query cases/iterations differ across backends or rounds')
                expected_cases[rows] = cases
                result = raw | {'suite': args.suite, 'reference_order': args.reference_order, 'round': round_index + 1, 'order_index': order_index,
                                'resources': resource, 'client_resources': resource}
                if server is not None:
                    result['server_resources'] = server
                results.append(result)
                save(args.output / 'results.json', results)
                save(args.output / 'summary.json', summarize(results))
                print(f'round {round_index + 1} {mode} {rows}: ' + str([
                    (q['case'], round(q['query_us'], 3)) for q in raw['queries']]), flush=True)


if __name__ == '__main__':
    main()
