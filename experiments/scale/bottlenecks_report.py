#!/usr/bin/env python3
"""Render measured storage/memory/read follow-up; never execute workloads."""
import json
from pathlib import Path
from analyze import analyze

BASE = Path(__file__).resolve().parents[2] / 'docs/benchmarks'
RUNS = {'before': 'before-repeat', 'after': 'after-final', 'controls': 'controls',
        'sustained-before': 'sustained-before', 'sustained-after': 'sustained-after'}
data = {name: analyze(BASE / f'bottlenecks-2026-10-25-{directory}')
        for name, directory in RUNS.items()
        if (BASE / f'bottlenecks-2026-10-25-{directory}' / 'summary.json').exists()}
lines = []


def line(text=''):
    lines.append(text)


def number(value, digits=0):
    return '—' if value is None else f'{value:,.{digits}f}'


def milliseconds(value):
    return None if value is None else value / 1000


def table(headers, rows):
    line('| ' + ' | '.join(headers) + ' |')
    line('| ' + ' | '.join(['---'] * len(headers)) + ' |')
    for row in rows:
        line('| ' + ' | '.join(map(str, row)) + ' |')
    line()


def label(run, backend):
    return run if backend == 'substrate' else backend


line('# Bounded checkpoints, compact conflict versions, and scoped reads\n')
line('Measured on 2026-10-25 on the same Apple M5 / 10-core / 32 GiB host as the [previous report](OWNERSHIP-AND-QUERIES.md). Timed workloads run serially without builds or tests. Raw metadata identifies each executable, configuration and resource guard.\n')
line('## Changes and guarantees\n')
line('- V3 checkpoints retain a bounded materialized overlay of the latest changed rows and tombstones, flushing sorted subsets into the base before exceeding 4,096 keys or 8 MiB. Metadata, rows, compaction, checkpoint manifest, durable head and redo pruning commit atomically. Immediate durability and the default 128-commit / 8 MiB redo bound remain. V1/V2 files remain readable; the next checkpoint writes V3, which older binaries reject.\n- Conflict timestamps use typed maps grouped by relation/role, removing repeated IDs and large enum-key padding. All historical conflict keys remain: this does not reclaim tombstones or weaken serializability.\n- Fresh scalar `Session::get` can reuse compiled capability scope with live entry/release checks. `Session::get_shared` returns an authorized immutable `Arc<Row>` without copying its payload. Reference-bearing rows keep the complete visibility-closure path. Clock reads avoid a shared atomic write when time has not advanced. Compiled grants use persistent maps grouped by principal and relation, so permission checks borrow names without allocating temporary keys. Ordinary scalar reads reuse one grant lookup for relation and row permission checks.\n')
line('## Checkpoint I/O experiment\n')
line('The final storage-only probe applies 32,768 deterministic random updates to 100,000 integer rows, with identical Immediate durability, a 64 MiB backend cache, and the unchanged 128-commit checkpoint bound. The V2 writer is a test-only control in the same executable. Measurements include every partial merge and the final checkpoint, not just buffer filling.\n')
storage_rows = []
for name, filename in [('V2 direct', '2026-10-25-storage-overlay-v2-final-control.jsonl'),
                       ('V3 partial', '2026-10-25-storage-overlay-v3-partial128.jsonl')]:
    path = BASE / filename
    if not path.exists():
        continue
    records = [json.loads(row) for row in path.read_text().splitlines() if row.strip()]
    for group in [32, 128]:
        phases = [x for x in records if x['transactions_per_group'] == group and 'phase' in x]
        transactions = sum(x['transactions'] for x in phases)
        seconds = sum(x['seconds'] for x in phases)
        checkpoint_max = max(x['max_seconds'] for x in phases if x['phase'] in ['checkpoint', 'compaction'])
        storage_rows.append([name, group, number(sum(x['backend_write_bytes'] for x in phases) / 1e6, 2),
                             number(transactions / seconds), number(checkpoint_max * 1000, 2),
                             sum(x['compactions'] for x in phases)])
table(['Format', 'Transactions/group', 'Backend writes MB', 'Storage-only TPS', 'Worst checkpoint ms', 'Merges'], storage_rows)
line('Partial compaction flushes at least 128 sorted non-pending keys, or enough to fit both bounds, then removes the selected keys after dropping the iterator; untouched remainder pages stay in place. Full merges were rejected: per-key clearing amplified writes, while whole-table reset still produced long pauses. The [I/O report](STORAGE-IO.md) retains these failed experiments and per-phase counters. Maxima are single-run observations, not p99 estimates. Backend-call bytes are not device traffic; storage-only TPS excludes engine validation, logical indexes and publication. Recovery verifies all 32,768 increments.\n')
line('## Whole-engine load and recovery\n')
line('The one-million-record fixture has five populated relations across core, fitness and crm, including 111,112 Person records. Rows and indexes remain resident in memory. File length and allocated disk space differ because sparse/preallocated regions are possible. PostgreSQL reports database size; its separate cluster/WAL footprint is in the raw output. MB below means decimal megabytes; peak RSS is process lifetime, not retained heap.\n')
rows = []
for run, result in data.items():
    for x in result['loads']:
        rows.append([label(run, x['backend']), number(x['rows']),
                     number(x['ingest_seconds'], 2),
                     number((x['file_bytes'] or 0) / 1e6, 1),
                     number(x['allocated_bytes'] / 1e6, 1) if x['allocated_bytes'] else '—',
                     number(x['load_peak_rss_bytes'] / 1e6, 1) if x['load_peak_rss_bytes'] else '—'])
table(['Run/backend', 'Records', 'Load s', 'File/database MB', 'Allocated MB', 'Peak RSS MB'], rows)
rows = []
for run, result in data.items():
    for x in result['recovery']:
        if x.get('event') == 'open' and x.get('mode') == 'recover':
            rows.append([label(run, x['backend']), number(x['rows']),
                         number(x['resource']['wall_seconds'], 3),
                         number(x['resource']['process_lifetime_peak_rss_bytes'] / 1e6, 1)])
table(['Run/backend', 'Records', 'Open after crash s', 'Recovery peak RSS MB'], rows)
line('Recovery above includes rebuilding the full resident state and indexes. Bounded redo and a bounded overlay do not make startup independent of database size. Raw recovery output also verifies the committed sentinel and aggregate state.\n')
line('## Durable writes and concurrent reads\n')
line('The ordinary before/after cases last five seconds each; any sustained runs are separately labeled and record their durations. These are local API/IPC workload measurements, not production concurrency maxima. PostgreSQL controls, where present, use the recorded SERIALIZABLE setting; SQL control users can access the whole fixture; Substrate additionally checks its explicit grants and reference policies. PostgreSQL includes local IPC, whereas Substrate uses its native in-process API, so these are not equal protocol-cost comparisons. OS-attributed write bytes are not SSD traffic.\n')
rows = []
for run, result in data.items():
    for x in result['concurrency']:
        if x['writers']:
            rows.append([label(run, x['backend']), x['writers'], x['readers'],
                         number(x['write_tps']), number(milliseconds(x['write_p50_us']), 2),
                         number(milliseconds(x['write_p99_us']), 2), number(x['read_ops_per_second']),
                         number(x['failed']), number(x.get('cpu_core_equivalents', x.get('sampled_cpu_core_equivalents')), 2)])
table(['Run/backend', 'Writers', 'Readers', 'TPS', 'Write p50 ms', 'Write p99 ms', 'Reads/s', 'Failed attempts', 'CPU cores'], rows)
rows = []
for run, result in data.items():
    for x in result['concurrency']:
        if not x['writers']:
            rows.append([label(run, x['backend']), x['readers'], number(x['read_ops_per_second']),
                         number(x['read_p99_us'], 2), number(x.get('cpu_core_equivalents', x.get('sampled_cpu_core_equivalents')), 2)])
table(['Run/backend', 'Readers', 'Reads/s', 'p99 µs', 'CPU cores'], rows)
line('Native Substrate reads here use owned `Session::get`; the shared-row/scoped probes have separate boundaries. Full normalized CSVs include footprint/RSS, logical bandwidth, OS disk bandwidth, per-stage persistence timing, compaction counters and workers without successes.\n')
read_repeats = []
for name in ['before', 'final']:
    path = BASE / f'bottlenecks-read128-{name}'
    if (path / 'summary.json').exists():
        result = analyze(path)
        x = next(x for x in result['concurrency'] if x['readers'] == 128 and not x['writers'])
        read_repeats.append([name, number(x['read_ops_per_second']),
                             number(x['read_p99_us'], 2), number(x['cpu_core_equivalents'], 2)])
if read_repeats:
    line('The five-second matrix still showed a weaker 128-reader result, so a separate matched ten-second read-only comparison was run with the final binary first and the preserved baseline second. It recovered the gap; both observations are retained rather than replacing the weaker result. This shows workload/host sensitivity and does not establish a production concurrency ceiling. [Final repeat](bottlenecks-read128-final/metadata.json), [baseline repeat](bottlenecks-read128-before/metadata.json).\n')
    table(['Ten-second repeat', 'Reads/s', 'p99 µs', 'CPU cores'], read_repeats)
line('### Resource view at 32 writers, no readers\n')
rows = []
for run, result in data.items():
    for x in result['concurrency']:
        if x['writers'] != 32 or x['readers']:
            continue
        def scaled(key, divisor):
            value = x.get(key)
            return number(value / divisor, 2) if value is not None else '—'
        rows.append([label(run, x['backend']), scaled('sampled_peak_footprint_sum_bytes', 1e6),
                     scaled('sampled_peak_rss_sum_bytes', 1e6), scaled('logical_app_bytes_per_second', 1e3),
                     scaled('sampled_disk_write_bytes_per_second', 1e6),
                     number(x.get('writes_per_durable_group'), 1)])
table(['Run/backend', 'Peak footprint MB', 'Peak RSS MB', 'Logical KB/s', 'OS write MB/s', 'Writes/durable group'], rows)
line('These phase-sampled resource figures can miss short peaks. PostgreSQL process sums can count shared pages more than once; OS write attribution can include checkpoint/background work and is not the controlled backend-call experiment above. Logical bytes count application IDs and changed values, excluding indexes, log and protocol.\n')
line('## SQL parity and regression check\n')
hashes = {}
for run, result in data.items():
    for x in result['queries']:
        key = (x['rows'], x['query'])
        value = (x['result_rows'], x['sha256'])
        if key in hashes and hashes[key] != value:
            raise ValueError(f'SQL parity mismatch in {run}: {key}')
        hashes[key] = value
names = sorted({x['query'] for d in data.values() for x in d['queries']})
rows = []
for query in names:
    row = [query]
    for run, backend in [('before', 'substrate'), ('after', 'substrate'), ('controls', 'sqlite'), ('controls', 'postgres')]:
        x = next((x for x in data.get(run, {}).get('queries', [])
                  if x['backend'] == backend and x['query'] == query), None)
        row.append(number(x['p50_us'], 2) if x else '—')
    rows.append(row)
table(['Query', 'Before µs', 'After µs', 'SQLite µs', 'PostgreSQL µs'], rows)
line('Results are fully materialized and checked by row count and exact hash. SQL medians are seven warm prepared samples; SQLite/PostgreSQL controls hold read transactions. This milestone does not add broader SQL plan eligibility.\n')
line('## Isolated conflict-version memory\n')
line('The [allocator-accounted comparison](version-memory-2026-10-25/README.md) runs identical keys and revisions through the legacy flat map and the typed grouped representation. At one million five-family records, requested version-map heap falls from 595.23 to 247.27 MB (58.5%); the unique-text fixture falls from 730.99 to 353.00 MB (51.7%). These are version-map allocations, not whole-engine RSS.\n')
line('The five-family lookup pass including identical key generation improves from 442 to 305 ms. The cost is more root handles: clone/drop changes from 3.4 to 13.9 ns and inline map storage from 24 to 208 bytes. Exact output hashes, retained-snapshot reads, and complete allocation reclamation are checked. These single runs motivate the whole-engine comparison above rather than replacing it.\n')
line('A matched flat-versus-compact version-map control retained all other engine changes: 32 readers delivered 11.47M versus 11.44M reads/s while the compact form used 25–28% less process memory. This isolated the earlier concurrent-read regression from the version-map change. The final grant lookup change is measured separately in the whole-engine tables. [Flat control](bottlenecks-flat-version-control/metadata.json), [compact control](bottlenecks-compact-version-control/metadata.json).\n')
line('## Scoped reads and avoiding payload copies\n')
def read_rate(size, mode):
    records = [json.loads(x) for x in (BASE / f'bottlenecks-2026-10-25-read-{size}.jsonl').read_text().splitlines() if x.strip()]
    result = next(x for x in records if x['clients'] == 32 and x['mode'] == mode)
    return number(result['ops_per_second'] / 1e6, 2)

line(f'In the [small-row probe](bottlenecks-2026-10-25-read-small.jsonl), 32 local credential-backed readers achieve {read_rate("small", "fresh-scoped")}M reads/s using fresh snapshots and {read_rate("small", "session-scoped")}M using the new scoped session path. Both paths use the same final clock implementation; the comparison isolates their different read paths. A separate old-RMW/new-load clock control measures {read_rate("small", "clock-rmw")}M versus {read_rate("small", "clock-read")}M observations/s at 32 threads, not full authentication throughput.\n')
line(f'For [64 KiB scalar payloads](bottlenecks-2026-10-25-read-large.jsonl), 32 scoped readers return {read_rate("large", "session-scoped")}M owned rows/s versus {read_rate("large", "shared-scoped")}M shared handles/s. This measures avoiding a payload copy, not reading 64 KiB per operation or transport bandwidth. Small scoped rows return {read_rate("small", "session-scoped")}M owned rows versus {read_rate("small", "shared-scoped")}M shared handles/s; shared ownership is not universally faster. Measurements are one-second local API screenings with sampled latency; [metadata and commands](bottlenecks-2026-10-25-read-metadata.json) record the exact setup.\n')
line('## Validation\n')
line('[Validation commands and totals](bottlenecks-2026-10-25-validation.json) record the final integrated checks.\n')
line('The integrated Rust library/integration suite passes 505 tests; two manual performance probes are excluded from normal runs, and the storage probe was invoked separately. Five example tests pass, including the compact-map reference properties. The shared-read example executes successfully. Strict Clippy covers the library, tests and four touched benchmark/example targets; formatting and diff checks pass. Independent subagent reviews found no remaining blockers in the compact version/grant maps, scoped/shared reads, or partial compaction.\n')
line('Coverage includes grant-map equivalence and ordered serialization, retained-snapshot conflict timestamps, scope/credential switching, hidden references, expiry and revocation during result mapping, exact byte/key caps, oversized rows, mixed pending deletes/reinserts, pinned backend readers, V2-to-V3 upgrade, corruption rejection, and crashes around both full and partial compaction. Process-crash tests and backend fault injection do not emulate a physical power failure.\n')
line('## Remaining limits\n')
line('Mutable rows/indexes still live in RAM. A bounded cache for mutable disk-backed serving, compact row handles, and safe historical conflict-key reclamation remain separate architectural work. Compaction is synchronous within its durable transaction and can increase tail latency; throughput gains must be read alongside p99/max pauses. Shared rows retain their payload until callers release them, and still incur per-row reference-count updates. Capability/reference traversal beyond the scalar fast path and general analytical execution remain.\n')
line('## Rejected intermediate whole-engine result\n')
line('The first combined candidate used a generic ordinary-read path and 512-key compaction with remainder rebuilding. At one million records it reduced load peak RSS from 1,473 to 1,060 MB, but 32-reader throughput fell from 14.21M to 10.01M reads/s and 32-writer p99 rose from 14.02 to 26.42 ms. A fresh baseline repeat confirmed the concurrent-read gap (13.96M reads/s). The initial load-time difference was largely host variation: baseline ingestion also increased from 6.12 to 8.05 seconds on repetition. These results are preserved in the [original baseline](bottlenecks-2026-10-25-before/metadata.json), [first candidate](bottlenecks-2026-10-25-after/metadata.json), and [repeated baseline](bottlenecks-2026-10-25-before-repeat/metadata.json); the main tables use the repeated baseline and final candidate. A second [split-path candidate](bottlenecks-2026-10-25-after-split/metadata.json) fixed checkpoint tails but still lost concurrent read throughput because compiler inlining recombined the two paths. Preventing inlining restored the small stack frame but still left a concurrent-read gap, retained in the [codegen candidate](bottlenecks-2026-10-25-after-codegen/metadata.json). Matched [before](bottlenecks-profile-before/stacks.txt) and [after](bottlenecks-profile-after/stacks.txt) profiles then showed allocation and map lookups dominating, with ArcSwap below 1% of sampled leaves. The final candidate groups compiled grants by principal and relation, removing temporary principal-string allocations, and reuses one lookup for relation and row checks. Both authorization fences remain.\n')
line('## Reproduction and raw evidence\n')
line('Use the [scale harness](../../experiments/scale/README.md) with one million rows, clients 1/32/128, five seconds, read/write/mixed modes, held SQL reads and `--point-api session`. Use `--binary` for the preserved baseline. Fixture creation, measurements and crash/recovery checks use disposable directories.\n')
for run in data:
    directory = f'bottlenecks-2026-10-25-{RUNS[run]}'
    line(f'- [{run}: normalized output]({directory}/analysis.json), [configuration]({directory}/metadata.json), [completion/limits]({directory}/summary.json).')
(BASE / 'BOTTLENECKS.md').write_text('\n'.join(lines) + '\n')
