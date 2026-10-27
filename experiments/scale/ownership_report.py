#!/usr/bin/env python3
"""Summarize the ownership/query follow-up without executing workloads."""
import json
from pathlib import Path
from analyze import analyze

BASE = Path(__file__).resolve().parents[2] / 'docs/benchmarks'
data = {name: analyze(BASE / f'ownership-2026-10-25-{name}')
        for name in ['before', 'after', 'session', 'final', 'before-repeat', 'final-session', 'restored', 'restored-session']
        if (BASE / f'ownership-2026-10-25-{name}').exists()}
hashes = {}
for run, result in data.items():
    for query in result['queries']:
        key = (query['rows'], query['query'])
        value = (query['result_rows'], query['sha256'])
        if key in hashes and hashes[key] != value:
            raise ValueError(f'query parity mismatch in {run}: {key}')
        hashes[key] = value
lines = []
def line(s=''): lines.append(s)
def table(headers, rows):
    line('| ' + ' | '.join(headers) + ' |')
    line('| ' + ' | '.join(['---'] * len(headers)) + ' |')
    for row in rows: line('| ' + ' | '.join(map(str, row)) + ' |')
    line()

line('# Ownership traffic and indexed partition reads\n')
line('Measured on 2026-10-25 on the same Apple M5/32 GiB host as the [capacity report](CAPACITY-TUNING.md). This follow-up preserves that report’s 64 MiB backend cache, 128-commit/8 MiB recovery bound, and Immediate durable acknowledgements. Each raw run records the executable fingerprint. Timed runs execute serially without concurrent builds/tests.\n')
line('## Keep shared ownership; avoid unnecessary counter traffic\n')
line('`Arc` remains the shared ownership mechanism. Replacing it throughout the engine with `Rc` would remove cross-thread safety; Rust documents their distinction in the [Arc reference](https://doc.rust-lang.org/std/sync/struct.Arc.html). The change instead borrows where an existing session already guarantees lifetime, and moves handles where an intermediate snapshot immediately goes out of scope. No custom unsafe reclamation or weakened authorization fence is introduced.\n')
line('The [baseline native sample](ownership-2026-10-25-profile-before/stacks.txt) identifies `LiveGuard::check_published` as the largest collapsed leaf category (40,280 samples). [Disassembly](ownership-2026-10-25-profile-before/guard-instructions.txt) ties the hot offsets to atomic upgrade/decrement instructions. This is hotspot evidence, not an exact CPU-percentage measurement; sampling adds overhead and includes waiting threads. Sampled TPS is not used in the comparison tables.\n')
if (BASE/'ownership-2026-10-25-profile-after/summary.json').exists():
    line('The [final session-path sample](ownership-2026-10-25-profile-after/stacks.txt) records the same 32-reader workload separately from throughput runs. Its borrowed guard has 685 collapsed leaf samples, while allocation, copying, map lookups and clock calls are prominent. Allocation and clock costs include the harness’s returned-name clone, expected-name formatting, timing and histogram work as well as engine work. This confirms the shifted hotspot, not a precise engine-only CPU breakdown; raw sample counts are not normalized CPU percentages.\n')
line('`Session::get` returns an owned row from a fresh publication. Warm unscoped scalar reads borrow the published state, cached authority, schema, and database handles, retaining entry/release authorization checks. Capability-scoped and reference-bearing reads retain the complete snapshot path; they do not receive this fast-path gain. Retained snapshots still have their original revision and attachment semantics. The existing snapshot fence and relation lookup retain their original structure; transactions move temporary snapshot handles instead of cloning them.\n')
line('```rust\nlet current_person = session.get("core.Person", person_id)?;\n// Keep a snapshot when multiple operations must share one revision.\nlet snapshot = session.snapshot();\nlet pinned_person = snapshot.get("core.Person", person_id)?;\n```\n')
line('## Million-record fresh reads\n')
line('The fixture contains five populated relations across core, fitness, and crm: one million total application records, including 111,112 Person rows. Point reads sample Person; the SQL cases also traverse relationships and application extensions. This is not a million-row single-table fixture.\n')
rows=[]
for name, label in [('before','Before: snapshot.get'),('after','Initial: snapshot.get'),('session','Initial: session.get'),('final','Inline experiment: snapshot.get'),('before-repeat','Before repeat: snapshot.get'),('final-session','Inline experiment: session.get'),('restored','Final: snapshot.get'),('restored-session','Final: session.get')]:
    for x in data.get(name,{}).get('concurrency',[]):
        if x['backend']=='substrate' and not x['writers']:
            rows.append([label,x['readers'],f"{x['read_ops_per_second']:,.0f}",f"{x['read_p99_us']:.2f}",f"{x['cpu_core_equivalents']:.2f}"])
table(['API/version','Readers','Reads/s','p99 µs','CPU cores'], rows)
if 'restored-session' in data and data['restored-session']['concurrency']:
    line('In the final 32-reader run, the new API achieves 12.14 million reads/s with 1.38 µs p99, versus the original 2.20 million and 13.17 µs. Existing snapshot callers measure 2.13 million and 10.38 µs; this does not demonstrate a throughput improvement for that API. The repeated original baseline is 2.30 million.\n')
line('Each case runs for five seconds after thread setup. These are short screenings, not sustained production limits. Every operation obtains fresh committed data, materializes and checks the returned name, and includes the harness clock/histogram overhead. The new API is opt-in: its speedup must not be attributed to unchanged snapshot callers, remote credentials, or arbitrary relational reads.\n')
line('The first attempt also changed the existing snapshot fence and schema lookup. A forced-inlining experiment did not reliably improve it and reduced session throughput in its run. Those changes were restored to their original structure; only the separate borrowed session path remains. The table retains the initial, rejected inline, repeated baseline, and final measurements. Single-reader variability is substantial; these screenings do not establish a single-reader speedup.\n')
line('## Focused ownership controls\n')
focused=[json.loads(s) for s in (BASE/'ownership-2026-10-25-read-contention.jsonl').read_text().splitlines() if s.strip()]
table(['Mode','Readers','Operations/s','CPU cores'], [[x['mode'],x['clients'],f"{x['ops_per_second']:,.0f}",f"{x['cpu_cores']:.2f}"] for x in focused if x['mode'] in ['arc-shared','arc-local','fresh','session'] and x['clients'] in [1,32]])
line('These one-second controls use 10,000 scalar rows. Shared-Arc cloning contends on one counter; the local control still uses Arc but each worker has its own allocation. They isolate sharing patterns, not interchangeable database implementations. Sampled focused latencies and raw output are in [the probe results](ownership-2026-10-25-read-contention.jsonl).\n')
line('## SQL: bounded best-related-row queries\n')
rows=[]
names=sorted({x['query'] for d in data.values() for x in d['queries']})
for name in names:
    row=[name]
    for run,backend in [('before','substrate'),('restored' if 'restored' in data else 'after','substrate'),('after','sqlite')]:
        x=next((x for x in data.get(run,{}).get('queries',[]) if x['backend']==backend and x['query']==name),None)
        row.append(f"{x['p50_us']:,.2f}" if x else '—')
    rows.append(row)
table(['Query','Before Substrate µs','After Substrate µs','SQLite µs'],rows)
line('Each SQL result is fully materialized and hashed; medians use seven warm prepared samples. SQLite retains a read transaction for comparable snapshot behavior. The new executor handles a deliberately bounded ROW_NUMBER top-one-per-root-identity shape: enumerate roots in identity order, probe each authorized partition, choose its deterministic best tuple, and stop at the output limit. Eligibility requires occurrence-ID tie breakers for multiplying joins. Unsupported shapes, coercions, or fanout use DataFusion with the remaining deadline/source budget. This is not a general replacement for window execution.\n')
line('## Checkpoint copies and the remaining storage cost\n')
line('Checkpoint encoding now borrows canonical rows, and automatic checkpoints consume accepted operations by reference. Exact byte-parity properties preserve the existing format. These remove temporary deep copies; no disk-write reduction is attributed to them.\n')
line('The [storage backend probe](STORAGE-IO.md) measures about 880 KB of backend writes for 7.3 KB of encoded checkpoint rows (120.6×). An atomic log-table replacement experiment produced identical writes and inconclusive timing, so it was rejected. These are actual backend-call bytes, distinct from OS-attributed traffic and SSD writes. Shared-reference changes cannot fix this page amplification.\n')
line('Mutable rows/indexes still reside in RAM; checkpoint recovery still reconstructs them. A compact, versioned segment/checkpoint representation and bounded-cache mutable serving remain architectural work. Owned result allocation, retained-snapshot authorization contention, capability-scoped read overhead, and broader analytical execution also remain.\n')
line('## Verification\n')
line('The integrated Rust library/integration suite passed 483 tests. Two manual performance probes were ignored by the ordinary suite; the storage probe was executed separately. Three benchmark-example tests also passed. Strict Clippy passed for the library, tests and read/scale examples, and formatting checks passed. New coverage includes fresh/retained semantics, visibility and private references, revocation during release, credential rotation/expiry, deterministic window ties and randomized SQLite/DataFusion parity, exact checkpoint byte encoding, crash boundaries, and pinned-reader lifetime. Peer subagent reviews found no blocking issue. All 36 concurrent-reader cases completed without operation errors; all 81 SQL outputs across nine query shapes matched by row count and exact hash across versions and SQLite. Completed recovery checks are retained with the runs.\n')
line('## Reproduction\n')
line('Use the [scale harness](../../experiments/scale/README.md) with `--rows 1000000 --clients 1 8 32 128 --seconds 5 --modes read --held-read`. Select `--point-api session` only for the new API case. Use a preserved executable with `--binary` for the before run. The focused probe is `cargo run --release --offline --example read_contention -- 10000 1 1,8,32`. `profile_reads.py` records a separate native sample; it is never used as an unsampled throughput result.\n')
for name in data:
    directory=f'ownership-2026-10-25-{name}'
    line(f'- [{name} normalized output]({directory}/analysis.json), [configuration]({directory}/metadata.json), [completion/recovery]({directory}/summary.json).')
(BASE/'OWNERSHIP-AND-QUERIES.md').write_text('\n'.join(lines)+'\n')
