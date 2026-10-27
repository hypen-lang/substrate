#!/usr/bin/env python3
"""Generate the capacity-tuning report from retained benchmark outputs."""
import json
from pathlib import Path
from analyze import analyze
ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'docs/benchmarks'
directories = sorted(BASE.glob('capacity-2026-10-25-*'))
data = {p.name: analyze(p) for p in directories}
old32 = analyze(BASE/'scale-2026-10-25-sustained32')
old128 = analyze(BASE/'scale-2026-10-25-sustained')
old = analyze(BASE/'scale-2026-10-25')
held = analyze(BASE/'scale-2026-10-25-repeat1')
lines = []
def line(s=''): lines.append(s)
def number(v, precision=0): return '—' if v is None else f'{v:,.{precision}f}'
def table(headers, rows):
    line('| '+' | '.join(headers)+' |')
    line('| '+' | '.join(['---']*len(headers))+' |')
    for row in rows: line('| '+' | '.join(map(str,row))+' |')
    line()
def write_rows(d):
    return [x for x in d['concurrency'] if x['writers'] and not x['readers']]
line('# Capacity tuning after the scale benchmark\n')
line('Measured 2026-10-25 on the same Apple M5, 10-core, 32 GiB machine as the [scale baseline](SCALE.md). The working tree is based on `76ebf34`; each run records its executable fingerprint, effective configuration, and resource limits. No schema or on-disk format migration is required.\n')
line('## What changed\n')
line('- Published data, authority generation, and failure status are captured atomically. Fresh snapshots and live authorization checks avoid the writer mutex. Immutable authority payloads are reused only for matching state/principal/credentials, with copy-on-write for transaction-specific expiry. The cache retains no row state.\n- Sparse secondary/reference postings use inline singletons or small shared sorted arrays; dense postings retain persistent trees. Existing rows and durable formats are unchanged.\n- The backend cache defaults to 64 MiB instead of redb’s implicit 1 GiB. `StorageOptions` makes this target configurable; it does not bound resident engine memory.\n- Checkpoint upserts directly replace existing values. Count-bound checkpoint groups skip redo encoding that would otherwise be discarded. Stage timings and encoded-byte counters distinguish persistence work.\n- Commit groups now allow 128 transactions and collect for up to 500 µs when current/recent concurrent demand exists. Steady isolated writers skip the collection window; the first request after a concurrent group can still wait once. Enqueue notifications only wake waiters when a batch fill threshold is crossed; completion still wakes every caller. Durable acknowledgement and individual conflict outcomes are unchanged.\n- Eligible ordered/limited correlated EXISTS queries use authorized index probes and stop on the first match. Unsupported shapes/coercions fall back to DataFusion with the remaining source budget and deadline. Checked paging charges denied candidates too; one reference-closure traversal remains non-preemptible.\n')
line('## Sixty-second durable write comparisons\n')
line('One million total records. Substrate uses its native API, SQLite uses embedded SQL, and PostgreSQL uses prepared SQL over local IPC. Final PostgreSQL runs use SERIALIZABLE isolation; earlier controls used READ COMMITTED. Strong macOS durability settings remain matched. A write increments one random indexed integer, without retries. CPU is process core-equivalents (sampled process tree for PostgreSQL); memory is peak sampled physical footprint, not exclusive allocations. PostgreSQL sums can count shared pages twice.\n')
rows=[]
for label,d in [('before32',old32),('before128',old128),('candidate32',data.get('capacity-2026-10-25-final32',{})),('candidate128',data.get('capacity-2026-10-25-final128',{})),('tuned32',data.get('capacity-2026-10-25-wake32',{})),('tuned128',data.get('capacity-2026-10-25-wake128',{}))]:
    if not d: continue
    for x in write_rows(d):
        disk=x.get('sampled_disk_write_bytes_per_second')
        rows.append([label,x['backend'],x['writers'],number(x['write_tps']),number(x['write_p50_us']/1000,2),number(x['write_p99_us']/1000,2),x['failed'],number(x.get('cpu_core_equivalents',x.get('sampled_cpu_core_equivalents')),2),number(x.get('sampled_peak_footprint_sum_bytes',0)/1e6,1),number(disk/1e6 if disk is not None else None,1),number(disk/x['write_tps']/1000 if disk is not None and x['write_tps'] else None,1)])
table(['Run','Engine','Writers','TPS','p50 ms','p99 ms','Failures','CPU cores','Footprint MB','OS write MB/s','OS kB/write'],rows)
line('OS bandwidth is attributed process I/O over interior sampling windows, not physical SSD traffic. Per-write traffic divides that sampled rate by whole-run successful TPS and is approximate. Failure policies differ: Substrate rejects optimistic conflicts; SQLite waits up to 250 ms for its writer lock; PostgreSQL uses its transaction conflict/wait behavior. Rust percentile bins have up to 2% relative error. Sixty seconds is not an hours-long soak. Guarded runs remain absent from completed-result tables.\n')
line('Candidate runs preceded the enqueue wakeup fix; tuned runs include it. The candidate at 128 writers improved throughput but regressed p99 from 21.25 to 29.17 ms, which prompted the follow-up. A smaller group cap and a 256 MiB cache did not resolve the tail problem in follow-up screenings; those runs execute 32 then 128 writers on the same database and are not isolated single-variable comparisons against the fresh single-phase controls.\n')
line('## Persistence breakdown\n')
rows=[]
for name in ['capacity-2026-10-25-wake32','capacity-2026-10-25-wake128']:
    for x in write_rows(data.get(name,{'concurrency':[]})):
        if x['backend']!='substrate':continue
        rows.append([x['writers'],number(x['writes_per_durable_group'],1),number(x['validation_apply_ns']/1e9,2),number(x['persistence_ns']/1e9,2),number(x['publication_ns']/1e9,3),number(x['storage_immediate_commit_ns']/1e9,2),x['storage_checkpoints']])
table(['Writers','Writes/durable group','Validation/apply s','Persistence s','Publication s','Immediate commit s (nested)','Checkpoints'],rows)
line('These counters cover each 60-second write phase. Persistence dominates coordinator work; atomic publication is short. Immediate commit includes backend page writes and is nested inside persistence, so these columns must not be added together.\n')
line('## Fresh snapshot reads at one million records\n')
rows=[]
for label,d in [('before',old),('after',data.get('capacity-2026-10-25-batch128',{}))]:
    for x in d.get('concurrency',[]):
        if x['backend']=='substrate' and x['rows']==1000000 and not x['writers'] and x['readers'] in [1,8,32,128]:
            rows.append([label,x['readers'],number(x['read_ops_per_second']),number(x['read_p99_us'],2),number(x.get('cpu_core_equivalents'),2)])
table(['Run','Readers','Reads/s','Read p99 µs','CPU cores'],rows)
line('These are short 2-second baseline / 3-second follow-up screenings. Each read obtains a fresh snapshot and materializes/validates its result. The change reduces contention and repeated authority construction, but shared reference counts, allocation, scheduling, and memory access still prevent linear scaling.\n')
line('## SQL: held read snapshots, one million records\n')
rows=[]
new=data.get('capacity-2026-10-25-wake32',data.get('capacity-2026-10-25-final32',{}))
controls=data.get('capacity-2026-10-25-final32',{})
for q in sorted({x['query'] for x in held['queries']}):
    row=[q]
    for d,b in [(held,'substrate'),(new,'substrate'),(controls,'sqlite'),(controls,'postgres')]:
        x=next((x for x in d.get('queries',[]) if x['rows']==1000000 and x['backend']==b and x['query']==q),None)
        row.append(number(x['p50_us'],2) if x else '—')
    rows.append(row)
table(['Query','Before Substrate µs','After Substrate µs','SQLite µs','PostgreSQL µs'],rows)
line('Seven warm samples per case are screening medians. PostgreSQL includes IPC/Python decoding; it is not an engine-only ranking. Fast-path eligibility is deliberately narrow: positive EXISTS, ascending outer identity order, literal LIMIT ≤1,000, connected inner equality joins, and supported AND predicates. NULL and unsupported coercions preserve SQL semantics through explicit handling or fallback.\n')
line('## Follow-up grouping/cache experiments\n')
rows=[]
for name in ['capacity-2026-10-25-group64','capacity-2026-10-25-cache256']:
    for x in write_rows(data.get(name,{'concurrency':[]})):
        rows.append([name.removeprefix('capacity-2026-10-25-'),x['writers'],number(x['write_tps']),number(x['write_p99_us']/1000,2),number(x['cpu_core_equivalents'],2)])
table(['Experiment','Writers','TPS','p99 ms','CPU cores'],rows)
line('Both runs precede the wakeup fix and run 32 then 128 writers sequentially on one database. They retain the same 128-commit checkpoint bound and 500 µs adaptive collection window. Group64 changes only the group cap from 128 to 64; cache256 instead changes the 64 MiB cache to 256 MiB. Neither provides evidence to replace the selected defaults; phase history and single-run variation prevent precise attribution.\n')
line('## Checkpoint budget experiments\n')
rows=[]
for name,d in data.items():
    if not any(part in name for part in ['policy','million','group100']):continue
    for x in write_rows(d):
        rows.append([name.removeprefix('capacity-2026-10-25-'),number(x['rows']),x['writers'],number(x['write_tps']),number(x['write_p99_us']/1000,2),x.get('storage_checkpoints','—'),number(x.get('sampled_disk_write_bytes_per_second',0)/1e6,1)])
table(['Experiment','Rows','Writers','TPS','p99 ms','Checkpoints','OS write MB/s'],rows)
line('The initial policy128/2048/8192 runs isolate redo-count budgets on one intermediate binary (before direct checkpoint replacement and the smaller cache). Later experiments include those changes; compare their recorded configurations/fingerprints rather than treating every row as a single-variable test. Larger budgets improved throughput but increased checkpoint pauses. **The default recovery bound therefore remains 128 transactions or 8 MiB.** The selected grouping/cache changes do not silently trade away bounded recovery. Larger explicit budgets remain available for workloads that accept the tail-latency/recovery tradeoff.\n')
line('Stage timers show where work occurs, not exhaustive causal profiles: Immediate-commit time is nested inside log/checkpoint time, which is nested in coordinator persistence time. Some cleanup is outside the inner timers. Encoding counters count encoded records, not redb pages or physical device writes.\n')
line('## Memory and larger datasets\n')
line('The isolated [posting allocation experiment](COMPACT-POSTINGS.md) reduces singleton-posting allocations by 72.5%, but dense buckets carry modest overhead. Whole-engine loader peak RSS fell from about 2.34 GB in the baseline to about 1.48 GB in the one-million-record follow-up. The cache reduction and posting changes are combined in that whole-engine comparison; it does not attribute the gain to either change alone.\n')
large=data.get('capacity-2026-10-25-tenmillion')
if large and (BASE/'capacity-2026-10-25-tenmillion/summary.json').exists():
    summary=json.loads((BASE/'capacity-2026-10-25-tenmillion/summary.json').read_text())
    for x in summary:
        line(f"- `{x.get('label')}`: {'completed' if x.get('success') else 'incomplete'}; limit: {x.get('resource_limit') or 'none'}; peak sampled footprint {number(x.get('peak_sampled_footprint_bytes',0)/1e6,1)} MB.")
    line()
    for f in (BASE/'capacity-2026-10-25-tenmillion').glob('*-load.jsonl'):
        progress=[json.loads(s) for s in f.read_text().splitlines() if s.strip()]
        progress=[x for x in progress if x.get('event')=='load_progress']
        if progress:
            last=progress[-1]
            line(f"Last confirmed progress: {number(last['loaded_rows'])} rows after {number(last['elapsed_seconds'],2)} seconds; {number(last['size']['allocated_bytes']/1e6,1)} MB allocated on disk. This is a partial load, not a completed-target claim.\n")
line('The bounded ten-million-row retry reached **3.969 million confirmed rows**, compared with **2.619 million** before tuning, then exceeded the same 5 GiB process-footprint guard. Neither run completed ten million rows.\n')
line('The mutable engine still rebuilds and retains its logical state and indexes in RAM. This patch does **not** implement mutable storage-backed serving, chunked packed row families, compact internal row handles, or safe reclamation of historical conflict keys. Immutable `DiskSnapshot` leases already exist, but do not solve that capacity requirement. Startup still rebuilds resident indexes; the full ten-million-row target and bounded-cache mutable serving must not be inferred from the one-million-row gains. Disk amplification also remains a measured concern.\n')
line('## Verification and reproduction\n')
line('The full Rust lib/integration suite passed: 470 tests, with one manual SQL performance probe ignored; crash subprocess tests passed. After selecting grouping defaults, the 16 commit tests passed again. Coverage includes crash injection around persistence/publication/checkpoints, corruption and sync failures, serializable conflicts, conservative schema extension, visibility, capability scope/expiry, live revocation, and transport. New tests cover compact posting transitions/ranges/snapshots, atomic authority capture and cache isolation, checkpoint replacement accounting, and EXISTS parity including randomized bags. All five benchmark-example tests, strict Clippy for the library and benchmark examples, and formatting checks also passed. Independent subagent reviews found no blocking issue.\n')
line('Completed query outputs also match across the retained before/after and database-control runs: [cross-run digest check](capacity-query-parity.json). Missing or resource-limited cases are not counted as matches.\n')
line('Commands and metric definitions are in the [scale harness README](../../experiments/scale/README.md). Raw runs, effective configuration, fingerprints, timing counters, and normalized CSVs:\n')
for name in data:
    links=[f'[{label}]({name}/{file})' for label,file in [('concurrency','concurrency.csv'),('SQL','queries.csv'),('loads','loads.csv'),('completion/limits','summary.json')] if (BASE/name/file).exists()]
    line(f'- [{name}]({name}/analysis.json): '+', '.join(links)+'.')
(BASE/'CAPACITY-TUNING.md').write_text('\n'.join(lines)+'\n')
