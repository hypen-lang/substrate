#!/usr/bin/env python3
"""Build the human-readable report from normalized, retained measurements."""
from pathlib import Path
import json, statistics
from analyze import analyze
ROOT=Path(__file__).resolve().parents[2]
BASE=ROOT/'docs/benchmarks'
NAMES=['substrate','sqlite','postgres']
LABELS={'substrate':'Substrate','sqlite':'SQLite','postgres':'PostgreSQL'}
DIRS=['scale-2026-10-25','scale-2026-10-25-repeat1','scale-2026-10-25-repeat2','scale-2026-10-25-sustained','scale-2026-10-25-pg10m','scale-2026-10-25-sustained32']
data={name:analyze(BASE/name) for name in DIRS if (BASE/name).exists()}
base=data[DIRS[0]]
lines=[]
def line(s=''):lines.append(s)
def table(headers,rows):
    line('| '+' | '.join(headers)+' |');line('| '+' | '.join(['---']*len(headers))+' |')
    for row in rows:line('| '+' | '.join(map(str,row))+' |')
    line()
def num(n,d=0):return '—' if n is None else f'{n:,.{d}f}'
def concurrency(d,b,n,readers=0):return [x for x in d['concurrency'] if x['backend']==b and x['rows']==n and x['readers']==readers and x['writers']]
line('# Scale, concurrency, and resource measurements')
line('\nMeasured 2026-10-25 on an Apple M5 Mac with 10 CPU cores and 32 GiB RAM. Engine base: `76ebf34`; executable fingerprints and exact arguments are retained per run. This is a local synthetic benchmark on an interactive machine, not a production capacity certification. No engine implementation changed for this benchmark.\n')
line('## Coverage and interpretation\n')
line('Five populated application tables cross three namespaces: `core.Person`, `fitness.Workout`, `fitness.Performed`, `crm.Deal`, and `crm.CustomerDeal`. Each complete person contributes nine rows; a final person makes the requested counts exact. One million total rows means 111,112 people, not one million people. Substrate also registers an empty `TrainingGoal` definition and internal visibility metadata. SQLite flattens namespaces; PostgreSQL uses actual schemas.\n')
line('The main sweep covers 100, 1,000, 10,000, 100,000, 1,000,000, and 10,000,000 rows, with 1–128 clients. Follow-ups test 256 clients at one million rows and 60-second write runs at 32 and 128 writers. Every run uses acknowledged durable commits with matching strong macOS flush requests. Substrate enforces explicit grants and live read gates; the SQL controls do not implement that authorization layer.\n')
line('Each write transaction increments one uniformly random `Workout` integer; each read fetches one uniformly random `Person` and its name. Mixed runs add four reader clients. There are no automatic retries. At 100 total rows there are only 22 write targets, versus 222,222 at one million: the smallest case deliberately has much more hot-row contention. Substrate rejects optimistic conflicts, SQLite waits up to 250 ms for its writer lock, and PostgreSQL waits on row locks. Failure counts therefore reflect different waiting policies. Reported latency percentiles cover successful operations; Rust probes retain failed-operation latency separately. Rust histogram percentile precision is approximately 2%; PostgreSQL uses logged samples.\n')
line('Concurrency TPS measures the native Substrate API, embedded SQLite SQL, and PostgreSQL SQL over local IPC. The separate nine-query comparison measures Substrate SQL. These are deliberately labeled separately. PostgreSQL connection/IPC overhead and unsynchronized mixed client pools prevent a pure engine-only ranking.\n')
line('## Durable write throughput across dataset sizes\n')
line('Best observed successful write TPS in each **two-second screening sweep**; parentheses identify writers. This is selection of the highest short sample, not a sustained or maximum-supported claim. Failures are excluded from TPS and retained in raw results.\n')
rows=[]
for n in [100,1000,10000,100000,1000000,10000000]:
    row=[num(n)]
    for b in NAMES:
        matches=concurrency(base,b,n)
        best=max(matches,key=lambda x:x['write_tps']) if matches else None
        row.append(f"{num(best['write_tps'])} ({best['writers']})" if best else 'incomplete')
    rows.append(row)
table(['Total rows',*map(LABELS.get,NAMES)],rows)
line('Substrate’s ten-million-row load was stopped at the 5 GiB process-footprint guard; its last confirmed progress was **2,619,000 committed rows**, not ten million. PostgreSQL loaded and queried ten million rows, but the main sweep hit the summed-process-footprint guard during its 64-reader run. Summed PostgreSQL memory can count shared pages repeatedly; this guard is not proof of an intrinsic database capacity limit. SQLite completed the ten-million-row sweep and process-crash recovery.\n')
line('## One million rows: reads and mixed traffic\n')
line('Two-second screening results. Reads are native Substrate point reads versus SQL point reads in the controls. Mixed PostgreSQL reader/writer pools start independently, and its common-wall rates include connection setup.\n')
rows=[]
for b in NAMES:
    for x in base['concurrency']:
        if x['backend'] != b or x['rows'] != 1000000:continue
        if not ((x['writers']==0 and x['readers'] in [1,32,128]) or (x['readers']==4 and x['writers'] in [1,32,128])):continue
        rows.append([LABELS[b],x['readers'],x['writers'],num(x['read_ops_per_second']),num(x['write_tps']),num(x['read_p99_us'],2),num(x['write_p99_us']/1000,2) if x['write_p99_us'] is not None else '—',x['failed'],num(x['workers_without_success'])])
table(['Engine','Readers','Writers','Read ops/s','Write TPS','Read p99 µs','Write p99 ms','Failures','Workers without success'],rows)
line('## One million rows: writer saturation\n')
rows=[]
for key in DIRS[:3]:
    if key not in data:continue
    for b in NAMES:
        for x in concurrency(data[key],b,1000000):
            if x['writers'] in [1,32,128,256]:rows.append([key.removeprefix('scale-2026-10-25') or 'main',LABELS[b],x['writers'],num(x['write_tps']),num(x['write_p50_us']/1000,2),num(x['write_p99_us']/1000,2),x['failed'],num(x['workers_without_success'])])
table(['Run','Engine','Writers','Successful TPS','p50 ms','p99 ms','Failed attempts','Workers without success'],rows)
line('Both Substrate follow-ups completed at 256 writers with progress from every writer. Throughput fell 7–10% from 128 to 256, and p99 approximately doubled. Thus **128 is the best tested throughput point for this fixture**, not a maximum supported client count. PostgreSQL’s 256-client attempts exceeded the process-tree memory budget, so there is no completed 256-client PostgreSQL throughput result. SQLite can accept many submitting clients, but its physical writer remains serialized and busy-timeout failures/starvation appear under contention.\n')
line('Substrate likewise retains one active physical redb writer. Its throughput gain comes from grouping independent commits, not parallel redb write execution. The default admission queue allows 1,024 waiting transactions subject to a byte cap; that is configuration, not a measured productive concurrency level.\n')
if any(k in data for k in [DIRS[3],DIRS[5]]):
    line('## Sixty-second write checks at one million rows\n')
    rows=[]
    for key in [DIRS[3],DIRS[5]]:
        if key not in data:continue
        for b in NAMES:
            for x in concurrency(data[key],b,1000000):
                rows.append([LABELS[b],x['writers'],num(x['write_tps']),num(x['write_p50_us']/1000,2),num(x['write_p99_us']/1000,2),x['failed'],num(x.get('cpu_core_equivalents',x.get('sampled_cpu_core_equivalents')),2),num(x.get('sampled_peak_footprint_sum_bytes',0)/1e6,1),num(x.get('sampled_disk_write_bytes_per_second',0)/1e6,1),num(x.get('logical_app_bytes_per_second',x.get('logical_app_bytes_per_second_estimate',0))/1e6,3)])
    table(['Engine','Writers','Successful TPS','p50 ms','p99 ms','Failed attempts','CPU cores','Peak footprint MB','OS write MB/s','Logical MB/s'],rows)
    line('PostgreSQL’s 128-client sixty-second attempt hit the summed-process memory guard before completion; it has no completed result in that configuration. The 32-writer run is the matched lower-concurrency control.\n')
    line('CPU is exact process CPU/wall time for the Rust probes and sampled process-tree CPU for PostgreSQL. A value of 2.0 means two fully busy cores, not 2%. Memory is sampled process physical footprint; PostgreSQL’s sum is not exclusive memory. OS write bandwidth uses the interior sampling window and is **not physical SSD-controller bandwidth**. Logical writes count a 16-byte ID plus an 8-byte value. Neither measure is remote-network throughput. Sixty seconds is a useful check beyond short bursts, not an hours-long steady-state soak.\n')
if DIRS[4] in data:
    line('## Bounded PostgreSQL follow-up at ten million rows\n')
    rows=[]
    for x in data[DIRS[4]]['concurrency']:
        if x['writers']:rows.append([x['writers'],x['readers'],num(x['write_tps']),num(x['write_p99_us']/1000,2),num(x['read_ops_per_second']),x['failed']])
    table(['Writers','Readers','Write TPS','Write p99 ms','Read ops/s','Failures'],rows)
    line('This follow-up uses fewer clients to complete useful work within the same resource budget. Its throughput is not a maximum across the original 1–128-client sweep.\n')
    for x in data[DIRS[4]]['recovery']:
        if x.get('phase')=='crash_restart' and x.get('committed_sentinel_preserved'):
            line(f"PostgreSQL also completed process-crash recovery at ten million rows in **{num(x['elapsed_ms'],1)} ms**, including server startup/polling and a verified point read. The acknowledged sentinel survived.\n")
line('## Load rate and disk size\n')
rows=[]
for x in sorted(base['loads'],key=lambda x:(x['rows'],NAMES.index(x['backend']))):
    rows.append([num(x['rows']),LABELS[x['backend']],num(x['ingest_seconds'],3),num(x['rows_per_second']),num(x['maintenance_seconds'],3),num(x['file_bytes']/1e6,2),num(x['allocated_bytes']/1e6,2) if x['allocated_bytes'] is not None else '—'])
table(['Rows','Engine','Load seconds','Rows/sec','Final maintenance seconds','Apparent DB MB','Allocated DB MB'],rows)
line('Substrate and SQLite construct and insert records in Rust. PostgreSQL uses server-side `generate_series`; its load rate is **not** a matched client-ingest comparison. All retain active indexes/foreign keys and batch up to 1,000 people (9,000 rows) per durable transaction. Final maintenance is a Substrate checkpoint, SQLite ANALYZE/checkpoint, or PostgreSQL ANALYZE. PostgreSQL database bytes exclude the cluster’s WAL; compare the next table before treating these as whole-storage totals.\n')
rows=[]
for b in NAMES:
    x=next(x for x in base['loads'] if x['backend']==b and x['rows']==1000000)
    rows.append([LABELS[b],num(x['file_bytes']/1e6,2),num(x['allocated_bytes']/1e6,2) if x['allocated_bytes'] else '—',num(x['table_and_index_bytes']/1e6,2) if x['table_and_index_bytes'] else '—',num(x['cluster_bytes']/1e6,2) if x['cluster_bytes'] else '—'])
table(['One million rows','DB apparent MB','DB allocated MB','PG tables + indexes MB','PG whole cluster MB'],rows)
line('The fixture contains **35.11 MB of raw IDs and field values** at one million rows. Substrate occupies **345.8 allocated bytes/row** (676.3 apparent), SQLite **104.5 bytes/row**, and PostgreSQL application tables/indexes **161.9 bytes/row** (169.9 including database catalog; about 521.7 including this cluster’s WAL/other files). These are narrow synthetic records, not a universal bytes-per-record figure.\n')
line('Substrate’s one-million-row loader sampled about 2.34 GB peak RSS including teardown, and the query/concurrency process reached roughly 2.9 GB. The latter includes recovery, SQL conversion/cache allocations, and allocator retention; it is not a measurement of exclusive canonical-row memory. Checkpointed storage bounds redo history, but the mutable working set and indexes still reside in RAM.\n')
line('## SQL at one million rows\n')
line('Median warm times in microseconds. The main run keeps Substrate’s snapshot but uses autocommit SQLite/PostgreSQL statements. The follow-up holds read transactions in all engines to remove that mismatch. PostgreSQL still includes local IPC and Python result decoding; Rust includes canonical result construction. Seven samples per case are screening medians, not robust SQL p99 estimates.\n')
rows=[]
held=data.get(DIRS[1],{})
for q in sorted({x['query'] for x in base['queries']}):
    row=[q]
    for d in [base,held]:
        for b in NAMES:
            x=next((x for x in d.get('queries',[]) if x['rows']==1000000 and x['backend']==b and x['query']==q),None)
            row.append(num(x['p50_us'],2) if x else '—')
    rows.append(row)
table(['Query','Substrate main','SQLite main','PG main','Substrate held','SQLite held','PG held'],rows)
line('At this larger size, the earlier broad analytical advantage is no longer universal: correlated EXISTS remains a pronounced Substrate weakness. PostgreSQL is faster on the nested window; SQLite and Substrate are similar in the held-snapshot follow-up, with their order varying between runs. Point/indexed operations remain fast. Packed persistence does not eliminate expanded in-memory index/state costs or row-to-column conversion costs.\n')
line('## Recovery and validation\n')
rows=[]
for x in base['recovery']:
    if x.get('rows')!=1000000:continue
    if x.get('event')=='open' and x.get('mode') in ['run','recover']:
        rows.append([LABELS[x['backend']],x['mode'],num(x['resource']['wall_seconds']*1000,2)])
    elif x.get('phase') in ['clean_restart','crash_restart']:
        rows.append(['PostgreSQL',x['phase'],num(x['elapsed_ms'],2)])
table(['Engine','Operation','Milliseconds to usable point read'],rows)
line('OS caches are warm. PostgreSQL timings include server launch/polling and connection setup; SQLite opens lazily, so the first point read is included. Substrate rebuilds its complete resident state/indexes. Abrupt process termination preserves the acknowledged sentinel in completed recovery runs. This is not power-loss simulation.\n')
line('The harness unit tests cover large-ID separation, exact fixture counts, and histogram quantile bounds. Smoke tests cover each backend, identical nine-query digests, full committed-increment accounting, and process-crash recovery. The main sweep checks 54 distinct SQL query/scale pairs with matching outputs wherever at least two backends completed (three engines through one million rows, SQLite/PostgreSQL at ten million). Resource-limited stages remain explicitly incomplete. Strict example Clippy and formatting checks pass.\n')
line('## Reproduction and raw measurements\n')
line('See [harness method and commands](../../experiments/scale/README.md). Each directory contains raw per-phase measurements, resource samples, fingerprints, errors, parity coverage, and normalized CSV/JSON:\n')
for key in data:line(f'- [{key}]({key}/): [concurrency CSV]({key}/concurrency.csv), [SQL CSV]({key}/queries.csv), [load CSV]({key}/loads.csv), [normalized JSON]({key}/analysis.json).')
line('\nThe next measured priorities are resident-memory/recovery allocation overhead, durable write amplification, and the EXISTS/window query paths. Remote transport bandwidth, larger-than-RAM mutable serving, multi-machine load, and long-duration steady-state behavior are not established by these runs.')
(BASE/'SCALE.md').write_text('\n'.join(lines)+'\n')
