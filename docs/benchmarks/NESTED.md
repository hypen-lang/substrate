# Typed query and nested-read measurements — 2026-10-24

Apple Silicon/macOS, Rust release profile, cached dependencies. Exact complete
result trees are compared before timing; duplicate occurrences and scalar types are
preserved. These measurements establish one statement per selected relationship
branch and quantify the current implementation, not an overall scalability claim.

## Embedded execution

`cargo bench --offline --bench nested`: 10/100/500 parents, two relationship
occurrences and two workouts per parent, restricted principal with all three family
grants and reference-closure checks. One immutable snapshot; warm-up then seven
samples of five iterations. Values are median batch means in milliseconds. The
batched path includes plan validation, connection setup and typed tree assembly;
the naive path reuses its connection, queries root, then links per parent and target
per link, and assembles the identical full tree.

| Parents | Batched statements | Naive statements | Batched ms | Naive ms | Naive / batched |
|---|---:|---:|---:|---:|---:|
| 10 | 3 | 31 | 0.330 | 0.212 | 0.64× |
| 100 | 3 | 301 | 0.965 | 0.918 | 0.95× |
| 500 | 3 | 1501 | 3.597 | 4.689 | 1.30× |

Batching is slower at the smallest scale. Windows, contract validation and output
assembly have overhead; no transport round trips exist here. Three statements can
still perform many index probes. Raw output: [nested-native.txt](nested-native.txt).

## Separate-process Node client

`npm run bench:nested:processes`: release Rust host, Unix socket, generated Node
client, one warm correctness run and three timed samples per path. Medians include
request serialization, transport, server execution and decoding. Both paths produce
identical complete typed trees on a quiescent fixture. The naive path makes fresh
snapshot requests; only the batched path guarantees one snapshot under concurrency.

| Parents | Batched requests | Naive requests | Batched median ms | Naive median ms |
|---|---:|---:|---:|---:|
| 10 | 1 | 31 | 6.095 | 191.524 |
| 100 | 1 | 301 | 10.485 | 1856.786 |

The host currently polls for idle connections every **5 ms**, and each client request
opens a connection. This substantially inflates the repeated-request baseline.
The 31.4×/177.1× end-to-end ratios are **not** engine-only speedups or WAN benchmarks.
Persistent connections or event-driven accept would reduce this gap. No artificial
latency was injected. Raw ranges/settings: [nested-processes.json](nested-processes.json).

## TypeScript compilation

`npm run build:ts && node tools/bench-typescript.mjs` now exercises 501 generated
relations across 11 modules, 500 typed query consumers and ten nested query plans,
in addition to typed records. It compiles real SDK source, not stub types.

| Measurement | Result |
|---|---:|
| Generation | 294.9 ms |
| Cold TypeScript check | 1341.6 ms |
| Unchanged incremental check | 472.3 ms |
| Compiler-reported memory | 196,240 KiB |
| Generated TypeScript | 399,936 bytes |

Cold means no TypeScript build-info cache; OS caches may be warm. Compiler-reported
memory is not RSS. Editor latency and bundled SDK size are unmeasured. Existing
regression ceilings remain 15 s cold / 10 s incremental / 350,000 KiB / 2 MB generated
source. The older [DX report](DX.md) is a smaller consumer fixture and must not be
compared as a pure SDK regression. Raw [results](nested-typescript.json) and
[compiler diagnostics](nested-typescript-compiler.txt). The source-byte field for the
client measures client.mjs only, not the full SDK; the harness now reports query
builder source separately on future runs.

The separate [cross-engine suite](COMPARISON.md) covers SQLite, PostgreSQL and
Substrate from point lookups through multi-app joins and filtered nested results.
