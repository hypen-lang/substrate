# Typed JSON parser experiment

Keep `serde_json` in production. On this ARM machine, `simd-json` improves parsing of the tested structured batches and schema metadata, but its default convenience API is slower for small reads and a large plain-text value. More importantly, the pinned candidate accepts malformed Unicode that the current parser rejects. No production parser or durable format was changed.

The isolated harness is in [experiments/json](../../experiments/json/), with its own manifest and lockfile. It compares `serde_json 1.0.151` against `simd-json 0.18.1` using Substrate's actual public `Request`, `Command`, `Mutation`, and `Module` types. This measures typed deserialization, including owned output allocation and destruction, rather than a generic JSON DOM. Ordinary durable row records are binary and are outside this experiment.

## Correctness blocks adoption

The nine automated tests include explicit incompatibility checks, not just successful equivalence cases:

- For an actual query request containing an unmatched high-surrogate escape (`\uD800` or `\uDBFF`), `serde_json` rejects the input. `simd-json 0.18.1` accepts it and produces a NUL byte (`0x00`) in the decoded SQL string. The test inspects bytes rather than printing the malformed text. Valid surrogate pairs agree, and an unmatched low surrogate is rejected by both. This changes accepted inputs and their meaning; it is an adoption blocker. It is not evidence of an authorization exploit.
- A request with 160 nested `UsingCapability` envelopes is rejected by the current parser's depth limit but accepted by the candidate's convenience API. Explicit `Buffers::with_max_depth(..., 128)` rejects this example, but does not fix the Unicode discrepancy. Exact depth-boundary equivalence still needs broader testing. Gateway validation is still required after parsing.
- Unknown struct fields, duplicate struct fields, truncation, trailing content, invalid raw UTF-8, and incorrect field types are rejected equally in the tested cases. Duplicate named-map keys retain the existing last-value-wins behavior in both; the experiment does not strengthen or weaken that existing policy.
- Valid fixtures deserialize to equal typed values. Exact integer strings, signed 64-bit boundaries, and out-of-range integer strings preserve the same Gateway validation behavior. Numeric JSON in place of the protocol's integer string is rejected by both.
- An unchanged external 1 MiB size gate rejects oversized input before either parser. Semantic schema validation remains `SchemaManifest::validate()` after typed decoding; valid and invalid integer-range constraints produce matching outcomes. `SchemaManifest` has no public `Deserialize` implementation, so metadata timing uses its actual `Module` representation rather than introducing a substitute manifest parser.

These cases are executable in [the harness tests](../../experiments/json/src/main.rs). There is no normalization shim hiding differences and no production validation bypass. Adoption would require fixing the candidate behavior, extending differential/fuzz coverage, preserving depth and framing limits, and then repeating measurements.

## Measurements

Measured 2026-10-25, macOS `aarch64`, Rust 1.94.0, normal release profile, system allocator, default crate features. The candidate documents ARM NEON support and Serde integration in its [versioned documentation](https://docs.rs/simd-json/0.18.1/simd_json/). No allocator change or `target-cpu=native` flag was applied. Compilation and the other agents' benchmark processes were paused for these three runs. Background OS activity was not controlled.

Numbers below are microseconds per typed parse, the median of three independent process medians. Each process runs nine rounds per mode, rotating mode order, with warmup. Each round parses approximately 16 MiB, bounded to 16–10,000 iterations. The raw values are **round means**, not individual-operation latency percentiles.

| Request / metadata | Bytes | serde, immutable input | serde, copy included | simd, copy included | simd with reused scratch, copy included |
|---|---:|---:|---:|---:|---:|
| Small SQL read | 198 | 0.257 | 0.279 | 0.415 | 0.228 |
| One named insert, Unicode and escapes | 287 | 0.931 | 0.959 | 0.935 | 0.756 |
| Atomic batch, 100 named inserts | 19,898 | 58.831 | 59.826 | 50.010 | 49.398 |
| Schema proposal, 100 relation families | 22,401 | 80.182 | 81.259 | 68.720 | 67.187 |
| One insert, 900 KiB plain-text value | 921,864 | 79.169 | 91.188 | 142.891 | 145.012 |
| Module metadata, 100 relation families | 22,245 | 44.301 | 45.131 | 37.575 | 36.323 |

Compared with the current immutable-input baseline, copying plus the simd convenience parser takes about 15% less time for the 100-write batch and 14% less for the schema proposal. It takes 61% more time for the small read and 81% more for the near-limit plain-text case. These are workload-specific parsing results, not database throughput improvements.

`simd-json` requires mutable input and mutates escaped strings. Copy-inclusive modes allocate and copy fresh bytes within the timed region, then parse and destroy the result. Reused scratch modes retain and warm `simd_json::Buffers` outside timing, with maximum depth 128; they still include the input copy. This can represent a long-lived worker, but incurs retained memory and requires integration work. The Unicode correctness blocker persists with reused scratch.

Separately, these modes model a receiver that already owns a fresh input buffer. Filling those input buffers is excluded for both parsers; every parse gets untouched bytes, so the experiment never repeatedly parses an already-mutated escaped string:

| Request / metadata | serde, ready input | simd, ready input | simd, ready input and reused scratch |
|---|---:|---:|---:|
| Small SQL read | 0.257 | 0.391 | 0.211 |
| One named insert | 0.939 | 0.929 | 0.748 |
| Atomic batch, 100 inserts | 59.514 | 50.094 | 49.695 |
| Schema proposal, 100 families | 80.104 | 68.031 | 66.643 |
| 900 KiB plain-text value | 79.042 | 134.313 | 130.817 |
| Module metadata, 100 families | 44.869 | 37.445 | 36.143 |

The different copy/ready cache conditions matter. The raw results should be used to select the mode matching an eventual transport implementation, rather than quoting only the fastest column.

## Contribution to a real operation

The harness also parses the small read and sends it through the actual authenticated `Gateway` against an in-memory database, validating that one row is returned. This includes credential lookup, dispatch, SQL setup/execution, and typed response allocation/destruction. It excludes sockets, TLS, response JSON serialization, and durable writes.

| Parse + Gateway mode | Microseconds per operation |
|---|---:|
| serde, immutable input | 24.254 |
| simd, copy included | 24.614 |
| simd, ready input | 24.750 |

There is no demonstrated operation-level improvement in this probe. Standalone serde parsing is about 1.1% of the measured serde parse-plus-Gateway time. Treat that ratio as a bounded workload estimate, not a component profile: even eliminating parsing entirely would save only about a quarter microsecond under those measurement assumptions. Reused scratch reduces the isolated small-read parse by about 0.03–0.05 microseconds, but was not separately measured through the Gateway. A large batch can save several microseconds of parsing; its authorization, transaction, and durability costs still need a separate end-to-end measurement before claiming a transport or commit speedup.

Raw samples: [run 1](json-parser-run-1.json), [run 2](json-parser-run-2.json), [run 3](json-parser-run-3.json). They contain input sizes, all mode medians, and individual round means. The small-read process medians were 0.254–0.259 microseconds for serde and 0.410–0.418 for simd with copying; the parse-plus-Gateway serde medians were 24.116–24.930 microseconds. This is three local process repetitions, not a confidence interval or a cross-machine result.

## Reproduction and verification

From the repository root:

```sh
cargo test --locked --manifest-path experiments/json/Cargo.toml
cargo clippy --locked --manifest-path experiments/json/Cargo.toml --all-targets -- -D warnings
cargo build --release --locked --manifest-path experiments/json/Cargo.toml
experiments/json/target/release/substrate-json-experiment > /tmp/json-parser-results.json
```

Nine tests pass, including checks that preserve and expose the two incompatibilities. Clippy passes with warnings denied. The benchmark checks typed-value equivalence for every measured valid fixture before timing. Its independent workspace and pinned dependencies leave the engine's dependency graph and default parser unchanged.
