# Incremental checkpoint measurements — 2026-10-25

The probe inserts one short `Person` row per Immediate-durability commit and manually checkpoints every 128 inserts. It tests 512, 1,024 and 2,048 rows, with three samples at each size. Every sample closes/reopens its database and checks the final row count, indexes and zero replayed commits. There are no timing assertions. Reproduce the new path with:

```sh
cargo run --offline --release --example checkpoint_scaling
```

The baseline ran before the incremental implementation; its checkpoint row count equals rows written because that implementation cleared and rewrote the complete table each time. The new probe reports the implementation's `written_rows` counter. These are logical checkpoint entry updates, not measured physical device/page writes.

| Final rows | Full-rewrite row entries | Incremental row entries | Reduction | Baseline checkpoint time, median | Incremental repeat, median |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 512 | 1,280 | 512 | 2.5× fewer | 25.066 ms | 21.243 ms |
| 1,024 | 4,608 | 1,024 | 4.5× fewer | 71.553 ms | 47.224 ms |
| 2,048 | 17,408 | 2,048 | 8.5× fewer | 200.062 ms | 105.685 ms |

The old row rewrite count is `128 + 256 + … + N`, or `N(N + 128)/256` for these multiples of 128. The new count is exactly `N`; individual identity metadata entries are also written once instead of rewriting a monolithic identity catalog. The persistent B-tree still adds its normal lookup/page-update costs, and there is one atomic manifest/head/log-pruning commit per checkpoint.

At 2,048 rows, the repeat checkpoint time was about 47% lower (1.89× faster). **End-to-end throughput did not materially improve in this small-row workload:** median total time was 8,598 ms before versus 8,534 ms in the repeat. Thousands of individually durable insert commits dominate the total. Do not turn the entry-count improvement into a claim of an 8.5× overall speedup.

These are local shared-machine results, not isolated hardware benchmarks. An initial after run overlapped disk-intensive checkpoint property tests and showed large contention: its 2,048-row checkpoint samples were 212.576, 163.402 and 86.590 ms. That run is retained rather than discarded. The repeat started after those tests finished; its corresponding samples were 101.845, 105.685 and 110.626 ms. Unrelated development still ran on the machine. The deterministic entry-count reduction and recovery verification are stronger evidence than the timing estimate.

Raw data:

- [Before](2026-10-25-checkpoint-before.json)
- [Initial after, contended](2026-10-25-checkpoint-after-contended.json)
- [After repeat](2026-10-25-checkpoint-after-repeat.json)

The code and format details are in [incremental checkpoint storage](../INCREMENTAL-CHECKPOINTS.md). Existing format-1 files perform a one-time full conversion; that migration is excluded from this new-database measurement. Background checkpointing, publication-lock stall removal and serving working sets larger than RAM are not implemented by this change.
