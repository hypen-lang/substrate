# Reference-closure visibility and quota work

Implemented 2026-10-25. Snapshot reads now memoize completed reference-closure
visibility decisions, avoiding repeated traversal of a shared graph for each
candidate row. Native scans/lookups/gets, SQL's shared-row cursors, and page/range
reads share the same snapshot-local mechanism.

## Correctness boundary

The memo is bound to the exact immutable `Arc<State>` and principal. Snapshot
clones may share it. Rebinding either state (including effective capability grants)
or principal clears it before use. New transaction snapshots receive fresh caches;
no cache persists in storage, crosses restarts, or becomes a new permission source.

Traversal explores unknown reachable nodes and records reverse edges. Local denied
nodes and previously completed false results seed a backwards denial pass. All
remaining explored nodes are visible: this computes the greatest fixed point for
cycles. Merely encountering a node already on the worklist does not prematurely
mark it true. A denied branch does not poison an unrelated valid branch. Both true
and false completed results are memoized, including for graphs with shared denied
exits.

Each previously unknown node is expanded once for a snapshot/policy context. The
memo retains at most the traversed graph's node decisions; temporary traversal
storage is proportional to new reachable vertices and edges. Ordered maps/sets
add logarithmic factors, so this is not a claim of constant-cost lookup or strict
linear CPU complexity. Cache access uses one snapshot-local mutex. Independent
snapshots do not serialize on it. It does not lock the live database head during
traversal.

Entry and final live authorization gates remain authoritative. Revocation, credential
rotation and expiry still reject retained reads even when their row visibility was
cached. Old data snapshots still retain old data when authority remains valid.

**Transactional reads deliberately retain uncached traversal.** Their complete
row/family/bucket dependencies still participate in commit validation. We did not
trade away dependency tracking to accelerate transactions. This means reference-heavy
reads inside mutable transactions can still repeat graph traversal; dependency-aware
transaction caching remains future work.

Snapshot-only reads no longer allocate dependency sets which are discarded.
Administrative snapshot reads check existence directly. Credential/capability quota
checks now read a privileged family count without cloning complete ledger rows;
they retain the family dependency, so concurrent issuance still conflicts/retries
correctly. The per-issuer capability quota still scans compiled capability metadata;
this change does not add an issuer count index.

## Measurements

Run:

```sh
cargo run --release --offline --example visibility_performance
```

The fixture has N chain nodes, one leaf and N roots all referring to the same chain.
Every read is authorized. Setup is excluded. Each of seven measured scans creates a
fresh snapshot; the cache therefore earns its benefit during that scan rather than
reusing a warmed previous result. The public API returns owned rows in both versions.
The table reports the median of those seven scans.

| Chain nodes / roots | Before | After |
| --- | ---: | ---: |
| 250 / 250 | 12.072 ms | 0.393 ms |
| 500 / 500 | 55.629 ms | 0.799 ms |
| 1,000 / 1,000 | 248.497 ms | 1.128 ms |

The baseline was captured with the same harness before visibility edits. Other
agents were developing on the same machine, so absolute timing and small-ratio
comparisons are not isolated production estimates. The reduction in traversal work
is also checked deterministically: for both visible and hidden shared chains,
2,000 distinct nodes are expanded exactly 2,000 times, not once per root's path.
Raw medians: [before](benchmarks/2026-10-25-visibility-before.csv) and
[after](benchmarks/2026-10-25-visibility-after.csv).

## Verification

Seven dedicated tests include 128 property cases over arbitrary cyclic graphs,
comparing cached answers with the original fresh reference traversal. Other tests
cover denied exits through cycles, unrelated valid branches, exact state/principal
cache binding, completed-negative reuse, retained live revocation, transaction
conflicts after a referenced row changes, old/new snapshot semantics, and quota
family phantom conflicts. Existing credential-expiry tests warm snapshot reads
before crossing their deadline. Existing SQL, delegation, concurrency and mutation
security regressions also pass.
