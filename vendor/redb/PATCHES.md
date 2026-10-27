
## Page payload allocation

Cached pages now use internal `Arc<Box<[u8]>>` handles instead of an inline
`Arc<[u8]>` allocation. The reference counts and slice descriptor occupy one
small allocation; the payload occupies an exactly page-sized allocation. On the development
macOS allocator, a 4,112-byte inline Arc allocation rounds to 5,120 bytes, while a
4,096-byte payload plus a 32-byte Arc allocation requests approximately 4,128
bytes. A second allocation/free and one additional pointer load are the tradeoff;
other allocators and page sizes may behave differently.

`cached_file.rs` reads into a zero-filled Vec, then moves its allocation into a
boxed slice without a payload copy. The Arc is created only after a successful
read. Every backend still receives initialized `&mut [u8]`, including for zero
length. A failed partial read neither publishes a page nor charges the read
cache, and sticky I/O failures remain unchanged. Cache limits continue to count
logical payload bytes. Immutable guards and writable-page unique ownership,
checksums, disk formats, durability and transaction semantics are unchanged.

The representation is internal to cached_file.rs, page_store/base.rs,
btree_base.rs and btree_mutator.rs. Public StorageBackend and access-guard APIs
remain unchanged. Focused tests assert the backend's payload address survives
through Box/Arc construction, cache hits share the allocation, invalidation does
not invalidate retained guards, and partial-read failure leaves cache state
unchanged. Run the existing page/cache and transaction tests plus measured
memory/latency comparisons when changing this representation.

An earlier one-million-record comparison against an otherwise identical inline-page
build motivated retaining this representation for its memory benefit despite mixed
load/read results. That allocation experiment is historical; later load, probe and
aggregate optimizations leave this backend representation unchanged. The repository
retains only the [latest completed benchmark set](../../docs/benchmarks/latest/README.md),
whose hashes and comparison columns define what it measures. Do not attribute later
before/after gains to page boxing when both binaries already use boxed payloads.
The allocation choice is not a universal speed or RSS guarantee. Re-evaluate its
memory/latency tradeoff on each supported allocator and backend upgrade; the earlier
integration passed all 41 vendored-backend tests.

## Borrowed range visitation

`Range::visit_borrowed` visits the remaining ascending range using a typed,
higher-ranked callback. Keys and values borrow the current immutable leaf only
for the callback invocation; the signature prevents retaining those borrows in
captured state or returning them as the break payload. The range keeps its
transaction guard. No unsafe lifetime extension, row buffering, or new cache is
introduced. Existing owning `next` and `next_back` APIs remain available.

All three methods share the same iterator advancement and bound-crossing logic.
A callback break consumes that entry, returns the application-defined payload,
and permits resuming either direction. Storage failures retain normal iterator
error ordering and cursor transitions. Callback panics also leave the current
entry consumed. This removes per-entry owning page-handle construction from
streaming callers; callbacks can still perform their own allocations or copies.
It does not change storage encoding, validation, checksums, durability, or bounds.

`tests/redb_lower_bound.rs` checks parity against an ordered model across leaves,
variable-size keys/values, inclusive/exclusive bounds, mixed forward/backward
iteration, repeated break/resume, cloned iterators, concurrent commits while
visiting a pinned snapshot, retained owning guards, callback panic, and backend
read failures. The API documentation contains a compile-fail example ensuring
page borrows cannot escape. Run those tests, the vendored tests, and that doctest
when upgrading the backend or changing the cursor state machine.

## Reusing unpinned cache allocations

On a read-cache miss that would exceed the configured payload budget, the backend
retires one lowest-priority entry from the same cache stripe before reading its
replacement. If that allocation has the requested size and `Arc::get_mut` proves
exclusive ownership, its existing Arc and boxed payload are reused. Retained
snapshot/page guards prevent reuse; different-sized or shared allocations use the
ordinary allocation path. The payload is zero-filled before invoking the backend,
and the cache lock is released before I/O. A partial read failure publishes no
replacement and keeps the existing sticky failure behavior.

This removes repeated allocation/free of cache-sized working sets without a
spare pool, platform-specific allocator calls, or larger cache budget. Retired
payload bytes are debited immediately; successful insertions and any remaining
overflow use the existing accounting. As before, cache accounting covers retained
cache entries, while caller-held pages and concurrent in-flight reads have separate
lifetimes. Concurrent misses may still perform duplicate reads; they never mutate
an allocation reachable through an existing guard. RSS improvements remain a
measurement question, not a guarantee of the allocator's reservation policy.

Focused tests check allocation identity reuse, immutable retained guards, disabled
and variable-size cache behavior, failed recycled reads, and concurrent misses
with final byte accounting. Run these with the ordinary cache/transaction tests
and paired memory and latency diagnostics before retaining the optimization.
