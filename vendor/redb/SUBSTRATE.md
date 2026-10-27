# Local redb read-path fixes

This directory vendors the crates.io redb 3.1.3 distribution. Upstream crate SHA-256:
`4ba239c1c1693315d3cc0e601db3b3965543afbf48c41730fdca2f069f510f4a`.
The MIT and Apache-2.0 licenses are retained. `Cargo.toml.orig` is the upstream manifest, not the active manifest.

Substrate uses this dependency through an explicit versioned path dependency. This also applies when Substrate is a path dependency of another workspace; a root-only `[patch.crates-io]` would be ignored in that situation. Local changes are limited to separately allocated, reference-counted page payloads, direct reads into their initialized buffers, and a read-only first-at-or-after lookup. They do not change the file format, checksum validation, transaction behavior, write durability or existing range semantics. Patch-specific notes and tests document each change.

The active manifest and toolchain require Rust 1.94, matching Substrate. This local dependency is temporary: replace it with an upstream release offering equivalent operations after the same recovery, corruption, authorization and benchmark checks pass. Do not update this directory without reviewing the local patch diff against its recorded upstream version.

Package publication is outside this milestone. Publishing Substrate requires an upstream redb release with these operations or an explicit dependency-packaging decision; the local path must not be assumed to survive registry publication. Frozen collection comparisons copy and hash this vendor directory with the engine source.
