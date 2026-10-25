# Architecture decisions

These are prototype decisions. The logical guarantees are commitments for this
supported subset; physical choices remain replaceable after measurement.

- [001: Storage and transaction publication](001-storage.md)
- [002: Identity and conservative schema contracts](002-identities-schemas.md)
- [003: SQL interoperability](003-sql.md)
- [004: Authorization and snapshots](004-authorization.md)

- [005: Checkpoints, bounded redo and compiled metadata](005-checkpoints.md)

- [006: DX contracts, generated SDKs and local application host](006-dx-contracts.md)

- [007: Typed queries, batched relationship reads and scoped discovery](007-nested-queries.md)

- [008: Scoped schema control and runtime operations](008-schema-control.md)

- [009: Scoped delegation and live revocation](009-delegation-revocation.md)

- [010: Authenticated remote access, credential rotation and expiry](010-authenticated-remote.md)
