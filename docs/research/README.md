# Research notes and open questions

Consulted for the initial implementation:

- [Functorial Data Migration](https://arxiv.org/abs/1009.1166): schema/instance model
  and restriction direction. The prototype implements a restricted relational signature,
  not general categorical migration functors.
- [Categorical Data Structures for Technical Computing](https://arxiv.org/abs/2106.04703):
  attributed C-set framing for typed relations and scalar roles. The mathematical
  precedent is acknowledged; no claim of novelty is made for this representation.
- [DataFusion custom table providers](https://datafusion.apache.org/library-user-guide/custom-table-providers.html):
  schema and scan-plan boundary, filter/projection/limit pushdown, deferred execution.
- [SQLite virtual-table mechanism](https://sqlite.org/vtab.html) and the locally installed
  rusqlite 0.32.1 provider source: lifecycle, best-index constraints, read-only callbacks,
  WITHOUT ROWID tables, and final predicate rechecking.
- [redb design](https://github.com/cberner/redb/blob/master/docs/design.md) and installed
  redb 3.1.3 API/source: atomic durable commits, file locking and read transactions.
- [PostgreSQL RLS](https://www.postgresql.org/docs/current/ddl-rowsecurity.html): default
  denial, input filtering, old/new write checks and integrity-check side channels.
- [Zanzibar](https://www.usenix.org/conference/atc19/presentation/pang): authorization
  consistency with object-content changes. This remains context for future ReBAC.

This is focused prototype research, not a completed review of the supplied bibliography.
DBSP, Leapfrog Triejoin, provenance semirings, online incompatible schema changes,
FoundationDB operational feasibility, and Bend 2 execution are subsequent research work.
Do not infer that their algorithms or proofs have been implemented.

## Next decisions

1. Incremental/background checkpoints and deliberate history retention/physical erasure.
2. Stable module and principal identities and incompatible version adapters.
3. A fresh-authorization service boundary for strict revocation and query resource budgets.
4. Streaming query cursors, range indexes, projection/limit pushdown and planner statistics.
5. Exact SQL decimal representation versus the current explicit minor-unit representation.
6. Richer typed query APIs, module artifacts/watch mode and IDE measurements.
7. Which cardinality and cross-relation constraints can be installed with bounded work.
8. Workload-selected indexes and specialization of in-memory tuple encodings.
9. Bend operation-trace interchange with opaque canonical identity symbols. Keep the
   reference evaluator independently simple; test the adapter separately. Broader proofs
   may benefit from a separate proof assistant. No proof claim follows from test success.
10. FoundationDB versus custom distribution, only after the local limits are measured.


The checkpoint format and the basic manifest/generation/local service have since
been implemented. Fresh requests observe current policies; strict in-flight revocation
and complete query resource accounting are still open. The DX research decisions and
current official source links are recorded in [ADR 006](../adr/006-dx-contracts.md).
