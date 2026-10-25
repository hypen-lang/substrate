# DataFusion and imbl implementation review

Reviewed on 2026-10-25 against baseline `abb0a72`, followed by a second review of the fixes below. This was an independent, primarily read-only review; only this report was written by the reviewer.

## Scope

- The default DataFusion SQL adapter, isolated authorized catalogs, live authorization guards, parameter rewriting, read-only statement validation, scalar conversions, output budgets, task failure handling, and repeated prepared execution.
- SQL transaction read dependencies and failure handling, explicit SQLite compatibility, persisted view integration, profiling changes, and the `im` to `imbl` dependency substitution.
- The checked-arithmetic helper and its aggregate/window state handling after the initial findings were addressed.

No additional concrete authorization bypass or transaction-boundary defect was found in this scope. This is a code review, not a proof of security or exhaustive validation of upstream DataFusion.

## Findings and fixes

**P1: integer arithmetic could silently wrap.** DataFusion's default physical integer operators and SUM could return incorrect values, including incorrect predicate matches. Small independent probes using the same native DataFusion execution version reproduced `i64::MAX + 1` becoming `i64::MIN`, and narrower or mixed integer casts wrapping as well.

The implementation now rewrites signed integer arithmetic before optimization and again after parameter binding to private checked operations. Narrow results are checked when cast back to their inferred type. Integer SUM uses checked widened accumulation, checked final conversion, and non-null counts for sliding-window retraction. Unsupported unsigned/decimal arithmetic requires an explicit cast; integer `SUM DISTINCT` is explicitly rejected. Regression tests cover constants, columns, mixed widths, parameters, predicates, grouped and windowed SUM, nulls, cancellation of intermediate sums, repeated execution, and non-finite results.

**P1: OVERLAY bypassed scalar allocation restrictions.** Both function syntax and the special `OVERLAY(... PLACING ... FROM ... FOR ...)` expression can concatenate strings. A short chain of queries can amplify allocations before output limits apply. The validator now rejects both AST forms, with small regression cases that do not perform large allocations.

Both findings were re-reviewed after their fixes. No outstanding actionable finding remains from this review.

## Verification evidence

The reviewer inspected the implementing agent's completed test logs: `cargo test --test datafusion` passed all **16 integration tests**, and the focused DataFusion unit tests passed **2 tests**. The reviewer independently reproduced the original arithmetic and OVERLAY behavior with small queries against native DataFusion 55.2.0 and inspected the final Rust fixes and regression tests. The reviewer did not run another Rust build or benchmark. The root agent's broader suite and final lint checks were still running when this report was written; this report does not assert their outcome.

## Remaining limits

The catalog is eagerly materialized within documented source-row and byte caps. The operator memory pool is not a hard process-RSS limit, and deadlines/cancellation remain cooperative. Rejecting known scalar amplifiers does not establish hard isolation for arbitrary hostile SQL; stronger isolation remains future work. SQL transaction reads conservatively track all readable families, which can create extra conflicts. DataFusion has dialect and supported-type differences from SQLite, available through the explicit compatibility adapter. Native query execution is otherwise unchanged.
