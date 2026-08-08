# W1b-1 merge-analysis readiness package

This package is test/report-only. It is anchored to the exact e1af source and
contains no production source, Cargo manifest, adapter, benchmark, runtime, or
PR change.

## Immutable source anchor

- Commit: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- Tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- Parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- Planned package ref: `origin/codex/w1b1-merge-analysis-oracle-e1af`

## Package files

- `W1B1_MERGE_ANALYSIS_READINESS.md`: scope, call graph, contract, RED
  calibration, deletion order, and future commands.
- `SOURCE_ALLOWLIST.md`: exact candidate production path allowlist and
  forbidden widening.
- `merge_analysis_oracle.rs`: standalone semantic model and discriminating
  positive/negative fixtures; not run in this task.
- `verify_source_contract.sh`: source-only verifier. On exact e1af it is
  intentionally RED because the old merge reader remains.
- `EXPECTED_RED.txt`: deterministic source-only calibration expected from
  exact e1af.
- `SHA256SUMS`: hashes of all package files except this checksum file.

## Non-goals

W1a/changelog, stale reconciliation, undo/redo, typed transitions,
checkpoint/history, working-diff, selector/BranchRef, writer/publication, GC,
CAS/blob layout, and W3-W5 are excluded. No adapter runtime or compiler
qualification is claimed.
