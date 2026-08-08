# W1b-2 stale reconciliation readiness package

This package is test/report-only. It is anchored to exact e1af and contains
no production source, Cargo manifest, adapter, benchmark, runtime, PR, or
merge change.

## Immutable source anchor

- Commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- Tree: bfa0d271a723da8250ab76ada16fda90926f1099
- Parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- Planned package ref: origin/codex/w1b2-stale-reconciliation-oracle-e1af

## Package files

- W1B2_STALE_RECONCILIATION_READINESS.md: call graph, authority contract,
  semantic gates, deletion order, RED calibration, and future commands.
- SOURCE_ALLOWLIST.md: exact candidate production path allowlist and forbidden
  widening.
- stale_reconciliation_oracle.rs: standalone deterministic model and
  positive/negative fixtures; not compiled or run in this task.
- verify_source_contract.sh: source-only verifier; exact e1af is intentionally
  RED because the legacy stale reader remains.
- EXPECTED_RED.txt: exact source-only calibration from e1af.
- SHA256SUMS: hashes of all package files except this checksum file.

## Non-goals

Merge analysis, W1a/changelog, undo/redo, typed transitions,
checkpoint/history, working-diff, selectors/BranchRef, writer/publication,
GC, CAS/blob layout, and W3-W5 are excluded. No adapter or compiler
qualification is claimed.
