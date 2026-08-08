# W1b-2 stale reconciliation correction readiness package

This direct successor is test/report-only and is based on immutable correction
head `8b44e8cbd226e8820498e7c5c8e02d291c34abb8`. It is anchored to exact e1af
and contains no production source, Cargo manifest, adapter, benchmark,
runtime, PR, or merge change.

## Immutable source anchor

- Commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- Tree: bfa0d271a723da8250ab76ada16fda90926f1099
- Parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- Direct predecessor: origin/codex/review/w1b2-stale-reconciliation-correction-e1af
- This successor is to be published as a new immutable ref; no predecessor is
  rewritten.

## Package files

- W1B2_STALE_RECONCILIATION_READINESS.md: call graph, authority contract,
  semantic gates, deletion order, RED calibration, and future commands.
- SOURCE_ALLOWLIST.md: exact candidate production path allowlist and forbidden
  widening.
- stale_reconciliation_oracle.rs: standalone deterministic model and
  positive/negative identity, read, idempotency, and ordering fixtures; the
  correction gate compiles it with `rustc --edition=2024 --test -D warnings`.
- negative_reconciliation_fixtures.rs: independently compiled negative-fixture
  runner over the same stateful model.
- fixtures/green/: hermetic six-path source tree proving a genuine structural
  GREEN verifier result.
- verify_source_contract.sh: candidate-parametric, function-scoped source
  verifier; exact e1af remains intentionally RED because the legacy stale
  reader remains.
- EXPECTED_RED.txt: exact source-only calibration from e1af.
- SHA256SUMS: hashes of all package artifacts except this checksum file.

## Non-goals

Merge analysis, W1a/changelog, undo/redo, typed transitions,
checkpoint/history, working-diff, selectors/BranchRef, writer/publication,
GC, CAS/blob layout, and W3-W5 are excluded. No adapter or compiler
qualification is claimed.
