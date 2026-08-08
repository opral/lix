# W1b-2 stale reconciliation correction readiness package

This direct successor is test/report-only and is based on blocked immutable
correction head `7d71c5c381a2ab1eb049d955258d20291bc3a611` (tree
`c1d688ecff0be0e68d41436b54db21eadd45cd38`), whose parent is
`8b44e8cbd226e8820498e7c5c8e02d291c34abb8`. It is anchored to exact e1af
and contains no production source, Cargo manifest, adapter, benchmark,
runtime, PR, or merge change.

## Immutable source anchor

- Commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- Tree: bfa0d271a723da8250ab76ada16fda90926f1099
- Parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- Blocker report: `fa5294b50e30c8ca7fdaf98166b6dd7c606b5f6cd598852f38c2d4dbebfc687d`
- Direct predecessor: `7d71c5c381a2ab1eb049d955258d20291bc3a611`
- Parent of predecessor: `8b44e8cbd226e8820498e7c5c8e02d291c34abb8`
- This successor is published as a new immutable ref; no predecessor is
  rewritten or self-approved.

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
- fixtures/green/runtime.rs: executable retained-read/complete-plan/atomic-
  commit fixture compiled with warnings denied and run as nine tests.
- fixtures/green/: hermetic six-path source tree plus the executable runtime
  proving a genuine structural GREEN verifier result.
- verify_source_contract.sh: candidate-parametric, function-scoped source
  verifier; exact e1af remains intentionally RED because the legacy stale
  reader remains.
- EXPECTED_RED.txt: exact source-only calibration from e1af.
- SHA256SUMS: hashes of all package artifacts except this checksum file.

## V3 correction requirements

- The positive fixture binds one `OpeningStorageRead` identity through an
  operation-owned `ForkTreeReadFacade`, validates every write in a complete
  ordered plan, and reaches one `AtomicCommit` only after authentication.
- Swapped views, partial plans, duplicate operations, second reads, second
  commits, fallback/cache/alternate-authority tokens, and changed-path scope
  escapes are negative conditions.
- Idempotency walks every operation before replay success; mixed
  match/mismatch, reordered, and duplicate controls are executable.
- Owner/plugin/generation/revision/change/selector/commit identities are
  cross-bound for unrelated-owner paths; same-owner stale reconciliation and
  unrelated-owner success remain distinct.

## Non-goals

Merge analysis, W1a/changelog, undo/redo, typed transitions,
checkpoint/history, working-diff, selectors/BranchRef, writer/publication,
GC, CAS/blob layout, and W3-W5 are excluded. No adapter or compiler
qualification is claimed.
