# Independent review: whole-module TrackedStateStoreReader deletion gate

Verdict: **BLOCKER** for integration against the accepted `713/b59` lineage.
This is a TEST/REPORT-ONLY review. No production source was edited, compiled,
or benchmarked.

## Immutable objects

Reviewed gate:

* head `72f10a4412dbea93c3a266a20a9c2df91d02193c`
* tree `67c9b631fb701da23f79e2e41d057d027d304e6a`
* parent/base `be6ea48cfea4d4a49844216aee683f6ada9ec708`
* parent-to-head full-index diff SHA-256
  `8e16788c905cbcee032a3c9a8f5e9dd86c5cd94a173d57836bf759ab2359cb0e`
* stable patch ID `a4f718e106c139d7c28851ed57c5ce04d86a7830`

Accepted lineage to which the gate was requested to apply:

* duplicate-overlay guard `713455a3557907ce705d06f720fcdc4486bddd4a`,
  tree `9c15144678fa952e1f50c5259df1c4dbb0199168`, parent `ab90fc51…`;
  parent-to-head full-index diff
  `61d764ab877b75c7726a2ebea8020177a5bf819eed202abecdb359dc7a517c19`,
  patch ID `1a3dd2e4ddc1a6e0fbe0dcdc45e001ea0a2a94e5`.
* historical fail-closed successor `b59e1f11a51153e0a787a81f0f25bf104d150aaf`,
  tree `700fd04d21bc40c05425c9fc9e10d65c9e1eda24`, parent exactly 713;
  parent-to-head full-index diff
  `4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4`,
  patch ID `63dcb8dcecba8a25dea0ce8be19d26cdac264729`.

`72f10a4` is not descended from either 713 or b59; its parent is the older
`be6ea4` acceptance branch. Its source binding remains exact 413 tree
`820fe560da3bbd2b00b788b0b1759c409048cd6e`, not b59 tree `700fd0…`.

## Checks reproduced

The gate was run read-only against an exact 413 worktree:

```text
bash packages/lix/tests/forktree_tracked_state_reader_deletion_gate.sh \
  /root/repos/lix-stage2-frontier-413-for-review \
  413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d \
  820fe560da3bbd2b00b788b0b1759c409048cd6e
```

Result: `RESULT=RED`.

The gate correctly finds 29 `TrackedStateStoreReader` references, all four
reader-only module paths, their reexports, and the listed legacy spaces. The
required ForkTree and derived/history guard tokens pass. The same 29 direct
reader references and all four module paths remain present on both accepted
713 and b59, so the deletion work is still genuinely outstanding.

The dormant negative probes are present and correctly marked
`EXPECT_COMPILE_FAIL`:

* `forktree_tracked_state_forbidden_reader.rs`
* `forktree_tracked_state_forbidden_space.rs`

They were not compiled, as required by the gate contract; no candidate Lix
rlib exists in this review.

## Blocking findings

1. **Stale lineage and prerequisite binding.** The gate checks 413 and the
   old `derived.rs` guard. It does not check b59’s accepted correction:
   `forktree/serving.rs:599-609` now requires a selected CommitCatalog entry,
   and `forktree/tests.rs:841-946` covers missing commit catalog and missing
   state root. The gate therefore cannot certify the current historical
   fail-closed owner or prevent a future deletion candidate from regressing
   it.

2. **Wrapper/adapter residue is under-probed.** The forbidden list checks
   names such as `TrackedStateReaderAdapter`, `TrackedStateReaderWrapper`,
   and `TrackedStateReaderCompat`, but does not forbid the exact legacy
   factories named in its own deletion contract:
   `TrackedStateContext::reader`, `tracked_state_reader`, and
   `with_opening_tracked_reader`. A candidate could retain a renamed wrapper
   and pass the negative API probe while violating the no-wrapper/no-fallback
   policy.

3. **Deletion ordering is descriptive, not compiler-proven.** The gate
   reports direct uses in `gc.rs`, `storage_bench.rs`, `commit_graph`,
   `session/execute`, checkpoint/history, SQL diff, merge, and transaction
   cohorts, but does not separate writer ownership from reader-only uses or
   enforce the required order: migrate semantic readers, delete reader-only
   modules/reexports, then remove spaces and GC/benchmark references. It
   cannot prove that no unique writer or retention owner is deleted early.

4. **The dual-adapter gate is not frozen as an executable target.** The
   documented commands name
   `packages/lix/tests/forktree_tracked_state_reader_acceptance` and
   `lix_tests`, but neither the test target nor its source is present in the
   72f package or in b59. The commands are a future contract only; they do
   not yet bind exact Memory/RocksDB/SlateDB semantics, cold reopen,
   corruption, or read/view counters.

## Required report-only correction

Before approval, rebind the gate to exact b59 (or a newer immutable successor)
and preserve 713 as its parent lineage. Add source-only checks for the b59
historical owner and regression tests, reject the three exact legacy factories
above, and freeze a real adapter-test source or an explicit implementer
contract with exact test names for Memory, RocksDB, and SlateDB. Keep the
negative probes and legacy-space residue checks unchanged, but make deletion
ordering compiler-observable by cohort and owner.
