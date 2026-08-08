# W1b-1 merge-analysis readiness package — exact e1af

Status: test/report-only, frozen for independent review. No production edit,
Cargo/build, adapter runtime, benchmark, PR, or merge was performed.

## Pinned source and package boundary

- Anchor commit: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- Anchor tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- Anchor parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- Source worktree: `/tmp/lix-w1b1-merge-analysis-e1af`
- Exact production allowlist: see `SOURCE_ALLOWLIST.md`.
- Package-only path: `test-reports/w1b1-merge-analysis-e1af/`.

This package is the first W1b partition only. It does not authorize W1a,
changelog/change-provider, stale reconciliation, undo/redo, typed
transitions, checkpoint/history, working-diff, selector/BranchRef,
writer/publication, GC, CAS/blob layout, or W3-W5 changes.

## Current merge-analysis owner and call chain

The exact e1af chain is:

```text
SessionContext::merge_branch_preview / merge_branch
  session/merge/branch.rs:132-240,248-324
  ├─ branch_ref_reader_on_opening_read:144-164,260-283
  ├─ commit_graph_reader_on_opening_read:166-171,285-293
  ├─ ForkTree facade construction:173,295
  └─ with_opening_tracked_reader:176-187,298-311
       └─ session/merge/analysis.rs:46-111
            ├─ TrackedStateStoreReader::diff_commits(base, source)
            ├─ TrackedStateStoreReader::diff_commits(base, target)
            ├─ payload fallback/load path
            └─ plan_merge and conflict statistics
```

The same branch module later uses the ForkTree facade for derived plugin
conflicts, descriptors, registry entries, and semantic rows
(`session/merge/branch.rs:419-700,894-1200,1619-1725`). The W1b-1
correction must not leave that ForkTree path beside a second historical
reader. Merge-base chronology remains topology authority; state/member
payloads remain the authenticated ForkTree authority.

Direct reader plumbing that must disappear with this partition:

- `transaction/context.rs:7315-7323`,
  `with_opening_tracked_reader` and its merge callback plumbing;
- `tracked_state/diff.rs:340-380`, the legacy `diff_commits`
  implementation;
- `session/merge/analysis.rs:6,46-111`, its reader parameter and calls.

The implementation may add only the smallest authenticated ForkTree diff,
member, or payload method through the exact allowlist. It must not add a raw
store getter, a detached reader, or a new persistence authority.

## Target authority contract

The merge operation must use one transaction-opening retained
`ForkTreeReadFacade`/`CoherentView` over one `StorageRead` for:

1. target/source branch-ref selection;
2. merge-base topology and authenticated CommitCatalog identities;
3. base/source/target state-root and member-row reads;
4. plugin registry, file-owner, descriptor, and payload authentication;
5. diff, merge-plan, conflict, and terminal projection.

Branch-bound descriptors may borrow that exact view but may not begin, refresh,
clone, extract, replace, or cross-use the read. There must be no nested
`begin_read`, raw `HistoryQuerySource`/`JsonStoreReader`, detached
`TrackedStateStoreReader`, second `CommitGraphReader`, fallback/retry,
durable cache/index, or compatibility reader. The package model uses a view
identity and exactly one begin event to make this requirement discriminating.

Missing, malformed, wrong-kind, non-adjacent, cyclic, identity-substituted,
or corrupt catalog/root/member/payload authority must fail with a typed error
before any partial plan, conflict row, or LIMIT projection. Authenticated
absence is ordinary absence; `NULL`, tombstone, and an authenticated
zero-length BlobRef remain distinct values.

## Semantic acceptance gates

The future candidate must preserve, with exact deterministic ordering:

- merge-base, source, target commit IDs and generations;
- fast-forward, already-up-to-date, and ordinary three-way analysis;
- added, updated, deleted, unchanged, NULL, tombstone, and authenticated
  zero-length BlobRef states;
- same-entity conflicting changes, convergent equal-value changes, and
  conflict-free disjoint changes;
- plugin registry/file-owner and descriptor identity handoff;
- missing/malformed/wrong-kind/cyclic/identity-substituted authority;
- no partial result before a corruption error, including when a LIMIT is
  requested;
- one retained read/view identity and no second read/cache/fallback authority.

`merge_analysis_oracle.rs` is a standalone model for these cases. It is not
a claim that e1af passes; it is intentionally not compiled or run in this
task.

## Expected exact-e1af RED calibration

`verify_source_contract.sh` is source-only and intentionally exits `1` on
exact e1af. It must report the legacy reader in merge analysis, legacy
`diff_commits`, merge callback plumbing, and legacy tracked-state diff owner.
The opening-read branch-ref, topology, and ForkTree anchors are positive
controls. The captured expected output is in `EXPECTED_RED.txt`.

This RED is a readiness discriminator, not a production failure report. No
compiler or adapter result is inferred from it.

## Compiler-driven deletion order

1. Add or verify the smallest authenticated ForkTree diff/member/payload
   primitives and focused source tests; do not delete the old reader yet.
2. Convert `session/merge/analysis.rs` to the retained ForkTree owner and
   remove its `TrackedStateStoreReader` parameter and legacy
   `diff_commits` / payload fallback calls.
3. Remove only merge callback plumbing from `session/merge/branch.rs` and
   `transaction/context.rs`; keep transaction writer and merge publication
   semantics unchanged.
4. Convert or delete `tracked_state/diff.rs` only after the compiler proves
   it has no W1b-2 or other remaining production consumer.
5. Port/quarantine only merge-analysis test fixtures that depended on the old
   reader. Do not touch tests for reconciliation, undo/redo, checkpoint,
   working-diff, selectors, writers, GC, CAS, or W3-W5.

## Future commands (each bounded to 1200 seconds)

Run only after a candidate has the exact allowlist and source/compiler gate:

```sh
timeout 1200s test-reports/w1b1-merge-analysis-e1af/verify_source_contract.sh \
  "$PWD" HEAD e1af471b9ab0f598dafa7c2ddec7867667c81740

timeout 1200s rustc --edition=2024 --test \
  test-reports/w1b1-merge-analysis-e1af/merge_analysis_oracle.rs \
  -o /tmp/w1b1-merge-analysis-oracle
timeout 1200s /tmp/w1b1-merge-analysis-oracle

timeout 1200s cargo test -p lix merge::analysis --lib
timeout 1200s cargo test -p lix merge_branch --lib
timeout 1200s cargo test -p lix --lib --features slatedb merge::analysis
```

The future runtime order is Memory/default first, then exact RocksDB, then
SlateDB. Required focused controls are merge preview/commit, fast-forward and
three-way/disjoint/same-entity cases, plugin/file-owner identity, corruption
and cold reopen. No broad matrix is authorized before the source gate passes.

## Review verdict boundary

This package is a readiness oracle, not an approval of a future production
candidate. Independent review should approve only if provenance, exact
allowlist, RED calibration, one-read authority, fail-closed semantics, and
the compiler-driven deletion order all match this package. Any production
path outside the allowlist, second reader/view, fallback/cache/compatibility
path, or W1b scope widening is a blocker.
