# TrackedHead GC/current-generation migration acceptance oracle

## Immutable binding

```text
source 413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
tree   820fe560da3bbd2b00b788b0b1759c409048cd6e
parent 11442c1e0023e20307a7231d88cd557bc704fd13
gate   0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d
```

The whole-module deletion gate is a prerequisite and remains the sole
residue/authority deletion proof. This first GC oracle may not restore a
TrackedHead module, add a compatibility reader, or bypass the gate.

## Exact allowed production paths

The first migration's production logic may change only:

```text
packages/lix/src/gc.rs
packages/lix/src/forktree/view.rs
packages/lix/src/forktree/model.rs
packages/lix/src/forktree/state.rs
packages/lix/src/forktree/serving.rs
packages/lix/src/forktree/publication.rs
packages/lix/src/forktree/mod.rs
```

Test/report-only wiring may be under:

```text
packages/engine-benchmarks/tests/tracked-head-gc-migration-oracle.rs
packages/lix/tests/tracked-head-gc-migration-oracle.rs
test-reports/tracked-head-gc-oracle-413e/**
```

Any production diff in `init.rs`, `functions/*`, transaction code,
`sql2/providers/working_diff.rs`, storage adapters, or `tracked_state` is
outside this first GC/current-generation cut and blocks admission. The
whole-module gate separately owns compile-driven removal of obsolete imports.

## Sole authority and same-view contract

1. Open one retained `CoherentView` from the authenticated global selector,
   branch selector, repository root, branch snapshot, and catalog roots.
2. Resolve current generation from ForkTree state roots/catalogs and derive
   `(live_count, ordered_identity_digest)` in that view. No BranchHead
   `tracked_generation`/`untracked_generation`, hot generation, marker row,
   current-state side index, or fallback is authoritative.
3. Observe GC roots from authenticated branch/global roots, commit/change
   catalogs, recovery references, retention roots, and content-addressed
   object/state edges. A missing or malformed root is corruption, never an
   empty live set.
4. Progress, owner, selector, and epoch preconditions are captured from the
   same view. The write plan must stage root retirement and progress together;
   one owner-selector CAS fences the publication.
5. A non-no-op GC publication has one coherent view, one plan, one existing
   prepared write/commit. No helper or GC worker performs an independent
   publication commit.
6. No-op, unsupported, stale, unrelated-owner, malformed, or wrong-kind paths
   create zero plans and zero backend writes.

## Required GC cases

```text
GEN-SELECTOR-ROOT       current count/digest from ForkTree selector/root
GEN-BRANCH-GLOBAL       branch-local and global roots remain isolated
ROOT-HISTORY-CHECKPOINT  retained chronology/recovery roots stay live
ROOT-SHARED-FINAL       shared object survives until final owner is gone
RACE-PUBLICATION-FIRST   newer publication makes old GC fence stale
RACE-GC-FIRST            GC cannot delete a root accepted by newer publication
RACE-SAME-OWNER          owner match with changed epoch fails closed
RACE-UNRELATED-OWNER     unrelated owner cannot satisfy selector/progress CAS
DRAIN-65                 64-entry batch plus one suffix drains exactly once
DRAIN-BLOCKED            debt/progress resumes without spin or skipped entry
CORR-MISSING             selector/root/catalog/progress missing fails closed
CORR-MALFORMED           malformed/wrong-kind object fails before deletion
CORR-MISMATCH            branch/global/root/catalog identity mismatch fails
CORR-CYCLE-DUPLICATE     cycle, duplicate, or back-edge fails closed
REOPEN-FLUSH-DROP        cold reopen reproduces roots, progress, and live set
```

The 65-entry control must prove monotonic progress, no duplicate or skipped
sequence, exact suffix handling, and persisted progress that can be reopened.

## Legacy closure prohibition

The GC/current-generation closure must contain zero:

```text
TrackedHeadContext
tracked_serving_commit_dependencies
untracked_json_refs
stage_collect_stale_current_state_generations
stage_collect_stale_working_diff_indexes
tracked_reachability(
TRACKED_WORKING_DIFF_MARKER_SPACE
CURRENT_STATE_DATA_PART_SPACE
CURRENT_STATE_DATA_PART_REFS_SPACE
```

The source verifier applies these checks to the root-observation,
`validate_live_native_parts`, and recovery/current-generation staging regions,
not to unrelated historical object-retention helpers. This avoids treating
generic `tracked_state` history maintenance as a false positive while still
forbidding the old current-generation owner and its spaces in the closure.

## Terminal rule

413e is a RED calibration until the whole-module gate and this GC closure gate
are GREEN. Stop at the first source/model/Memory/RocksDB/SlateDB blocker. No
performance, current-main comparison, init/transaction/SQL widening, or
production edit is part of this oracle.
