# First migration oracle: transaction working-diff/generation

## Immutable binding

Source anchor:

```text
413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
tree 820fe560da3bbd2b00b788b0b1759c409048cd6e
parent 11442c1e0023e20307a7231d88cd557bc704fd13
```

This package is bound to the previously frozen whole-module gate:

```text
gate ref  origin/codex/tracked-head-whole-module-oracle-413e
gate head 0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d
gate tree 029a89195741920a7ff50a6a79bdefe0ec35f927
```

The gate is a prerequisite, not a second authority. The first migration may
not restore or bypass its deletion/residue proof.

## Exact allowed paths

Future production logic for this first migration is limited to:

```text
packages/lix/src/transaction/context.rs
packages/lix/src/transaction/types.rs
packages/lix/src/transaction/staging.rs
packages/lix/src/collection_generation.rs
packages/lix/src/forktree/view.rs
packages/lix/src/forktree/model.rs
packages/lix/src/forktree/state.rs
packages/lix/src/forktree/serving.rs
packages/lix/src/forktree/publication.rs
packages/lix/src/forktree/mod.rs
packages/lix/src/live_state/forktree_reader.rs
```

Compile-driven deletion may remove only obsolete imports/reexports in
`packages/lix/src/live_state/context.rs`, `live_state/mod.rs`, and
`storage_bench.rs`; it must not add a new authority there.

Test/report-only additions may be under:

```text
packages/engine-benchmarks/tests/tracked-head-transaction-migration-oracle.rs
packages/lix/tests/tracked-head-transaction-migration-oracle.rs
test-reports/tracked-head-transaction-oracle-413e/**
```

The following are explicitly outside this first cut and any production diff
there blocks admission: `init.rs`, `gc.rs`, `functions/state.rs`,
`functions/context.rs`, `sql2/providers/working_diff.rs`, storage-bench logic,
new `tracked_state` readers/writers, any `live_state/tracked_head*` path, and
any adapter implementation. SQL's public working-diff surface is qualified by
the later SQL/whole-module gate; this oracle covers the transaction caller
closure only.

## Ownership and atomicity contract

1. `working_diff_at_head` and collection-generation resolution open one
   transaction-scoped `CoherentView`; all selector, branch snapshot, root,
   catalog, and checkpoint reads use that retained view.
2. The ForkTree state root and commit/change catalogs are the only facts used
   to resolve current identity, checkpoint-relative history, and generation.
   Working diff and `(live_count, ordered_identity_digest)` are derived,
   root-bound terminal results. They are not persisted hot markers, generation
   rows, side indexes, or caches crossing views.
3. A staged transaction overlay may alter the derived result locally. Savepoint
   rollback discards that overlay and any derived cache; it never mutates the
   opening view or durable authority.
4. An advanced write performs exactly one `PreparedPublication`, one
   `into_storage_plan`, one existing `prepare_write_set`, one prepared backend
   commit, and one selector+epoch CAS. No independent ForkTree commit or
   legacy tracked-head write is permitted.
5. True no-op, unsupported cohort, stale same-owner, stale unrelated-owner,
   and corruption paths create zero plans and zero commits.
6. Missing, malformed, wrong-kind, wrong-owner, stale, cyclic, duplicate, or
   cross-view data fails closed. It cannot be converted into an empty diff or
   generation and cannot invoke a legacy reader/writer.

## Caller-closure obligations

The candidate must eliminate these current 413e responsibilities without
changing public semantics:

- `packages/lix/src/transaction/context.rs:6520-6528`: packed identity
  membership must use the opening ForkTree state view and staged overlay.
- `:7420-7441`: `working_diff_at_head` must derive the checkpoint-relative
  diff from the branch snapshot/head and ForkTree catalogs/state, not
  `TrackedHeadContext` or a hidden `TrackedStateContext` fallback.
- `:8139-8167`: prepared mutation generation must derive count/digest from the
  same view plus overlay.
- `:8916-8970`: collection generation and exact live count must not read a
  hot generation or rotate a selector from a read helper.

The caller closure has zero legacy tracked-head reads/writes. In particular,
the final diff must contain no `TrackedHeadContext`, `TrackedWorkingDiff`,
`working_diff_for_control`, old marker, old staging method, or old generation
writer in `transaction/context.rs`. The working-diff function body must not
open `TrackedStateContext` as a fallback.

## Race, rollback, and corruption cases

The pure model and future adapters must cover:

```text
WD-ADD-MOD-REMOVE       exact added/modified/removed diff and digest
WD-CHECKPOINT-FLOOR     checkpoint baseline and history floor
GEN-COUNT-DIGEST        exact count plus ordered identity digest
GEN-STAGED-REPLACE      overlay changes result without durable generation row
SAVEPOINT-ROLLBACK      rollback restores view-derived result and cache state
STALE-SAME-OWNER        selector epoch mismatch rejects same logical owner
STALE-UNRELATED-OWNER   owner token mismatch cannot satisfy the CAS
NOOP-ZERO-WRITE         no plan, selector rotation, or backend commit
CORR-MISSING-ROOT       missing selector/root/catalog fails before output
CORR-MALFORMED-KIND     wrong object domain/kind fails before publication
CORR-ROOT-MISMATCH      branch/catalog/checkpoint identity mismatch fails
CORR-CYCLE-DUPLICATE    topology cycle/duplicate/back-edge fails closed
REOPEN-FLUSH-DROP       cold reopen equals the committed old/new state
```

Each failure must be deterministic and typed. No fallback may mask a corrupted
or stale authority.

## Model-to-adapter result requirements

Memory, RocksDB, and SlateDB must produce identical result digests and decision
classes. Counters must prove the one-view/one-plan/one-commit shape, selector+
epoch precondition, and zero writes for no-op/unsupported/failure cases.

Cold reopen must verify the selector, state root, catalog root, working-diff
floor, and generation digest rather than trusting an in-process cache.

## Terminal rule

The 413e source is a RED calibration until the whole-module gate and this
caller-closure gate are both GREEN. Stop on the first source, model, Memory,
RocksDB, or SlateDB blocker. Do not broaden to init/GC/SQL production paths or
performance measurements from this first migration oracle.
