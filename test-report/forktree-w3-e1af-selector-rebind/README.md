# W3 checkpoint/selector readiness rebind — e1af

Status: **TEST/REPORT-ONLY SOURCE CALIBRATION FROZEN**. This package rebinds
the completed W3 map to exact e1af. It contains no production change and no
compiler, adapter, runtime, or benchmark result.

## Immutable source

- commit: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- parent tree: `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- parent-to-head full-index binary diff SHA-256:
  `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- stable patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`
- changed production paths: exactly
  `packages/lix/src/sql2/providers/file_history.rs` and
  `packages/lix/src/sql2/providers/filesystem_working_diff.rs`
- e1af source blobs:
  - `file_history.rs`: `ae03fe0b8ecbc17d691e197429654c81c535e850`
  - `filesystem_working_diff.rs`: `b08c7b1c8a4c45d35e165f352c64cd4626dbcf32`

The accepted b484 nine-seam oracle remains bound as an acceptance dependency:
head `48a20f14dd1f95062c1900e8b81328cc3dc33199`, tree
`350f5160fb3f3fdc6e2760872c7695c20639c9bd`, terminal review SHA
`ec47a94cfd0d08a6503184e847cfaafe52911ba7d79ba859fa007bd40ce62413`.

## Rebound W3 contract

The full 14-cluster reader-first/writer-last map is preserved at
`/root/repos/evidence/forktree-w3-b484-selector-dependency-map/`. Its sole
destination remains one caller-owned `CoherentView`, authenticated
`GlobalSelectorV1`/`BranchSelectorV1` roots, one global epoch/CAS fence, one
`PreparedPublication`, and one prepare/commit boundary.
The corrected map SHA is
`d9a6653f5f5f62e476d7dac10a7bcb5377d0642d9365cbd330c13e778841e471` and its
14-cluster diagnostics SHA is
`1d6cb84157c64eed06d5e4a3cc6925b645fd2ddab2c28a901a774aaf55d49126`.

The rebind preserves these required semantics:

- checkpoint chronology and checkpoint-floor movement are separate; a
  checkpoint never creates a permanent checkpoint-to-historical-parent edge
  or a later diverged-main bridge;
- branch chronology remains `[S first-parent H, serving C]`, with the
  authenticated bridge to semantically equivalent `C` only;
- GC drains 64 prefix entries plus the suffix (65 total), and a blocked
  advance records one delayed debt without spinning or falsely reporting
  drained; the debt is released only after the reader safe point advances;
- selector publication, snapshot pin/release, checkpoint/recovery, and GC
  observe the same exact raw selector/epoch fence; stale work cannot delete or
  publish partially; and
- W4 owns selector encoding/view identity/publication primitives, while W5
  owns persisted bounded queue/mark/continuation state, safe points, crash
  continuation, sweep, and final-reference reclamation.

No W3 wrapper, compatibility reader, fallback, cache, second root authority,
or independent writer is permitted.

## e1af source-only RED calibration

These are deterministic inventory counts, not compiler or runtime results.
Broad terms include legitimate public names; the forbidden classifier must be
path/owner-aware and must reject the legacy authority symbols and spaces.

| Pattern | e1af matches | Interpretation |
|---|---:|---|
| `BranchHeadControl|TrackedHead|current.?generation` | 58 | legacy control/generation residue remains |
| `checkpoint|recovery|snapshot.?pin|undo|redo` | 1139 | broad checkpoint/history inventory |
| `snapshot.?pin` | 16 | pin publication/retirement inventory |
| `GlobalSelectorV1|BranchSelectorV1|global.?epoch|selector` | 770 | owner plus caller selector inventory |
| `stage_branch_head_control|branch_head_control_precondition|stage_mutation_revision|MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE` | 24 | legacy mutation/revision residue remains |

Expected source gate: **RED** until the legacy control, generation,
recovery-ref, mutation-revision, fresh-read, and independent-writer residues
are deleted or moved to the sole ForkTree owner. This package does not claim
that any broad count is itself a deletion verdict.

## Dormant first-runnable order

Each cell is a fresh isolated target and is capped at 20 minutes. These
commands are recipes only and were not run here. Substitute only the focused
test target named by the first explicitly compile-green successor.

```sh
# static source/deletion gate; no compatibility residue accepted
sh packages/lix/tests/forktree_stage2_residue_gate.sh <candidate-root>
cargo fmt --all -- --check
git diff --check

# Memory
CARGO_TARGET_DIR=<memory-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix --lib <w3_focused_target> -- \
  --exact --nocapture --test-threads=1

# RocksDB, including cold reopen in the same focused target
CARGO_TARGET_DIR=<rocks-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix --features storage-benches --lib <w3_focused_target> -- \
  --exact --nocapture --test-threads=1

# SlateDB, identical fixture and semantic target
CARGO_TARGET_DIR=<slate-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix --features 'storage-benches slatedb' --lib <w3_focused_target> -- \
  --exact --nocapture --test-threads=1
```

The focused target must cover init/create/switch/delete, global sequence and
epoch, stale same-owner versus unrelated-owner publication, no-op and
unsupported intent, rollback/savepoint/idempotency, checkpoint floor and
undo/redo, cold reopen/corruption, then branch-first/GC-first, reader-pin,
65-entry suffix/debt, and final-reference reclamation. Any second read,
fallback, partial publication, unsupported write, or correctness failure
stops the wave before widening.

## Reproduction

Run the source calibration without building or executing production code:

```sh
sh verify_e1af_source.sh /root/repos/lix
```
