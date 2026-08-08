# Checkpoint branch-bridge source review: APPROVE

## Immutable candidate

- Rejected parent preserved as evidence: `a39cb9e893ed13ab0a24644461feb491b521d012`
- Parent tree: `8cb09248198d99af21305ec655d593e4b305ccfb`
- Reviewed head: `743ab59da3eec3822fdcfb89237613552d4ac2d1`
- Reviewed tree: `fa020ef01c81ea40efc5980e4b705876affb6207`
- Remote ref: `origin/codex/checkpoint-ancestry-merge-fix`
- Parent..head full-index SHA-256: `c266e80ec254c7973e849782d8ebe31bb8d28fdf546dfe39c1df1f7a09c90dc3`
- `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3..head` full-index SHA-256: `890f4e12f9d53571cca732c524b1fafd10260ecaa2a0f3035f193c512df4959f`
- Stable patch ID: `9c0fad19b8ea25a8d3e7b30fc939264f0820e557`
- Changed production paths only:
  - `packages/lix/src/transaction/commit.rs`, blob `26028af81e040257950bc68bff59276f3ad7cb77`
  - `packages/lix/src/transaction/context.rs`, blob `a5230c90d282b31deeeff072b0354eeaa54451ff`
- Detached review worktree was clean and `git ls-remote` resolved the reviewed ref to the exact head.
- `git diff --check a39..head` passed.

## Verdict

**SOURCE APPROVE.** The successor corrects the rejected ownership error without creating a second checkpoint publication path. A historical `create_branch(from H)` resolves an authenticated pending `H -> C` replacement on the transaction's existing read, returns a typed branch-only bridge, and consumes it only while lowering the explicit new branch publication. The complete root-backed branch control, empty working-diff epoch, recovery row, root delta, and queue CAS are staged in the same `StorageWriteSet` and commit boundary.

This is a source/correctness ownership verdict, not an independent runtime verdict. The author reported production `cargo check` green; the focused owner-test build was resource-killed before execution and remains for the runtime qualifier.

## Authority and atomicity trace

1. `session/create_branch.rs:81-94` stages only the existing branch descriptor/ref rows and records a replacement-resolution request for an explicit historical source.
2. `transaction/context.rs:1392-1489` resolves the existing authenticated queue proof on the retained transaction read, validates semantic equivalence and the checkpoint's typed branch marker, rejects overlap with a genuine checkpoint publication, and returns `BTreeMap<branch_id, CheckpointRecoveryRef>`. Unlike rejected `a39`, it does not append `CheckpointPublication` and does not load or mutate checkpoint GC state.
3. `transaction/context.rs:1784-1867` carries that typed map directly into the ordinary commit lowerer while preserving the same transaction read.
4. `transaction/commit.rs:4942-5038` uses the existing root-backed branch publisher to stage a complete current base and `BranchHeadControl` for recovered `H`.
5. `transaction/commit.rs:4908-4940, 5211-5225` binds the bridge only to a new explicit branch whose target/control both equal `H`; it rejects deletion, an existing branch, mismatched branch/head, `H == C`, empty interval, or a pre-bound checkpoint. It then stages only the existing empty `TrackedWorkingDiffEpoch` and existing `CheckpointRecoveryRef` row and sets the control's baseline to `C`.
6. `transaction/commit.rs:5250-5289` computes `RootReachabilityDelta.new_control` from that already-modified control and stages the same control. `transaction/commit.rs:772-790` passes the resulting delta to the existing `stage_reachability_delta_batch`, whose exact queue-row precondition fences concurrent queue advancement. No bridge checkpoint is added to `checkpoint_roots`; those remain derived solely from real `CheckpointPublication` values.

The bridge therefore has one owner and one atomic write: branch ref/descriptor + complete root-backed control/current base + empty epoch/recovery row + root delta/queue CAS. There is no commit/HOT-member synthesis.

## Forbidden-path audit

- No `CheckpointPublication` is constructed by branch replacement resolution. The only changed call boundary carries a separate internal map into branch-control lowering.
- Genuine checkpoint validation at `transaction/commit.rs:4781-4855` is unchanged and still requires a complete HOT control with an exact checkpoint baseline.
- No storage format, namespace, space, public API, migration, compatibility decoder, fallback, retry, or secondary writer is added. The diff modifies only the two transaction files listed above.
- No queue object/state is exposed to merge, history, or commit-graph readers. Queue proof resolution remains GC-owned. The recovery row and control are serving context consumed by `attach_checkpoint_branch_parents` for the first ordinary commit; the resulting ordinary graph parent is what later merge/history readers consume.
- Every bridge must correspond to exactly one explicit branch publication; checkpoint/bridge overlap and unconsumed bridges fail closed.

## Required controls

- Valid branch and atomic serving context: successor test `branch_creation_owner_publishes_checkpoint_serving_context` verifies complete control at `H`, baseline `C`, root-backed publication, `None -> H` delta carrying the exact control, queue fencing, persisted empty epoch, recovery resolution, and one atomic commit.
- Missing/consumed proof: inherited `recovery_ref_without_pending_replacement_is_not_branchable_authority` proves a recovery row alone cannot authorize branch creation and returns commit-not-found.
- Malformed proof: inherited `malformed_pending_checkpoint_replacement_fails_closed` rejects a replacement lacking the exact checkpoint baseline.
- Ambiguous proof and stale queue: inherited `pending_checkpoint_replacement_is_unique_and_queue_cas_fenced` authenticates one candidate, rejects two candidates as ambiguous, and proves a stale publisher loses the queue CAS.
- Genuine checkpoint missing HOT: successor test `real_checkpoint_without_complete_hot_control_still_fails` directly preserves the complete-HOT invariant and checks the fail-closed error.
- Bridge-shape guards: `bind_branch_checkpoint_bridge` rejects deletion, existing-branch use, branch/head mismatch, degenerate interval, and preexisting baseline; branch-control lowering rejects real-checkpoint overlap and bridges without explicit publication.

## Remaining integration/runtime gate

No source blocker remains at this immutable head. Before integration, the runtime owner should execute the focused public `create_branch(from recovered H)` lifecycle on RocksDB and SlateDB, including missing/malformed/ambiguous proof and stale-queue race controls, plus the real-checkpoint-missing-HOT regression. This review intentionally did not build or run tests under the read-only assignment.
