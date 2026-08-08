# BranchHeadControl semantic acceptance oracle — exact b59

Status: FROZEN TEST/REPORT-ONLY ORACLE.

No production edit, build, adapter run, backend runtime, or benchmark was
performed. The oracle is complementary to the BranchHeadControl deletion
inventory and is bound to the exact accepted historical ForkTree source line.

## Immutable source identity

    remote: https://github.com/opral/lix.git
    ref: refs/heads/codex/forktree-stage2-historical-fail-closed-713
    head: b59e1f11a51153e0a787a81f0f25bf104d150aaf
    tree: 700fd04d21bc40c05425c9fc9e10d65c9e1eda24
    parent: 713455a3557907ce705d06f720fcdc4486bddd4a

Independent git ls-remote and a read-only shallow source export reproduced the
exact head/tree/parent. Relevant source blob IDs were independently verified:

    4a78d26d6c09a7c91f5316e9dea8ac20958c94fd  packages/lix/src/forktree/serving.rs
    7b87292e1b837aee47c9cc56265140667ff074f3  packages/lix/src/forktree/view.rs
    97e0bc132e96d726f29970b459942332e33ed748  packages/lix/src/forktree/tests.rs
    8e189f17aceb1fb0731824eaee643001b16085c6  packages/lix/src/handle.rs
    b02979e39aede783c0cab0b61c74c7a536d8b020  packages/lix/src/storage_bench.rs
    edf265838b3fd86eb7392c49a930794666b6250e  packages/cli/src/commands/exp/git_replay.rs

## Current b59 source map

The authenticated selector owner already exists in:

    forktree/model.rs:620-701
      GlobalSelectorV1 { repository_root, epoch, selector_generation }
      BranchSelectorV1 { branch_id, branch_snapshot_object_id, selector_generation }

    forktree/publication.rs:51-150, 898-1011, 1115-1198
      one PreparedPublication, exact global selector fence, branch selector put/delete

    forktree/view.rs:380-485
      one retained read, selector-pair authentication, root-envelope validation

    forktree/reachability.rs:160-190, 340-380
      authenticated global and branch selector enumeration for reachability

The intended replacement must keep those owners and route branch lifecycle,
transaction fences, checkpoint/undo/redo, and GC root validation through them.

The b59 source still contains the superseded control plane. Static inventory
from the exact b59 source export is RED:

    forbidden token                                  occurrences
    BranchHeadControl                                34
    BranchHeadControlContext                         35
    stage_branch_head_control                        32
    branch_head_control_precondition                  6
    BranchHeadControlCache                            13
    BranchHead                                       96
    BranchRefReader                                 117
    BRANCH_REF_SCHEMA_KEY                            25
    branch_ref_stage_row                              8
    branch_ref_tombstone_row                          4
    tracked_generation                               24
    untracked_generation                             28
    current_state_revision                           22
    working_diff_checkpoint_commit_id                12
    schema_presence_bloom                            12

Required selector-owner tokens remain present:

    GlobalSelectorV1                                19
    BranchSelectorV1                                 22
    global_selector_key                              16
    branch_selector_key                              17
    PreparedPublication                              61

Verifier output SHA-256:
c94b1578dda3cfe41e4ae2a39ad9d7816af8da8c3abd042589e647912c140220

The exact b59 source gate exits 1 by design; this is baseline attribution, not
a candidate acceptance.

Primary legacy call-site clusters are init.rs, gc.rs, live_state/context.rs,
functions/context.rs, functions/state.rs, transaction/context.rs,
transaction/bench_support.rs, session/execute.rs,
sql2/providers/working_diff.rs, tracked_state/context.rs, branch/refs.rs,
and test_support.rs. These are the future migration/deletion boundary. No
scalar, CAS, storage format, merge-analysis, or unrelated ForkTree owner is
admitted by this gate.

## Pure model contract

branch_head_control_model.rs is a dependency-free deterministic model. It does
not define a production format. It requires:

1. init atomically creates one authenticated global selector and the main
   branch selector; no half-initialized repository is observable.
2. Global selector identity includes repository root, epoch, generation, and a
   monotonic observed publication sequence. Branch selector identity includes
   branch id, snapshot object, and generation. Sequence is derived observation,
   never a second durable authority.
3. Unrelated branch owners may publish after another branch advances the global
   sequence. A same-branch transaction with an old selector identity or
   generation rejects as stale with an unchanged state digest.
4. Create, switch, and delete preserve public branch semantics. Duplicate,
   missing, active, and main-branch deletion cases reject atomically.
5. Undo and redo move the selected branch through authenticated history;
   checkpoint establishes an undo floor. Crossing the floor, empty redo, and
   stale ownership reject without partial publication.
6. A branch-first race advances the global epoch and makes an old GC ticket
   reject before deletion. A GC-first race may complete, after which a branch
   publication remains live and cannot be reclaimed by that completed sweep.
7. Missing, malformed, wrong-kind, zero-identity, and mismatched selectors or
   controls fail closed. Cold reopen validates the same selector identity.
8. Every accepted publication advances global order exactly once; rejected and
   no-op operations do not advance it.

## Future adapter acceptance

The exact Memory, RocksDB, and SlateDB commands are frozen in RUN_COMMANDS.md.
They are not current b59 evidence. Before running them, the successor must pass
the source gate, package no-run, and warnings-denied Clippy on one immutable
head.

Each adapter must replay the same scenario and emit exact semantic state
digests plus begin-read/write, get/scan, key/row/byte, retry/CAS, object,
physical-byte, allocation/RSS, and immediate/settled-disk counters. The Memory
result is the correctness oracle; RocksDB and SlateDB are the only durable
adapter gates. No SQLite path, cache, fallback, legacy control writer, second
selector, or compatibility reader is accepted.

Terminal status for this assignment: FROZEN / BASELINE RED, successor gate ready.
