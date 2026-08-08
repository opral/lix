# ForkTree selector authority acceptance package

Status: TEST/REPORT-ONLY, source-calibrated RED. No production source, Cargo
wiring, storage data, PR, or ref outside this package is changed.

## Immutable provenance

```text
anchor head: 705440f55eccba9e2d55c0951d6a684737005d76
anchor tree: 2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d
anchor parent: 9f3c703e953440cde1d60b1511467c4337648c8f
selector contract supplied by manager: SELECTOR_CONTRACT.md
selector contract SHA-256: ff784043429f563fb01a29c42eecc90a939f7ce8ac7926d9db07a0f13313da24
```

The contract file named by the manager was not present in the local evidence
or repository files, so this package binds to its supplied digest and restates
only the acceptance obligations in the assignment. It does not claim a local
copy of that document.

## Authority map

| Fact | Sole intended authority | Source evidence on 705 | Acceptance invariant |
| --- | --- | --- | --- |
| repository-wide selected root/epoch | `GlobalSelectorV1`, `SELECTOR_SPACE` key `global` | `forktree/model.rs:620-664`, `view.rs:155-180` | authenticated decode; nonzero root and generation; every publication exact-CASes old bytes and rotates epoch |
| selected branch snapshot | `BranchSelectorV1`, `SELECTOR_SPACE` key `branch/<canonical UUID>` | `model.rs:666-711,1846-1851`, `view.rs:155-185` | key/id agreement; authenticated snapshot/root closure; same branch only |
| root/object closure | `RepositoryRootV1`, `BranchSnapshotV1`, authenticated object space | `view.rs:188-248`, `serving.rs` object loaders | missing, wrong-kind, hash, or malformed child fails closed |
| chronology | commit object parent list / semantic head | `publication.rs:40-54,650-868`, transaction commit path | selector generations, recovery refs, and checkpoint rows never become merge/history parents |
| checkpoint/undo floor | serving checkpoint projection and first-parent walk | `gc.rs:93-161`, checkpoint history helpers | floor bounds undo/recovery traversal; cycles, missing parents, and unreachable floor fail closed |
| atomic publication | `PreparedPublication` | `publication.rs:68-180,1061-1145` | one retained view → one plan → one backend commit; no partial selector/object publication |

`BranchRefReader`, `lix_branch_ref`, `lix_branch_descriptor`, and the
`lix_undo_redo_marker` are not accepted as a second selector/chronology owner.
Their remaining public call paths are the source RED items below.

## Source calibration on 705

The source verifier records these positive controls:

- `GlobalSelectorV1` and `BranchSelectorV1` have authenticated canonical
  encode/decode and reject zero roots or zero generations.
- `open_coherent_view_on_read` acquires one caller-owned read, loads both raw
  selectors in one `get_many`, validates selector identity, then loads the
  authenticated repository root and branch snapshot while retaining that read.
- `PreparedPublication::from_branch_view` fences both global and branch raw
  selector bytes; `from_global_epoch` rotates the global epoch/generation;
  `into_storage_plan` emits one CAS/write plan.
- GC/control code keeps chronology roots separate from serving checkpoint
  roots and rejects missing-parent/cycle walks in its source-level validators.

The verifier intentionally emits SOURCE RED for current integration residue:

1. `session/create_branch.rs:41-92` creates a branch through
   `branch_descriptor_stage_row` + `branch_ref_stage_row`, not a
   `BranchSelectorV1` publication.
2. `session/switch_branch.rs:35-118` validates through `BranchRefReader`; the
   workspace case stages a `lix_key_value` selector row rather than a
   `BranchSelectorV1` move.
3. `branch/stage_rows.rs:37-78` retains branch-ref stage/tombstone writers,
   so branch delete/retirement is not yet proven to be the sole selector
   authority.
4. `session/undo_redo.rs:12-40,200-337` persists
   `lix_undo_redo_marker`; no source-level bridge proves undo/redo selector
   moves use the ForkTree snapshot selector without a second durable marker
   authority.

These are acceptance blockers, not proposed edits in this package. The
compiler-red frontier is not a runtime waiver.

## Discriminating acceptance matrix

The future candidate must add or expose test-only counters/barriers, while
preserving public semantics. Every case runs on Memory first, then RocksDB and
SlateDB, with no direct physical-file mutation.

| ID | Scenario | Required result |
| --- | --- | --- |
| A1 | initialize/reopen with valid global + branch selectors | one coherent view; raw selector bytes, decoded roots, branch id, and view id agree |
| A2 | create branch from current head | descriptor/catalog and one authenticated branch selector publish atomically; no branch-ref selector duplicate |
| A3 | switch pinned and workspace branch | target selector is authenticated; unrelated owner cannot rewrite it; workspace state is serving context only |
| A4 | delete branch | exact old branch selector CAS and catalog/closure retirement publish together; second delete fails closed; final objects become reclaimable only after root release |
| A5 | same-owner stale writer | stale global or branch raw selector rejects with storage precondition failure; no object/selector partial write |
| A6 | unrelated-owner writer | a mismatched branch key/id or owner cannot satisfy another branch’s expectation; it must not be accepted as the target selector |
| A7 | chronology vs checkpoint | selector generation/epoch and checkpoint floor never appear in commit parents or merge-base input; first-parent undo stops exactly at the floor |
| A8 | undo → redo → reopen | selector/root and semantic chronology remain stable; redo cursor survives reopen; no marker-only alternate authority |
| A9 | missing/malformed global selector | view acquisition fails typed corruption/unavailable before any state row or publication |
| A10 | missing/malformed/wrong-id branch selector | same fail-closed result; no fallback to branch-ref/control/current cache |
| A11 | missing/wrong-kind repository root or branch snapshot | authenticated child closure fails before serving; no partial view |
| A12 | missing commit, wrong parent, parent cycle | history/checkpoint/undo traversal returns typed corruption; no omission, reset, or guessed floor |
| A13 | publication crash/error before/after backend commit | reopen is entirely old or entirely new; selector, root, catalog, and checkpoint floor never form a mixed view |
| A14 | one retained view across publication + GC | old view continues to authenticate its bound roots; new view sees new selectors; GC cannot delete pinned objects |
| A15 | selector byte/key/domain substitution | canonical key, authenticated domain, UUID, generation, and root binding all reject substitution |

For each failure, assert zero writes/deletes/selector rotations after the
failure and no read of a legacy owner. A successful publication records one
`begin_read`, one retained view identity, one prepared plan, one backend commit,
and exact old/new selector byte digests.

## Required model/public test names

`selector_authority_model.rs` is a dependency-free, intentionally unwired
model for the above state machine. The eventual public/internal test names are
fixed here for review:

```text
selector_codec_and_key_binding_rejects_malformed_missing_and_wrong_owner
selector_publication_is_atomic_and_exact_cas_fenced
branch_lifecycle_uses_one_selector_authority_across_create_switch_delete
checkpoint_floor_is_serving_context_not_commit_parent
undo_redo_reopen_preserves_selector_and_chronology_semantics
selector_corruption_missing_parent_and_cycle_fail_closed
coherent_view_survives_publication_and_gc_without_partial_visibility
```

## Qualification commands (not run)

No runtime/build command is claimed on compiler-red 705. Once the production
wave is source-complete, the smallest ordered commands are:

```text
cargo test -p lix --lib forktree::tests::selector_codecs_have_single_edges_and_canonical_keys -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::full_selector_scan_crosses_storage_page_and_corruption_fails_closed -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::publication_cancels_active_gc_without_becoming_a_global_writer_lock -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::deterministic_reader_pin_safe_point_and_cursor_oracle -- --exact --nocapture --test-threads=1
cargo test -p lix --lib session::undo_redo::tests::checkpoint_is_an_undo_floor -- --exact --nocapture --test-threads=1
cargo test -p lix --lib session::undo_redo::tests::redo_cursor_is_durable_across_fresh_sessions -- --exact --nocapture --test-threads=1
```

Run the package's source verifier first. Runtime acceptance requires Memory,
RocksDB, and SlateDB results with exact selector/view/write digests; this
package makes no runtime or performance claim.
