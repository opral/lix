# BranchHeadControl / BranchRef whole-closure deletion oracle

This is a test/report-only correction package anchored directly to the
compiler-red b59 frontier. It adds no production code, no Cargo wiring, no
adapter fixture, and no compatibility implementation. Its source gate is
intentionally RED on b59 because the old closure is still referenced while the
defining BranchHead control owner has already been removed from the tree.

This successor closes the blocked 482e oracle's H1 gaps in the pure model and
future source contract: selector fingerprints include every authenticated root,
generation, canonical selector byte string, catalog root, and owner identity;
publication checks distinguish same-owner stale CAS from unrelated-owner
attempts; a forged derived branch-ref authority is rejected; and create,
switch, advance, delete/retire, retained-view GC, and cold reopen are exercised
as state transitions rather than token-presence claims.

## Immutable anchor

```text
commit b59e1f11a51153e0a787a81f0f25bf104d150aaf
tree   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
parent 713455a3557907ce705d06f720fcdc4486bddd4a
```

The oracle must be applied only to a successor descended from this anchor.
`verify_branch_ref_whole_closure.sh` scans all source, compiled-test, and
benchmark roots under `packages` (not only `packages/lix/src`), including
aliases, reexports, wrappers, caches, fallback writers, and second-authority
mentions. It prints deterministic, path-normalized inventories and fails
closed on legacy authority residue.

## Current b59 closure inventory

The inventory is organized by duty rather than by one symbol list. Every listed
duty must move to the same selector/view/publication owner before the old types
and spaces are deleted.

| Duty | Current b59 evidence | Required replacement |
| --- | --- | --- |
| branch reader and cache | `branch/refs.rs`, `branch/context.rs`, `sql2/branch_ref.rs`, `sql2/session.rs` | `ForkTreeReadFacade` over one `CoherentView`; no `BranchRefReader` wrapper/cache |
| control fields | `BranchHeadControl` uses head, tracked/untracked generation, current revision, bloom, checkpoint marker | authenticated `BranchSelectorV1` plus selected `BranchSnapshotV1`; generation/revision/bloom are derived or deleted, never a second selector |
| stage/precondition | `branch/stage_rows.rs`, `stage_branch_head_control`, `branch_head_control_precondition` | `PreparedPublication::from_branch_view`, exact raw selector CAS, one adapter commit |
| initialization | `init.rs` creates global/main controls and branch-ref rows | initialize `GlobalSelectorV1` and branch selector/snapshot objects atomically |
| reader/writer transaction | `transaction/context.rs`, `transaction/bench_support.rs`, `live_state/context.rs`, `functions/*` load or stage controls | one `CoherentView` retained by the operation; one `PreparedPublication` for writes; no mutable control cache |
| lifecycle | `session/create_branch.rs`, `switch_branch.rs`, `checkpoint.rs`, `undo_redo.rs`, merge branch paths | branch selector transitions with exact old bytes, explicit missing/corrupt rejection |
| SQL/session | `sql2/session.rs`, `sql2/branch_ref.rs`, branch/working-diff/checkpoint providers and `lix_branch_ref` surface | selectors are the only branch authority; `lix_branch_ref` is derived read-only projection |
| filesystem/history | directory/file/working-diff/history providers consume `BranchRefReader` and branch controls | same coherent branch/global selector view, or explicit unsupported/fail-closed result |
| GC/reachability | `gc.rs` scans controls, uses control generations/bloom/checkpoint roots and control preconditions | enumerate selector/catalog/object roots, fence exact selector bytes, retain branch/checkpoint/undo roots, delete only after final selector release |
| fixtures/tests | `test_support.rs`, integration branch/checkpoint/GC tests, branch-ref tests | model and future adapter controls; no test-only legacy authority or fixture bypass |
| benchmarks/reexports/spaces | `storage_bench.rs`, engine benchmark sources, `branch/mod.rs`, live-state reexports, built-in `lix_branch_ref` schema | compiler-driven deletion; preserve only derived projection and explicit benchmark ownership |

The b59 tree has `BranchHeadControl`/`BranchHeadControlContext` references but
no defining branch-control file. That is a compiler-cluster input, not proof
that the old closure is gone. The gate reports both missing definitions and
all remaining consumers.

## Replacement authority contract

* `GlobalSelectorV1` is the sole repository-wide mutable selector. It binds the
  repository root, epoch, and selector generation.
* `BranchSelectorV1` is the sole mutable per-branch selector. It binds the
  canonical branch ID, branch snapshot object, and selector generation.
* `CoherentView` authenticates the global selector, branch selector, repository
  root, branch snapshot, and selected objects from one retained `StorageRead`.
  Point/range, history, diff, working-diff, and GC root reads must reuse it.
* `PreparedPublication` stages immutable objects and selector puts/deletes,
  carries exact raw selector expectations from that view, rotates the global
  epoch, and commits exactly once. Construction is not authority.
* Every model `SelectorFingerprint` carries the global and branch selector key,
  global root, branch root, global epoch/generation, branch generation,
  canonical selector bytes, catalog root, global and branch owner identities,
  and deterministic authentication tags.
  Same-size root or owner substitutions invalidate the selector before any
  read or write is returned.
* A same-owner publication whose expected selector bytes are stale returns
  `StaleSelector`; a publication relabeled to an unrelated branch owner returns
  `UnrelatedOwner`. Both fail before mutation. A `DerivedBranchRef` publication
  returns `DualAuthority` and cannot alter the selector view.
* Missing selector is ordinary absence only where the API defines bootstrap;
  malformed, wrong-key, wrong-branch, wrong-root, stale, duplicate, or
  unreachable selector/object data fails closed without fallback or repair.
* `lix_branch_ref` may remain a derived SQL projection for the public read
  surface, but it cannot be loaded, written, or fenced as branch authority. It
  must be generated from the selector view.

## Lifecycle and race oracle

The pure model covers create, switch, advance, delete, retire, undo, redo,
checkpoint-like rotation, GC, retained reader snapshots, cold reopen, and
complete state/selector fingerprints. Every selector-changing operation is one
prepared publication and one commit; session switch is read-validated state
only; GC maintenance is separately fenced and never a second selector
authority. The required future adapter controls include:

1. create/switch/delete with stale selector CAS in both publication orders;
2. undo/redo and checkpoint while an old `CoherentView` is retained;
3. branch deletion versus GC, including final-reference reclamation and a
   branch whose checkpoint/working interval remains reachable;
4. global publication versus branch publication and GC-first versus
   publication-first ordering;
5. malformed/missing selector, malformed/noncanonical branch identity, wrong
   embedded branch ID, wrong root, generation regression, cycle/missing object,
   and forged derived `lix_branch_ref` rows;
6. empty undo/redo and failed multi-row operations must be no-op: no selector
   rotation, write, commit, or partial state;
7. flush/drop/cold reopen on Memory, RocksDB, and SlateDB, including global
   sequence validation and fingerprint equality.

The corrected model also requires the future adapter lane to report the exact
selector byte/authentication fingerprint and owner identity in every CAS
failure, and to prove that an unrelated-owner CAS fails without a backend
write. A forged `lix_branch_ref` row is a negative input only; it is never a
selector read or write source.

No race may publish a root selected from a different view. No corruption case
may write, rotate a selector, or fall back to the old control/row space.

## Explicit exclusions

This package does not implement or qualify the direct SQL entity/PK/columnar
reader lane, row-group physical deletion, binary CAS GC, upload ownership, or
current-main performance. Those lanes have separate owners and must not be
silently included in this closure.

## Future gate

Run the exact order in `FUTURE_GATE_COMMANDS.md`: source verifier, formatting
and diff checks, standalone model, candidate compiler/negative consumers, then
Memory → RocksDB → SlateDB. Every adapter cell is capped at 20 minutes and
the sequence stops at the first blocker.
