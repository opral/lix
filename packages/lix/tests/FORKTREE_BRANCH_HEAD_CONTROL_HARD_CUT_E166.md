# BranchHeadControl hard-cut contract

Status: design and source map only. This artifact is not a production change,
does not authorize a compatibility reader or migration, and does not create a
second selector or branch-state authority.

## Immutable review base

- reviewed head: `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
- reviewed tree: `c680bd7e7f7b70cd784676515839af2dcbbc7917`
- parent: `3def82e48ed74ab3d914867767e3bf06def3ffc2`
- subject: `stage2: delete dead live-state hot scan helpers`
- last materialized BranchHeadControl owner inspected for field/codec mapping:
  `7f4786a12848411758f78202f1c105b17c3ce541`
- last owner blob:
  `packages/lix/src/branch/control.rs` /
  `05dcc43463d32adc21e8c6e3c9a72c0d04dc4896`

At `e1666e`, `branch/control.rs` has already been deleted while its imports,
callers, and tests remain. That is a useful compiler boundary, not an
accepted implementation: the first runnable state must replace every caller
below and then delete the residual names and old space before compiling.

## Sole replacement authority

The replacement is the already accepted ForkTree owner in this exact tree:

```text
SELECTOR_SPACE = engine-declared mutable selector space

global key:         b"global"
branch key:         b"branch/" || canonical raw BranchId UUID bytes

GlobalSelectorV1 {
    repository_root: ObjectId,
    epoch: u64,
    selector_generation: u64,
}

BranchSelectorV1 {
    branch_id: CanonicalBranchId,
    branch_snapshot_object_id: ObjectId,
    selector_generation: u64,
}

BranchSnapshotV1 {
    branch_id: CanonicalBranchId,
    local_state_root: ObjectId,
    semantic_head_commit_object_id: ObjectId,
    latest_ref_change_object_id: Option<ObjectId>,
    historical_global_state_root: ObjectId,
}
```

`GlobalSelectorV1` is the only repository-wide selector and epoch fence.
`BranchSelectorV1` is the only durable per-branch selector. The authenticated
`BranchSnapshotV1`, CommitCatalog, ChangeCatalog, and RefChange object carry
the semantic facts; neither a selector nor a cache stores a duplicate
`ref_change_id`, current row, generation root, schema bloom, or branch head.

`open_coherent_view` performs one `begin_read`, one same-handle selector
`get_many`, and same-handle root authentication. Its raw selector bytes and
`view_id` remain attached to every later read and publication. A branch
publication starts with `PreparedPublication::from_branch_view`; a global,
untracked, root-only, upload, or GC publication starts with
`PreparedPublication::from_global_epoch`. The resulting object/selector puts,
deletes, exact raw preconditions, and rotated global selector are one adapter
commit.

## Field and responsibility deletion map

| old control responsibility | sole replacement | hard-cut rule |
| --- | --- | --- |
| `head_commit_id: CommitId` | `BranchSnapshotV1.semantic_head_commit_object_id`, validated through CommitCatalog and the authenticated Commit object | No direct control record or cold ID lookup. |
| `tracked_generation: CommitId` | selected global/local state roots in `CoherentView`; the branch selector generation is only a selector fence | Do not persist a second current-serving generation or use it as a row authority. |
| `untracked_generation: CommitId` | untracked-only rows remain their explicitly independent semantic owner; their publication is fenced by the same global epoch and relevant branch selector CAS | No derived generation object, branch-control row, or fallback reader. |
| `current_state_revision: u64` | `BranchSelectorV1.selector_generation` for branch-visible mutations, plus `GlobalSelectorV1.epoch` for every root/object/root-set mutation | Increment and exact-CAS in the same write; pure no-op/read does neither. |
| `working_diff_checkpoint_commit_id` | typed `SnapshotSelectorV1`/`SnapshotTargetV1` checkpoint/recovery/undo/redo roots and authenticated root-to-root diff | Delete working-diff marker/control storage; never keep this field as a shortcut. |
| `created_at`, `updated_at`, `ref_change_id` | immutable `ChangeObjectV1::BranchRef` in the unified ChangeCatalog, reached from `BranchSnapshotV1.latest_ref_change_object_id` | Only the RefChange object owns the public ID and chronology. |
| `schema_presence_bloom` | authenticated state/catalog reads and rebuildable, non-authoritative computation | Delete the bloom from the durable control and do not serve a stale skip. |
| `BranchHeadTrackedReachability` | selector-root traversal: global selector, every branch selector/snapshot, retained snapshot selectors, upload selectors, and GC progress | GC never interprets a control field or invents a root table. |
| `BranchHeadControlObservation.raw_token` | `CoherentView.raw_global_selector`, `raw_branch_selector`, and `view_id` | A stale raw selector fails the owner CAS; no second read reconstructs it. |
| `BranchHeadControlCache` | one transaction-owned `CoherentView`/typed selector snapshot bound to one `StorageRead` | Delete the mutex/BTreeMap cache; it cannot pin or authorize a durable view. |

The public `BranchHead`/`BranchRefReader` facade may remain as API vocabulary,
but its implementation must delegate only to `ForkTree::load_branch_head` and
the authenticated selector scan. It is not permission to retain the old
control codec or space.

## Exact production caller map

The following are the direct residual consumers at `e1666e`; function names
and line anchors are source-map anchors, not a permission to preserve the
calls.

| source | current use | required owner after cut |
| --- | --- | --- |
| `branch/refs.rs:87-203` | tests stage and read a direct control | Tests use typed selector/object publication and `BranchRefReader` only; delete direct staging. |
| `functions/context.rs:129,359-400` | persists global deterministic/untracked lifecycle state and stages a global control | Load one coherent global view; put the untracked-only sequence row through `from_global_epoch`; rotate epoch in the same plan. |
| `functions/state.rs:7,81-137,144,539-580` | observes control bytes, derives lifecycle generation, stages global control, and creates a precondition | Replace observation with retained raw global selector/view; sequence mutation uses the one typed publication and exact epoch CAS. |
| `init.rs:6,112,150,254-288,501` | constructs global/main `InitBranchHeadControl` records and writes the old space | Bootstrap RepositoryRoot, global selector, main BranchSnapshot/BranchSelector, RefChange/catalog edges, and untracked seed in one typed plan. |
| `live_state/context.rs:7,35-47,354-432,607-980` | point/list current-state reads, scans controls, and maintains a 64-entry generation cache | Read through one `CoherentView`; selector enumeration is owner-side and same-read; delete `BranchHeadControlCache`, `load_branch_head_controls`, and ID scans. |
| `live_state/context.rs:1245-1317,1654-1733` | cache pinning and lifecycle test/writer fixtures | Replace with stale-view/no-op/selector-CAS tests and typed `PreparedPublication`; no raw control fixture. |
| `transaction/context.rs:25,557,916,1515,2425,2710,6522,6796-7427,8143-8307,8926-8959` | threads the cache through transaction readers and directly observes controls | Store the transaction's exact coherent view/raw selectors; remove cache parameters and lower commit preconditions through `PreparedPublication`. |
| `session/execute.rs:8675` | reads all controls after execution | Observe the committed ForkTree selector/root through the same transaction/publication result; never scan a control space. |
| `sql2/providers/working_diff.rs:11,145` | reads a direct control to form a diff | Diff selected authenticated roots against transaction-local state; no working-diff/control fallback. |
| `gc.rs:15-16,100-546,786,1346,2054-2136,2532` | scans controls, projects reachability, and stages control preconditions | `reachability` scans typed selector roots and uses the raw global epoch fence; branch snapshot/ref edges are authenticated object edges. |
| `gc.rs:3441-8114` | dozens of control fixtures for checkpoint, queue, branch, upload, stale, and reclamation tests | Rewrite as typed selector/object fixtures; retain both race orders, final-reference release, and corruption fail-closed tests. |
| `storage_bench.rs:2214` | lists `BRANCH_HEAD_CONTROL_SPACE` as a physical space | Delete the space from layout/metrics; report SELECTOR_SPACE/object/maintenance bytes instead. |
| `transaction/bench_support.rs:8,377,605-617` | benchmark reads and stages controls | Use typed view/publication setup or remove obsolete benchmark support. |
| `test_support.rs:24,171-180` | generic test repository seeds a control | Seed through the typed init/publication owner. |

Branch lifecycle APIs in `session/create_branch.rs`, `switch_branch.rs`, and
`merge/branch.rs` currently validate refs through `BranchLifecycle` and
`BranchRefReader`. Keep those public semantics, but make the reader resolve
the authenticated BranchSelector/Snapshot/RefChange graph on the transaction's
opening `StorageRead`. Create/delete/rename/merge publication must not consult
the removed control as a second validation source.

## One publication contract

1. Acquire the exact global and requested branch selectors through one
   `StorageRead`; authenticate all selected roots and derive `view_id`.
2. Classify intent before constructing a plan. A pure read, session-local
   branch switch, or true semantic no-op has zero writes, zero CAS, and zero
   epoch rotation. An unsupported cohort fails before any plan/write.
3. For branch-visible state/ref changes, construct one
   `PreparedPublication::from_branch_view`. It exact-CASes both raw global and
   raw branch bytes. Path-copy only the changed state/catalog/snapshot objects,
   install the BranchSelector, and rotate GlobalSelector. Do not copy or touch
   unrelated branch selectors.
4. For global state, global deterministic-sequence/untracked publication,
   root-only checkpoint/undo/redo, upload, or GC, construct
   `from_global_epoch`. Exact-CAS the raw global selector and rotate its epoch;
   add the typed selector/object updates to that same adapter transaction.
5. A deduplicated object/root-only publication still rotates the epoch because
   it changes reachability or selector state. A no-op does not.
6. On CAS failure, return stale/serialization failure without fallback or
   partial writes; reopen a new coherent view and restage only through the
   typed owner. Malformed, missing, wrong-domain, mismatched-key, or missing
   RefChange/catalog edges fail closed.

The global epoch is an ordering fence, not a long-lived lock. An unrelated
branch's durable publication may make an older writer stale because all root,
selector, upload, and GC mutations share the one epoch. That serialization is
intentional and does not permit O(branches) copying. Session-local switching
does not publish and therefore does not conflict or rotate anything.

## Lifecycle mapping

- Initialize: create only the new object/selector format. Publish the global
  RepositoryRoot/GlobalSelector and the initial main BranchSnapshot/Selector,
  RefChange, catalogs, and untracked seed atomically. Pre-cut bytes fail the
  repository protocol check.
- Create branch: read source and global selectors from one view; create a
  BranchSnapshot with local root, selected semantic head, historical global
  root, and a BranchRef Change object; add the ChangeCatalog entry and install
  the new BranchSelector under exact absence plus global epoch CAS. No
  branch-wide global update.
- Switch branch: validate the target selector/object graph on the opening
  read. A pinned/session switch is in-memory only. If workspace selection is
  durable, publish the untracked-only row with the global epoch fence; do not
  mutate the target branch selector merely to switch a session.
- Branch update/merge: path-copy only the selected local/global root and
  catalogs, append semantic and RefChange objects, and move one BranchSelector
  plus the global selector in one commit.
- Delete/retire branch: publish a typed branch-tombstone snapshot selector and
  RefChange/retention edge, then remove the live BranchSelector only when the
  same proof releases branch/history/checkpoint/recovery/undo/redo/ref roots.
- Checkpoint/undo/redo/recovery: use typed SnapshotSelector/Target roots, not
  `working_diff_checkpoint_commit_id` or a control row.
- GC: root from the authenticated selector universe. The exact raw global
  selector/epoch fences mark progress and sweep; a stale sweep cannot delete
  objects published by a concurrent branch, sequence, upload, or root-only
  writer.

## Race and edge semantics

| case | required result |
| --- | --- |
| two writers from the same branch view | exactly one raw branch+global CAS wins; the other is stale with no partial write |
| global writer versus branch writer | one global epoch wins; the loser reopens and restages, never overwrites the winner |
| writers on unrelated branches | older epoch is stale even without branch-wide copying; both branch roots remain intact |
| branch creation versus creation | exact absence precondition allows one selector; the other fails closed |
| branch delete versus update/merge | selector CAS/retention edge decides; no use-after-delete or resurrected old control |
| checkpoint/GC versus publication | stale global epoch aborts maintenance/publication; retry uses a fresh typed root snapshot |
| control/selector missing or malformed | branch absence is only an absent exact selector; present malformed selector, snapshot, commit, catalog, or RefChange is corruption |
| same head/ref/no-op publication | no object/selector write and no epoch rotation |
| unrelated semantic owner | must not be routed through BranchHeadControl; only the typed owner for that state/selector family may stage it |

## Compiler-driven deletion order

The order is deliberately reader-first and writer-last. No intermediate state
is runnable or publishable.

### W4: install and prove the fence first

Use the existing typed `CoherentView` and `PreparedPublication` boundary. Add
owner-local tests for raw global/branch equality, generation/epoch overflow,
no-op zero rotation, branch absence, stale global-first and branch-first
writers, unrelated-branch serialization, and malformed selector/object graphs.
No caller may receive a raw `StorageSpace`, selector bytes, or generic put/
delete capability.

### R1: migrate readers and observations

Move `branch/refs.rs`, `live_state/context.rs`, transaction reader setup,
session execution, SQL working-diff, lifecycle validation, and GC root
observation to one same-read selector/object owner. Delete `BranchHeadControl`
decode/read/scan APIs and the cache only after all readers are moved. A reader
must never fall back to the control space or an empty-success result.

### W1-W3: migrate writers in one non-runnable wave

Move init, ordinary transaction publication, untracked deterministic sequence,
branch create/delete/switch/merge, checkpoint/undo/redo, and upload/root-only
publication to the one typed plan. Every path must classify intent first and
use exactly one prepared adapter commit. Remove direct `stage_branch_head_control`,
`stage_delete_branch_head_control`, and `branch_head_control_precondition`.

### W5: migrate reachability and delete physical owner

Move GC root enumeration and sweep to selector/object closure, including reader
pin and retention roots. Then delete `BRANCH_HEAD_CONTROL_SPACE`, namespace,
codec/key types, `BranchHeadTrackedReachability`, `BranchHeadControlObservation`,
the control module, the cache, old tests/benchmarks, and all residual imports.
The first accepted compile is after this deletion, not between reader and writer
steps.

## Dual-adapter acceptance gates

Run each focused cell separately on Memory, RocksDB, and SlateDB where the
existing owner supports Memory; the landing gate requires both RocksDB and
SlateDB. No broad benchmark is a substitute.

1. Static residue: zero old control names/space/codec/reader/writer references
   outside an explicit historical report; only public BranchHead/Reader facade
   names may remain, and they must delegate to ForkTree.
2. Initialize, flush/drop/reopen: exactly one global selector and one main
   branch selector; pre-cut bytes fail protocol validation.
3. Branch create/switch/delete/rename: exact absence/CAS, authenticated
   RefChange chronology, no O(branches) update, session-local switch remains
   non-publishing.
4. Transaction and global-sequence publication: one coherent view, one plan,
   one prepare, one adapter commit; no-op/unsupported zero-write; stale and
   rollback do not rotate epoch.
5. Current read and history: branch/global state, NULL/tombstone, point/range,
   commit/change ordering, diff/merge, undo/redo, and cold readback are byte
   identical across adapters.
6. GC: every branch/checkpoint/recovery/undo/redo/upload/reader-pin root is
   retained; publication-first and GC-first races fail stale; final selector
   release reclaims the old root and never the surviving branch.
7. Corruption: malformed/missing selector, wrong key/branch ID, wrong domain,
   missing snapshot/commit/catalog/RefChange edge, stale epoch, and partial
   publication all fail closed with no fallback or repair.
8. Metrics: report selector/object puts, logical/backend reads and writes,
   and settled disk. The removed control space must contribute zero bytes and
   zero calls.

## Big-O and rejection criteria

Branch selector lookup is `O(log_F B)` authenticated reads (or one bounded
point get for the selector plus root traversal); branch publication is
`O(U log_F N + Z + log_F M)` for changed state/catalog paths. Global sequence or
untracked-only publication is `O(U + Z)` plus one selector CAS. GC selector
root enumeration is paged `O(B + selectors)` with bounded page memory, and
object reachability remains `O(R + O)` persisted maintenance work. Nothing in
the replacement may scan or rewrite all branches for one global update.

Reject the cut if any implementation retains a direct control reader/writer,
stores `ref_change_id` or a serving generation in a selector-side duplicate,
uses a cache as authority, refreshes a selector in a transaction, permits raw
space mutation, rotates epoch for a no-op, accepts stale writes, or leaves the
old control space as a compatibility/fallback path.
