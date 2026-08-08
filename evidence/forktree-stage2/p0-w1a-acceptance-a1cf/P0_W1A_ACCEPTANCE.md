# Stage2 next-wave P0 + W1a acceptance contract

Status: frozen read-only reviewer package

## Immutable baseline

- ref: `origin/codex/forktree-stage2-milestone5a2-runtime-intent`
- head: `a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`
- tree: `d8326da2b1d38bd51b8ac7229d00684a6865bce2`
- approved narrow-source report:
  `../atomic-writer-milestone5a2-a1cf/TERMINAL_SOURCE_ATOMICITY_APPROVE.md`
- approved report SHA-256:
  `25b2bf80f493dfdb291e3813249aa2467c74ce52cee5da7a12a4bcca00ce7042`

This package may be run only against a clean immutable descendant of `a1cf`.
It does not authorize inspection of a mutable author tree, production edits by
the reviewer, a build, 5B expansion, or merge.

## P0 — transaction commit is the only publication boundary

P0 is a compiler deletion, not a policy convention.

Required end state:

1. `PreparedPublication::commit` does not exist, is not reexported, cannot be
   named through a compile anchor, and has no test-only/public/private alias.
2. ForkTree production contains no direct `Storage::begin_write`,
   `StorageWrite::commit`, `StorageWriteSet::commit`, adapter commit, retry, or
   flush. This includes reachability and corrupt-GC abort paths.
3. ForkTree owners may return only typed immutable objects/selectors or one
   in-memory `StorageWriteSet` plus exact raw preconditions.
4. The existing transaction owner consumes exactly one such plan, appends
   runtime/idempotency/atomic metadata, calls `prepare_write_set` once, and
   calls the prepared backend commit once.
5. Test fixtures publish through a test-only wrapper of that same
   transaction-owned plan/prepare/commit seam. They must not restore a direct
   `PreparedPublication` commit helper.

At the baseline, the exact P0 residue is deliberate and measurable:

- `forktree/publication.rs:779-795`: direct `PreparedPublication::commit`;
- `forktree/mod.rs:153`: compile anchor makes it nameable;
- `forktree/reachability.rs:176-207`: corrupt-progress direct writer;
- `forktree/reachability.rs:1329+`: progress/sweep direct writer;
- tests directly call the same publication commit owner.

All must be compiler-deleted or converted to plan-only/test transaction
consumption. Merely making a function private, feature-gated, unreachable by a
current call graph, or returning an error after `begin_write` is a blocker.

## W1a — one ordered single-branch history publication

W1a admits only these additional cohorts:

- zero or more ordered `StagedIntermediateCommit` values;
- one final commit for one branch;
- an optional first-parent override;
- existing extra parents for that same commit;
- selected historical semantic members;
- ordinary fresh tracked members, untracked rows, and the accepted 5A2 runtime
  row in the same transaction.

Everything is derived from the caller-owned coherent `StorageRead` and lowered
to one `PreparedForkTreePlan`, one `into_storage_plan`, one
`prepare_write_set`, and one backend commit. Only the final branch snapshot and
ref fact advance the selected branch; intermediate commits are cataloged and
retained but never become separately visible heads.

### Commit chronology and order

- Every staged commit ID is unique. Duplicate IDs reject before a plan.
- The intermediate/final graph is topologically complete and acyclic.
- Each intermediate's explicit parent is exact. The final first parent is the
  explicit override when present, otherwise the observed head. Extra parents
  retain caller order after deterministic duplicate removal.
- A parent already staged in this batch is resolved from the batch; an
  external parent is loaded and authenticated from the same read.
- Parent object domain, embedded CommitId, CommitCatalog key/object back-edge,
  and strict `parent.generation < child.generation` all validate.
- Child generation is checked-add of `max(parent generations) + 1`; a root is
  generation zero. Missing, malformed, cyclic, duplicate, non-decreasing, or
  overflowing parent authority fails before plan creation.
- Parent override does not rewrite chronology authority. The final Commit
  names the override parent, while the branch-ref transition's `before` edge
  remains the currently selected head fenced by the raw branch selector.
- The final selected branch snapshot points only to the final commit. The
  global/branch selectors transition once for the complete batch.

### Member identity, ordering, and selected history

For each commit, one deterministic ordered sequence is authoritative. The
test oracle records the exact pre-hard-cut public order and requires equality;
an implementation must not sort by ChangeId/ObjectId or iteration order.
Fresh prepared rows preserve their canonical prepared order. Selected batches
preserve batch order and row order after the existing logical-identity
deduplication contract. Intermediate commit order is independent of map order
and follows explicit parent chronology.

Every visited member validates all of:

1. ordinal converts without overflow and indexes the exact Commit member edge;
2. the member object exists and its ObjectId authenticates exact bytes;
3. the object domain is semantic Change, never ref/blob/tree/commit;
4. embedded ChangeId equals the requested/catalog key;
5. ChangeCatalog resolves that ChangeId to the exact Change object;
6. source-selected membership is authenticated against its declared source
   commit, source ordinal/order, CommitCatalog back-edge, and generation;
7. the target commit's ordered edge remains valid after reopen/history/diff.

Duplicate logical identities across fresh and selected members reject. A
duplicate selected member across batches follows the existing staging
deduplication result exactly; it must not acquire two target ordinals.

### Single-valued owner hard-cut

Baseline `ChangeCatalogOwner::CommitMember { commit_object_id, ordinal }` is
single-valued. A historical Change selected into another commit cannot be made
valid by overwriting that owner from source commit to target commit: doing so
breaks source history and creates last-writer authority. It also cannot be
listed in the target while retaining an owner that fails target ordinal
validation.

The successor must make one compiler hard cut to a many-membership-safe
authenticated projection. Acceptable representations preserve one immutable
Change object/ChangeId and authenticate each visited commit+ordinal edge plus
the exact ChangeCatalog/object identity. An authenticated membership object or
an owner-neutral catalog locator can satisfy this if there is exactly one
reader/writer and no old single-owner compatibility path. A second catalog,
dual writer/reader, scan fallback, mutable membership index, or unauthenticated
reuse is a blocker.

If the format changes, all model/serving/tree/reachability/test matches must be
compiler-exhaustive and the obsolete `CommitMember` encoding/decoder must be
deleted. No compatibility tag/reader or migration is allowed on this hard-cut
non-runnable branch.

## Unsupported families remain fail-closed

Until their own waves, these cohorts reject during complete intent
classification, before `open_coherent_view_on_read`, publication construction,
or storage preparation:

- file payload/upload/receipt;
- checkpoint/snapshot publication;
- mutation-journal/bulk replacement;
- more than one branch;
- reachability and GC publication;
- any publication family not explicitly listed under W1a.

Failure means zero writes/deletes, zero object or catalog additions, zero raw
selector/global epoch or GC-progress change, zero runtime sequence row, zero
tracked revision, and zero idempotency receipt. The transaction may have
opened its one non-mutating read for validation, but no plan exists.

## Required test layers

The machine-readable matrix is `P0_W1A_CASES.tsv`.

### Source/compile-negative tests

- `PreparedPublication::commit` and aliases are unnameable.
- ForkTree production has no direct begin-write/commit token or callable seam.
- only transaction commit consumes the plan;
- deferred cohorts remain in the classifier, not a late publication error;
- old single-owner selected-membership path is absent if the owner shape is
  hard-cut;
- no old production space/module/reader/writer is restored.

### Runtime/corruption tests

Use one counting adapter around Memory first, then RocksDB and SlateDB when the
compiler frontier is runnable. Snapshot complete storage state before every
failure injection and compare it byte-for-byte afterward. Count `begin_read`,
`prepare_write_set`, backend commit, puts/deletes/bytes/spaces, raw global and
branch selector bytes, GC progress, object/catalog key sets, runtime row, and
idempotency receipt.

Required controls include:

- intermediate chain plus final commit, exact history/diff/undo/redo/reopen;
- parent override where selected head differs from chronology first parent;
- selected source members from two commits with exact target order;
- duplicate fresh/selected identity and duplicate commit ID;
- swapped member order, wrong/out-of-range ordinal, wrong source/target owner;
- missing/malformed/wrong-domain/substituted Change object;
- missing/wrong CommitCatalog and ChangeCatalog back-edges;
- duplicate parent object/CommitId, missing parent, non-decreasing generation,
  parent cycle, and generation overflow;
- stale raw branch selector and stale raw global selector in both orderings;
- true no-op, unadvanced/advanced runtime, untracked-only;
- unsupported file/checkpoint/multi-branch/reachability/GC zero-write paths;
- explicit rollback, failed-statement savepoint with deterministic sequence
  gap, idempotent first commit/replay/conflicting fingerprint;
- close/drop/cold reopen after success and after every injected failure.

Success has exactly one transaction `begin_read`, one plan, one prepare, and
one backend commit. Stale or corrupt failure has no partial objects, selectors,
receipt, runtime row, or history.

## Canonical deletion scanner

Run the exact 1dbbf source oracle and compare both baseline and candidate:

- oracle commit: `1dbbf3d206540d36f5912eab8372a42819778b47`
- source SHA-256:
  `f71e91fcbccbb7d6df676a95e9d747725856b77f7e3177ec42f12ca8b28736cc`
- accepted reviewer binary SHA-256:
  `2aaf81d937110b5a248621420f0b3cbc7b5a116da8fbec0bb66453dde4e91585`
- exact a1cf baseline count: `166`
- exact baseline output SHA-256:
  `3891a48613e5d6ebd3d0ab2780aed13c6dd0236f1c2ff343320dd73fb2158a0d`

Candidate findings may only disappear or decrease. A new finding key or an
increased per-key count is a blocker. The older 170/`ae4250...` scanner policy
is non-comparable and excluded.

## Replay order and verdict

1. Verify immutable ref/head/tree/diff/patch and clean detached worktree.
2. Run `verify_p0_w1a_successor.sh` first.
3. Manually review selected-membership authority and exact parent/ref split.
4. Run source-negative tests.
5. If runnable, execute Memory cases in TSV; stop on first blocker.
6. Only then execute identical RocksDB and SlateDB cases and cold reopen.
7. Freeze terminal SOURCE/ATOMICITY APPROVE or BLOCKER with exact hashes.

No broad build, benchmark, 5B work, PR update, or merge belongs to this gate.
