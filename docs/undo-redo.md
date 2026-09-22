---
description: Undo and redo committed SQL changes with durable receipts and checkpoint-aware working baselines.
---

# Undo and redo

Undo and redo are SQL table functions. They create ordinary immutable commits
and return the new commit ID in a single nullable `commit_id` column. They do
not move a branch ref to an earlier commit or erase the commit graph.

## Signatures

```sql
SELECT commit_id FROM lix_undo();
SELECT commit_id FROM lix_undo($target_commit_id);
SELECT commit_id FROM lix_undo($target_commit_id, ARRAY[lix_row_ref('acme_task', $file_id, $task_id)]);

SELECT commit_id FROM lix_redo();
SELECT commit_id FROM lix_redo($undo_commit_id);
SELECT commit_id FROM lix_redo($undo_commit_id, ARRAY[lix_row_ref('acme_task', $file_id, $task_id)]);
```

The signatures below use `row_refs` to mean an `ARRAY[...]` or `ARRAY(SELECT ...)` SQL expression:

```text
lix_undo()
lix_undo(target_commit_id TEXT)
lix_undo(target_commit_id TEXT, row_refs ROW_REF[])

lix_redo()
lix_redo(undo_commit_id TEXT)
lix_redo(undo_commit_id TEXT, row_refs ROW_REF[])
```

`lix_undo` accepts an original ordinary commit or a checkpoint commit. Its
explicit target is the commit whose effects are to be reversed. `lix_redo`
accepts the `commit_id` returned by `lix_undo`; it does not accept the original
target or another redo commit. A `row_refs` argument limits the operation to
the selected effects. An omitted argument means the complete eligible effect
set. An explicit empty array selects nothing and returns `NULL`. A `NULL`
selection is an error.

Use the top-level `SELECT commit_id FROM ...` shape shown above. These mutation
functions cannot be nested in a CTE, joined, aliased, or combined with query
modifiers. Pass selected identities through an SQL array expression rather
than a bound array parameter. Root commits and merge commits are not valid
undo targets.

The no-argument forms use the durable logical action stack for the active
branch. Ordinary undo stops at the active checkpoint; undo that checkpoint
explicitly by passing its commit ID. Undo and redo commits themselves are navigation records and are
skipped while choosing the next action. A later ordinary write discards the
convenience redo cursor. Existing receipts and immutable history remain
available for explicit operations when their target is still valid.

```sql
-- Capture the receipt; it is the only valid explicit redo target.
SELECT commit_id FROM lix_undo($original_commit);

-- A partial redo consumes only the selected effects from that receipt.
SELECT commit_id
FROM lix_redo(
  $undo_receipt,
  ARRAY[lix_row_ref('acme_task', $file_id, $task_id)]
);
```

Select rows from a historical diff using its explicit endpoints. The row refs
choose identities; `lix_undo` still derives the inverse from its target commit:

```sql
SELECT commit_id FROM lix_undo(
  $checkpoint,
  ARRAY(
    SELECT row_ref FROM lix_diff('acme_task', $previous_checkpoint, $checkpoint)
    WHERE id = $task_id
  )
);
```

When an operation has no eligible effect, the function returns one row with a
NULL `commit_id`. The mutation is still atomic: a conflict, stale receipt,
unknown target, wrong receipt kind, or missing required dependency rejects the
statement without publishing a partial commit. Use `lix_restore`,
`lix_revert`, or `lix_apply` for historical content operations when an undo
receipt is no longer valid.

Within one explicit SQL transaction, a receipt-producing undo or redo must be
the transaction's only mutation. Reads may follow it, but another write,
checkpoint, recovery operation, or undo/redo is rejected; commit-internal
materialization still completes the receipt commit. A no-op undo or redo that
returns `NULL` does not claim the mutation slot, so later writes remain
allowed. Use separate explicit transactions when combining a successful
undo/redo with other mutations.

## Choosing a recovery operation

Use undo/redo for the active branch's logical editing history. Use the other
SQL functions when the content operation itself is the intent:

| Intent | SQL function | Effect |
| --- | --- | --- |
| Return selected content to a source commit | `lix_restore(source [, row_refs])` | Creates a new commit whose selected tracked rows match the source. |
| Reverse one historical commit | `lix_revert(commit [, row_refs])` | Applies the inverse of that commit against its actual first parent. |
| Reverse a historical span | `lix_revert_range(before, after [, row_refs])` | Applies the inverse of the endpoint difference. |
| Replay a historical span | `lix_apply(before, after [, row_refs])` | Applies the forward endpoint difference. |
| Mark the current working state | `lix_create_checkpoint([row_refs])` | Creates a checkpoint boundary; it is not an undo receipt. |
| Navigate recent logical actions | `lix_undo` / `lix_redo` | Creates receipt commits and maintains the branch's undo/redo state. |

Restore, revert, apply, and checkpoint creation do not consume undo receipts.
They remain ordinary SQL actions with their own explicit endpoints and
selection rules. A restore or revert commit is itself an ordinary action that
can become the next no-argument undo target.

## Checkpoint cycles

For checkpoints A, B, C (C newest), `lix_undo(C)` appends receipt U:

```text
Physical history, newest first: U, C, B, A
Active checkpoints:            B, A
Working baseline:              B
```

`lix_redo(U)` appends another commit and reactivates C. Neither operation
changes C's immutable internal checkpoint identity. Retirement is recorded as new tracked
state, so replicas receive it with the operation. Unrelated working edits are
preserved; conflicting selected edits cause the whole operation to fail.

A checkpoint is a durable working baseline, not only a flag on one commit. If
only some checkpoint effects are undone, the checkpoint remains the working
baseline and the selected effects become ordinary working changes. The final
causal undo retires that checkpoint and publishes the recorded pre-checkpoint
baseline B. A complete redo reactivates the recorded checkpoint baseline C.

The content transition and the baseline transition are one atomic publication.
Checkpoint undo applies the checkpoint cycle's recorded C-to-B interval,
including the complete interval represented by a metadata-only checkpoint. For
locally authored checkpoints, C's semantic and physical first parent is B; the
separate recovered-head alias used to compact the interval is not an undo
endpoint. The recorded working baseline remains authoritative across
incorporation. Even a checkpoint with an empty content diff has a checkpoint
effect, so its full undo and complete redo return non-NULL receipt commits.

After a newer checkpoint starts a new cycle, receipts for the older cycle are
stale, including filtered undo and redo requests. A stale receipt cannot
resurrect the older baseline, even if the resulting row content happens to be
equal. Use restore or revert when intentionally applying historical content
after a cycle has been superseded.

When C has been retired and B is the active baseline, B may be selected as a
fresh undo target within the current checkpoint epoch. This does not revive
receipts invalidated by a newer checkpoint. Redo B first to restore its
baseline, then C can be redone using its still-current receipt.

## Replication and partial replicas

Undo and redo state is ordinary durable tracked data carried by the same
incorporation path as the content commit. A partial replica may execute a
request only when it has authoritative complete effect coverage, before-images,
dependency closure, and checkpoint metadata for the requested operation. A
row subset or event marker alone is insufficient authority.

Incorporation publishes the content rows, receipt marker, undo state, working
baseline, and logical-stack transition together. If hydration is incomplete or
a selected effect conflicts, none of those pieces becomes visible. This keeps
the receipt consumable exactly once on every replica that accepts it.

A fast-forward merge adopts the selected source branch's working baseline
with its head. The source control is checked atomically; another branch at the
same commit cannot supply that baseline. Foreign undo/redo markers do not
become the destination's no-argument editor stack. Explicit operations can
still use incorporated, eligible targets and receipts.

Undo and redo are SQL-only surfaces. Remote callers use `execute` with the
statements above; there is no separate undo/redo protocol endpoint or typed SDK
method. Internal marker and state relations are reserved implementation data
and must not be written directly.

## Upgrade compatibility

This is a breaking replacement of the previous undo/redo implementation.
Pending undo/redo navigation from repositories written before the state ledger
is not migrated. An old undo marker is not a valid redo receipt for this API;
requests that require its missing ledger state are rejected. There is no
legacy-marker fallback or typed-API compatibility shim.

Existing content commits remain in history. Use `lix_restore`, `lix_revert`, or
`lix_apply` to recover or replay historical content when an old pending receipt
cannot be used. Newly created undo/redo receipts follow the contracts above.

## Checkpoint history in applications

`lix_log().is_checkpoint` is the only public checkpoint status. It reflects
retirement at the log's anchor. Row history carries endpoint IDs and can join
the log at the same anchor. Applications list effective checkpoints with:

```sql
SELECT commit_id, parent_commit_id, created_at
FROM lix_log()
WHERE is_checkpoint
ORDER BY position;
```

Show **Undo** for the latest effective checkpoint when it is also the branch's
`working_base_commit_id`, and **Restore** for earlier ones. A branch forked at
an ordinary commit can have a working baseline that differs from its latest
marked checkpoint; use Restore there. An undone checkpoint remains in immutable
history and stays retired after a newer checkpoint is created.

## Regression sources

The regression suite adapts logical stack navigation from
[jj's undo/redo tests](https://github.com/jj-vcs/jj/blob/aa8c087f7d2f32cdbbc41a7e3816e7a9f1cfc001/cli/tests/test_undo_redo_commands.rs),
historical inverse and replay scenarios from
[Git's revert/cherry-pick tests](https://github.com/git/git/blob/master/t/t3501-revert-cherry-pick.sh),
and disjoint edits and conflicts from
[Dolt's SQL revert tests](https://github.com/dolthub/dolt/blob/main/go/libraries/doltcore/sqle/enginetest/dolt_queries_revert.go).
These are adapted scenarios, not claims of identical APIs. Row-filtered
checkpoint undo, receipt consumption, and partial-replica baseline publication
are Lix-specific contracts tested separately.
