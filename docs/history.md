---
description: Query commit logs, endpoint changes, working diffs, and historical snapshots.
---

# History

Lix automatically commits tracked writes. Checkpoints compact a working interval
into a retained commit. SQL exposes four complementary reads:

| Function | Result |
| --- | --- |
| `lix_log([anchor])` | Retained commits along the anchor's first-parent chain |
| `lix_history(relation [, anchor])` | Logical rows changed by each of those commits |
| `lix_diff(relation [, from, to])` | Net differences between two complete states |
| `lix_as_of(relation, commit)` | The complete relation state at one commit |

Relation arguments are text literals such as `'lix_file'`. Commit arguments may
be bound text parameters. Log and history default to the active head pinned for
the statement. Pass the same explicit anchor across requests to keep a page
stable while the branch advances. Automatic commits that compaction removes
are not permanently retained.

`lix_as_of` and the explicit commit arguments of `lix_diff` also accept
uncorrelated scalar subqueries. They resolve within the same statement read
context as the outer query, so a branch lookup needs no separate round trip:

```sql
SELECT id, path, content
FROM lix_as_of('lix_file', (
  SELECT commit_id FROM lix_branch WHERE name = $1
));
```

The commit argument must resolve to one non-null text commit ID. A scalar
subquery returning multiple rows produces an error; no rows yields NULL, which
must be handled (for example, with `COALESCE`) or the commit argument is rejected.
References to rows of the outer query are not supported in these commit
arguments. Relation names retain their existing argument rules.
Each commit argument resolves independently. Bind a shared commit ID when
multiple arguments must reuse a value computed by a volatile expression;
CTEs do not guarantee shared evaluation across arguments.

## Commit log

```sql
SELECT commit_id, parent_commit_id, created_at, is_checkpoint, position
FROM lix_log($1)
WHERE is_checkpoint
ORDER BY position
LIMIT 20;
```

Position zero is the anchor; positions increase toward older commits and are
assigned before filtering, so filtered positions can have gaps. The parent is
the actual first parent, not the preceding marked checkpoint. Empty commits
appear in the log. The parentless root is a baseline and produces no synthetic
history additions.

`lix_commit` is the repository-global inventory, including retained off-branch
checkpoints. Use `SELECT count(*) FROM lix_commit WHERE is_checkpoint` for a
global checkpoint metric. `lix_log` is branch-relative. For full DAG inspection,
use `lix_commit.parent_commit_ids` and `lix_commit_ancestry`.

## Row history

```sql
SELECT id, diff_type, from_path, to_path,
       lixcol_from_commit_id, lixcol_to_commit_id,
       lixcol_commit_created_at, lixcol_commit_is_checkpoint, lixcol_position
FROM lix_history('lix_file', $1)
WHERE id = $2 AND lixcol_commit_is_checkpoint
ORDER BY lixcol_position;
```

Every event compares a commit with its actual first parent. `WHERE`, projection,
ordering, and checkpoint filtering never alter those endpoints. A merge event
includes changes received relative to its first parent; commits on merged side
branches are not separately enumerated.

History shares diff's typed primary keys, opaque `row_ref`, `diff_type` (`added`,
`modified`, `removed`), and paired `from_<column>` / `to_<column>`
columns. Absent sides are null. The endpoint metadata identifies the compared
states; the checkpoint flag, time, and position describe the destination.
Repeated edits compact to one net row difference; net-zero differences produce
no row. Filtering unmarked history is not a working-status query.

A dirty fork can start at an ordinary commit A after checkpoint C0. A new
checkpoint C1 compares against A. Its history describes its own contribution;
`lix_diff(relation, C0, C1)` answers the different question of what changed
between the two marked states.

## Checkpoint previews

Page commits before expanding changed rows, preserving empty checkpoints:

```sql
WITH page AS (
  SELECT commit_id, created_at, position
  FROM lix_log($1)
  WHERE is_checkpoint
  ORDER BY position
  LIMIT 20
)
SELECT p.commit_id, p.created_at, h.id, h.diff_type,
       coalesce(h.to_path, h.from_path) AS path
FROM page p
LEFT JOIN lix_history('lix_file', $1) h
  ON h.lixcol_to_commit_id = p.commit_id
ORDER BY p.position, path;
```

A two-query version first selects page IDs, then reads history with
`WHERE lixcol_to_commit_id IN (...)` using the same anchor. This supports lazy
viewport previews without one diff query for every checkpoint.

## File and directory history

File and directory events describe their logical before/after projections.
Moving an ancestor directory changes descendant paths even if their own records
did not change. `COUNT(*)` counts the logical rows in the relation being
displayed, including descendants whose paths changed. There is no aggregate
count of internal descriptor or content records.

Project `from_content` / `to_content` directly from file history for byte
previews. Filter by file identity or path to select files before loading bytes.

Load file bytes lazily from the endpoint snapshot:

```sql
SELECT id, path, content
FROM lix_as_of('lix_file', $1)
WHERE id = $2;
```

An absent file returns no row; an existing empty file returns a row with
zero-length `BYTEA` content. Read or reconstruction failures produce errors,
not missing rows or empty content. For each present side of a file diff,
`from_content` / `to_content` equals the snapshot's `content` at that endpoint.
Use `diff_type` to distinguish absent diff sides from nullable values.

Select only metadata columns when bytes are unnecessary. Content projections
can demand deferred historical state and blob chunks on partial replicas,
including in working diffs. Snapshot reads use live relation columns
and include complete tracked state, with its pinned global state, but exclude
untracked rows. Collection-generation expansion restrictions remain explicit
errors where row-level expansion is unsupported.

## Working review and commands

```sql
SELECT id, diff_type, from_path, to_path,
       lixcol_from_commit_id, lixcol_to_commit_id
FROM lix_diff('lix_file');

SELECT working_base_commit_id, commit_id
FROM lix_branch
WHERE id = lix_active_branch_id();
```

The one-argument diff uses the active branch's `working_base_commit_id` and
`commit_id` (current head), pinned for the statement. The baseline can be an
ordinary commit after branch creation or restore. It must
not be inferred from the latest checkpoint. Read the branch pair and diff in a
coherent batch when an empty diff still needs an addressable context.

Use `row_ref` from the intended current or explicit-pair diff for selected
commands. A logical row can recur in many history commits, so a multi-commit
history selection is not an unambiguous command source. Guard commands against
stale endpoints and handle an empty/stale selection explicitly.

## Source-record activity

`lix_change` lists retained repository-global source records. It is distinct
from first-parent endpoint history and from checkpoint counts. New checkpoints
store membership directly on the commit and do not publish a marker change.
Historical marker records from migrated repositories can remain as old facts
until normal garbage collection.

## Breaking migration

`lix_state_at` is renamed to `lix_as_of`; `lix_checkpoint` is replaced by
`lix_commit WHERE is_checkpoint`. Replace the latest-checkpoint scalar with a
filtered, ordered `lix_log` query when a checkpoint is required, or the actual
branch working baseline for working changes. Existing revision-style history
queries must migrate to endpoint events; there is no permanent alias for the
old observation/depth/source-provenance shape. Upgrade storage and sync peers
together before writing with the new version.
