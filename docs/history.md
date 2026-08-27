---
description: Query typed history for branch-reachable row states and lix_change for repository-wide activity.
---

# History

Typed history answers what a row looked like across the commits reachable
from the current head. The common query needs no arguments:

```sql
SELECT title, done, lixcol_depth, lixcol_is_deleted
FROM lix_history('acme_task')
WHERE id = 't1'
ORDER BY lixcol_depth;
```

| title | done | lixcol_depth | lixcol_is_deleted |
| :-- | :-- | --: | :-- |
| Ship v2 | true | 0 | false |
| Ship v2 | false | 1 | false |
| NULL | NULL | 2 | true |

`lixcol_depth` `0` is the state at the head; higher depths walk back through
reachable commits. The `lixcol_is_deleted` row is a tombstone: the task was
deleted at that revision and recreated later, so its state columns are `NULL`.

Lix exposes two history concepts. Choose the surface that matches the scope of
the question:

| Surface | What it answers |
| :-- | :-- |
| `lix_history('<schema>')`, `lix_history('lix_file')`, `lix_history('lix_directory')` | Which logical revisions are reachable from a commit? |
| `lix_change` | Which retained changes exist anywhere in this repository? |

Typed history is exposed through table-valued functions. It is schema-specific
and commit-reachability scoped. `lix_change` is heterogeneous and
repository-wide. A change on an unmerged sibling branch can appear in
`lix_change` without appearing in history read from the active branch.

For the full surface grid, insert policies, and the
`information_schema` contract, see [SQL Surfaces](./surfaces.md).

## Typed row history

A registered application schema such as `acme_task` has one typed current-state
relation and supports the generic history function: `acme_task` for the current
session and `lix_history('acme_task')` for branch-reachable revision history.

History starts at the active branch head. The user columns are the same typed
columns exposed by the base relation. Row history adds these system
columns:

| Column | What it is |
| :-- | :-- |
| `lixcol_row_ref` | Relation-qualified `lix_row_ref` for the logical row. |
| `lixcol_schema_key` | The registered schema key. |
| `lixcol_file_id` | The owning file, or `NULL`. |
| `lixcol_metadata` | JSON change metadata. |
| `lixcol_change_id` | The `lix_change.id` that produced this state. |
| `lixcol_change_created_at` | When that source change was created. |
| `lixcol_origin_key` | Optional origin key attached to the source change. |
| `lixcol_observed_commit_id` | The commit where this state was observed. |
| `lixcol_commit_created_at` | When that commit was created. It never falls back to the change timestamp. |
| `lixcol_depth` | `0` is the revision at the anchor; higher values walk back through reachable history. |
| `lixcol_is_deleted` | `true` when the revision is a tombstone. The row keeps the row identity — every declared primary-key root, including nested JSON roots — and the history metadata, while the nullable state columns are `NULL`. |

The optional second argument sets the starting point for the whole query. Omit
it to start at the active head, or pass a commit id for time travel. The first
argument is always the relation name as a text literal:

```sql
SELECT id, title, lixcol_depth
FROM lix_history('acme_task', $1)
WHERE id = $2
ORDER BY lixcol_depth;
```

To inspect another branch, resolve its `commit_id` and bind it:

```ts
const branch = await lix.execute(
  "SELECT commit_id FROM lix_branch WHERE id = $1",
  [branchId],
);
const commitId = branch.rows[0].commit_id as string;

const history = await lix.execute(
  `SELECT id, title, lixcol_depth
     FROM lix_history('acme_task', $1)
    WHERE id = $2
    ORDER BY lixcol_depth`,
  [commitId, "t1"],
);
```

For composite primary keys, filter the named typed columns. Their predicate
order does not change the identity encoded by the schema:

```sql
SELECT project_id, issue_number, title, lixcol_depth
FROM lix_history('acme_issue')
WHERE project_id = 'launch'
  AND issue_number = '7'
ORDER BY lixcol_depth;
```

## File and directory history

`lix_history('lix_file')` and `lix_history('lix_directory')` expose logical filesystem
history. Lix does not expose the underlying storage rows as SQL relations.

Both follow the same active-head and explicit-anchor convention:

```sql
lix_history('lix_file')
lix_history('lix_file', $as_of)

lix_history('lix_directory')
lix_history('lix_directory', $as_of)
```

Use a stable ID to follow an object across renames:

```sql
SELECT path, name, lixcol_depth, lixcol_observed_commit_id
FROM lix_history('lix_file')
WHERE id = $1
ORDER BY lixcol_depth;
```

A path predicate keeps ordinary SQL meaning: it returns revisions whose path
matched the predicate. It does not resolve a path to an ID and then return the
object's complete lifetime.

Filesystem history describes a composed projection. Renaming, moving,
deleting, or restoring an ancestor directory creates a revision for every
affected descendant even when the descendant's own descriptor did not change.
Each row records all same-commit causes in the structured
`lixcol_source_changes` JSON array. Each source object carries a `row_ref`
instead of a JSON primary-key tuple. It deliberately does not expose singular
`lixcol_change_id`, `lixcol_schema_key`, or `lixcol_origin_key` columns.

Lix reconstructs rows through the anchor commit's ancestry. It does not treat
equal-depth sibling commits as ancestors. When a directory is deleted
recursively, Lix retains the ancestor tombstones that explain each descendant
revision.

## Repository activity with `lix_change`

`lix_change` contains every retained change across branches, without proving
branch reachability. Ordinary untracked writes do not create change rows.

| Column | What it is |
| :-- | :-- |
| `id` | Unique change ID. |
| `row_ref` | Relation-qualified row reference, or `NULL` for private engine rows. |
| `schema_key` | Changed Schema v1 key. |
| `file_id` | Owning file, or `NULL`. |
| `metadata` | JSON change metadata. |
| `snapshot_content` | Snapshot after the change, or `NULL` for a deletion. |
| `account_id` | Account that authored the change. |
| `origin_key` | Optional origin key attached to the change. |
| `created_at` | Change timestamp. |

Use `lix_row_ref()` to address a public logical row without reconstructing its
primary-key encoding:

```sql
SELECT created_at, id, snapshot_content
FROM lix_change
WHERE row_ref = lix_row_ref('acme_issue', 'launch', '7')
ORDER BY created_at, id;
```

The `(created_at, id)` ordering is deterministic for repeated result sets. It
is a presentation order, not a causal order between changes.
