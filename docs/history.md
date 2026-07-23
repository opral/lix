---
description: Query typed history for branch-reachable entity states and lix_change for workspace-wide activity.
---

# Change History

Lix exposes two history concepts. Choose the surface that matches the scope of
the question:

| Surface | What it answers |
| :-- | :-- |
| `<schema>_history`, `lix_file_history`, `lix_directory_history` | Which logical revisions are reachable from a commit? |
| `lix_change` | Which retained changes exist anywhere in this workspace? |

Typed history is schema-specific and commit-reachability scoped.
`lix_change` is heterogeneous and workspace-wide. A change on an unmerged
sibling branch can therefore appear in `lix_change` without appearing in
history read from the active branch.

For the full surface grid, see [SQL Surfaces](./surfaces.md).

## Typed entity history

A registered application schema such as `acme_task` has three typed relations:
`acme_task` for the active branch, `acme_task_by_branch` for explicit branch
scope, and `acme_task_history` for branch-reachable history.

History starts at the active branch head pinned for the statement or coherent
read batch, so the common query needs no anchor predicate:

```sql
SELECT
  id,
  title,
  lixcol_depth,
  lixcol_observed_commit_id,
  lixcol_commit_created_at,
  lixcol_is_deleted
FROM acme_task_history
WHERE id = $1
ORDER BY lixcol_depth;
```

The user columns are the same typed columns exposed by the base relation.
Entity history adds these system columns:

| Column | What it is |
| :-- | :-- |
| `lixcol_entity_pk` | JSON array of primary-key values in `x-lix-primary-key` order. |
| `lixcol_schema_key` | The registered schema key. |
| `lixcol_file_id` | The owning file, or `NULL`. |
| `lixcol_snapshot_content` | JSON snapshot at this revision, or `NULL` for a tombstone. |
| `lixcol_metadata` | JSON change metadata. |
| `lixcol_change_id` | The `lix_change.id` that produced this state. |
| `lixcol_change_created_at` | When that source change was created. |
| `lixcol_origin_key` | Optional origin key attached to the source change. |
| `lixcol_observed_commit_id` | The commit where this state was observed. |
| `lixcol_commit_created_at` | When that commit was created. It never falls back to the change timestamp. |
| `lixcol_as_of_commit_id` | The commit anchoring the history walk. |
| `lixcol_depth` | `0` is the revision at the anchor; higher values walk back through reachable history. |
| `lixcol_is_deleted` | `true` when the revision is a tombstone. |

For time travel, use exact equality or a non-empty `IN` predicate on
`lixcol_as_of_commit_id`. Ranges, `LIKE`, `NOT IN`, expressions around the
anchor, and mixed `OR` conditions are rejected instead of silently using the
pinned head.

To inspect another branch, resolve its `commit_id` and bind it:

```ts
const branch = await lix.execute(
  "SELECT commit_id FROM lix_branch WHERE id = $1",
  [branchId],
);
const commitId = branch.rows[0].value("commit_id").asText();

const history = await lix.execute(
  `SELECT id, title, lixcol_depth
     FROM acme_task_history
    WHERE id = $1
      AND lixcol_as_of_commit_id = $2
    ORDER BY lixcol_depth`,
  ["t1", commitId],
);
```

For composite primary keys, filter the named typed columns. Their predicate
order does not change the identity encoded by the schema:

```sql
SELECT project_id, issue_number, title, lixcol_depth
FROM acme_issue_history
WHERE project_id = 'launch'
  AND issue_number = '7'
ORDER BY lixcol_depth;
```

## File and directory history

`lix_file_history` and `lix_directory_history` expose logical filesystem
history. Their storage descriptors are not public SQL relations.

Use a stable ID to follow an object across renames:

```sql
SELECT path, name, lixcol_depth, lixcol_observed_commit_id
FROM lix_file_history
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
`lixcol_source_changes` JSON array. It deliberately does not expose singular
`lixcol_change_id`, `lixcol_schema_key`, or `lixcol_origin_key` columns.

Rows are reconstructed through the anchor commit's ancestry. Equal-depth
sibling commits are not treated as ancestors, and recursive deletion
provenance retains the relevant ancestor tombstones.

## Workspace activity with `lix_change`

`lix_change` contains retained tracked changes across branches plus the latest
compactable untracked change for each current identity.

| Column | What it is |
| :-- | :-- |
| `id` | Unique change ID. |
| `entity_pk` | JSON array of primary-key values in schema order. |
| `schema_key` | Changed schema (`x-lix-key`). |
| `file_id` | Owning file, or `NULL`. |
| `metadata` | JSON change metadata. |
| `snapshot_content` | Snapshot after the change, or `NULL` for deletion. |
| `created_at` | Change timestamp. |

`entity_pk` is an ordered JSON array even for a singleton key. Use numeric path
segments for array indexes:

```sql
SELECT created_at, id, snapshot_content
FROM lix_change
WHERE schema_key = 'acme_issue'
  AND lix_json_get_text(entity_pk, 0) = 'launch'
  AND lix_json_get_text(entity_pk, 1) = '7'
ORDER BY created_at, id;
```

The `(created_at, id)` ordering is deterministic for repeated result sets. It
is a presentation order, not a causal order between changes.

## Tombstones

A deletion produces a change with `snapshot_content = NULL`. Typed history
retains the entity identity and history metadata while its nullable state
columns represent the tombstone.
