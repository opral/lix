---
description: Lix retains tracked history and canonical untracked changes. Query lix_change for ledger entries, lix_state_history for what's reachable from a version, and <schema>_by_version for current per-version state.
---

# Change History

Lix gives you three SQL surfaces for history. Pick the one that matches the question you're asking. For the full grid of state, version, and history surfaces see [SQL Surfaces](./surfaces.md).

| Surface | What you ask it |
| --- | --- |
| `lix_change` | "Which canonical changes exist?" Tracked history plus the latest compactable untracked change for each current identity. |
| `lix_state_history` | "What did this version see?" State walked back from a commit, with `lixcol_depth` for time-travel. |
| `<schema>_by_version` | "What's in this version right now?" Current rows in each version. Documented in [Versions & Merging](./versions.md). |

Versions don't filter `lix_change` directly; commit membership lives in the commit graph. Tracked changes are retained by commits. Untracked changes are real change rows too, but a later mutation of the same current identity compacts the superseded untracked change. To scope retained history to a version, use `lix_state_history` with the version's `commit_id`.

## `lix_change` columns

| Column             | What it is                                                                                              |
| ------------------ | ------------------------------------------------------------------------------------------------------- |
| `id`               | Unique change id.                                                                                       |
| `entity_pk`        | JSON array of the changed row's primary-key values, in `x-lix-primary-key` order.                       |
| `schema_key`       | Which schema (`x-lix-key`).                                                                             |
| `file_id`          | The file the change belongs to, or `null` for entity-only changes.                                      |
| `metadata`         | JSON metadata attached to the change.                                                                   |
| `snapshot_content` | JSON snapshot of the row after the change, or `null` for deletions (tombstones).                        |
| `created_at`       | ISO timestamp.                                                                                          |

Read JSON cells with `row.value("snapshot_content").asJson()` or `row.get("snapshot_content")`. Don't `JSON.parse` it as text, and handle `null` for tombstones.

`entity_pk` is a JSON array even for single-column primary keys. For `x-lix-primary-key: ["/id"]`, the first primary-key value is `lix_json_get_text(entity_pk, 0)`. Use numeric path segments for array indexes; `'0'` is a string key, not index `0`. For composite keys, address each part by index:

```sql
SELECT created_at, snapshot_content
FROM lix_change
WHERE schema_key = 'line_item'
  AND lix_json_get_text(entity_pk, 0) = 'order-1'
  AND lix_json_get_text(entity_pk, 1) = 'line-2'
ORDER BY created_at;
```

## `lix_state_history` columns

| Column                     | What it is                                                                                     |
| -------------------------- | ---------------------------------------------------------------------------------------------- |
| `lixcol_entity_pk`         | JSON array of the row's primary-key values, in `x-lix-primary-key` order.                       |
| `lixcol_schema_key`        | Which schema.                                                                                  |
| `lixcol_file_id`           | The file the row belongs to, or `null`.                                                        |
| `lixcol_snapshot_content`  | JSON snapshot at this revision, or `null` for a tombstone.                                     |
| `lixcol_metadata`          | JSON metadata.                                                                                 |
| `lixcol_change_id`         | The `lix_change.id` that produced this state.                                                  |
| `lixcol_change_created_at` | When that source change was created.                                                           |
| `lixcol_origin_key`        | Optional origin key attached to the source change.                                             |
| `lixcol_observed_commit_id`| The commit where this state was recorded.                                                      |
| `lixcol_commit_created_at` | When that commit was created. This never falls back to the change timestamp.                    |
| `lixcol_as_of_commit_id`   | The commit anchoring the walk (typically the active branch tip).                                |
| `lixcol_depth`             | `0` = revision at the anchor. Higher values walk back through reachable history.                |
| `lixcol_is_deleted`        | `true` when this revision is a tombstone.                                                      |

## Recipes

### Per-entity history (across all versions)

```sql
SELECT created_at, snapshot_content
FROM lix_change
WHERE schema_key = $1
  AND lix_json_get_text(entity_pk, 0) = $2
ORDER BY created_at;
```

### Latest activity for a schema

```sql
SELECT created_at, entity_pk, snapshot_content
FROM lix_change
WHERE schema_key = $1
ORDER BY created_at DESC
LIMIT 20;
```

### What's in this version right now

Use the schema's `_by_version` surface (see [Versions & Merging](./versions.md)):

```sql
SELECT entity_pk, snapshot_content
FROM acme_section_by_version
WHERE lixcol_version_id = $1;
```

### What did this version see, walked back through history

```sql
SELECT lixcol_entity_pk, lixcol_schema_key, lixcol_snapshot_content,
       lixcol_depth, lixcol_observed_commit_id, lixcol_is_deleted
FROM lix_state_history
WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id()
  AND lixcol_depth >= 0
ORDER BY lixcol_depth, lixcol_schema_key, lixcol_entity_pk;
```

`lixcol_depth = 0` is the current state of that version. Higher depths walk back through earlier commits. Filter by `lixcol_schema_key` or `lixcol_entity_pk` to narrow.

For filesystem history, `lix_file_history` and `lix_directory_history` expose
logical projection revisions. A directory change is visible in every
descendant whose composed path depends on that directory, even when the
descendant's own descriptor did not change. Rows keep immutable file or
directory identity and list all same-commit causes in
`lixcol_source_changes`. Deletions use the exact direct-parent roots to retain
ancestor tombstones in descendant provenance; merge siblings are never inferred
from depth.

### Diff one entity between two versions

```sql
SELECT v.id AS version_id, v.name, s.snapshot_content
FROM acme_section_by_version s
JOIN lix_version v ON v.id = s.lixcol_version_id
WHERE s.id = $1
  AND s.lixcol_version_id IN ($2, $3);
```

Compare the two `snapshot_content` JSON values field-by-field in your code to render a per-field diff.

### Undo the last change to an entity

```ts
const prev = await lix.execute(
  `SELECT snapshot_content
     FROM lix_change
    WHERE schema_key = $1
      AND lix_json_get_text(entity_pk, 0) = $2
      AND snapshot_content IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1 OFFSET 1`,
  ["acme_section", "s1"],
);

const snapshot = prev.rows[0]?.value("snapshot_content").asJson();
// then UPDATE acme_section with the snapshot fields
```

The `snapshot_content IS NOT NULL` filter skips tombstones (deletions).

## Tombstones

A deletion produces a `lix_change` row with `snapshot_content = null`. Branch on null when rendering or replaying history.
