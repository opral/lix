---
description: The application-oriented SQL surfaces in Lix: typed entities, files, directories, schema discovery, and workspace activity.
---

# SQL Surfaces

Lix exposes logical application data through typed SQL relations:

| Data | Active branch | Cross-branch | Branch-reachable history |
| :-- | :-- | :-- | :-- |
| Registered application entity `X` | `<schema>` | `<schema>_by_branch` | `<schema>_history` |
| Files | `lix_file` | `lix_file_by_branch` | `lix_file_history` |
| Directories | `lix_directory` | `lix_directory_by_branch` | `lix_directory_history` |

`lix_schema` provides schema discovery, `lix_schema_definition` provides the
schema mutation surface, `lix_key_value*` provides shared workspace metadata,
and `lix_change` provides workspace-wide activity. There is no generic
`lix_state*` SQL family.

## The executable column contract

The SQL engine is backed by DataFusion. Query
`information_schema.columns` for the executable public contract instead of
inferring types from Arrow or JSON Schema names. Lix reports the canonical SQL
types `TEXT`, `BYTEA`, `BIGINT`, `DOUBLE PRECISION`, and `BOOLEAN`.

JSON-backed columns remain SQL `TEXT` and are marked with
`lix_value_kind = 'JSON'`. `is_nullable` describes values returned by reads;
`column_default` and `lix_insert_policy` separately describe whether a write
may omit a column. A defaulted ID, for example, is non-null when read, may be
omitted on insert, and rejects an explicit `NULL`.

The reported scalar type name is executable as an explicit `CAST` in
`SELECT`, `INSERT`, and `UPDATE`. Bound Lix writes use those canonical names;
read expressions retain DataFusion's wider cast dialect. `BINARY` is retired
in favor of `BYTEA`.

`lix_insert_policy` describes omission on `INSERT`:

| Policy | Meaning |
| :-- | :-- |
| `READ_ONLY` | The column cannot be supplied on insert. |
| `REQUIRED` | Every inserted row must supply the column. |
| `OPTIONAL` | The column may be omitted without generating a value. |
| `DEFAULT` | Omission evaluates the expression in `column_default`. |
| `CONDITIONAL` | Whether the column is required depends on the row's other inputs. |

`CONDITIONAL` covers deliberate alternative forms: filesystem rows can use a
`path` or their directory/name fields, and typed entities can derive
`lixcol_entity_pk` from their public primary-key columns. These policies
describe omission only; `is_nullable` still describes read values.

```sql
SELECT column_name, data_type, is_nullable, column_default,
       lix_value_kind, lix_insert_policy
FROM information_schema.columns
WHERE table_name = 'lix_file'
ORDER BY ordinal_position;
```

## Typed entity surfaces

Registering an application schema with `x-lix-key: "acme_task"` produces:

| Surface | Use for |
| :-- | :-- |
| `acme_task` | Read and mutate tasks on the active branch. |
| `acme_task_by_branch` | Read or mutate tasks with an explicit `lixcol_branch_id`. |
| `acme_task_history` | Read task revisions reachable from a commit. |

User properties become ordinary typed columns:

```sql
SELECT id, title, done
FROM acme_task
WHERE done = false;
```

Lix bookkeeping columns use the `lixcol_*` prefix. The base relation is scoped
implicitly to the active branch, while `_by_branch` adds
`lixcol_branch_id`.

History starts at the active branch head pinned for the statement or coherent
read batch. It adds `lixcol_entity_pk`, `lixcol_observed_commit_id`,
`lixcol_commit_created_at`, `lixcol_as_of_commit_id`, `lixcol_depth`, and
`lixcol_is_deleted`, together with singular change provenance such as
`lixcol_change_id`, `lixcol_change_created_at`, and `lixcol_origin_key`.

```sql
SELECT lixcol_depth, lixcol_observed_commit_id, title, done
FROM acme_task_history
WHERE id = 't1'
ORDER BY lixcol_depth;
```

Add exact `lixcol_as_of_commit_id = ...` or a non-empty
`lixcol_as_of_commit_id IN (...)` only for time travel. Other anchor predicates
are rejected instead of silently using the pinned head.

Typed history preserves every declared primary-key root on deletion rows,
including nested JSON roots. For a composite key, constrain every public key
column for an exact entity lookup; Lix encodes identity in the schema's
`x-lix-primary-key` order regardless of predicate order:

```sql
SELECT lixcol_depth, value, lixcol_is_deleted
FROM localized_message_history
WHERE key = 'welcome'
  AND locale = 'en'
ORDER BY lixcol_depth;
```

There are no bare history aliases. Every public history surface uses the
`lixcol_` prefix and the `lixcol_as_of_commit_id` spelling.

## Schema discovery and interoperability

`lix_schema` is the authoritative read-only schema catalog:

```sql
SELECT key, primary_key, surfaces, definition
FROM lix_schema
ORDER BY key;
```

Write definitions through `lix_schema_definition`; its `key` is derived from
`definition."x-lix-key"` and is read-only:

```sql
INSERT INTO lix_schema_definition (definition)
VALUES (lix_json($1));
```

The catalog contains both application schemas and schemas bootstrapped by Lix.
Registration does not imply that a Lix bootstrap schema has a public SQL
relation. The storage-level schemas `lix_file_descriptor`,
`lix_directory_descriptor`, and `lix_binary_blob_ref` remain registered for
interoperability while their implementation relations are private.

Applications and plugins cannot register the exact `x-lix-key` `lix` or a key
beginning with `lix_`; their base or generated SQL names occupy the namespace
reserved for Lix bootstrap schemas. Use an owner-specific prefix such as
`acme_task`.

`lix_key_value`, `lix_key_value_by_branch`, and `lix_key_value_history` remain
public for shared workspace settings and interoperability metadata.

## Files

`lix_file` exposes logical files, including their byte content:

| Surface | Use for |
| :-- | :-- |
| `lix_file` | Current files on the active branch. |
| `lix_file_by_branch` | Files with explicit branch scope. |
| `lix_file_history` | File revisions reachable from a commit. |

User columns are `id`, `path`, `directory_id`, `name`, and `data`.

File history records changes to the composed file projection, not only changes
to a file descriptor or its bytes. Renaming, moving, deleting, or restoring an
ancestor directory creates a revision for every affected descendant. The file
`id` remains stable while `path` reflects the observed commit.

Each revision contains `lixcol_source_changes`, a deterministic JSON array of
the descriptor, blob, plugin, and ancestor-directory changes that caused it.
Because one logical revision can have several causes, file history deliberately
has no singular `lixcol_change_id`, `lixcol_schema_key`, or
`lixcol_origin_key`.

```sql
INSERT INTO lix_file (path, data)
VALUES ('/orders.xlsx', CAST($1 AS BYTEA));

SELECT data
FROM lix_file
WHERE path = '/orders.xlsx';

SELECT lixcol_depth, lixcol_observed_commit_id, data, lixcol_source_changes
FROM lix_file_history
WHERE path = '/orders.xlsx'
ORDER BY lixcol_depth;
```

`path` has ordinary SQL row semantics on history: this query returns only
revisions whose path was `/orders.xlsx`. Filter by immutable `id` when you
want the complete lineage across renames and moves.

In JavaScript, pass a `Uint8Array` or `ArrayBuffer` for a byte parameter and
read `data` with `row.value("data").asBytes()`.

## Directories

Directories use the same three scopes:

| Surface | Use for |
| :-- | :-- |
| `lix_directory` | Current directories on the active branch. |
| `lix_directory_by_branch` | Directories with explicit branch scope. |
| `lix_directory_history` | Directory revisions reachable from a commit. |

User columns are `id`, `path`, `parent_id`, and `name`. Directory history uses
the same common history columns and structured `lixcol_source_changes`
provenance as file history. Directory paths end with a slash (`/data/`, not
`/data`).

An ancestor rename, move, deletion, or restoration creates a revision for each
descendant directory whose composed path changed. Recursive deletion
provenance includes both a descendant's own tombstone and relevant ancestor
tombstones.

Inserting a file at `/a/b/c.txt` creates `/a/` and `/a/b/` when needed. Insert
directories explicitly only when they should exist before any file.

## `lix_change`: workspace-wide activity

`lix_change` is heterogeneous and is not filtered to the active branch:

```sql
SELECT created_at, id, schema_key, entity_pk, snapshot_content
FROM lix_change
WHERE schema_key NOT LIKE 'lix_%'
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

The `(created_at, id)` ordering makes repeated result sets deterministic. It is
not a causal ordering between changes. Use a typed `<schema>_history` relation
when the question is which states are reachable from a branch; use
`lix_change` when the question is which activity exists anywhere in the
workspace.

## Removed generic and storage relations

The public SQL catalog has no compatibility aliases for `lix_state`,
`lix_state_by_branch`, `lix_state_history`, or the
`lix_file_descriptor*`, `lix_directory_descriptor*`, and
`lix_binary_blob_ref*` families.

| Removed use | Public replacement |
| :-- | :-- |
| Current entities for a known schema | `<schema>` |
| Cross-branch current entities | `<schema>_by_branch` |
| Branch-reachable entity revisions | `<schema>_history` |
| Current and historical files | `lix_file*` |
| Current and historical directories | `lix_directory*` |
| Shared workspace metadata | `lix_key_value*` |
| Workspace-wide heterogeneous activity | `lix_change` |
| Schema discovery | `lix_schema` |
