---
description: "The application-oriented SQL surfaces in Lix: typed rows, files, directories, schema discovery, and insert policies."
---

# SQL Surfaces

Lix exposes logical application data through typed SQL relations:

| Data                            | Current session              | History / comparison                   |
| :------------------------------ | :--------------------------- | :------------------------------------- |
| Registered application row `X` | `<schema>`                   | `<schema>_history()`                   |
| Files                           | `lix_file`                   | `lix_file_history()`                   |
| Directories                     | `lix_directory`              | `lix_directory_history()`              |
| Working diffs                   | `lix_working_diff`           | `lix_diff(from_commit, to_commit)`     |
| File working diffs              | `lix_file_working_diff`      | —                                      |
| Directory working diffs         | `lix_directory_working_diff` | —                                      |
| Checkpoints                     | `lix_checkpoint`             | `lix_checkpoint_history()`             |

The history functions read revisions reachable from a commit; `lix_diff`
compares two arbitrary commits. `lix_registered_schema` and its history
function provide schema discovery; `lix_key_value` and its history function
provide shared repository metadata.
`lix_change` records repository-wide activity; [History](./history.md)
documents it together with the history functions.

`lix_file` represents regular file contents only. Its public columns are `id`,
`path`, `directory_id`, `name`, and `content`, plus the standard `lixcol_*`
bookkeeping columns. `path` is an absolute, literal UTF-8 path and `content` is
the file's bytes. Path characters such as spaces, `%`, `#`, `?`, and `@` are
not URL-encoded. Lix does not represent symbolic
links, device nodes, sockets, or other non-regular filesystem entries as
`lix_file` rows. Executable and
other permission bits are not part of the file contract.

The engine defines a Lix logical path as an absolute `/`-separated sequence of
literal UTF-8 segments. Empty segments, `.`, `..`, `/` within a segment, NUL,
and a trailing slash are invalid; `/` itself is only the root directory. All
other segment text is preserved exactly: the engine does not URL-decode,
case-fold, or Unicode-normalize paths. Filesystem adapters diagnose names that
the target host cannot represent.

The checkpoint and diff relations are read-only. `lix_working_diff` and
`lix_diff()` expose a `diff_id` that can
feed the `lix_revert`, `lix_apply`, and `lix_create_checkpoint` command sinks.
`lix_checkpoint` and the file and directory working-diff relations do not
carry a `diff_id`. See [Checkpoints](./checkpoints.md) and
[Diff commands](./diff-commands.md).

## The executable column contract

The SQL engine is backed by DataFusion. Query `information_schema.columns` for
the executable public contract instead of inferring types from Arrow or JSON
Schema names:

```sql
SELECT column_name, data_type, is_nullable, column_default,
       lix_value_kind, lix_insert_policy
FROM information_schema.columns
WHERE table_name = 'lix_file'
ORDER BY ordinal_position;
```

Lix reports the canonical SQL types `TEXT`, `BYTEA`, `BIGINT`,
`DOUBLE PRECISION`, and `BOOLEAN`. The reported scalar type name is executable
as an explicit `CAST` in `SELECT`, `INSERT`, and `UPDATE`. Bound Lix writes
use those canonical names; read expressions accept DataFusion's wider cast
dialect.

History functions are discoverable through
`information_schema.table_functions`, which reports their argument signature
and result columns. They do not appear in `information_schema.tables` or
`information_schema.columns`.

JSON-backed columns are SQL `TEXT` and are marked with
`lix_value_kind = 'JSONB'`. `is_nullable` describes values returned by reads;
`column_default` and `lix_insert_policy` separately describe whether a write
may omit a column. A defaulted ID, for example, is non-null when read, may be
omitted on insert, and rejects an explicit `NULL`.

`lix_insert_policy` describes omission on `INSERT`:

| Policy        | Meaning                                                           |
| :------------ | :---------------------------------------------------------------- |
| `READ_ONLY`   | The column cannot be supplied on insert.                          |
| `REQUIRED`    | Every inserted row must supply the column.                        |
| `OPTIONAL`    | The column may be omitted without generating a value.             |
| `DEFAULT`     | Omission evaluates the expression in `column_default`.            |
| `CONDITIONAL` | Whether the column is required depends on the row's other inputs. |

`CONDITIONAL` covers deliberate alternative forms: filesystem rows can use a
`path` or their directory/name fields, and typed rows can derive
`lixcol_row_pk` from their public primary-key columns. These policies
describe omission only; `is_nullable` still describes read values.

## Typed schema surfaces

Registering a Schema v1 document with `key: "acme_task"` produces:

| Surface                      | Use for                                                   |
| :--------------------------- | :-------------------------------------------------------- |
| `acme_task`                  | Read and mutate tasks in the current session.              |
| `acme_task_history()`        | Read task revisions reachable from the active head.       |
| `acme_task_history($commit)` | Read task revisions reachable from an explicit commit.    |

User properties become ordinary typed columns:

```sql
SELECT id, title, done
FROM acme_task
WHERE done = false;
```

Lix bookkeeping columns use the `lixcol_*` prefix. Relations are scoped to the
session's active branch. Open another session to work on another branch.

Every public history read calls its table-valued function with zero or one
commit-id argument; there are no bare history table aliases.
[History](./history.md) documents the history columns, depth ordering,
composite-key lookups, and tombstones.

## Schema discovery and interoperability

`lix_registered_schema` is the authoritative schema registry:

```sql
SELECT schema_key, value -> 'primary_key' AS primary_key
FROM lix_registered_schema
ORDER BY schema_key;
```

The registry contains both application schemas and schemas bootstrapped by
Lix. Registration does not imply that a Lix bootstrap schema has a public SQL
relation. The storage-level schemas `lix_file_descriptor`,
`lix_directory_descriptor`, and `lix_binary_blob_ref` are registered for
interoperability while their implementation relations are private.

Applications and plugins cannot register the exact Schema v1 key `lix` or a key
beginning with `lix_`; their base or generated SQL names occupy the namespace
reserved for Lix bootstrap schemas. Use an owner-specific prefix such as
`acme_task`.

`lix_key_value` and `lix_key_value_history()` are public for shared repository
settings and interoperability metadata.

## Files

`lix_file` exposes logical files, including their byte content:

| Surface              | Use for                                 |
| :------------------- | :-------------------------------------- |
| `lix_file`           | Current files on the active branch.     |
| `lix_file_history()` | File revisions reachable from a commit. |

User columns are `id`, `path`, `directory_id`, `name`, and `content`.

For text content, bind a text parameter and cast it to `BYTEA`:

```sql
INSERT INTO lix_file (path, content)
VALUES ('/orders.md', CAST($1 AS BYTEA));

SELECT CAST(content AS TEXT)
FROM lix_file
WHERE path = '/orders.md';
```

For the server wire protocol, the `$1` parameter is a plain text value such as
`{ "kind": "text", "value": "# Orders\n" }`. Use a `Uint8Array` parameter
when the file contains arbitrary non-UTF-8 bytes, and read `content` with
`row.value("content").asBytes()`.

`length(content)` is character-oriented even though `content` is `BYTEA`. Use
the standard `OCTET_LENGTH(content)` function to verify the stored byte count;
for example, `aé—` has length `3` and octet length `6`.

File history records revisions of the composed file projection with structured
`lixcol_source_changes` provenance; see
[History](./history.md#file-and-directory-history).

## Directories

Directories use the same three scopes:

| Surface                   | Use for                                      |
| :------------------------ | :------------------------------------------- |
| `lix_directory`           | Current directories on the active branch.    |
| `lix_directory_history()` | Directory revisions reachable from a commit. |

User columns are `id`, `path`, `parent_id`, and `name`. Directory and file
paths share the same canonical syntax: non-root paths do not end with a slash.
The typed SQL surface determines whether `/data` names a directory or a file.

Inserting a file at `/a/b/c.txt` creates `/a` and `/a/b` when needed. Insert
directories explicitly only when they should exist before any file.

Directory history follows the same composed-projection semantics as file
history; see [History](./history.md#file-and-directory-history).
