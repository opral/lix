# Lix Schema v1

`https://lix.dev/schema-v1.json` is the canonical JSON representation of the
PostgreSQL-derived relational schema subset supported by Lix.

## Compatibility contract

Schema v1 uses PostgreSQL 18 semantics for:

- `text`, `uuid`, `int8`, `float8`, `boolean`, `jsonb`, and `timestamptz`;
- `NULL` and `NOT NULL`;
- literal and expression defaults;
- ordered, composite primary keys;
- ordered, composite unique constraints; and
- ordered, composite foreign keys.

`row_ref` columns are the one Lix extension: a Lix logical row reference with
no PostgreSQL equivalent.

Omitted foreign-key options mean PostgreSQL's defaults: `MATCH SIMPLE`,
`ON DELETE NO ACTION`, `ON UPDATE NO ACTION`, and `NOT DEFERRABLE`.

Schema and column identifiers are snake_case ASCII and at most 63 UTF-8 bytes.
The restriction avoids PostgreSQL identifier truncation and quoting ambiguity.

## PostgreSQL mapping

| Schema v1 field | PostgreSQL 18 DDL |
| --- | --- |
| `key` | table name |
| `columns[].name` | column name |
| `columns[].type` | the same PostgreSQL type name (`row_ref` has none) |
| `nullable: false` | `NOT NULL` |
| `default_value` | typed `DEFAULT` literal |
| `default_expression` | `DEFAULT uuidv7()` |
| `primary_key` | ordered `PRIMARY KEY (...)` |
| `unique[]` | ordered `UNIQUE (...)` |
| `foreign_keys[]` | `FOREIGN KEY (...) REFERENCES ... (...)` |
| `row_refs[]` | delete action of a `row_ref` column (no PostgreSQL DDL equivalent) |

`primary_key` is required and must contain at least one column. Primary-key
columns must be non-null and use `text`, `uuid`, `int8`, or `row_ref`, the
identity types Lix can encode losslessly.

## Canonicalization

Canonical documents use the model's declaration order, preserve column and
constraint array order, omit absent optional fields, and serialize without
insignificant whitespace. A BLAKE3 hash of those UTF-8 bytes is the schema
fingerprint. JSONB column values are canonicalized as semantic JSON separately
from the schema document.

## Example

```json
{
  "$schema": "https://lix.dev/schema-v1.json",
  "key": "example_task",
  "columns": [
    {
      "name": "id",
      "type": "uuid",
      "nullable": false,
      "default_expression": "uuidv7()"
    },
    {
      "name": "metadata",
      "type": "jsonb",
      "nullable": true
    }
  ],
  "primary_key": ["id"]
}
```

`default_value` and `default_expression` are mutually exclusive. Schema v1
currently accepts `uuidv7()` on `uuid` columns and `CURRENT_TIMESTAMP` on
`timestamptz` columns. This deliberately small
PostgreSQL expression dialect can be extended in later schema versions.

## Row references

A `row_ref` column holds a canonical Lix row reference, the value SQL
`lix_row_ref(relation, file_id, key...)` returns. The value carries the target
relation, optional file scope, and typed primary-key values, so one column can
reference rows of any schema. It is stored as text, but it is its own SQL type,
`ROW_REF`, and a `row_ref` column cannot have a literal default. Its `nullable`
setting determines whether SQL `NULL` is accepted. PostgreSQL has no
equivalent, so a schema with a `row_ref` column has no PostgreSQL DDL.

Every `row_ref` value must resolve to an existing row in the writing row's scope
when a write sets it. Deleting a referenced row follows the column's delete
action. `row_refs` names that action for a column; a column without an entry
uses `no_action`:

| `on_delete` | Deleting the referenced row |
| --- | --- |
| `no_action` (no entry) | fails while a row still references it |
| `cascade` | also deletes the referencing rows |
| `detach` | leaves the referencing rows untouched, including the reference |

A `detach` reference keeps its value after its target is deleted; it is simply
no longer enforced for that row. It resolves again if the target returns, for
example through undo or a restore, without a write to the referencing row.
Writes that change other columns of such a row do not re-check the unchanged
reference, and a merge keeps a referencing row written on the other branch as
it was written. Setting the column to a new value, including the same
reference written again as a new row, requires that value to resolve.

```json
{
  "columns": [
    { "name": "id", "type": "uuid", "nullable": false },
    { "name": "target", "type": "row_ref", "nullable": true }
  ],
  "row_refs": [{ "column": "target", "on_delete": "detach" }]
}
```

## JSONB

`jsonb` stores semantic JSON. It does not preserve whitespace, object-key order,
duplicate keys, or original number spelling. SQL `NULL` and JSONB `null` are
distinct. Source fragments that require lexical preservation must use `text`.

Schema v1 intentionally does not include nested JSON validation or PostgreSQL
arrays.

## Amendments

A document with an existing `key` is an amendment. Schema v1 permits:

- documentation-only changes to existing declarations; and
- appending a nullable column or a column with a default.

It rejects removal, rename, reorder, type/nullability/default changes, and all
primary-key, unique, foreign-key, or `row_refs` changes. A row-reference
deletion policy is part of constraint semantics and cannot be changed by a safe
append-only amendment. An appended nullable `row_ref` column uses `no_action`. Incompatible evolution requires an
explicit future migration facility or a new schema key.

Foreign keys may declare `"on_delete": "cascade"` to delete referencing rows
when the referenced row is deleted. Omission and `"no_action"` retain final-state
foreign-key validation. `restrict`, `set_null`, `set_default`, and `detach` are
not supported for foreign keys.
A deletion policy is part of the schema's constraint semantics and cannot be
changed by a safe append-only amendment. Existing stored schemas need no rewrite:
an omitted action retains its original serialization and behavior.

Cascades are ordinary transactional row deletions: subsequent statements see
removed dependents, rollback restores them, and committed deletions have normal
history. Composite foreign keys use PostgreSQL `MATCH SIMPLE` null semantics.
Branch merge preparation applies actions to winning deletion events in the
resolved candidate, including deletions already present on the destination.
