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
- ordered, composite foreign keys; and
- Lix logical row-reference constraints.

Omitted foreign-key options mean PostgreSQL's defaults: `MATCH SIMPLE`,
`ON DELETE NO ACTION`, `ON UPDATE NO ACTION`, and `NOT DEFERRABLE`.

Schema and column identifiers are snake_case ASCII and at most 63 UTF-8 bytes.
The restriction avoids PostgreSQL identifier truncation and quoting ambiguity.

## PostgreSQL mapping

| Schema v1 field | PostgreSQL 18 DDL |
| --- | --- |
| `key` | table name |
| `columns[].name` | column name |
| `columns[].type` | the same PostgreSQL type name |
| `nullable: false` | `NOT NULL` |
| `default_value` | typed `DEFAULT` literal |
| `default_expression` | `DEFAULT uuidv7()` |
| `primary_key` | ordered `PRIMARY KEY (...)` |
| `unique[]` | ordered `UNIQUE (...)` |
| `foreign_keys[]` | `FOREIGN KEY (...) REFERENCES ... (...)` |
| `row_refs[]` | Lix row-reference constraint (no direct PostgreSQL DDL equivalent) |

`primary_key` is required and must contain at least one column. Primary-key
columns must be non-null and use `text`, `uuid`, or `int8`, the identity
types Lix can encode losslessly.

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

`row_refs` declares a Lix logical row-reference constraint for each listed
column. The column must exist and use `text`; its `nullable` setting determines
whether SQL `NULL` is accepted. The row-reference value carries the target
relation, optional file scope, and typed primary-key values, so this constraint
has no direct PostgreSQL `FOREIGN KEY` representation. `on_delete` defaults to
`no_action` and may be set to `cascade` or `set_null`. `set_null` requires a
nullable column: deleting the target keeps the referencing row and clears the
reference. With `set_null`, `detached_column` optionally names another nullable
`text` column that receives the cleared reference, so the row records what it
was attached to. A row may not set both columns: re-attaching writes the
reference and clears the detached column.

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
primary-key, unique, foreign-key, or row-reference constraints. A
row-reference deletion policy is part of constraint semantics and cannot be
changed by a safe append-only amendment. Incompatible evolution requires an
explicit future migration facility or a new schema key.

Foreign keys may declare `"on_delete": "cascade"` to delete referencing rows
when the referenced row is deleted. Omission and `"no_action"` retain final-state
foreign-key validation. `restrict`, `set_null`, and `set_default` are not supported.
A deletion policy is part of the schema's constraint semantics and cannot be
changed by a safe append-only amendment. Existing stored schemas need no rewrite:
an omitted action retains its original serialization and behavior.

Cascades are ordinary transactional row deletions: subsequent statements see
removed dependents, rollback restores them, and committed deletions have normal
history. Composite foreign keys use PostgreSQL `MATCH SIMPLE` null semantics.
Branch merge preparation applies actions to winning deletion events in the
resolved candidate, including deletions already present on the destination.
