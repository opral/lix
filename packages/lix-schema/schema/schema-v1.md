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
currently accepts only `uuidv7()` on `uuid` columns. This deliberately small
PostgreSQL expression dialect can be extended in later schema versions.

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
primary-key, unique, or foreign-key changes. Incompatible evolution requires an
explicit future migration facility or a new schema key.
