---
description: Define PostgreSQL-derived entity schemas with Lix Schema v1.
---

# Schemas

Lix Schema v1 is a strict JSON representation of the PostgreSQL table subset
supported by Lix. Its public identifier is
`https://lix.dev/schema-v1.json`. It is not JSON Schema: there are no
`x-lix-*` extensions and no nested JSON validation.

Agents should query `lix_registered_schema`; its `schema_key` and JSONB
`value` columns are authoritative:

```sql
SELECT schema_key, value
FROM lix_registered_schema
ORDER BY schema_key;
```

## Register a schema

```sql
INSERT INTO lix_registered_schema (schema_key, value)
VALUES ('acme_section', '{
  "$schema": "https://lix.dev/schema-v1.json",
  "key": "acme_section",
  "columns": [
    { "name": "id", "type": "uuid", "nullable": false,
      "default_expression": "uuidv7()" },
    { "name": "title", "type": "text", "nullable": false },
    { "name": "body", "type": "text", "nullable": false },
    { "name": "metadata", "type": "jsonb", "nullable": true }
  ],
  "primary_key": ["id"],
  "unique": [["title"]]
}'::jsonb);
```

`schema_key` must equal `value.key`. After registration, `acme_section`,
`acme_section_by_branch`, and `acme_section_history()` expose the typed entity.

## Contract

Schema v1 supports:

- PostgreSQL type names `text`, `uuid`, `bigint`, `double precision`,
  `boolean`, and `jsonb`;
- ordered columns, a required non-empty `primary_key`, unique constraints,
  and foreign keys;
- `nullable`, `default_value`, and the `uuidv7()` default expression; and
- `description`, `examples`, and `deprecated` annotations.

Identifiers must be lowercase `snake_case` and no longer than PostgreSQL's
63-byte identifier limit. Primary-key columns must be non-null `text`, `uuid`,
or `bigint`. Composite keys preserve their declared order:

```json
"primary_key": ["order_id", "line_number"],
"unique": [["order_id", "sku"]]
```

Foreign keys name local and referenced columns directly:

```json
"foreign_keys": [{
  "columns": ["author_id"],
  "references": {
    "schema_key": "acme_author",
    "columns": ["id"]
  }
}]
```

Omitted options use PostgreSQL defaults: `MATCH SIMPLE`, `ON DELETE NO
ACTION`, `ON UPDATE NO ACTION`, and `NOT DEFERRABLE`.

`jsonb` accepts any JSON value but does not validate nested structure. It
discards whitespace, object-key order, duplicate keys, and numeric spelling.
Use `text` when lexical preservation matters.

## Amendments

Re-registering the same key is an amendment. Lix permits documentation-only
changes and appending a nullable column or a column with a compatible default.
It rejects key changes; column removal, rename, reorder, type, nullability, or
default changes; and primary-key, unique, or foreign-key changes. Use a new
schema key for an incompatible model until an explicit migration API exists.

## Naming

Use an owner prefix such as `acme_task` or `xlsx_cell`. The `lix` and `lix_*`
names are reserved for Lix. A schema key is both durable entity identity and
SQL table name, so treat it like a stable package name.

The complete machine-readable meta-schema and semantic mapping live in the
standalone `lix-schema` crate under `schema/schema-v1.json` and
`schema/schema-v1.md`.
