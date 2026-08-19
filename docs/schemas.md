---
description: Define PostgreSQL-derived row schemas with Lix Schema v1.
---

# Schemas

A schema declares a table: columns, types, primary key, constraints. Register
one and Lix gives you a typed SQL surface for it, with the same branches,
history, and [diffs](./diffs.md) as every other row.

Lix Schema v1 is a strict JSON representation of the PostgreSQL table subset
supported by Lix. Its public identifier is `https://lix.dev/schema-v1.json`.

## Schemas are JSON, so plugins can ship them

A schema is data, not code. That is deliberate.

Plugins run as sandboxed WASM components and may be written in any language, so
they cannot hand Lix a Rust struct or a TypeScript type. They ship JSON files
instead. A plugin manifest lists them by path inside its archive:

```json
{
  "key": "plugin_csv",
  "file_match": { "path_glob": "*.{csv,tsv}", "content": "text" },
  "entry": "plugin.wasm",
  "schemas": ["schema/csv_table.json", "schema/csv_row.json"]
}
```

Each listed file is a Schema v1 document. Installing the plugin is a normal
tracked write: its schemas become `lix_registered_schema` rows. From then on the
plugin's rows behave like any other rows — same SQL surfaces, same history, same
[diffs](./diffs.md). A manifest may declare between 1 and 64 schemas.

The CSV plugin defines what a CSV record is. The Markdown plugin defines what a
block is. Your application defines its own tables the same way. Lix has no
per-format code path.

## Discover registered schemas

Query `lix_registered_schema` to list every registered schema. `schema_key`
holds the key. The JSONB `value` column holds the Schema v1 document:

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

`schema_key` must equal `value.key`. After registration, `acme_section` and
`acme_section_history()` expose the typed current row and its revision history.

## Contract

Schema v1 supports:

- PostgreSQL type names `text`, `uuid`, `int8`, `float8`, `boolean`, `jsonb`,
  and `timestamptz`;
- ordered columns, a required non-empty `primary_key`, unique constraints,
  and foreign keys;
- `nullable`, `default_value`, and the `uuidv7()` and `CURRENT_TIMESTAMP`
  default expressions; and
- `description`, `examples`, and `deprecated` annotations.

Identifiers must be lowercase `snake_case` and no longer than PostgreSQL's
63-byte identifier limit. Primary-key columns must be non-null `text`, `uuid`,
or `int8`. Composite keys preserve their declared order:

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
Lix rejects everything else: key changes, removing or renaming or reordering or
retyping a column, changing a column's nullability or default, and changing a
primary key, unique constraint, or foreign key. Use a new
schema key for an incompatible model until an explicit migration API exists.

## Naming

Use an owner prefix such as `acme_task` or `xlsx_cell`. The `lix` and `lix_*`
names are reserved for Lix. A schema key identifies the durable schema and its
SQL surface, so treat it like a stable package name. Each row has its own
primary-key identity within that schema.

The machine-readable meta-schema and its PostgreSQL mapping live in the
`lix-schema` crate, at `schema/schema-v1.json` and `schema/schema-v1.md`.
