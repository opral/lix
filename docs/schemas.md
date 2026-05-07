---
description: Define the entity types Lix tracks for you. The x-lix-* JSON Schema extensions control the SQL table name, primary keys, and uniqueness.
---

# Schemas

Schemas describe the entities Lix tracks. You declare each entity type as a JSON Schema with a few `x-lix-*` extensions, and Lix exposes a SQL table for it.

Schemas are also the foundation file-format plugins will build on: the planned plugin API lets a plugin parse a file format (XLSX, DOCX, CAD, …) into entities described by a schema. Today you register schemas yourself; once the API lands, plugin authors register theirs.

## Register a schema

```ts
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [
    JSON.stringify({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      "x-lix-key": "acme_section",
      "x-lix-version": "1",
      "x-lix-primary-key": ["/id"],
      type: "object",
      required: ["id", "title", "body"],
      properties: {
        id: { type: "string" },
        title: { type: "string" },
        body: { type: "string" },
      },
      additionalProperties: false,
    }),
  ],
);
```

After registration, `acme_section` is a SQL table you can `INSERT`, `SELECT`, `UPDATE`, and `DELETE` against. A sibling table `acme_section_by_version` exposes the same rows across all versions (see [Versions & Merging](./versions.md)).

## The `x-lix-*` extensions

| Field               | Purpose                                                                                                      |
| ------------------- | ------------------------------------------------------------------------------------------------------------ |
| `x-lix-key`         | Required. Becomes the SQL table name. Use stable, lowercase, prefixed keys: `acme_section`, not `section`. See [Prefix your schema keys](#prefix-your-schema-keys). |
| `x-lix-version`     | Required. Schema contract version, e.g. `"1"`. Bump when the shape changes.                                  |
| `x-lix-primary-key` | Required for table-style INSERTs. Array of JSON Pointer paths into the entity.                               |
| `x-lix-unique`      | Optional. Array of unique constraints, each itself an array of JSON Pointer paths.                           |

Without `x-lix-primary-key` you'll get an error like `requires lixcol_entity_id because the schema has no x-lix-primary-key`.

### JSON Pointer paths

Primary-key and uniqueness paths are [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) strings: leading slash, slash-separated segments, pointing into the entity. For most schemas this is just `["/id"]`, but it works for nested fields:

```ts
"x-lix-primary-key": ["/owner/email"]
```

### Composite primary keys and uniqueness

```ts
"x-lix-primary-key": ["/order_id", "/line_no"],
"x-lix-unique": [["/sku"], ["/order_id", "/sku"]],
```

Uniqueness is **not** inferred from JSON Schema metadata. If a non-primary-key field must be unique, declare it with `x-lix-unique`.

### `additionalProperties: false`

Always include `additionalProperties: false` on app schemas. Lix validates writes against the schema, and accidental fields will fail fast instead of silently writing garbage.

## Prefix your schema keys

`x-lix-key` is the global identifier for an entity type inside a Lix instance. It's also the SQL table name. Pick a prefix tied to your app, plugin, or organization, and put every schema you own behind it:

| Good | Bad |
| :-- | :-- |
| `acme_task`, `acme_section` | `task`, `section` |
| `xlsx_cell`, `xlsx_sheet` | `cell`, `sheet` |
| `figma_layer`, `figma_frame` | `layer`, `frame` |

Why it matters: a single Lix can hold many files and many schemas at once. App-level entities, file-format plugins (XLSX, DOCX, CAD, …), and Lix's own internal schemas all share the `lix_registered_schema` namespace. An unprefixed `task` collides the moment a second source registers the same name. The `lix_*` prefix is reserved for Lix-internal schemas; don't use it for your own.

Treat `x-lix-key` like a package name: lowercase, stable, namespaced. Once data is written, the key is hard to change.

## Don't store lifecycle timestamps

You don't need `created_at` or `updated_at` on app schemas. Lix already records lifecycle in [`lix_change`](./history.md). Add timestamp fields only when they're domain data, like `due_at` or `published_at`.

## Inspecting registered schemas

```ts
const schemas = await lix.execute(
  "SELECT lixcol_entity_id, value FROM lix_registered_schema ORDER BY lixcol_entity_id",
);

for (const row of schemas.rows) {
  const schema = row.get("value") as { "x-lix-key"?: string };
  console.log(schema["x-lix-key"]);
}
```

## Design for querying, not for merging

Shape your entities the way your reads want them. Document blocks, spreadsheet cells, line items: model whatever's natural for the questions your code asks.

Don't shrink rows just to avoid merge conflicts. Lix's conflict detection is row-level today (two versions editing different fields of the same row still conflict), but conflict semantics and resolution are an active roadmap item; designs that bend around today's limitation will look strange once that lands. See the [roadmap](https://github.com/opral/lix#roadmap).

If two collaborators are likely to edit the same logical thing concurrently and your domain naturally splits it (a document into blocks, an invoice into line items), split it because the *data* makes sense that way. Don't split a single record into ten just because a future merge might collide.
