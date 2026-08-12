---
description: Define the entity types Lix tracks for you. The x-lix-* JSON Schema extensions control the SQL table name, primary keys, uniqueness, and foreign keys.
---

# Schemas

Schemas describe the entities Lix tracks. You declare each entity type as a JSON Schema with a few `x-lix-*` extensions, and Lix exposes a SQL table for it.

Schemas are also the foundation for file plugins. A plugin maps a file format to entities described by a schema. The SDK includes Markdown and CSV plugins. Other formats, such as XLSX, DOCX, and CAD, need a plugin.

> [!NOTE]
> **For agents.** Lix is self-documenting. When operating against a Lix repository, query `lix_registered_schema` to discover every schema currently in effect (including Lix's own internal schemas `lix_*`) rather than relying on a snapshot of these docs. The schemas you read back are authoritative and current.
>
> ```sql
> SELECT value FROM lix_registered_schema;
> ```

## Register a schema

```sql
INSERT INTO lix_registered_schema (value) VALUES (lix_json('{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "x-lix-key": "acme_section",
  "x-lix-primary-key": ["/id"],
  "type": "object",
  "required": ["id", "title", "body"],
  "properties": {
    "id":    { "type": "string" },
    "title": { "type": "string" },
    "body":  { "type": "string" }
  },
  "additionalProperties": false
}'));
```

After registration, `acme_section` is a SQL table you can `INSERT`, `SELECT`, `UPDATE`, and `DELETE` against. A sibling table `acme_section_by_branch` exposes the same rows across all branches (see [Branching](./branching.md)).

```sql
INSERT INTO acme_section (id, title, body) VALUES ($1, $2, $3);
SELECT id, title, body FROM acme_section WHERE id = $1;
```

## The `x-lix-*` extensions

| Field                | Purpose                                                                                                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `x-lix-key`          | Required. Becomes the SQL table name and the durable identity of the relation. Use stable, lowercase, prefixed keys: `acme_section`, not `section`. See [Prefix your schema keys](#prefix-your-schema-keys). |
| `x-lix-primary-key`  | Required for table-style INSERTs. Array of JSON Pointer paths into the entity. Column order is semantic.                                                                                                     |
| `x-lix-unique`       | Optional. Array of unique constraints, each itself an array of JSON Pointer paths.                                                                                                                           |
| `x-lix-foreign-keys` | Optional. Array of foreign keys to other registered schemas. See [Foreign keys](#foreign-keys).                                                                                                              |

Without `x-lix-primary-key` you'll get an error like `requires entity_pk because the schema has no x-lix-primary-key`.

Primary-key properties must be listed in `required`, and each must be a non-null string or integer.

Schema identity is `x-lix-key` alone. There is no version field. Evolution is governed by the [amendment rules](#schema-amendment-rules).

### JSON Pointer paths

Primary-key, unique, and foreign-key paths are [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) strings: leading slash, slash-separated segments, pointing into the entity. For most schemas this is just `["/id"]`, but it works for nested fields:

```ts
"x-lix-primary-key": ["/owner/email"]
```

### Composite primary keys and uniqueness

```ts
"x-lix-primary-key": ["/order_id", "/line_no"],
"x-lix-unique": [["/sku"], ["/order_id", "/sku"]],
```

Uniqueness is **not** inferred from JSON Schema metadata. If a non-primary-key field must be unique, declare it with `x-lix-unique`.

### Foreign keys

Foreign keys reference another registered schema by `x-lix-key`:

```ts
"x-lix-foreign-keys": [
  {
    "properties": ["/author_id"],
    "references": {
      "schemaKey": "acme_author",
      "properties": ["/id"]
    }
  }
]
```

The reference is **identity-only**: there is no `schemaVersion` on the right-hand side. A foreign key points at a schema by its stable `x-lix-key` and trusts that the referenced schema evolves under the same compatibility rules described below. This keeps cross-plugin references sane: a markdown plugin can FK into an author plugin without tracking which revision of the author schema is currently registered.

### `additionalProperties: false`

Always include `additionalProperties: false`. Lix validates writes against the schema, and accidental fields will fail fast instead of silently writing garbage. It's also required by the amendment rules below: schemas that don't set it cannot be safely amended.

## Schema amendment rules

A registered schema's `x-lix-key` is the relation's durable identity. You can re-register the same `x-lix-key` to amend the schema, but Lix only accepts changes that keep existing data valid. The rules are mechanical: a diff of old vs new must satisfy every constraint below or the amendment is rejected.

### Why amendments must be backward compatible

Lix is a version-controlled repository: changes retained by commits are immutable, and a repository may hold years of committed rows across many branches and many authors' schemas. There is no point in time at which Lix could rewrite old rows into a new shape, so retroactive migrations are impossible and the only safe direction of evolution is additive.

```
       schema grows forward (additive only) -------------->
       v1: {id, body}              v2: {id, body, tag?}

time   --o------o------o---------o------o-------------->
         c1     c2     c3        c4     c5
         +-- written under v1 ---+ +-- under v2 --+
                    |
                    +-- immutable; reading c1 must still
                       succeed after the v1 -> v2 amendment.
```

### What you can change

- **Add a new optional property.** It must not appear in `required`, and it must not be referenced by any existing primary-key, unique, or foreign-key constraint. Existing rows simply lack the field.
- **Edit doc-only fields** anywhere in the schema: `description`, `title`, `$comment`, `deprecated`. These never affect storage or validation, so you can iterate on them freely.

### What you cannot change

- **`x-lix-key`.** Renaming creates a new relation; it is not an amendment.
- **`additionalProperties`.** Must remain `false`.
- **Existing properties.** Type, default, format, nested schema, enum: all frozen. Once a property has shipped, its semantics are permanent.
- **`required`.** The required set is frozen. Neither additions nor removals.
- **Constraints (`x-lix-primary-key`, `x-lix-unique`, `x-lix-foreign-keys`).** Frozen. You can reorder list elements cosmetically (Lix normalizes the comparison), but you can't add, remove, or modify a constraint. Primary-key column order is semantic and cannot be reordered.
- **Top-level keywords** like `type`, `examples`, `patternProperties`. Frozen.
- **Nested object schemas.** A property whose `type` is `object` is frozen as a unit: you cannot add subproperties to it. Recursive schema evolution is intentionally a later, explicit feature.
- **`x-lix-version`.** Rejected if present on either side.

### What to do when you really need a breaking change

Mint a new `x-lix-key`. Ship `acme_section_v2` as a separate schema, leave the old key's data untouched in history, and write plugin-level migration code to move data at your own pace. The two keys coexist while consumers cut over: old data stays valid under the old key, new data lives under the new key, and foreign keys pointing at the old key keep working while new ones point at the new key. This is how protobuf, GraphQL, RDF, and OpenAPI all handle hard breaks: the new identity _is_ the version bump, and it cascades through references naturally.

## Prefix your schema keys

`x-lix-key` is the global identifier for an entity type inside a Lix instance. It's also the SQL table name. Pick a prefix tied to your app, plugin, or organization, and put every schema you own behind it:

| Good                         | Bad               |
| :--------------------------- | :---------------- |
| `acme_task`, `acme_section`  | `task`, `section` |
| `xlsx_cell`, `xlsx_sheet`    | `cell`, `sheet`   |
| `figma_layer`, `figma_frame` | `layer`, `frame`  |

Why it matters: a single Lix can hold many files and many schemas at once.
App entities, file plugins, and Lix's own
internal schemas all share the `lix_registered_schema` namespace. An
unprefixed `task` collides the moment a second source registers the same name.
The exact key `lix` and the `lix_*` prefix are reserved and enforced for
schemas bootstrapped by Lix; public runtime registration in that namespace
fails with
`LIX_RESERVED_SCHEMA_NAMESPACE`.

Treat `x-lix-key` like a package name: lowercase, stable, namespaced. Once data is written, the key is permanent (see the amendment rules above).

## Best practices

### Keep Lix lifecycle and provenance out of entity schemas

Entity schemas describe domain state, not the mechanics of the Lix write that
produced that state. Do not add `created_at`, `updated_at`, change or commit
IDs, `origin_key`, or generic audit metadata solely to describe a Lix write.
Those facts already belong to the change:

- [`lix_change`](./history.md) records the change ID, timestamp, metadata, and
  origin key for workspace-wide activity.
- `<schema>_history()` records branch-reachable revisions and exposes the
  source change and commit provenance through `lixcol_change_id`,
  `lixcol_change_created_at`, `lixcol_observed_commit_id`, and
  `lixcol_commit_created_at`.
- The current entity relation exposes `lixcol_created_at`,
  `lixcol_updated_at`, `lixcol_change_id`, and `lixcol_commit_id` for its
  current revision.

Derive a lifecycle or audit view from those surfaces when you need one; do not
make the entity carry a second, mutable copy of the same facts. Add timestamp
or metadata fields only when they are domain data with independent meaning,
such as `due_at`, `published_at`, `external_system_updated_at`, or a business
event's own payload.

### Design for querying, not for merging

Shape your entities the way your reads want them. Document blocks, spreadsheet cells, line items: model whatever's natural for the questions your code asks.

Don't shrink rows just to avoid merge conflicts. Lix's conflict detection is row-level today, so two branches that edit different fields of the same row still conflict. Conflict behavior is still evolving. Designs built around today's limitation may not make sense later.

If two collaborators are likely to edit the same logical thing concurrently and your domain naturally splits it (a document into blocks, an invoice into line items), split it because the _data_ makes sense that way. Don't split a single record into ten just because a future merge might collide.
