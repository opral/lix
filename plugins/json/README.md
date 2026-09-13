# JSON plugin

The JSON plugin maps every document node to a typed row through the existing
`FileProjection` API. Rows carry enough formatting information to reconstruct the
original bytes without a plugin cache. Duplicate decoded object keys have
separate zero-based `occurrence` values.

Supported documents use valid UTF-8 and Unicode strings (no lone surrogate
escapes), nesting of at most 1024 levels, and fewer than 4 GiB of source bytes.
Native JSONB also rejects U+0000 in scalar strings; escaped NUL object keys can
be preserved.
Every emitted row must fit the host's plugin API batch budget; a sufficiently
large individual scalar or formatting field can exceed that budget even when
the total file is below 4 GiB. Numbers must fit native JSONB's numeric domain,
described below. Lossless roundtrips apply to documents accepted within these
limits.

| Relation | Primary key | Children |
| --- | --- | --- |
| `json_root` | `id = 'root'` | Child `parent_id = 'root'` |
| `json_object_member` | `parent_id, key, occurrence` | Child `parent_id = container_id` |
| `json_array_item` | UUID `id` | Child `parent_id = id::text` |

Identities are scoped to a file. Filter by `lixcol_file_id`, including both sides
of joins: two files may have the same root and object-member keys. Object `key`
is the decoded string, including empty strings, slashes, tildes, and Unicode;
it is not a JSON Pointer. Array identity is independent of its current position.
`occurrence` defaults to zero and distinguishes repeated decoded keys in source
order. Include it in a predicate to update only one duplicate: `key = 'name'
AND occurrence = 1` selects the second `name` member. Inserting or deleting an
earlier duplicate may renumber later occurrences.

## Query and edit scalars

For a document such as `{"name":"Ada","settings":{"enabled":true}}`:

```sql
SELECT key, kind, scalar_json
FROM json_object_member
WHERE lixcol_file_id = $1 AND parent_id = 'root'
ORDER BY order_key, key, occurrence;

UPDATE json_object_member
SET scalar_json = '"Grace"'::jsonb
WHERE lixcol_file_id = $1 AND parent_id = 'root' AND key = 'name';

SELECT child.key, child.scalar_json
FROM json_object_member AS parent
JOIN json_object_member AS child
  ON child.parent_id = parent.container_id
 AND child.lixcol_file_id = parent.lixcol_file_id
WHERE parent.lixcol_file_id = $1
  AND parent.parent_id = 'root' AND parent.key = 'settings';
```

`kind` is one of `object`, `array`, `string`, `number`, `boolean`, or `null`.
A scalar's `scalar_json` is native JSONB and must match `kind`; change both when
changing its type. With `kind = 'null'`, native JSONB null, SQL `NULL`, or an
omitted `scalar_json` all represent JSON null. Other scalar kinds require a
matching JSONB value; containers use SQL `NULL` or omit `scalar_json`. Containers store their
contents in child rows, never in `scalar_json`.

Native JSONB numbers use signed/unsigned 64-bit integers or finite 64-bit floats.
`scalar_text` retains high-precision, underflow, and negative-zero source spellings
for byte-exact reconstruction, while SQL sees the host numeric precision in
`scalar_json`. Numeric overflow such as `1e9999` is rejected.

`prefix_json`, `suffix_json`, `empty_json`, and `scalar_text` preserve lexical
formatting. Ordinary value updates can retain these fields. A stale scalar
spelling is ignored when it no longer agrees with `scalar_json`; renaming a
member retains whitespace and rewrites its encoded key.

## Ordering and structure

Read siblings with `ORDER BY order_key, key, occurrence` for object members and
`ORDER BY order_key, id` for array items. `order_key` is a lexically sorted
fractional byte string encoded as lowercase hexadecimal: it must be nonempty,
have an even number of digits, and must not end in `00`. For example, `40`,
`60`, and `80` order three siblings, and `50` inserts between `40` and `60`.
The default is `80`; equal order keys use the member key and occurrence, or the
array UUID, as a deterministic tie-breaker. The default does not append to an
existing container.
`parent_id` defaults to `root`, and `json_root.id` defaults to `root`. Array IDs
default to `uuidv7()`.

Plugin row batches must describe a valid final tree with exactly one root,
existing parents of the matching container kind, and no cycles. Deleting a
container also requires deleting or moving its descendants. Array subtrees can
move by updating the array item's `parent_id`; their UUIDs remain unchanged.
Fresh imports derive object-container identities from their parent, decoded
key, and occurrence. SQL may supply any nonempty `container_id` unique within the
file, so a nested object can be created without computing a plugin-specific hash:

```sql
INSERT INTO json_object_member (key, kind, container_id, lixcol_file_id)
VALUES ('settings', 'object', 'settings-container', $1);
INSERT INTO json_object_member (parent_id, key, kind, scalar_json, lixcol_file_id)
VALUES ('settings-container', 'enabled', 'boolean', 'true'::jsonb, $1);
```

Primary-key columns are immutable through SQL `UPDATE`. Rename or move a scalar
or empty container with an atomic `execute_batch` containing a `DELETE` of its
old primary key and an `INSERT` of its new row. Each SQL statement must leave a
valid tree, including statements inside an atomic batch. For a nonempty
container, save its descendant rows, delete them leaf-first, replace the parent,
then reinsert descendants parent-first using separate committed `execute` calls.
The current engine rejects deleting and reinserting the same primary key within
one `execute_batch`, so this workaround is not atomic. Retain the parent's `container_id` and
restore each descendant's original primary key (including array UUIDs), value,
formatting, and `order_key`. This preserves descendant identities without
rekeying them, while keeping every intermediate tree valid.

IDs also survive subsequent source edits for matched containers. Changing a
container's ID explicitly requires the same staged deletion and reinsertion,
with its direct children's `parent_id` changed to the new ID.
