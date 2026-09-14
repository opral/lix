# Excalidraw rows and editing

The Excalidraw plugin maps a scene into three tables. Every query and write should
scope `lixcol_file_id` when more than one drawing is present, because native IDs
are unique within a drawing, not across all files.

| Table | Identity | Authoritative content |
| --- | --- | --- |
| `excalidraw_scene` | `id = 'scene'` | `scene_json`: all root metadata except `elements` and `files`, including `appState`, version, source, and unknown properties |
| `excalidraw_element` | Native Excalidraw element `id` | `element_json`: complete object, including type, geometry, text, bindings, deletion state, and unknown properties |
| `excalidraw_file` | Native key in the `files` object | `file_json`: complete attachment object, including data URL and metadata |

`element_json.id` must match the row ID. Element type and soft deletion live only
in the JSON object; there are no duplicate columns to keep synchronized. Query
properties with JSON extraction:

```sql
SELECT id, element_json->>'type' AS element_type, element_json
FROM excalidraw_element
WHERE lixcol_file_id = $1
ORDER BY order_key, id;
```

Read the JSON object, change the desired properties, and write it back as a JSONB
parameter. Keep fields you did not intend to edit:

```sql
UPDATE excalidraw_element
SET element_json = $1
WHERE id = $2 AND lixcol_file_id = $3;

UPDATE excalidraw_scene
SET scene_json = $1
WHERE lixcol_file_id = $2;
```

Setting `element_json.isDeleted` performs Excalidraw soft deletion. SQL `DELETE`
removes the element from the array. The plugin preserves references such as
`boundElements`, group IDs, and file IDs as supplied; applications remain
responsible for maintaining their relationships when deleting or renaming IDs.

## Inserts and ordering

Formatting columns have defaults or accept null. Ordinary creation does not
require constructing internal templates or spelling JSON map keys:

```sql
INSERT INTO excalidraw_element (id, element_json, lixcol_file_id)
VALUES ($1, $2, $3);

INSERT INTO excalidraw_file (id, file_json, lixcol_file_id)
VALUES ($1, $2, $3);
```

A first attachment creates the `files` collection when the original drawing did
not have one. Deleting it again restores the original collection-presence hint.

Both child tables sort by `order_key`, then native `id`. The default key is `80`;
equal keys are supported and use deterministic ID order. For a particular visual
position, explicitly set `order_key` with `lix_order_between(previous, next)`.
Use null for an unbounded end. When neighboring keys tie, give the surrounding
rows distinct keys before requesting a position between them. A file edit that
inserts between tied keys reassigns keys to preserve the requested array order.

## Exact roundtrips

Unchanged durable rows reproduce exact accepted UTF-8 JSON bytes, including
whitespace, escaped strings, numeric spelling, unknown nested values, element
order, and embedded files. Native IDs remain stable across edits and reopening.

`source_json`, `leading_json`, `prefix_json`, `template_json`, and collection tails
are formatting hints. Keep them when updating existing rows; omit the optional
hints for new content. Edited JSONB content remains authoritative over stale
spelling. Existing scalar-property edits retain unrelated object spelling where
possible. Root metadata changes may canonicalize root layout. Unrepresentable
JSON, duplicate native IDs, or inconsistent ID/payload pairs reject the entire
statement without changing accepted file bytes.

Private indexes are disposable. The engine supplies durable rows to full
serialization for cold SQL edits and to cold parsing for file edits. Native
`serialize_changes` by itself requires an accepted indexed snapshot to retain
custom ordering and layout; use the harness's full `serialize` hook after clearing
private state.

## Scaling

Warm content edits look up native IDs in paged indexes and read only changed
element spans. Multiple field edits in one element and edits to multiple elements
remain sparse. Indexed element no-ops emit no row or state changes. Length changes use a
bounded overlay of up to 4,096 distinct element entries; structural changes and
overlay compaction rebuild indexes. Rebuilds apply accumulated shifts in a linear
sweep. Index pages stay below default operation limits, including long IDs.

Import, structural changes, attachment edits, and the first SQL edit after a cold
reopen still process the document. End-to-end SQL also pays host storage and
transaction costs, so bounded guest reads do not imply file-size-independent SQL
latency. See [Excalidraw performance](performance/excalidraw.md) for measurements
and reproducible commands.
