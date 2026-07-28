# Lix Plugin API v2

This is the small Rust authoring API for Lix's production Component-v2 ABI.
It does **not** replace Wasm or WIT. It hides packet framing, pages,
attachments, resources, and transport limits while exposing bounded lazy byte
sources where a format genuinely needs local context.

An author implements one trait with four required lifecycle operations. The
trait also has one stateless conflict hook whose default deterministically
takes canonical `b` (or deletes when `b` is absent):

```rust
use lix_plugin_api_v2 as lix;
use std::sync::Arc;

struct MyFormat;

impl lix::FormatPlugin for MyFormat {
    // The API runtime's `fork` clones this value. Use Arc or another persistent
    // structure so speculative transitions stay cheap.
    type Document = Arc<MyPersistentDocument>;

    fn open_file(input: lix::OpenFile<'_>) -> lix::Result<(Self::Document, lix::Changes)> {
        // Explicit cold-path materialization for a parser that needs all bytes.
        let bytes = input.source.read_all()?;
        let (document, changes) =
            MyPersistentDocument::parse(bytes, input.file.path.as_deref(), input.creates)?;
        Ok((Arc::new(document), lix::changes(changes)))
    }

    fn open_entities(mut input: lix::OpenEntities<'_>)
        -> lix::Result<(Self::Document, lix::Edits)>
    {
        // If `input.accepted` is present, edits are relative to that verified
        // checkpoint; otherwise they are relative to an empty file.
        let (document, edits) =
            MyPersistentDocument::from_entities(&mut input.entities, input.accepted)?;
        Ok((Arc::new(document), lix::edits(edits)))
    }

    fn file_changed(document: &Self::Document, update: lix::FileUpdate<'_>)
        -> lix::Result<(Self::Document, lix::Changes)>
    {
        // `update.edits` are verified base-relative splices. For a large
        // replacement, `update.read_insert(edit)` reads only that range.
        let (next, changes) = document.apply_file_splices(&update.edits, update.creates)?;
        Ok((Arc::new(next), lix::changes(changes)))
    }

    fn entities_changed(document: &Self::Document, update: lix::EntityUpdate<'_>)
        -> lix::Result<(Self::Document, lix::Edits)>
    {
        let (next, edits) = document.apply_entity_changes(&mut update.changes)?;
        Ok((Arc::new(next), lix::edits(edits)))
    }
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v2::export_v2!(MyFormat);
```

The API package owns generated WIT traits, `document`/cursor resources, packet-v1
encoding, bounded pages, lazy snapshots, output attachments, EOF rules, and
host error mapping. It lowers to the `lix:plugin@2.1.0` world.

## Start a plugin

```toml
[dependencies]
lix_plugin_api_v2 = "0.8.4"
```

Build the crate as a `cdylib` for `wasm32-wasip2`, implement `FormatPlugin`,
and invoke `export_v2!(MyFormat)` under `cfg(target_family = "wasm")`. The
plugin archive still supplies a normal Lix manifest, for example:

```json
{
  "runtime": "wasm-component-v2",
  "api_version": "2.1.0",
  "key": "example-format",
  "entry": "plugin.wasm"
}
```

Add the matcher and schemas appropriate to the format. The archive contains
the compiled Wasm component; it does not need the host engine crate.

## Four lifecycle methods and one conflict hook

| Method | Author reads | Author returns | Coordinate/base rule |
|---|---|---|---|
| `open_file` | `input.file`, `input.source`, `input.creates` | initial complete entity creates | First import of file bytes. |
| `open_entities` | `input.file`, `input.entities`, optionally `input.accepted` | renderer edits | `accepted` is the edit base when present; otherwise the base is empty. |
| `file_changed` | `update.before`/`after`, verified `update.edits`, optional bounded sources | complete upserts/tombstones | Splices are relative to the prior accepted file. |
| `entities_changed` | final `update.changes`, `update.before`/`after`, optional `before_source` | sparse `ByteEdit`s | Edits are relative to the accepted materialized file. |
| `resolve_conflict` | one lazy `base`/`a`/`b` semantic collision | `TakeBase`, `TakeA`, `TakeB`, `Replace`, or `Delete` | Stateless; `a` and `b` are canonically ordered independently of merge direction. |

Most formats can keep the default `resolve_conflict`. Override it only for a
bounded, safe format rule. `ConflictValue::read()` is explicit: returning a
`Take*` result retains the selected host-owned value without copying it through
Wasm, while `Replace` carries one newly composed complete snapshot.

## TSV-shaped entity example

A small table plugin can keep its format-specific state private and emit a
complete row snapshot such as:

```rust
let row_id = input.creates.id(new_row_ordinal)?;
let snapshot = r#"{
  "id": "…",
  "order": "00000042",
  "cells": ["alpha", "one"]
}"#
    .replace("…", &row_id)
    .into_bytes();

let change = input.creates.keyless(lix::EntityChange::upsert(
    "tsv_row",
    vec![row_id],
    snapshot,
))?;
```

The `order` fact is deliberate: row position is not an identity, but a row
reorder is still semantic and must emit an updated complete snapshot. On a
localized edit, preserve the acknowledged `id`, change only the affected row
snapshot, and return exactly that `EntityChange`. In the reverse direction,
turn only the changed row into a base-relative `ByteEdit`.

## Performance rules

- `Source::read_all()` is explicit. It is appropriate for cold parsing, not a
  default warm-edit action.
- Use `FileUpdate.edits` rather than comparing complete before/after files.
  For a large splice, call `update.read_insert(edit)`; it reads only the
  referenced range from the lazy `after_source`.
- Iterate `EntityReader` and `EntityChangeReader`; do not collect a whole file
  unless the format genuinely needs it.
- Return `Changes` and `Edits` lazily. Large snapshots and inserts are attached
  out of line automatically.
- In `resolve_conflict`, inspect lengths before reading values and preserve the
  zero-copy deterministic fallback for inputs too large for the format's
  bounded heuristic.
- Keep `Document` immutable and persistent. The API runtime makes `fork` a clone; the
  format controls whether that clone is cheap.

JSON, CSV, Markdown, and Excalidraw in this branch are executable examples of
the same interface. Their parsers, stable identity rules, and semantic models
remain separate by design.

Creation is inferred from the schema. A creatable schema has `/id` as its
primary key and gives that string property both `"format": "uuid"` and
`"x-lix-default": "lix_uuid_v7()"`. Keep durable IDs for existing entities.
For a new entity, choose a transition-local `u32` reference, derive the UUID
with `input.creates.id(local_ref)`, and pass the complete upsert through
`input.creates.keyless(...)`. The adapter verifies and removes the derived ID
from the packet; the host applies the schema default, validates the completed
snapshot, and returns the same canonical UUIDv7. An array position, row number,
or byte offset is never a durable identity.

## Semantic contract

- Emit complete entity snapshots for an upsert, or an `EntityChange::delete`
  tombstone. Prefer `EntityChange::upsert` and `EntityChange::delete`; the API package
  does not merge partial records for a format.
- `ChangeEffect::Content` is the normal semantic change. Use
  `ChangeEffect::FormatOnly` when the same facts are serialized differently
  (for example a CSV dialect or Markdown formatting change) and that lexical
  fact must become durable.
- `ByteEdit` coordinates refer to the accepted base file. Emit them in
  ascending, non-overlapping order; do not make the next offset depend on a
  previous inserted length.
- If order is semantic (CSV rows, array items, or Markdown siblings), put a
  stable order fact in each complete snapshot and compare it during a file
  change. Matching only an entity's identity/content is not enough to make a
  reorder a no-op.
- `changes` and `edits` accept normal lazy iterators. If generating a later
  record can fail, use `try_changes` or `try_edits` instead.
