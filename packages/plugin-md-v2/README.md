# plugin-md-v2

Rust/WASM component Markdown plugin for the Lix engine.

## Current scope

- `detect-changes` parses markdown with `markdown-rs` using GFM + MDX + math + frontmatter options.
- Emits block-level rows (`markdown_v2_block`) plus a document order row (`markdown_v2_document`).
- `apply-changes` materializes markdown from the latest block snapshots and document order.

This establishes a deterministic block-level projection baseline with unit tests and benchmarks.

## Identity Model (v2)

`plugin-md-v2` uses two detect modes for top-level block IDs:

- With `detect_changes.state_context.include_active_state: true`: existing IDs are reused from active state rows whenever blocks can be matched (exact + fuzzy matching).
- Without active state context: ID input = `(node_type, normalized AST fingerprint, occurrence_index)`.
- Fingerprint normalization includes:
  - line ending normalization (`CRLF`/`CR` -> `LF`)
  - Unicode NFC normalization for all string fields

Practical behavior:

- Pure reorder of unchanged blocks keeps IDs stable and only updates the document `order`.
- With active state context, content edits can keep existing IDs and emit only an upsert.
- Without active state context, content edits replace identity:
  - old ID tombstone (`snapshot_content: null`)
  - new ID upsert with latest snapshot
  - updated document `order` containing the new ID
- Cross-type edits (e.g. paragraph -> heading) also produce tombstone + upsert + document update.

## Expected Change Shapes

Common detect scenarios:

- No-op: `[]`
- New file with `N` top-level blocks: `N` block upserts + `1` document row
- Pure reorder: `1` document row only
- Insert one block: `1` block upsert + `1` document row
- Delete one block: `1` block tombstone + `1` document row
- Edit one block: `1` block tombstone + `1` block upsert + `1` document row

This is intentionally different from v1 nested-node identity. v2 tracks identity at top-level block granularity.
