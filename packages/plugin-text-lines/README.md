# plugin-text-lines

Rust/WASM component plugin that models files as line entities for the Lix engine.

- Uses `packages/engine/wit/lix-plugin.wit`.
- `detect-changes` emits:
  - `text_line` rows for inserted/deleted lines (order-preserving line matching, Git-style)
  - one `text_document` row with ordered `line_ids`
- `apply-changes` rebuilds exact bytes from the latest projection.

This plugin is byte-safe (works with non-UTF-8 files) by storing line content as hex in
snapshot payloads.
