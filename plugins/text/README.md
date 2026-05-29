# text-plugin

Rust/WASM component plugin that models files as line entities for the Lix engine.

- Uses `packages/engine/wit/lix-plugin.wit`.
- Provides `manifest.json` for install metadata (`text_plugin`).
- Provides Lix schema docs:
  - `schema/text_line.json`
  - `schema/text_document.json`
- `detect-changes` emits:
  - `text_line` rows for inserted/deleted lines (order-preserving line matching, Git-style)
  - one `text_document` row with ordered `line_ids`
- `apply-changes` rebuilds exact bytes from the latest projection.

This plugin is byte-safe (works with non-UTF-8 files) by storing line content as base64 in
snapshot payloads.

## Benchmarks

Run plugin micro-benchmarks:

```bash
cargo bench -p text_plugin --bench detect_changes
cargo bench -p text_plugin --bench apply_changes
```
