# plugin-text-lines

Rust/WASM component plugin that models files as line entities for the Lix engine.

- Uses `packages/engine/wit/lix-plugin.wit`.
- Provides `manifest.json` for install metadata (`plugin_text_lines`).
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
cargo bench -p plugin_text_lines --bench detect_changes
cargo bench -p plugin_text_lines --bench apply_changes
```
