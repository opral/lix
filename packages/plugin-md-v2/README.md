# plugin-md-v2

Rust/WASM component Markdown plugin for the Lix engine.

## Current scope

- `detect-changes` parses markdown with `markdown-rs` using GFM + MDX + math + frontmatter options.
- Emits block-level rows (`markdown_v2_block`) plus a document order row (`markdown_v2_document`).
- `apply-changes` materializes markdown from the latest block snapshots and document order.

This establishes a deterministic block-level projection baseline with unit tests and benchmarks.
