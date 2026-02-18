# plugin-md-v2

Rust/WASM component Markdown plugin for the Lix engine.

## Scope

Minimal v2 implementation:

- `detect-changes` parses markdown with `markdown-rs` using GFM + MDX + math + frontmatter options.
- Emits a single root row (`entity_id = "root"`) when parsed AST changes.
- `apply-changes` materializes file bytes from that root row snapshot.

This is intentionally minimal to establish test and benchmark baselines.
