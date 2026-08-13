---
type: minor
---

Moved Rust plugin authoring into `lix::plugin` and removed the separate `lix-plugin-api` crate.

Plugin crates now depend only on `lix` and compile the target-selected authoring surface for `wasm32-wasip2`. Plugins remain ordinary tracked files under `/.lix/plugins/`; no separate installation API is introduced.

The engine-side plugin Component contract moved from `lix::wasm` to
`lix::plugin::runtime`, and plugin page/layout encodings now live under
`lix::plugin::wire`. `lix::wasm` is reserved for Lix's general WebAssembly
compute layer. This is a hard cut with no compatibility re-exports.
