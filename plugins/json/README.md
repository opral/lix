# plugin-json-v2

Rust/WASM component JSON plugin for the Lix engine.

- Uses `packages/engine/wit/lix-plugin.wit` as the API contract.
- Implements JSON pointer based `detect-changes` and `render`.
- Intended to be installed through `Engine::install_plugin(manifest_json, wasm_bytes)`.
- `render` treats active state as an unordered latest-state projection and reconstructs JSON
  deterministically from upsert rows.
