# plugin-binary

Rust/WASM component fallback plugin for binary and otherwise unsupported files.

- Uses `packages/engine/wit/lix-plugin.wit` as the API contract.
- Stores whole-file snapshots as base64 in a single entity row.
- Designed as a catch-all plugin (`match.path_glob = "*"`).
