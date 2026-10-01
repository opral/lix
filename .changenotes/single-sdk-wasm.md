---
type: patch
---

The JavaScript SDK now ships one WebAssembly engine for ordinary opening and maintenance operations, removing the duplicate migration WASM download. The `@lix-js/sdk/migration` API remains available and shares engine initialization with normal bindings. Source builds and artifact consumers should use `build:wasm` and `dist/wasm` instead of the removed `build:migration:wasm` script and `dist/migration-wasm` directory.
