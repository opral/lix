---
type: patch
---

The native SDK now reuses compiled WASM components across Lix opens, bounds the
compiled-component cache, and enforces `WasmLimits.max_memory_bytes` during
component instantiation and growth.
