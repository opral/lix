---
type: patch
---

Fixed in-memory Lix on Node.js when the native addon is unavailable by falling back to the bundled WebAssembly engine.

This restores compatibility for memory-backed consumers on musl-based Linux distributions such as Alpine while keeping native-only features unchanged.
