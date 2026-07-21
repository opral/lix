---
type: patch
---

Repeated JavaScript SDK opens now reuse prepared WebAssembly plugins.

Lix keeps a bounded plugin preparation cache while preserving a fresh isolated plugin instance for every open.
