---
type: patch
---

Fresh independent WASM plugin documents now open and drain concurrently within
the bounded live-Store working set. Create-reservation preflights use aligned
batch reads, while semantic rows are still eagerly validated and persisted.
