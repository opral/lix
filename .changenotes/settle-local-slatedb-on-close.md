---
type: patch
---

Reduced local SlateDB disk usage by releasing completed compactor safety checkpoints and collecting obsolete WAL and compacted SST files when the last handle closes.
