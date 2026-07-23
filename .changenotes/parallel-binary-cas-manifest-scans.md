---
type: patch
---

Improved batched binary file read latency when multiple files span several storage chunks.

Lix now overlaps the independent manifest scans for those files with bounded concurrency while retaining ordered results and a single batched chunk fetch.
