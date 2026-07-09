---
type: patch
---

Improved `lix_file` read and write performance.

Simple single- and multi-row `lix_file (path, data)` inserts and upserts take a fast path that makes large file writes roughly 10x faster. File bytes are hashed once per write, unchanged chunks skip re-writes, and filesystem sync batches its upserts: in repository benchmarks, a 1,000-row `lix_file` insert dropped from ~95 ms to ~41 ms and a 200-file filesystem cold open from ~780 ms to ~210 ms. `SELECT` queries that project `data` now batch their blob reads.
