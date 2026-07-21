---
type: patch
---

Lix now batches exact correlated live-state row reads for ID-constrained `lix_file` queries and writes.

This removes the previous Cartesian point-read expansion for batches of up to 32 files and the per-file prefix-scan fallback for larger batches. `SELECT`, `UPDATE`, `DELETE`, and ID-based upsert conflict probes use aligned exact identities while preserving branch/global visibility, tombstones, tracked and untracked rows, and staged transaction overlays.
