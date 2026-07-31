---
type: patch
---

Large immutable binary chunks now bypass repeated LSM-tree rewriting while retaining transactional visibility and snapshot-safe reads.

Git LFS replay stores content-addressed chunk payloads once in the backing object store and keeps only compact transactional markers in SlateDB.
