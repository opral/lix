---
type: patch
---

Improved `executeBatch()` performance for read-only workloads without adding a new API.

Pure-read batches now reuse one storage snapshot and prepared SQL session, while batches containing writes or durable runtime functions retain their existing transactional semantics.
