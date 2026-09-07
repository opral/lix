---
type: patch
---

Prevent concurrent writes from incorrectly delaying eligible garbage collection after checkpoint cleanup encounters write conflicts.

Write contention now delays automatic cleanup scheduling without marking reclamation as failed. Retention protections and backoff for genuine storage failures are unchanged.
