---
type: patch
---

Improved RocksDB and SlateDB CRUD performance by reusing compiled schema catalogs across ordinary implicit transactions.

Schema registrations, merges, branch-head changes, and tracked-state repairs still invalidate the cached catalog atomically before the next transaction opens.
