---
type: patch
---

Improved ordinary SQL read performance by skipping deterministic runtime-function state work when a query does not call `lix_uuid_v7()` or `lix_timestamp()`.

Queries that use durable runtime functions retain their existing deterministic sequencing and persistence behavior.
