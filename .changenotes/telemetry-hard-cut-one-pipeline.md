---
type: patch
---

Lix is now the only producer of engine performance records: every stable production span goes through the per-engine TelemetrySink.

Hosts attach one sink. JavaScript `onSpan` and native tracing export the same eleven names and ids. Coordinated commits carry `lix.commit_cohort_id` and links to every logical transaction. Dropped operations finish as `cancelled`. Legacy INFO names (`SELECT`, `SQL batch`, `lix.opened`) are gone; the SQL verb lives on `db.operation.name`.
