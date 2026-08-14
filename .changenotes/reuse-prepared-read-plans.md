---
type: patch
---

Repeated SQL reads are around 35-40% faster.

Lix now reuses the prepared query plan for a statement it has already executed instead of re-running the query planner every time. Ordered reads — the common `SELECT ... WHERE id IN (...) ORDER BY id` shape behind entity point reads and full scans — previously fell outside the plan cache entirely and were planned from scratch on every call.
