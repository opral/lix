---
type: patch
---

Reduced memory use for SQL row counts.

Count-only scans avoid loading row values and use exact collection counts when the current columnar layout can serve the query. Filtered counts retain the values needed to evaluate their predicates.
