---
type: patch
---

Improved SQL write performance by constructing only the target table's
DataFusion provider for UPDATE, DELETE, and VALUES-based INSERT statements.
Query-backed INSERT statements retain catalog-wide provider registration.
