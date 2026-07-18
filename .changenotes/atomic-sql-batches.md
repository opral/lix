---
type: minor
---

Lix now supports `executeBatch()` for sequential SQL statements that commit atomically.

Each statement keeps its own parameters and result, and a failed statement rolls back the complete batch.
