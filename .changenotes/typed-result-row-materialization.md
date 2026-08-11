---
type: patch
---

Faster SQL result materialization for row-heavy reads.

Query results are now read column-by-column out of their typed Arrow arrays instead of building one intermediate scalar per cell, which cuts about 13% off large tracked-state scans. Result columns whose type Lix cannot represent — dates, timestamps, decimals, lists, structs — now raise a clear type error instead of silently returning engine debug text; cast them to TEXT, BIGINT, DOUBLE, BOOLEAN, or BYTEA.
