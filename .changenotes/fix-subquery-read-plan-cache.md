---
type: patch
---

Fixed SQL reads that contain a subquery failing with `LIX_STORAGE_ERROR: shared storage read still has N active handles`.

Statements using `IN (SELECT ...)`, `EXISTS (SELECT ...)` or a scalar subquery were placed in the query planning cache together with the storage read they were planned against, which kept that read alive after the query finished and made the statement error out. These statements now skip the planning cache, so they execute normally.
