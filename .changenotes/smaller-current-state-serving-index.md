---
type: patch
---

Reduced current-state serving-index storage for large tracked repositories.

Lix now uses one authenticated scoped-range index for point reads, diffs, and sparse state sharing while preserving transactional history and branch semantics.
