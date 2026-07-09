---
type: patch
---

Removed a 2 GB size ceiling on file data read through SQL.

The `data` column on `lix_file`, `lix_file_by_branch`, and `lix_file_history` now uses a large binary representation, so reads no longer fail when file bytes in a result exceed Arrow's 32-bit offset limit.
