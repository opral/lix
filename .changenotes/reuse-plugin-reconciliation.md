---
type: patch
---

Improved the speed of batched file updates in workspaces with many files.

Lix now reuses the plugin reconciliation view for files in the same write batch, avoiding repeated full filesystem scans during common multi-file uploads.
