---
type: patch
---

Improved root-file listings in workspaces with many nested files.

Lix now uses its filesystem path index for `lix_file` queries filtered by
`directory_id IS NULL`, avoiding an unnecessary full descriptor and blob scan.
