---
type: patch
---

Improved root-directory listings in workspaces with many directories.

Lix now uses its filesystem path index for `lix_directory` queries filtered by
`parent_id IS NULL`, avoiding an unnecessary full descriptor scan.
