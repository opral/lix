---
type: patch
---

Accelerated exact `lix_file` path batch reads by moving owned file bytes directly into SQL results instead of copying them through Arrow.

Exact batch reads also acknowledge delivered plugin-backed file views so later file writes retain the correct session merge base.
