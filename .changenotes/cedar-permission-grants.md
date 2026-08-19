---
type: minor
---

Added repository-owned Cedar permissions with a global `lix_permission_grant` sharing model.

Tracked grants now express account, group, or anonymous access to repositories, directories, files, tables, and rows. The grants are shared across every branch and remain extensible through repository-authored Cedar policies.
