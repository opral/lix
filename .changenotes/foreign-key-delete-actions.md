---
type: minor
target: lix
---
Support `on_delete: "cascade"` on declared foreign keys, with transactional dependent deletions and merge preparation that honors winning deletes on either branch.

Omitted actions retain `no_action` behavior. Cascades are visible to subsequent transaction statements, roll back atomically, and produce ordinary history. Merge previews use the same preparation and constraint validation as execution. Existing schema declarations retain their serialization and require no migration.
