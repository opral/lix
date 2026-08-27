---
type: minor
---

Made bundled `lix_*` schemas immutable engine authority instead of deriving
their availability from branch-visible `lix_registered_schema` rows.

Repository format v77 migrates v72-v76 repositories through the existing
copy-and-activate epoch path. Retained built-in registration rows remain
introspection and history projections, while custom registered schemas remain
repository-owned. Sync protocol v2 rejects peers with the older catalog
semantics.
