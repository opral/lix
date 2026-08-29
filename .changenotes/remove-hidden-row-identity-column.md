---
type: minor
---

Removed the obsolete hidden JSON primary-key projection from current SQL relations.

Current relations now derive identity exclusively from their declared primary-key columns. Cross-relation addresses continue to use opaque row references, and derived columnar accelerators use a private physical identity field that is not part of any SQL schema.
