---
type: patch
---

Report schema-declared UUID columns as `UUID` in SQL metadata.

`information_schema.columns` now preserves and exposes logical UUID types for schema-declared surfaces such as `lix_account`, including their history and state views.
