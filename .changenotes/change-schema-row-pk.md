---
type: minor
---
Restore `lix_change` record identities as `schema_key`, JSONB `row_pk`, and `file_id`, replacing its public `row_ref`.

History `lixcol_source_changes` objects use the same record identity. Snapshots and identities describe the same underlying schema record. Public history rows and diff commands continue to use opaque row references.
