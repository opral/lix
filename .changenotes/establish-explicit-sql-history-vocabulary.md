---
type: minor
---

Lix SQL history surfaces now use one explicit, prefixed vocabulary.

Every public history surface exposes `lixcol_entity_pk`, `lixcol_observed_commit_id`, `lixcol_commit_created_at`, `lixcol_as_of_commit_id`, `lixcol_depth`, and `lixcol_is_deleted`. The ambiguous `start_commit_id` and `lixcol_start_commit_id` spellings, along with all bare generic-history column names, were removed without aliases.

Raw state and typed entity histories expose singular provenance through `lixcol_change_id`, `lixcol_change_created_at`, and `lixcol_origin_key`. Commit time is loaded from the observed commit and no longer silently falls back to change time.

Composed `lix_file_history` and `lix_directory_history` rows expose `lixcol_source_changes` instead of singular change, schema, origin, snapshot, and metadata columns. The non-null JSON array is ordered by change ID and contains `id`, `entity_pk`, `schema_key`, `file_id`, `snapshot_content`, `metadata`, `created_at`, and `origin_key` for each source change.
