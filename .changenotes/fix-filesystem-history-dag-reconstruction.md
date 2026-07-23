---
type: minor
---

Fixed `lix_file_history` and `lix_directory_history` reconstruction across commit DAGs.

Composed history rows are now identified by the requested start commit, observed commit, and logical entity. Equal-depth sibling revisions are preserved, while descriptor, blob, direct-directory, plugin-owner, registry, and plugin state is reconstructed from the observed commit's immutable state root instead of inferred from traversal depth.

Plugin-backed file history now follows the durable per-file owner used by live `lix_file`. Plugin upgrades and uninstalls create projection revisions for files owned by the changed plugin, overlapping plugin globs do not reassign existing files, unavailable historical owners return `LIX_PLUGIN_UNAVAILABLE` when `data` is projected, and plugin-state tombstones remain in composed provenance.

Exact public `id` predicates are routed through observed-state descriptor, blob, owner, and plugin-state reads, avoiding a second full observed-root materialization. Commit-provenance traversal and unfiltered history remain bulk operations.

The composed histories now expose `lixcol_source_changes`, a non-null JSON array ordered by change ID. Each element mirrors the stable `lix_change` payload fields: `id`, `entity_pk`, `schema_key`, `file_id`, `snapshot_content`, `metadata`, `created_at`, and `origin_key`. Multiple source changes in one commit produce one logical revision with every source in this array.

This is a breaking SQL catalog change. The misleading singular `lixcol_schema_key`, `lixcol_file_id`, `lixcol_snapshot_content`, `lixcol_change_id`, `lixcol_origin_key`, and `lixcol_metadata` columns were removed from the composed filesystem histories. Inspect the structured `lixcol_source_changes` objects, or join their `id` fields to `lix_change`, when raw provenance is required.
