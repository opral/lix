pub(crate) const LIX_STATE_VISIBLE_COLUMNS: [&str; 14] = [
    "entity_id",
    "schema_key",
    "file_id",
    "plugin_key",
    "snapshot_content",
    "schema_version",
    "created_at",
    "updated_at",
    "inherited_from_version_id",
    "change_id",
    "commit_id",
    "untracked",
    "writer_key",
    "metadata",
];

pub(crate) const LIX_STATE_BY_VERSION_MUTABLE_COLUMNS: [&str; 11] = [
    "entity_id",
    "schema_key",
    "file_id",
    "version_id",
    "plugin_key",
    "schema_version",
    "snapshot_content",
    "metadata",
    "writer_key",
    "untracked",
    "inherited_from_version_id",
];

pub(crate) const LIX_STATE_MUTABLE_COLUMNS: [&str; 12] = [
    "entity_id",
    "schema_key",
    "file_id",
    "version_id",
    "lixcol_version_id",
    "plugin_key",
    "schema_version",
    "snapshot_content",
    "metadata",
    "writer_key",
    "untracked",
    "inherited_from_version_id",
];

pub(crate) fn lix_state_visible_columns_without_commit() -> Vec<&'static str> {
    LIX_STATE_VISIBLE_COLUMNS
        .iter()
        .copied()
        .filter(|column| *column != "commit_id")
        .collect()
}
