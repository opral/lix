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
    "global",
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
    "global",
];
