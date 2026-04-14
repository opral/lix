#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemPayloadChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub untracked: bool,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
}
