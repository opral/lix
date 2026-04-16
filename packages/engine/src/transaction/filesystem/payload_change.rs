#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemPayloadChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub untracked: bool,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub origin_key: Option<String>,
}
