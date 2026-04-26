/// Immutable canonical change fact stored in the changelog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CanonicalChange {
    pub(crate) id: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    /// TODO model as binary for higher performance and avoiding serialization/deserialization?
    pub(crate) snapshot_content: Option<String>,
    /// TODO model as binary for higher performance and avoiding serialization/deserialization?
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

/// Minimal changelog scan request.
///
/// TODO(engine2): add filters and append-order cursors once changelog storage
/// has real append sequence keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChangelogScanRequest {
    pub(crate) limit: Option<usize>,
}
