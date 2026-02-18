#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotContent {
    pub(crate) markdown: String,
}
