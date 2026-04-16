/// Narrow shared adapter for commit-authored state-change rows.
///
/// This seam is shared on purpose: `streams/*` converts commit-member change
/// rows into commit-stream payloads, while `sql/*`, `transaction/*`, and
/// `live_state/*` reuse the same row-shaped metadata to derive adjacent runtime
/// effects such as session-selector updates, file-cache refresh targets, and
/// writer-key annotations.
pub(crate) trait StateChangeRecord {
    fn entity_id(&self) -> &str;
    fn schema_key(&self) -> &str;
    fn schema_version(&self) -> Option<&str>;
    fn file_id(&self) -> Option<&str>;
    fn plugin_key(&self) -> Option<&str>;
    fn snapshot_content(&self) -> Option<&str>;
    fn version_id(&self) -> &str;
    fn writer_key(&self) -> Option<&str>;
}
