pub trait TrackedChangeView {
    fn entity_id(&self) -> &str;
    fn schema_key(&self) -> &str;
    fn schema_version(&self) -> Option<&str>;
    fn file_id(&self) -> Option<&str>;
    fn plugin_key(&self) -> Option<&str>;
    fn snapshot_content(&self) -> Option<&str>;
    fn version_id(&self) -> &str;
    fn writer_key(&self) -> Option<&str>;
}
