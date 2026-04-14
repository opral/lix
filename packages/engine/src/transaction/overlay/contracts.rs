use crate::sql::OptionalTextPatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PendingSemanticStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone)]
pub struct PendingSemanticRow {
    pub storage: PendingSemanticStorage,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFilesystemDescriptorView {
    pub directory_id: String,
    pub name: String,
    pub extension: Option<String>,
    pub metadata: Option<String>,
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFilesystemFileView {
    pub file_id: String,
    pub version_id: String,
    pub untracked: bool,
    pub descriptor: Option<PendingFilesystemDescriptorView>,
    pub metadata_patch: OptionalTextPatch,
    pub deleted: bool,
}

pub trait PendingOverlay {
    fn has_overlays(&self) -> bool {
        false
    }

    fn visible_registered_schema_entries(&self) -> Vec<(String, Option<String>)>;

    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow>;

    fn visible_directory_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow>;

    fn visible_files(&self) -> Vec<PendingFilesystemFileView>;

    fn writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>>;
}
