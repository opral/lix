use async_trait::async_trait;

use crate::filesystem::runtime::FilesystemTransactionFileState;
pub(crate) use crate::live_state::{EffectiveRowSet, EffectiveRowsRequest};
use crate::live_state::{LiveReadViews, RowIdentity};
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PendingSemanticStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingSemanticRow {
    pub(crate) storage: PendingSemanticStorage,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) tombstone: bool,
}

pub(crate) trait PendingView {
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

    fn visible_files(&self) -> Vec<FilesystemTransactionFileState>;

    fn workspace_writer_key_annotation(&self, identity: &RowIdentity) -> Option<Option<String>>;

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>>;
}

#[async_trait(?Send)]
pub(crate) trait EffectiveRowProvider {
    async fn resolve_effective_rows(
        &self,
        request: &EffectiveRowsRequest,
    ) -> Result<EffectiveRowSet, LixError>;
}

#[async_trait(?Send)]
impl EffectiveRowProvider for LiveReadViews<'_> {
    async fn resolve_effective_rows(
        &self,
        request: &EffectiveRowsRequest,
    ) -> Result<EffectiveRowSet, LixError> {
        crate::live_state::resolve_effective_rows(request, self).await
    }
}

#[allow(dead_code)]
pub(crate) trait ReadContext: EffectiveRowProvider {
    fn pending_view(&self) -> Option<&dyn PendingView> {
        None
    }
}
