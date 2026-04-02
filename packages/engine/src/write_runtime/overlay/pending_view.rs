use crate::contracts::traits::{PendingSemanticRow, PendingSemanticStorage, PendingView};
use crate::write_runtime::buffered::{
    PendingFilesystemOverlay, PendingRegisteredSchemaOverlay, PendingSemanticOverlay,
    PendingWorkspaceWriterKeyOverlay,
};

#[derive(Clone, Default)]
pub(crate) struct PendingTransactionView {
    registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
    semantic_overlay: Option<PendingSemanticOverlay>,
    filesystem_overlay: Option<PendingFilesystemOverlay>,
    workspace_writer_key_overlay: Option<PendingWorkspaceWriterKeyOverlay>,
}

impl PendingTransactionView {
    pub(crate) fn new(
        registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
        semantic_overlay: Option<PendingSemanticOverlay>,
        filesystem_overlay: Option<PendingFilesystemOverlay>,
        workspace_writer_key_overlay: Option<PendingWorkspaceWriterKeyOverlay>,
    ) -> Option<Self> {
        let view = Self {
            registered_schema_overlay,
            semantic_overlay,
            filesystem_overlay,
            workspace_writer_key_overlay,
        };
        view.has_overlays().then_some(view)
    }

    pub(crate) fn has_overlays(&self) -> bool {
        self.registered_schema_overlay.is_some()
            || self.semantic_overlay.is_some()
            || self.filesystem_overlay.is_some()
            || self.workspace_writer_key_overlay.is_some()
    }

    pub(crate) fn registered_schema_overlay(&self) -> Option<&PendingRegisteredSchemaOverlay> {
        self.registered_schema_overlay.as_ref()
    }

    pub(crate) fn filesystem_overlay(&self) -> Option<&PendingFilesystemOverlay> {
        self.filesystem_overlay.as_ref()
    }

    pub(crate) fn semantic_overlay(&self) -> Option<&PendingSemanticOverlay> {
        self.semantic_overlay.as_ref()
    }

    pub(crate) fn workspace_writer_key_overlay(&self) -> Option<&PendingWorkspaceWriterKeyOverlay> {
        self.workspace_writer_key_overlay.as_ref()
    }
}

impl PendingView for PendingTransactionView {
    fn has_overlays(&self) -> bool {
        self.has_overlays()
    }

    fn visible_registered_schema_entries(&self) -> Vec<(String, Option<String>)> {
        self.registered_schema_overlay()
            .map(|overlay| {
                overlay
                    .visible_entries()
                    .map(|(entity_id, entry)| {
                        (entity_id.to_string(), entry.snapshot_content.clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow> {
        self.semantic_overlay()
            .map(|overlay| {
                overlay
                    .visible_rows(storage, schema_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn visible_directory_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow> {
        self.filesystem_overlay()
            .map(|overlay| {
                overlay
                    .visible_directory_rows(storage, schema_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn visible_files(&self) -> Vec<crate::filesystem::runtime::FilesystemTransactionFileState> {
        self.filesystem_overlay()
            .map(|overlay| overlay.visible_files().cloned().collect())
            .unwrap_or_default()
    }

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        self.workspace_writer_key_overlay()
            .and_then(|overlay| {
                overlay.annotation_for_state_row(version_id, schema_key, entity_id, file_id)
            })
            .cloned()
    }
}
