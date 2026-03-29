use super::write_plan::{
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
