use crate::sql::{
    SqlPreparationPendingOverlay, SqlPreparationPendingRow, SqlPreparationPendingStorage,
};
use crate::transaction::buffered::{
    PendingFilesystemOverlay, PendingRegisteredSchemaOverlay, PendingSemanticOverlay,
};

use super::{PendingFilesystemFileView, PendingOverlay, PendingSemanticRow};

#[derive(Clone, Default)]
pub(crate) struct PendingWriteOverlay {
    registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
    semantic_overlay: Option<PendingSemanticOverlay>,
    filesystem_overlay: Option<PendingFilesystemOverlay>,
}

impl PendingWriteOverlay {
    pub(crate) fn new(
        registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
        semantic_overlay: Option<PendingSemanticOverlay>,
        filesystem_overlay: Option<PendingFilesystemOverlay>,
    ) -> Option<Self> {
        let view = Self {
            registered_schema_overlay,
            semantic_overlay,
            filesystem_overlay,
        };
        view.has_overlays().then_some(view)
    }

    pub(crate) fn has_overlays(&self) -> bool {
        self.registered_schema_overlay.is_some()
            || self.semantic_overlay.is_some()
            || self.filesystem_overlay.is_some()
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

    #[allow(dead_code)]
    pub(crate) fn visible_all_semantic_rows(&self) -> Vec<PendingSemanticRow> {
        self.semantic_overlay()
            .map(|overlay| overlay.visible_all_rows().cloned().collect())
            .unwrap_or_default()
    }
}

impl PendingOverlay for PendingWriteOverlay {
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

    fn visible_semantic_rows(&self, untracked: bool, schema_key: &str) -> Vec<PendingSemanticRow> {
        self.semantic_overlay()
            .map(|overlay| {
                overlay
                    .visible_rows(untracked, schema_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn visible_directory_rows(&self, untracked: bool, schema_key: &str) -> Vec<PendingSemanticRow> {
        self.filesystem_overlay()
            .map(|overlay| {
                overlay
                    .visible_directory_rows(untracked, schema_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn visible_files(&self) -> Vec<PendingFilesystemFileView> {
        self.filesystem_overlay()
            .map(|overlay| {
                overlay
                    .visible_files()
                    .map(PendingFilesystemFileView::from)
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl SqlPreparationPendingOverlay for PendingWriteOverlay {
    fn visible_registered_schema_entries(&self) -> Vec<(String, Option<String>)> {
        PendingOverlay::visible_registered_schema_entries(self)
    }

    fn visible_registered_schema_rows(
        &self,
        storage: SqlPreparationPendingStorage,
    ) -> Vec<SqlPreparationPendingRow> {
        let untracked = match storage {
            SqlPreparationPendingStorage::Tracked => false,
            SqlPreparationPendingStorage::Untracked => true,
        };

        PendingOverlay::visible_semantic_rows(self, untracked, "lix_registered_schema")
            .into_iter()
            .map(|row| SqlPreparationPendingRow {
                snapshot_content: row.snapshot_content,
                tombstone: row.tombstone,
            })
            .collect()
    }
}
