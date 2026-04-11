use async_trait::async_trait;
use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::common::LixError;
use crate::common::{QueryResult, Value};
use crate::contracts::InstalledPlugin;
#[cfg(test)]
use crate::contracts::{
    EffectiveRowSet, EffectiveRowsRequest, RowIdentity, ScanRequest, TrackedRow,
    TrackedTombstoneMarker, UntrackedRow,
};
use crate::contracts::{
    LiveFilter, LiveSnapshotRow, LiveSnapshotStorage, LiveStateProjectionStatus, OptionalTextPatch,
    SchemaKey, SchemaRegistration, StateHistoryRequest, StateHistoryRow,
};

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

#[cfg(test)]
#[async_trait(?Send)]
pub trait WriterKeyReadView {
    #[allow(dead_code)]
    async fn load_annotation(&self, row_identity: &RowIdentity)
        -> Result<Option<String>, LixError>;

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError>;
}

pub trait PendingView {
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

#[async_trait(?Send)]
pub trait FilesystemPluginMaterializer {
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError>;

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError>;
}

pub trait CompiledSchemaCache {
    fn get_compiled_schema(&self, key: &SchemaKey) -> Option<Arc<JSONSchema>>;

    fn insert_compiled_schema(&self, key: SchemaKey, schema: Arc<JSONSchema>);
}

#[async_trait(?Send)]
pub trait SqlPreparationMetadataReader {
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError>;

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError>;

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError>;
}

pub trait PendingStateOverlay {
    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow>;

    fn writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>>;

    fn as_pending_view(&self) -> &dyn PendingView;
}

pub struct PendingStateOverlayRef<'a> {
    view: &'a dyn PendingView,
}

impl<'a> PendingStateOverlayRef<'a> {
    pub fn new(view: &'a dyn PendingView) -> Self {
        Self { view }
    }
}

impl<T> PendingStateOverlay for T
where
    T: PendingView,
{
    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow> {
        PendingView::visible_semantic_rows(self, storage, schema_key)
    }

    fn writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        PendingView::writer_key_annotation_for_state_row(
            self, version_id, schema_key, entity_id, file_id,
        )
    }

    fn as_pending_view(&self) -> &dyn PendingView {
        self
    }
}

impl PendingStateOverlay for dyn PendingView + '_ {
    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow> {
        PendingView::visible_semantic_rows(self, storage, schema_key)
    }

    fn writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        PendingView::writer_key_annotation_for_state_row(
            self, version_id, schema_key, entity_id, file_id,
        )
    }

    fn as_pending_view(&self) -> &dyn PendingView {
        self
    }
}

impl PendingStateOverlay for PendingStateOverlayRef<'_> {
    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow> {
        PendingView::visible_semantic_rows(self.view, storage, schema_key)
    }

    fn writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        PendingView::writer_key_annotation_for_state_row(
            self.view, version_id, schema_key, entity_id, file_id,
        )
    }

    fn as_pending_view(&self) -> &dyn PendingView {
        self.view
    }
}

pub trait LiveReadShapeContract {
    fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String;

    fn snapshot_from_projected_row(
        &self,
        schema_key: &str,
        row: &[Value],
        snapshot_index: usize,
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError>;
}

#[cfg(test)]
#[async_trait(?Send)]
pub trait TrackedReadView {
    async fn load_exact_rows(
        &self,
        request: &crate::contracts::BatchRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError>;

    #[cfg(test)]
    async fn scan_rows(&self, request: &ScanRequest) -> Result<Vec<TrackedRow>, LixError>;
}

#[cfg(test)]
#[async_trait(?Send)]
pub trait TrackedTombstoneView {
    async fn scan_tombstones(
        &self,
        request: &ScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, LixError>;
}

#[cfg(test)]
#[async_trait(?Send)]
pub trait UntrackedReadView {
    async fn load_exact_rows(
        &self,
        request: &crate::contracts::BatchRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError>;

    #[cfg(test)]
    async fn scan_rows(&self, request: &ScanRequest) -> Result<Vec<UntrackedRow>, LixError>;
}

#[cfg(test)]
pub struct LiveReadContext<'a> {
    pub tracked: &'a dyn TrackedReadView,
    pub untracked: &'a dyn UntrackedReadView,
    pub tracked_tombstones: Option<&'a dyn TrackedTombstoneView>,
    pub writer_keys: &'a dyn WriterKeyReadView,
}

#[cfg(test)]
impl<'a> LiveReadContext<'a> {
    pub fn new(
        tracked: &'a dyn TrackedReadView,
        untracked: &'a dyn UntrackedReadView,
        writer_keys: &'a dyn WriterKeyReadView,
    ) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
            writer_keys,
        }
    }

    pub fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn TrackedTombstoneView,
    ) -> Self {
        self.tracked_tombstones = Some(tracked_tombstones);
        self
    }
}

#[cfg(test)]
#[async_trait(?Send)]
pub trait EffectiveRowsResolver {
    async fn resolve_effective_rows(
        &self,
        request: &EffectiveRowsRequest,
    ) -> Result<EffectiveRowSet, LixError>;
}

#[async_trait(?Send)]
pub trait CommittedStateHistoryReader {
    async fn load_committed_state_history_rows(
        &self,
        request: &StateHistoryRequest,
    ) -> Result<Vec<StateHistoryRow>, LixError>;
}

#[async_trait(?Send)]
pub trait LiveStateTransactionBridge {
    async fn register_live_state_schema(
        &mut self,
        registration: &SchemaRegistration,
    ) -> Result<(), LixError>;

    async fn advance_live_state_replay_boundary(
        &mut self,
        replay_cursor: &crate::contracts::ReplayCursor,
    ) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub trait LiveStateQueryBackend {
    async fn load_live_read_shape_for_table_name(
        &self,
        table_name: &str,
    ) -> Result<Option<Box<dyn LiveReadShapeContract>>, LixError>;

    async fn load_live_snapshot_rows(
        &self,
        storage: LiveSnapshotStorage,
        schema_key: &str,
        version_id: &str,
        filters: &[LiveFilter],
    ) -> Result<Vec<LiveSnapshotRow>, LixError>;

    async fn normalize_live_snapshot_values(
        &self,
        schema_key: &str,
        snapshot_content: Option<&str>,
    ) -> Result<BTreeMap<String, Value>, LixError>;

    async fn load_live_state_projection_status(
        &self,
    ) -> Result<LiveStateProjectionStatus, LixError>;
}

#[async_trait(?Send)]
pub trait PendingPublicReadTransaction {
    async fn require_live_state_ready(&mut self) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub trait BlobDataReader {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError>;
}
