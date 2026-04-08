use async_trait::async_trait;
use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::contracts::artifacts::{
    EffectiveRowSet, EffectiveRowsRequest, ExactUntrackedLookupRequest, LiveFilter,
    LiveQueryEffectiveRow, LiveQueryOverlayLane, LiveSnapshotRow, LiveSnapshotStorage,
    LiveStateProjectionStatus, OptionalTextPatch, RowIdentity, ScanRequest, SchemaKey,
    SchemaRegistration, StateHistoryRequest, StateHistoryRow, TrackedRow,
    TrackedTombstoneLookupRequest, TrackedTombstoneMarker, TrackedWriteRow, UntrackedRow,
    UntrackedWriteRow,
};
use crate::contracts::ReplayCursor;
use crate::common::error::LixError;
use crate::common::types::{QueryResult, Value};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingFilesystemDescriptorView {
    pub(crate) directory_id: String,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingFilesystemFileView {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) descriptor: Option<PendingFilesystemDescriptorView>,
    pub(crate) metadata_patch: OptionalTextPatch,
    pub(crate) deleted: bool,
}

#[async_trait(?Send)]
pub(crate) trait WorkspaceWriterKeyReadView {
    #[allow(dead_code)]
    async fn load_annotation(&self, row_identity: &RowIdentity)
        -> Result<Option<String>, LixError>;

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError>;
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

    fn visible_files(&self) -> Vec<PendingFilesystemFileView>;

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>>;
}

pub(crate) trait CompiledSchemaCache {
    fn get_compiled_schema(&self, key: &SchemaKey) -> Option<Arc<JSONSchema>>;

    fn insert_compiled_schema(&self, key: SchemaKey, schema: Arc<JSONSchema>);
}

#[async_trait(?Send)]
pub(crate) trait SqlPreparationMetadataReader {
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

pub(crate) trait PendingStateOverlay {
    fn visible_semantic_rows(
        &self,
        storage: PendingSemanticStorage,
        schema_key: &str,
    ) -> Vec<PendingSemanticRow>;

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>>;

    fn as_pending_view(&self) -> &dyn PendingView;
}

pub(crate) struct PendingStateOverlayRef<'a> {
    view: &'a dyn PendingView,
}

impl<'a> PendingStateOverlayRef<'a> {
    pub(crate) fn new(view: &'a dyn PendingView) -> Self {
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

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        PendingView::workspace_writer_key_annotation_for_state_row(
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

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        PendingView::workspace_writer_key_annotation_for_state_row(
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

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>> {
        PendingView::workspace_writer_key_annotation_for_state_row(
            self.view, version_id, schema_key, entity_id, file_id,
        )
    }

    fn as_pending_view(&self) -> &dyn PendingView {
        self.view
    }
}

pub(crate) trait LiveReadShapeContract {
    fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String;

    fn snapshot_from_projected_row(
        &self,
        schema_key: &str,
        row: &[Value],
        snapshot_index: usize,
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait TrackedReadView {
    async fn load_exact_rows(
        &self,
        request: &crate::contracts::artifacts::BatchRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError>;

    async fn scan_rows(&self, request: &ScanRequest) -> Result<Vec<TrackedRow>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait TrackedTombstoneView {
    async fn scan_tombstones(
        &self,
        request: &ScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait TrackedWriteParticipant {
    async fn apply_tracked_write_batch(
        &mut self,
        batch: &[TrackedWriteRow],
    ) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub(crate) trait UntrackedReadView {
    async fn load_exact_rows(
        &self,
        request: &crate::contracts::artifacts::BatchRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError>;

    async fn scan_rows(&self, request: &ScanRequest) -> Result<Vec<UntrackedRow>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait UntrackedWriteParticipant {
    async fn apply_untracked_write_batch(
        &mut self,
        batch: &[UntrackedWriteRow],
    ) -> Result<(), LixError>;
}

pub(crate) struct LiveReadContext<'a> {
    pub(crate) tracked: &'a dyn TrackedReadView,
    pub(crate) untracked: &'a dyn UntrackedReadView,
    pub(crate) tracked_tombstones: Option<&'a dyn TrackedTombstoneView>,
    pub(crate) workspace_writer_keys: &'a dyn WorkspaceWriterKeyReadView,
}

impl<'a> LiveReadContext<'a> {
    pub(crate) fn new(
        tracked: &'a dyn TrackedReadView,
        untracked: &'a dyn UntrackedReadView,
        workspace_writer_keys: &'a dyn WorkspaceWriterKeyReadView,
    ) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
            workspace_writer_keys,
        }
    }

    pub(crate) fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn TrackedTombstoneView,
    ) -> Self {
        self.tracked_tombstones = Some(tracked_tombstones);
        self
    }
}

#[async_trait(?Send)]
pub(crate) trait EffectiveRowsResolver {
    async fn resolve_effective_rows(
        &self,
        request: &EffectiveRowsRequest,
    ) -> Result<EffectiveRowSet, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait CommittedStateHistoryReader {
    async fn load_committed_state_history_rows(
        &self,
        request: &StateHistoryRequest,
    ) -> Result<Vec<StateHistoryRow>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait LiveStateTransactionBridge {
    async fn register_live_state_schema(
        &mut self,
        registration: &SchemaRegistration,
    ) -> Result<(), LixError>;

    async fn mark_live_state_projection_ready(&mut self) -> Result<ReplayCursor, LixError>;

    async fn advance_live_state_replay_boundary(
        &mut self,
        replay_cursor: &ReplayCursor,
    ) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub(crate) trait LiveStateQueryBackend {
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

    async fn load_exact_untracked_effective_row(
        &self,
        request: &ExactUntrackedLookupRequest,
        requested_version_id: &str,
        overlay_lane: LiveQueryOverlayLane,
    ) -> Result<Option<LiveQueryEffectiveRow>, LixError>;

    async fn tracked_tombstone_shadows_exact_row(
        &self,
        request: &TrackedTombstoneLookupRequest,
    ) -> Result<bool, LixError>;

    async fn load_live_state_projection_status(
        &self,
    ) -> Result<LiveStateProjectionStatus, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait PendingPublicReadTransaction {
    async fn require_live_state_ready(&mut self) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub(crate) trait BlobDataReader {
    async fn load_blob_data_by_hash(
        &self,
        blob_hash: &str,
    ) -> Result<Option<Vec<u8>>, LixError>;
}
