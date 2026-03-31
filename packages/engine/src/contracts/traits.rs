use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

use crate::contracts::artifacts::{
    EffectiveRowSet, EffectiveRowsRequest, ExactUntrackedLookupRequest, LiveFilter,
    LiveQueryEffectiveRow, LiveQueryOverlayLane, LiveSnapshotRow, LiveSnapshotStorage,
    LiveStateProjectionStatus, PreparedPublicReadContract, PublicReadExecutionMode, ScanRequest,
    SchemaRegistration, TrackedRow, TrackedTombstoneLookupRequest, TrackedTombstoneMarker,
    TrackedWriteRow, UntrackedRow, UntrackedWriteRow,
};
use crate::contracts::surface::SurfaceRegistry;
use crate::filesystem::runtime::FilesystemTransactionFileState;
use crate::workspace::writer_key::WorkspaceWriterKeyReadView;
use crate::{
    commit::CanonicalCommitReceipt, LixBackend, LixBackendTransaction, LixError, QueryResult,
    ReplayCursor, Value,
};

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

    fn workspace_writer_key_annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<Option<String>>;
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
pub(crate) trait LiveStateTransactionBridge {
    async fn register_live_state_schema(
        &mut self,
        registration: &SchemaRegistration,
    ) -> Result<(), LixError>;

    async fn mark_live_state_projection_ready(&mut self) -> Result<ReplayCursor, LixError>;

    async fn apply_canonical_receipt_to_live_state(
        &mut self,
        receipt: &CanonicalCommitReceipt,
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
pub(crate) trait PendingPublicReadBackend {
    async fn bootstrap_public_surface_registry_with_pending_view(
        &self,
        pending_view: Option<&dyn PendingView>,
    ) -> Result<SurfaceRegistry, LixError>;

    async fn execute_prepared_public_read_with_pending_view(
        &self,
        pending_view: Option<&dyn PendingView>,
        public_read: &dyn PreparedPublicReadExecutor,
    ) -> Result<QueryResult, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait PendingPublicReadTransaction {
    async fn require_live_state_ready(&mut self) -> Result<(), LixError>;

    async fn execute_prepared_public_read_with_pending_view(
        &mut self,
        pending_view: Option<&dyn PendingView>,
        public_read: &dyn PreparedPublicReadExecutor,
    ) -> Result<QueryResult, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait PreparedPublicReadExecutor {
    fn contract(&self) -> PreparedPublicReadContract;

    fn execution_mode(&self) -> PublicReadExecutionMode {
        self.contract().execution_mode()
    }

    async fn execute(&self, backend: &dyn LixBackend) -> Result<QueryResult, LixError>;

    async fn execute_without_freshness_check(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<QueryResult, LixError>;

    async fn execute_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<QueryResult, LixError>;
}
