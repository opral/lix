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
use crate::write_runtime::commit::CanonicalCommitReceipt;
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, ReplayCursor, Value};

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

/// Declarative projection definition boundary.
///
/// `ProjectionTrait` describes:
/// - which tracked/untracked inputs a projection needs
/// - which public surfaces it serves
/// - how hydrated source input is turned into derived rows
///
/// It does not own:
/// - storage hydration
/// - replay/catch-up
/// - readiness/progress/checkpointing
/// - runtime surface binding
///
/// Lifecycle is intentionally not part of the trait. The same projection
/// definition can be registered under different lifecycles.
#[allow(dead_code)]
pub(crate) trait ProjectionTrait {
    fn name(&self) -> &'static str;

    fn inputs(&self) -> Vec<crate::contracts::artifacts::ProjectionInputSpec>;

    fn surfaces(&self) -> Vec<crate::contracts::artifacts::ProjectionSurfaceSpec>;

    fn derive(
        &self,
        input: &crate::contracts::artifacts::ProjectionInput,
    ) -> Result<Vec<crate::contracts::artifacts::DerivedRow>, LixError>;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::ProjectionTrait;
    use crate::contracts::artifacts::{
        DerivedRow, ProjectionHydratedRow, ProjectionInput, ProjectionInputRows,
        ProjectionInputSpec, ProjectionLifecycle, ProjectionRegistration, ProjectionSurfaceSpec,
        RowIdentity, UntrackedRow,
    };
    use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};
    use crate::Value;

    #[test]
    fn same_projection_definition_can_be_registered_under_multiple_lifecycles() {
        let read_time = ProjectionRegistration::new(DemoProjection, ProjectionLifecycle::ReadTime);
        let write_time =
            ProjectionRegistration::new(DemoProjection, ProjectionLifecycle::WriteTime);

        assert_eq!(read_time.projection().name(), "demo_projection");
        assert_eq!(write_time.projection().name(), "demo_projection");
        assert_eq!(read_time.lifecycle(), ProjectionLifecycle::ReadTime);
        assert_eq!(write_time.lifecycle(), ProjectionLifecycle::WriteTime);
    }

    #[test]
    fn projection_trait_stays_declarative_over_inputs_and_surfaces() {
        let projection = DemoProjection;

        assert_eq!(
            projection.inputs(),
            vec![ProjectionInputSpec::untracked("lix_version_ref")]
        );
        assert_eq!(
            projection.surfaces(),
            vec![ProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        );
    }

    #[test]
    fn projection_trait_derives_rows_from_hydrated_input_without_storage_access() {
        let projection = DemoProjection;
        let input_spec = ProjectionInputSpec::untracked("lix_version_ref");
        let input = ProjectionInput::new(vec![ProjectionInputRows::new(
            input_spec,
            vec![ProjectionHydratedRow::Untracked(sample_version_ref_row())],
        )]);

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].surface_name, "lix_version");
        assert_eq!(
            derived[0].values.get("version_id"),
            Some(&Value::Text("version-1".into()))
        );
        assert_eq!(
            derived[0].identity.as_ref().map(|id| id.entity_id.as_str()),
            Some("ref-1")
        );
    }

    #[derive(Clone, Copy)]
    struct DemoProjection;

    impl ProjectionTrait for DemoProjection {
        fn name(&self) -> &'static str {
            "demo_projection"
        }

        fn inputs(&self) -> Vec<ProjectionInputSpec> {
            vec![ProjectionInputSpec::untracked("lix_version_ref")]
        }

        fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
            vec![ProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(&self, input: &ProjectionInput) -> Result<Vec<DerivedRow>, crate::LixError> {
            let Some(rows) = input.rows_for(&ProjectionInputSpec::untracked("lix_version_ref"))
            else {
                return Ok(Vec::new());
            };

            Ok(rows
                .iter()
                .filter_map(|row| match row {
                    ProjectionHydratedRow::Untracked(row) => Some(row),
                    ProjectionHydratedRow::Tracked(_) => None,
                })
                .map(|row| {
                    DerivedRow::new(
                        "lix_version",
                        BTreeMap::from([(
                            "version_id".to_string(),
                            row.values.get("version_id").cloned().unwrap_or(Value::Null),
                        )]),
                    )
                    .with_identity(RowIdentity::from_untracked_row(row))
                })
                .collect())
        }
    }

    fn sample_version_ref_row() -> UntrackedRow {
        UntrackedRow {
            entity_id: "ref-1".into(),
            schema_key: "lix_version_ref".into(),
            schema_version: "1".into(),
            file_id: "file-1".into(),
            version_id: "version-1".into(),
            global: false,
            plugin_key: "demo".into(),
            metadata: None,
            writer_key: None,
            created_at: "2026-04-01T00:00:00Z".into(),
            updated_at: "2026-04-01T00:00:00Z".into(),
            values: BTreeMap::from([("version_id".to_string(), Value::Text("version-1".into()))]),
        }
    }
}
