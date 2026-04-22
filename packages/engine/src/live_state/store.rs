#![allow(dead_code)]

use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::version::CommittedVersionFrontier;
use crate::LixError;

use super::{
    lifecycle::LiveStateSnapshot,
    materialize::{LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildRequest},
    snapshot_queries::{
        load_live_read_shape_for_table_name, load_live_snapshot_rows_with_backend,
        normalize_live_snapshot_values_with_backend, LiveRowShapeContract, LiveStateQueryBackend,
    },
    ExactLiveRowQuery, LiveRow, LiveRowQuery, LiveStateMode, ProjectionStatus, ReplayCursor,
    SchemaRegistration,
};

pub(crate) type LiveStateBackendRef<'a> = &'a (dyn crate::LixBackend + 'a);
pub(crate) type LiveStateExecutorRef<'a> = &'a mut (dyn crate::QueryExecutor + 'a);
pub(crate) type LiveStateTransactionRef<'a> = &'a mut (dyn crate::LixBackendTransaction + 'a);

#[async_trait(?Send)]
impl LiveStateQueryBackend for dyn crate::LixBackend + '_ {
    async fn load_live_read_shape_for_table_name(
        &self,
        table_name: &str,
    ) -> Result<Option<Box<dyn LiveRowShapeContract>>, LixError> {
        load_live_read_shape_for_table_name(self, table_name)
            .await
            .map(|shape| shape.map(|shape| Box::new(shape) as Box<dyn LiveRowShapeContract>))
    }

    async fn load_live_snapshot_rows(
        &self,
        storage: super::LiveSnapshotStorage,
        schema_key: &str,
        version_id: &str,
        filters: &[super::LiveFilter],
    ) -> Result<Vec<super::LiveSnapshotRow>, LixError> {
        load_live_snapshot_rows_with_backend(self, storage, schema_key, version_id, filters).await
    }

    async fn normalize_live_snapshot_values(
        &self,
        schema_key: &str,
        snapshot_content: Option<&str>,
    ) -> Result<BTreeMap<String, crate::Value>, LixError> {
        normalize_live_snapshot_values_with_backend(self, schema_key, snapshot_content).await
    }
}

#[async_trait(?Send)]
impl super::LiveStateTransactionBridge for dyn crate::LixBackendTransaction + '_ {
    async fn register_live_state_schema(
        &mut self,
        registration: &SchemaRegistration,
    ) -> Result<(), LixError> {
        super::register_schema_in_transaction(self, registration.clone()).await
    }

    async fn advance_live_state_replay_boundary(
        &mut self,
        replay_cursor: &ReplayCursor,
    ) -> Result<(), LixError> {
        super::projection::replay::advance_live_state_projection_replay_boundary_to_cursor_in_transaction(
            self,
            replay_cursor,
        )
        .await
    }
}

/// Owner-facing read surface for durable live-state persistence.
#[async_trait(?Send)]
pub(crate) trait LiveStateReadStore {
    async fn require_ready(&self) -> Result<(), LixError>;

    async fn projection_status(&self) -> Result<ProjectionStatus, LixError>;

    async fn scan_live_rows(&self, request: &LiveRowQuery) -> Result<Vec<LiveRow>, LixError>;

    async fn load_exact_live_row(
        &self,
        request: &ExactLiveRowQuery,
    ) -> Result<Option<LiveRow>, LixError>;
}

/// Owner-facing write surface for durable live-state persistence.
#[async_trait(?Send)]
pub(crate) trait LiveStateWriteStore {
    async fn register_schema(&mut self, registration: &SchemaRegistration) -> Result<(), LixError>;

    async fn write_live_rows(&mut self, rows: &[LiveRow]) -> Result<(), LixError>;

    async fn mark_ready_at_latest_replay_cursor(&mut self) -> Result<ReplayCursor, LixError>;
}

/// Owner-facing rebuild/materialization surface for durable live-state persistence.
#[async_trait(?Send)]
pub(crate) trait LiveStateMaterializeStore {
    async fn rebuild_plan(
        &mut self,
        request: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildPlan, LixError>;

    async fn apply_rebuild_plan(
        &mut self,
        plan: &LiveStateRebuildPlan,
    ) -> Result<LiveStateApplyReport, LixError>;

    async fn rebuild_scope(
        &mut self,
        request: &LiveStateRebuildRequest,
    ) -> Result<LiveStateApplyReport, LixError>;
}

/// Read-side lifecycle surface for live-state readiness and replay status.
#[async_trait(?Send)]
pub(crate) trait LiveStateLifecycleReadStore {
    async fn load_live_state_snapshot(&self) -> Result<LiveStateSnapshot, LixError>;

    async fn load_latest_replay_cursor(&self) -> Result<Option<ReplayCursor>, LixError>;

    async fn load_live_state_mode(&self) -> Result<LiveStateMode, LixError>;
}

/// Write-side lifecycle surface for transaction-scoped live-state persistence.
#[async_trait(?Send)]
pub(crate) trait LiveStateLifecycleWriteStore {
    async fn load_live_state_snapshot(&mut self) -> Result<LiveStateSnapshot, LixError>;

    async fn load_latest_replay_cursor(&mut self) -> Result<Option<ReplayCursor>, LixError>;

    async fn ensure_live_state_status_row(&mut self) -> Result<(), LixError>;

    async fn try_claim_live_state_bootstrap(&mut self) -> Result<bool, LixError>;

    async fn load_current_committed_frontier(
        &mut self,
    ) -> Result<CommittedVersionFrontier, LixError>;

    async fn load_current_applied_frontier(
        &mut self,
    ) -> Result<Option<CommittedVersionFrontier>, LixError>;

    async fn mark_live_state_ready(
        &mut self,
        cursor: &ReplayCursor,
        frontier: &CommittedVersionFrontier,
    ) -> Result<(), LixError>;

    async fn mark_live_state_ready_without_cursor(
        &mut self,
        frontier: &CommittedVersionFrontier,
    ) -> Result<(), LixError>;

    async fn mark_live_state_mode(&mut self, mode: LiveStateMode) -> Result<(), LixError>;

    async fn mark_live_state_mode_with_cursor_and_frontier(
        &mut self,
        mode: LiveStateMode,
        cursor: &ReplayCursor,
        frontier: Option<&CommittedVersionFrontier>,
    ) -> Result<(), LixError>;

    async fn stamp_live_state_durable_consumer_cursor(
        &mut self,
        cursor: &ReplayCursor,
    ) -> Result<(), LixError>;

    async fn clear_live_state_durable_consumer_cursor(&mut self) -> Result<(), LixError>;
}

/// Non-transactional lifecycle mutation surface for live-state persistence.
#[async_trait(?Send)]
pub(crate) trait LiveStateLifecycleAdminStore {
    async fn init_live_state_status_storage(&self) -> Result<(), LixError>;

    async fn try_claim_live_state_bootstrap(&self) -> Result<bool, LixError>;

    async fn load_current_committed_frontier(&self) -> Result<CommittedVersionFrontier, LixError>;

    async fn mark_live_state_mode(&self, mode: LiveStateMode) -> Result<(), LixError>;

    async fn mark_live_state_ready(
        &self,
        cursor: &ReplayCursor,
        frontier: &CommittedVersionFrontier,
    ) -> Result<(), LixError>;

    async fn stamp_live_state_durable_consumer_cursor(
        &self,
        cursor: &ReplayCursor,
    ) -> Result<(), LixError>;
}

/// Read-side committed-frontier surface for live-state owner logic.
#[async_trait(?Send)]
pub(crate) trait LiveStateFrontierReadStore {
    async fn load_version_head_commit_id(
        &mut self,
        version_id: &str,
    ) -> Result<Option<String>, LixError>;

    async fn load_version_head_commit_map(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError>;

    async fn load_current_committed_version_frontier(
        &mut self,
    ) -> Result<CommittedVersionFrontier, LixError>;
}
