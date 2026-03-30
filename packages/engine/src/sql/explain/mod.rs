//! Explain stage ownership.
//!
//! This stage owns explain parsing, stable explain artifacts, stage timings,
//! and the final text/JSON rendering returned to callers.

use crate::backend::prepared::PreparedStatement;
use crate::backend::SqlDialect;
use crate::filesystem::runtime::{
    FilesystemDescriptorState, FilesystemTransactionFileState, FilesystemTransactionState,
};
use crate::session::contracts::{SessionDependency, SessionStateDelta};
use crate::sql::backend::{PushdownDecision, PushdownSupport};
use crate::sql::binder::runtime::{RuntimeBindingKind, StatementBindingSource};
use crate::sql::catalog::{
    SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceReadFreshness, SurfaceReadSemantics,
    SurfaceVariant,
};
use crate::sql::executor::contracts::effects::PlanEffects;
use crate::sql::executor::contracts::planned_statement::{
    MutationOperation, MutationRow, SchemaLiveTableRequirement, UpdateValidationPlan,
};
use crate::sql::logical_plan::direct_reads::{
    DirectDirectoryHistoryField, DirectEntityHistoryField, DirectFileHistoryField,
    DirectPublicReadPlan, DirectStateHistoryField, DirectoryHistoryAggregate,
    DirectoryHistoryDirectReadPlan, DirectoryHistoryPredicate, DirectoryHistoryProjection,
    DirectoryHistorySortKey, EntityHistoryDirectReadPlan, EntityHistoryPredicate,
    EntityHistoryProjection, EntityHistorySortKey, FileHistoryAggregate, FileHistoryDirectReadPlan,
    FileHistoryPredicate, FileHistoryProjection, FileHistorySortKey, StateHistoryAggregate,
    StateHistoryAggregatePredicate, StateHistoryDirectReadPlan, StateHistoryPredicate,
    StateHistoryProjection, StateHistoryProjectionValue, StateHistorySortKey,
    StateHistorySortValue,
};
use crate::sql::logical_plan::public_ir::{
    CanonicalAdminKind, CanonicalAdminScan, CanonicalChangeScan, CanonicalFilesystemScan,
    CanonicalStateScan, CanonicalWorkingChangesScan, CommitPreconditions, ExpectedHead,
    FilesystemKind, InsertOnConflict, InsertOnConflictAction, MutationPayload,
    NormalizedPublicReadQuery, OptionalTextPatch, PlannedStateRow, PlannedWrite, ReadCommand,
    ReadContract, ReadPlan, ResolvedRowRef, ResolvedWritePartition, ResolvedWritePlan, RowLineage,
    SchemaProof, ScopeProof, StateSourceKind, StructuredPublicRead, TargetSetProof, VersionScope,
    WriteCommand, WriteLane, WriteMode, WriteModeRequest, WriteOperationKind, WriteSelector,
};
use crate::sql::logical_plan::{
    DependencyPrecision, DependencySpec, InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan,
    PublicWriteLogicalPlan, ResultContract,
};
use crate::sql::optimizer::OptimizerPassTrace;
use crate::sql::physical_plan::plan::{LoweredReadStatement, LoweredStatementBindings};
use crate::sql::physical_plan::{
    LoweredReadProgram, LoweredResultColumn, LoweredResultColumns, PhysicalPlan,
    PreparedPublicReadExecution, PreparedPublicWriteExecution, PublicWriteExecutionPartition,
    PublicWriteMaterialization, TerminalRelationRenderNode, TrackedWriteExecution,
    UntrackedWriteExecution,
};
use crate::sql::semantic_ir::internal::NormalizedInternalStatements;
use crate::sql::semantic_ir::semantics::domain_changes::{
    DomainChangeBatch, PublicDomainChange, SemanticEffect,
};
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest, StateSourceAuthority,
};
use crate::sql::semantic_ir::semantics::surface_semantics::OverlayLane;
use crate::sql::semantic_ir::{
    BoundPublicLeaf, PublicReadSemantics, PublicWriteInvariantTrace, PublicWriteSemantics,
    SemanticStatement,
};
use crate::state::stream::StateCommitStreamChange;
use crate::{LixError, QueryResult, Value};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlparser::ast::{AnalyzeFormatKind, DescribeAlias, Expr, Statement, UtilityOption};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainMode {
    Plan,
    Analyze,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainStage {
    Parse,
    Bind,
    SemanticAnalysis,
    LogicalPlanning,
    Optimizer,
    PhysicalPlanning,
    ExecutorPreparation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainStageTiming {
    pub(crate) stage: ExplainStage,
    pub(crate) duration_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainAnalyzedRuntime {
    pub(crate) execution_duration_us: u64,
    pub(crate) output_row_count: usize,
    pub(crate) output_column_count: usize,
    #[serde(default)]
    pub(crate) output_columns: Vec<String>,
}

impl ExplainAnalyzedRuntime {
    fn from_query_result(result: &QueryResult, execution_duration: Duration) -> Self {
        Self {
            execution_duration_us: saturating_duration_us(execution_duration),
            output_row_count: result.rows.len(),
            output_column_count: result.columns.len(),
            output_columns: result.columns.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ExplainTimingCollector {
    stage_timings: Vec<ExplainStageTiming>,
}

impl ExplainTimingCollector {
    pub(crate) fn new(parse_duration: Option<Duration>) -> Self {
        let mut collector = Self::default();
        if let Some(parse_duration) = parse_duration {
            collector.record(ExplainStage::Parse, parse_duration);
        }
        collector
    }

    pub(crate) fn record(&mut self, stage: ExplainStage, duration: Duration) {
        self.stage_timings.push(stage_timing(stage, duration));
    }

    pub(crate) fn finish(self) -> Vec<ExplainStageTiming> {
        self.stage_timings
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainPredicateSupport {
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainRejectedPredicate {
    pub(crate) predicate: String,
    pub(crate) reason: String,
    pub(crate) support: ExplainPredicateSupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainSurfaceFamily {
    State,
    Entity,
    Filesystem,
    Admin,
    Change,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainSurfaceVariant {
    Default,
    ByVersion,
    History,
    WorkingChanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainSurfaceCapability {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BoundPublicLeafSnapshot {
    pub(crate) public_name: String,
    pub(crate) surface_family: ExplainSurfaceFamily,
    pub(crate) surface_variant: ExplainSurfaceVariant,
    pub(crate) capability: ExplainSurfaceCapability,
    pub(crate) requires_effective_state: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SurfaceBindingSnapshot {
    pub(crate) public_name: String,
    pub(crate) surface_family: ExplainSurfaceFamily,
    pub(crate) surface_variant: ExplainSurfaceVariant,
    pub(crate) capability: ExplainSurfaceCapability,
    pub(crate) read_freshness: String,
    pub(crate) read_semantics: String,
    pub(crate) default_scope: String,
    pub(crate) exposed_columns: Vec<String>,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) hidden_columns: Vec<String>,
    pub(crate) fixed_schema_key: Option<String>,
    pub(crate) expose_version_id: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DependencyWriterFilterSnapshot {
    pub(crate) include: Vec<String>,
    pub(crate) exclude: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DependencySpecSnapshot {
    pub(crate) relations: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) version_ids: Vec<String>,
    pub(crate) session_dependencies: Vec<SessionDependency>,
    pub(crate) writer_filter: DependencyWriterFilterSnapshot,
    pub(crate) include_untracked: bool,
    pub(crate) depends_on_active_version: bool,
    pub(crate) precision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EffectiveStateRequestSnapshot {
    pub(crate) schema_set: Vec<String>,
    pub(crate) version_scope: String,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) predicate_classes: Vec<String>,
    pub(crate) required_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EffectiveStatePlanSnapshot {
    pub(crate) state_source: String,
    pub(crate) overlay_lanes: Vec<String>,
    pub(crate) pushdown_safe_predicates: Vec<String>,
    pub(crate) residual_predicates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PushdownExplainArtifacts {
    #[serde(default)]
    pub(crate) accepted_predicates: Vec<String>,
    #[serde(default)]
    pub(crate) rejected_predicates: Vec<ExplainRejectedPredicate>,
    #[serde(default)]
    pub(crate) residual_predicates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PreparedStatementSnapshot {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaLiveTableRequirementSnapshot {
    pub(crate) schema_key: String,
    pub(crate) schema_definition: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MutationRowSnapshot {
    pub(crate) operation: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<JsonValue>,
    pub(crate) untracked: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct UpdateValidationPlanSnapshot {
    pub(crate) delete: bool,
    pub(crate) table: String,
    pub(crate) where_clause: Option<String>,
    pub(crate) snapshot_content: Option<JsonValue>,
    pub(crate) snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct InternalStatementsSnapshot {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatementSnapshot>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirementSnapshot>,
    pub(crate) mutations: Vec<MutationRowSnapshot>,
    pub(crate) update_validations: Vec<UpdateValidationPlanSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExecutionContextSnapshot {
    pub(crate) dialect: Option<String>,
    pub(crate) writer_key: Option<String>,
    pub(crate) requested_version_id: Option<String>,
    pub(crate) active_account_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PredicateSpecSnapshot {
    pub(crate) sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ProjectionExprSnapshot {
    pub(crate) output_name: String,
    pub(crate) source_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SortKeySnapshot {
    pub(crate) column_name: String,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CanonicalStateScanSnapshot {
    pub(crate) binding: SurfaceBindingSnapshot,
    pub(crate) version_scope: String,
    pub(crate) expose_version_id: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) entity_projection: Option<EntityProjectionSpecSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EntityProjectionSpecSnapshot {
    pub(crate) schema_key: String,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) hide_version_columns_by_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CanonicalFilesystemScanSnapshot {
    pub(crate) binding: SurfaceBindingSnapshot,
    pub(crate) kind: String,
    pub(crate) version_scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CanonicalAdminScanSnapshot {
    pub(crate) binding: SurfaceBindingSnapshot,
    pub(crate) kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CanonicalChangeScanSnapshot {
    pub(crate) binding: SurfaceBindingSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CanonicalWorkingChangesScanSnapshot {
    pub(crate) binding: SurfaceBindingSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainReadPlan {
    Scan(CanonicalStateScanSnapshot),
    FilesystemScan(CanonicalFilesystemScanSnapshot),
    AdminScan(CanonicalAdminScanSnapshot),
    ChangeScan(CanonicalChangeScanSnapshot),
    WorkingChangesScan(CanonicalWorkingChangesScanSnapshot),
    Filter {
        input: Box<ExplainReadPlan>,
        predicate: PredicateSpecSnapshot,
    },
    Project {
        input: Box<ExplainReadPlan>,
        expressions: Vec<ProjectionExprSnapshot>,
    },
    Sort {
        input: Box<ExplainReadPlan>,
        ordering: Vec<SortKeySnapshot>,
    },
    Limit {
        input: Box<ExplainReadPlan>,
        limit: Option<u64>,
        offset: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReadCommandSnapshot {
    pub(crate) root: Box<ExplainReadPlan>,
    pub(crate) contract: String,
    pub(crate) requested_commit_mapping: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct NormalizedPublicReadQuerySnapshot {
    pub(crate) source_alias: Option<String>,
    pub(crate) projection: Vec<String>,
    pub(crate) selection: Option<String>,
    pub(crate) selection_predicates: Vec<String>,
    pub(crate) group_by: String,
    pub(crate) having: Option<String>,
    pub(crate) order_by: Option<String>,
    pub(crate) limit_clause: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StructuredPublicReadSnapshot {
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) requested_version_id: Option<String>,
    pub(crate) surface_binding: SurfaceBindingSnapshot,
    pub(crate) read_command: Box<ReadCommandSnapshot>,
    pub(crate) query: NormalizedPublicReadQuerySnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct WriteSelectorSnapshot {
    pub(crate) residual_predicates: Vec<String>,
    pub(crate) exact_filters: BTreeMap<String, Value>,
    pub(crate) exact_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub(crate) enum MutationPayloadSnapshot {
    InsertRows(Vec<BTreeMap<String, Value>>),
    UpdatePatch(BTreeMap<String, Value>),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct InsertOnConflictSnapshot {
    pub(crate) conflict_columns: Vec<String>,
    pub(crate) action: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct WriteCommandSnapshot {
    pub(crate) operation_kind: String,
    pub(crate) target: SurfaceBindingSnapshot,
    pub(crate) selector: WriteSelectorSnapshot,
    pub(crate) payload: MutationPayloadSnapshot,
    pub(crate) on_conflict: Option<InsertOnConflictSnapshot>,
    pub(crate) requested_mode: String,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) execution_context: ExecutionContextSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ScopeProofSnapshot {
    pub(crate) kind: String,
    pub(crate) versions: Vec<String>,
    pub(crate) version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaProofSnapshot {
    pub(crate) kind: String,
    pub(crate) schema_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TargetSetProofSnapshot {
    pub(crate) kind: String,
    pub(crate) entity_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ResolvedRowRefSnapshot {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) source_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PlannedStateRowSnapshot {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RowLineageSnapshot {
    pub(crate) entity_id: String,
    pub(crate) source_change_id: Option<String>,
    pub(crate) source_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FilesystemDescriptorStateSnapshot {
    pub(crate) directory_id: String,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FilesystemTransactionFileStateSnapshot {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) descriptor: Option<FilesystemDescriptorStateSnapshot>,
    pub(crate) metadata_patch: String,
    pub(crate) has_data: bool,
    pub(crate) data_len: Option<usize>,
    pub(crate) deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FilesystemTransactionStateSnapshot {
    pub(crate) files: Vec<FilesystemTransactionFileStateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ResolvedWritePartitionSnapshot {
    pub(crate) execution_mode: String,
    pub(crate) authoritative_pre_state: Vec<ResolvedRowRefSnapshot>,
    pub(crate) authoritative_pre_state_rows: Vec<PlannedStateRowSnapshot>,
    pub(crate) intended_post_state: Vec<PlannedStateRowSnapshot>,
    pub(crate) tombstones: Vec<ResolvedRowRefSnapshot>,
    pub(crate) lineage: Vec<RowLineageSnapshot>,
    pub(crate) target_write_lane: Option<String>,
    pub(crate) filesystem_state: FilesystemTransactionStateSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ResolvedWritePlanSnapshot {
    pub(crate) partitions: Vec<ResolvedWritePartitionSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CommitPreconditionsSnapshot {
    pub(crate) write_lane: String,
    pub(crate) expected_head: String,
    pub(crate) idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PublicDomainChangeSnapshot {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: Option<String>,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) version_id: String,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SemanticEffectSnapshot {
    pub(crate) effect_key: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DomainChangeBatchSnapshot {
    pub(crate) changes: Vec<PublicDomainChangeSnapshot>,
    pub(crate) write_lane: String,
    pub(crate) writer_key: Option<String>,
    pub(crate) semantic_effects: Vec<SemanticEffectSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PlanEffectsSnapshot {
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) session_delta: SessionStateDelta,
    pub(crate) file_cache_refresh_targets: Vec<FileCacheRefreshTargetSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FileCacheRefreshTargetSnapshot {
    pub(crate) file_id: String,
    pub(crate) schema_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PublicWriteMaterializationSnapshot {
    pub(crate) partitions: Vec<PublicWriteExecutionPartitionSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum PublicWriteExecutionPartitionSnapshot {
    Tracked(TrackedWriteExecutionSnapshot),
    Untracked(UntrackedWriteExecutionSnapshot),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct TrackedWriteExecutionSnapshot {
    pub(crate) schema_live_table_requirements: Vec<SchemaLiveTableRequirementSnapshot>,
    pub(crate) domain_change_batch: Option<DomainChangeBatchSnapshot>,
    pub(crate) create_preconditions: CommitPreconditionsSnapshot,
    pub(crate) semantic_effects: PlanEffectsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct UntrackedWriteExecutionSnapshot {
    pub(crate) intended_post_state: Vec<PlannedStateRowSnapshot>,
    pub(crate) semantic_effects: PlanEffectsSnapshot,
    pub(crate) persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LoweredStatementBindingsSnapshot {
    pub(crate) used_bindings: Vec<ExplainStatementBindingSnapshot>,
    pub(crate) minimum_param_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainRuntimeBindingKind {
    ActiveVersionId,
    ActiveAccountIdsJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainStatementBindingSnapshot {
    UserParam(usize),
    Runtime(ExplainRuntimeBindingKind),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TerminalRelationRenderNodeSnapshot {
    pub(crate) placeholder_relation_name: String,
    pub(crate) alias: String,
    pub(crate) rendered_factor_sql: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LoweredReadStatementSnapshot {
    pub(crate) shell_statement_sql: String,
    pub(crate) bindings: LoweredStatementBindingsSnapshot,
    pub(crate) relation_render_nodes: Vec<TerminalRelationRenderNodeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LoweredResultColumnsSnapshot {
    pub(crate) kind: String,
    pub(crate) static_columns: Vec<String>,
    pub(crate) by_column_name: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LoweredReadProgramSnapshot {
    pub(crate) statements: Vec<LoweredReadStatementSnapshot>,
    pub(crate) pushdown_decision: PushdownExplainArtifacts,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StateHistoryRequestSnapshot {
    pub(crate) root_scope: String,
    pub(crate) requested_roots: Vec<String>,
    pub(crate) lineage_scope: String,
    pub(crate) active_version_id: Option<String>,
    pub(crate) version_scope: String,
    pub(crate) requested_versions: Vec<String>,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) plugin_keys: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) content_mode: String,
    pub(crate) order: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FileHistoryRequestSnapshot {
    pub(crate) lineage_scope: String,
    pub(crate) active_version_id: Option<String>,
    pub(crate) root_scope: String,
    pub(crate) requested_roots: Vec<String>,
    pub(crate) version_scope: String,
    pub(crate) requested_versions: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) content_mode: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectoryHistoryRequestSnapshot {
    pub(crate) lineage_scope: String,
    pub(crate) active_version_id: Option<String>,
    pub(crate) root_scope: String,
    pub(crate) requested_roots: Vec<String>,
    pub(crate) version_scope: String,
    pub(crate) requested_versions: Vec<String>,
    pub(crate) directory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum DirectPlanRequestSnapshot {
    StateLikeHistory(StateHistoryRequestSnapshot),
    FileHistory(FileHistoryRequestSnapshot),
    DirectoryHistory(DirectoryHistoryRequestSnapshot),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectPlanSummarySnapshot {
    pub(crate) request: DirectPlanRequestSnapshot,
    pub(crate) predicates: Vec<String>,
    pub(crate) projections: Vec<String>,
    pub(crate) sort_keys: Vec<String>,
    pub(crate) group_by: Vec<String>,
    pub(crate) having: Option<String>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) aggregate: Option<String>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
    pub(crate) surface_binding: Option<SurfaceBindingSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainDirectPublicReadPlan {
    StateHistory(Box<DirectPlanSummarySnapshot>),
    EntityHistory(Box<DirectPlanSummarySnapshot>),
    FileHistory(Box<DirectPlanSummarySnapshot>),
    DirectoryHistory(Box<DirectPlanSummarySnapshot>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainPublicReadExecution {
    LoweredSql(Box<LoweredReadProgramSnapshot>),
    Direct(Box<ExplainDirectPublicReadPlan>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainPublicWriteExecution {
    Noop,
    Materialize(Box<PublicWriteMaterializationSnapshot>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainPublicReadLogicalPlan {
    pub(crate) strategy: String,
    pub(crate) surface_bindings: Vec<SurfaceBindingSnapshot>,
    pub(crate) read: Option<Box<StructuredPublicReadSnapshot>>,
    pub(crate) dependency_spec: Option<Box<DependencySpecSnapshot>>,
    pub(crate) effective_state_request: Option<Box<EffectiveStateRequestSnapshot>>,
    pub(crate) effective_state_plan: Option<Box<EffectiveStatePlanSnapshot>>,
    pub(crate) direct_plan: Option<Box<ExplainDirectPublicReadPlan>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainPublicWriteLogicalPlan {
    pub(crate) planned_write: Box<PlannedWriteSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainInternalLogicalPlan {
    pub(crate) statements: Box<InternalStatementsSnapshot>,
    pub(crate) result_contract: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainSemanticStatement {
    PublicRead(Box<ExplainPublicReadSemantics>),
    PublicWrite(Box<ExplainPublicWriteSemantics>),
    Internal(Box<InternalStatementsSnapshot>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainPublicReadSemantics {
    pub(crate) surface_bindings: Vec<SurfaceBindingSnapshot>,
    pub(crate) structured_read: Option<Box<StructuredPublicReadSnapshot>>,
    pub(crate) effective_state_request: Option<Box<EffectiveStateRequestSnapshot>>,
    pub(crate) effective_state_plan: Option<Box<EffectiveStatePlanSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainPublicWriteSemantics {
    pub(crate) surface_binding: SurfaceBindingSnapshot,
    pub(crate) write_command: Box<WriteCommandSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainLogicalPlanSnapshot {
    PublicRead(Box<ExplainPublicReadLogicalPlan>),
    PublicWrite(Box<ExplainPublicWriteLogicalPlan>),
    Internal(Box<ExplainInternalLogicalPlan>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainPhysicalPlanSnapshot {
    PublicRead(Box<ExplainPublicReadExecution>),
    PublicWrite(Box<ExplainPublicWriteExecution>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PlannedWriteSnapshot {
    pub(crate) command: WriteCommandSnapshot,
    pub(crate) scope_proof: ScopeProofSnapshot,
    pub(crate) schema_proof: SchemaProofSnapshot,
    pub(crate) target_set_proof: Option<TargetSetProofSnapshot>,
    pub(crate) state_source: String,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlanSnapshot>,
    pub(crate) commit_preconditions: Vec<CommitPreconditionsSnapshot>,
    pub(crate) residual_execution_predicates: Vec<String>,
    pub(crate) backend_rejections: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub(crate) struct ExecutorExplainArtifacts {
    #[serde(default)]
    pub(crate) surface_bindings: Vec<String>,
    #[serde(default)]
    pub(crate) bound_public_leaves: Vec<BoundPublicLeafSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) dependency_spec: Option<Box<DependencySpecSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) effective_state_request: Option<Box<EffectiveStateRequestSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) effective_state_plan: Option<Box<EffectiveStatePlanSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pushdown: Option<Box<PushdownExplainArtifacts>>,
    #[serde(default)]
    pub(crate) lowered_sql: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) write_command: Option<Box<WriteCommandSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope_proof: Option<Box<ScopeProofSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) schema_proof: Option<Box<SchemaProofSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target_set_proof: Option<Box<TargetSetProofSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) resolved_write_plan: Option<Box<ResolvedWritePlanSnapshot>>,
    #[serde(default)]
    pub(crate) domain_change_batches: Vec<DomainChangeBatchSnapshot>,
    #[serde(default)]
    pub(crate) commit_preconditions: Vec<CommitPreconditionsSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) invariant_trace: Option<Box<PublicWriteInvariantTrace>>,
    #[serde(default)]
    pub(crate) write_phase_trace: Vec<String>,
    #[serde(default)]
    pub(crate) internal_live_table_requirements: Vec<SchemaLiveTableRequirementSnapshot>,
    #[serde(default)]
    pub(crate) internal_mutations: Vec<MutationRowSnapshot>,
    #[serde(default)]
    pub(crate) internal_update_validations: Vec<UpdateValidationPlanSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainArtifacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request: Option<ExplainRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) semantic_statement: Option<Box<ExplainSemanticStatement>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) logical_plan: Option<Box<ExplainLogicalPlanSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) optimized_logical_plan: Option<Box<ExplainLogicalPlanSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) physical_plan: Option<Box<ExplainPhysicalPlanSnapshot>>,
    pub(crate) executor_artifacts: ExecutorExplainArtifacts,
    #[serde(default)]
    pub(crate) optimizer_passes: Vec<OptimizerPassTrace>,
    #[serde(default)]
    pub(crate) stage_timings: Vec<ExplainStageTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) analyzed_runtime: Option<ExplainAnalyzedRuntime>,
}

impl ExplainArtifacts {
    pub(crate) fn request(&self) -> Option<&ExplainRequest> {
        self.request.as_ref()
    }

    pub(crate) fn requires_execution(&self) -> bool {
        self.request()
            .is_some_and(ExplainRequest::requires_execution)
    }

    pub(crate) fn render_query_result(&self) -> Result<QueryResult, LixError> {
        let Some(request) = self.request.as_ref() else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "explain rendering requires an explain request",
            ));
        };

        match request.output_format() {
            ExplainOutputFormat::Text => self.render_text_result(),
            ExplainOutputFormat::Json => self.render_json_result(),
        }
    }

    pub(crate) fn render_analyzed_query_result(
        &self,
        result: &QueryResult,
        execution_duration: Duration,
    ) -> Result<QueryResult, LixError> {
        if !self.requires_execution() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "analyzed explain rendering requires EXPLAIN ANALYZE",
            ));
        }

        let mut analyzed = self.clone();
        analyzed.analyzed_runtime = Some(ExplainAnalyzedRuntime::from_query_result(
            result,
            execution_duration,
        ));
        analyzed.render_query_result()
    }

    fn render_text_result(&self) -> Result<QueryResult, LixError> {
        let mut rows = Vec::new();
        for (key, value) in self.text_sections()? {
            rows.push(vec![Value::Text(key), Value::Text(value)]);
        }
        Ok(QueryResult {
            columns: vec!["explain_key".to_string(), "explain_value".to_string()],
            rows,
        })
    }

    fn render_json_result(&self) -> Result<QueryResult, LixError> {
        let explain_json = serde_json::to_value(self).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to serialize explain output: {error}"),
            )
        })?;
        Ok(QueryResult {
            columns: vec!["explain_json".to_string()],
            rows: vec![vec![Value::Json(explain_json)]],
        })
    }

    fn text_sections(&self) -> Result<Vec<(String, String)>, LixError> {
        let mut sections = Vec::new();

        if let Some(request) = self.request.as_ref() {
            sections.push(("request".to_string(), pretty_json(request)?));
        }
        if let Some(semantic_statement) = self.semantic_statement.as_ref() {
            sections.push((
                "semantic_statement".to_string(),
                pretty_json(semantic_statement)?,
            ));
        }
        if let Some(logical_plan) = self.logical_plan.as_ref() {
            sections.push(("logical_plan".to_string(), pretty_json(logical_plan)?));
        }
        if let Some(optimized_logical_plan) = self.optimized_logical_plan.as_ref() {
            sections.push((
                "optimized_logical_plan".to_string(),
                pretty_json(optimized_logical_plan)?,
            ));
        }
        if let Some(physical_plan) = self.physical_plan.as_ref() {
            sections.push(("physical_plan".to_string(), pretty_json(physical_plan)?));
        }
        sections.push((
            "executor_artifacts".to_string(),
            pretty_json(&self.executor_artifacts)?,
        ));
        if !self.optimizer_passes.is_empty() {
            sections.push((
                "optimizer_passes".to_string(),
                pretty_json(&self.optimizer_passes)?,
            ));
        }
        if !self.stage_timings.is_empty() {
            sections.push((
                "stage_timings".to_string(),
                pretty_json(&self.stage_timings)?,
            ));
        }
        if let Some(analyzed_runtime) = self.analyzed_runtime.as_ref() {
            sections.push((
                "analyzed_runtime".to_string(),
                pretty_json(analyzed_runtime)?,
            ));
        }

        Ok(sections)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainRequest {
    pub(crate) mode: ExplainMode,
    #[serde(rename = "format")]
    pub(crate) output_format: ExplainOutputFormat,
}

impl ExplainRequest {
    pub(crate) fn requires_execution(&self) -> bool {
        self.mode == ExplainMode::Analyze
    }

    pub(crate) fn output_format(&self) -> ExplainOutputFormat {
        self.output_format
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExplainStatement {
    pub(crate) statement: Statement,
    pub(crate) request: Option<ExplainRequest>,
}

fn utility_option_name_is(option: &UtilityOption, expected: &str) -> bool {
    option.name.value.eq_ignore_ascii_case(expected)
}

fn utility_option_word(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::SingleQuotedString(inner) => Some(inner.clone()),
            sqlparser::ast::Value::Boolean(value) => Some(value.to_string()),
            _ => None,
        },
        _ => None,
    }
}

fn utility_option_bool(option: &UtilityOption) -> Option<bool> {
    match option.arg.as_ref() {
        None => Some(true),
        Some(expr) => utility_option_word(expr).and_then(|word| {
            if word.eq_ignore_ascii_case("true") {
                Some(true)
            } else if word.eq_ignore_ascii_case("false") {
                Some(false)
            } else {
                None
            }
        }),
    }
}

fn explain_format_from_utility_options(options: &[UtilityOption]) -> Option<String> {
    options.iter().find_map(|option| {
        utility_option_name_is(option, "format")
            .then(|| option.arg.as_ref())
            .flatten()
            .and_then(utility_option_word)
            .map(|word| word.to_ascii_uppercase())
    })
}

fn explain_flag_from_utility_options(options: &[UtilityOption], flag: &str) -> Option<bool> {
    options
        .iter()
        .find(|option| utility_option_name_is(option, flag))
        .and_then(utility_option_bool)
}

// Launch contract:
// - supported statements: EXPLAIN and EXPLAIN ANALYZE
// - supported formats: TEXT (default) and JSON
// - supported utility options: ANALYZE and FORMAT
// Everything else is rejected here so later stages never see inert metadata.
fn normalize_explain_request(
    describe_alias: &sqlparser::ast::DescribeAlias,
    analyze: bool,
    verbose: bool,
    query_plan: bool,
    estimate: bool,
    format: Option<&AnalyzeFormatKind>,
    options: Option<&Vec<UtilityOption>>,
) -> Result<ExplainRequest, LixError> {
    if !matches!(describe_alias, DescribeAlias::Explain) {
        return Err(unsupported_explain_alias_error(describe_alias));
    }
    if verbose {
        return Err(unsupported_explain_modifier_error("VERBOSE"));
    }
    if query_plan {
        return Err(unsupported_explain_modifier_error("QUERY PLAN"));
    }
    if estimate {
        return Err(unsupported_explain_modifier_error("ESTIMATE"));
    }
    if format.is_some() {
        return Err(legacy_explain_format_syntax_error());
    }

    let options = options.cloned().unwrap_or_default();
    let analyze = explain_flag_from_utility_options(&options, "analyze").unwrap_or(analyze);
    let mut saw_analyze_option = false;
    let mut saw_format_option = false;
    for option in &options {
        let option_name = option.name.value.to_ascii_uppercase();
        match option_name.as_str() {
            "ANALYZE" => {
                if saw_analyze_option {
                    return Err(duplicate_explain_option_error("ANALYZE"));
                }
                saw_analyze_option = true;
                if utility_option_bool(option).is_none() {
                    return Err(invalid_explain_option_value_error(
                        "ANALYZE",
                        "expected TRUE or FALSE",
                    ));
                }
            }
            "FORMAT" => {
                if saw_format_option {
                    return Err(duplicate_explain_option_error("FORMAT"));
                }
                saw_format_option = true;
                let Some(format_expr) = option.arg.as_ref() else {
                    return Err(invalid_explain_option_value_error(
                        "FORMAT",
                        "expected TEXT or JSON",
                    ));
                };
                let Some(format_word) = utility_option_word(format_expr) else {
                    return Err(invalid_explain_option_value_error(
                        "FORMAT",
                        "expected TEXT or JSON",
                    ));
                };
                explain_output_format_from_word(&format_word)?;
            }
            "VERBOSE" => return Err(unsupported_explain_modifier_error("VERBOSE")),
            other => return Err(unsupported_explain_option_error(other)),
        }
    }
    let format = explain_format_from_utility_options(&options)
        .map(|value| explain_output_format_from_word(&value))
        .transpose()?
        .unwrap_or(ExplainOutputFormat::Text);

    Ok(ExplainRequest {
        mode: if analyze {
            ExplainMode::Analyze
        } else {
            ExplainMode::Plan
        },
        output_format: format,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadExplainRuntimeArtifacts {
    pub(crate) pushdown_decision: Option<PushdownDecision>,
    pub(crate) lowered_sql: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadExplainBuildInput {
    pub(crate) request: Option<ExplainRequest>,
    pub(crate) semantics: PublicReadSemantics,
    pub(crate) logical_plan: PublicReadLogicalPlan,
    pub(crate) optimized_logical_plan: PublicReadLogicalPlan,
    pub(crate) execution: PreparedPublicReadExecution,
    pub(crate) runtime_artifacts: PublicReadExplainRuntimeArtifacts,
    pub(crate) optimizer_passes: Vec<OptimizerPassTrace>,
    pub(crate) stage_timings: Vec<ExplainStageTiming>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicWriteExplainBuildInput {
    pub(crate) request: Option<ExplainRequest>,
    pub(crate) semantics: PublicWriteSemantics,
    pub(crate) planned_write: PlannedWrite,
    pub(crate) execution: PreparedPublicWriteExecution,
    pub(crate) domain_change_batches: Vec<DomainChangeBatch>,
    pub(crate) invariant_trace: Option<PublicWriteInvariantTrace>,
    pub(crate) write_phase_trace: Vec<String>,
    pub(crate) stage_timings: Vec<ExplainStageTiming>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InternalExplainBuildInput {
    pub(crate) request: ExplainRequest,
    pub(crate) logical_plan: InternalLogicalPlan,
    pub(crate) stage_timings: Vec<ExplainStageTiming>,
}

pub(crate) fn unwrap_explain_statement(
    statement: &Statement,
) -> Result<ExplainStatement, LixError> {
    match statement {
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => Ok(ExplainStatement {
            statement: statement.as_ref().clone(),
            request: Some(normalize_explain_request(
                describe_alias,
                *analyze,
                *verbose,
                *query_plan,
                *estimate,
                format.as_ref(),
                options.as_ref(),
            )?),
        }),
        _ => Ok(ExplainStatement {
            statement: statement.clone(),
            request: None,
        }),
    }
}

pub(crate) fn stage_timing(stage: ExplainStage, duration: Duration) -> ExplainStageTiming {
    ExplainStageTiming {
        stage,
        duration_us: saturating_duration_us(duration),
    }
}

pub(crate) fn unsupported_explain_analyze_error(statement_class: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "EXPLAIN ANALYZE is not supported for {statement_class} yet; only read-only statements execute through analyzed explain"
        ),
    )
}

fn unsupported_explain_alias_error(alias: &DescribeAlias) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("unsupported EXPLAIN alias {alias}; use EXPLAIN or EXPLAIN ANALYZE"),
    )
}

fn unsupported_explain_modifier_error(modifier: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "unsupported EXPLAIN modifier {modifier}; supported launch modifiers are ANALYZE and FORMAT"
        ),
    )
}

fn unsupported_explain_option_error(option: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "unsupported EXPLAIN option {option}; supported launch options are ANALYZE and FORMAT"
        ),
    )
}

fn invalid_explain_option_value_error(option: &str, details: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("invalid EXPLAIN option {option}: {details}"),
    )
}

fn duplicate_explain_option_error(option: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("duplicate EXPLAIN option {option} is not supported"),
    )
}

fn legacy_explain_format_syntax_error() -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        "legacy EXPLAIN FORMAT syntax is not supported; use EXPLAIN (FORMAT JSON) or EXPLAIN (FORMAT TEXT)",
    )
}

fn explain_output_format_from_word(word: &str) -> Result<ExplainOutputFormat, LixError> {
    if word.eq_ignore_ascii_case("TEXT") {
        return Ok(ExplainOutputFormat::Text);
    }
    if word.eq_ignore_ascii_case("JSON") {
        return Ok(ExplainOutputFormat::Json);
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("unsupported EXPLAIN FORMAT {word}; supported formats are TEXT and JSON"),
    ))
}

pub(crate) fn build_public_read_explain_artifacts(
    input: PublicReadExplainBuildInput,
) -> ExplainArtifacts {
    let executor_artifacts = executor_artifacts_for_public_read(
        &input.semantics,
        &input.optimized_logical_plan,
        &input.runtime_artifacts,
    );

    build_explain_artifacts(
        input.request,
        Some(SemanticStatement::PublicRead(input.semantics)),
        Some(LogicalPlan::PublicRead(input.logical_plan)),
        Some(LogicalPlan::PublicRead(input.optimized_logical_plan)),
        Some(PhysicalPlan::PublicRead(input.execution)),
        executor_artifacts,
        input.optimizer_passes,
        input.stage_timings,
    )
}

pub(crate) fn build_public_write_explain_artifacts(
    input: PublicWriteExplainBuildInput,
) -> ExplainArtifacts {
    let executor_artifacts = executor_artifacts_for_public_write(
        &input.planned_write,
        &input.domain_change_batches,
        input.invariant_trace.as_ref(),
        &input.write_phase_trace,
    );

    build_explain_artifacts(
        input.request,
        Some(SemanticStatement::PublicWrite(input.semantics)),
        Some(LogicalPlan::PublicWrite(PublicWriteLogicalPlan {
            planned_write: input.planned_write.clone(),
        })),
        Some(LogicalPlan::PublicWrite(PublicWriteLogicalPlan {
            planned_write: input.planned_write,
        })),
        Some(PhysicalPlan::PublicWrite(input.execution)),
        executor_artifacts,
        Vec::new(),
        input.stage_timings,
    )
}

pub(crate) fn build_internal_explain_artifacts(
    input: InternalExplainBuildInput,
) -> ExplainArtifacts {
    let executor_artifacts = executor_artifacts_for_internal(&input.logical_plan);

    build_explain_artifacts(
        Some(input.request),
        Some(SemanticStatement::Internal(
            input.logical_plan.normalized_statements.clone(),
        )),
        Some(LogicalPlan::Internal(input.logical_plan.clone())),
        Some(LogicalPlan::Internal(input.logical_plan)),
        None,
        executor_artifacts,
        Vec::new(),
        input.stage_timings,
    )
}

fn build_explain_artifacts(
    request: Option<ExplainRequest>,
    semantic_statement: Option<SemanticStatement>,
    logical_plan: Option<LogicalPlan>,
    optimized_logical_plan: Option<LogicalPlan>,
    physical_plan: Option<PhysicalPlan>,
    executor_artifacts: ExecutorExplainArtifacts,
    optimizer_passes: Vec<OptimizerPassTrace>,
    stage_timings: Vec<ExplainStageTiming>,
) -> ExplainArtifacts {
    ExplainArtifacts {
        request,
        semantic_statement: semantic_statement
            .as_ref()
            .map(semantic_statement_snapshot)
            .map(Box::new),
        logical_plan: logical_plan
            .as_ref()
            .map(logical_plan_snapshot)
            .map(Box::new),
        optimized_logical_plan: optimized_logical_plan
            .as_ref()
            .map(logical_plan_snapshot)
            .map(Box::new),
        physical_plan: physical_plan
            .as_ref()
            .map(physical_plan_snapshot)
            .map(Box::new),
        executor_artifacts,
        optimizer_passes,
        stage_timings,
        analyzed_runtime: None,
    }
}

fn executor_artifacts_for_public_read(
    semantics: &PublicReadSemantics,
    optimized_logical_plan: &PublicReadLogicalPlan,
    runtime_artifacts: &PublicReadExplainRuntimeArtifacts,
) -> ExecutorExplainArtifacts {
    ExecutorExplainArtifacts {
        surface_bindings: semantics
            .surface_bindings
            .iter()
            .map(|binding| binding.descriptor.public_name.clone())
            .collect(),
        bound_public_leaves: semantics
            .surface_bindings
            .iter()
            .map(BoundPublicLeaf::from_surface_binding)
            .map(|leaf| bound_public_leaf_snapshot(&leaf))
            .collect(),
        dependency_spec: optimized_logical_plan
            .dependency_spec()
            .map(dependency_spec_snapshot)
            .map(Box::new),
        effective_state_request: optimized_logical_plan
            .effective_state_request()
            .map(effective_state_request_snapshot)
            .map(Box::new),
        effective_state_plan: optimized_logical_plan
            .effective_state_plan()
            .map(effective_state_plan_snapshot)
            .map(Box::new),
        pushdown: runtime_artifacts
            .pushdown_decision
            .as_ref()
            .map(pushdown_snapshot)
            .map(Box::new),
        lowered_sql: runtime_artifacts.lowered_sql.clone(),
        write_command: None,
        scope_proof: None,
        schema_proof: None,
        target_set_proof: None,
        resolved_write_plan: None,
        domain_change_batches: Vec::new(),
        commit_preconditions: Vec::new(),
        invariant_trace: None,
        write_phase_trace: Vec::new(),
        internal_live_table_requirements: Vec::new(),
        internal_mutations: Vec::new(),
        internal_update_validations: Vec::new(),
    }
}

fn executor_artifacts_for_public_write(
    planned_write: &PlannedWrite,
    domain_change_batches: &[DomainChangeBatch],
    invariant_trace: Option<&PublicWriteInvariantTrace>,
    write_phase_trace: &[String],
) -> ExecutorExplainArtifacts {
    let target = &planned_write.command.target;

    ExecutorExplainArtifacts {
        surface_bindings: vec![target.descriptor.public_name.clone()],
        bound_public_leaves: vec![BoundPublicLeaf::from_surface_binding(target)]
            .iter()
            .map(bound_public_leaf_snapshot)
            .collect(),
        dependency_spec: None,
        effective_state_request: None,
        effective_state_plan: None,
        pushdown: None,
        lowered_sql: Vec::new(),
        write_command: Some(&planned_write.command)
            .map(write_command_snapshot)
            .map(Box::new),
        scope_proof: Some(&planned_write.scope_proof)
            .map(scope_proof_snapshot)
            .map(Box::new),
        schema_proof: Some(&planned_write.schema_proof)
            .map(schema_proof_snapshot)
            .map(Box::new),
        target_set_proof: planned_write
            .target_set_proof
            .as_ref()
            .map(target_set_proof_snapshot)
            .map(Box::new),
        resolved_write_plan: planned_write
            .resolved_write_plan
            .as_ref()
            .map(resolved_write_plan_snapshot)
            .map(Box::new),
        domain_change_batches: domain_change_batches
            .iter()
            .map(domain_change_batch_snapshot)
            .collect(),
        commit_preconditions: planned_write
            .commit_preconditions
            .iter()
            .map(commit_preconditions_snapshot)
            .collect(),
        invariant_trace: invariant_trace.cloned().map(Box::new),
        write_phase_trace: write_phase_trace.to_vec(),
        internal_live_table_requirements: Vec::new(),
        internal_mutations: Vec::new(),
        internal_update_validations: Vec::new(),
    }
}

fn executor_artifacts_for_internal(logical_plan: &InternalLogicalPlan) -> ExecutorExplainArtifacts {
    let statements = &logical_plan.normalized_statements;

    ExecutorExplainArtifacts {
        surface_bindings: Vec::new(),
        bound_public_leaves: Vec::new(),
        dependency_spec: None,
        effective_state_request: None,
        effective_state_plan: None,
        pushdown: None,
        lowered_sql: statements
            .prepared_statements
            .iter()
            .map(|statement| statement.sql.clone())
            .collect(),
        write_command: None,
        scope_proof: None,
        schema_proof: None,
        target_set_proof: None,
        resolved_write_plan: None,
        domain_change_batches: Vec::new(),
        commit_preconditions: Vec::new(),
        invariant_trace: None,
        write_phase_trace: Vec::new(),
        internal_live_table_requirements: statements
            .live_table_requirements
            .iter()
            .map(schema_live_table_requirement_snapshot)
            .collect(),
        internal_mutations: statements
            .mutations
            .iter()
            .map(mutation_row_snapshot)
            .collect(),
        internal_update_validations: statements
            .update_validations
            .iter()
            .map(update_validation_plan_snapshot)
            .collect(),
    }
}

fn pushdown_snapshot(decision: &PushdownDecision) -> PushdownExplainArtifacts {
    PushdownExplainArtifacts {
        accepted_predicates: decision
            .accepted_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
        rejected_predicates: decision
            .rejected_predicates
            .iter()
            .map(|predicate| ExplainRejectedPredicate {
                predicate: predicate.predicate.to_string(),
                reason: predicate.reason.clone(),
                support: match predicate.support {
                    PushdownSupport::Unsupported => ExplainPredicateSupport::Unsupported,
                },
            })
            .collect(),
        residual_predicates: decision
            .residual_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

fn bound_public_leaf_snapshot(leaf: &BoundPublicLeaf) -> BoundPublicLeafSnapshot {
    BoundPublicLeafSnapshot {
        public_name: leaf.public_name.clone(),
        surface_family: surface_family_snapshot(leaf.surface_family),
        surface_variant: surface_variant_snapshot(leaf.surface_variant),
        capability: surface_capability_snapshot(leaf.capability),
        requires_effective_state: leaf.requires_effective_state,
    }
}

fn semantic_statement_snapshot(statement: &SemanticStatement) -> ExplainSemanticStatement {
    match statement {
        SemanticStatement::PublicRead(semantics) => ExplainSemanticStatement::PublicRead(Box::new(
            public_read_semantics_snapshot(semantics),
        )),
        SemanticStatement::PublicWrite(semantics) => ExplainSemanticStatement::PublicWrite(
            Box::new(public_write_semantics_snapshot(semantics)),
        ),
        SemanticStatement::Internal(statements) => {
            ExplainSemanticStatement::Internal(Box::new(internal_statements_snapshot(statements)))
        }
    }
}

fn public_read_semantics_snapshot(semantics: &PublicReadSemantics) -> ExplainPublicReadSemantics {
    ExplainPublicReadSemantics {
        surface_bindings: semantics
            .surface_bindings
            .iter()
            .map(surface_binding_snapshot)
            .collect(),
        structured_read: semantics
            .structured_read
            .as_ref()
            .map(structured_public_read_snapshot)
            .map(Box::new),
        effective_state_request: semantics
            .effective_state_request
            .as_ref()
            .map(effective_state_request_snapshot)
            .map(Box::new),
        effective_state_plan: semantics
            .effective_state_plan
            .as_ref()
            .map(effective_state_plan_snapshot)
            .map(Box::new),
    }
}

fn public_write_semantics_snapshot(
    semantics: &PublicWriteSemantics,
) -> ExplainPublicWriteSemantics {
    ExplainPublicWriteSemantics {
        surface_binding: surface_binding_snapshot(&semantics.canonicalized.surface_binding),
        write_command: Box::new(write_command_snapshot(
            &semantics.canonicalized.write_command,
        )),
    }
}

fn internal_statements_snapshot(
    statements: &NormalizedInternalStatements,
) -> InternalStatementsSnapshot {
    InternalStatementsSnapshot {
        sql: statements.sql.clone(),
        prepared_statements: statements
            .prepared_statements
            .iter()
            .map(prepared_statement_snapshot)
            .collect(),
        live_table_requirements: statements
            .live_table_requirements
            .iter()
            .map(schema_live_table_requirement_snapshot)
            .collect(),
        mutations: statements
            .mutations
            .iter()
            .map(mutation_row_snapshot)
            .collect(),
        update_validations: statements
            .update_validations
            .iter()
            .map(update_validation_plan_snapshot)
            .collect(),
    }
}

fn logical_plan_snapshot(plan: &LogicalPlan) -> ExplainLogicalPlanSnapshot {
    match plan {
        LogicalPlan::PublicRead(plan) => {
            ExplainLogicalPlanSnapshot::PublicRead(Box::new(match plan {
                PublicReadLogicalPlan::Structured {
                    read,
                    dependency_spec,
                    effective_state_request,
                    effective_state_plan,
                } => ExplainPublicReadLogicalPlan {
                    strategy: "structured".to_string(),
                    surface_bindings: vec![surface_binding_snapshot(&read.surface_binding)],
                    read: Some(Box::new(structured_public_read_snapshot(read))),
                    dependency_spec: dependency_spec
                        .as_ref()
                        .map(dependency_spec_snapshot)
                        .map(Box::new),
                    effective_state_request: effective_state_request
                        .as_ref()
                        .map(effective_state_request_snapshot)
                        .map(Box::new),
                    effective_state_plan: effective_state_plan
                        .as_ref()
                        .map(effective_state_plan_snapshot)
                        .map(Box::new),
                    direct_plan: None,
                },
                PublicReadLogicalPlan::DirectHistory {
                    read,
                    direct_plan,
                    dependency_spec,
                    effective_state_request,
                    effective_state_plan,
                } => ExplainPublicReadLogicalPlan {
                    strategy: "direct_history".to_string(),
                    surface_bindings: vec![surface_binding_snapshot(&read.surface_binding)],
                    read: Some(Box::new(structured_public_read_snapshot(read))),
                    dependency_spec: dependency_spec
                        .as_ref()
                        .map(dependency_spec_snapshot)
                        .map(Box::new),
                    effective_state_request: effective_state_request
                        .as_ref()
                        .map(effective_state_request_snapshot)
                        .map(Box::new),
                    effective_state_plan: effective_state_plan
                        .as_ref()
                        .map(effective_state_plan_snapshot)
                        .map(Box::new),
                    direct_plan: Some(Box::new(direct_public_read_plan_snapshot(direct_plan))),
                },
                PublicReadLogicalPlan::Broad {
                    surface_bindings,
                    dependency_spec,
                } => ExplainPublicReadLogicalPlan {
                    strategy: "broad".to_string(),
                    surface_bindings: surface_bindings
                        .iter()
                        .map(surface_binding_snapshot)
                        .collect(),
                    read: None,
                    dependency_spec: dependency_spec
                        .as_ref()
                        .map(dependency_spec_snapshot)
                        .map(Box::new),
                    effective_state_request: None,
                    effective_state_plan: None,
                    direct_plan: None,
                },
            }))
        }
        LogicalPlan::PublicWrite(plan) => {
            ExplainLogicalPlanSnapshot::PublicWrite(Box::new(ExplainPublicWriteLogicalPlan {
                planned_write: Box::new(planned_write_snapshot(&plan.planned_write)),
            }))
        }
        LogicalPlan::Internal(plan) => {
            ExplainLogicalPlanSnapshot::Internal(Box::new(internal_logical_plan_snapshot(plan)))
        }
    }
}

fn internal_logical_plan_snapshot(plan: &InternalLogicalPlan) -> ExplainInternalLogicalPlan {
    ExplainInternalLogicalPlan {
        statements: Box::new(internal_statements_snapshot(&plan.normalized_statements)),
        result_contract: result_contract_name(plan.result_contract).to_string(),
    }
}

fn physical_plan_snapshot(plan: &PhysicalPlan) -> ExplainPhysicalPlanSnapshot {
    match plan {
        PhysicalPlan::PublicRead(execution) => ExplainPhysicalPlanSnapshot::PublicRead(Box::new(
            public_read_execution_snapshot(execution),
        )),
        PhysicalPlan::PublicWrite(execution) => ExplainPhysicalPlanSnapshot::PublicWrite(Box::new(
            public_write_execution_snapshot(execution),
        )),
    }
}

fn public_read_execution_snapshot(
    execution: &PreparedPublicReadExecution,
) -> ExplainPublicReadExecution {
    match execution {
        PreparedPublicReadExecution::LoweredSql(program) => {
            ExplainPublicReadExecution::LoweredSql(Box::new(lowered_read_program_snapshot(program)))
        }
        PreparedPublicReadExecution::Direct(plan) => {
            ExplainPublicReadExecution::Direct(Box::new(direct_public_read_plan_snapshot(plan)))
        }
    }
}

fn public_write_execution_snapshot(
    execution: &PreparedPublicWriteExecution,
) -> ExplainPublicWriteExecution {
    match execution {
        PreparedPublicWriteExecution::Noop => ExplainPublicWriteExecution::Noop,
        PreparedPublicWriteExecution::Materialize(materialization) => {
            ExplainPublicWriteExecution::Materialize(Box::new(
                public_write_materialization_snapshot(materialization),
            ))
        }
    }
}

fn public_write_materialization_snapshot(
    materialization: &PublicWriteMaterialization,
) -> PublicWriteMaterializationSnapshot {
    PublicWriteMaterializationSnapshot {
        partitions: materialization
            .partitions
            .iter()
            .map(public_write_execution_partition_snapshot)
            .collect(),
    }
}

fn public_write_execution_partition_snapshot(
    partition: &PublicWriteExecutionPartition,
) -> PublicWriteExecutionPartitionSnapshot {
    match partition {
        PublicWriteExecutionPartition::Tracked(execution) => {
            PublicWriteExecutionPartitionSnapshot::Tracked(tracked_write_execution_snapshot(
                execution,
            ))
        }
        PublicWriteExecutionPartition::Untracked(execution) => {
            PublicWriteExecutionPartitionSnapshot::Untracked(untracked_write_execution_snapshot(
                execution,
            ))
        }
    }
}

fn tracked_write_execution_snapshot(
    execution: &TrackedWriteExecution,
) -> TrackedWriteExecutionSnapshot {
    TrackedWriteExecutionSnapshot {
        schema_live_table_requirements: execution
            .schema_live_table_requirements
            .iter()
            .map(schema_live_table_requirement_snapshot)
            .collect(),
        domain_change_batch: execution
            .domain_change_batch
            .as_ref()
            .map(domain_change_batch_snapshot),
        create_preconditions: commit_preconditions_snapshot(&execution.create_preconditions),
        semantic_effects: plan_effects_snapshot(&execution.semantic_effects),
    }
}

fn untracked_write_execution_snapshot(
    execution: &UntrackedWriteExecution,
) -> UntrackedWriteExecutionSnapshot {
    UntrackedWriteExecutionSnapshot {
        intended_post_state: execution
            .intended_post_state
            .iter()
            .map(planned_state_row_snapshot)
            .collect(),
        semantic_effects: plan_effects_snapshot(&execution.semantic_effects),
        persist_filesystem_payloads_before_write: execution
            .persist_filesystem_payloads_before_write,
    }
}

fn structured_public_read_snapshot(read: &StructuredPublicRead) -> StructuredPublicReadSnapshot {
    StructuredPublicReadSnapshot {
        bound_parameters: read.bound_parameters.clone(),
        requested_version_id: read.requested_version_id.clone(),
        surface_binding: surface_binding_snapshot(&read.surface_binding),
        read_command: Box::new(read_command_snapshot(&read.read_command)),
        query: normalized_public_read_query_snapshot(&read.query),
    }
}

fn read_command_snapshot(command: &ReadCommand) -> ReadCommandSnapshot {
    ReadCommandSnapshot {
        root: Box::new(read_plan_snapshot(&command.root)),
        contract: read_contract_name(command.contract).to_string(),
        requested_commit_mapping: command.requested_commit_mapping.clone(),
    }
}

fn read_plan_snapshot(plan: &ReadPlan) -> ExplainReadPlan {
    match plan {
        ReadPlan::Scan(scan) => ExplainReadPlan::Scan(canonical_state_scan_snapshot(scan)),
        ReadPlan::FilesystemScan(scan) => {
            ExplainReadPlan::FilesystemScan(canonical_filesystem_scan_snapshot(scan))
        }
        ReadPlan::AdminScan(scan) => {
            ExplainReadPlan::AdminScan(canonical_admin_scan_snapshot(scan))
        }
        ReadPlan::ChangeScan(scan) => {
            ExplainReadPlan::ChangeScan(canonical_change_scan_snapshot(scan))
        }
        ReadPlan::WorkingChangesScan(scan) => {
            ExplainReadPlan::WorkingChangesScan(canonical_working_changes_scan_snapshot(scan))
        }
        ReadPlan::Filter { input, predicate } => ExplainReadPlan::Filter {
            input: Box::new(read_plan_snapshot(input)),
            predicate: PredicateSpecSnapshot {
                sql: predicate.sql.clone(),
            },
        },
        ReadPlan::Project { input, expressions } => ExplainReadPlan::Project {
            input: Box::new(read_plan_snapshot(input)),
            expressions: expressions
                .iter()
                .map(|expr| ProjectionExprSnapshot {
                    output_name: expr.output_name.clone(),
                    source_name: expr.source_name.clone(),
                })
                .collect(),
        },
        ReadPlan::Sort { input, ordering } => ExplainReadPlan::Sort {
            input: Box::new(read_plan_snapshot(input)),
            ordering: ordering
                .iter()
                .map(|sort_key| SortKeySnapshot {
                    column_name: sort_key.column_name.clone(),
                    descending: sort_key.descending,
                })
                .collect(),
        },
        ReadPlan::Limit {
            input,
            limit,
            offset,
        } => ExplainReadPlan::Limit {
            input: Box::new(read_plan_snapshot(input)),
            limit: *limit,
            offset: *offset,
        },
    }
}

fn canonical_state_scan_snapshot(scan: &CanonicalStateScan) -> CanonicalStateScanSnapshot {
    CanonicalStateScanSnapshot {
        binding: surface_binding_snapshot(&scan.binding),
        version_scope: version_scope_name(scan.version_scope).to_string(),
        expose_version_id: scan.expose_version_id,
        include_tombstones: scan.include_tombstones,
        entity_projection: scan.entity_projection.as_ref().map(|projection| {
            EntityProjectionSpecSnapshot {
                schema_key: projection.schema_key.clone(),
                visible_columns: projection.visible_columns.clone(),
                hide_version_columns_by_default: projection.hide_version_columns_by_default,
            }
        }),
    }
}

fn canonical_filesystem_scan_snapshot(
    scan: &CanonicalFilesystemScan,
) -> CanonicalFilesystemScanSnapshot {
    CanonicalFilesystemScanSnapshot {
        binding: surface_binding_snapshot(&scan.binding),
        kind: filesystem_kind_name(scan.kind).to_string(),
        version_scope: version_scope_name(scan.version_scope).to_string(),
    }
}

fn canonical_admin_scan_snapshot(scan: &CanonicalAdminScan) -> CanonicalAdminScanSnapshot {
    CanonicalAdminScanSnapshot {
        binding: surface_binding_snapshot(&scan.binding),
        kind: canonical_admin_kind_name(scan.kind).to_string(),
    }
}

fn canonical_change_scan_snapshot(scan: &CanonicalChangeScan) -> CanonicalChangeScanSnapshot {
    CanonicalChangeScanSnapshot {
        binding: surface_binding_snapshot(&scan.binding),
    }
}

fn canonical_working_changes_scan_snapshot(
    scan: &CanonicalWorkingChangesScan,
) -> CanonicalWorkingChangesScanSnapshot {
    CanonicalWorkingChangesScanSnapshot {
        binding: surface_binding_snapshot(&scan.binding),
    }
}

fn normalized_public_read_query_snapshot(
    query: &NormalizedPublicReadQuery,
) -> NormalizedPublicReadQuerySnapshot {
    NormalizedPublicReadQuerySnapshot {
        source_alias: query.source_alias.as_ref().map(ToString::to_string),
        projection: query.projection.iter().map(ToString::to_string).collect(),
        selection: query.selection.as_ref().map(ToString::to_string),
        selection_predicates: query
            .selection_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
        group_by: query.group_by.to_string(),
        having: query.having.as_ref().map(ToString::to_string),
        order_by: query.order_by.as_ref().map(ToString::to_string),
        limit_clause: query.limit_clause.as_ref().map(ToString::to_string),
    }
}

fn planned_write_snapshot(plan: &PlannedWrite) -> PlannedWriteSnapshot {
    PlannedWriteSnapshot {
        command: write_command_snapshot(&plan.command),
        scope_proof: scope_proof_snapshot(&plan.scope_proof),
        schema_proof: schema_proof_snapshot(&plan.schema_proof),
        target_set_proof: plan
            .target_set_proof
            .as_ref()
            .map(target_set_proof_snapshot),
        state_source: state_source_kind_name(plan.state_source).to_string(),
        resolved_write_plan: plan
            .resolved_write_plan
            .as_ref()
            .map(resolved_write_plan_snapshot),
        commit_preconditions: plan
            .commit_preconditions
            .iter()
            .map(commit_preconditions_snapshot)
            .collect(),
        residual_execution_predicates: plan.residual_execution_predicates.clone(),
        backend_rejections: plan.backend_rejections.clone(),
    }
}

fn write_command_snapshot(command: &WriteCommand) -> WriteCommandSnapshot {
    WriteCommandSnapshot {
        operation_kind: write_operation_kind_name(command.operation_kind).to_string(),
        target: surface_binding_snapshot(&command.target),
        selector: write_selector_snapshot(&command.selector),
        payload: mutation_payload_snapshot(&command.payload),
        on_conflict: command
            .on_conflict
            .as_ref()
            .map(insert_on_conflict_snapshot),
        requested_mode: write_mode_request_name(command.requested_mode).to_string(),
        bound_parameters: command.bound_parameters.clone(),
        execution_context: execution_context_snapshot(&command.execution_context),
    }
}

fn write_selector_snapshot(selector: &WriteSelector) -> WriteSelectorSnapshot {
    WriteSelectorSnapshot {
        residual_predicates: selector
            .residual_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
        exact_filters: selector.exact_filters.clone(),
        exact_only: selector.exact_only,
    }
}

fn mutation_payload_snapshot(payload: &MutationPayload) -> MutationPayloadSnapshot {
    match payload {
        MutationPayload::InsertRows(rows) => MutationPayloadSnapshot::InsertRows(rows.clone()),
        MutationPayload::UpdatePatch(patch) => MutationPayloadSnapshot::UpdatePatch(patch.clone()),
        MutationPayload::Tombstone => MutationPayloadSnapshot::Tombstone,
    }
}

fn insert_on_conflict_snapshot(conflict: &InsertOnConflict) -> InsertOnConflictSnapshot {
    InsertOnConflictSnapshot {
        conflict_columns: conflict.conflict_columns.clone(),
        action: match conflict.action {
            InsertOnConflictAction::DoUpdate => "do_update",
            InsertOnConflictAction::DoNothing => "do_nothing",
        }
        .to_string(),
    }
}

fn execution_context_snapshot(
    context: &crate::sql::semantic_ir::ExecutionContext,
) -> ExecutionContextSnapshot {
    ExecutionContextSnapshot {
        dialect: context
            .dialect
            .map(sql_dialect_name)
            .map(ToString::to_string),
        writer_key: context.writer_key.clone(),
        requested_version_id: context.requested_version_id.clone(),
        active_account_ids: context.active_account_ids.clone(),
    }
}

fn scope_proof_snapshot(proof: &ScopeProof) -> ScopeProofSnapshot {
    match proof {
        ScopeProof::ActiveVersion => ScopeProofSnapshot {
            kind: "active_version".to_string(),
            versions: Vec::new(),
            version: None,
        },
        ScopeProof::SingleVersion(version) => ScopeProofSnapshot {
            kind: "single_version".to_string(),
            versions: Vec::new(),
            version: Some(version.clone()),
        },
        ScopeProof::GlobalAdmin => ScopeProofSnapshot {
            kind: "global_admin".to_string(),
            versions: Vec::new(),
            version: None,
        },
        ScopeProof::FiniteVersionSet(versions) => ScopeProofSnapshot {
            kind: "finite_version_set".to_string(),
            versions: versions.iter().cloned().collect(),
            version: None,
        },
        ScopeProof::Unbounded => ScopeProofSnapshot {
            kind: "unbounded".to_string(),
            versions: Vec::new(),
            version: None,
        },
        ScopeProof::Unknown => ScopeProofSnapshot {
            kind: "unknown".to_string(),
            versions: Vec::new(),
            version: None,
        },
    }
}

fn schema_proof_snapshot(proof: &SchemaProof) -> SchemaProofSnapshot {
    match proof {
        SchemaProof::Exact(schema_keys) => SchemaProofSnapshot {
            kind: "exact".to_string(),
            schema_keys: schema_keys.iter().cloned().collect(),
        },
        SchemaProof::Unknown => SchemaProofSnapshot {
            kind: "unknown".to_string(),
            schema_keys: Vec::new(),
        },
    }
}

fn target_set_proof_snapshot(proof: &TargetSetProof) -> TargetSetProofSnapshot {
    match proof {
        TargetSetProof::Exact(entity_ids) => TargetSetProofSnapshot {
            kind: "exact".to_string(),
            entity_ids: entity_ids.iter().cloned().collect(),
        },
        TargetSetProof::Unknown => TargetSetProofSnapshot {
            kind: "unknown".to_string(),
            entity_ids: Vec::new(),
        },
    }
}

fn resolved_write_plan_snapshot(plan: &ResolvedWritePlan) -> ResolvedWritePlanSnapshot {
    ResolvedWritePlanSnapshot {
        partitions: plan
            .partitions
            .iter()
            .map(resolved_write_partition_snapshot)
            .collect(),
    }
}

fn resolved_write_partition_snapshot(
    partition: &ResolvedWritePartition,
) -> ResolvedWritePartitionSnapshot {
    ResolvedWritePartitionSnapshot {
        execution_mode: write_mode_name(partition.execution_mode).to_string(),
        authoritative_pre_state: partition
            .authoritative_pre_state
            .iter()
            .map(resolved_row_ref_snapshot)
            .collect(),
        authoritative_pre_state_rows: partition
            .authoritative_pre_state_rows
            .iter()
            .map(planned_state_row_snapshot)
            .collect(),
        intended_post_state: partition
            .intended_post_state
            .iter()
            .map(planned_state_row_snapshot)
            .collect(),
        tombstones: partition
            .tombstones
            .iter()
            .map(resolved_row_ref_snapshot)
            .collect(),
        lineage: partition.lineage.iter().map(row_lineage_snapshot).collect(),
        target_write_lane: partition
            .target_write_lane
            .as_ref()
            .map(write_lane_name)
            .map(ToString::to_string),
        filesystem_state: filesystem_transaction_state_snapshot(&partition.filesystem_state),
    }
}

fn resolved_row_ref_snapshot(row: &ResolvedRowRef) -> ResolvedRowRefSnapshot {
    ResolvedRowRefSnapshot {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        version_id: row.version_id.clone(),
        source_change_id: row.source_change_id.clone(),
        source_commit_id: row.source_commit_id.clone(),
    }
}

fn planned_state_row_snapshot(row: &PlannedStateRow) -> PlannedStateRowSnapshot {
    PlannedStateRowSnapshot {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        version_id: row.version_id.clone(),
        values: row.values.clone(),
        tombstone: row.tombstone,
    }
}

fn row_lineage_snapshot(lineage: &RowLineage) -> RowLineageSnapshot {
    RowLineageSnapshot {
        entity_id: lineage.entity_id.clone(),
        source_change_id: lineage.source_change_id.clone(),
        source_commit_id: lineage.source_commit_id.clone(),
    }
}

fn filesystem_transaction_state_snapshot(
    state: &FilesystemTransactionState,
) -> FilesystemTransactionStateSnapshot {
    FilesystemTransactionStateSnapshot {
        files: state
            .files
            .values()
            .map(filesystem_transaction_file_state_snapshot)
            .collect(),
    }
}

fn filesystem_transaction_file_state_snapshot(
    state: &FilesystemTransactionFileState,
) -> FilesystemTransactionFileStateSnapshot {
    FilesystemTransactionFileStateSnapshot {
        file_id: state.file_id.clone(),
        version_id: state.version_id.clone(),
        untracked: state.untracked,
        descriptor: state
            .descriptor
            .as_ref()
            .map(filesystem_descriptor_state_snapshot),
        metadata_patch: optional_text_patch_name(&state.metadata_patch).to_string(),
        has_data: state.data.is_some(),
        data_len: state.data.as_ref().map(Vec::len),
        deleted: state.deleted,
    }
}

fn filesystem_descriptor_state_snapshot(
    state: &FilesystemDescriptorState,
) -> FilesystemDescriptorStateSnapshot {
    FilesystemDescriptorStateSnapshot {
        directory_id: state.directory_id.clone(),
        name: state.name.clone(),
        extension: state.extension.clone(),
        metadata: state.metadata.clone(),
        hidden: state.hidden,
    }
}

fn commit_preconditions_snapshot(
    preconditions: &CommitPreconditions,
) -> CommitPreconditionsSnapshot {
    CommitPreconditionsSnapshot {
        write_lane: write_lane_name(&preconditions.write_lane).to_string(),
        expected_head: expected_head_name(&preconditions.expected_head).to_string(),
        idempotency_key: preconditions.idempotency_key.0.clone(),
    }
}

fn domain_change_batch_snapshot(batch: &DomainChangeBatch) -> DomainChangeBatchSnapshot {
    DomainChangeBatchSnapshot {
        changes: batch
            .changes
            .iter()
            .map(public_domain_change_snapshot)
            .collect(),
        write_lane: write_lane_name(&batch.write_lane).to_string(),
        writer_key: batch.writer_key.clone(),
        semantic_effects: batch
            .semantic_effects
            .iter()
            .map(semantic_effect_snapshot)
            .collect(),
    }
}

fn public_domain_change_snapshot(change: &PublicDomainChange) -> PublicDomainChangeSnapshot {
    PublicDomainChangeSnapshot {
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone(),
        file_id: change.file_id.clone(),
        plugin_key: change.plugin_key.clone(),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        version_id: change.version_id.clone(),
        writer_key: change.writer_key.clone(),
    }
}

fn semantic_effect_snapshot(effect: &SemanticEffect) -> SemanticEffectSnapshot {
    SemanticEffectSnapshot {
        effect_key: effect.effect_key.clone(),
        target: effect.target.clone(),
    }
}

fn plan_effects_snapshot(effects: &PlanEffects) -> PlanEffectsSnapshot {
    PlanEffectsSnapshot {
        state_commit_stream_changes: effects.state_commit_stream_changes.clone(),
        session_delta: effects.session_delta.clone(),
        file_cache_refresh_targets: effects
            .file_cache_refresh_targets
            .iter()
            .map(|(file_id, schema_key)| FileCacheRefreshTargetSnapshot {
                file_id: file_id.clone(),
                schema_key: schema_key.clone(),
            })
            .collect(),
    }
}

fn lowered_read_program_snapshot(program: &LoweredReadProgram) -> LoweredReadProgramSnapshot {
    LoweredReadProgramSnapshot {
        statements: program
            .statements
            .iter()
            .map(lowered_read_statement_snapshot)
            .collect(),
        pushdown_decision: pushdown_snapshot(&program.pushdown_decision),
        result_columns: lowered_result_columns_snapshot(&program.result_columns),
    }
}

fn lowered_read_statement_snapshot(
    statement: &LoweredReadStatement,
) -> LoweredReadStatementSnapshot {
    LoweredReadStatementSnapshot {
        shell_statement_sql: statement.shell_statement.to_string(),
        bindings: lowered_statement_bindings_snapshot(&statement.bindings),
        relation_render_nodes: statement
            .relation_render_nodes
            .iter()
            .map(terminal_relation_render_node_snapshot)
            .collect(),
    }
}

fn lowered_statement_bindings_snapshot(
    bindings: &LoweredStatementBindings,
) -> LoweredStatementBindingsSnapshot {
    LoweredStatementBindingsSnapshot {
        used_bindings: bindings
            .used_bindings
            .iter()
            .map(statement_binding_snapshot)
            .collect(),
        minimum_param_count: bindings.minimum_param_count,
    }
}

fn statement_binding_snapshot(binding: &StatementBindingSource) -> ExplainStatementBindingSnapshot {
    match binding {
        StatementBindingSource::UserParam(index) => {
            ExplainStatementBindingSnapshot::UserParam(*index)
        }
        StatementBindingSource::Runtime(kind) => {
            ExplainStatementBindingSnapshot::Runtime(runtime_binding_kind_snapshot(*kind))
        }
    }
}

fn runtime_binding_kind_snapshot(kind: RuntimeBindingKind) -> ExplainRuntimeBindingKind {
    match kind {
        RuntimeBindingKind::ActiveVersionId => ExplainRuntimeBindingKind::ActiveVersionId,
        RuntimeBindingKind::ActiveAccountIdsJson => ExplainRuntimeBindingKind::ActiveAccountIdsJson,
    }
}

fn terminal_relation_render_node_snapshot(
    node: &TerminalRelationRenderNode,
) -> TerminalRelationRenderNodeSnapshot {
    TerminalRelationRenderNodeSnapshot {
        placeholder_relation_name: node.placeholder_relation_name.clone(),
        alias: node.alias.to_string(),
        rendered_factor_sql: node.rendered_factor_sql.clone(),
    }
}

fn lowered_result_columns_snapshot(columns: &LoweredResultColumns) -> LoweredResultColumnsSnapshot {
    match columns {
        LoweredResultColumns::Static(columns) => LoweredResultColumnsSnapshot {
            kind: "static".to_string(),
            static_columns: columns
                .iter()
                .map(lowered_result_column_name)
                .map(ToString::to_string)
                .collect(),
            by_column_name: BTreeMap::new(),
        },
        LoweredResultColumns::ByColumnName(columns) => LoweredResultColumnsSnapshot {
            kind: "by_column_name".to_string(),
            static_columns: Vec::new(),
            by_column_name: columns
                .iter()
                .map(|(name, column)| {
                    (name.clone(), lowered_result_column_name(column).to_string())
                })
                .collect(),
        },
    }
}

fn direct_public_read_plan_snapshot(plan: &DirectPublicReadPlan) -> ExplainDirectPublicReadPlan {
    match plan {
        DirectPublicReadPlan::StateHistory(plan) => ExplainDirectPublicReadPlan::StateHistory(
            Box::new(state_history_direct_plan_snapshot(plan)),
        ),
        DirectPublicReadPlan::EntityHistory(plan) => ExplainDirectPublicReadPlan::EntityHistory(
            Box::new(entity_history_direct_plan_snapshot(plan)),
        ),
        DirectPublicReadPlan::FileHistory(plan) => ExplainDirectPublicReadPlan::FileHistory(
            Box::new(file_history_direct_plan_snapshot(plan)),
        ),
        DirectPublicReadPlan::DirectoryHistory(plan) => {
            ExplainDirectPublicReadPlan::DirectoryHistory(Box::new(
                directory_history_direct_plan_snapshot(plan),
            ))
        }
    }
}

fn state_history_direct_plan_snapshot(
    plan: &StateHistoryDirectReadPlan,
) -> DirectPlanSummarySnapshot {
    DirectPlanSummarySnapshot {
        request: DirectPlanRequestSnapshot::StateLikeHistory(state_history_request_snapshot(
            &plan.request,
        )),
        predicates: plan
            .predicates
            .iter()
            .map(state_history_predicate_text)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(state_history_projection_text)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(state_history_sort_key_text)
            .collect(),
        group_by: plan
            .group_by_fields
            .iter()
            .map(direct_state_history_field_name)
            .map(ToString::to_string)
            .collect(),
        having: plan
            .having
            .as_ref()
            .map(state_history_aggregate_predicate_text),
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        aggregate: None,
        aggregate_output_name: None,
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
        surface_binding: None,
    }
}

fn entity_history_direct_plan_snapshot(
    plan: &EntityHistoryDirectReadPlan,
) -> DirectPlanSummarySnapshot {
    DirectPlanSummarySnapshot {
        request: DirectPlanRequestSnapshot::StateLikeHistory(state_history_request_snapshot(
            &plan.request,
        )),
        predicates: plan
            .predicates
            .iter()
            .map(entity_history_predicate_text)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(entity_history_projection_text)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(entity_history_sort_key_text)
            .collect(),
        group_by: Vec::new(),
        having: None,
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        aggregate: None,
        aggregate_output_name: None,
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
        surface_binding: Some(surface_binding_snapshot(&plan.surface_binding)),
    }
}

fn file_history_direct_plan_snapshot(
    plan: &FileHistoryDirectReadPlan,
) -> DirectPlanSummarySnapshot {
    DirectPlanSummarySnapshot {
        request: DirectPlanRequestSnapshot::FileHistory(file_history_request_snapshot(
            &plan.request,
        )),
        predicates: plan
            .predicates
            .iter()
            .map(file_history_predicate_text)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(file_history_projection_text)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(file_history_sort_key_text)
            .collect(),
        group_by: Vec::new(),
        having: None,
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        aggregate: plan
            .aggregate
            .as_ref()
            .map(file_history_aggregate_name)
            .map(ToString::to_string),
        aggregate_output_name: plan.aggregate_output_name.clone(),
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
        surface_binding: None,
    }
}

fn directory_history_direct_plan_snapshot(
    plan: &DirectoryHistoryDirectReadPlan,
) -> DirectPlanSummarySnapshot {
    DirectPlanSummarySnapshot {
        request: DirectPlanRequestSnapshot::DirectoryHistory(directory_history_request_snapshot(
            &plan.request,
        )),
        predicates: plan
            .predicates
            .iter()
            .map(directory_history_predicate_text)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(directory_history_projection_text)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(directory_history_sort_key_text)
            .collect(),
        group_by: Vec::new(),
        having: None,
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        aggregate: plan
            .aggregate
            .as_ref()
            .map(directory_history_aggregate_name)
            .map(ToString::to_string),
        aggregate_output_name: plan.aggregate_output_name.clone(),
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
        surface_binding: None,
    }
}

fn state_history_request_snapshot(
    request: &crate::read::history::StateHistoryRequest,
) -> StateHistoryRequestSnapshot {
    StateHistoryRequestSnapshot {
        root_scope: state_history_root_scope_name(&request.root_scope).to_string(),
        requested_roots: state_history_requested_roots(&request.root_scope),
        lineage_scope: state_history_lineage_scope_name(request.lineage_scope).to_string(),
        active_version_id: request.active_version_id.clone(),
        version_scope: state_history_version_scope_name(&request.version_scope).to_string(),
        requested_versions: state_history_requested_versions(&request.version_scope),
        entity_ids: request.entity_ids.clone(),
        file_ids: request.file_ids.clone(),
        schema_keys: request.schema_keys.clone(),
        plugin_keys: request.plugin_keys.clone(),
        min_depth: request.min_depth,
        max_depth: request.max_depth,
        content_mode: state_history_content_mode_name(request.content_mode).to_string(),
        order: state_history_order_name(request.order).to_string(),
    }
}

fn file_history_request_snapshot(
    request: &crate::read::models::FileHistoryRequest,
) -> FileHistoryRequestSnapshot {
    FileHistoryRequestSnapshot {
        lineage_scope: file_history_lineage_scope_name(request.lineage_scope).to_string(),
        active_version_id: request.active_version_id.clone(),
        root_scope: file_history_root_scope_name(&request.root_scope).to_string(),
        requested_roots: file_history_requested_roots(&request.root_scope),
        version_scope: file_history_version_scope_name(&request.version_scope).to_string(),
        requested_versions: file_history_requested_versions(&request.version_scope),
        file_ids: request.file_ids.clone(),
        content_mode: file_history_content_mode_name(request.content_mode).to_string(),
    }
}

fn directory_history_request_snapshot(
    request: &crate::read::models::DirectoryHistoryRequest,
) -> DirectoryHistoryRequestSnapshot {
    DirectoryHistoryRequestSnapshot {
        lineage_scope: file_history_lineage_scope_name(request.lineage_scope).to_string(),
        active_version_id: request.active_version_id.clone(),
        root_scope: file_history_root_scope_name(&request.root_scope).to_string(),
        requested_roots: file_history_requested_roots(&request.root_scope),
        version_scope: file_history_version_scope_name(&request.version_scope).to_string(),
        requested_versions: file_history_requested_versions(&request.version_scope),
        directory_ids: request.directory_ids.clone(),
    }
}

fn surface_binding_snapshot(binding: &SurfaceBinding) -> SurfaceBindingSnapshot {
    SurfaceBindingSnapshot {
        public_name: binding.descriptor.public_name.clone(),
        surface_family: surface_family_snapshot(binding.descriptor.surface_family),
        surface_variant: surface_variant_snapshot(binding.descriptor.surface_variant),
        capability: surface_capability_snapshot(binding.capability),
        read_freshness: surface_read_freshness_name(binding.read_freshness).to_string(),
        read_semantics: surface_read_semantics_name(binding.read_semantics).to_string(),
        default_scope: default_scope_name(binding.default_scope).to_string(),
        exposed_columns: binding.exposed_columns.clone(),
        visible_columns: binding.descriptor.visible_columns.clone(),
        hidden_columns: binding.descriptor.hidden_columns.clone(),
        fixed_schema_key: binding.implicit_overrides.fixed_schema_key.clone(),
        expose_version_id: binding.implicit_overrides.expose_version_id,
    }
}

fn dependency_spec_snapshot(spec: &DependencySpec) -> DependencySpecSnapshot {
    DependencySpecSnapshot {
        relations: spec.relations.iter().cloned().collect(),
        schema_keys: spec.schema_keys.iter().cloned().collect(),
        entity_ids: spec.entity_ids.iter().cloned().collect(),
        file_ids: spec.file_ids.iter().cloned().collect(),
        version_ids: spec.version_ids.iter().cloned().collect(),
        session_dependencies: spec.session_dependencies.iter().cloned().collect(),
        writer_filter: DependencyWriterFilterSnapshot {
            include: spec.writer_filter.include.iter().cloned().collect(),
            exclude: spec.writer_filter.exclude.iter().cloned().collect(),
        },
        include_untracked: spec.include_untracked,
        depends_on_active_version: spec.depends_on_active_version,
        precision: dependency_precision_name(spec.precision).to_string(),
    }
}

fn surface_read_semantics_name(semantics: SurfaceReadSemantics) -> &'static str {
    match semantics {
        SurfaceReadSemantics::CommittedGraph => "committed_graph",
        SurfaceReadSemantics::WorkspaceEffective => "workspace_effective",
        SurfaceReadSemantics::CanonicalHistory => "canonical_history",
        SurfaceReadSemantics::WorkspaceChanges => "workspace_changes",
    }
}

fn effective_state_request_snapshot(
    request: &EffectiveStateRequest,
) -> EffectiveStateRequestSnapshot {
    EffectiveStateRequestSnapshot {
        schema_set: request.schema_set.iter().cloned().collect(),
        version_scope: version_scope_name(request.version_scope).to_string(),
        include_global_overlay: request.include_global_overlay,
        include_untracked_overlay: request.include_untracked_overlay,
        include_tombstones: request.include_tombstones,
        predicate_classes: request.predicate_classes.clone(),
        required_columns: request.required_columns.clone(),
    }
}

fn effective_state_plan_snapshot(plan: &EffectiveStatePlan) -> EffectiveStatePlanSnapshot {
    EffectiveStatePlanSnapshot {
        state_source: state_source_authority_name(plan.state_source).to_string(),
        overlay_lanes: plan
            .overlay_lanes
            .iter()
            .map(overlay_lane_name)
            .map(ToString::to_string)
            .collect(),
        pushdown_safe_predicates: plan
            .pushdown_safe_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
        residual_predicates: plan
            .residual_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

fn prepared_statement_snapshot(statement: &PreparedStatement) -> PreparedStatementSnapshot {
    PreparedStatementSnapshot {
        sql: statement.sql.clone(),
        params: statement.params.clone(),
    }
}

fn schema_live_table_requirement_snapshot(
    requirement: &SchemaLiveTableRequirement,
) -> SchemaLiveTableRequirementSnapshot {
    SchemaLiveTableRequirementSnapshot {
        schema_key: requirement.schema_key.clone(),
        schema_definition: requirement.schema_definition.clone(),
    }
}

fn mutation_row_snapshot(row: &MutationRow) -> MutationRowSnapshot {
    MutationRowSnapshot {
        operation: match row.operation {
            MutationOperation::Insert => "insert",
        }
        .to_string(),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        plugin_key: row.plugin_key.clone(),
        snapshot_content: row.snapshot_content.clone(),
        untracked: row.untracked,
    }
}

fn update_validation_plan_snapshot(plan: &UpdateValidationPlan) -> UpdateValidationPlanSnapshot {
    UpdateValidationPlanSnapshot {
        delete: plan.delete,
        table: plan.table.clone(),
        where_clause: plan.where_clause.as_ref().map(ToString::to_string),
        snapshot_content: plan.snapshot_content.clone(),
        snapshot_patch: plan.snapshot_patch.clone(),
    }
}

fn pretty_json<T: Serialize>(value: &T) -> Result<String, LixError> {
    serde_json::to_string_pretty(value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to serialize explain section: {error}"),
        )
    })
}

fn surface_family_snapshot(family: SurfaceFamily) -> ExplainSurfaceFamily {
    match family {
        SurfaceFamily::State => ExplainSurfaceFamily::State,
        SurfaceFamily::Entity => ExplainSurfaceFamily::Entity,
        SurfaceFamily::Filesystem => ExplainSurfaceFamily::Filesystem,
        SurfaceFamily::Admin => ExplainSurfaceFamily::Admin,
        SurfaceFamily::Change => ExplainSurfaceFamily::Change,
    }
}

fn surface_variant_snapshot(variant: SurfaceVariant) -> ExplainSurfaceVariant {
    match variant {
        SurfaceVariant::Default => ExplainSurfaceVariant::Default,
        SurfaceVariant::ByVersion => ExplainSurfaceVariant::ByVersion,
        SurfaceVariant::History => ExplainSurfaceVariant::History,
        SurfaceVariant::WorkingChanges => ExplainSurfaceVariant::WorkingChanges,
    }
}

fn surface_capability_snapshot(capability: SurfaceCapability) -> ExplainSurfaceCapability {
    match capability {
        SurfaceCapability::ReadOnly => ExplainSurfaceCapability::ReadOnly,
        SurfaceCapability::ReadWrite => ExplainSurfaceCapability::ReadWrite,
    }
}

fn sql_dialect_name(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => "sqlite",
        SqlDialect::Postgres => "postgres",
    }
}

fn surface_read_freshness_name(freshness: SurfaceReadFreshness) -> &'static str {
    match freshness {
        SurfaceReadFreshness::RequiresFreshProjection => "requires_fresh_projection",
        SurfaceReadFreshness::AllowsStaleProjection => "allows_stale_projection",
    }
}

fn default_scope_name(scope: crate::sql::catalog::DefaultScopeSemantics) -> &'static str {
    match scope {
        crate::sql::catalog::DefaultScopeSemantics::ActiveVersion => "active_version",
        crate::sql::catalog::DefaultScopeSemantics::ExplicitVersion => "explicit_version",
        crate::sql::catalog::DefaultScopeSemantics::History => "history",
        crate::sql::catalog::DefaultScopeSemantics::GlobalAdmin => "global_admin",
        crate::sql::catalog::DefaultScopeSemantics::WorkingChanges => "working_changes",
    }
}

fn dependency_precision_name(precision: DependencyPrecision) -> &'static str {
    match precision {
        DependencyPrecision::Precise => "precise",
        DependencyPrecision::Conservative => "conservative",
    }
}

fn version_scope_name(scope: VersionScope) -> &'static str {
    match scope {
        VersionScope::ActiveVersion => "active_version",
        VersionScope::ExplicitVersion => "explicit_version",
        VersionScope::History => "history",
    }
}

fn filesystem_kind_name(kind: FilesystemKind) -> &'static str {
    match kind {
        FilesystemKind::File => "file",
        FilesystemKind::Directory => "directory",
    }
}

fn canonical_admin_kind_name(kind: CanonicalAdminKind) -> &'static str {
    match kind {
        CanonicalAdminKind::Version => "version",
    }
}

fn read_contract_name(contract: ReadContract) -> &'static str {
    match contract {
        ReadContract::CommittedAtStart => "committed_at_start",
    }
}

fn write_operation_kind_name(kind: WriteOperationKind) -> &'static str {
    match kind {
        WriteOperationKind::Insert => "insert",
        WriteOperationKind::Update => "update",
        WriteOperationKind::Delete => "delete",
    }
}

fn write_mode_request_name(mode: WriteModeRequest) -> &'static str {
    match mode {
        WriteModeRequest::Auto => "auto",
        WriteModeRequest::ForceTracked => "force_tracked",
        WriteModeRequest::ForceUntracked => "force_untracked",
    }
}

fn write_mode_name(mode: WriteMode) -> &'static str {
    match mode {
        WriteMode::Tracked => "tracked",
        WriteMode::Untracked => "untracked",
    }
}

fn state_source_kind_name(kind: StateSourceKind) -> &'static str {
    match kind {
        StateSourceKind::AuthoritativeCommitted => "authoritative_committed",
        StateSourceKind::UntrackedOverlay => "untracked_overlay",
    }
}

fn state_source_authority_name(kind: StateSourceAuthority) -> &'static str {
    match kind {
        StateSourceAuthority::AuthoritativeCommitted => "authoritative_committed",
    }
}

fn write_lane_name(lane: &WriteLane) -> &'static str {
    match lane {
        WriteLane::ActiveVersion => "active_version",
        WriteLane::SingleVersion(_) => "single_version",
        WriteLane::GlobalAdmin => "global_admin",
    }
}

fn expected_head_name(expected: &ExpectedHead) -> &'static str {
    match expected {
        ExpectedHead::CurrentHead => "current_head",
    }
}

fn overlay_lane_name(lane: &OverlayLane) -> &'static str {
    match lane {
        OverlayLane::GlobalTracked => "global_tracked",
        OverlayLane::LocalTracked => "local_tracked",
        OverlayLane::GlobalUntracked => "global_untracked",
        OverlayLane::LocalUntracked => "local_untracked",
    }
}

fn optional_text_patch_name(patch: &OptionalTextPatch) -> &'static str {
    match patch {
        OptionalTextPatch::Unchanged => "unchanged",
    }
}

fn lowered_result_column_name(column: &LoweredResultColumn) -> &'static str {
    match column {
        LoweredResultColumn::Untyped => "untyped",
        LoweredResultColumn::Boolean => "boolean",
    }
}

fn result_contract_name(contract: ResultContract) -> &'static str {
    match contract {
        ResultContract::Select => "select",
        ResultContract::DmlNoReturning => "dml_no_returning",
        ResultContract::DmlReturning => "dml_returning",
        ResultContract::Other => "other",
    }
}

fn state_history_root_scope_name(
    scope: &crate::read::history::StateHistoryRootScope,
) -> &'static str {
    match scope {
        crate::read::history::StateHistoryRootScope::AllRoots => "all_roots",
        crate::read::history::StateHistoryRootScope::RequestedRoots(_) => "requested_roots",
    }
}

fn state_history_requested_roots(
    scope: &crate::read::history::StateHistoryRootScope,
) -> Vec<String> {
    match scope {
        crate::read::history::StateHistoryRootScope::AllRoots => Vec::new(),
        crate::read::history::StateHistoryRootScope::RequestedRoots(roots) => roots.clone(),
    }
}

fn state_history_lineage_scope_name(
    scope: crate::read::history::StateHistoryLineageScope,
) -> &'static str {
    match scope {
        crate::read::history::StateHistoryLineageScope::Standard => "standard",
        crate::read::history::StateHistoryLineageScope::ActiveVersion => "active_version",
    }
}

fn state_history_version_scope_name(
    scope: &crate::read::history::StateHistoryVersionScope,
) -> &'static str {
    match scope {
        crate::read::history::StateHistoryVersionScope::Any => "any",
        crate::read::history::StateHistoryVersionScope::RequestedVersions(_) => {
            "requested_versions"
        }
    }
}

fn state_history_requested_versions(
    scope: &crate::read::history::StateHistoryVersionScope,
) -> Vec<String> {
    match scope {
        crate::read::history::StateHistoryVersionScope::Any => Vec::new(),
        crate::read::history::StateHistoryVersionScope::RequestedVersions(versions) => {
            versions.clone()
        }
    }
}

fn state_history_content_mode_name(
    mode: crate::read::history::StateHistoryContentMode,
) -> &'static str {
    match mode {
        crate::read::history::StateHistoryContentMode::MetadataOnly => "metadata_only",
        crate::read::history::StateHistoryContentMode::IncludeSnapshotContent => {
            "include_snapshot_content"
        }
    }
}

fn state_history_order_name(order: crate::read::history::StateHistoryOrder) -> &'static str {
    match order {
        crate::read::history::StateHistoryOrder::EntityFileSchemaDepthAsc => {
            "entity_file_schema_depth_asc"
        }
    }
}

fn file_history_root_scope_name(scope: &crate::read::models::FileHistoryRootScope) -> &'static str {
    match scope {
        crate::read::models::FileHistoryRootScope::AllRoots => "all_roots",
        crate::read::models::FileHistoryRootScope::RequestedRoots(_) => "requested_roots",
    }
}

fn file_history_requested_roots(scope: &crate::read::models::FileHistoryRootScope) -> Vec<String> {
    match scope {
        crate::read::models::FileHistoryRootScope::AllRoots => Vec::new(),
        crate::read::models::FileHistoryRootScope::RequestedRoots(roots) => roots.clone(),
    }
}

fn file_history_version_scope_name(
    scope: &crate::read::models::FileHistoryVersionScope,
) -> &'static str {
    match scope {
        crate::read::models::FileHistoryVersionScope::Any => "any",
        crate::read::models::FileHistoryVersionScope::RequestedVersions(_) => "requested_versions",
    }
}

fn file_history_requested_versions(
    scope: &crate::read::models::FileHistoryVersionScope,
) -> Vec<String> {
    match scope {
        crate::read::models::FileHistoryVersionScope::Any => Vec::new(),
        crate::read::models::FileHistoryVersionScope::RequestedVersions(versions) => {
            versions.clone()
        }
    }
}

fn file_history_lineage_scope_name(
    scope: crate::read::models::FileHistoryLineageScope,
) -> &'static str {
    match scope {
        crate::read::models::FileHistoryLineageScope::ActiveVersion => "active_version",
        crate::read::models::FileHistoryLineageScope::Standard => "standard",
    }
}

fn file_history_content_mode_name(
    mode: crate::read::models::FileHistoryContentMode,
) -> &'static str {
    match mode {
        crate::read::models::FileHistoryContentMode::MetadataOnly => "metadata_only",
        crate::read::models::FileHistoryContentMode::IncludeData => "include_data",
    }
}

fn direct_state_history_field_name(field: &DirectStateHistoryField) -> &'static str {
    match field {
        DirectStateHistoryField::EntityId => "entity_id",
        DirectStateHistoryField::SchemaKey => "schema_key",
        DirectStateHistoryField::FileId => "file_id",
        DirectStateHistoryField::PluginKey => "plugin_key",
        DirectStateHistoryField::SnapshotContent => "snapshot_content",
        DirectStateHistoryField::Metadata => "metadata",
        DirectStateHistoryField::SchemaVersion => "schema_version",
        DirectStateHistoryField::ChangeId => "change_id",
        DirectStateHistoryField::CommitId => "commit_id",
        DirectStateHistoryField::CommitCreatedAt => "commit_created_at",
        DirectStateHistoryField::RootCommitId => "root_commit_id",
        DirectStateHistoryField::Depth => "depth",
        DirectStateHistoryField::VersionId => "version_id",
    }
}

fn direct_entity_history_field_name(field: &DirectEntityHistoryField) -> String {
    match field {
        DirectEntityHistoryField::Property(name) => format!("property:{name}"),
        DirectEntityHistoryField::State(field) => {
            format!("state:{}", direct_state_history_field_name(field))
        }
    }
}

fn direct_file_history_field_name(field: &DirectFileHistoryField) -> &'static str {
    match field {
        DirectFileHistoryField::Id => "id",
        DirectFileHistoryField::Path => "path",
        DirectFileHistoryField::Data => "data",
        DirectFileHistoryField::Metadata => "metadata",
        DirectFileHistoryField::Hidden => "hidden",
        DirectFileHistoryField::EntityId => "entity_id",
        DirectFileHistoryField::SchemaKey => "schema_key",
        DirectFileHistoryField::FileId => "file_id",
        DirectFileHistoryField::VersionId => "version_id",
        DirectFileHistoryField::PluginKey => "plugin_key",
        DirectFileHistoryField::SchemaVersion => "schema_version",
        DirectFileHistoryField::ChangeId => "change_id",
        DirectFileHistoryField::LixcolMetadata => "lixcol_metadata",
        DirectFileHistoryField::CommitId => "commit_id",
        DirectFileHistoryField::CommitCreatedAt => "commit_created_at",
        DirectFileHistoryField::RootCommitId => "root_commit_id",
        DirectFileHistoryField::Depth => "depth",
    }
}

fn direct_directory_history_field_name(field: &DirectDirectoryHistoryField) -> &'static str {
    match field {
        DirectDirectoryHistoryField::Id => "id",
        DirectDirectoryHistoryField::ParentId => "parent_id",
        DirectDirectoryHistoryField::Name => "name",
        DirectDirectoryHistoryField::Path => "path",
        DirectDirectoryHistoryField::Hidden => "hidden",
        DirectDirectoryHistoryField::EntityId => "entity_id",
        DirectDirectoryHistoryField::SchemaKey => "schema_key",
        DirectDirectoryHistoryField::FileId => "file_id",
        DirectDirectoryHistoryField::VersionId => "version_id",
        DirectDirectoryHistoryField::PluginKey => "plugin_key",
        DirectDirectoryHistoryField::SchemaVersion => "schema_version",
        DirectDirectoryHistoryField::ChangeId => "change_id",
        DirectDirectoryHistoryField::LixcolMetadata => "lixcol_metadata",
        DirectDirectoryHistoryField::CommitId => "commit_id",
        DirectDirectoryHistoryField::CommitCreatedAt => "commit_created_at",
        DirectDirectoryHistoryField::RootCommitId => "root_commit_id",
        DirectDirectoryHistoryField::Depth => "depth",
    }
}

fn value_literal(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Boolean(value) => value.to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => format!("{value:?}"),
        Value::Json(value) => value.to_string(),
        Value::Blob(value) => format!("blob(len={})", value.len()),
    }
}

fn state_history_projection_text(projection: &StateHistoryProjection) -> String {
    let value = match &projection.value {
        StateHistoryProjectionValue::Field(field) => {
            direct_state_history_field_name(field).to_string()
        }
        StateHistoryProjectionValue::Aggregate(aggregate) => {
            state_history_aggregate_name(aggregate).to_string()
        }
    };
    format!("{} <- {}", projection.output_name, value)
}

fn state_history_sort_key_text(key: &StateHistorySortKey) -> String {
    let value = key
        .value
        .as_ref()
        .map(|value| match value {
            StateHistorySortValue::Field(field) => {
                direct_state_history_field_name(field).to_string()
            }
            StateHistorySortValue::Aggregate(aggregate) => {
                state_history_aggregate_name(aggregate).to_string()
            }
        })
        .unwrap_or_else(|| key.output_name.clone());
    format!("{} {}", value, if key.descending { "desc" } else { "asc" })
}

fn state_history_predicate_text(predicate: &StateHistoryPredicate) -> String {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => {
            format!(
                "{} = {}",
                direct_state_history_field_name(field),
                value_literal(value)
            )
        }
        StateHistoryPredicate::NotEq(field, value) => {
            format!(
                "{} != {}",
                direct_state_history_field_name(field),
                value_literal(value)
            )
        }
        StateHistoryPredicate::Gt(field, value) => {
            format!(
                "{} > {}",
                direct_state_history_field_name(field),
                value_literal(value)
            )
        }
        StateHistoryPredicate::GtEq(field, value) => {
            format!(
                "{} >= {}",
                direct_state_history_field_name(field),
                value_literal(value)
            )
        }
        StateHistoryPredicate::Lt(field, value) => {
            format!(
                "{} < {}",
                direct_state_history_field_name(field),
                value_literal(value)
            )
        }
        StateHistoryPredicate::LtEq(field, value) => {
            format!(
                "{} <= {}",
                direct_state_history_field_name(field),
                value_literal(value)
            )
        }
        StateHistoryPredicate::In(field, values) => format!(
            "{} IN ({})",
            direct_state_history_field_name(field),
            values
                .iter()
                .map(value_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        StateHistoryPredicate::IsNull(field) => {
            format!("{} IS NULL", direct_state_history_field_name(field))
        }
        StateHistoryPredicate::IsNotNull(field) => {
            format!("{} IS NOT NULL", direct_state_history_field_name(field))
        }
    }
}

fn state_history_aggregate_name(aggregate: &StateHistoryAggregate) -> &'static str {
    match aggregate {
        StateHistoryAggregate::Count => "count",
    }
}

fn state_history_aggregate_predicate_text(predicate: &StateHistoryAggregatePredicate) -> String {
    match predicate {
        StateHistoryAggregatePredicate::Eq(aggregate, value) => {
            format!("{} = {}", state_history_aggregate_name(aggregate), value)
        }
        StateHistoryAggregatePredicate::NotEq(aggregate, value) => {
            format!("{} != {}", state_history_aggregate_name(aggregate), value)
        }
        StateHistoryAggregatePredicate::Gt(aggregate, value) => {
            format!("{} > {}", state_history_aggregate_name(aggregate), value)
        }
        StateHistoryAggregatePredicate::GtEq(aggregate, value) => {
            format!("{} >= {}", state_history_aggregate_name(aggregate), value)
        }
        StateHistoryAggregatePredicate::Lt(aggregate, value) => {
            format!("{} < {}", state_history_aggregate_name(aggregate), value)
        }
        StateHistoryAggregatePredicate::LtEq(aggregate, value) => {
            format!("{} <= {}", state_history_aggregate_name(aggregate), value)
        }
    }
}

fn entity_history_projection_text(projection: &EntityHistoryProjection) -> String {
    format!(
        "{} <- {}",
        projection.output_name,
        direct_entity_history_field_name(&projection.field)
    )
}

fn entity_history_sort_key_text(key: &EntityHistorySortKey) -> String {
    let value = key
        .field
        .as_ref()
        .map(direct_entity_history_field_name)
        .unwrap_or_else(|| key.output_name.clone());
    format!("{} {}", value, if key.descending { "desc" } else { "asc" })
}

fn entity_history_predicate_text(predicate: &EntityHistoryPredicate) -> String {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => {
            format!(
                "{} = {}",
                direct_entity_history_field_name(field),
                value_literal(value)
            )
        }
        EntityHistoryPredicate::NotEq(field, value) => {
            format!(
                "{} != {}",
                direct_entity_history_field_name(field),
                value_literal(value)
            )
        }
        EntityHistoryPredicate::Gt(field, value) => {
            format!(
                "{} > {}",
                direct_entity_history_field_name(field),
                value_literal(value)
            )
        }
        EntityHistoryPredicate::GtEq(field, value) => {
            format!(
                "{} >= {}",
                direct_entity_history_field_name(field),
                value_literal(value)
            )
        }
        EntityHistoryPredicate::Lt(field, value) => {
            format!(
                "{} < {}",
                direct_entity_history_field_name(field),
                value_literal(value)
            )
        }
        EntityHistoryPredicate::LtEq(field, value) => {
            format!(
                "{} <= {}",
                direct_entity_history_field_name(field),
                value_literal(value)
            )
        }
        EntityHistoryPredicate::In(field, values) => format!(
            "{} IN ({})",
            direct_entity_history_field_name(field),
            values
                .iter()
                .map(value_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        EntityHistoryPredicate::IsNull(field) => {
            format!("{} IS NULL", direct_entity_history_field_name(field))
        }
        EntityHistoryPredicate::IsNotNull(field) => {
            format!("{} IS NOT NULL", direct_entity_history_field_name(field))
        }
    }
}

fn file_history_projection_text(projection: &FileHistoryProjection) -> String {
    format!(
        "{} <- {}",
        projection.output_name,
        direct_file_history_field_name(&projection.field)
    )
}

fn file_history_sort_key_text(key: &FileHistorySortKey) -> String {
    let value = key
        .field
        .as_ref()
        .map(|field| direct_file_history_field_name(field).to_string())
        .unwrap_or_else(|| key.output_name.clone());
    format!("{} {}", value, if key.descending { "desc" } else { "asc" })
}

fn file_history_predicate_text(predicate: &FileHistoryPredicate) -> String {
    match predicate {
        FileHistoryPredicate::Eq(field, value) => {
            format!(
                "{} = {}",
                direct_file_history_field_name(field),
                value_literal(value)
            )
        }
        FileHistoryPredicate::NotEq(field, value) => {
            format!(
                "{} != {}",
                direct_file_history_field_name(field),
                value_literal(value)
            )
        }
        FileHistoryPredicate::Gt(field, value) => {
            format!(
                "{} > {}",
                direct_file_history_field_name(field),
                value_literal(value)
            )
        }
        FileHistoryPredicate::GtEq(field, value) => {
            format!(
                "{} >= {}",
                direct_file_history_field_name(field),
                value_literal(value)
            )
        }
        FileHistoryPredicate::Lt(field, value) => {
            format!(
                "{} < {}",
                direct_file_history_field_name(field),
                value_literal(value)
            )
        }
        FileHistoryPredicate::LtEq(field, value) => {
            format!(
                "{} <= {}",
                direct_file_history_field_name(field),
                value_literal(value)
            )
        }
        FileHistoryPredicate::In(field, values) => format!(
            "{} IN ({})",
            direct_file_history_field_name(field),
            values
                .iter()
                .map(value_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        FileHistoryPredicate::IsNull(field) => {
            format!("{} IS NULL", direct_file_history_field_name(field))
        }
        FileHistoryPredicate::IsNotNull(field) => {
            format!("{} IS NOT NULL", direct_file_history_field_name(field))
        }
    }
}

fn file_history_aggregate_name(aggregate: &FileHistoryAggregate) -> &'static str {
    match aggregate {
        FileHistoryAggregate::Count => "count",
    }
}

fn directory_history_projection_text(projection: &DirectoryHistoryProjection) -> String {
    format!(
        "{} <- {}",
        projection.output_name,
        direct_directory_history_field_name(&projection.field)
    )
}

fn directory_history_sort_key_text(key: &DirectoryHistorySortKey) -> String {
    let value = key
        .field
        .as_ref()
        .map(|field| direct_directory_history_field_name(field).to_string())
        .unwrap_or_else(|| key.output_name.clone());
    format!("{} {}", value, if key.descending { "desc" } else { "asc" })
}

fn directory_history_predicate_text(predicate: &DirectoryHistoryPredicate) -> String {
    match predicate {
        DirectoryHistoryPredicate::Eq(field, value) => format!(
            "{} = {}",
            direct_directory_history_field_name(field),
            value_literal(value)
        ),
        DirectoryHistoryPredicate::NotEq(field, value) => format!(
            "{} != {}",
            direct_directory_history_field_name(field),
            value_literal(value)
        ),
        DirectoryHistoryPredicate::Gt(field, value) => format!(
            "{} > {}",
            direct_directory_history_field_name(field),
            value_literal(value)
        ),
        DirectoryHistoryPredicate::GtEq(field, value) => format!(
            "{} >= {}",
            direct_directory_history_field_name(field),
            value_literal(value)
        ),
        DirectoryHistoryPredicate::Lt(field, value) => format!(
            "{} < {}",
            direct_directory_history_field_name(field),
            value_literal(value)
        ),
        DirectoryHistoryPredicate::LtEq(field, value) => format!(
            "{} <= {}",
            direct_directory_history_field_name(field),
            value_literal(value)
        ),
        DirectoryHistoryPredicate::In(field, values) => format!(
            "{} IN ({})",
            direct_directory_history_field_name(field),
            values
                .iter()
                .map(value_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DirectoryHistoryPredicate::IsNull(field) => {
            format!("{} IS NULL", direct_directory_history_field_name(field))
        }
        DirectoryHistoryPredicate::IsNotNull(field) => {
            format!("{} IS NOT NULL", direct_directory_history_field_name(field))
        }
    }
}

fn directory_history_aggregate_name(aggregate: &DirectoryHistoryAggregate) -> &'static str {
    match aggregate {
        DirectoryHistoryAggregate::Count => "count",
    }
}

fn saturating_duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}
