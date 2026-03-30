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
    SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceReadFreshness, SurfaceVariant,
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
    BroadPublicReadJoin, BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect,
    BroadPublicReadSetExpr, BroadPublicReadStatement, BroadPublicReadTableFactor,
    BroadPublicReadTableWithJoins, CanonicalAdminKind, CanonicalAdminScan, CanonicalChangeScan,
    CanonicalFilesystemScan, CanonicalStateScan, CanonicalWorkingChangesScan, CommitPreconditions,
    ExpectedHead, FilesystemKind, InsertOnConflict, InsertOnConflictAction, MutationPayload,
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
use sqlparser::ast::{
    AnalyzeFormatKind, DescribeAlias, Expr, SetOperator, SetQuantifier, Statement, UtilityOption,
};
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
pub(crate) enum ExplainPublicReadStrategy {
    Structured,
    DirectHistory,
    Broad,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainStateSourceKind {
    AuthoritativeCommitted,
    UntrackedOverlay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainStage {
    Parse,
    Bind,
    SemanticAnalysis,
    LogicalPlanning,
    Optimizer,
    CapabilityResolution,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainSqlDialect {
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainSurfaceReadFreshness {
    RequiresFreshProjection,
    AllowsStaleProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainDefaultScope {
    ActiveVersion,
    ExplicitVersion,
    History,
    GlobalAdmin,
    WorkingChanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainDependencyPrecision {
    Precise,
    Conservative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainVersionScope {
    ActiveVersion,
    ExplicitVersion,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainFilesystemKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainCanonicalAdminKind {
    Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainReadContract {
    CommittedAtStart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainMutationOperation {
    Insert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainInsertOnConflictAction {
    DoUpdate,
    DoNothing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainWriteOperationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainWriteModeRequest {
    Auto,
    ForceTracked,
    ForceUntracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainScopeProofKind {
    ActiveVersion,
    SingleVersion,
    GlobalAdmin,
    FiniteVersionSet,
    Unbounded,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainSchemaProofKind {
    Exact,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainTargetSetProofKind {
    Exact,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainWriteMode {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainExpectedHead {
    CurrentHead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainWriteLaneKind {
    ActiveVersion,
    SingleVersion,
    GlobalAdmin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainOptionalTextPatch {
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainLoweredResultColumnsKind {
    Static,
    ByColumnName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainLoweredResultColumnType {
    Untyped,
    Boolean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainResultContract {
    Select,
    DmlNoReturning,
    DmlReturning,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainHistoryRootScopeKind {
    AllRoots,
    RequestedRoots,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainHistoryLineageScope {
    Standard,
    ActiveVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainHistoryVersionScopeKind {
    Any,
    RequestedVersions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainStateHistoryContentMode {
    MetadataOnly,
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainStateHistoryOrder {
    EntityFileSchemaDepthAsc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainFileHistoryContentMode {
    MetadataOnly,
    IncludeData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainOverlayLane {
    GlobalTracked,
    LocalTracked,
    GlobalUntracked,
    LocalUntracked,
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
    pub(crate) read_freshness: ExplainSurfaceReadFreshness,
    pub(crate) default_scope: ExplainDefaultScope,
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
    pub(crate) precision: ExplainDependencyPrecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EffectiveStateRequestSnapshot {
    pub(crate) schema_set: Vec<String>,
    pub(crate) version_scope: ExplainVersionScope,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) predicate_classes: Vec<String>,
    pub(crate) required_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EffectiveStatePlanSnapshot {
    pub(crate) state_source: ExplainStateSourceKind,
    pub(crate) overlay_lanes: Vec<ExplainOverlayLane>,
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
    pub(crate) operation: ExplainMutationOperation,
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
    pub(crate) dialect: Option<ExplainSqlDialect>,
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
    pub(crate) version_scope: ExplainVersionScope,
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
    pub(crate) kind: ExplainFilesystemKind,
    pub(crate) version_scope: ExplainVersionScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CanonicalAdminScanSnapshot {
    pub(crate) binding: SurfaceBindingSnapshot,
    pub(crate) kind: ExplainCanonicalAdminKind,
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
    pub(crate) contract: ExplainReadContract,
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
    pub(crate) action: ExplainInsertOnConflictAction,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct WriteCommandSnapshot {
    pub(crate) operation_kind: ExplainWriteOperationKind,
    pub(crate) target: SurfaceBindingSnapshot,
    pub(crate) selector: WriteSelectorSnapshot,
    pub(crate) payload: MutationPayloadSnapshot,
    pub(crate) on_conflict: Option<InsertOnConflictSnapshot>,
    pub(crate) requested_mode: ExplainWriteModeRequest,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) execution_context: ExecutionContextSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ScopeProofSnapshot {
    pub(crate) kind: ExplainScopeProofKind,
    pub(crate) versions: Vec<String>,
    pub(crate) version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaProofSnapshot {
    pub(crate) kind: ExplainSchemaProofKind,
    pub(crate) schema_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TargetSetProofSnapshot {
    pub(crate) kind: ExplainTargetSetProofKind,
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
    pub(crate) metadata_patch: ExplainOptionalTextPatch,
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
    pub(crate) execution_mode: ExplainWriteMode,
    pub(crate) authoritative_pre_state: Vec<ResolvedRowRefSnapshot>,
    pub(crate) authoritative_pre_state_rows: Vec<PlannedStateRowSnapshot>,
    pub(crate) intended_post_state: Vec<PlannedStateRowSnapshot>,
    pub(crate) tombstones: Vec<ResolvedRowRefSnapshot>,
    pub(crate) lineage: Vec<RowLineageSnapshot>,
    pub(crate) target_write_lane: Option<ExplainWriteLaneKind>,
    pub(crate) filesystem_state: FilesystemTransactionStateSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ResolvedWritePlanSnapshot {
    pub(crate) partitions: Vec<ResolvedWritePartitionSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CommitPreconditionsSnapshot {
    pub(crate) write_lane: ExplainWriteLaneKind,
    pub(crate) expected_head: ExplainExpectedHead,
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
    pub(crate) write_lane: ExplainWriteLaneKind,
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
    pub(crate) kind: ExplainLoweredResultColumnsKind,
    pub(crate) static_columns: Vec<ExplainLoweredResultColumnType>,
    pub(crate) by_column_name: BTreeMap<String, ExplainLoweredResultColumnType>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LoweredReadProgramSnapshot {
    pub(crate) statements: Vec<LoweredReadStatementSnapshot>,
    pub(crate) pushdown_decision: PushdownExplainArtifacts,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StateHistoryRequestSnapshot {
    pub(crate) root_scope: ExplainHistoryRootScopeKind,
    pub(crate) requested_roots: Vec<String>,
    pub(crate) lineage_scope: ExplainHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) version_scope: ExplainHistoryVersionScopeKind,
    pub(crate) requested_versions: Vec<String>,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) plugin_keys: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) content_mode: ExplainStateHistoryContentMode,
    pub(crate) order: ExplainStateHistoryOrder,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FileHistoryRequestSnapshot {
    pub(crate) lineage_scope: ExplainHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) root_scope: ExplainHistoryRootScopeKind,
    pub(crate) requested_roots: Vec<String>,
    pub(crate) version_scope: ExplainHistoryVersionScopeKind,
    pub(crate) requested_versions: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) content_mode: ExplainFileHistoryContentMode,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectoryHistoryRequestSnapshot {
    pub(crate) lineage_scope: ExplainHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) root_scope: ExplainHistoryRootScopeKind,
    pub(crate) requested_roots: Vec<String>,
    pub(crate) version_scope: ExplainHistoryVersionScopeKind,
    pub(crate) requested_versions: Vec<String>,
    pub(crate) directory_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainDirectAggregate {
    Count,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainPredicateOperator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    In,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainAggregatePredicateOperator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainDirectStateHistoryField {
    EntityId,
    SchemaKey,
    FileId,
    PluginKey,
    SnapshotContent,
    Metadata,
    SchemaVersion,
    ChangeId,
    CommitId,
    CommitCreatedAt,
    RootCommitId,
    Depth,
    VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainDirectEntityHistoryField {
    Property(String),
    State(ExplainDirectStateHistoryField),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainDirectFileHistoryField {
    Id,
    Path,
    Data,
    Metadata,
    Hidden,
    EntityId,
    SchemaKey,
    FileId,
    VersionId,
    PluginKey,
    SchemaVersion,
    ChangeId,
    LixcolMetadata,
    CommitId,
    CommitCreatedAt,
    RootCommitId,
    Depth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainDirectDirectoryHistoryField {
    Id,
    ParentId,
    Name,
    Path,
    Hidden,
    EntityId,
    SchemaKey,
    FileId,
    VersionId,
    PluginKey,
    SchemaVersion,
    ChangeId,
    LixcolMetadata,
    CommitId,
    CommitCreatedAt,
    RootCommitId,
    Depth,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectPredicateSnapshot<Field> {
    pub(crate) operator: ExplainPredicateOperator,
    pub(crate) field: Field,
    pub(crate) value: Option<Value>,
    #[serde(default)]
    pub(crate) values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectFieldProjectionSnapshot<Field> {
    pub(crate) output_name: String,
    pub(crate) field: Field,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectSortKeySnapshot<Field> {
    pub(crate) output_name: String,
    pub(crate) field: Option<Field>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum StateHistoryProjectionValueSnapshot {
    Field(ExplainDirectStateHistoryField),
    Aggregate(ExplainDirectAggregate),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StateHistoryProjectionSnapshot {
    pub(crate) output_name: String,
    pub(crate) value: StateHistoryProjectionValueSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum StateHistorySortValueSnapshot {
    Field(ExplainDirectStateHistoryField),
    Aggregate(ExplainDirectAggregate),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StateHistorySortKeySnapshot {
    pub(crate) output_name: String,
    pub(crate) value: Option<StateHistorySortValueSnapshot>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StateHistoryAggregatePredicateSnapshot {
    pub(crate) operator: ExplainAggregatePredicateOperator,
    pub(crate) aggregate: ExplainDirectAggregate,
    pub(crate) value: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StateHistoryDirectPlanSnapshot {
    pub(crate) request: StateHistoryRequestSnapshot,
    pub(crate) predicates: Vec<DirectPredicateSnapshot<ExplainDirectStateHistoryField>>,
    pub(crate) projections: Vec<StateHistoryProjectionSnapshot>,
    pub(crate) sort_keys: Vec<StateHistorySortKeySnapshot>,
    pub(crate) group_by: Vec<ExplainDirectStateHistoryField>,
    pub(crate) having: Option<StateHistoryAggregatePredicateSnapshot>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct EntityHistoryDirectPlanSnapshot {
    pub(crate) request: StateHistoryRequestSnapshot,
    pub(crate) predicates: Vec<DirectPredicateSnapshot<ExplainDirectEntityHistoryField>>,
    pub(crate) projections: Vec<DirectFieldProjectionSnapshot<ExplainDirectEntityHistoryField>>,
    pub(crate) sort_keys: Vec<DirectSortKeySnapshot<ExplainDirectEntityHistoryField>>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
    pub(crate) surface_binding: SurfaceBindingSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FileHistoryDirectPlanSnapshot {
    pub(crate) request: FileHistoryRequestSnapshot,
    pub(crate) predicates: Vec<DirectPredicateSnapshot<ExplainDirectFileHistoryField>>,
    pub(crate) projections: Vec<DirectFieldProjectionSnapshot<ExplainDirectFileHistoryField>>,
    pub(crate) sort_keys: Vec<DirectSortKeySnapshot<ExplainDirectFileHistoryField>>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) aggregate: Option<ExplainDirectAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DirectoryHistoryDirectPlanSnapshot {
    pub(crate) request: DirectoryHistoryRequestSnapshot,
    pub(crate) predicates: Vec<DirectPredicateSnapshot<ExplainDirectDirectoryHistoryField>>,
    pub(crate) projections: Vec<DirectFieldProjectionSnapshot<ExplainDirectDirectoryHistoryField>>,
    pub(crate) sort_keys: Vec<DirectSortKeySnapshot<ExplainDirectDirectoryHistoryField>>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) aggregate: Option<ExplainDirectAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumnsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainDirectPublicReadPlan {
    StateHistory(Box<StateHistoryDirectPlanSnapshot>),
    EntityHistory(Box<EntityHistoryDirectPlanSnapshot>),
    FileHistory(Box<FileHistoryDirectPlanSnapshot>),
    DirectoryHistory(Box<DirectoryHistoryDirectPlanSnapshot>),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadStatementSnapshot {
    Query(Box<ExplainBroadPublicReadQuerySnapshot>),
    Explain {
        statement: Box<ExplainBroadPublicReadStatementSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadQuerySnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) with: Option<Box<ExplainBroadPublicReadWithSnapshot>>,
    pub(crate) body: Box<ExplainBroadPublicReadSetExprSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadWithSnapshot {
    pub(crate) cte_tables: Vec<ExplainBroadPublicReadQuerySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadSetExprSnapshot {
    Select(Box<ExplainBroadPublicReadSelectSnapshot>),
    Query(Box<ExplainBroadPublicReadQuerySnapshot>),
    SetOperation {
        operator: ExplainBroadSetOperationKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quantifier: Option<ExplainBroadSetQuantifier>,
        left: Box<ExplainBroadPublicReadSetExprSnapshot>,
        right: Box<ExplainBroadPublicReadSetExprSnapshot>,
    },
    Table {
        relation: ExplainBroadPublicReadRelationSnapshot,
    },
    Other {
        sql: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainBroadSetOperationKind {
    Union,
    Except,
    Intersect,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExplainBroadSetQuantifier {
    All,
    Distinct,
    ByName,
    AllByName,
    DistinctByName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadSelectSnapshot {
    pub(crate) from: Vec<ExplainBroadPublicReadTableWithJoinsSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadTableWithJoinsSnapshot {
    pub(crate) relation: ExplainBroadPublicReadTableFactorSnapshot,
    pub(crate) joins: Vec<ExplainBroadPublicReadJoinSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadJoinSnapshot {
    pub(crate) operator: String,
    pub(crate) relation: ExplainBroadPublicReadTableFactorSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadTableFactorSnapshot {
    Table {
        relation: ExplainBroadPublicReadRelationSnapshot,
    },
    Derived {
        subquery: Box<ExplainBroadPublicReadQuerySnapshot>,
    },
    NestedJoin {
        table_with_joins: Box<ExplainBroadPublicReadTableWithJoinsSnapshot>,
    },
    Other {
        sql: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadRelationSnapshot {
    Public(SurfaceBindingSnapshot),
    LoweredPublic(SurfaceBindingSnapshot),
    Internal { relation_name: String },
    External { relation_name: String },
    Cte { relation_name: String },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainPublicReadLogicalPlan {
    pub(crate) strategy: ExplainPublicReadStrategy,
    pub(crate) surface_bindings: Vec<SurfaceBindingSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) broad_statement: Option<Box<ExplainBroadPublicReadStatementSnapshot>>,
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
    pub(crate) result_contract: ExplainResultContract,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) broad_statement: Option<Box<ExplainBroadPublicReadStatementSnapshot>>,
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
    pub(crate) state_source: ExplainStateSourceKind,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlanSnapshot>,
    pub(crate) commit_preconditions: Vec<CommitPreconditionsSnapshot>,
    pub(crate) residual_execution_predicates: Vec<String>,
    pub(crate) backend_rejections: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub(crate) struct ExecutorExplainArtifacts {
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
            sections.push(("request".to_string(), render_request_text(request)));
        }
        if let Some(semantic_statement) = self.semantic_statement.as_ref() {
            sections.push((
                "semantic_statement".to_string(),
                render_semantic_statement_text(semantic_statement),
            ));
        }
        if let Some(logical_plan) = self.logical_plan.as_ref() {
            sections.push((
                "logical_plan".to_string(),
                render_logical_plan_text(logical_plan),
            ));
        }
        if let Some(optimized_logical_plan) = self.optimized_logical_plan.as_ref() {
            sections.push((
                "optimized_logical_plan".to_string(),
                render_logical_plan_text(optimized_logical_plan),
            ));
        }
        if let Some(physical_plan) = self.physical_plan.as_ref() {
            sections.push((
                "physical_plan".to_string(),
                render_physical_plan_text(physical_plan),
            ));
        }
        sections.push((
            "executor_artifacts".to_string(),
            render_executor_artifacts_text(&self.executor_artifacts),
        ));
        if !self.optimizer_passes.is_empty() {
            sections.push((
                "optimizer_passes".to_string(),
                render_optimizer_passes_text(&self.optimizer_passes),
            ));
        }
        if !self.stage_timings.is_empty() {
            sections.push((
                "stage_timings".to_string(),
                render_stage_timings_text(&self.stage_timings),
            ));
        }
        if let Some(analyzed_runtime) = self.analyzed_runtime.as_ref() {
            sections.push((
                "analyzed_runtime".to_string(),
                render_analyzed_runtime_text(analyzed_runtime),
            ));
        }

        Ok(sections)
    }
}

fn render_request_text(request: &ExplainRequest) -> String {
    format!(
        "mode: {}\nformat: {}",
        explain_mode_label(request.mode),
        explain_output_format_label(request.output_format())
    )
}

fn render_semantic_statement_text(statement: &ExplainSemanticStatement) -> String {
    match statement {
        ExplainSemanticStatement::PublicRead(details) => format!(
            "kind: public_read\nsurfaces: {}\nbroad_statement: {}\nstructured_read: {}\neffective_state_request: {}\neffective_state_plan: {}",
            join_public_names(&details.surface_bindings),
            yes_no(details.broad_statement.is_some()),
            yes_no(details.structured_read.is_some()),
            yes_no(details.effective_state_request.is_some()),
            yes_no(details.effective_state_plan.is_some()),
        ),
        ExplainSemanticStatement::PublicWrite(details) => format!(
            "kind: public_write\ntarget: {}\noperation: {}",
            details.surface_binding.public_name,
            explain_write_operation_kind_label(details.write_command.operation_kind),
        ),
        ExplainSemanticStatement::Internal(statements) => format!(
            "kind: internal\nprepared_statements: {}\nmutations: {}\nupdate_validations: {}",
            statements.prepared_statements.len(),
            statements.mutations.len(),
            statements.update_validations.len(),
        ),
    }
}

fn render_logical_plan_text(plan: &ExplainLogicalPlanSnapshot) -> String {
    match plan {
        ExplainLogicalPlanSnapshot::PublicRead(details) => format!(
            "kind: public_read\nstrategy: {}\nsurfaces: {}\nbroad_statement: {}\nstructured_read: {}\ndirect_plan: {}\ndependency_spec: {}\neffective_state_plan: {}",
            explain_public_read_strategy_label(details.strategy),
            join_public_names(&details.surface_bindings),
            yes_no(details.broad_statement.is_some()),
            yes_no(details.read.is_some()),
            yes_no(details.direct_plan.is_some()),
            yes_no(details.dependency_spec.is_some()),
            yes_no(details.effective_state_plan.is_some()),
        ),
        ExplainLogicalPlanSnapshot::PublicWrite(details) => format!(
            "kind: public_write\ntarget: {}\noperation: {}\nstate_source: {}",
            details.planned_write.command.target.public_name,
            explain_write_operation_kind_label(details.planned_write.command.operation_kind),
            explain_state_source_kind_label(details.planned_write.state_source),
        ),
        ExplainLogicalPlanSnapshot::Internal(details) => format!(
            "kind: internal\nresult_contract: {}\nprepared_statements: {}\nmutations: {}",
            explain_result_contract_label(details.result_contract),
            details.statements.prepared_statements.len(),
            details.statements.mutations.len(),
        ),
    }
}

fn render_physical_plan_text(plan: &ExplainPhysicalPlanSnapshot) -> String {
    match plan {
        ExplainPhysicalPlanSnapshot::PublicRead(execution) => match execution.as_ref() {
            ExplainPublicReadExecution::LoweredSql(program) => format!(
                "kind: public_read\nexecution: lowered_sql\nstatements: {}\nresult_columns: {}",
                program.statements.len(),
                explain_lowered_result_columns_kind_label(program.result_columns.kind),
            ),
            ExplainPublicReadExecution::Direct(plan) => format!(
                "kind: public_read\nexecution: direct\ndirect_plan: {}",
                explain_direct_public_read_plan_label(plan),
            ),
        },
        ExplainPhysicalPlanSnapshot::PublicWrite(execution) => match execution.as_ref() {
            ExplainPublicWriteExecution::Noop => "kind: public_write\nexecution: noop".to_string(),
            ExplainPublicWriteExecution::Materialize(materialization) => format!(
                "kind: public_write\nexecution: materialize\npartitions: {}",
                materialization.partitions.len(),
            ),
        },
    }
}

fn render_executor_artifacts_text(artifacts: &ExecutorExplainArtifacts) -> String {
    let mut lines = vec![
        format!(
            "bound_public_leaves: {}",
            artifacts.bound_public_leaves.len()
        ),
        format!(
            "dependency_spec: {}",
            yes_no(artifacts.dependency_spec.is_some())
        ),
        format!(
            "effective_state_request: {}",
            yes_no(artifacts.effective_state_request.is_some())
        ),
        format!(
            "effective_state_plan: {}",
            yes_no(artifacts.effective_state_plan.is_some())
        ),
        format!("pushdown: {}", yes_no(artifacts.pushdown.is_some())),
        format!("lowered_sql_statements: {}", artifacts.lowered_sql.len()),
        format!(
            "commit_preconditions: {}",
            artifacts.commit_preconditions.len()
        ),
        format!(
            "domain_change_batches: {}",
            artifacts.domain_change_batches.len()
        ),
        format!(
            "internal_live_table_requirements: {}",
            artifacts.internal_live_table_requirements.len()
        ),
        format!("internal_mutations: {}", artifacts.internal_mutations.len()),
        format!(
            "internal_update_validations: {}",
            artifacts.internal_update_validations.len()
        ),
    ];

    if let Some(first_sql) = artifacts.lowered_sql.first() {
        lines.push(format!("first_lowered_sql: {first_sql}"));
    }

    lines.join("\n")
}

fn render_optimizer_passes_text(passes: &[OptimizerPassTrace]) -> String {
    passes
        .iter()
        .map(|pass| {
            let status = if pass.enabled { "enabled" } else { "disabled" };
            let changed = if pass.changed { "changed" } else { "unchanged" };
            let duration = pass
                .duration_us
                .map(|duration| format!("{duration}us"))
                .unwrap_or_else(|| "not_run".to_string());
            if pass.diagnostics.is_empty() {
                format!(
                    "{}. {} [{status}, {changed}, {duration}]",
                    pass.order, pass.name
                )
            } else {
                format!(
                    "{}. {} [{status}, {changed}, {duration}] {}",
                    pass.order,
                    pass.name,
                    pass.diagnostics.join(" | ")
                )
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_stage_timings_text(stage_timings: &[ExplainStageTiming]) -> String {
    stage_timings
        .iter()
        .map(|timing| {
            format!(
                "{}: {}us",
                explain_stage_label(timing.stage),
                timing.duration_us
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_analyzed_runtime_text(runtime: &ExplainAnalyzedRuntime) -> String {
    format!(
        "execution_duration_us: {}\noutput_row_count: {}\noutput_column_count: {}\noutput_columns: {}",
        runtime.execution_duration_us,
        runtime.output_row_count,
        runtime.output_column_count,
        runtime.output_columns.join(", "),
    )
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
        internal_live_table_requirements: Vec::new(),
        internal_mutations: Vec::new(),
        internal_update_validations: Vec::new(),
    }
}

fn executor_artifacts_for_public_write(
    planned_write: &PlannedWrite,
    domain_change_batches: &[DomainChangeBatch],
    invariant_trace: Option<&PublicWriteInvariantTrace>,
) -> ExecutorExplainArtifacts {
    let target = &planned_write.command.target;

    ExecutorExplainArtifacts {
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
        internal_live_table_requirements: Vec::new(),
        internal_mutations: Vec::new(),
        internal_update_validations: Vec::new(),
    }
}

fn executor_artifacts_for_internal(logical_plan: &InternalLogicalPlan) -> ExecutorExplainArtifacts {
    let statements = &logical_plan.normalized_statements;

    ExecutorExplainArtifacts {
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
        broad_statement: semantics
            .broad_statement
            .as_deref()
            .map(broad_public_read_statement_snapshot)
            .map(Box::new),
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

fn broad_public_read_statement_snapshot(
    statement: &BroadPublicReadStatement,
) -> ExplainBroadPublicReadStatementSnapshot {
    match statement {
        BroadPublicReadStatement::Query(query) => ExplainBroadPublicReadStatementSnapshot::Query(
            Box::new(broad_public_read_query_snapshot(query)),
        ),
        BroadPublicReadStatement::Explain { statement, .. } => {
            ExplainBroadPublicReadStatementSnapshot::Explain {
                statement: Box::new(broad_public_read_statement_snapshot(statement)),
            }
        }
    }
}

fn broad_public_read_query_snapshot(
    query: &BroadPublicReadQuery,
) -> ExplainBroadPublicReadQuerySnapshot {
    ExplainBroadPublicReadQuerySnapshot {
        with: query
            .with
            .as_ref()
            .map(broad_public_read_with_snapshot)
            .map(Box::new),
        body: Box::new(broad_public_read_set_expr_snapshot(&query.body)),
    }
}

fn broad_public_read_with_snapshot(
    with: &crate::sql::logical_plan::public_ir::BroadPublicReadWith,
) -> ExplainBroadPublicReadWithSnapshot {
    ExplainBroadPublicReadWithSnapshot {
        cte_tables: with
            .cte_tables
            .iter()
            .map(broad_public_read_query_snapshot)
            .collect(),
    }
}

fn broad_public_read_set_expr_snapshot(
    expr: &BroadPublicReadSetExpr,
) -> ExplainBroadPublicReadSetExprSnapshot {
    match expr {
        BroadPublicReadSetExpr::Select(select) => ExplainBroadPublicReadSetExprSnapshot::Select(
            Box::new(broad_public_read_select_snapshot(select)),
        ),
        BroadPublicReadSetExpr::Query(query) => ExplainBroadPublicReadSetExprSnapshot::Query(
            Box::new(broad_public_read_query_snapshot(query)),
        ),
        BroadPublicReadSetExpr::SetOperation {
            original,
            left,
            right,
        } => {
            let (operator, quantifier) = match original {
                sqlparser::ast::SetExpr::SetOperation {
                    op, set_quantifier, ..
                } => (
                    explain_broad_set_operation_kind(*op),
                    explain_broad_set_quantifier(*set_quantifier),
                ),
                _ => unreachable!("broad set operation snapshot expects a set operation"),
            };
            ExplainBroadPublicReadSetExprSnapshot::SetOperation {
                operator,
                quantifier,
                left: Box::new(broad_public_read_set_expr_snapshot(left)),
                right: Box::new(broad_public_read_set_expr_snapshot(right)),
            }
        }
        BroadPublicReadSetExpr::Table { relation, .. } => {
            ExplainBroadPublicReadSetExprSnapshot::Table {
                relation: broad_public_read_relation_snapshot(relation),
            }
        }
        BroadPublicReadSetExpr::Other(expr) => ExplainBroadPublicReadSetExprSnapshot::Other {
            sql: expr.to_string(),
        },
    }
}

fn broad_public_read_select_snapshot(
    select: &BroadPublicReadSelect,
) -> ExplainBroadPublicReadSelectSnapshot {
    ExplainBroadPublicReadSelectSnapshot {
        from: select
            .from
            .iter()
            .map(broad_public_read_table_with_joins_snapshot)
            .collect(),
    }
}

fn broad_public_read_table_with_joins_snapshot(
    table: &BroadPublicReadTableWithJoins,
) -> ExplainBroadPublicReadTableWithJoinsSnapshot {
    ExplainBroadPublicReadTableWithJoinsSnapshot {
        relation: broad_public_read_table_factor_snapshot(&table.relation),
        joins: table
            .joins
            .iter()
            .map(broad_public_read_join_snapshot)
            .collect(),
    }
}

fn broad_public_read_join_snapshot(
    join: &BroadPublicReadJoin,
) -> ExplainBroadPublicReadJoinSnapshot {
    ExplainBroadPublicReadJoinSnapshot {
        operator: broad_public_read_join_operator_label(&join.original.join_operator).to_string(),
        relation: broad_public_read_table_factor_snapshot(&join.relation),
    }
}

fn broad_public_read_table_factor_snapshot(
    relation: &BroadPublicReadTableFactor,
) -> ExplainBroadPublicReadTableFactorSnapshot {
    match relation {
        BroadPublicReadTableFactor::Table { relation, .. } => {
            ExplainBroadPublicReadTableFactorSnapshot::Table {
                relation: broad_public_read_relation_snapshot(relation),
            }
        }
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            ExplainBroadPublicReadTableFactorSnapshot::Derived {
                subquery: Box::new(broad_public_read_query_snapshot(subquery)),
            }
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => ExplainBroadPublicReadTableFactorSnapshot::NestedJoin {
            table_with_joins: Box::new(broad_public_read_table_with_joins_snapshot(
                table_with_joins,
            )),
        },
        BroadPublicReadTableFactor::Other(relation) => {
            ExplainBroadPublicReadTableFactorSnapshot::Other {
                sql: relation.to_string(),
            }
        }
    }
}

fn broad_public_read_relation_snapshot(
    relation: &BroadPublicReadRelation,
) -> ExplainBroadPublicReadRelationSnapshot {
    match relation {
        BroadPublicReadRelation::Public(binding) => {
            ExplainBroadPublicReadRelationSnapshot::Public(surface_binding_snapshot(binding))
        }
        BroadPublicReadRelation::LoweredPublic(binding) => {
            ExplainBroadPublicReadRelationSnapshot::LoweredPublic(surface_binding_snapshot(binding))
        }
        BroadPublicReadRelation::Internal(name) => {
            ExplainBroadPublicReadRelationSnapshot::Internal {
                relation_name: name.clone(),
            }
        }
        BroadPublicReadRelation::External(name) => {
            ExplainBroadPublicReadRelationSnapshot::External {
                relation_name: name.clone(),
            }
        }
        BroadPublicReadRelation::Cte(name) => ExplainBroadPublicReadRelationSnapshot::Cte {
            relation_name: name.clone(),
        },
    }
}

fn explain_broad_set_operation_kind(operator: SetOperator) -> ExplainBroadSetOperationKind {
    match operator {
        SetOperator::Union => ExplainBroadSetOperationKind::Union,
        SetOperator::Except => ExplainBroadSetOperationKind::Except,
        SetOperator::Intersect => ExplainBroadSetOperationKind::Intersect,
        SetOperator::Minus => ExplainBroadSetOperationKind::Minus,
    }
}

fn explain_broad_set_quantifier(quantifier: SetQuantifier) -> Option<ExplainBroadSetQuantifier> {
    match quantifier {
        SetQuantifier::All => Some(ExplainBroadSetQuantifier::All),
        SetQuantifier::Distinct => Some(ExplainBroadSetQuantifier::Distinct),
        SetQuantifier::ByName => Some(ExplainBroadSetQuantifier::ByName),
        SetQuantifier::AllByName => Some(ExplainBroadSetQuantifier::AllByName),
        SetQuantifier::DistinctByName => Some(ExplainBroadSetQuantifier::DistinctByName),
        SetQuantifier::None => None,
    }
}

fn broad_public_read_join_operator_label(operator: &sqlparser::ast::JoinOperator) -> &'static str {
    match operator {
        sqlparser::ast::JoinOperator::Join(_) => "join",
        sqlparser::ast::JoinOperator::Inner(_) => "inner",
        sqlparser::ast::JoinOperator::Left(_) => "left",
        sqlparser::ast::JoinOperator::LeftOuter(_) => "left_outer",
        sqlparser::ast::JoinOperator::Right(_) => "right",
        sqlparser::ast::JoinOperator::RightOuter(_) => "right_outer",
        sqlparser::ast::JoinOperator::FullOuter(_) => "full_outer",
        sqlparser::ast::JoinOperator::CrossJoin(_) => "cross_join",
        sqlparser::ast::JoinOperator::Semi(_) => "semi",
        sqlparser::ast::JoinOperator::LeftSemi(_) => "left_semi",
        sqlparser::ast::JoinOperator::RightSemi(_) => "right_semi",
        sqlparser::ast::JoinOperator::Anti(_) => "anti",
        sqlparser::ast::JoinOperator::LeftAnti(_) => "left_anti",
        sqlparser::ast::JoinOperator::RightAnti(_) => "right_anti",
        sqlparser::ast::JoinOperator::CrossApply => "cross_apply",
        sqlparser::ast::JoinOperator::OuterApply => "outer_apply",
        sqlparser::ast::JoinOperator::AsOf { .. } => "as_of",
        sqlparser::ast::JoinOperator::StraightJoin(_) => "straight_join",
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
                    strategy: ExplainPublicReadStrategy::Structured,
                    surface_bindings: vec![surface_binding_snapshot(&read.surface_binding)],
                    broad_statement: None,
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
                    strategy: ExplainPublicReadStrategy::DirectHistory,
                    surface_bindings: vec![surface_binding_snapshot(&read.surface_binding)],
                    broad_statement: None,
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
                    broad_statement,
                    surface_bindings,
                    dependency_spec,
                } => ExplainPublicReadLogicalPlan {
                    strategy: ExplainPublicReadStrategy::Broad,
                    surface_bindings: surface_bindings
                        .iter()
                        .map(surface_binding_snapshot)
                        .collect(),
                    broad_statement: Some(Box::new(broad_public_read_statement_snapshot(
                        broad_statement,
                    ))),
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
        result_contract: result_contract_snapshot(plan.result_contract),
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
        contract: read_contract_snapshot(command.contract),
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
        version_scope: version_scope_snapshot(scan.version_scope),
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
        kind: filesystem_kind_snapshot(scan.kind),
        version_scope: version_scope_snapshot(scan.version_scope),
    }
}

fn canonical_admin_scan_snapshot(scan: &CanonicalAdminScan) -> CanonicalAdminScanSnapshot {
    CanonicalAdminScanSnapshot {
        binding: surface_binding_snapshot(&scan.binding),
        kind: canonical_admin_kind_snapshot(scan.kind),
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
        state_source: state_source_kind_snapshot(plan.state_source),
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
        operation_kind: write_operation_kind_snapshot(command.operation_kind),
        target: surface_binding_snapshot(&command.target),
        selector: write_selector_snapshot(&command.selector),
        payload: mutation_payload_snapshot(&command.payload),
        on_conflict: command
            .on_conflict
            .as_ref()
            .map(insert_on_conflict_snapshot),
        requested_mode: write_mode_request_snapshot(command.requested_mode),
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
        action: insert_on_conflict_action_snapshot(conflict.action),
    }
}

fn execution_context_snapshot(
    context: &crate::sql::semantic_ir::ExecutionContext,
) -> ExecutionContextSnapshot {
    ExecutionContextSnapshot {
        dialect: context.dialect.map(sql_dialect_snapshot),
        writer_key: context.writer_key.clone(),
        requested_version_id: context.requested_version_id.clone(),
        active_account_ids: context.active_account_ids.clone(),
    }
}

fn scope_proof_snapshot(proof: &ScopeProof) -> ScopeProofSnapshot {
    match proof {
        ScopeProof::ActiveVersion => ScopeProofSnapshot {
            kind: ExplainScopeProofKind::ActiveVersion,
            versions: Vec::new(),
            version: None,
        },
        ScopeProof::SingleVersion(version) => ScopeProofSnapshot {
            kind: ExplainScopeProofKind::SingleVersion,
            versions: Vec::new(),
            version: Some(version.clone()),
        },
        ScopeProof::GlobalAdmin => ScopeProofSnapshot {
            kind: ExplainScopeProofKind::GlobalAdmin,
            versions: Vec::new(),
            version: None,
        },
        ScopeProof::FiniteVersionSet(versions) => ScopeProofSnapshot {
            kind: ExplainScopeProofKind::FiniteVersionSet,
            versions: versions.iter().cloned().collect(),
            version: None,
        },
        ScopeProof::Unbounded => ScopeProofSnapshot {
            kind: ExplainScopeProofKind::Unbounded,
            versions: Vec::new(),
            version: None,
        },
        ScopeProof::Unknown => ScopeProofSnapshot {
            kind: ExplainScopeProofKind::Unknown,
            versions: Vec::new(),
            version: None,
        },
    }
}

fn schema_proof_snapshot(proof: &SchemaProof) -> SchemaProofSnapshot {
    match proof {
        SchemaProof::Exact(schema_keys) => SchemaProofSnapshot {
            kind: ExplainSchemaProofKind::Exact,
            schema_keys: schema_keys.iter().cloned().collect(),
        },
        SchemaProof::Unknown => SchemaProofSnapshot {
            kind: ExplainSchemaProofKind::Unknown,
            schema_keys: Vec::new(),
        },
    }
}

fn target_set_proof_snapshot(proof: &TargetSetProof) -> TargetSetProofSnapshot {
    match proof {
        TargetSetProof::Exact(entity_ids) => TargetSetProofSnapshot {
            kind: ExplainTargetSetProofKind::Exact,
            entity_ids: entity_ids.iter().cloned().collect(),
        },
        TargetSetProof::Unknown => TargetSetProofSnapshot {
            kind: ExplainTargetSetProofKind::Unknown,
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
        execution_mode: write_mode_snapshot(partition.execution_mode),
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
            .map(write_lane_kind_snapshot),
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
        metadata_patch: optional_text_patch_snapshot(&state.metadata_patch),
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
        write_lane: write_lane_kind_snapshot(&preconditions.write_lane),
        expected_head: expected_head_snapshot(&preconditions.expected_head),
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
        write_lane: write_lane_kind_snapshot(&batch.write_lane),
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
            kind: ExplainLoweredResultColumnsKind::Static,
            static_columns: columns.iter().map(lowered_result_column_name).collect(),
            by_column_name: BTreeMap::new(),
        },
        LoweredResultColumns::ByColumnName(columns) => LoweredResultColumnsSnapshot {
            kind: ExplainLoweredResultColumnsKind::ByColumnName,
            static_columns: Vec::new(),
            by_column_name: columns
                .iter()
                .map(|(name, column)| (name.clone(), lowered_result_column_name(column)))
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
) -> StateHistoryDirectPlanSnapshot {
    StateHistoryDirectPlanSnapshot {
        request: state_history_request_snapshot(&plan.request),
        predicates: plan
            .predicates
            .iter()
            .map(state_history_predicate_snapshot)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(state_history_projection_snapshot)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(state_history_sort_key_snapshot)
            .collect(),
        group_by: plan
            .group_by_fields
            .iter()
            .map(direct_state_history_field_snapshot)
            .collect(),
        having: plan
            .having
            .as_ref()
            .map(state_history_aggregate_predicate_snapshot),
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
    }
}

fn entity_history_direct_plan_snapshot(
    plan: &EntityHistoryDirectReadPlan,
) -> EntityHistoryDirectPlanSnapshot {
    EntityHistoryDirectPlanSnapshot {
        request: state_history_request_snapshot(&plan.request),
        predicates: plan
            .predicates
            .iter()
            .map(entity_history_predicate_snapshot)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(entity_history_projection_snapshot)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(entity_history_sort_key_snapshot)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
        surface_binding: surface_binding_snapshot(&plan.surface_binding),
    }
}

fn file_history_direct_plan_snapshot(
    plan: &FileHistoryDirectReadPlan,
) -> FileHistoryDirectPlanSnapshot {
    FileHistoryDirectPlanSnapshot {
        request: file_history_request_snapshot(&plan.request),
        predicates: plan
            .predicates
            .iter()
            .map(file_history_predicate_snapshot)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(file_history_projection_snapshot)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(file_history_sort_key_snapshot)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        aggregate: plan.aggregate.as_ref().map(file_history_aggregate_snapshot),
        aggregate_output_name: plan.aggregate_output_name.clone(),
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
    }
}

fn directory_history_direct_plan_snapshot(
    plan: &DirectoryHistoryDirectReadPlan,
) -> DirectoryHistoryDirectPlanSnapshot {
    DirectoryHistoryDirectPlanSnapshot {
        request: directory_history_request_snapshot(&plan.request),
        predicates: plan
            .predicates
            .iter()
            .map(directory_history_predicate_snapshot)
            .collect(),
        projections: plan
            .projections
            .iter()
            .map(directory_history_projection_snapshot)
            .collect(),
        sort_keys: plan
            .sort_keys
            .iter()
            .map(directory_history_sort_key_snapshot)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        aggregate: plan
            .aggregate
            .as_ref()
            .map(directory_history_aggregate_snapshot),
        aggregate_output_name: plan.aggregate_output_name.clone(),
        result_columns: lowered_result_columns_snapshot(&plan.result_columns),
    }
}

fn state_history_request_snapshot(
    request: &crate::read::history::StateHistoryRequest,
) -> StateHistoryRequestSnapshot {
    StateHistoryRequestSnapshot {
        root_scope: state_history_root_scope_snapshot(&request.root_scope),
        requested_roots: state_history_requested_roots(&request.root_scope),
        lineage_scope: state_history_lineage_scope_snapshot(request.lineage_scope),
        active_version_id: request.active_version_id.clone(),
        version_scope: state_history_version_scope_snapshot(&request.version_scope),
        requested_versions: state_history_requested_versions(&request.version_scope),
        entity_ids: request.entity_ids.clone(),
        file_ids: request.file_ids.clone(),
        schema_keys: request.schema_keys.clone(),
        plugin_keys: request.plugin_keys.clone(),
        min_depth: request.min_depth,
        max_depth: request.max_depth,
        content_mode: state_history_content_mode_snapshot(request.content_mode),
        order: state_history_order_snapshot(request.order),
    }
}

fn file_history_request_snapshot(
    request: &crate::read::models::FileHistoryRequest,
) -> FileHistoryRequestSnapshot {
    FileHistoryRequestSnapshot {
        lineage_scope: file_history_lineage_scope_snapshot(request.lineage_scope),
        active_version_id: request.active_version_id.clone(),
        root_scope: file_history_root_scope_snapshot(&request.root_scope),
        requested_roots: file_history_requested_roots(&request.root_scope),
        version_scope: file_history_version_scope_snapshot(&request.version_scope),
        requested_versions: file_history_requested_versions(&request.version_scope),
        file_ids: request.file_ids.clone(),
        content_mode: file_history_content_mode_snapshot(request.content_mode),
    }
}

fn directory_history_request_snapshot(
    request: &crate::read::models::DirectoryHistoryRequest,
) -> DirectoryHistoryRequestSnapshot {
    DirectoryHistoryRequestSnapshot {
        lineage_scope: file_history_lineage_scope_snapshot(request.lineage_scope),
        active_version_id: request.active_version_id.clone(),
        root_scope: file_history_root_scope_snapshot(&request.root_scope),
        requested_roots: file_history_requested_roots(&request.root_scope),
        version_scope: file_history_version_scope_snapshot(&request.version_scope),
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
        read_freshness: surface_read_freshness_snapshot(binding.read_freshness),
        default_scope: default_scope_snapshot(binding.default_scope),
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
        precision: dependency_precision_snapshot(spec.precision),
    }
}

fn effective_state_request_snapshot(
    request: &EffectiveStateRequest,
) -> EffectiveStateRequestSnapshot {
    EffectiveStateRequestSnapshot {
        schema_set: request.schema_set.iter().cloned().collect(),
        version_scope: version_scope_snapshot(request.version_scope),
        include_global_overlay: request.include_global_overlay,
        include_untracked_overlay: request.include_untracked_overlay,
        include_tombstones: request.include_tombstones,
        predicate_classes: request.predicate_classes.clone(),
        required_columns: request.required_columns.clone(),
    }
}

fn effective_state_plan_snapshot(plan: &EffectiveStatePlan) -> EffectiveStatePlanSnapshot {
    EffectiveStatePlanSnapshot {
        state_source: state_source_authority_snapshot(plan.state_source),
        overlay_lanes: plan
            .overlay_lanes
            .iter()
            .map(overlay_lane_snapshot)
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
        operation: mutation_operation_snapshot(&row.operation),
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

fn sql_dialect_snapshot(dialect: SqlDialect) -> ExplainSqlDialect {
    match dialect {
        SqlDialect::Sqlite => ExplainSqlDialect::Sqlite,
        SqlDialect::Postgres => ExplainSqlDialect::Postgres,
    }
}

fn surface_read_freshness_snapshot(freshness: SurfaceReadFreshness) -> ExplainSurfaceReadFreshness {
    match freshness {
        SurfaceReadFreshness::RequiresFreshProjection => {
            ExplainSurfaceReadFreshness::RequiresFreshProjection
        }
        SurfaceReadFreshness::AllowsStaleProjection => {
            ExplainSurfaceReadFreshness::AllowsStaleProjection
        }
    }
}

fn default_scope_snapshot(
    scope: crate::sql::catalog::DefaultScopeSemantics,
) -> ExplainDefaultScope {
    match scope {
        crate::sql::catalog::DefaultScopeSemantics::ActiveVersion => {
            ExplainDefaultScope::ActiveVersion
        }
        crate::sql::catalog::DefaultScopeSemantics::ExplicitVersion => {
            ExplainDefaultScope::ExplicitVersion
        }
        crate::sql::catalog::DefaultScopeSemantics::History => ExplainDefaultScope::History,
        crate::sql::catalog::DefaultScopeSemantics::GlobalAdmin => ExplainDefaultScope::GlobalAdmin,
        crate::sql::catalog::DefaultScopeSemantics::WorkingChanges => {
            ExplainDefaultScope::WorkingChanges
        }
    }
}

fn dependency_precision_snapshot(precision: DependencyPrecision) -> ExplainDependencyPrecision {
    match precision {
        DependencyPrecision::Precise => ExplainDependencyPrecision::Precise,
        DependencyPrecision::Conservative => ExplainDependencyPrecision::Conservative,
    }
}

fn version_scope_snapshot(scope: VersionScope) -> ExplainVersionScope {
    match scope {
        VersionScope::ActiveVersion => ExplainVersionScope::ActiveVersion,
        VersionScope::ExplicitVersion => ExplainVersionScope::ExplicitVersion,
        VersionScope::History => ExplainVersionScope::History,
    }
}

fn filesystem_kind_snapshot(kind: FilesystemKind) -> ExplainFilesystemKind {
    match kind {
        FilesystemKind::File => ExplainFilesystemKind::File,
        FilesystemKind::Directory => ExplainFilesystemKind::Directory,
    }
}

fn canonical_admin_kind_snapshot(kind: CanonicalAdminKind) -> ExplainCanonicalAdminKind {
    match kind {
        CanonicalAdminKind::Version => ExplainCanonicalAdminKind::Version,
    }
}

fn read_contract_snapshot(contract: ReadContract) -> ExplainReadContract {
    match contract {
        ReadContract::CommittedAtStart => ExplainReadContract::CommittedAtStart,
    }
}

fn write_operation_kind_snapshot(kind: WriteOperationKind) -> ExplainWriteOperationKind {
    match kind {
        WriteOperationKind::Insert => ExplainWriteOperationKind::Insert,
        WriteOperationKind::Update => ExplainWriteOperationKind::Update,
        WriteOperationKind::Delete => ExplainWriteOperationKind::Delete,
    }
}

fn mutation_operation_snapshot(operation: &MutationOperation) -> ExplainMutationOperation {
    match operation {
        MutationOperation::Insert => ExplainMutationOperation::Insert,
    }
}

fn insert_on_conflict_action_snapshot(
    action: InsertOnConflictAction,
) -> ExplainInsertOnConflictAction {
    match action {
        InsertOnConflictAction::DoUpdate => ExplainInsertOnConflictAction::DoUpdate,
        InsertOnConflictAction::DoNothing => ExplainInsertOnConflictAction::DoNothing,
    }
}

fn write_mode_request_snapshot(mode: WriteModeRequest) -> ExplainWriteModeRequest {
    match mode {
        WriteModeRequest::Auto => ExplainWriteModeRequest::Auto,
        WriteModeRequest::ForceTracked => ExplainWriteModeRequest::ForceTracked,
        WriteModeRequest::ForceUntracked => ExplainWriteModeRequest::ForceUntracked,
    }
}

fn write_mode_snapshot(mode: WriteMode) -> ExplainWriteMode {
    match mode {
        WriteMode::Tracked => ExplainWriteMode::Tracked,
        WriteMode::Untracked => ExplainWriteMode::Untracked,
    }
}

fn state_source_authority_snapshot(kind: StateSourceAuthority) -> ExplainStateSourceKind {
    match kind {
        StateSourceAuthority::AuthoritativeCommitted => {
            ExplainStateSourceKind::AuthoritativeCommitted
        }
    }
}

fn write_lane_kind_snapshot(lane: &WriteLane) -> ExplainWriteLaneKind {
    match lane {
        WriteLane::ActiveVersion => ExplainWriteLaneKind::ActiveVersion,
        WriteLane::SingleVersion(_) => ExplainWriteLaneKind::SingleVersion,
        WriteLane::GlobalAdmin => ExplainWriteLaneKind::GlobalAdmin,
    }
}

fn expected_head_snapshot(expected: &ExpectedHead) -> ExplainExpectedHead {
    match expected {
        ExpectedHead::CurrentHead => ExplainExpectedHead::CurrentHead,
    }
}

fn overlay_lane_snapshot(lane: &OverlayLane) -> ExplainOverlayLane {
    match lane {
        OverlayLane::GlobalTracked => ExplainOverlayLane::GlobalTracked,
        OverlayLane::LocalTracked => ExplainOverlayLane::LocalTracked,
        OverlayLane::GlobalUntracked => ExplainOverlayLane::GlobalUntracked,
        OverlayLane::LocalUntracked => ExplainOverlayLane::LocalUntracked,
    }
}

fn optional_text_patch_snapshot(patch: &OptionalTextPatch) -> ExplainOptionalTextPatch {
    match patch {
        OptionalTextPatch::Unchanged => ExplainOptionalTextPatch::Unchanged,
    }
}

fn lowered_result_column_name(column: &LoweredResultColumn) -> ExplainLoweredResultColumnType {
    match column {
        LoweredResultColumn::Untyped => ExplainLoweredResultColumnType::Untyped,
        LoweredResultColumn::Boolean => ExplainLoweredResultColumnType::Boolean,
    }
}

fn result_contract_snapshot(contract: ResultContract) -> ExplainResultContract {
    match contract {
        ResultContract::Select => ExplainResultContract::Select,
        ResultContract::DmlNoReturning => ExplainResultContract::DmlNoReturning,
        ResultContract::DmlReturning => ExplainResultContract::DmlReturning,
        ResultContract::Other => ExplainResultContract::Other,
    }
}

fn state_history_root_scope_snapshot(
    scope: &crate::read::history::StateHistoryRootScope,
) -> ExplainHistoryRootScopeKind {
    match scope {
        crate::read::history::StateHistoryRootScope::AllRoots => {
            ExplainHistoryRootScopeKind::AllRoots
        }
        crate::read::history::StateHistoryRootScope::RequestedRoots(_) => {
            ExplainHistoryRootScopeKind::RequestedRoots
        }
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

fn state_history_lineage_scope_snapshot(
    scope: crate::read::history::StateHistoryLineageScope,
) -> ExplainHistoryLineageScope {
    match scope {
        crate::read::history::StateHistoryLineageScope::Standard => {
            ExplainHistoryLineageScope::Standard
        }
        crate::read::history::StateHistoryLineageScope::ActiveVersion => {
            ExplainHistoryLineageScope::ActiveVersion
        }
    }
}

fn state_history_version_scope_snapshot(
    scope: &crate::read::history::StateHistoryVersionScope,
) -> ExplainHistoryVersionScopeKind {
    match scope {
        crate::read::history::StateHistoryVersionScope::Any => ExplainHistoryVersionScopeKind::Any,
        crate::read::history::StateHistoryVersionScope::RequestedVersions(_) => {
            ExplainHistoryVersionScopeKind::RequestedVersions
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

fn state_history_content_mode_snapshot(
    mode: crate::read::history::StateHistoryContentMode,
) -> ExplainStateHistoryContentMode {
    match mode {
        crate::read::history::StateHistoryContentMode::MetadataOnly => {
            ExplainStateHistoryContentMode::MetadataOnly
        }
        crate::read::history::StateHistoryContentMode::IncludeSnapshotContent => {
            ExplainStateHistoryContentMode::IncludeSnapshotContent
        }
    }
}

fn state_history_order_snapshot(
    order: crate::read::history::StateHistoryOrder,
) -> ExplainStateHistoryOrder {
    match order {
        crate::read::history::StateHistoryOrder::EntityFileSchemaDepthAsc => {
            ExplainStateHistoryOrder::EntityFileSchemaDepthAsc
        }
    }
}

fn file_history_root_scope_snapshot(
    scope: &crate::read::models::FileHistoryRootScope,
) -> ExplainHistoryRootScopeKind {
    match scope {
        crate::read::models::FileHistoryRootScope::AllRoots => {
            ExplainHistoryRootScopeKind::AllRoots
        }
        crate::read::models::FileHistoryRootScope::RequestedRoots(_) => {
            ExplainHistoryRootScopeKind::RequestedRoots
        }
    }
}

fn file_history_requested_roots(scope: &crate::read::models::FileHistoryRootScope) -> Vec<String> {
    match scope {
        crate::read::models::FileHistoryRootScope::AllRoots => Vec::new(),
        crate::read::models::FileHistoryRootScope::RequestedRoots(roots) => roots.clone(),
    }
}

fn file_history_version_scope_snapshot(
    scope: &crate::read::models::FileHistoryVersionScope,
) -> ExplainHistoryVersionScopeKind {
    match scope {
        crate::read::models::FileHistoryVersionScope::Any => ExplainHistoryVersionScopeKind::Any,
        crate::read::models::FileHistoryVersionScope::RequestedVersions(_) => {
            ExplainHistoryVersionScopeKind::RequestedVersions
        }
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

fn file_history_lineage_scope_snapshot(
    scope: crate::read::models::FileHistoryLineageScope,
) -> ExplainHistoryLineageScope {
    match scope {
        crate::read::models::FileHistoryLineageScope::ActiveVersion => {
            ExplainHistoryLineageScope::ActiveVersion
        }
        crate::read::models::FileHistoryLineageScope::Standard => {
            ExplainHistoryLineageScope::Standard
        }
    }
}

fn file_history_content_mode_snapshot(
    mode: crate::read::models::FileHistoryContentMode,
) -> ExplainFileHistoryContentMode {
    match mode {
        crate::read::models::FileHistoryContentMode::MetadataOnly => {
            ExplainFileHistoryContentMode::MetadataOnly
        }
        crate::read::models::FileHistoryContentMode::IncludeData => {
            ExplainFileHistoryContentMode::IncludeData
        }
    }
}

fn state_source_kind_snapshot(kind: StateSourceKind) -> ExplainStateSourceKind {
    match kind {
        StateSourceKind::AuthoritativeCommitted => ExplainStateSourceKind::AuthoritativeCommitted,
        StateSourceKind::UntrackedOverlay => ExplainStateSourceKind::UntrackedOverlay,
    }
}

fn direct_state_history_field_snapshot(
    field: &DirectStateHistoryField,
) -> ExplainDirectStateHistoryField {
    match field {
        DirectStateHistoryField::EntityId => ExplainDirectStateHistoryField::EntityId,
        DirectStateHistoryField::SchemaKey => ExplainDirectStateHistoryField::SchemaKey,
        DirectStateHistoryField::FileId => ExplainDirectStateHistoryField::FileId,
        DirectStateHistoryField::PluginKey => ExplainDirectStateHistoryField::PluginKey,
        DirectStateHistoryField::SnapshotContent => ExplainDirectStateHistoryField::SnapshotContent,
        DirectStateHistoryField::Metadata => ExplainDirectStateHistoryField::Metadata,
        DirectStateHistoryField::SchemaVersion => ExplainDirectStateHistoryField::SchemaVersion,
        DirectStateHistoryField::ChangeId => ExplainDirectStateHistoryField::ChangeId,
        DirectStateHistoryField::CommitId => ExplainDirectStateHistoryField::CommitId,
        DirectStateHistoryField::CommitCreatedAt => ExplainDirectStateHistoryField::CommitCreatedAt,
        DirectStateHistoryField::RootCommitId => ExplainDirectStateHistoryField::RootCommitId,
        DirectStateHistoryField::Depth => ExplainDirectStateHistoryField::Depth,
        DirectStateHistoryField::VersionId => ExplainDirectStateHistoryField::VersionId,
    }
}

fn direct_entity_history_field_snapshot(
    field: &DirectEntityHistoryField,
) -> ExplainDirectEntityHistoryField {
    match field {
        DirectEntityHistoryField::Property(name) => {
            ExplainDirectEntityHistoryField::Property(name.clone())
        }
        DirectEntityHistoryField::State(field) => {
            ExplainDirectEntityHistoryField::State(direct_state_history_field_snapshot(field))
        }
    }
}

fn direct_file_history_field_snapshot(
    field: &DirectFileHistoryField,
) -> ExplainDirectFileHistoryField {
    match field {
        DirectFileHistoryField::Id => ExplainDirectFileHistoryField::Id,
        DirectFileHistoryField::Path => ExplainDirectFileHistoryField::Path,
        DirectFileHistoryField::Data => ExplainDirectFileHistoryField::Data,
        DirectFileHistoryField::Metadata => ExplainDirectFileHistoryField::Metadata,
        DirectFileHistoryField::Hidden => ExplainDirectFileHistoryField::Hidden,
        DirectFileHistoryField::EntityId => ExplainDirectFileHistoryField::EntityId,
        DirectFileHistoryField::SchemaKey => ExplainDirectFileHistoryField::SchemaKey,
        DirectFileHistoryField::FileId => ExplainDirectFileHistoryField::FileId,
        DirectFileHistoryField::VersionId => ExplainDirectFileHistoryField::VersionId,
        DirectFileHistoryField::PluginKey => ExplainDirectFileHistoryField::PluginKey,
        DirectFileHistoryField::SchemaVersion => ExplainDirectFileHistoryField::SchemaVersion,
        DirectFileHistoryField::ChangeId => ExplainDirectFileHistoryField::ChangeId,
        DirectFileHistoryField::LixcolMetadata => ExplainDirectFileHistoryField::LixcolMetadata,
        DirectFileHistoryField::CommitId => ExplainDirectFileHistoryField::CommitId,
        DirectFileHistoryField::CommitCreatedAt => ExplainDirectFileHistoryField::CommitCreatedAt,
        DirectFileHistoryField::RootCommitId => ExplainDirectFileHistoryField::RootCommitId,
        DirectFileHistoryField::Depth => ExplainDirectFileHistoryField::Depth,
    }
}

fn direct_directory_history_field_snapshot(
    field: &DirectDirectoryHistoryField,
) -> ExplainDirectDirectoryHistoryField {
    match field {
        DirectDirectoryHistoryField::Id => ExplainDirectDirectoryHistoryField::Id,
        DirectDirectoryHistoryField::ParentId => ExplainDirectDirectoryHistoryField::ParentId,
        DirectDirectoryHistoryField::Name => ExplainDirectDirectoryHistoryField::Name,
        DirectDirectoryHistoryField::Path => ExplainDirectDirectoryHistoryField::Path,
        DirectDirectoryHistoryField::Hidden => ExplainDirectDirectoryHistoryField::Hidden,
        DirectDirectoryHistoryField::EntityId => ExplainDirectDirectoryHistoryField::EntityId,
        DirectDirectoryHistoryField::SchemaKey => ExplainDirectDirectoryHistoryField::SchemaKey,
        DirectDirectoryHistoryField::FileId => ExplainDirectDirectoryHistoryField::FileId,
        DirectDirectoryHistoryField::VersionId => ExplainDirectDirectoryHistoryField::VersionId,
        DirectDirectoryHistoryField::PluginKey => ExplainDirectDirectoryHistoryField::PluginKey,
        DirectDirectoryHistoryField::SchemaVersion => {
            ExplainDirectDirectoryHistoryField::SchemaVersion
        }
        DirectDirectoryHistoryField::ChangeId => ExplainDirectDirectoryHistoryField::ChangeId,
        DirectDirectoryHistoryField::LixcolMetadata => {
            ExplainDirectDirectoryHistoryField::LixcolMetadata
        }
        DirectDirectoryHistoryField::CommitId => ExplainDirectDirectoryHistoryField::CommitId,
        DirectDirectoryHistoryField::CommitCreatedAt => {
            ExplainDirectDirectoryHistoryField::CommitCreatedAt
        }
        DirectDirectoryHistoryField::RootCommitId => {
            ExplainDirectDirectoryHistoryField::RootCommitId
        }
        DirectDirectoryHistoryField::Depth => ExplainDirectDirectoryHistoryField::Depth,
    }
}

fn direct_aggregate_snapshot(_: &StateHistoryAggregate) -> ExplainDirectAggregate {
    ExplainDirectAggregate::Count
}

fn file_history_aggregate_snapshot(_: &FileHistoryAggregate) -> ExplainDirectAggregate {
    ExplainDirectAggregate::Count
}

fn directory_history_aggregate_snapshot(_: &DirectoryHistoryAggregate) -> ExplainDirectAggregate {
    ExplainDirectAggregate::Count
}

fn direct_predicate_snapshot<Field>(
    operator: ExplainPredicateOperator,
    field: Field,
    value: Option<Value>,
    values: Vec<Value>,
) -> DirectPredicateSnapshot<Field> {
    DirectPredicateSnapshot {
        operator,
        field,
        value,
        values,
    }
}

fn state_history_projection_snapshot(
    projection: &StateHistoryProjection,
) -> StateHistoryProjectionSnapshot {
    StateHistoryProjectionSnapshot {
        output_name: projection.output_name.clone(),
        value: match &projection.value {
            StateHistoryProjectionValue::Field(field) => {
                StateHistoryProjectionValueSnapshot::Field(direct_state_history_field_snapshot(
                    field,
                ))
            }
            StateHistoryProjectionValue::Aggregate(aggregate) => {
                StateHistoryProjectionValueSnapshot::Aggregate(direct_aggregate_snapshot(aggregate))
            }
        },
    }
}

fn state_history_sort_key_snapshot(key: &StateHistorySortKey) -> StateHistorySortKeySnapshot {
    StateHistorySortKeySnapshot {
        output_name: key.output_name.clone(),
        value: key.value.as_ref().map(|value| match value {
            StateHistorySortValue::Field(field) => {
                StateHistorySortValueSnapshot::Field(direct_state_history_field_snapshot(field))
            }
            StateHistorySortValue::Aggregate(aggregate) => {
                StateHistorySortValueSnapshot::Aggregate(direct_aggregate_snapshot(aggregate))
            }
        }),
        descending: key.descending,
    }
}

fn state_history_predicate_snapshot(
    predicate: &StateHistoryPredicate,
) -> DirectPredicateSnapshot<ExplainDirectStateHistoryField> {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Eq,
            direct_state_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        StateHistoryPredicate::NotEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::NotEq,
            direct_state_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        StateHistoryPredicate::Gt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Gt,
            direct_state_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        StateHistoryPredicate::GtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::GtEq,
            direct_state_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        StateHistoryPredicate::Lt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Lt,
            direct_state_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        StateHistoryPredicate::LtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::LtEq,
            direct_state_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        StateHistoryPredicate::In(field, values) => direct_predicate_snapshot(
            ExplainPredicateOperator::In,
            direct_state_history_field_snapshot(field),
            None,
            values.clone(),
        ),
        StateHistoryPredicate::IsNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNull,
            direct_state_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
        StateHistoryPredicate::IsNotNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNotNull,
            direct_state_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
    }
}

fn state_history_aggregate_predicate_snapshot(
    predicate: &StateHistoryAggregatePredicate,
) -> StateHistoryAggregatePredicateSnapshot {
    match predicate {
        StateHistoryAggregatePredicate::Eq(aggregate, value) => {
            state_history_aggregate_predicate_snapshot_with_operator(
                ExplainAggregatePredicateOperator::Eq,
                aggregate,
                *value,
            )
        }
        StateHistoryAggregatePredicate::NotEq(aggregate, value) => {
            state_history_aggregate_predicate_snapshot_with_operator(
                ExplainAggregatePredicateOperator::NotEq,
                aggregate,
                *value,
            )
        }
        StateHistoryAggregatePredicate::Gt(aggregate, value) => {
            state_history_aggregate_predicate_snapshot_with_operator(
                ExplainAggregatePredicateOperator::Gt,
                aggregate,
                *value,
            )
        }
        StateHistoryAggregatePredicate::GtEq(aggregate, value) => {
            state_history_aggregate_predicate_snapshot_with_operator(
                ExplainAggregatePredicateOperator::GtEq,
                aggregate,
                *value,
            )
        }
        StateHistoryAggregatePredicate::Lt(aggregate, value) => {
            state_history_aggregate_predicate_snapshot_with_operator(
                ExplainAggregatePredicateOperator::Lt,
                aggregate,
                *value,
            )
        }
        StateHistoryAggregatePredicate::LtEq(aggregate, value) => {
            state_history_aggregate_predicate_snapshot_with_operator(
                ExplainAggregatePredicateOperator::LtEq,
                aggregate,
                *value,
            )
        }
    }
}

fn state_history_aggregate_predicate_snapshot_with_operator(
    operator: ExplainAggregatePredicateOperator,
    aggregate: &StateHistoryAggregate,
    value: i64,
) -> StateHistoryAggregatePredicateSnapshot {
    StateHistoryAggregatePredicateSnapshot {
        operator,
        aggregate: direct_aggregate_snapshot(aggregate),
        value,
    }
}

fn entity_history_projection_snapshot(
    projection: &EntityHistoryProjection,
) -> DirectFieldProjectionSnapshot<ExplainDirectEntityHistoryField> {
    DirectFieldProjectionSnapshot {
        output_name: projection.output_name.clone(),
        field: direct_entity_history_field_snapshot(&projection.field),
    }
}

fn entity_history_sort_key_snapshot(
    key: &EntityHistorySortKey,
) -> DirectSortKeySnapshot<ExplainDirectEntityHistoryField> {
    DirectSortKeySnapshot {
        output_name: key.output_name.clone(),
        field: key.field.as_ref().map(direct_entity_history_field_snapshot),
        descending: key.descending,
    }
}

fn entity_history_predicate_snapshot(
    predicate: &EntityHistoryPredicate,
) -> DirectPredicateSnapshot<ExplainDirectEntityHistoryField> {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Eq,
            direct_entity_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        EntityHistoryPredicate::NotEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::NotEq,
            direct_entity_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        EntityHistoryPredicate::Gt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Gt,
            direct_entity_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        EntityHistoryPredicate::GtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::GtEq,
            direct_entity_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        EntityHistoryPredicate::Lt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Lt,
            direct_entity_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        EntityHistoryPredicate::LtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::LtEq,
            direct_entity_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        EntityHistoryPredicate::In(field, values) => direct_predicate_snapshot(
            ExplainPredicateOperator::In,
            direct_entity_history_field_snapshot(field),
            None,
            values.clone(),
        ),
        EntityHistoryPredicate::IsNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNull,
            direct_entity_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
        EntityHistoryPredicate::IsNotNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNotNull,
            direct_entity_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
    }
}

fn file_history_projection_snapshot(
    projection: &FileHistoryProjection,
) -> DirectFieldProjectionSnapshot<ExplainDirectFileHistoryField> {
    DirectFieldProjectionSnapshot {
        output_name: projection.output_name.clone(),
        field: direct_file_history_field_snapshot(&projection.field),
    }
}

fn file_history_sort_key_snapshot(
    key: &FileHistorySortKey,
) -> DirectSortKeySnapshot<ExplainDirectFileHistoryField> {
    DirectSortKeySnapshot {
        output_name: key.output_name.clone(),
        field: key.field.as_ref().map(direct_file_history_field_snapshot),
        descending: key.descending,
    }
}

fn file_history_predicate_snapshot(
    predicate: &FileHistoryPredicate,
) -> DirectPredicateSnapshot<ExplainDirectFileHistoryField> {
    match predicate {
        FileHistoryPredicate::Eq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Eq,
            direct_file_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        FileHistoryPredicate::NotEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::NotEq,
            direct_file_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        FileHistoryPredicate::Gt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Gt,
            direct_file_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        FileHistoryPredicate::GtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::GtEq,
            direct_file_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        FileHistoryPredicate::Lt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Lt,
            direct_file_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        FileHistoryPredicate::LtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::LtEq,
            direct_file_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        FileHistoryPredicate::In(field, values) => direct_predicate_snapshot(
            ExplainPredicateOperator::In,
            direct_file_history_field_snapshot(field),
            None,
            values.clone(),
        ),
        FileHistoryPredicate::IsNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNull,
            direct_file_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
        FileHistoryPredicate::IsNotNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNotNull,
            direct_file_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
    }
}

fn directory_history_projection_snapshot(
    projection: &DirectoryHistoryProjection,
) -> DirectFieldProjectionSnapshot<ExplainDirectDirectoryHistoryField> {
    DirectFieldProjectionSnapshot {
        output_name: projection.output_name.clone(),
        field: direct_directory_history_field_snapshot(&projection.field),
    }
}

fn directory_history_sort_key_snapshot(
    key: &DirectoryHistorySortKey,
) -> DirectSortKeySnapshot<ExplainDirectDirectoryHistoryField> {
    DirectSortKeySnapshot {
        output_name: key.output_name.clone(),
        field: key
            .field
            .as_ref()
            .map(direct_directory_history_field_snapshot),
        descending: key.descending,
    }
}

fn directory_history_predicate_snapshot(
    predicate: &DirectoryHistoryPredicate,
) -> DirectPredicateSnapshot<ExplainDirectDirectoryHistoryField> {
    match predicate {
        DirectoryHistoryPredicate::Eq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Eq,
            direct_directory_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        DirectoryHistoryPredicate::NotEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::NotEq,
            direct_directory_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        DirectoryHistoryPredicate::Gt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Gt,
            direct_directory_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        DirectoryHistoryPredicate::GtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::GtEq,
            direct_directory_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        DirectoryHistoryPredicate::Lt(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::Lt,
            direct_directory_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        DirectoryHistoryPredicate::LtEq(field, value) => direct_predicate_snapshot(
            ExplainPredicateOperator::LtEq,
            direct_directory_history_field_snapshot(field),
            Some(value.clone()),
            Vec::new(),
        ),
        DirectoryHistoryPredicate::In(field, values) => direct_predicate_snapshot(
            ExplainPredicateOperator::In,
            direct_directory_history_field_snapshot(field),
            None,
            values.clone(),
        ),
        DirectoryHistoryPredicate::IsNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNull,
            direct_directory_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
        DirectoryHistoryPredicate::IsNotNull(field) => direct_predicate_snapshot(
            ExplainPredicateOperator::IsNotNull,
            direct_directory_history_field_snapshot(field),
            None,
            Vec::new(),
        ),
    }
}

fn explain_output_format_label(format: ExplainOutputFormat) -> &'static str {
    match format {
        ExplainOutputFormat::Text => "text",
        ExplainOutputFormat::Json => "json",
    }
}

fn explain_mode_label(mode: ExplainMode) -> &'static str {
    match mode {
        ExplainMode::Plan => "plan",
        ExplainMode::Analyze => "analyze",
    }
}

fn explain_public_read_strategy_label(strategy: ExplainPublicReadStrategy) -> &'static str {
    match strategy {
        ExplainPublicReadStrategy::Structured => "structured",
        ExplainPublicReadStrategy::DirectHistory => "direct_history",
        ExplainPublicReadStrategy::Broad => "broad",
    }
}

fn explain_write_operation_kind_label(kind: ExplainWriteOperationKind) -> &'static str {
    match kind {
        ExplainWriteOperationKind::Insert => "insert",
        ExplainWriteOperationKind::Update => "update",
        ExplainWriteOperationKind::Delete => "delete",
    }
}

fn explain_state_source_kind_label(kind: ExplainStateSourceKind) -> &'static str {
    match kind {
        ExplainStateSourceKind::AuthoritativeCommitted => "authoritative_committed",
        ExplainStateSourceKind::UntrackedOverlay => "untracked_overlay",
    }
}

fn explain_result_contract_label(contract: ExplainResultContract) -> &'static str {
    match contract {
        ExplainResultContract::Select => "select",
        ExplainResultContract::DmlNoReturning => "dml_no_returning",
        ExplainResultContract::DmlReturning => "dml_returning",
        ExplainResultContract::Other => "other",
    }
}

fn explain_lowered_result_columns_kind_label(
    kind: ExplainLoweredResultColumnsKind,
) -> &'static str {
    match kind {
        ExplainLoweredResultColumnsKind::Static => "static",
        ExplainLoweredResultColumnsKind::ByColumnName => "by_column_name",
    }
}

fn explain_stage_label(stage: ExplainStage) -> &'static str {
    match stage {
        ExplainStage::Parse => "parse",
        ExplainStage::Bind => "bind",
        ExplainStage::SemanticAnalysis => "semantic_analysis",
        ExplainStage::LogicalPlanning => "logical_planning",
        ExplainStage::Optimizer => "optimizer",
        ExplainStage::CapabilityResolution => "capability_resolution",
        ExplainStage::PhysicalPlanning => "physical_planning",
        ExplainStage::ExecutorPreparation => "executor_preparation",
    }
}

fn explain_direct_public_read_plan_label(plan: &ExplainDirectPublicReadPlan) -> &'static str {
    match plan {
        ExplainDirectPublicReadPlan::StateHistory(_) => "state_history",
        ExplainDirectPublicReadPlan::EntityHistory(_) => "entity_history",
        ExplainDirectPublicReadPlan::FileHistory(_) => "file_history",
        ExplainDirectPublicReadPlan::DirectoryHistory(_) => "directory_history",
    }
}

fn join_public_names(bindings: &[SurfaceBindingSnapshot]) -> String {
    if bindings.is_empty() {
        return "(none)".to_string();
    }
    bindings
        .iter()
        .map(|binding| binding.public_name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn saturating_duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}
