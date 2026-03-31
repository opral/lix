//! Explain stage ownership.
//!
//! This stage owns explain parsing, stable explain artifacts, stage timings,
//! and the final text/JSON rendering returned to callers.

use crate::backend::prepared::PreparedStatement;
use crate::backend::SqlDialect;
use crate::contracts::artifacts::{
    CommitPreconditions, DirectoryHistoryRequest, DomainChangeBatch, EffectiveStateRequest,
    EffectiveStateVersionScope, ExpectedHead, FileHistoryContentMode, FileHistoryLineageScope,
    FileHistoryRequest, FileHistoryRootScope, FileHistoryVersionScope, PublicDomainChange,
    SemanticEffect, SessionDependency, SessionStateDelta, StateHistoryContentMode,
    StateHistoryLineageScope, StateHistoryOrder, StateHistoryRequest, StateHistoryRootScope,
    StateHistoryVersionScope,
};
use crate::contracts::surface::{
    SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceReadFreshness, SurfaceReadSemantics,
    SurfaceVariant,
};
use crate::sql::backend::{PushdownDecision, PushdownSupport};
use crate::sql::binder::runtime::{RuntimeBindingKind, StatementBindingSource};
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
    BroadPublicReadAlias, BroadPublicReadDistinct, BroadPublicReadGroupBy,
    BroadPublicReadGroupByKind, BroadPublicReadJoin, BroadPublicReadJoinConstraint,
    BroadPublicReadJoinKind, BroadPublicReadLimitClause, BroadPublicReadOffset,
    BroadPublicReadOrderBy, BroadPublicReadOrderByExpr, BroadPublicReadOrderByKind,
    BroadPublicReadProjectionItem, BroadPublicReadProjectionItemKind, BroadPublicReadQuery,
    BroadPublicReadRelation, BroadPublicReadSelect, BroadPublicReadSetExpr,
    BroadPublicReadStatement, BroadPublicReadTableFactor, BroadPublicReadTableWithJoins,
    BroadSqlCaseWhen, BroadSqlExpr, BroadSqlExprKind, BroadSqlFunction, BroadSqlFunctionArg,
    BroadSqlFunctionArgExpr, BroadSqlFunctionArguments, CanonicalAdminKind, CanonicalAdminScan,
    CanonicalChangeScan, CanonicalFilesystemScan, CanonicalStateScan, CanonicalWorkingChangesScan,
    FilesystemKind, InsertOnConflict, InsertOnConflictAction, MutationPayload,
    NormalizedPublicReadQuery, OptionalTextPatch, PlannedFilesystemDescriptor,
    PlannedFilesystemFile, PlannedFilesystemState, PlannedStateRow, PlannedWrite, ReadCommand,
    ReadContract, ReadPlan, ResolvedRowRef, ResolvedWritePartition, ResolvedWritePlan, RowLineage,
    SchemaProof, ScopeProof, StateSourceKind, StructuredPublicRead, TargetSetProof, VersionScope,
    WriteCommand, WriteLane, WriteMode, WriteModeRequest, WriteOperationKind, WriteSelector,
};
use crate::sql::logical_plan::{
    DependencyPrecision, DependencySpec, InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan,
    PublicWriteLogicalPlan, ResultContract,
};
use crate::sql::physical_plan::plan::{
    LoweredReadStatement, LoweredReadStatementShape, LoweredStatementBindings,
};
use crate::sql::physical_plan::{
    LoweredReadProgram, LoweredResultColumn, LoweredResultColumns, PhysicalPlan,
    PreparedPublicReadExecution, PreparedPublicWriteExecution, PublicWriteExecutionPartition,
    PublicWriteMaterialization, TerminalRelationRenderNode, TrackedWriteExecution,
    UntrackedWriteExecution,
};
use crate::sql::routing::RoutingPassTrace;
use crate::sql::semantic_ir::internal::NormalizedInternalStatements;
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    EffectiveStatePlan, StateSourceAuthority,
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
use std::collections::{BTreeMap, BTreeSet};
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
    /// Parse SQL text into AST statements.
    Parse,
    /// Bind parsed statements into runtime-bound form.
    ///
    /// For broad public reads this includes both generic statement binding and
    /// the broad front-end bind that produces typed broad IR.
    Bind,
    /// Canonicalize already-bound public statements into semantic IR.
    SemanticAnalysis,
    /// Construct and verify logical plans from already-bound IR.
    ///
    /// For broad public reads this stage begins only after `Bind` has already
    /// produced the typed broad statement.
    LogicalPlanning,
    /// Route logical plans into execution strategies or lowerable relations.
    Routing,
    /// Load backend capability state such as live schemas or layouts required
    /// before routing or physical planning can proceed.
    CapabilityResolution,
    /// Lower logical plans into physical plans or lowered programs.
    PhysicalPlanning,
    /// Prepare executor artifacts such as rendered backend SQL.
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
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum LoweredReadStatementSnapshot {
    Final {
        statement_sql: String,
        bindings: LoweredStatementBindingsSnapshot,
    },
    Template {
        shell_statement_sql: String,
        bindings: LoweredStatementBindingsSnapshot,
        relation_render_nodes: Vec<TerminalRelationRenderNodeSnapshot>,
    },
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) order_by: Option<Box<ExplainBroadPublicReadOrderBySnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) limit_clause: Option<Box<ExplainBroadPublicReadLimitClauseSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadWithSnapshot {
    pub(crate) recursive: bool,
    pub(crate) cte_tables: Vec<ExplainBroadPublicReadCteSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadCteSnapshot {
    pub(crate) alias: ExplainBroadPublicReadAliasSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) materialized: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<String>,
    pub(crate) query: Box<ExplainBroadPublicReadQuerySnapshot>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) distinct: Option<ExplainBroadPublicReadDistinctSnapshot>,
    pub(crate) projection: Vec<ExplainBroadPublicReadProjectionItemSnapshot>,
    pub(crate) from: Vec<ExplainBroadPublicReadTableWithJoinsSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) selection: Option<Box<ExplainBroadSqlExprSnapshot>>,
    pub(crate) group_by: ExplainBroadPublicReadGroupBySnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) having: Option<Box<ExplainBroadSqlExprSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadDistinctSnapshot {
    Distinct,
    On {
        expressions: Vec<ExplainBroadSqlExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadTableWithJoinsSnapshot {
    pub(crate) relation: ExplainBroadPublicReadTableFactorSnapshot,
    pub(crate) joins: Vec<ExplainBroadPublicReadJoinSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadJoinSnapshot {
    pub(crate) global: bool,
    pub(crate) kind: ExplainBroadPublicReadJoinKindSnapshot,
    pub(crate) relation: ExplainBroadPublicReadTableFactorSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadJoinKindSnapshot {
    Join {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    Inner {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    Left {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    LeftOuter {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    Right {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    RightOuter {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    FullOuter {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    CrossJoin {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    Semi {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    LeftSemi {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    RightSemi {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    Anti {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    LeftAnti {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    RightAnti {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    StraightJoin {
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
    CrossApply,
    OuterApply,
    AsOf {
        match_condition: Box<ExplainBroadSqlExprSnapshot>,
        constraint: ExplainBroadPublicReadJoinConstraintSnapshot,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadJoinConstraintSnapshot {
    None,
    Natural,
    Using {
        columns: Vec<String>,
    },
    On {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadTableFactorSnapshot {
    Table {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<ExplainBroadPublicReadAliasSnapshot>,
        relation: ExplainBroadPublicReadRelationSnapshot,
    },
    Derived {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<ExplainBroadPublicReadAliasSnapshot>,
        subquery: Box<ExplainBroadPublicReadQuerySnapshot>,
    },
    NestedJoin {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<ExplainBroadPublicReadAliasSnapshot>,
        table_with_joins: Box<ExplainBroadPublicReadTableWithJoinsSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadAliasSnapshot {
    pub(crate) explicit: bool,
    pub(crate) name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadProjectionItemSnapshot {
    Wildcard,
    QualifiedWildcard {
        qualifier: Vec<String>,
    },
    Expr {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadGroupBySnapshot {
    All,
    Expressions {
        expressions: Vec<ExplainBroadSqlExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadOrderBySnapshot {
    All,
    Expressions {
        expressions: Vec<ExplainBroadPublicReadOrderByExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadOrderByExprSnapshot {
    pub(crate) expr: Box<ExplainBroadSqlExprSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) asc: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadPublicReadLimitClauseSnapshot {
    LimitOffset {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<Box<ExplainBroadSqlExprSnapshot>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<Box<ExplainBroadPublicReadOffsetSnapshot>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        limit_by: Vec<ExplainBroadSqlExprSnapshot>,
    },
    OffsetCommaLimit {
        offset: Box<ExplainBroadSqlExprSnapshot>,
        limit: Box<ExplainBroadSqlExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadOffsetSnapshot {
    pub(crate) value: Box<ExplainBroadSqlExprSnapshot>,
    pub(crate) rows: ExplainBroadOffsetRows,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadOffsetRows {
    None,
    Row,
    Rows,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadSqlExprSnapshot {
    Identifier {
        name: String,
    },
    CompoundIdentifier {
        parts: Vec<String>,
    },
    Value {
        value: String,
    },
    TypedString {
        data_type: String,
        value: String,
        uses_odbc_syntax: bool,
    },
    BinaryOp {
        left: Box<ExplainBroadSqlExprSnapshot>,
        op: String,
        right: Box<ExplainBroadSqlExprSnapshot>,
    },
    AnyOp {
        left: Box<ExplainBroadSqlExprSnapshot>,
        compare_op: String,
        right: Box<ExplainBroadSqlExprSnapshot>,
        is_some: bool,
    },
    AllOp {
        left: Box<ExplainBroadSqlExprSnapshot>,
        compare_op: String,
        right: Box<ExplainBroadSqlExprSnapshot>,
    },
    UnaryOp {
        op: String,
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    Nested {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsNull {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsNotNull {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsTrue {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsNotTrue {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsFalse {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsNotFalse {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsUnknown {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsNotUnknown {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsDistinctFrom {
        left: Box<ExplainBroadSqlExprSnapshot>,
        right: Box<ExplainBroadSqlExprSnapshot>,
    },
    IsNotDistinctFrom {
        left: Box<ExplainBroadSqlExprSnapshot>,
        right: Box<ExplainBroadSqlExprSnapshot>,
    },
    Cast {
        kind: String,
        expr: Box<ExplainBroadSqlExprSnapshot>,
        data_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<String>,
    },
    InList {
        expr: Box<ExplainBroadSqlExprSnapshot>,
        list: Vec<ExplainBroadSqlExprSnapshot>,
        negated: bool,
    },
    InSubquery {
        expr: Box<ExplainBroadSqlExprSnapshot>,
        subquery: Box<ExplainBroadPublicReadQuerySnapshot>,
        negated: bool,
    },
    InUnnest {
        expr: Box<ExplainBroadSqlExprSnapshot>,
        array_expr: Box<ExplainBroadSqlExprSnapshot>,
        negated: bool,
    },
    Between {
        expr: Box<ExplainBroadSqlExprSnapshot>,
        negated: bool,
        low: Box<ExplainBroadSqlExprSnapshot>,
        high: Box<ExplainBroadSqlExprSnapshot>,
    },
    Like {
        negated: bool,
        any: bool,
        expr: Box<ExplainBroadSqlExprSnapshot>,
        pattern: Box<ExplainBroadSqlExprSnapshot>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        escape_char: Option<String>,
    },
    ILike {
        negated: bool,
        any: bool,
        expr: Box<ExplainBroadSqlExprSnapshot>,
        pattern: Box<ExplainBroadSqlExprSnapshot>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        escape_char: Option<String>,
    },
    Function {
        function: Box<ExplainBroadSqlFunctionSnapshot>,
    },
    Case {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operand: Option<Box<ExplainBroadSqlExprSnapshot>>,
        conditions: Vec<ExplainBroadSqlCaseWhenSnapshot>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        else_result: Option<Box<ExplainBroadSqlExprSnapshot>>,
    },
    Exists {
        negated: bool,
        subquery: Box<ExplainBroadPublicReadQuerySnapshot>,
    },
    ScalarSubquery {
        query: Box<ExplainBroadPublicReadQuerySnapshot>,
    },
    Tuple {
        items: Vec<ExplainBroadSqlExprSnapshot>,
    },
    Unsupported {
        diagnostics_sql: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadSqlCaseWhenSnapshot {
    pub(crate) condition: ExplainBroadSqlExprSnapshot,
    pub(crate) result: ExplainBroadSqlExprSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadSqlFunctionSnapshot {
    pub(crate) name: Vec<String>,
    pub(crate) uses_odbc_syntax: bool,
    pub(crate) parameters: ExplainBroadSqlFunctionArgumentsSnapshot,
    pub(crate) args: ExplainBroadSqlFunctionArgumentsSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) filter: Option<Box<ExplainBroadSqlExprSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) null_treatment: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) within_group: Vec<ExplainBroadPublicReadOrderByExprSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadSqlFunctionArgumentsSnapshot {
    None,
    Subquery {
        query: Box<ExplainBroadPublicReadQuerySnapshot>,
    },
    List {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duplicate_treatment: Option<String>,
        args: Vec<ExplainBroadSqlFunctionArgSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadSqlFunctionArgSnapshot {
    Named {
        name: String,
        arg: Box<ExplainBroadSqlFunctionArgExprSnapshot>,
        operator: String,
    },
    ExprNamed {
        name: Box<ExplainBroadSqlExprSnapshot>,
        arg: Box<ExplainBroadSqlFunctionArgExprSnapshot>,
        operator: String,
    },
    Unnamed {
        arg: Box<ExplainBroadSqlFunctionArgExprSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub(crate) enum ExplainBroadSqlFunctionArgExprSnapshot {
    Expr {
        expr: Box<ExplainBroadSqlExprSnapshot>,
    },
    QualifiedWildcard {
        qualifier: Vec<String>,
    },
    Wildcard,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainBroadPublicReadRelationSummarySnapshot {
    pub(crate) public_relations: Vec<String>,
    pub(crate) lowered_public_relations: Vec<String>,
    pub(crate) internal_relations: Vec<String>,
    pub(crate) external_relations: Vec<String>,
    pub(crate) cte_relations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ExplainPublicReadLogicalPlan {
    pub(crate) strategy: ExplainPublicReadStrategy,
    pub(crate) surface_bindings: Vec<SurfaceBindingSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) broad_statement: Option<Box<ExplainBroadPublicReadStatementSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) broad_relation_summary: Option<Box<ExplainBroadPublicReadRelationSummarySnapshot>>,
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
    pub(crate) routing_passes: Vec<RoutingPassTrace>,
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
        if !self.routing_passes.is_empty() {
            sections.push((
                "routing_passes".to_string(),
                render_routing_passes_text(&self.routing_passes),
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
        ExplainLogicalPlanSnapshot::PublicRead(details) => {
            let mut lines = vec![
                "kind: public_read".to_string(),
                format!(
                    "strategy: {}",
                    explain_public_read_strategy_label(details.strategy)
                ),
                format!("surfaces: {}", join_public_names(&details.surface_bindings)),
                format!(
                    "broad_statement: {}",
                    yes_no(details.broad_statement.is_some())
                ),
            ];
            if let Some(summary) = details.broad_relation_summary.as_ref() {
                lines.push(format!(
                    "broad_public_relations: {}",
                    join_names_or_none(&summary.public_relations)
                ));
                lines.push(format!(
                    "broad_lowered_public_relations: {}",
                    join_names_or_none(&summary.lowered_public_relations)
                ));
                lines.push(format!(
                    "broad_internal_relations: {}",
                    join_names_or_none(&summary.internal_relations)
                ));
                lines.push(format!(
                    "broad_external_relations: {}",
                    join_names_or_none(&summary.external_relations)
                ));
                lines.push(format!(
                    "broad_cte_relations: {}",
                    join_names_or_none(&summary.cte_relations)
                ));
            }
            lines.push(format!(
                "structured_read: {}",
                yes_no(details.read.is_some())
            ));
            lines.push(format!(
                "direct_plan: {}",
                yes_no(details.direct_plan.is_some())
            ));
            lines.push(format!(
                "dependency_spec: {}",
                yes_no(details.dependency_spec.is_some())
            ));
            lines.push(format!(
                "effective_state_plan: {}",
                yes_no(details.effective_state_plan.is_some())
            ));
            lines.join("\n")
        }
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

fn render_routing_passes_text(passes: &[RoutingPassTrace]) -> String {
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
    pub(crate) routing_passes: Vec<RoutingPassTrace>,
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
) -> Result<ExplainArtifacts, LixError> {
    validate_public_read_explain_artifacts(&input)?;
    let executor_artifacts = executor_artifacts_for_public_read(
        &input.semantics,
        &input.optimized_logical_plan,
        &input.runtime_artifacts,
    );

    Ok(build_explain_artifacts(
        input.request,
        Some(SemanticStatement::PublicRead(input.semantics)),
        Some(LogicalPlan::PublicRead(input.logical_plan)),
        Some(LogicalPlan::PublicRead(input.optimized_logical_plan)),
        Some(PhysicalPlan::PublicRead(input.execution)),
        executor_artifacts,
        input.routing_passes,
        input.stage_timings,
    ))
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

fn validate_public_read_explain_artifacts(
    input: &PublicReadExplainBuildInput,
) -> Result<(), LixError> {
    if broad_public_read_explain_artifacts_collapsed(
        &input.logical_plan,
        &input.optimized_logical_plan,
        &input.routing_passes,
    ) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public read explain artifacts are inconsistent: broad routing reported changes but logical_plan and optimized_logical_plan collapsed to the same artifact",
        ));
    }
    Ok(())
}

fn broad_public_read_explain_artifacts_collapsed(
    logical_plan: &PublicReadLogicalPlan,
    optimized_logical_plan: &PublicReadLogicalPlan,
    routing_passes: &[RoutingPassTrace],
) -> bool {
    matches!(
        (logical_plan, optimized_logical_plan),
        (
            PublicReadLogicalPlan::Broad { .. },
            PublicReadLogicalPlan::Broad { .. }
        )
    ) && routing_passes.iter().any(|pass| pass.changed)
        && logical_plan == optimized_logical_plan
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
    routing_passes: Vec<RoutingPassTrace>,
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
        routing_passes,
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

fn broad_public_read_relation_summary_snapshot(
    statement: &BroadPublicReadStatement,
) -> ExplainBroadPublicReadRelationSummarySnapshot {
    let mut public_relations = BTreeSet::new();
    let mut lowered_public_relations = BTreeSet::new();
    let mut internal_relations = BTreeSet::new();
    let mut external_relations = BTreeSet::new();
    let mut cte_relations = BTreeSet::new();
    collect_broad_public_read_relation_summary(
        statement,
        &mut public_relations,
        &mut lowered_public_relations,
        &mut internal_relations,
        &mut external_relations,
        &mut cte_relations,
    );
    ExplainBroadPublicReadRelationSummarySnapshot {
        public_relations: public_relations.into_iter().collect(),
        lowered_public_relations: lowered_public_relations.into_iter().collect(),
        internal_relations: internal_relations.into_iter().collect(),
        external_relations: external_relations.into_iter().collect(),
        cte_relations: cte_relations.into_iter().collect(),
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
        order_by: query
            .order_by
            .as_ref()
            .map(broad_public_read_order_by_snapshot)
            .map(Box::new),
        limit_clause: query
            .limit_clause
            .as_ref()
            .map(broad_public_read_limit_clause_snapshot)
            .map(Box::new),
    }
}

fn broad_public_read_with_snapshot(
    with: &crate::sql::logical_plan::public_ir::BroadPublicReadWith,
) -> ExplainBroadPublicReadWithSnapshot {
    ExplainBroadPublicReadWithSnapshot {
        recursive: with.recursive,
        cte_tables: with
            .cte_tables
            .iter()
            .map(|cte| ExplainBroadPublicReadCteSnapshot {
                alias: broad_public_read_alias_snapshot(&cte.alias),
                materialized: cte.materialized.as_ref().map(ToString::to_string),
                from: cte.from.clone(),
                query: Box::new(broad_public_read_query_snapshot(&cte.query)),
            })
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
            operator,
            quantifier,
            left,
            right,
            ..
        } => ExplainBroadPublicReadSetExprSnapshot::SetOperation {
            operator: match operator {
                crate::sql::logical_plan::public_ir::BroadPublicReadSetOperationKind::Union => {
                    ExplainBroadSetOperationKind::Union
                }
                crate::sql::logical_plan::public_ir::BroadPublicReadSetOperationKind::Except => {
                    ExplainBroadSetOperationKind::Except
                }
                crate::sql::logical_plan::public_ir::BroadPublicReadSetOperationKind::Intersect => {
                    ExplainBroadSetOperationKind::Intersect
                }
                crate::sql::logical_plan::public_ir::BroadPublicReadSetOperationKind::Minus => {
                    ExplainBroadSetOperationKind::Minus
                }
            },
            quantifier: match quantifier {
                Some(crate::sql::logical_plan::public_ir::BroadPublicReadSetQuantifier::All) => {
                    Some(ExplainBroadSetQuantifier::All)
                }
                Some(
                    crate::sql::logical_plan::public_ir::BroadPublicReadSetQuantifier::Distinct,
                ) => Some(ExplainBroadSetQuantifier::Distinct),
                Some(crate::sql::logical_plan::public_ir::BroadPublicReadSetQuantifier::ByName) => {
                    Some(ExplainBroadSetQuantifier::ByName)
                }
                Some(
                    crate::sql::logical_plan::public_ir::BroadPublicReadSetQuantifier::AllByName,
                ) => Some(ExplainBroadSetQuantifier::AllByName),
                Some(
                    crate::sql::logical_plan::public_ir::BroadPublicReadSetQuantifier::DistinctByName,
                ) => Some(ExplainBroadSetQuantifier::DistinctByName),
                None => None,
            },
            left: Box::new(broad_public_read_set_expr_snapshot(left)),
            right: Box::new(broad_public_read_set_expr_snapshot(right)),
        },
        BroadPublicReadSetExpr::Table { relation, .. } => {
            ExplainBroadPublicReadSetExprSnapshot::Table {
                relation: broad_public_read_relation_snapshot(relation),
            }
        }
    }
}

fn broad_public_read_select_snapshot(
    select: &BroadPublicReadSelect,
) -> ExplainBroadPublicReadSelectSnapshot {
    ExplainBroadPublicReadSelectSnapshot {
        distinct: select
            .distinct
            .as_ref()
            .map(broad_public_read_distinct_snapshot),
        projection: select
            .projection
            .iter()
            .map(broad_public_read_projection_item_snapshot)
            .collect(),
        from: select
            .from
            .iter()
            .map(broad_public_read_table_with_joins_snapshot)
            .collect(),
        selection: select
            .selection
            .as_ref()
            .map(broad_sql_expr_snapshot)
            .map(Box::new),
        group_by: broad_public_read_group_by_snapshot(&select.group_by),
        having: select
            .having
            .as_ref()
            .map(broad_sql_expr_snapshot)
            .map(Box::new),
    }
}

fn broad_public_read_distinct_snapshot(
    distinct: &BroadPublicReadDistinct,
) -> ExplainBroadPublicReadDistinctSnapshot {
    match distinct {
        BroadPublicReadDistinct::Distinct => ExplainBroadPublicReadDistinctSnapshot::Distinct,
        BroadPublicReadDistinct::On(expressions) => ExplainBroadPublicReadDistinctSnapshot::On {
            expressions: expressions.iter().map(broad_sql_expr_snapshot).collect(),
        },
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
        global: join.global,
        kind: broad_public_read_join_kind_snapshot(&join.kind),
        relation: broad_public_read_table_factor_snapshot(&join.relation),
    }
}

fn broad_public_read_table_factor_snapshot(
    relation: &BroadPublicReadTableFactor,
) -> ExplainBroadPublicReadTableFactorSnapshot {
    match relation {
        BroadPublicReadTableFactor::Table {
            alias, relation, ..
        } => ExplainBroadPublicReadTableFactorSnapshot::Table {
            alias: alias.as_ref().map(broad_public_read_alias_snapshot),
            relation: broad_public_read_relation_snapshot(relation),
        },
        BroadPublicReadTableFactor::Derived {
            alias, subquery, ..
        } => ExplainBroadPublicReadTableFactorSnapshot::Derived {
            alias: alias.as_ref().map(broad_public_read_alias_snapshot),
            subquery: Box::new(broad_public_read_query_snapshot(subquery)),
        },
        BroadPublicReadTableFactor::NestedJoin {
            alias,
            table_with_joins,
            ..
        } => ExplainBroadPublicReadTableFactorSnapshot::NestedJoin {
            alias: alias.as_ref().map(broad_public_read_alias_snapshot),
            table_with_joins: Box::new(broad_public_read_table_with_joins_snapshot(
                table_with_joins,
            )),
        },
    }
}

fn broad_public_read_alias_snapshot(
    alias: &BroadPublicReadAlias,
) -> ExplainBroadPublicReadAliasSnapshot {
    ExplainBroadPublicReadAliasSnapshot {
        explicit: alias.explicit,
        name: alias.name.clone(),
        columns: alias.columns.clone(),
    }
}

fn broad_public_read_projection_item_snapshot(
    item: &BroadPublicReadProjectionItem,
) -> ExplainBroadPublicReadProjectionItemSnapshot {
    match &item.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadProjectionItemKind::Wildcard => {
            ExplainBroadPublicReadProjectionItemSnapshot::Wildcard
        }
        crate::sql::logical_plan::public_ir::BroadPublicReadProjectionItemKind::QualifiedWildcard {
            qualifier,
        } => ExplainBroadPublicReadProjectionItemSnapshot::QualifiedWildcard {
            qualifier: object_name_snapshot(qualifier),
        },
        crate::sql::logical_plan::public_ir::BroadPublicReadProjectionItemKind::Expr {
            alias,
            expr,
        } => ExplainBroadPublicReadProjectionItemSnapshot::Expr {
            alias: alias.clone(),
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
    }
}

fn broad_public_read_group_by_snapshot(
    group_by: &BroadPublicReadGroupBy,
) -> ExplainBroadPublicReadGroupBySnapshot {
    match &group_by.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadGroupByKind::All => {
            ExplainBroadPublicReadGroupBySnapshot::All
        }
        crate::sql::logical_plan::public_ir::BroadPublicReadGroupByKind::Expressions(
            expressions,
        ) => ExplainBroadPublicReadGroupBySnapshot::Expressions {
            expressions: expressions.iter().map(broad_sql_expr_snapshot).collect(),
        },
    }
}

fn broad_public_read_order_by_snapshot(
    order_by: &BroadPublicReadOrderBy,
) -> ExplainBroadPublicReadOrderBySnapshot {
    match &order_by.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadOrderByKind::All => {
            ExplainBroadPublicReadOrderBySnapshot::All
        }
        crate::sql::logical_plan::public_ir::BroadPublicReadOrderByKind::Expressions(
            expressions,
        ) => ExplainBroadPublicReadOrderBySnapshot::Expressions {
            expressions: expressions
                .iter()
                .map(broad_public_read_order_by_expr_snapshot)
                .collect(),
        },
    }
}

fn broad_public_read_order_by_expr_snapshot(
    expr: &BroadPublicReadOrderByExpr,
) -> ExplainBroadPublicReadOrderByExprSnapshot {
    ExplainBroadPublicReadOrderByExprSnapshot {
        expr: Box::new(broad_sql_expr_snapshot(&expr.expr)),
        asc: expr.asc,
        nulls_first: expr.nulls_first,
    }
}

fn broad_public_read_limit_clause_snapshot(
    limit_clause: &BroadPublicReadLimitClause,
) -> ExplainBroadPublicReadLimitClauseSnapshot {
    match &limit_clause.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => ExplainBroadPublicReadLimitClauseSnapshot::LimitOffset {
            limit: limit.as_ref().map(broad_sql_expr_snapshot).map(Box::new),
            offset: offset
                .as_ref()
                .map(broad_public_read_offset_snapshot)
                .map(Box::new),
            limit_by: limit_by.iter().map(broad_sql_expr_snapshot).collect(),
        },
        crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::OffsetCommaLimit {
            offset,
            limit,
        } => ExplainBroadPublicReadLimitClauseSnapshot::OffsetCommaLimit {
            offset: Box::new(broad_sql_expr_snapshot(offset)),
            limit: Box::new(broad_sql_expr_snapshot(limit)),
        },
    }
}

fn broad_public_read_offset_snapshot(
    offset: &BroadPublicReadOffset,
) -> ExplainBroadPublicReadOffsetSnapshot {
    ExplainBroadPublicReadOffsetSnapshot {
        value: Box::new(broad_sql_expr_snapshot(&offset.value)),
        rows: broad_offset_rows_snapshot(offset.rows),
    }
}

fn broad_offset_rows_snapshot(rows: sqlparser::ast::OffsetRows) -> ExplainBroadOffsetRows {
    match rows {
        sqlparser::ast::OffsetRows::None => ExplainBroadOffsetRows::None,
        sqlparser::ast::OffsetRows::Row => ExplainBroadOffsetRows::Row,
        sqlparser::ast::OffsetRows::Rows => ExplainBroadOffsetRows::Rows,
    }
}

fn broad_cast_kind_snapshot(kind: &sqlparser::ast::CastKind) -> &'static str {
    match kind {
        sqlparser::ast::CastKind::Cast => "cast",
        sqlparser::ast::CastKind::TryCast => "try_cast",
        sqlparser::ast::CastKind::SafeCast => "safe_cast",
        sqlparser::ast::CastKind::DoubleColon => "double_colon",
    }
}

fn broad_public_read_join_kind_snapshot(
    kind: &BroadPublicReadJoinKind,
) -> ExplainBroadPublicReadJoinKindSnapshot {
    match kind {
        BroadPublicReadJoinKind::Join(constraint) => ExplainBroadPublicReadJoinKindSnapshot::Join {
            constraint: broad_public_read_join_constraint_snapshot(constraint),
        },
        BroadPublicReadJoinKind::Inner(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::Inner {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::Left(constraint) => ExplainBroadPublicReadJoinKindSnapshot::Left {
            constraint: broad_public_read_join_constraint_snapshot(constraint),
        },
        BroadPublicReadJoinKind::LeftOuter(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::LeftOuter {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::Right(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::Right {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::RightOuter(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::RightOuter {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::FullOuter(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::FullOuter {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::CrossJoin(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::CrossJoin {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::Semi(constraint) => ExplainBroadPublicReadJoinKindSnapshot::Semi {
            constraint: broad_public_read_join_constraint_snapshot(constraint),
        },
        BroadPublicReadJoinKind::LeftSemi(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::LeftSemi {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::RightSemi(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::RightSemi {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::Anti(constraint) => ExplainBroadPublicReadJoinKindSnapshot::Anti {
            constraint: broad_public_read_join_constraint_snapshot(constraint),
        },
        BroadPublicReadJoinKind::LeftAnti(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::LeftAnti {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::RightAnti(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::RightAnti {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::StraightJoin(constraint) => {
            ExplainBroadPublicReadJoinKindSnapshot::StraightJoin {
                constraint: broad_public_read_join_constraint_snapshot(constraint),
            }
        }
        BroadPublicReadJoinKind::CrossApply => ExplainBroadPublicReadJoinKindSnapshot::CrossApply,
        BroadPublicReadJoinKind::OuterApply => ExplainBroadPublicReadJoinKindSnapshot::OuterApply,
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => ExplainBroadPublicReadJoinKindSnapshot::AsOf {
            match_condition: Box::new(broad_sql_expr_snapshot(match_condition)),
            constraint: broad_public_read_join_constraint_snapshot(constraint),
        },
    }
}

fn broad_public_read_join_constraint_snapshot(
    constraint: &BroadPublicReadJoinConstraint,
) -> ExplainBroadPublicReadJoinConstraintSnapshot {
    match constraint {
        BroadPublicReadJoinConstraint::None => ExplainBroadPublicReadJoinConstraintSnapshot::None,
        BroadPublicReadJoinConstraint::Natural => {
            ExplainBroadPublicReadJoinConstraintSnapshot::Natural
        }
        BroadPublicReadJoinConstraint::Using(columns) => {
            ExplainBroadPublicReadJoinConstraintSnapshot::Using {
                columns: columns.clone(),
            }
        }
        BroadPublicReadJoinConstraint::On(expr) => {
            ExplainBroadPublicReadJoinConstraintSnapshot::On {
                expr: Box::new(broad_sql_expr_snapshot(expr)),
            }
        }
    }
}

fn broad_sql_expr_snapshot(expr: &BroadSqlExpr) -> ExplainBroadSqlExprSnapshot {
    match &expr.kind {
        BroadSqlExprKind::Identifier(ident) => ExplainBroadSqlExprSnapshot::Identifier {
            name: ident.value.clone(),
        },
        BroadSqlExprKind::CompoundIdentifier(parts) => {
            ExplainBroadSqlExprSnapshot::CompoundIdentifier {
                parts: parts.iter().map(|part| part.value.clone()).collect(),
            }
        }
        BroadSqlExprKind::Value(value) => ExplainBroadSqlExprSnapshot::Value {
            value: value.to_string(),
        },
        BroadSqlExprKind::TypedString {
            data_type,
            value,
            uses_odbc_syntax,
        } => ExplainBroadSqlExprSnapshot::TypedString {
            data_type: data_type.to_string(),
            value: value.to_string(),
            uses_odbc_syntax: *uses_odbc_syntax,
        },
        BroadSqlExprKind::BinaryOp { left, op, right } => ExplainBroadSqlExprSnapshot::BinaryOp {
            left: Box::new(broad_sql_expr_snapshot(left)),
            op: op.to_string(),
            right: Box::new(broad_sql_expr_snapshot(right)),
        },
        BroadSqlExprKind::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => ExplainBroadSqlExprSnapshot::AnyOp {
            left: Box::new(broad_sql_expr_snapshot(left)),
            compare_op: compare_op.to_string(),
            right: Box::new(broad_sql_expr_snapshot(right)),
            is_some: *is_some,
        },
        BroadSqlExprKind::AllOp {
            left,
            compare_op,
            right,
        } => ExplainBroadSqlExprSnapshot::AllOp {
            left: Box::new(broad_sql_expr_snapshot(left)),
            compare_op: compare_op.to_string(),
            right: Box::new(broad_sql_expr_snapshot(right)),
        },
        BroadSqlExprKind::UnaryOp { op, expr } => ExplainBroadSqlExprSnapshot::UnaryOp {
            op: op.to_string(),
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::Nested(expr) => ExplainBroadSqlExprSnapshot::Nested {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsNull(expr) => ExplainBroadSqlExprSnapshot::IsNull {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsNotNull(expr) => ExplainBroadSqlExprSnapshot::IsNotNull {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsTrue(expr) => ExplainBroadSqlExprSnapshot::IsTrue {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsNotTrue(expr) => ExplainBroadSqlExprSnapshot::IsNotTrue {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsFalse(expr) => ExplainBroadSqlExprSnapshot::IsFalse {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsNotFalse(expr) => ExplainBroadSqlExprSnapshot::IsNotFalse {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsUnknown(expr) => ExplainBroadSqlExprSnapshot::IsUnknown {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsNotUnknown(expr) => ExplainBroadSqlExprSnapshot::IsNotUnknown {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlExprKind::IsDistinctFrom { left, right } => {
            ExplainBroadSqlExprSnapshot::IsDistinctFrom {
                left: Box::new(broad_sql_expr_snapshot(left)),
                right: Box::new(broad_sql_expr_snapshot(right)),
            }
        }
        BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            ExplainBroadSqlExprSnapshot::IsNotDistinctFrom {
                left: Box::new(broad_sql_expr_snapshot(left)),
                right: Box::new(broad_sql_expr_snapshot(right)),
            }
        }
        BroadSqlExprKind::Cast {
            kind,
            expr,
            data_type,
            format,
        } => ExplainBroadSqlExprSnapshot::Cast {
            kind: broad_cast_kind_snapshot(kind).to_string(),
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            data_type: data_type.to_string(),
            format: format.as_ref().map(ToString::to_string),
        },
        BroadSqlExprKind::InList {
            expr,
            list,
            negated,
        } => ExplainBroadSqlExprSnapshot::InList {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            list: list.iter().map(broad_sql_expr_snapshot).collect(),
            negated: *negated,
        },
        BroadSqlExprKind::InSubquery {
            expr,
            subquery,
            negated,
        } => ExplainBroadSqlExprSnapshot::InSubquery {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            subquery: Box::new(broad_public_read_query_snapshot(subquery)),
            negated: *negated,
        },
        BroadSqlExprKind::InUnnest {
            expr,
            array_expr,
            negated,
        } => ExplainBroadSqlExprSnapshot::InUnnest {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            array_expr: Box::new(broad_sql_expr_snapshot(array_expr)),
            negated: *negated,
        },
        BroadSqlExprKind::Between {
            expr,
            negated,
            low,
            high,
        } => ExplainBroadSqlExprSnapshot::Between {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            negated: *negated,
            low: Box::new(broad_sql_expr_snapshot(low)),
            high: Box::new(broad_sql_expr_snapshot(high)),
        },
        BroadSqlExprKind::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => ExplainBroadSqlExprSnapshot::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            pattern: Box::new(broad_sql_expr_snapshot(pattern)),
            escape_char: escape_char.as_ref().map(ToString::to_string),
        },
        BroadSqlExprKind::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => ExplainBroadSqlExprSnapshot::ILike {
            negated: *negated,
            any: *any,
            expr: Box::new(broad_sql_expr_snapshot(expr)),
            pattern: Box::new(broad_sql_expr_snapshot(pattern)),
            escape_char: escape_char.as_ref().map(ToString::to_string),
        },
        BroadSqlExprKind::Function(function) => ExplainBroadSqlExprSnapshot::Function {
            function: Box::new(broad_sql_function_snapshot(function)),
        },
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => ExplainBroadSqlExprSnapshot::Case {
            operand: operand
                .as_ref()
                .map(|expr| Box::new(broad_sql_expr_snapshot(expr))),
            conditions: conditions
                .iter()
                .map(broad_sql_case_when_snapshot)
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|expr| Box::new(broad_sql_expr_snapshot(expr))),
        },
        BroadSqlExprKind::Exists { negated, subquery } => ExplainBroadSqlExprSnapshot::Exists {
            negated: *negated,
            subquery: Box::new(broad_public_read_query_snapshot(subquery)),
        },
        BroadSqlExprKind::ScalarSubquery(query) => ExplainBroadSqlExprSnapshot::ScalarSubquery {
            query: Box::new(broad_public_read_query_snapshot(query)),
        },
        BroadSqlExprKind::Tuple(items) => ExplainBroadSqlExprSnapshot::Tuple {
            items: items.iter().map(broad_sql_expr_snapshot).collect(),
        },
        BroadSqlExprKind::Unsupported { diagnostics_sql } => {
            ExplainBroadSqlExprSnapshot::Unsupported {
                diagnostics_sql: diagnostics_sql.clone(),
            }
        }
    }
}

fn broad_sql_case_when_snapshot(when: &BroadSqlCaseWhen) -> ExplainBroadSqlCaseWhenSnapshot {
    ExplainBroadSqlCaseWhenSnapshot {
        condition: broad_sql_expr_snapshot(&when.condition),
        result: broad_sql_expr_snapshot(&when.result),
    }
}

fn broad_sql_function_snapshot(function: &BroadSqlFunction) -> ExplainBroadSqlFunctionSnapshot {
    ExplainBroadSqlFunctionSnapshot {
        name: object_name_snapshot(&function.name),
        uses_odbc_syntax: function.uses_odbc_syntax,
        parameters: broad_sql_function_arguments_snapshot(&function.parameters),
        args: broad_sql_function_arguments_snapshot(&function.args),
        filter: function
            .filter
            .as_ref()
            .map(|expr| Box::new(broad_sql_expr_snapshot(expr))),
        null_treatment: function.null_treatment.as_ref().map(ToString::to_string),
        within_group: function
            .within_group
            .iter()
            .map(broad_public_read_order_by_expr_snapshot)
            .collect(),
    }
}

fn broad_sql_function_arguments_snapshot(
    arguments: &BroadSqlFunctionArguments,
) -> ExplainBroadSqlFunctionArgumentsSnapshot {
    match arguments {
        BroadSqlFunctionArguments::None => ExplainBroadSqlFunctionArgumentsSnapshot::None,
        BroadSqlFunctionArguments::Subquery(query) => {
            ExplainBroadSqlFunctionArgumentsSnapshot::Subquery {
                query: Box::new(broad_public_read_query_snapshot(query)),
            }
        }
        BroadSqlFunctionArguments::List(list) => ExplainBroadSqlFunctionArgumentsSnapshot::List {
            duplicate_treatment: list.duplicate_treatment.as_ref().map(ToString::to_string),
            args: list
                .args
                .iter()
                .map(broad_sql_function_arg_snapshot)
                .collect(),
        },
    }
}

fn broad_sql_function_arg_snapshot(
    arg: &BroadSqlFunctionArg,
) -> ExplainBroadSqlFunctionArgSnapshot {
    match arg {
        BroadSqlFunctionArg::Named {
            name,
            arg,
            operator,
        } => ExplainBroadSqlFunctionArgSnapshot::Named {
            name: name.value.clone(),
            arg: Box::new(broad_sql_function_arg_expr_snapshot(arg)),
            operator: operator.to_string(),
        },
        BroadSqlFunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => ExplainBroadSqlFunctionArgSnapshot::ExprNamed {
            name: Box::new(broad_sql_expr_snapshot(name)),
            arg: Box::new(broad_sql_function_arg_expr_snapshot(arg)),
            operator: operator.to_string(),
        },
        BroadSqlFunctionArg::Unnamed(arg) => ExplainBroadSqlFunctionArgSnapshot::Unnamed {
            arg: Box::new(broad_sql_function_arg_expr_snapshot(arg)),
        },
    }
}

fn broad_sql_function_arg_expr_snapshot(
    arg: &BroadSqlFunctionArgExpr,
) -> ExplainBroadSqlFunctionArgExprSnapshot {
    match arg {
        BroadSqlFunctionArgExpr::Expr(expr) => ExplainBroadSqlFunctionArgExprSnapshot::Expr {
            expr: Box::new(broad_sql_expr_snapshot(expr)),
        },
        BroadSqlFunctionArgExpr::QualifiedWildcard(object_name) => {
            ExplainBroadSqlFunctionArgExprSnapshot::QualifiedWildcard {
                qualifier: object_name_snapshot(object_name),
            }
        }
        BroadSqlFunctionArgExpr::Wildcard => ExplainBroadSqlFunctionArgExprSnapshot::Wildcard,
    }
}

fn object_name_snapshot(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0.iter().map(ToString::to_string).collect()
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

fn collect_broad_public_read_relation_summary(
    statement: &BroadPublicReadStatement,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    collect_broad_public_read_relation_summary_in_statement(
        statement,
        public_relations,
        lowered_public_relations,
        internal_relations,
        external_relations,
        cte_relations,
    );
}

fn collect_broad_public_read_relation_summary_in_statement(
    statement: &BroadPublicReadStatement,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            collect_broad_public_read_relation_summary_in_query(
                query,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            collect_broad_public_read_relation_summary_in_statement(
                statement,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
}

fn collect_broad_public_read_relation_summary_in_query(
    query: &BroadPublicReadQuery,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_broad_public_read_relation_summary_in_query(
                &cte.query,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
    collect_broad_public_read_relation_summary_in_set_expr(
        &query.body,
        public_relations,
        lowered_public_relations,
        internal_relations,
        external_relations,
        cte_relations,
    );
    if let Some(order_by) = &query.order_by {
        collect_broad_public_read_relation_summary_in_order_by(
            order_by,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
    if let Some(limit_clause) = &query.limit_clause {
        collect_broad_public_read_relation_summary_in_limit_clause(
            limit_clause,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
}

fn collect_broad_public_read_relation_summary_in_set_expr(
    expr: &BroadPublicReadSetExpr,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            collect_broad_public_read_relation_summary_in_select(
                select,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadPublicReadSetExpr::Query(query) => {
            collect_broad_public_read_relation_summary_in_query(
                query,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            collect_broad_public_read_relation_summary_in_set_expr(
                left,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_set_expr(
                right,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadPublicReadSetExpr::Table { relation, .. } => collect_broad_public_read_relation_name(
            relation,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        ),
    }
}

fn collect_broad_public_read_relation_summary_in_select(
    select: &BroadPublicReadSelect,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    if let Some(distinct) = &select.distinct {
        collect_broad_public_read_relation_summary_in_distinct(
            distinct,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
    for projection in &select.projection {
        if let BroadPublicReadProjectionItemKind::Expr { expr, .. } = &projection.kind {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
    for table in &select.from {
        collect_broad_public_read_relation_summary_in_table_with_joins(
            table,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
    if let Some(selection) = &select.selection {
        collect_broad_public_read_relation_summary_in_sql_expr(
            selection,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
    if let BroadPublicReadGroupByKind::Expressions(expressions) = &select.group_by.kind {
        for expr in expressions {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
    if let Some(having) = &select.having {
        collect_broad_public_read_relation_summary_in_sql_expr(
            having,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
}

fn collect_broad_public_read_relation_summary_in_distinct(
    distinct: &BroadPublicReadDistinct,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    if let BroadPublicReadDistinct::On(expressions) = distinct {
        for expr in expressions {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
}

fn collect_broad_public_read_relation_summary_in_order_by(
    order_by: &BroadPublicReadOrderBy,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    if let BroadPublicReadOrderByKind::Expressions(expressions) = &order_by.kind {
        for expr in expressions {
            collect_broad_public_read_relation_summary_in_sql_expr(
                &expr.expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
}

fn collect_broad_public_read_relation_summary_in_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    collect_broad_public_read_relation_summary_in_table_factor(
        &table.relation,
        public_relations,
        lowered_public_relations,
        internal_relations,
        external_relations,
        cte_relations,
    );
    for join in &table.joins {
        collect_broad_public_read_relation_summary_in_table_factor(
            &join.relation,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
        collect_broad_public_read_relation_summary_in_join_kind(
            &join.kind,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
}

fn collect_broad_public_read_relation_summary_in_join_kind(
    kind: &BroadPublicReadJoinKind,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match kind {
        BroadPublicReadJoinKind::Join(constraint)
        | BroadPublicReadJoinKind::Inner(constraint)
        | BroadPublicReadJoinKind::Left(constraint)
        | BroadPublicReadJoinKind::LeftOuter(constraint)
        | BroadPublicReadJoinKind::Right(constraint)
        | BroadPublicReadJoinKind::RightOuter(constraint)
        | BroadPublicReadJoinKind::FullOuter(constraint)
        | BroadPublicReadJoinKind::CrossJoin(constraint)
        | BroadPublicReadJoinKind::Semi(constraint)
        | BroadPublicReadJoinKind::LeftSemi(constraint)
        | BroadPublicReadJoinKind::RightSemi(constraint)
        | BroadPublicReadJoinKind::Anti(constraint)
        | BroadPublicReadJoinKind::LeftAnti(constraint)
        | BroadPublicReadJoinKind::RightAnti(constraint)
        | BroadPublicReadJoinKind::StraightJoin(constraint) => {
            collect_broad_public_read_relation_summary_in_join_constraint(
                constraint,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadPublicReadJoinKind::CrossApply | BroadPublicReadJoinKind::OuterApply => {}
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                match_condition,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_join_constraint(
                constraint,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
}

fn collect_broad_public_read_relation_summary_in_join_constraint(
    constraint: &BroadPublicReadJoinConstraint,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    if let BroadPublicReadJoinConstraint::On(expr) = constraint {
        collect_broad_public_read_relation_summary_in_sql_expr(
            expr,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
}

fn collect_broad_public_read_relation_summary_in_limit_clause(
    limit_clause: &BroadPublicReadLimitClause,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match &limit_clause.kind {
        crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    limit,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
            if let Some(offset) = offset {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    &offset.value,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
            for expr in limit_by {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    expr,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
        }
        crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::OffsetCommaLimit {
            offset,
            limit,
        } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                offset,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_sql_expr(
                limit,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
}

fn collect_broad_public_read_relation_summary_in_table_factor(
    factor: &BroadPublicReadTableFactor,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => {
            collect_broad_public_read_relation_name(
                relation,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            )
        }
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            collect_broad_public_read_relation_summary_in_query(
                subquery,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_broad_public_read_relation_summary_in_table_with_joins(
            table_with_joins,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        ),
    }
}

fn collect_broad_public_read_relation_summary_in_sql_expr(
    expr: &BroadSqlExpr,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match &expr.kind {
        BroadSqlExprKind::Identifier(_)
        | BroadSqlExprKind::CompoundIdentifier(_)
        | BroadSqlExprKind::Value(_)
        | BroadSqlExprKind::TypedString { .. }
        | BroadSqlExprKind::Unsupported { .. } => {}
        BroadSqlExprKind::BinaryOp { left, right, .. }
        | BroadSqlExprKind::IsDistinctFrom { left, right }
        | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                left,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_sql_expr(
                right,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::AnyOp { left, right, .. }
        | BroadSqlExprKind::AllOp { left, right, .. } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                left,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_sql_expr(
                right,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::UnaryOp { expr, .. }
        | BroadSqlExprKind::Nested(expr)
        | BroadSqlExprKind::IsNull(expr)
        | BroadSqlExprKind::IsNotNull(expr)
        | BroadSqlExprKind::IsTrue(expr)
        | BroadSqlExprKind::IsNotTrue(expr)
        | BroadSqlExprKind::IsFalse(expr)
        | BroadSqlExprKind::IsNotFalse(expr)
        | BroadSqlExprKind::IsUnknown(expr)
        | BroadSqlExprKind::IsNotUnknown(expr)
        | BroadSqlExprKind::Cast { expr, .. } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::InList { expr, list, .. } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            for item in list {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    item,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
        }
        BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_query(
                subquery,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::InUnnest {
            expr, array_expr, ..
        } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_sql_expr(
                array_expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::Between {
            expr, low, high, ..
        } => {
            for child in [expr.as_ref(), low.as_ref(), high.as_ref()] {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    child,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
        }
        BroadSqlExprKind::Like { expr, pattern, .. }
        | BroadSqlExprKind::ILike { expr, pattern, .. } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                expr,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_sql_expr(
                pattern,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::Function(function) => {
            collect_broad_public_read_relation_summary_in_sql_function(
                function,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    operand,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
            for when in conditions {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    &when.condition,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
                collect_broad_public_read_relation_summary_in_sql_expr(
                    &when.result,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
            if let Some(else_result) = else_result {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    else_result,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
        }
        BroadSqlExprKind::Exists { subquery, .. } | BroadSqlExprKind::ScalarSubquery(subquery) => {
            collect_broad_public_read_relation_summary_in_query(
                subquery,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlExprKind::Tuple(items) => {
            for item in items {
                collect_broad_public_read_relation_summary_in_sql_expr(
                    item,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
        }
    }
}

fn collect_broad_public_read_relation_summary_in_sql_function(
    function: &BroadSqlFunction,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    collect_broad_public_read_relation_summary_in_sql_function_arguments(
        &function.parameters,
        public_relations,
        lowered_public_relations,
        internal_relations,
        external_relations,
        cte_relations,
    );
    collect_broad_public_read_relation_summary_in_sql_function_arguments(
        &function.args,
        public_relations,
        lowered_public_relations,
        internal_relations,
        external_relations,
        cte_relations,
    );
    if let Some(filter) = &function.filter {
        collect_broad_public_read_relation_summary_in_sql_expr(
            filter,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
    for expr in &function.within_group {
        collect_broad_public_read_relation_summary_in_sql_expr(
            &expr.expr,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
}

fn collect_broad_public_read_relation_summary_in_sql_function_arguments(
    arguments: &BroadSqlFunctionArguments,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match arguments {
        BroadSqlFunctionArguments::None => {}
        BroadSqlFunctionArguments::Subquery(query) => {
            collect_broad_public_read_relation_summary_in_query(
                query,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlFunctionArguments::List(list) => {
            for arg in &list.args {
                collect_broad_public_read_relation_summary_in_sql_function_arg(
                    arg,
                    public_relations,
                    lowered_public_relations,
                    internal_relations,
                    external_relations,
                    cte_relations,
                );
            }
        }
    }
}

fn collect_broad_public_read_relation_summary_in_sql_function_arg(
    arg: &BroadSqlFunctionArg,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match arg {
        BroadSqlFunctionArg::Named { arg, .. } => {
            collect_broad_public_read_relation_summary_in_sql_function_arg_expr(
                arg,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlFunctionArg::ExprNamed { name, arg, .. } => {
            collect_broad_public_read_relation_summary_in_sql_expr(
                name,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
            collect_broad_public_read_relation_summary_in_sql_function_arg_expr(
                arg,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
        BroadSqlFunctionArg::Unnamed(arg) => {
            collect_broad_public_read_relation_summary_in_sql_function_arg_expr(
                arg,
                public_relations,
                lowered_public_relations,
                internal_relations,
                external_relations,
                cte_relations,
            );
        }
    }
}

fn collect_broad_public_read_relation_summary_in_sql_function_arg_expr(
    arg: &BroadSqlFunctionArgExpr,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    if let BroadSqlFunctionArgExpr::Expr(expr) = arg {
        collect_broad_public_read_relation_summary_in_sql_expr(
            expr,
            public_relations,
            lowered_public_relations,
            internal_relations,
            external_relations,
            cte_relations,
        );
    }
}

fn collect_broad_public_read_relation_name(
    relation: &BroadPublicReadRelation,
    public_relations: &mut BTreeSet<String>,
    lowered_public_relations: &mut BTreeSet<String>,
    internal_relations: &mut BTreeSet<String>,
    external_relations: &mut BTreeSet<String>,
    cte_relations: &mut BTreeSet<String>,
) {
    match relation {
        BroadPublicReadRelation::Public(binding) => {
            public_relations.insert(binding.descriptor.public_name.clone());
        }
        BroadPublicReadRelation::LoweredPublic(binding) => {
            lowered_public_relations.insert(binding.descriptor.public_name.clone());
        }
        BroadPublicReadRelation::Internal(name) => {
            internal_relations.insert(name.clone());
        }
        BroadPublicReadRelation::External(name) => {
            external_relations.insert(name.clone());
        }
        BroadPublicReadRelation::Cte(name) => {
            cte_relations.insert(name.clone());
        }
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
                    broad_relation_summary: None,
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
                    broad_relation_summary: None,
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
                    broad_relation_summary: Some(Box::new(
                        broad_public_read_relation_summary_snapshot(broad_statement),
                    )),
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
    state: &PlannedFilesystemState,
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
    state: &PlannedFilesystemFile,
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
    state: &PlannedFilesystemDescriptor,
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
    let bindings = lowered_statement_bindings_snapshot(&statement.bindings);
    match &statement.shape {
        LoweredReadStatementShape::Final { statement_sql } => LoweredReadStatementSnapshot::Final {
            statement_sql: statement_sql.clone(),
            bindings,
        },
        LoweredReadStatementShape::Template {
            shell_statement_sql,
            relation_render_nodes,
            ..
        } => LoweredReadStatementSnapshot::Template {
            shell_statement_sql: shell_statement_sql.clone(),
            bindings,
            relation_render_nodes: relation_render_nodes
                .iter()
                .map(terminal_relation_render_node_snapshot)
                .collect(),
        },
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

fn state_history_request_snapshot(request: &StateHistoryRequest) -> StateHistoryRequestSnapshot {
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

fn file_history_request_snapshot(request: &FileHistoryRequest) -> FileHistoryRequestSnapshot {
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
    request: &DirectoryHistoryRequest,
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
        precision: dependency_precision_snapshot(spec.precision),
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
        version_scope: effective_state_version_scope_snapshot(request.version_scope),
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

fn surface_read_freshness_name(freshness: SurfaceReadFreshness) -> &'static str {
    match freshness {
        SurfaceReadFreshness::RequiresFreshProjection => "requires_fresh_projection",
        SurfaceReadFreshness::AllowsStaleProjection => "allows_stale_projection",
    }
}

fn default_scope_name(scope: crate::contracts::surface::DefaultScopeSemantics) -> &'static str {
    match scope {
        crate::contracts::surface::DefaultScopeSemantics::ActiveVersion => "active_version",
        crate::contracts::surface::DefaultScopeSemantics::ExplicitVersion => "explicit_version",
        crate::contracts::surface::DefaultScopeSemantics::History => "history",
        crate::contracts::surface::DefaultScopeSemantics::GlobalAdmin => "global_admin",
        crate::contracts::surface::DefaultScopeSemantics::WorkingChanges => "working_changes",
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

fn effective_state_version_scope_snapshot(
    scope: EffectiveStateVersionScope,
) -> ExplainVersionScope {
    match scope {
        EffectiveStateVersionScope::ActiveVersion => ExplainVersionScope::ActiveVersion,
        EffectiveStateVersionScope::ExplicitVersion => ExplainVersionScope::ExplicitVersion,
        EffectiveStateVersionScope::History => ExplainVersionScope::History,
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

fn state_history_root_scope_snapshot(scope: &StateHistoryRootScope) -> ExplainHistoryRootScopeKind {
    match scope {
        StateHistoryRootScope::AllRoots => ExplainHistoryRootScopeKind::AllRoots,
        StateHistoryRootScope::RequestedRoots(_) => ExplainHistoryRootScopeKind::RequestedRoots,
    }
}

fn state_history_requested_roots(scope: &StateHistoryRootScope) -> Vec<String> {
    match scope {
        StateHistoryRootScope::AllRoots => Vec::new(),
        StateHistoryRootScope::RequestedRoots(roots) => roots.clone(),
    }
}

fn state_history_lineage_scope_snapshot(
    scope: StateHistoryLineageScope,
) -> ExplainHistoryLineageScope {
    match scope {
        StateHistoryLineageScope::Standard => ExplainHistoryLineageScope::Standard,
        StateHistoryLineageScope::ActiveVersion => ExplainHistoryLineageScope::ActiveVersion,
    }
}

fn state_history_version_scope_snapshot(
    scope: &StateHistoryVersionScope,
) -> ExplainHistoryVersionScopeKind {
    match scope {
        StateHistoryVersionScope::Any => ExplainHistoryVersionScopeKind::Any,
        StateHistoryVersionScope::RequestedVersions(_) => {
            ExplainHistoryVersionScopeKind::RequestedVersions
        }
    }
}

fn state_history_requested_versions(scope: &StateHistoryVersionScope) -> Vec<String> {
    match scope {
        StateHistoryVersionScope::Any => Vec::new(),
        StateHistoryVersionScope::RequestedVersions(versions) => versions.clone(),
    }
}

fn state_history_content_mode_snapshot(
    mode: StateHistoryContentMode,
) -> ExplainStateHistoryContentMode {
    match mode {
        StateHistoryContentMode::MetadataOnly => ExplainStateHistoryContentMode::MetadataOnly,
        StateHistoryContentMode::IncludeSnapshotContent => {
            ExplainStateHistoryContentMode::IncludeSnapshotContent
        }
    }
}

fn state_history_order_snapshot(order: StateHistoryOrder) -> ExplainStateHistoryOrder {
    match order {
        StateHistoryOrder::EntityFileSchemaDepthAsc => {
            ExplainStateHistoryOrder::EntityFileSchemaDepthAsc
        }
    }
}

fn file_history_root_scope_snapshot(scope: &FileHistoryRootScope) -> ExplainHistoryRootScopeKind {
    match scope {
        FileHistoryRootScope::AllRoots => ExplainHistoryRootScopeKind::AllRoots,
        FileHistoryRootScope::RequestedRoots(_) => ExplainHistoryRootScopeKind::RequestedRoots,
    }
}

fn file_history_requested_roots(scope: &FileHistoryRootScope) -> Vec<String> {
    match scope {
        FileHistoryRootScope::AllRoots => Vec::new(),
        FileHistoryRootScope::RequestedRoots(roots) => roots.clone(),
    }
}

fn file_history_version_scope_snapshot(
    scope: &FileHistoryVersionScope,
) -> ExplainHistoryVersionScopeKind {
    match scope {
        FileHistoryVersionScope::Any => ExplainHistoryVersionScopeKind::Any,
        FileHistoryVersionScope::RequestedVersions(_) => {
            ExplainHistoryVersionScopeKind::RequestedVersions
        }
    }
}

fn file_history_requested_versions(scope: &FileHistoryVersionScope) -> Vec<String> {
    match scope {
        FileHistoryVersionScope::Any => Vec::new(),
        FileHistoryVersionScope::RequestedVersions(versions) => versions.clone(),
    }
}

fn file_history_lineage_scope_snapshot(
    scope: FileHistoryLineageScope,
) -> ExplainHistoryLineageScope {
    match scope {
        FileHistoryLineageScope::ActiveVersion => ExplainHistoryLineageScope::ActiveVersion,
        FileHistoryLineageScope::Standard => ExplainHistoryLineageScope::Standard,
    }
}

fn file_history_content_mode_snapshot(
    mode: FileHistoryContentMode,
) -> ExplainFileHistoryContentMode {
    match mode {
        FileHistoryContentMode::MetadataOnly => ExplainFileHistoryContentMode::MetadataOnly,
        FileHistoryContentMode::IncludeData => ExplainFileHistoryContentMode::IncludeData,
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
        ExplainStage::Routing => "routing",
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

fn join_names_or_none(names: &[String]) -> String {
    if names.is_empty() {
        return "(none)".to_string();
    }
    names.join(", ")
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
