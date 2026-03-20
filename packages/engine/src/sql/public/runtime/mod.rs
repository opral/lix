use crate::errors::schema_not_registered_error;
use crate::errors::{
    file_data_expects_bytes_error, mixed_public_internal_query_error, read_only_view_write_error,
};
use crate::filesystem::history::{DirectoryHistoryRequest, FileHistoryRequest};
use crate::schema::builtin::builtin_schema_definition;
use crate::schema::live_layout::{
    builtin_live_table_layout, live_column_name_for_property, live_table_layout_from_schema,
    untracked_live_table_name,
};
use crate::sql::analysis::state_resolution::canonical::statement_targets_table_name;
use crate::sql::ast::lowering::lower_statement;
use crate::sql::common::dependency_spec::DependencySpec;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::sql::execution::intent::authoritative_binary_blob_write_targets;
use crate::sql::execution::runtime_effects::{
    binary_blob_writes_from_filesystem_state, delete_targets_from_filesystem_state,
    FilesystemTransactionState,
};
use crate::sql::public::backend::PushdownDecision;
use crate::sql::public::catalog::{
    SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceRegistry, SurfaceVariant,
};
use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
use crate::sql::public::planner::backend::lowerer::{
    rewrite_supported_public_read_surfaces_in_statement_with_registry_and_dialect,
    summarize_bound_public_read_statement_with_registry, LoweredReadProgram, LoweredResultColumns,
};
use crate::sql::public::planner::canonicalize::{canonicalize_write, CanonicalizedWrite};
use crate::sql::public::planner::ir::{
    CommitPreconditions, PlannedWrite, ResolvedWritePlan, SchemaProof, ScopeProof,
    StructuredPublicRead, TargetSetProof, WriteCommand, WriteOperationKind,
};
use crate::sql::public::planner::semantics::dependency_spec::{
    derive_dependency_spec_from_bound_public_surface_bindings,
    derive_dependency_spec_from_structured_public_read,
};
use crate::sql::public::planner::semantics::domain_changes::{
    build_domain_change_batch, derive_commit_preconditions, DomainChangeBatch,
};
use crate::sql::public::planner::semantics::effective_state_resolver::{
    build_effective_state, EffectiveStatePlan, EffectiveStateRequest,
};
use crate::sql::public::planner::semantics::write_analysis::analyze_write;
use crate::sql::public::planner::semantics::write_resolver::resolve_write_plan;
use crate::state::commit::{
    load_committed_version_head_commit_id_from_live_state, CreateCommitExpectedHead,
    CreateCommitIdempotencyKey, CreateCommitPreconditions, CreateCommitWriteLane,
    ProposedDomainChange,
};
use crate::state::history::ensure_state_history_timeline_materialized_for_root;
use crate::state::history::StateHistoryRequest;
use crate::state::stream::{
    state_commit_stream_changes_from_domain_changes, state_commit_stream_changes_from_planned_rows,
    StateCommitStreamOperation,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, LimitClause, ObjectNamePart, OrderBy, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue, Visit, Visitor,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

#[derive(Debug, Clone, PartialEq)]
struct ExplainEnvelope {
    describe_alias: sqlparser::ast::DescribeAlias,
    analyze: bool,
    verbose: bool,
    query_plan: bool,
    estimate: bool,
    format: Option<sqlparser::ast::AnalyzeFormatKind>,
    options: Option<Vec<sqlparser::ast::UtilityOption>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PublicExecutionDebugTrace {
    pub(crate) bound_statements: Vec<BoundStatement>,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) bound_public_leaves: Vec<BoundPublicLeaf>,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
    pub(crate) pushdown_decision: Option<PushdownDecision>,
    pub(crate) write_command: Option<WriteCommand>,
    pub(crate) scope_proof: Option<ScopeProof>,
    pub(crate) schema_proof: Option<SchemaProof>,
    pub(crate) target_set_proof: Option<TargetSetProof>,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlan>,
    pub(crate) domain_change_batches: Vec<DomainChangeBatch>,
    pub(crate) commit_preconditions: Vec<CommitPreconditions>,
    pub(crate) invariant_trace: Option<PublicWriteInvariantTrace>,
    pub(crate) write_phase_trace: Vec<String>,
    pub(crate) lowered_sql: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PublicWriteInvariantTrace {
    pub(crate) batch_local_checks: Vec<String>,
    pub(crate) commit_time_checks: Vec<String>,
    pub(crate) physical_checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadOptimization {
    pub(crate) structured_read: StructuredPublicRead,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectStateHistoryField {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryProjectionValue {
    Field(DirectStateHistoryField),
    Aggregate(StateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) value: StateHistoryProjectionValue,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistorySortValue {
    Field(DirectStateHistoryField),
    Aggregate(StateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) value: Option<StateHistorySortValue>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryPredicate {
    Eq(DirectStateHistoryField, Value),
    NotEq(DirectStateHistoryField, Value),
    Gt(DirectStateHistoryField, Value),
    GtEq(DirectStateHistoryField, Value),
    Lt(DirectStateHistoryField, Value),
    LtEq(DirectStateHistoryField, Value),
    In(DirectStateHistoryField, Vec<Value>),
    IsNull(DirectStateHistoryField),
    IsNotNull(DirectStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistoryDirectReadPlan {
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<StateHistoryPredicate>,
    pub(crate) projections: Vec<StateHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) group_by_fields: Vec<DirectStateHistoryField>,
    pub(crate) having: Option<StateHistoryAggregatePredicate>,
    pub(crate) sort_keys: Vec<StateHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryAggregatePredicate {
    Eq(StateHistoryAggregate, i64),
    NotEq(StateHistoryAggregate, i64),
    Gt(StateHistoryAggregate, i64),
    GtEq(StateHistoryAggregate, i64),
    Lt(StateHistoryAggregate, i64),
    LtEq(StateHistoryAggregate, i64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectEntityHistoryField {
    Property(String),
    State(DirectStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: DirectEntityHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectEntityHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityHistoryPredicate {
    Eq(DirectEntityHistoryField, Value),
    NotEq(DirectEntityHistoryField, Value),
    Gt(DirectEntityHistoryField, Value),
    GtEq(DirectEntityHistoryField, Value),
    Lt(DirectEntityHistoryField, Value),
    LtEq(DirectEntityHistoryField, Value),
    In(DirectEntityHistoryField, Vec<Value>),
    IsNull(DirectEntityHistoryField),
    IsNotNull(DirectEntityHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistoryDirectReadPlan {
    pub(crate) surface_binding: SurfaceBinding,
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<EntityHistoryPredicate>,
    pub(crate) projections: Vec<EntityHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<EntityHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectFileHistoryField {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: DirectFileHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectFileHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryPredicate {
    Eq(DirectFileHistoryField, Value),
    NotEq(DirectFileHistoryField, Value),
    Gt(DirectFileHistoryField, Value),
    GtEq(DirectFileHistoryField, Value),
    Lt(DirectFileHistoryField, Value),
    LtEq(DirectFileHistoryField, Value),
    In(DirectFileHistoryField, Vec<Value>),
    IsNull(DirectFileHistoryField),
    IsNotNull(DirectFileHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistoryDirectReadPlan {
    pub(crate) request: FileHistoryRequest,
    pub(crate) predicates: Vec<FileHistoryPredicate>,
    pub(crate) projections: Vec<FileHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<FileHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) aggregate: Option<FileHistoryAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectDirectoryHistoryField {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: DirectDirectoryHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectDirectoryHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryPredicate {
    Eq(DirectDirectoryHistoryField, Value),
    NotEq(DirectDirectoryHistoryField, Value),
    Gt(DirectDirectoryHistoryField, Value),
    GtEq(DirectDirectoryHistoryField, Value),
    Lt(DirectDirectoryHistoryField, Value),
    LtEq(DirectDirectoryHistoryField, Value),
    In(DirectDirectoryHistoryField, Vec<Value>),
    IsNull(DirectDirectoryHistoryField),
    IsNotNull(DirectDirectoryHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistoryDirectReadPlan {
    pub(crate) request: DirectoryHistoryRequest,
    pub(crate) predicates: Vec<DirectoryHistoryPredicate>,
    pub(crate) projections: Vec<DirectoryHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<DirectoryHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) aggregate: Option<DirectoryHistoryAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectPublicReadPlan {
    StateHistory(StateHistoryDirectReadPlan),
    EntityHistory(EntityHistoryDirectReadPlan),
    FileHistory(FileHistoryDirectReadPlan),
    DirectoryHistory(DirectoryHistoryDirectReadPlan),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreparedPublicReadExecution {
    LoweredSql(LoweredReadProgram),
    Direct(DirectPublicReadPlan),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedPublicRead {
    pub(crate) optimization: Option<PublicReadOptimization>,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) execution: PreparedPublicReadExecution,
    pub(crate) debug_trace: PublicExecutionDebugTrace,
}

pub(crate) use read::{decode_public_read_result, execute_prepared_public_read};

impl PreparedPublicRead {
    pub(crate) fn structured_read(&self) -> Option<&StructuredPublicRead> {
        self.optimization
            .as_ref()
            .map(|optimization| &optimization.structured_read)
    }

    pub(crate) fn effective_state_request(&self) -> Option<&EffectiveStateRequest> {
        self.optimization
            .as_ref()
            .and_then(|optimization| optimization.effective_state_request.as_ref())
    }

    pub(crate) fn effective_state_plan(&self) -> Option<&EffectiveStatePlan> {
        self.optimization
            .as_ref()
            .and_then(|optimization| optimization.effective_state_plan.as_ref())
    }

    pub(crate) fn lowered_read(&self) -> Option<&LoweredReadProgram> {
        match &self.execution {
            PreparedPublicReadExecution::LoweredSql(lowered) => Some(lowered),
            PreparedPublicReadExecution::Direct(_) => None,
        }
    }

    pub(crate) fn direct_plan(&self) -> Option<&DirectPublicReadPlan> {
        match &self.execution {
            PreparedPublicReadExecution::LoweredSql(_) => None,
            PreparedPublicReadExecution::Direct(plan) => Some(plan),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundPublicLeaf {
    pub(crate) public_name: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) capability: SurfaceCapability,
    pub(crate) requires_effective_state: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedPublicWrite {
    pub(crate) canonicalized: CanonicalizedWrite,
    pub(crate) planned_write: PlannedWrite,
    pub(crate) domain_change_batches: Vec<DomainChangeBatch>,
    pub(crate) execution: Option<PublicWriteExecution>,
    pub(crate) debug_trace: PublicExecutionDebugTrace,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicSurfaceRegistryMutation {
    UpsertRegisteredSchemaSnapshot { snapshot: JsonValue },
    RemoveDynamicSchema { schema_key: String },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicWriteExecution {
    pub(crate) partitions: Vec<PublicWriteExecutionPartition>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicWriteExecutionPartition {
    Tracked(TrackedWriteExecution),
    Untracked(UntrackedWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TrackedWriteExecution {
    pub(crate) schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) create_preconditions: CreateCommitPreconditions,
    pub(crate) semantic_effects: PlanEffects,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UntrackedWriteExecution {
    pub(crate) intended_post_state: Vec<crate::sql::public::planner::ir::PlannedStateRow>,
    pub(crate) semantic_effects: PlanEffects,
    pub(crate) persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreparedPublicExecution {
    Read(PreparedPublicRead),
    Write(PreparedPublicWrite),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicExecutionRoute {
    Read,
    Write,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BoundPublicReadSummary {
    bound_surface_bindings: Vec<crate::sql::public::catalog::SurfaceBinding>,
    internal_relations: Vec<String>,
    external_relations: Vec<String>,
    requested_history_root_commit_ids: Vec<String>,
}

mod bind;
mod read;
mod tracked_write_plan;

pub(crate) use tracked_write_plan::{build_tracked_write_txn_plan, TrackedWriteTxnPlan};

pub(crate) async fn prepare_public_execution(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    prepare_public_execution_with_internal_access(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        false,
    )
    .await
}

pub(crate) async fn prepare_public_execution_with_registry_and_internal_access(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    let Some(route) = classify_public_execution_route_with_registry(registry, parsed_statements)
    else {
        return Ok(None);
    };

    match route {
        PublicExecutionRoute::Write => {
            let target_name = public_write_target_name(registry, parsed_statements)
                .expect("public write route must expose a target name");
            let prepared = try_prepare_public_write_with_registry(
                backend,
                registry,
                parsed_statements,
                params,
                active_version_id,
                writer_key,
            )
            .await?;
            prepared
                .map(PreparedPublicExecution::Write)
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public write target '{target_name}' must route through public lowering"
                        ),
                    )
                })
                .map(Some)
        }
        PublicExecutionRoute::Read => {
            if parsed_statements.len() != 1 {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "public read statement batches must route through public lowering one statement at a time",
                ));
            }

            read::try_prepare_public_read_with_registry_and_internal_access(
                backend,
                registry,
                parsed_statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await?
            .map(PreparedPublicExecution::Read)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "public read statements must route through public lowering",
                )
            })
            .map(Some)
        }
    }
}

pub(crate) async fn prepare_public_execution_with_internal_access(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    prepare_public_execution_with_registry_and_internal_access(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
    )
    .await
}

pub(crate) async fn classify_public_execution_route_with_backend(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
) -> Result<Option<PublicExecutionRoute>, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    Ok(classify_public_execution_route_with_registry(
        &registry,
        parsed_statements,
    ))
}

pub(crate) fn statement_references_public_surface_with_builtin_registry(
    statement: &Statement,
) -> bool {
    statement_references_public_surface(&SurfaceRegistry::with_builtin_surfaces(), statement)
}

pub(crate) async fn statement_references_public_surface_with_backend(
    backend: &dyn LixBackend,
    statement: &Statement,
) -> bool {
    let registry = match SurfaceRegistry::bootstrap_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => return statement_references_public_surface_with_builtin_registry(statement),
    };
    statement_references_public_surface(&registry, statement)
}

pub(crate) fn rewrite_public_read_statement_to_lowered_sql(
    statement: &mut Statement,
    dialect: crate::SqlDialect,
) -> Result<Statement, LixError> {
    rewrite_public_read_statement_to_lowered_sql_with_registry(
        statement,
        dialect,
        &SurfaceRegistry::with_builtin_surfaces(),
    )
}

fn rewrite_public_read_statement_to_lowered_sql_with_registry(
    statement: &mut Statement,
    dialect: crate::SqlDialect,
    registry: &SurfaceRegistry,
) -> Result<Statement, LixError> {
    rewrite_supported_public_read_surfaces_in_statement_with_registry_and_dialect(
        statement, registry, dialect,
    )?;
    lower_statement(statement.clone(), dialect)
}

pub(crate) fn rewrite_public_read_query_to_lowered_sql(
    query: Query,
    dialect: crate::SqlDialect,
) -> Result<Query, LixError> {
    rewrite_public_read_query_to_lowered_sql_with_registry(
        query,
        dialect,
        &SurfaceRegistry::with_builtin_surfaces(),
    )
}

fn rewrite_public_read_query_to_lowered_sql_with_registry(
    query: Query,
    dialect: crate::SqlDialect,
    registry: &SurfaceRegistry,
) -> Result<Query, LixError> {
    let mut statement = Statement::Query(Box::new(query));
    match rewrite_public_read_statement_to_lowered_sql_with_registry(
        &mut statement,
        dialect,
        registry,
    )? {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "expected lowered read query to remain a SELECT query",
        )),
    }
}

pub(crate) async fn lower_public_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<read::LoweredPublicReadQuery, LixError> {
    read::lower_public_read_query_with_backend(backend, query, params).await
}

async fn try_prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicRead>, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    read::try_prepare_public_read_with_registry_and_internal_access(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
    )
    .await
}

fn public_read_preflight_error(
    registry: &SurfaceRegistry,
    statement: &Statement,
) -> Option<LixError> {
    let Statement::Query(query) = statement else {
        return None;
    };
    let referenced_surfaces = collect_public_query_relation_names(query);
    if !referenced_surfaces.contains("lix_state")
        || !query_references_any_column(query, &["version_id", "lixcol_version_id"])
    {
        return None;
    }
    let other_version_exposing_surface_present = referenced_surfaces.iter().any(|surface_name| {
        !surface_name.eq_ignore_ascii_case("lix_state")
            && registry
                .bind_relation_name(surface_name)
                .is_some_and(|binding| {
                    binding
                        .exposed_columns
                        .iter()
                        .any(|column| matches!(column.as_str(), "version_id" | "lixcol_version_id"))
                })
    });
    if other_version_exposing_surface_present {
        return None;
    }
    Some(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "lix_state does not expose version_id; use lix_state_by_version for explicit version filters",
    ))
}

fn query_references_named_surface(query: &Query, surface_name: &str) -> bool {
    collect_public_query_relation_names(query)
        .into_iter()
        .any(|name| name.eq_ignore_ascii_case(surface_name))
}

fn query_references_any_column(query: &Query, columns: &[&str]) -> bool {
    struct Collector<'a> {
        columns: &'a [&'a str],
        matched: bool,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            let matches_identifier = |value: &str| {
                self.columns
                    .iter()
                    .any(|column| value.eq_ignore_ascii_case(column))
            };
            match expr {
                Expr::Identifier(identifier) if matches_identifier(&identifier.value) => {
                    self.matched = true;
                    ControlFlow::Break(())
                }
                Expr::CompoundIdentifier(parts)
                    if parts
                        .last()
                        .is_some_and(|identifier| matches_identifier(&identifier.value)) =>
                {
                    self.matched = true;
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }

    let mut collector = Collector {
        columns,
        matched: false,
    };
    let _ = query.visit(&mut collector);
    collector.matched
}

pub(crate) async fn prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<PreparedPublicRead> {
    read::prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}

pub(crate) async fn prepare_public_read_strict(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    read::prepare_public_read_strict(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}

pub(crate) async fn execute_public_read_query_strict(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    read::execute_public_read_query_strict(backend, query, params).await
}

fn statements_reference_public_surface(
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
) -> bool {
    parsed_statements
        .iter()
        .any(|statement| statement_references_public_surface(registry, statement))
}

pub(crate) fn classify_public_execution_route_with_registry(
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
) -> Option<PublicExecutionRoute> {
    if !statements_reference_public_surface(registry, parsed_statements) {
        return None;
    }
    if public_write_target_name(registry, parsed_statements).is_some() {
        return Some(PublicExecutionRoute::Write);
    }
    Some(PublicExecutionRoute::Read)
}

fn statement_references_public_surface(registry: &SurfaceRegistry, statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => query_references_public_surface(registry, query),
        Statement::Explain { statement, .. } => {
            statement_references_public_surface(registry, statement.as_ref())
        }
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            top_level_write_target_name(statement)
                .and_then(|name| registry.bind_relation_name(&name))
                .is_some()
        }
        _ => false,
    }
}

fn query_references_public_surface(registry: &SurfaceRegistry, query: &Query) -> bool {
    collect_public_query_relation_names(query)
        .into_iter()
        .any(|name| registry.bind_relation_name(&name).is_some())
}

fn summarize_bound_public_read_statement(
    registry: &SurfaceRegistry,
    statement: &Statement,
) -> BoundPublicReadSummary {
    let Statement::Query(query) = statement else {
        return BoundPublicReadSummary::default();
    };
    let Ok(Some(summary)) =
        summarize_bound_public_read_statement_with_registry(statement, registry)
    else {
        return BoundPublicReadSummary::default();
    };
    BoundPublicReadSummary {
        bound_surface_bindings: summary
            .public_relations
            .into_iter()
            .filter_map(|relation_name| registry.bind_relation_name(&relation_name))
            .collect(),
        internal_relations: summary.internal_relations.into_iter().collect(),
        external_relations: summary.external_relations.into_iter().collect(),
        requested_history_root_commit_ids: requested_history_root_commit_ids_from_selection(
            query_selection(query),
        ),
    }
}

fn query_selection(query: &Query) -> Option<&Expr> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    select.selection.as_ref()
}

fn bound_public_leaf(binding: &crate::sql::public::catalog::SurfaceBinding) -> BoundPublicLeaf {
    BoundPublicLeaf {
        public_name: binding.descriptor.public_name.clone(),
        surface_family: binding.descriptor.surface_family,
        surface_variant: binding.descriptor.surface_variant,
        capability: binding.capability,
        requires_effective_state: matches!(
            binding.descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity
        ),
    }
}

fn collect_public_query_relation_names(query: &Query) -> BTreeSet<String> {
    let mut relation_names = BTreeSet::new();
    collect_public_query_relation_names_scoped(query, &BTreeSet::new(), &mut relation_names);
    relation_names
}

fn collect_public_query_relation_names_scoped(
    query: &Query,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    let mut scoped_ctes = visible_ctes.clone();
    if let Some(with) = &query.with {
        let mut cte_scope = visible_ctes.clone();
        for cte in &with.cte_tables {
            collect_public_query_relation_names_scoped(&cte.query, &cte_scope, out);
            cte_scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        scoped_ctes = cte_scope;
    }

    collect_public_query_relation_names_in_set_expr(query.body.as_ref(), &scoped_ctes, out);
    if let Some(order_by) = &query.order_by {
        collect_public_query_relation_names_in_order_by(order_by, &scoped_ctes, out);
    }
    if let Some(limit_clause) = &query.limit_clause {
        collect_public_query_relation_names_in_limit_clause(limit_clause, &scoped_ctes, out);
    }
    if let Some(quantity) = query
        .fetch
        .as_ref()
        .and_then(|fetch| fetch.quantity.as_ref())
    {
        collect_public_query_relation_names_in_expr(quantity, &scoped_ctes, out);
    }
}

fn collect_public_query_relation_names_in_set_expr(
    expr: &SetExpr,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match expr {
        SetExpr::Select(select) => {
            collect_public_query_relation_names_in_select(select, visible_ctes, out)
        }
        SetExpr::Query(query) => {
            collect_public_query_relation_names_scoped(query, visible_ctes, out)
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_public_query_relation_names_in_set_expr(left.as_ref(), visible_ctes, out);
            collect_public_query_relation_names_in_set_expr(right.as_ref(), visible_ctes, out);
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
                }
            }
        }
        SetExpr::Insert(statement)
        | SetExpr::Update(statement)
        | SetExpr::Delete(statement)
        | SetExpr::Merge(statement) => {
            let _ = statement.visit(&mut PublicRelationCollectorVisitor { visible_ctes, out });
        }
        SetExpr::Table(table) => {
            if let Some(table_name) = &table.table_name {
                let normalized = table_name.to_ascii_lowercase();
                if !visible_ctes.contains(&normalized) {
                    out.insert(normalized);
                }
            }
        }
    }
}

fn collect_public_query_relation_names_in_select(
    select: &Select,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    for table in &select.from {
        collect_public_query_relation_names_in_table_with_joins(table, visible_ctes, out);
    }
    if let Some(prewhere) = &select.prewhere {
        collect_public_query_relation_names_in_expr(prewhere, visible_ctes, out);
    }
    if let Some(selection) = &select.selection {
        collect_public_query_relation_names_in_expr(selection, visible_ctes, out);
    }
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => collect_public_query_relation_names_in_expr(expr, visible_ctes, out),
            _ => {}
        }
    }
    collect_public_query_relation_names_in_group_by(&select.group_by, visible_ctes, out);
    for expr in &select.cluster_by {
        collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
    }
    for expr in &select.distribute_by {
        collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
    }
    for expr in &select.sort_by {
        collect_public_query_relation_names_in_order_by_expr(expr, visible_ctes, out);
    }
    if let Some(having) = &select.having {
        collect_public_query_relation_names_in_expr(having, visible_ctes, out);
    }
    if let Some(qualify) = &select.qualify {
        collect_public_query_relation_names_in_expr(qualify, visible_ctes, out);
    }
    if let Some(connect_by) = &select.connect_by {
        collect_public_query_relation_names_in_expr(&connect_by.condition, visible_ctes, out);
        for expr in &connect_by.relationships {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
        }
    }
}

fn collect_public_query_relation_names_in_table_with_joins(
    table: &TableWithJoins,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    collect_public_query_relation_names_in_table_factor(&table.relation, visible_ctes, out);
    for join in &table.joins {
        collect_public_query_relation_names_in_table_factor(&join.relation, visible_ctes, out);
        collect_public_query_relation_names_in_join_operator(
            &join.join_operator,
            visible_ctes,
            out,
        );
    }
}

fn collect_public_query_relation_names_in_table_factor(
    relation: &TableFactor,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match relation {
        TableFactor::Table { name, .. } => {
            if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                let normalized = identifier.value.to_ascii_lowercase();
                if !visible_ctes.contains(&normalized) {
                    out.insert(normalized);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_public_query_relation_names_scoped(subquery, visible_ctes, out);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_public_query_relation_names_in_table_with_joins(
            table_with_joins,
            visible_ctes,
            out,
        ),
        _ => {}
    }
}

fn collect_public_query_relation_names_in_group_by(
    group_by: &GroupByExpr,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(expressions, _) => {
            for expr in expressions {
                collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            }
        }
    }
}

fn collect_public_query_relation_names_in_order_by(
    order_by: &OrderBy,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match &order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => {}
        sqlparser::ast::OrderByKind::Expressions(expressions) => {
            for expr in expressions {
                collect_public_query_relation_names_in_order_by_expr(expr, visible_ctes, out);
            }
        }
    }
}

fn collect_public_query_relation_names_in_order_by_expr(
    order_by_expr: &OrderByExpr,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    collect_public_query_relation_names_in_expr(&order_by_expr.expr, visible_ctes, out);
    if let Some(with_fill) = &order_by_expr.with_fill {
        if let Some(from) = &with_fill.from {
            collect_public_query_relation_names_in_expr(from, visible_ctes, out);
        }
        if let Some(to) = &with_fill.to {
            collect_public_query_relation_names_in_expr(to, visible_ctes, out);
        }
        if let Some(step) = &with_fill.step {
            collect_public_query_relation_names_in_expr(step, visible_ctes, out);
        }
    }
}

fn collect_public_query_relation_names_in_limit_clause(
    limit_clause: &LimitClause,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                collect_public_query_relation_names_in_expr(limit, visible_ctes, out);
            }
            if let Some(offset) = offset {
                collect_public_query_relation_names_in_expr(&offset.value, visible_ctes, out);
            }
            for expr in limit_by {
                collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            }
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            collect_public_query_relation_names_in_expr(offset, visible_ctes, out);
            collect_public_query_relation_names_in_expr(limit, visible_ctes, out);
        }
    }
}

fn collect_public_query_relation_names_in_join_operator(
    join_operator: &JoinOperator,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    let (match_condition, constraint) = match join_operator {
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        JoinOperator::CrossApply | JoinOperator::OuterApply => (None, None),
    };
    if let Some(match_condition) = match_condition {
        collect_public_query_relation_names_in_expr(match_condition, visible_ctes, out);
    }
    if let Some(constraint) = constraint {
        collect_public_query_relation_names_in_join_constraint(constraint, visible_ctes, out);
    }
}

fn collect_public_query_relation_names_in_join_constraint(
    constraint: &JoinConstraint,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    if let JoinConstraint::On(expr) = constraint {
        collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
    }
}

fn collect_public_query_relation_names_in_expr(
    expr: &Expr,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_public_query_relation_names_in_expr(left, visible_ctes, out);
            collect_public_query_relation_names_in_expr(right, visible_ctes, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out)
        }
        Expr::InList { expr, list, .. } => {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            for item in list {
                collect_public_query_relation_names_in_expr(item, visible_ctes, out);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            collect_public_query_relation_names_scoped(subquery, visible_ctes, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            collect_public_query_relation_names_in_expr(low, visible_ctes, out);
            collect_public_query_relation_names_in_expr(high, visible_ctes, out);
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            collect_public_query_relation_names_in_expr(pattern, visible_ctes, out);
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
            collect_public_query_relation_names_in_expr(array_expr, visible_ctes, out);
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            collect_public_query_relation_names_in_expr(left, visible_ctes, out);
            collect_public_query_relation_names_in_expr(right, visible_ctes, out);
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            collect_public_query_relation_names_scoped(subquery, visible_ctes, out);
        }
        Expr::Function(function) => {
            collect_public_query_relation_names_in_function_args(&function.args, visible_ctes, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_public_query_relation_names_in_expr(operand, visible_ctes, out);
            }
            for condition in conditions {
                collect_public_query_relation_names_in_expr(
                    &condition.condition,
                    visible_ctes,
                    out,
                );
                collect_public_query_relation_names_in_expr(&condition.result, visible_ctes, out);
            }
            if let Some(else_result) = else_result {
                collect_public_query_relation_names_in_expr(else_result, visible_ctes, out);
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                collect_public_query_relation_names_in_expr(item, visible_ctes, out);
            }
        }
        _ => {}
    }
}

fn collect_public_query_relation_names_in_function_args(
    args: &FunctionArguments,
    visible_ctes: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    if let FunctionArguments::List(list) = args {
        for arg in &list.args {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
                }
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                    if let FunctionArgExpr::Expr(expr) = arg {
                        collect_public_query_relation_names_in_expr(expr, visible_ctes, out);
                    }
                }
                _ => {}
            }
        }
    }
}

struct PublicRelationCollectorVisitor<'a> {
    visible_ctes: &'a BTreeSet<String>,
    out: &'a mut BTreeSet<String>,
}

impl Visitor for PublicRelationCollectorVisitor<'_> {
    type Break = ();

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, .. } = table_factor {
            if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                let normalized = identifier.value.to_ascii_lowercase();
                if !self.visible_ctes.contains(&normalized) {
                    self.out.insert(normalized);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

async fn load_active_version_id_for_public_read(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    let layout = builtin_live_table_layout(active_version_schema_key())?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "builtin active version schema must compile to a live layout",
        )
    })?;
    let payload_version_column =
        live_column_name_for_property(&layout, "version_id").ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "active version live layout is missing version_id",
            )
        })?;
    let result = backend
        .execute(
            &format!(
                "SELECT {payload_version_column} \
                 FROM {} \
                 WHERE file_id = $1 \
                   AND version_id = $2 \
                   AND {payload_version_column} IS NOT NULL \
                 ORDER BY updated_at DESC \
                 LIMIT 1",
                untracked_live_table_name(crate::version::active_version_schema_key())
            ),
            &[
                Value::Text(crate::version::active_version_file_id().to_string()),
                Value::Text(crate::version::active_version_storage_version_id().to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(crate::version::DEFAULT_ACTIVE_VERSION_NAME.to_string());
    };
    let version_id = row.first().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "active version query row is missing version_id",
        )
    })?;
    match version_id {
        Value::Text(value) => Ok(value.clone()),
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("active version id must be text, got {other:?}"),
        )),
    }
}

async fn maybe_bind_active_history_root(
    backend: &dyn LixBackend,
    mut structured_read: StructuredPublicRead,
    active_version_id: &str,
) -> Option<StructuredPublicRead> {
    let descriptor = &structured_read.surface_binding.descriptor;
    let public_name = descriptor.public_name.as_str();
    let uses_active_history_root = descriptor.surface_variant == SurfaceVariant::History
        && matches!(
            descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
        )
        && !public_name.ends_with("_history_by_version");
    if !uses_active_history_root {
        return Some(structured_read);
    }
    if structured_read_has_root_commit_predicate(&structured_read) {
        return Some(structured_read);
    }

    let mut executor = backend;
    let root_commit_id =
        load_committed_version_head_commit_id_from_live_state(&mut executor, active_version_id)
            .await
            .ok()??;
    let root_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("lixcol_root_commit_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            SqlValue::SingleQuotedString(root_commit_id).into(),
        )),
    };

    structured_read.query.selection = Some(match structured_read.query.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(root_predicate.clone()),
        },
        None => root_predicate.clone(),
    });
    structured_read
        .query
        .selection_predicates
        .push(root_predicate);
    Some(structured_read)
}

fn structured_read_has_root_commit_predicate(structured_read: &StructuredPublicRead) -> bool {
    structured_read
        .query
        .selection_predicates
        .iter()
        .any(expr_has_root_commit_predicate)
}

fn expr_has_root_commit_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_references_root_commit(left)
                || expr_references_root_commit(right)
                || expr_has_root_commit_predicate(left)
                || expr_has_root_commit_predicate(right)
        }
        Expr::Nested(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::UnaryOp { expr: inner, .. } => expr_has_root_commit_predicate(inner),
        Expr::InList { expr, .. } | Expr::InSubquery { expr, .. } => {
            expr_references_root_commit(expr) || expr_has_root_commit_predicate(expr)
        }
        _ => false,
    }
}

async fn ensure_public_read_history_timeline_roots(
    backend: &dyn LixBackend,
    root_commit_ids: &[String],
) -> Result<(), LixError> {
    for root_commit_id in root_commit_ids {
        ensure_state_history_timeline_materialized_for_root(backend, &root_commit_id, 512).await?;
    }
    Ok(())
}

fn requested_history_root_commit_ids_from_selection(selection: Option<&Expr>) -> Vec<String> {
    let mut roots = std::collections::BTreeSet::new();
    if let Some(selection) = selection {
        collect_history_root_commit_ids(selection, &mut roots);
    }
    roots.into_iter().collect()
}

fn collect_history_root_commit_ids(expr: &Expr, roots: &mut std::collections::BTreeSet<String>) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::And {
                collect_history_root_commit_ids(left, roots);
                collect_history_root_commit_ids(right, roots);
                return;
            }
            if *op == BinaryOperator::Eq {
                if history_root_identifier(left).is_some() {
                    if let Some(value) = expr_string_literal(right) {
                        roots.insert(value.to_string());
                    }
                } else if history_root_identifier(right).is_some() {
                    if let Some(value) = expr_string_literal(left) {
                        roots.insert(value.to_string());
                    }
                }
            }
        }
        Expr::Nested(inner) => collect_history_root_commit_ids(inner, roots),
        Expr::InList {
            expr,
            list,
            negated: false,
        } if history_root_identifier(expr).is_some() => {
            for item in list {
                if let Some(value) = expr_string_literal(item) {
                    roots.insert(value.to_string());
                }
            }
        }
        _ => {}
    }
}

fn history_root_identifier(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
    .filter(|name| {
        name.eq_ignore_ascii_case("root_commit_id")
            || name.eq_ignore_ascii_case("lixcol_root_commit_id")
    })
}

fn expr_string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(value) => match value.value {
            SqlValue::SingleQuotedString(ref inner) => Some(inner.as_str()),
            _ => None,
        },
        _ => None,
    }
}

fn expr_references_root_commit(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(identifier) => {
            matches!(
                identifier.value.to_ascii_lowercase().as_str(),
                "lixcol_root_commit_id" | "root_commit_id"
            )
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => matches!(
            parts[1].value.to_ascii_lowercase().as_str(),
            "lixcol_root_commit_id" | "root_commit_id"
        ),
        Expr::Nested(inner) => expr_references_root_commit(inner),
        _ => false,
    }
}

fn dependency_spec_has_unknown_schema_keys(
    registry: &SurfaceRegistry,
    dependency_spec: Option<&DependencySpec>,
) -> bool {
    let Some(dependency_spec) = dependency_spec else {
        return false;
    };
    if dependency_spec.schema_keys.is_empty() {
        return false;
    }
    let registered = registry
        .registered_schema_keys()
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    dependency_spec
        .schema_keys
        .iter()
        .any(|schema_key| !registered.contains(schema_key))
}

fn unknown_public_state_schema_error(
    registry: &SurfaceRegistry,
    dependency_spec: Option<&DependencySpec>,
) -> Option<LixError> {
    if !dependency_spec_has_unknown_schema_keys(registry, dependency_spec) {
        return None;
    }
    let dependency_spec = dependency_spec?;
    let registered = registry.registered_state_backed_schema_keys();
    let available_refs = registered.iter().map(String::as_str).collect::<Vec<_>>();
    let unknown = dependency_spec.schema_keys.iter().find(|schema_key| {
        !registered
            .iter()
            .any(|registered| registered == *schema_key)
    })?;
    Some(schema_not_registered_error(unknown, &available_refs))
}

fn augment_dependency_spec_for_public_read(
    registry: &SurfaceRegistry,
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<DependencySpec>,
) -> Option<DependencySpec> {
    let dependency_spec = dependency_spec?;
    augment_dependency_spec_for_broad_public_read(registry, Some(dependency_spec)).map(
        |mut dependency_spec| {
            let has_state_schema_keys = dependency_spec
                .schema_keys
                .iter()
                .any(|schema_key| schema_key != "lix_active_version");
            if structured_read.surface_binding.descriptor.surface_family == SurfaceFamily::State
                && !has_state_schema_keys
            {
                dependency_spec.schema_keys = registry
                    .registered_state_backed_schema_keys()
                    .into_iter()
                    .collect();
            }
            dependency_spec
        },
    )
}

fn augment_dependency_spec_for_broad_public_read(
    registry: &SurfaceRegistry,
    dependency_spec: Option<DependencySpec>,
) -> Option<DependencySpec> {
    let mut dependency_spec = dependency_spec?;
    let references_state_like_surface = dependency_spec.relations.iter().any(|relation| {
        registry
            .bind_relation_name(relation)
            .is_some_and(|binding| {
                matches!(
                    binding.descriptor.surface_family,
                    SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
                )
            })
    });
    let has_state_schema_keys = dependency_spec
        .schema_keys
        .iter()
        .any(|schema_key| schema_key != "lix_active_version");
    if references_state_like_surface && !has_state_schema_keys {
        dependency_spec
            .schema_keys
            .extend(registry.registered_state_backed_schema_keys());
    }
    Some(dependency_spec)
}

fn explain_query_statement(statement: &Statement) -> Option<(Statement, Option<ExplainEnvelope>)> {
    match statement {
        Statement::Query(_) => Some((statement.clone(), None)),
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => match statement.as_ref() {
            Statement::Query(_) => Some((
                statement.as_ref().clone(),
                Some(ExplainEnvelope {
                    describe_alias: describe_alias.clone(),
                    analyze: *analyze,
                    verbose: *verbose,
                    query_plan: *query_plan,
                    estimate: *estimate,
                    format: format.clone(),
                    options: options.clone(),
                }),
            )),
            _ => None,
        },
        _ => None,
    }
}

fn wrap_lowered_read_for_explain(
    program: LoweredReadProgram,
    envelope: Option<&ExplainEnvelope>,
) -> LoweredReadProgram {
    let Some(envelope) = envelope else {
        return program;
    };

    LoweredReadProgram {
        statements: program
            .statements
            .into_iter()
            .map(|statement| Statement::Explain {
                describe_alias: envelope.describe_alias,
                analyze: envelope.analyze,
                verbose: envelope.verbose,
                query_plan: envelope.query_plan,
                estimate: envelope.estimate,
                statement: Box::new(statement),
                format: envelope.format.clone(),
                options: envelope.options.clone(),
            })
            .collect(),
        pushdown_decision: program.pushdown_decision,
        result_columns: program.result_columns,
    }
}

pub(crate) async fn try_prepare_public_write(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicWrite>, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(None);
    }

    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    try_prepare_public_write_with_registry(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}

pub(crate) async fn try_prepare_public_write_with_registry(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicWrite>, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(None);
    }

    let statement = parsed_statements[0].clone();
    let bound_statement = BoundStatement::from_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
        },
    );
    let filesystem_target_name =
        top_level_filesystem_write_target_name(&bound_statement.statement).map(str::to_string);
    let canonicalized = match canonicalize_write(bound_statement.clone(), &registry) {
        Ok(canonicalized) => canonicalized,
        Err(error) => {
            if let Some(binding) = top_level_write_target_name(&bound_statement.statement)
                .and_then(|name| registry.bind_relation_name(&name))
            {
                if let Some(operation_kind) =
                    statement_write_operation_kind(&bound_statement.statement)
                {
                    if let Some(error) = public_write_preparation_error_for_surface(
                        &binding,
                        operation_kind,
                        &error.message,
                    ) {
                        return Err(error);
                    }
                }
            }
            match filesystem_target_name.as_deref() {
                Some(target_name) => {
                    return Err(public_filesystem_write_error(target_name, &error.message));
                }
                None => return Ok(None),
            }
        }
    };
    let mut planned_write = match analyze_write(&canonicalized) {
        Ok(planned_write) => planned_write,
        Err(error) => {
            if let Some(error) = public_write_preparation_error(&canonicalized, &error.message) {
                return Err(error);
            }
            return Ok(None);
        }
    };
    let resolved_write_plan = match resolve_write_plan(backend, &planned_write).await {
        Ok(resolved_write_plan) => resolved_write_plan,
        Err(error) => match public_authoritative_write_error(&canonicalized, error.message) {
            Some(error) => return Err(error),
            None => return Ok(None),
        },
    };
    planned_write.resolved_write_plan = Some(resolved_write_plan.clone());
    let domain_change_batches = match build_domain_change_batch(&planned_write) {
        Ok(domain_change_batches) => domain_change_batches,
        Err(error) => {
            if let Some(error) = public_write_preparation_error(&canonicalized, &error.message) {
                return Err(error);
            }
            return Ok(None);
        }
    };
    let commit_preconditions = match derive_commit_preconditions(backend, &planned_write).await {
        Ok(commit_preconditions) => commit_preconditions,
        Err(error) => {
            if let Some(error) = public_write_preparation_error(&canonicalized, &error.message) {
                return Err(error);
            }
            return Ok(None);
        }
    };
    planned_write.commit_preconditions = commit_preconditions.clone();
    let invariant_trace = Some(build_public_write_invariant_trace(&planned_write));
    let execution = build_public_write_execution(
        &planned_write,
        &domain_change_batches,
        &commit_preconditions,
    )?;

    Ok(Some(PreparedPublicWrite {
        debug_trace: PublicExecutionDebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            bound_public_leaves: vec![bound_public_leaf(&canonicalized.surface_binding)],
            dependency_spec: None,
            effective_state_request: None,
            effective_state_plan: None,
            pushdown_decision: None,
            write_command: Some(canonicalized.write_command.clone()),
            scope_proof: Some(planned_write.scope_proof.clone()),
            schema_proof: Some(planned_write.schema_proof.clone()),
            target_set_proof: planned_write.target_set_proof.clone(),
            resolved_write_plan: Some(resolved_write_plan),
            domain_change_batches: domain_change_batches.clone(),
            commit_preconditions: commit_preconditions.clone(),
            invariant_trace,
            write_phase_trace: public_write_phase_trace(),
            lowered_sql: Vec::new(),
        },
        planned_write,
        domain_change_batches,
        execution,
        canonicalized,
    }))
}

pub(crate) async fn prepare_public_write(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<PreparedPublicWrite> {
    try_prepare_public_write(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
    .ok()
    .flatten()
}

fn build_public_write_execution(
    planned_write: &PlannedWrite,
    domain_change_batches: &[DomainChangeBatch],
    commit_preconditions: &[CommitPreconditions],
) -> Result<Option<PublicWriteExecution>, LixError> {
    let Some(resolved) = planned_write.resolved_write_plan.as_ref() else {
        return Ok(None);
    };
    let mut tracked_batches = domain_change_batches.iter();
    let mut tracked_preconditions = commit_preconditions.iter();
    let mut partitions = Vec::new();
    let mut filesystem_payloads_persisted = false;

    for partition in &resolved.partitions {
        let persist_filesystem_payloads_before_write = !filesystem_payloads_persisted
            && public_write_persists_filesystem_payloads(planned_write, partition);
        filesystem_payloads_persisted |= persist_filesystem_payloads_before_write;

        match partition.execution_mode {
            crate::sql::public::planner::ir::WriteMode::Tracked => {
                let Some(commit_preconditions) = tracked_preconditions.next() else {
                    return Ok(None);
                };
                if !tracked_public_write_operation_supported(planned_write, partition) {
                    return Ok(None);
                }

                let Some(domain_change_batch) = tracked_batches.next().cloned() else {
                    return Ok(None);
                };

                partitions.push(PublicWriteExecutionPartition::Tracked(
                    TrackedWriteExecution {
                        schema_live_table_requirements:
                            schema_live_table_requirements_from_partition(partition),
                        create_preconditions: create_commit_preconditions_for_public_write(
                            planned_write,
                            Some(&domain_change_batch),
                            commit_preconditions,
                        )?,
                        semantic_effects: semantic_plan_effects_from_domain_changes(
                            &domain_change_batch.changes,
                            state_commit_stream_operation(planned_write.command.operation_kind),
                        )?,
                        domain_change_batch: Some(domain_change_batch),
                    },
                ));
            }
            crate::sql::public::planner::ir::WriteMode::Untracked => {
                if !public_untracked_operation_supported(planned_write) {
                    return Ok(None);
                }
                partitions.push(PublicWriteExecutionPartition::Untracked(
                    UntrackedWriteExecution {
                        intended_post_state: partition.intended_post_state.clone(),
                        semantic_effects: PlanEffects::default(),
                        persist_filesystem_payloads_before_write,
                    },
                ));
            }
        }
    }

    if tracked_batches.next().is_some() || tracked_preconditions.next().is_some() {
        return Ok(None);
    }

    Ok(Some(PublicWriteExecution { partitions }))
}

pub(crate) fn finalize_public_write_execution(
    execution: &mut PublicWriteExecution,
    planned_write: &PlannedWrite,
    filesystem_state: &FilesystemTransactionState,
) -> Result<(), LixError> {
    for partition in &mut execution.partitions {
        let PublicWriteExecutionPartition::Untracked(untracked) = partition else {
            continue;
        };
        untracked.semantic_effects = semantic_plan_effects_from_untracked_public_write(
            planned_write,
            &untracked.intended_post_state,
            filesystem_state,
        )?;
    }
    Ok(())
}

fn schema_live_table_requirements_from_partition(
    partition: &crate::sql::public::planner::ir::ResolvedWritePartition,
) -> Vec<SchemaLiveTableRequirement> {
    let mut requirements = BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for row in &partition.intended_post_state {
        if row.schema_key != "lix_registered_schema" {
            requirements
                .entry(row.schema_key.clone())
                .or_insert(SchemaLiveTableRequirement {
                    schema_key: row.schema_key.clone(),
                    layout: None,
                });
        }

        if row.schema_key != "lix_registered_schema" || row.tombstone {
            continue;
        }

        let Some(snapshot_content) = planned_row_optional_json_text_value(row, "snapshot_content")
        else {
            continue;
        };
        let Ok(snapshot) = serde_json::from_str(&snapshot_content) else {
            continue;
        };
        let Ok((schema_key, schema)) = crate::schema::schema_from_registered_snapshot(&snapshot)
        else {
            continue;
        };
        let Ok(layout) = live_table_layout_from_schema(&schema) else {
            continue;
        };
        requirements.insert(
            schema_key.schema_key.clone(),
            SchemaLiveTableRequirement {
                schema_key: schema_key.schema_key,
                layout: Some(layout),
            },
        );
    }

    requirements
        .into_values()
        .filter(|requirement| builtin_schema_definition(&requirement.schema_key).is_none())
        .collect()
}

pub(crate) fn public_surface_registry_mutations(
    prepared: &PreparedPublicWrite,
) -> Result<Vec<PublicSurfaceRegistryMutation>, LixError> {
    let Some(resolved) = prepared.planned_write.resolved_write_plan.as_ref() else {
        return Ok(Vec::new());
    };

    let mut mutations = Vec::new();
    for row in resolved.intended_post_state() {
        if row.schema_key != "lix_registered_schema"
            || row.version_id.as_deref() != Some(GLOBAL_VERSION_ID)
        {
            continue;
        }

        if row.tombstone {
            if let Some((schema_key, _)) = row.entity_id.rsplit_once('~') {
                mutations.push(PublicSurfaceRegistryMutation::RemoveDynamicSchema {
                    schema_key: schema_key.to_string(),
                });
            }
            continue;
        }

        let Some(snapshot_content) = planned_row_optional_json_text_value(row, "snapshot_content")
        else {
            continue;
        };
        let snapshot = serde_json::from_str(snapshot_content.as_ref()).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("registered schema snapshot_content invalid JSON: {error}"),
            )
        })?;
        mutations.push(PublicSurfaceRegistryMutation::UpsertRegisteredSchemaSnapshot { snapshot });
    }

    Ok(mutations)
}

pub(crate) fn apply_public_surface_registry_mutations(
    registry: &mut SurfaceRegistry,
    mutations: &[PublicSurfaceRegistryMutation],
) -> Result<bool, LixError> {
    if mutations.is_empty() {
        return Ok(false);
    }

    for mutation in mutations {
        match mutation {
            PublicSurfaceRegistryMutation::UpsertRegisteredSchemaSnapshot { snapshot } => {
                registry.replace_dynamic_entity_surfaces_from_stored_snapshot(snapshot)?;
            }
            PublicSurfaceRegistryMutation::RemoveDynamicSchema { schema_key } => {
                registry.remove_dynamic_entity_surfaces_for_schema_key(schema_key);
            }
        }
    }

    Ok(true)
}

pub(crate) fn prepared_public_write_mutates_public_surface_registry(
    prepared: &PreparedPublicWrite,
) -> Result<bool, LixError> {
    Ok(!public_surface_registry_mutations(prepared)?.is_empty())
}

fn tracked_public_write_operation_supported(
    planned_write: &PlannedWrite,
    partition: &crate::sql::public::planner::ir::ResolvedWritePartition,
) -> bool {
    match planned_write.command.operation_kind {
        WriteOperationKind::Insert => true,
        WriteOperationKind::Update | WriteOperationKind::Delete => matches!(
            partition.target_write_lane.as_ref(),
            Some(crate::sql::public::planner::ir::WriteLane::SingleVersion(_))
                | Some(crate::sql::public::planner::ir::WriteLane::ActiveVersion)
                | Some(crate::sql::public::planner::ir::WriteLane::GlobalAdmin)
        ),
    }
}

fn public_untracked_operation_supported(planned_write: &PlannedWrite) -> bool {
    matches!(
        planned_write.command.target.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
    ) || matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_active_version" | "lix_active_account" | "lix_version"
    )
}

fn public_write_persists_filesystem_payloads(
    planned_write: &PlannedWrite,
    partition: &crate::sql::public::planner::ir::ResolvedWritePartition,
) -> bool {
    matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        partition.execution_mode,
        crate::sql::public::planner::ir::WriteMode::Tracked
            | crate::sql::public::planner::ir::WriteMode::Untracked
    )
}

pub(crate) fn state_commit_stream_operation(
    operation_kind: WriteOperationKind,
) -> StateCommitStreamOperation {
    match operation_kind {
        WriteOperationKind::Insert => StateCommitStreamOperation::Insert,
        WriteOperationKind::Update => StateCommitStreamOperation::Update,
        WriteOperationKind::Delete => StateCommitStreamOperation::Delete,
    }
}

fn create_commit_preconditions_for_public_write(
    planned_write: &PlannedWrite,
    batch: Option<&DomainChangeBatch>,
    commit_preconditions: &CommitPreconditions,
) -> Result<CreateCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        crate::sql::public::planner::ir::WriteLane::SingleVersion(version_id) => {
            CreateCommitWriteLane::Version(version_id.clone())
        }
        crate::sql::public::planner::ir::WriteLane::ActiveVersion => {
            let version_id = batch
                .into_iter()
                .flat_map(|batch| batch.changes.first())
                .map(|change| change.version_id.to_string())
                .next()
                .or_else(|| {
                    planned_write
                        .command
                        .execution_context
                        .requested_version_id
                        .clone()
                })
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "public commit execution requires a concrete active version id",
                    )
                })?;
            CreateCommitWriteLane::Version(version_id)
        }
        crate::sql::public::planner::ir::WriteLane::GlobalAdmin => {
            CreateCommitWriteLane::GlobalAdmin
        }
    };
    let expected_head = match &commit_preconditions.expected_head {
        crate::sql::public::planner::ir::ExpectedHead::CurrentHead => {
            CreateCommitExpectedHead::CurrentHead
        }
        crate::sql::public::planner::ir::ExpectedHead::CommitId(commit_id) => {
            CreateCommitExpectedHead::CommitId(commit_id.clone())
        }
        crate::sql::public::planner::ir::ExpectedHead::CreateIfMissing => {
            CreateCommitExpectedHead::CreateIfMissing
        }
    };

    Ok(CreateCommitPreconditions {
        write_lane,
        expected_head,
        idempotency_key: match &commit_preconditions.expected_head {
            crate::sql::public::planner::ir::ExpectedHead::CurrentHead => {
                CreateCommitIdempotencyKey::CurrentHeadFingerprint(
                    commit_preconditions.idempotency_key.0.clone(),
                )
            }
            _ => CreateCommitIdempotencyKey::Exact(commit_preconditions.idempotency_key.0.clone()),
        },
    })
}

fn semantic_plan_effects_from_untracked_public_write(
    planned_write: &PlannedWrite,
    intended_post_state: &[crate::sql::public::planner::ir::PlannedStateRow],
    filesystem_state: &FilesystemTransactionState,
) -> Result<PlanEffects, LixError> {
    let mut effects = PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_planned_rows(
            intended_post_state,
            state_commit_stream_operation(planned_write.command.operation_kind),
            true,
            planned_write
                .command
                .execution_context
                .writer_key
                .as_deref(),
        )?,
        ..PlanEffects::default()
    };
    if matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    ) {
        let binary_blob_writes = binary_blob_writes_from_filesystem_state(filesystem_state);
        let pending_file_delete_targets = delete_targets_from_filesystem_state(filesystem_state);
        effects.file_cache_refresh_targets =
            authoritative_binary_blob_write_targets(&binary_blob_writes);
        effects
            .file_cache_refresh_targets
            .extend(pending_file_delete_targets);
    }
    if planned_write.command.target.descriptor.public_name != "lix_active_version" {
        return Ok(effects);
    }
    for row in intended_post_state.iter().rev() {
        if row.schema_key != active_version_schema_key()
            || planned_row_optional_text_value(row, "file_id") != Some(active_version_file_id())
            || row.version_id.as_deref() != Some(active_version_storage_version_id())
            || row.tombstone
        {
            continue;
        }
        let Some(snapshot_content) = planned_row_optional_json_text_value(row, "snapshot_content")
        else {
            continue;
        };
        effects.next_active_version_id =
            Some(parse_active_version_snapshot(snapshot_content.as_ref())?);
        break;
    }
    Ok(effects)
}

pub(crate) fn semantic_plan_effects_from_domain_changes(
    changes: &[ProposedDomainChange],
    stream_operation: StateCommitStreamOperation,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_domain_changes(
            changes,
            stream_operation,
        )?,
        next_active_version_id: next_active_version_id_from_domain_changes(changes)?,
        file_cache_refresh_targets: file_cache_refresh_targets_from_domain_changes(changes),
    })
}

fn next_active_version_id_from_domain_changes(
    changes: &[ProposedDomainChange],
) -> Result<Option<String>, LixError> {
    for change in changes.iter().rev() {
        if change.schema_key != active_version_schema_key()
            || change.file_id.as_deref() != Some(active_version_file_id())
            || change.version_id != active_version_storage_version_id()
        {
            continue;
        }

        let Some(snapshot_content) = change.snapshot_content.as_deref() else {
            continue;
        };
        return parse_active_version_snapshot(snapshot_content).map(Some);
    }

    Ok(None)
}

fn file_cache_refresh_targets_from_domain_changes(
    changes: &[ProposedDomainChange],
) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| change.file_id.as_deref() != Some("lix"))
        .filter(|change| change.schema_key != "lix_file_descriptor")
        .filter(|change| change.schema_key != "lix_directory_descriptor")
        .filter_map(|change| {
            change
                .file_id
                .as_ref()
                .map(|file_id| (file_id.to_string(), change.version_id.to_string()))
        })
        .collect()
}

fn planned_row_optional_text_value<'a>(
    row: &'a crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<&'a str> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn planned_row_optional_json_text_value<'a>(
    row: &'a crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<std::borrow::Cow<'a, str>> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(std::borrow::Cow::Borrowed(value.as_str())),
        Some(Value::Json(value)) => Some(std::borrow::Cow::Owned(value.to_string())),
        _ => None,
    }
}

fn public_authoritative_write_error(
    canonicalized: &CanonicalizedWrite,
    message: String,
) -> Option<LixError> {
    public_write_preparation_error(canonicalized, &message)
}

fn public_write_preparation_error(
    canonicalized: &CanonicalizedWrite,
    message: &str,
) -> Option<LixError> {
    public_write_preparation_error_for_surface(
        &canonicalized.surface_binding,
        canonicalized.write_command.operation_kind,
        message,
    )
}

fn public_write_preparation_error_for_surface(
    surface_binding: &crate::sql::public::catalog::SurfaceBinding,
    operation_kind: WriteOperationKind,
    message: &str,
) -> Option<LixError> {
    let public_name = surface_binding.descriptor.public_name.as_str();
    if surface_binding.descriptor.capability == SurfaceCapability::ReadOnly
        || message.contains("is not writable in public lowering")
        || message.contains("is not writable in public write planning")
        || message.contains("does not support INSERT")
        || message.contains("does not support UPDATE")
        || message.contains("does not support DELETE")
    {
        let operation = match operation_kind {
            WriteOperationKind::Insert => "INSERT",
            WriteOperationKind::Update => "UPDATE",
            WriteOperationKind::Delete => "DELETE",
        };
        return Some(read_only_view_write_error(public_name, operation));
    }
    if message.contains("does not support ON CONFLICT DO NOTHING") {
        return Some(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "ON CONFLICT DO NOTHING is not supported",
        ));
    }
    if (message.contains("write analysis requires version_id")
        || message.contains("requires a concrete version_id"))
        && public_name.ends_with("_by_version")
    {
        let action = match operation_kind {
            WriteOperationKind::Insert => "insert requires version_id",
            WriteOperationKind::Update => "update requires a version_id predicate",
            WriteOperationKind::Delete => "delete requires a version_id predicate",
        };
        return Some(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{public_name} {action}"),
        ));
    }

    match surface_binding.descriptor.surface_family {
        SurfaceFamily::Filesystem => Some(public_filesystem_write_error(public_name, message)),
        SurfaceFamily::State | SurfaceFamily::Entity => {
            Some(LixError::new("LIX_ERROR_UNKNOWN", message))
        }
        SurfaceFamily::Admin => Some(LixError::new(
            "LIX_ERROR_UNKNOWN",
            normalize_admin_public_write_message(public_name, message),
        )),
        _ => None,
    }
}

fn normalize_admin_public_write_message<'a>(
    public_name: &str,
    message: &'a str,
) -> std::borrow::Cow<'a, str> {
    match public_name {
        "lix_version" => message
            .strip_prefix("version ")
            .map(|suffix| std::borrow::Cow::Owned(format!("{public_name} {suffix}")))
            .or_else(|| {
                message
                    .strip_prefix("public version ")
                    .map(|suffix| std::borrow::Cow::Owned(format!("{public_name} {suffix}")))
            })
            .unwrap_or_else(|| std::borrow::Cow::Borrowed(message)),
        _ => std::borrow::Cow::Borrowed(message),
    }
}

fn statement_write_operation_kind(statement: &Statement) -> Option<WriteOperationKind> {
    match statement {
        Statement::Insert(_) => Some(WriteOperationKind::Insert),
        Statement::Update(_) => Some(WriteOperationKind::Update),
        Statement::Delete(_) => Some(WriteOperationKind::Delete),
        _ => None,
    }
}

fn public_write_target_name(
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
) -> Option<String> {
    if parsed_statements.len() != 1 {
        return None;
    }
    let target_name = top_level_write_target_name(&parsed_statements[0])?;
    let binding = registry.bind_relation_name(&target_name)?;
    Some(binding.descriptor.public_name)
}

fn top_level_filesystem_write_target_name(statement: &Statement) -> Option<&'static str> {
    [
        "lix_file",
        "lix_file_by_version",
        "lix_directory",
        "lix_directory_by_version",
        "lix_file_history",
        "lix_file_history_by_version",
        "lix_directory_history",
    ]
    .into_iter()
    .find(|target_name| statement_targets_table_name(statement, target_name))
}

fn top_level_write_target_name(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => Some(name.to_string()),
            _ => None,
        },
        Statement::Update(update) => match &update.table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            match &tables.first()?.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn public_filesystem_write_error(target_name: &str, message: &str) -> LixError {
    if message.contains("data expects bytes") {
        return file_data_expects_bytes_error();
    }

    if message.contains("untracked winner")
        || message.contains("untracked visible row")
        || message.contains("untracked visible rows")
        || message.contains("untracked winners in the cascade")
    {
        return LixError::new("LIX_ERROR_INVALID_INPUT", message);
    }

    LixError::new(
        "LIX_ERROR_UNKNOWN",
        &message.replace("surface ''", &format!("surface '{target_name}'")),
    )
}

fn public_write_phase_trace() -> Vec<String> {
    vec![
        "canonicalize_write".to_string(),
        "analyze_write".to_string(),
        "resolve_authoritative_pre_state".to_string(),
        "build_domain_change_batch".to_string(),
        "derive_commit_preconditions".to_string(),
        "validate_batch_local_write".to_string(),
        "commit_time_invariant_recheck".to_string(),
        "create_commit".to_string(),
    ]
}

fn build_public_write_invariant_trace(planned_write: &PlannedWrite) -> PublicWriteInvariantTrace {
    let mut batch_local_checks = Vec::new();
    let mut commit_time_checks = vec![
        "write_lane.head_precondition".to_string(),
        "idempotency_key.recheck".to_string(),
    ];
    let mut physical_checks = Vec::new();

    if planned_write.command.operation_kind
        == crate::sql::public::planner::ir::WriteOperationKind::Update
    {
        commit_time_checks.push("schema_mutability.recheck".to_string());
    }

    if let Some(resolved) = planned_write.resolved_write_plan.as_ref() {
        let mut saw_snapshot_validation = false;
        let mut saw_primary_key_consistency = false;
        let mut saw_registered_schema_definition = false;
        let mut saw_registered_schema_bootstrap_identity = false;

        for row in resolved.intended_post_state() {
            if row.tombstone {
                continue;
            }

            if !saw_snapshot_validation {
                batch_local_checks.push("snapshot_content.schema_validation".to_string());
                saw_snapshot_validation = true;
            }
            if !saw_primary_key_consistency {
                batch_local_checks.push("entity_id.primary_key_consistency".to_string());
                saw_primary_key_consistency = true;
            }
            if row.schema_key == "lix_registered_schema" {
                if !saw_registered_schema_definition {
                    batch_local_checks.push("registered_schema.definition_validation".to_string());
                    saw_registered_schema_definition = true;
                }
                if !saw_registered_schema_bootstrap_identity {
                    batch_local_checks.push("registered_schema.bootstrap_identity".to_string());
                    saw_registered_schema_bootstrap_identity = true;
                }
            }
        }
    }

    if planned_write
        .resolved_write_plan
        .as_ref()
        .map(|plan| {
            plan.partitions.iter().any(|partition| {
                partition.execution_mode != crate::sql::public::planner::ir::WriteMode::Untracked
            })
        })
        .unwrap_or(true)
    {
        physical_checks.push("backend_constraints.defense_in_depth".to_string());
    }

    PublicWriteInvariantTrace {
        batch_local_checks,
        commit_time_checks,
        physical_checks,
    }
}

#[cfg(test)]
mod tests {
    use super::read::try_prepare_public_read;
    use super::{
        lower_public_read_query_with_backend, prepare_public_execution,
        prepare_public_execution_with_internal_access, prepare_public_read,
        prepare_public_read_strict, DirectPublicReadPlan, PreparedPublicExecution,
        PreparedPublicReadExecution,
    };
    use crate::state::history::StateHistoryRootScope;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeBackend {
        registered_schema_rows: HashMap<String, String>,
        version_descriptor_rows: HashMap<String, String>,
        version_ref_rows: HashMap<String, String>,
        active_version_rows: Vec<(String, String)>,
        active_account_rows: Vec<String>,
        change_rows: Vec<Vec<Value>>,
        untracked_rows: Vec<Vec<Value>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_registered_schema_bootstrap") {
                let rows = self
                    .registered_schema_rows
                    .iter()
                    .map(|(schema_key, snapshot)| {
                        if sql.contains("SELECT schema_version, snapshot_content") {
                            let schema_version =
                                serde_json::from_str::<serde_json::Value>(snapshot)
                                    .ok()
                                    .and_then(|value| {
                                        value
                                            .get("value")
                                            .and_then(|value| value.get("x-lix-version"))
                                            .and_then(serde_json::Value::as_str)
                                            .map(ToString::to_string)
                                    })
                                    .unwrap_or_else(|| "1".to_string());
                            vec![Value::Text(schema_version), Value::Text(snapshot.clone())]
                        } else if sql.contains("substr(entity_id, 1,") {
                            if sql.contains(schema_key) {
                                vec![Value::Text(snapshot.clone())]
                            } else {
                                Vec::new()
                            }
                        } else {
                            vec![Value::Text(snapshot.clone())]
                        }
                    })
                    .filter(|row| !row.is_empty())
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: if sql.contains("SELECT schema_version, snapshot_content") {
                        vec!["schema_version".to_string(), "snapshot_content".to_string()]
                    } else {
                        vec!["snapshot_content".to_string()]
                    },
                });
            }
            if sql.contains("FROM lix_internal_live_untracked_v1_lix_active_version") {
                let rows = self
                    .active_version_rows
                    .iter()
                    .map(|(entity_id, snapshot)| {
                        vec![
                            Value::Text(entity_id.clone()),
                            Value::Text(snapshot.clone()),
                        ]
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["entity_id".to_string(), "snapshot_content".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_live_untracked_v1_lix_active_account") {
                let rows = self
                    .active_account_rows
                    .iter()
                    .map(|account_id| {
                        vec![
                            Value::Text(account_id.clone()),
                            Value::Text(crate::account::active_account_snapshot_content(
                                account_id,
                            )),
                        ]
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["entity_id".to_string(), "snapshot_content".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_live_untracked_v1_") {
                return Ok(QueryResult {
                    rows: self.untracked_rows.clone(),
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "plugin_key".to_string(),
                        "snapshot_content".to_string(),
                        "metadata".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_descriptor'")
            {
                let rows = self
                    .version_descriptor_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("c.entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(_, snapshot)| {
                        vec![
                            Value::Text(snapshot.clone()),
                            Value::Text("descriptor-change".to_string()),
                        ]
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string(), "change_id".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_live_v1_lix_version_descriptor") {
                let rows = self
                    .version_descriptor_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(_, snapshot)| {
                        if sql.contains("change_id") {
                            vec![
                                Value::Text(snapshot.clone()),
                                Value::Text("descriptor-change".to_string()),
                            ]
                        } else {
                            vec![Value::Text(snapshot.clone())]
                        }
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: if sql.contains("change_id") {
                        vec!["snapshot_content".to_string(), "change_id".to_string()]
                    } else {
                        vec!["snapshot_content".to_string()]
                    },
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_ref'")
            {
                let rows = self
                    .version_ref_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("c.entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(version_id, snapshot)| {
                        if sql.contains("SELECT c.entity_id, s.content AS snapshot_content") {
                            vec![
                                Value::Text(version_id.clone()),
                                Value::Text(snapshot.clone()),
                            ]
                        } else {
                            vec![Value::Text(snapshot.clone())]
                        }
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: if sql.contains("SELECT c.entity_id, s.content AS snapshot_content") {
                        vec!["entity_id".to_string(), "snapshot_content".to_string()]
                    } else {
                        vec!["snapshot_content".to_string()]
                    },
                });
            }
            if query_targets_table(sql, "lix_internal_live_untracked_v1_lix_version_ref") {
                let rows = self
                    .version_ref_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        query_has_text_equality(sql, "entity_id", version_id)
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(version_id, snapshot)| build_version_ref_live_row(version_id, snapshot))
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "plugin_key".to_string(),
                        "metadata".to_string(),
                        "commit_id".to_string(),
                        "id".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_ref'")
                && sql.contains("c.entity_id = 'global'")
            {
                let rows = self
                    .version_ref_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("c.entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(_, snapshot)| vec![Value::Text(snapshot.clone())])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }
            if sql.contains("SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content AS snapshot_content, c.metadata, c.created_at")
                && sql.contains("FROM lix_internal_change c")
            {
                return Ok(QueryResult {
                    rows: self.change_rows.clone(),
                    columns: vec![
                        "id".to_string(),
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "plugin_key".to_string(),
                        "snapshot_content".to_string(),
                        "metadata".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    fn parse_one(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&GenericDialect {}, sql).expect("SQL should parse")
    }

    fn query_targets_table(sql: &str, table_name: &str) -> bool {
        let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
            return false;
        };
        statements
            .iter()
            .any(|statement| statement_targets_table(statement, table_name))
    }

    fn statement_targets_table(statement: &Statement, table_name: &str) -> bool {
        match statement {
            Statement::Query(query) => query_targets_table_name(query, table_name),
            _ => false,
        }
    }

    fn query_targets_table_name(query: &Query, table_name: &str) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => select.from.iter().any(|table_with_joins| {
                table_factor_targets_table(&table_with_joins.relation, table_name)
                    || table_with_joins
                        .joins
                        .iter()
                        .any(|join| table_factor_targets_table(&join.relation, table_name))
            }),
            SetExpr::Query(query) => query_targets_table_name(query, table_name),
            _ => false,
        }
    }

    fn table_factor_targets_table(table_factor: &TableFactor, table_name: &str) -> bool {
        match table_factor {
            TableFactor::Table { name, .. } => name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
                .unwrap_or(false),
            TableFactor::Derived { subquery, .. } => query_targets_table_name(subquery, table_name),
            _ => false,
        }
    }

    fn query_has_text_equality(sql: &str, column_name: &str, expected: &str) -> bool {
        let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
            return false;
        };
        statements
            .iter()
            .any(|statement| statement_has_text_equality(statement, column_name, expected))
    }

    fn statement_has_text_equality(
        statement: &Statement,
        column_name: &str,
        expected: &str,
    ) -> bool {
        match statement {
            Statement::Query(query) => query_has_where_text_equality(query, column_name, expected),
            _ => false,
        }
    }

    fn query_has_where_text_equality(query: &Query, column_name: &str, expected: &str) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => select
                .selection
                .as_ref()
                .is_some_and(|expr| expr_has_text_equality(expr, column_name, expected)),
            SetExpr::Query(query) => query_has_where_text_equality(query, column_name, expected),
            _ => false,
        }
    }

    fn expr_has_text_equality(expr: &Expr, column_name: &str, expected: &str) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                expr_identifier_name(left)
                    .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
                    && expr_single_quoted_text(right).is_some_and(|value| value == expected)
                    || expr_identifier_name(right)
                        .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
                        && expr_single_quoted_text(left).is_some_and(|value| value == expected)
            }
            Expr::BinaryOp { left, right, .. } => {
                expr_has_text_equality(left, column_name, expected)
                    || expr_has_text_equality(right, column_name, expected)
            }
            Expr::Nested(inner) => expr_has_text_equality(inner, column_name, expected),
            _ => false,
        }
    }

    fn expr_identifier_name(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.as_str()),
            _ => None,
        }
    }

    fn expr_single_quoted_text(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::SingleQuotedString(text),
                ..
            }) => Some(text.as_str()),
            _ => None,
        }
    }

    fn extract_sql_string_filter(sql: &str, column: &str) -> Option<String> {
        let marker = format!("{column} = '");
        let start = sql.find(&marker)? + marker.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    fn build_committed_state_change_rows(
        entity_id: &str,
        version_id: &str,
        snapshot_content: &str,
        metadata: Option<&str>,
        change_id: &str,
        commit_id: &str,
    ) -> Vec<Vec<Value>> {
        let commit_snapshot = json!({
            "id": commit_id,
            "change_set_id": format!("change-set-{commit_id}"),
            "change_ids": [change_id],
            "author_account_ids": [],
            "parent_commit_ids": []
        })
        .to_string();
        let pointer_snapshot = json!({
            "id": version_id,
            "commit_id": commit_id
        })
        .to_string();
        vec![
            vec![
                Value::Text(change_id.to_string()),
                Value::Text(entity_id.to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("lix".to_string()),
                Value::Text(snapshot_content.to_string()),
                metadata
                    .map(|value| Value::Text(value.to_string()))
                    .unwrap_or(Value::Null),
                Value::Text("2026-03-06T18:00:00Z".to_string()),
            ],
            vec![
                Value::Text(format!("commit-change-{commit_id}")),
                Value::Text(commit_id.to_string()),
                Value::Text("lix_commit".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("lix".to_string()),
                Value::Text(commit_snapshot),
                Value::Null,
                Value::Text("2026-03-06T18:00:01Z".to_string()),
            ],
            vec![
                Value::Text(format!("pointer-change-{version_id}")),
                Value::Text(version_id.to_string()),
                Value::Text("lix_version_ref".to_string()),
                Value::Text("1".to_string()),
                Value::Text(crate::version::version_ref_file_id().to_string()),
                Value::Text(crate::version::version_ref_plugin_key().to_string()),
                Value::Text(pointer_snapshot),
                Value::Null,
                Value::Text("2026-03-06T18:00:02Z".to_string()),
            ],
        ]
    }

    fn build_untracked_state_rows(
        entity_id: &str,
        version_id: &str,
        file_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
        metadata: Option<&str>,
    ) -> Vec<Vec<Value>> {
        vec![vec![
            Value::Text(entity_id.to_string()),
            Value::Text("lix_key_value".to_string()),
            Value::Text("1".to_string()),
            Value::Text(file_id.to_string()),
            Value::Text(version_id.to_string()),
            Value::Text(plugin_key.to_string()),
            Value::Text(snapshot_content.to_string()),
            metadata
                .map(|value| Value::Text(value.to_string()))
                .unwrap_or(Value::Null),
        ]]
    }

    fn build_version_ref_live_row(version_id: &str, snapshot: &str) -> Vec<Value> {
        let parsed: serde_json::Value =
            serde_json::from_str(snapshot).expect("version ref test snapshot must be valid JSON");
        let commit_id = parsed
            .get("commit_id")
            .and_then(serde_json::Value::as_str)
            .expect("version ref test snapshot must include commit_id");
        vec![
            Value::Text(version_id.to_string()),
            Value::Text(crate::version::version_ref_schema_key().to_string()),
            Value::Text(crate::version::version_ref_schema_version().to_string()),
            Value::Text(crate::version::version_ref_file_id().to_string()),
            Value::Text(crate::version::version_ref_storage_version_id().to_string()),
            Value::Text(crate::version::version_ref_plugin_key().to_string()),
            Value::Null,
            Value::Text(commit_id.to_string()),
            Value::Text(version_id.to_string()),
        ]
    }

    #[tokio::test]
    async fn prepares_builtin_schema_derived_entity_reads() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one("SELECT key, value FROM lix_key_value WHERE key = 'hello'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("builtin entity read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_key_value"]);
        assert_eq!(
            prepared
                .dependency_spec
                .as_ref()
                .expect("dependency spec should be derived")
                .schema_keys
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_key_value".to_string()
            ]
        );
        assert_eq!(
            prepared
                .effective_state_request()
                .expect("effective-state request should be built")
                .schema_set
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec!["key = 'hello'".to_string()]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("live public entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert_eq!(
            extract_sql_string_filter(lowered_sql, "file_id").as_deref(),
            Some("lix")
        );
        assert_eq!(
            extract_sql_string_filter(lowered_sql, "plugin_key").as_deref(),
            Some("lix")
        );
    }

    #[tokio::test]
    async fn prepares_registered_schema_derived_entity_reads() {
        let mut backend = FakeBackend::default();
        backend.registered_schema_rows.insert(
            "message".to_string(),
            json!({
                "value": {
                    "x-lix-key": "message",
                    "x-lix-version": "1",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "body": { "type": "string" }
                    }
                }
            })
            .to_string(),
        );

        let prepared = prepare_public_read(
            &backend,
            &parse_one("SELECT body FROM message WHERE id = 'm1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("registered-schema entity read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["message"]);
        assert_eq!(
            prepared
                .structured_read()
                .expect("registered-schema entity read should use canonicalized path")
                .surface_binding
                .implicit_overrides
                .fixed_schema_key
                .as_deref(),
            Some("message")
        );
        assert!(prepared.dependency_spec.is_some());
        assert!(prepared.effective_state_plan().is_some());
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("registered-schema entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_live_v1_message"));
    }

    #[tokio::test]
    async fn lowers_backend_registered_public_queries_with_public_surface_lowering() {
        let mut backend = FakeBackend::default();
        backend.registered_schema_rows.insert(
            "message".to_string(),
            json!({
                "value": {
                    "x-lix-key": "message",
                    "x-lix-version": "1",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "body": { "type": "string" }
                    }
                }
            })
            .to_string(),
        );
        let mut statements = parse_one("SELECT body FROM message WHERE id = 'm1'");
        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected SELECT query");
        };

        let lowered = lower_public_read_query_with_backend(&backend, *query, &[])
            .await
            .expect("registered-schema derived public query should lower through backend registry");
        let lowered_sql = lowered.query.to_string();

        assert_eq!(
            lowered.required_schema_keys,
            ["message".to_string()].into_iter().collect()
        );
        assert!(lowered_sql.contains("lix_internal_live_v1_message"));
        assert!(!lowered_sql.contains("FROM message"));
    }

    #[tokio::test]
    async fn prepares_builtin_entity_by_version_reads() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT key, value, lixcol_version_id \
                 FROM lix_key_value_by_version \
                 WHERE key = 'hello' AND lixcol_version_id = 'version-a'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("builtin entity by-version read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_key_value_by_version"]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec![
                "key = 'hello'".to_string(),
                "lixcol_version_id = 'version-a'".to_string()
            ]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("entity by-version read should lower");
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("version_id AS lixcol_version_id"));
    }

    #[tokio::test]
    async fn prepares_builtin_entity_history_reads() {
        let mut backend = FakeBackend::default();
        backend.version_ref_rows.insert(
            "main".to_string(),
            crate::version::version_ref_snapshot_content("main", "commit-active-root"),
        );
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT key, value, lixcol_commit_id, lixcol_depth \
                 FROM lix_key_value_history \
                 WHERE key = 'hello' \
                 ORDER BY lixcol_depth ASC",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("builtin entity history read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_key_value_history"]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec!["key = 'hello'".to_string()]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    StateHistoryRootScope::RequestedRoots(vec!["commit-active-root".to_string()])
                );
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            _ => panic!("entity history read should use direct entity-history execution"),
        }
    }

    #[tokio::test]
    async fn prepares_lix_change_reads_without_effective_state_artifacts() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT id, schema_key, snapshot_content \
                 FROM lix_change \
                 WHERE entity_id = 'entity-1'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("change read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_change"]);
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .dependency_spec
                .as_ref()
                .expect("change read should derive dependency spec")
                .relations,
            ["lix_change".to_string()].into_iter().collect()
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec!["entity_id = 'entity-1'".to_string()]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("change read should lower");
        assert!(lowered_sql.contains("FROM lix_internal_change ch"));
        assert!(lowered_sql.contains("LEFT JOIN lix_internal_snapshot s"));
    }

    #[tokio::test]
    async fn prepares_lix_working_changes_reads_without_effective_state_artifacts() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT entity_id, status \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("working-changes read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_working_changes"]
        );
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .dependency_spec
                .as_ref()
                .expect("working-changes dependency spec should be recorded")
                .precision,
            crate::sql::common::dependency_spec::DependencyPrecision::Conservative
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("working-changes read should lower");
        assert!(lowered_sql.contains("FROM lix_internal_last_checkpoint"));
        assert!(lowered_sql.contains("tip_ancestry_walk AS"));
        assert!(lowered_sql.contains("baseline_ancestry_walk AS"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_commit_edge"));
    }

    #[tokio::test]
    async fn prepares_filesystem_reads_through_internal_projection_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one("SELECT id, path, data FROM lix_file WHERE id = 'file-1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("filesystem read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_file"]);
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .dependency_spec
                .as_ref()
                .expect("filesystem dependency spec should be recorded")
                .schema_keys
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_binary_blob_ref".to_string(),
                "lix_directory_descriptor".to_string(),
                "lix_file_descriptor".to_string(),
            ]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec!["id = 'file-1'".to_string()]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("filesystem read should lower");
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(lowered_sql.contains("lix_internal_binary_blob_store"));
        assert!(!lowered_sql.contains("FROM lix_file_by_version"));
    }

    #[tokio::test]
    async fn prepares_filesystem_by_version_reads_with_residual_version_filter() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT id, path FROM lix_directory_by_version \
                 WHERE id = 'dir-1' AND lixcol_version_id = 'version-a'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("filesystem by-version read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_directory_by_version"]
        );
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec![
                "id = 'dir-1'".to_string(),
                "lixcol_version_id = 'version-a'".to_string()
            ]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("filesystem by-version read should lower");
        assert!(lowered_sql.contains("all_target_versions AS"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(!lowered_sql.contains("FROM lix_directory_by_version"));
    }

    #[tokio::test]
    async fn prepares_filesystem_history_reads_through_internal_history_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT id, path, lixcol_root_commit_id \
                 FROM lix_file_history \
                 WHERE id = 'file-1' AND lixcol_root_commit_id = 'commit-1'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("filesystem history read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_file_history"]
        );
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec!["root_commit_id = 'commit-1'".to_string()]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    crate::filesystem::history::FileHistoryRootScope::RequestedRoots(vec![
                        "commit-1".to_string()
                    ])
                );
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(_)) => {
                panic!("filesystem history read should not use state-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(_)) => {
                panic!("filesystem history read should not use entity-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::DirectoryHistory(_)) => {
                panic!("filesystem history read should not use directory-history direct plan")
            }
            PreparedPublicReadExecution::LoweredSql(_) => {
                panic!("filesystem history read should not use lowered SQL")
            }
        }
    }

    #[tokio::test]
    async fn prepares_filesystem_history_by_version_reads_through_internal_history_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT id, path, lixcol_version_id, lixcol_root_commit_id \
                 FROM lix_file_history_by_version \
                 WHERE id = 'file-1' \
                   AND lixcol_version_id = 'version-a' \
                   AND lixcol_root_commit_id = 'commit-1'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("filesystem by-version history read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_file_history_by_version"]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec![
                "root_commit_id = 'commit-1'".to_string(),
                "version_id = 'version-a'".to_string()
            ]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(plan)) => {
                assert_eq!(
                    plan.request.version_scope,
                    crate::filesystem::history::FileHistoryVersionScope::RequestedVersions(vec![
                        "version-a".to_string()
                    ])
                );
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(_)) => {
                panic!(
                    "filesystem by-version history read should not use state-history direct plan"
                )
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(_)) => {
                panic!(
                    "filesystem by-version history read should not use entity-history direct plan"
                )
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::DirectoryHistory(_)) => {
                panic!(
                    "filesystem by-version history read should not use directory-history direct plan"
                )
            }
            PreparedPublicReadExecution::LoweredSql(_) => {
                panic!("filesystem by-version history read should not use lowered SQL")
            }
        }
    }

    #[tokio::test]
    async fn prepares_directory_history_reads_through_internal_history_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT id, path, lixcol_root_commit_id \
                 FROM lix_directory_history \
                 WHERE id = 'dir-1' AND lixcol_root_commit_id = 'commit-1'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("directory history read should canonicalize");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_directory_history"]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec![
                "root_commit_id = 'commit-1'".to_string(),
                "id = 'dir-1'".to_string()
            ]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::DirectoryHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    crate::filesystem::history::FileHistoryRootScope::RequestedRoots(vec![
                        "commit-1".to_string()
                    ])
                );
                assert_eq!(plan.request.directory_ids, vec!["dir-1".to_string()]);
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(_)) => {
                panic!("directory history read should not use state-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(_)) => {
                panic!("directory history read should not use entity-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(_)) => {
                panic!("directory history read should not use file-history direct plan")
            }
            PreparedPublicReadExecution::LoweredSql(_) => {
                panic!("directory history read should not use lowered SQL")
            }
        }
    }

    #[tokio::test]
    async fn binds_active_root_commit_for_filesystem_history_reads_without_explicit_root() {
        let mut backend = FakeBackend::default();
        backend.version_ref_rows.insert(
            "main".to_string(),
            crate::version::version_ref_snapshot_content("main", "commit-active-root"),
        );

        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT id, path, lixcol_commit_id, lixcol_depth \
                 FROM lix_file_history \
                 WHERE id = 'file-1' \
                 ORDER BY lixcol_depth ASC",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("filesystem history read should canonicalize");

        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    crate::filesystem::history::FileHistoryRootScope::RequestedRoots(vec![
                        "commit-active-root".to_string()
                    ])
                );
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(_)) => {
                panic!("filesystem history read should not use state-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(_)) => {
                panic!("filesystem history read should not use entity-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::DirectoryHistory(_)) => {
                panic!("filesystem history read should not use directory-history direct plan")
            }
            PreparedPublicReadExecution::LoweredSql(_) => {
                panic!("filesystem history read should not use lowered SQL")
            }
        }
    }

    #[tokio::test]
    async fn binds_active_root_commit_for_entity_history_reads_without_explicit_root() {
        let mut backend = FakeBackend::default();
        backend.version_ref_rows.insert(
            "main".to_string(),
            crate::version::version_ref_snapshot_content("main", "commit-active-root"),
        );

        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT key, value, lixcol_commit_id, lixcol_depth \
                 FROM lix_key_value_history \
                 WHERE key = 'hello' \
                 ORDER BY lixcol_depth ASC",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("entity history read should canonicalize");

        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    StateHistoryRootScope::RequestedRoots(vec!["commit-active-root".to_string()])
                );
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            _ => panic!("entity history read should use direct entity-history execution"),
        }
    }

    #[tokio::test]
    async fn rejects_explain_over_history_surfaces() {
        let backend = FakeBackend::default();
        let error = try_prepare_public_read(
            &backend,
            &parse_one("EXPLAIN SELECT key FROM lix_key_value_history WHERE key = 'hello'"),
            &[],
            "main",
            None,
        )
        .await
        .expect_err("history EXPLAIN should be rejected");

        assert!(
            error.description.contains("EXPLAIN is not supported")
                || error
                    .description
                    .contains("direct-only history surfaces do not support broad surface lowering"),
            "{}",
            error.description
        );
    }

    #[tokio::test]
    async fn prepares_explain_over_state_reads_with_public_lowered_query() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "EXPLAIN SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("explain state read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_state"]);
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("explain state read should lower");
        assert!(lowered_sql.starts_with("EXPLAIN SELECT"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
    }

    #[tokio::test]
    async fn classifies_public_reads_through_public_execution() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_execution(
            &backend,
            &parse_one("SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("public read classification should succeed");

        assert!(matches!(prepared, Some(PreparedPublicExecution::Read(_))));
    }

    #[tokio::test]
    async fn classifies_public_writes_through_public_execution() {
        let mut backend = FakeBackend::default();
        backend.version_ref_rows.insert(
            "main".to_string(),
            crate::version::version_ref_snapshot_content("main", "commit-active-root"),
        );
        let prepared = prepare_public_execution(
            &backend,
            &parse_one("INSERT INTO lix_key_value (key, value) VALUES ('phase1-boundary', 'ok')"),
            &[],
            "main",
            None,
        )
        .await
        .expect("public write classification should succeed");

        assert!(matches!(prepared, Some(PreparedPublicExecution::Write(_))));
    }

    #[tokio::test]
    async fn read_only_public_writes_are_owned_by_public_lowering_and_rejected_semantically() {
        let backend = FakeBackend::default();
        let error = prepare_public_execution(
            &backend,
            &parse_one(
                "INSERT INTO lix_change (id, entity_id, schema_key, schema_version, file_id, plugin_key, created_at) \
                 VALUES ('c1', 'e1', 's1', '1', 'lix', 'lix', '2026-01-01T00:00:00Z')",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect_err("read-only public write should be rejected by public lowering");

        assert_eq!(error.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }

    #[tokio::test]
    async fn commit_and_change_set_public_writes_are_rejected_semantically() {
        let backend = FakeBackend::default();

        for sql in [
            "INSERT INTO lix_commit (id, change_set_id) VALUES ('c1', 'cs1')",
            "INSERT INTO lix_change_set (id) VALUES ('cs1')",
        ] {
            let error = prepare_public_execution(&backend, &parse_one(sql), &[], "main", None)
                .await
                .expect_err("read-only public write should be rejected by public lowering");
            assert_eq!(error.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
        }
    }

    #[tokio::test]
    async fn prepares_bindable_cte_join_group_by_reads_via_surface_expansion() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read_strict(
            &backend,
            &parse_one(
                "WITH keyed AS ( \
                   SELECT entity_id, schema_key \
                   FROM lix_state \
                   WHERE schema_key = 'lix_key_value' \
                 ) \
                 SELECT keyed.schema_key, COUNT(*) \
                 FROM keyed \
                 JOIN lix_state_by_version sv \
                   ON sv.entity_id = keyed.entity_id \
                 WHERE sv.lixcol_version_id = 'main' \
                 GROUP BY keyed.schema_key \
                 ORDER BY keyed.schema_key",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("bindable cte/join/group-by read should not error")
        .expect("bindable cte/join/group-by read should prepare through public lowering");

        assert!(prepared.optimization.is_none());
        assert!(prepared.dependency_spec.is_some());
        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_state", "lix_state_by_version"]
        );
        assert_eq!(
            prepared
                .debug_trace
                .bound_public_leaves
                .iter()
                .map(|leaf| leaf.public_name.as_str())
                .collect::<Vec<_>>(),
            vec!["lix_state", "lix_state_by_version"]
        );
        assert_eq!(
            prepared
                .dependency_spec
                .as_ref()
                .expect("broad read should derive dependency spec")
                .precision,
            crate::sql::common::dependency_spec::DependencyPrecision::Conservative
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("surface-expanded read should lower");
        assert!(!lowered_sql.contains("FROM lix_state "));
        assert!(!lowered_sql.contains("JOIN lix_state_by_version"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[tokio::test]
    async fn prepares_group_by_having_reads_via_surface_expansion() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read_strict(
            &backend,
            &parse_one(
                "SELECT schema_key, COUNT(*) AS count_rows \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                 GROUP BY schema_key \
                 HAVING COUNT(*) > 0 \
                 ORDER BY schema_key",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("group-by/having public read should not error")
        .expect("group-by/having public read should prepare through public lowering");

        assert!(prepared.optimization.is_none());
        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_state"]);
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("group-by/having read should lower");
        assert!(!lowered_sql.contains("FROM lix_state"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("GROUP BY"));
        assert!(lowered_sql.contains("HAVING"));
    }

    #[tokio::test]
    async fn cte_shadowing_public_surface_names_stays_non_public() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_execution(
            &backend,
            &parse_one(
                "WITH lix_state AS (SELECT 'shadow' AS entity_id) SELECT entity_id FROM lix_state",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("cte shadowing should classify cleanly");

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepares_joined_admin_reads_via_surface_expansion() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT av.version_id, v.commit_id \
                 FROM lix_active_version av \
                 JOIN lix_version v ON v.id = av.version_id",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("joined admin read should prepare through public lowering");

        assert!(prepared.optimization.is_none());
        assert!(prepared.dependency_spec.is_some());
        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_active_version", "lix_version"]
        );
        assert_eq!(
            prepared
                .debug_trace
                .bound_public_leaves
                .iter()
                .map(|leaf| leaf.public_name.as_str())
                .collect::<Vec<_>>(),
            vec!["lix_active_version", "lix_version"]
        );
        assert_eq!(
            prepared
                .dependency_spec
                .as_ref()
                .expect("joined admin read should derive dependency spec")
                .relations,
            ["lix_active_version".to_string(), "lix_version".to_string()]
                .into_iter()
                .collect()
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("joined admin read should lower");
        assert!(lowered_sql.contains("lix_internal_live_untracked_v1_lix_active_version"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_version_descriptor"));
    }

    #[tokio::test]
    async fn prepares_public_reads_joined_with_backend_real_tables() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read_strict(
            &backend,
            &parse_one(
                "SELECT av.version_id \
                 FROM app_versions app \
                 JOIN lix_active_version av \
                   ON av.version_id = app.id",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("public/external mixed read should not error")
        .expect("public/external mixed read should prepare through public lowering");

        assert!(prepared.optimization.is_none());
        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_active_version"]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("mixed read should lower");
        assert!(lowered_sql.contains("app_versions"));
        assert!(lowered_sql.contains("lix_internal_live_untracked_v1_lix_active_version"));
    }

    #[tokio::test]
    async fn rejects_public_reads_mixed_with_internal_engine_tables() {
        let backend = FakeBackend::default();
        let error = prepare_public_read_strict(
            &backend,
            &parse_one(
                "SELECT av.version_id \
                 FROM lix_active_version av \
                 JOIN lix_internal_live_untracked_v1_lix_active_version u ON u.entity_id = av.id",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect_err("public/internal mixed read should be rejected");

        assert_eq!(error.code, "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED");
        assert!(error
            .description
            .contains("lix_internal_live_untracked_v1_lix_active_version"));
    }

    #[tokio::test]
    async fn allows_public_reads_mixed_with_internal_engine_tables_when_internal_access_enabled() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_execution_with_internal_access(
            &backend,
            &parse_one(
                "SELECT av.version_id \
                 FROM lix_active_version av \
                 JOIN lix_internal_live_untracked_v1_lix_active_version u ON u.entity_id = av.id",
            ),
            &[],
            "main",
            None,
            true,
        )
        .await
        .expect("public/internal mixed read should prepare when internal access is enabled")
        .expect("public/internal mixed read should return a prepared public read");

        let PreparedPublicExecution::Read(prepared) = prepared else {
            panic!("expected prepared public read");
        };

        assert!(prepared.optimization.is_none());
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("public/internal mixed read should lower");
        assert!(lowered_sql.contains("lix_internal_live_untracked_v1_lix_active_version"));
    }

    #[tokio::test]
    async fn prepares_state_reads_with_explicit_residual_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one("SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("state read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            Vec::<String>::new()
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("state read should lower");
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
    }

    #[tokio::test]
    async fn prepares_state_by_version_reads_with_version_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT entity_id FROM lix_state_by_version \
                 WHERE version_id = 'v1' AND schema_key = 'lix_key_value'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("state-by-version read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec![
                "version_id = 'v1'".to_string(),
                "schema_key = 'lix_key_value'".to_string()
            ]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("state-by-version read should lower");
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[tokio::test]
    async fn prepares_state_history_reads_with_root_commit_pushdown() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT snapshot_content, root_commit_id, depth \
                 FROM lix_state_history \
                 WHERE entity_id = 'entity1' AND root_commit_id = 'commit-1' \
                 ORDER BY depth ASC",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("state-history read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec!["root_commit_id = 'commit-1'".to_string()]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    StateHistoryRootScope::RequestedRoots(vec!["commit-1".to_string()])
                );
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(_)) => {
                panic!("state-history read should not use entity-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(_)) => {
                panic!("state-history read should not use file-history direct plan")
            }
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::DirectoryHistory(_)) => {
                panic!("state-history read should not use directory-history direct plan")
            }
            PreparedPublicReadExecution::LoweredSql(_) => {
                panic!("state-history read should not use lowered SQL")
            }
        }
    }

    #[tokio::test]
    async fn prepares_grouped_state_history_reads_through_direct_history_source() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT entity_id, root_commit_id, depth, COUNT(*) AS count_rows \
                 FROM lix_state_history \
                 WHERE root_commit_id = 'commit-1' \
                 GROUP BY entity_id, root_commit_id, depth \
                 HAVING COUNT(*) > 0 \
                 ORDER BY entity_id",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("grouped state-history read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec!["root_commit_id = 'commit-1'".to_string()]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    StateHistoryRootScope::RequestedRoots(vec!["commit-1".to_string()])
                );
                assert!(prepared.debug_trace.lowered_sql.is_empty());
            }
            _ => panic!("grouped state-history read should use direct state-history execution"),
        }
    }

    #[tokio::test]
    async fn prepares_nested_filesystem_subqueries_through_public_lowering() {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT COUNT(*) \
                 FROM lix_working_changes wc \
                 WHERE wc.file_id IN (SELECT f.id FROM lix_file f WHERE f.path = '/hello.txt')",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("nested filesystem subquery should prepare through public lowering");

        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("nested filesystem subquery should lower");
        assert!(!lowered_sql.contains("FROM lix_file"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
    }

    #[tokio::test]
    async fn prepares_nested_public_subqueries_via_surface_expansion_when_multiple_public_surfaces_are_bound(
    ) {
        let backend = FakeBackend::default();
        let prepared = prepare_public_read(
            &backend,
            &parse_one(
                "SELECT av.version_id \
                 FROM lix_active_version av \
                 WHERE av.version_id IN ( \
                   SELECT v.id \
                   FROM lix_version v \
                   WHERE v.commit_id IS NOT NULL \
                 )",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("nested public-subquery read should prepare through public lowering");

        assert!(prepared.optimization.is_none());
        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_active_version", "lix_version"]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("nested public-subquery read should lower");
        assert!(!lowered_sql.contains("FROM lix_active_version"));
        assert!(!lowered_sql.contains("FROM lix_version"));
        assert!(lowered_sql.contains("lix_internal_live_untracked_v1_lix_version_ref"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_version_descriptor"));
    }
}
