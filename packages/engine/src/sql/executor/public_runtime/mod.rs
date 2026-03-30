//! Executor-owned implementation of the public SQL runtime surface.

use crate::errors::{
    file_data_expects_bytes_error, mixed_public_internal_query_error, read_only_view_write_error,
};
use crate::filesystem::runtime::{
    binary_blob_writes_from_filesystem_state, delete_targets_from_filesystem_state,
    FilesystemTransactionState,
};
#[cfg(test)]
use crate::functions::SystemFunctionProvider;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::read::models::TrackedDomainChangeView;
use crate::schema::builtin::builtin_schema_definition;
use crate::session::contracts::SessionStateDelta;
use crate::sql::analysis::state_resolution::canonical::statement_targets_table_name;
use crate::sql::backend::PushdownDecision;
use crate::sql::binder::{bind_statement, RuntimeBindingValues};
use crate::sql::catalog::{
    SurfaceCapability, SurfaceFamily, SurfaceReadFreshness, SurfaceRegistry, SurfaceVariant,
};
use crate::sql::executor::contracts::effects::PlanEffects;
use crate::sql::executor::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::sql::executor::intent::authoritative_binary_blob_write_targets;
use crate::sql::explain::{
    build_public_write_explain_artifacts, unwrap_explain_statement, ExplainArtifacts, ExplainStage,
    ExplainTimingCollector, PublicWriteExplainBuildInput,
};
use crate::sql::logical_plan::public_ir::{
    CommitPreconditions, PlannedWrite, StructuredPublicRead, WriteOperationKind,
};
use crate::sql::logical_plan::{
    verify_logical_plan, DependencySpec, LogicalPlan, PublicReadLogicalPlan,
};
use crate::sql::physical_plan::{
    LoweredReadProgram, PreparedPublicReadExecution, PreparedPublicWriteExecution,
    PublicWriteExecutionPartition, PublicWriteMaterialization, TrackedWriteExecution,
    UntrackedWriteExecution,
};
use crate::sql::semantic_ir::canonicalize::CanonicalizedWrite;
use crate::sql::semantic_ir::semantics::domain_changes::{
    build_domain_change_batch, derive_commit_preconditions, DomainChangeBatch,
};
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest,
};
use crate::sql::semantic_ir::semantics::write_resolver::resolve_write_plan_with_functions;
use crate::sql::semantic_ir::{
    analyze_public_write_semantics, BoundStatement, ExecutionContext, PublicWriteInvariantTrace,
    PublicWriteSemantics,
};
use crate::state::stream::{
    state_commit_stream_changes_from_domain_changes, state_commit_stream_changes_from_planned_rows,
    StateCommitStreamOperation, StateCommitStreamRuntimeMetadata,
};
use crate::transaction::PendingTransactionView;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, LimitClause, ObjectNamePart, OrderBy, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue, Visit, Visitor,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicReadOptimization {
    pub(crate) structured_read: StructuredPublicRead,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedPublicRead {
    pub(crate) optimization: Option<PublicReadOptimization>,
    pub(crate) freshness_contract: SurfaceReadFreshness,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) logical_plan: PublicReadLogicalPlan,
    pub(crate) execution: PreparedPublicReadExecution,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) runtime_bindings: RuntimeBindingValues,
    pub(crate) public_output_columns: Option<Vec<String>>,
    pub(crate) explain: ExplainArtifacts,
}

pub(crate) use read::{
    decode_public_read_result, execute_prepared_public_read,
    execute_prepared_public_read_in_transaction,
    execute_prepared_public_read_without_freshness_check,
};

impl PreparedPublicRead {
    pub(crate) fn structured_read(&self) -> Option<&StructuredPublicRead> {
        self.logical_plan.structured_read()
    }

    pub(crate) fn effective_state_request(&self) -> Option<&EffectiveStateRequest> {
        self.logical_plan.effective_state_request()
    }

    pub(crate) fn effective_state_plan(&self) -> Option<&EffectiveStatePlan> {
        self.logical_plan.effective_state_plan()
    }

    pub(crate) fn dependency_spec(&self) -> Option<&DependencySpec> {
        self.logical_plan.dependency_spec()
    }

    pub(crate) fn lowered_read(&self) -> Option<&LoweredReadProgram> {
        match &self.execution {
            PreparedPublicReadExecution::LoweredSql(lowered) => Some(lowered),
            PreparedPublicReadExecution::Direct(_) => None,
        }
    }

    pub(crate) fn surface_bindings(&self) -> &[String] {
        &self.surface_bindings
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedPublicWrite {
    pub(crate) canonicalized: CanonicalizedWrite,
    pub(crate) planned_write: PlannedWrite,
    pub(crate) domain_change_batches: Vec<DomainChangeBatch>,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) execution: PreparedPublicWriteExecution,
    pub(crate) explain: ExplainArtifacts,
}

impl PreparedPublicWrite {
    pub(crate) fn materialization(&self) -> Option<&PublicWriteMaterialization> {
        match &self.execution {
            PreparedPublicWriteExecution::Noop => None,
            PreparedPublicWriteExecution::Materialize(materialization) => Some(materialization),
        }
    }

    pub(crate) fn materialization_mut(&mut self) -> Option<&mut PublicWriteMaterialization> {
        match &mut self.execution {
            PreparedPublicWriteExecution::Noop => None,
            PreparedPublicWriteExecution::Materialize(materialization) => Some(materialization),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicSurfaceRegistryMutation {
    UpsertRegisteredSchemaSnapshot { snapshot: JsonValue },
    RemoveDynamicSchema { schema_key: String },
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
    bound_surface_bindings: Vec<crate::sql::catalog::SurfaceBinding>,
    internal_relations: Vec<String>,
    external_relations: Vec<String>,
    requested_history_root_commit_ids: Vec<String>,
}

pub(crate) mod read;
pub(crate) mod tracked_write_plan;

pub(crate) use tracked_write_plan::{build_tracked_txn_unit, TrackedTxnUnit};

#[cfg(test)]
pub(crate) async fn prepare_public_execution(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    prepare_public_execution_with_internal_access(
        backend,
        parsed_statements,
        params,
        active_version_id,
        active_account_ids,
        writer_key,
        false,
    )
    .await
}

pub(crate) async fn prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view_and_functions<
    P,
>(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    pending_transaction_view: Option<&PendingTransactionView>,
    parse_duration: Option<Duration>,
    functions: SharedFunctionProvider<P>,
) -> Result<Option<PreparedPublicExecution>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let Some(route) = classify_public_execution_route_with_registry(registry, parsed_statements)
    else {
        return Ok(None);
    };

    match route {
        PublicExecutionRoute::Write => {
            let target_name = public_write_target_name(registry, parsed_statements)
                .expect("public write route must expose a target name");
            let prepared = try_prepare_public_write_with_registry_and_functions(
                backend,
                registry,
                parsed_statements,
                params,
                active_version_id,
                active_account_ids,
                writer_key,
                pending_transaction_view,
                parse_duration,
                functions,
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
                parse_duration,
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

#[cfg(test)]
pub(crate) async fn prepare_public_execution_with_internal_access(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    prepare_public_execution_with_internal_access_and_functions(
        backend,
        parsed_statements,
        params,
        active_version_id,
        active_account_ids,
        writer_key,
        allow_internal_tables,
        None,
        SharedFunctionProvider::new(SystemFunctionProvider),
    )
    .await
}

pub(crate) async fn prepare_public_execution_with_internal_access_and_functions<P>(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    parse_duration: Option<Duration>,
    functions: SharedFunctionProvider<P>,
) -> Result<Option<PreparedPublicExecution>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let builtin_registry = SurfaceRegistry::with_builtin_surfaces();
    if classify_public_execution_route_with_registry(&builtin_registry, parsed_statements).is_some()
    {
        return prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view_and_functions(
            backend,
            &builtin_registry,
            parsed_statements,
            params,
            active_version_id,
            active_account_ids,
            writer_key,
            allow_internal_tables,
            None,
            parse_duration,
            functions,
        )
        .await;
    }

    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view_and_functions(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        active_account_ids,
        writer_key,
        allow_internal_tables,
        None,
        parse_duration,
        functions,
    )
    .await
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
    if statement_references_public_surface_with_builtin_registry(statement) {
        return true;
    }

    let registry = match SurfaceRegistry::bootstrap_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => return statement_references_public_surface_with_builtin_registry(statement),
    };
    statement_references_public_surface(&registry, statement)
}

pub(crate) async fn try_prepare_public_read_with_registry_and_internal_access(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    parse_duration: Option<Duration>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    read::try_prepare_public_read_with_registry_and_internal_access(
        backend,
        registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
        parse_duration,
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

#[cfg(test)]
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

#[cfg(test)]
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
    let mut bound_surface_bindings = Vec::new();
    let mut internal_relations = Vec::new();
    let mut external_relations = Vec::new();

    for relation_name in collect_public_query_relation_names(query) {
        if let Some(binding) = registry.bind_relation_name(&relation_name) {
            bound_surface_bindings.push(binding);
        } else if relation_name.starts_with("lix_internal_") {
            internal_relations.push(relation_name);
        } else {
            external_relations.push(relation_name);
        }
    }

    BoundPublicReadSummary {
        bound_surface_bindings,
        internal_relations,
        external_relations,
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

fn merge_surface_read_freshness(
    left: SurfaceReadFreshness,
    right: SurfaceReadFreshness,
) -> SurfaceReadFreshness {
    match (left, right) {
        (SurfaceReadFreshness::RequiresFreshProjection, _)
        | (_, SurfaceReadFreshness::RequiresFreshProjection) => {
            SurfaceReadFreshness::RequiresFreshProjection
        }
        (
            SurfaceReadFreshness::AllowsStaleProjection,
            SurfaceReadFreshness::AllowsStaleProjection,
        ) => SurfaceReadFreshness::AllowsStaleProjection,
    }
}

fn bound_surface_freshness_contract(
    bindings: &[crate::sql::catalog::SurfaceBinding],
) -> Option<SurfaceReadFreshness> {
    let mut bindings = bindings.iter();
    let first = bindings.next()?;
    Some(bindings.fold(first.read_freshness, |merged, binding| {
        merge_surface_read_freshness(merged, binding.read_freshness)
    }))
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

pub(crate) async fn try_prepare_public_write_with_functions<P>(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    parse_duration: Option<Duration>,
    functions: SharedFunctionProvider<P>,
) -> Result<Option<PreparedPublicWrite>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    if parsed_statements.len() != 1 {
        return Ok(None);
    }

    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    try_prepare_public_write_with_registry_and_functions(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        active_account_ids,
        writer_key,
        None,
        parse_duration,
        functions,
    )
    .await
}

pub(crate) async fn try_prepare_public_write_with_registry_and_functions<P>(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    pending_transaction_view: Option<&PendingTransactionView>,
    parse_duration: Option<Duration>,
    functions: SharedFunctionProvider<P>,
) -> Result<Option<PreparedPublicWrite>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    if parsed_statements.len() != 1 {
        return Ok(None);
    }

    let explained = unwrap_explain_statement(&parsed_statements[0])?;
    let statement = explained.statement;
    let explain_request = explained.request;
    let bind_started = Instant::now();
    let bound_statement = bind_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
            active_account_ids: active_account_ids.to_vec(),
        },
    );
    let mut stage_timings = ExplainTimingCollector::new(parse_duration);
    stage_timings.record(ExplainStage::Bind, bind_started.elapsed());
    let filesystem_target_name =
        top_level_filesystem_write_target_name(&bound_statement.statement).map(str::to_string);
    let semantic_started = Instant::now();
    let semantics = match PublicWriteSemantics::prepare(bound_statement.clone(), &registry) {
        Ok(semantics) => semantics,
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
    stage_timings.record(ExplainStage::SemanticAnalysis, semantic_started.elapsed());
    let logical_started = Instant::now();
    let mut write_analysis = match analyze_public_write_semantics(&semantics) {
        Ok(write_analysis) => write_analysis,
        Err(error) => {
            if let Some(error) =
                public_write_preparation_error(&semantics.canonicalized, &error.message)
            {
                return Err(error);
            }
            return Ok(None);
        }
    };
    let canonicalized = &write_analysis.semantics.canonicalized;
    let mut planned_write = write_analysis.planned_write.clone();
    stage_timings.record(ExplainStage::LogicalPlanning, logical_started.elapsed());
    let physical_started = Instant::now();
    let resolved_write_plan = match resolve_write_plan_with_functions(
        backend,
        &planned_write,
        pending_transaction_view,
        functions.clone(),
    )
    .await
    {
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
    write_analysis.planned_write = planned_write.clone();
    let write_logical_plan = write_analysis.logical_plan();
    verify_logical_plan(&LogicalPlan::PublicWrite(write_logical_plan)).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "public write logical plan verification failed: {}",
                error.message
            ),
        )
    })?;
    let invariant_trace = Some(build_public_write_invariant_trace(&planned_write));
    let execution = build_public_write_execution(
        &planned_write,
        &domain_change_batches,
        &commit_preconditions,
    )?
    .ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public write target must route through explicit public materialization",
        )
    })?;
    stage_timings.record(ExplainStage::PhysicalPlanning, physical_started.elapsed());
    let explain = build_public_write_explain_artifacts(PublicWriteExplainBuildInput {
        request: explain_request,
        semantics: write_analysis.semantics.clone(),
        planned_write: planned_write.clone(),
        execution: execution.clone(),
        domain_change_batches: domain_change_batches.clone(),
        invariant_trace: invariant_trace.clone(),
        stage_timings: stage_timings.finish(),
    });

    Ok(Some(PreparedPublicWrite {
        explain,
        planned_write,
        domain_change_batches,
        surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
        execution,
        canonicalized: canonicalized.clone(),
    }))
}

fn build_public_write_execution(
    planned_write: &PlannedWrite,
    domain_change_batches: &[DomainChangeBatch],
    commit_preconditions: &[CommitPreconditions],
) -> Result<Option<PreparedPublicWriteExecution>, LixError> {
    let Some(resolved) = planned_write.resolved_write_plan.as_ref() else {
        return Ok(None);
    };
    let mut tracked_batches = domain_change_batches.iter();
    let mut tracked_preconditions = commit_preconditions.iter();
    let mut partitions = Vec::new();
    let mut filesystem_payloads_persisted = false;

    for partition in &resolved.partitions {
        if partition.intended_post_state.is_empty() {
            continue;
        }
        let persist_filesystem_payloads_before_write = !filesystem_payloads_persisted
            && public_write_persists_filesystem_payloads(planned_write, partition);
        filesystem_payloads_persisted |= persist_filesystem_payloads_before_write;

        match partition.execution_mode {
            crate::sql::logical_plan::public_ir::WriteMode::Tracked => {
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
                        create_preconditions: commit_preconditions.clone(),
                        semantic_effects: semantic_plan_effects_from_domain_changes(
                            &domain_change_batch.changes,
                            state_commit_stream_operation(planned_write.command.operation_kind),
                            domain_change_batch.writer_key.as_deref(),
                        )?,
                        domain_change_batch: Some(domain_change_batch),
                    },
                ));
            }
            crate::sql::logical_plan::public_ir::WriteMode::Untracked => {
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

    Ok(Some(if partitions.is_empty() {
        PreparedPublicWriteExecution::Noop
    } else {
        PreparedPublicWriteExecution::Materialize(PublicWriteMaterialization { partitions })
    }))
}

pub(crate) fn finalize_public_write_execution(
    execution: &mut PublicWriteMaterialization,
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
    partition: &crate::sql::logical_plan::public_ir::ResolvedWritePartition,
) -> Vec<SchemaLiveTableRequirement> {
    let mut requirements = BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for row in &partition.intended_post_state {
        if row.schema_key != "lix_registered_schema" {
            requirements
                .entry(row.schema_key.clone())
                .or_insert(SchemaLiveTableRequirement {
                    schema_key: row.schema_key.clone(),
                    schema_definition: None,
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
        requirements.insert(
            schema_key.schema_key.clone(),
            SchemaLiveTableRequirement {
                schema_key: schema_key.schema_key,
                schema_definition: Some(schema),
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
    partition: &crate::sql::logical_plan::public_ir::ResolvedWritePartition,
) -> bool {
    match planned_write.command.operation_kind {
        WriteOperationKind::Insert => true,
        WriteOperationKind::Update | WriteOperationKind::Delete => matches!(
            partition.target_write_lane.as_ref(),
            Some(crate::sql::logical_plan::public_ir::WriteLane::SingleVersion(_))
                | Some(crate::sql::logical_plan::public_ir::WriteLane::ActiveVersion)
                | Some(crate::sql::logical_plan::public_ir::WriteLane::GlobalAdmin)
        ),
    }
}

fn public_untracked_operation_supported(planned_write: &PlannedWrite) -> bool {
    matches!(
        planned_write.command.target.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
    ) || planned_write.command.target.descriptor.public_name == "lix_version"
}

fn public_write_persists_filesystem_payloads(
    planned_write: &PlannedWrite,
    partition: &crate::sql::logical_plan::public_ir::ResolvedWritePartition,
) -> bool {
    matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        partition.execution_mode,
        crate::sql::logical_plan::public_ir::WriteMode::Tracked
            | crate::sql::logical_plan::public_ir::WriteMode::Untracked
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

fn semantic_plan_effects_from_untracked_public_write(
    planned_write: &PlannedWrite,
    intended_post_state: &[crate::sql::logical_plan::public_ir::PlannedStateRow],
    filesystem_state: &FilesystemTransactionState,
) -> Result<PlanEffects, LixError> {
    let mut effects = PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_planned_rows(
            intended_post_state,
            state_commit_stream_operation(planned_write.command.operation_kind),
            true,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(
                planned_write
                    .command
                    .execution_context
                    .writer_key
                    .as_deref(),
            ),
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
    Ok(effects)
}

pub(crate) fn semantic_plan_effects_from_domain_changes<Change: TrackedDomainChangeView>(
    changes: &[Change],
    stream_operation: StateCommitStreamOperation,
    writer_key: Option<&str>,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_domain_changes(
            changes,
            stream_operation,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(writer_key),
        )?,
        session_delta: SessionStateDelta {
            next_active_version_id: next_active_version_id_from_domain_changes(changes)?,
            next_active_account_ids: None,
            persist_workspace: false,
        },
        file_cache_refresh_targets: file_cache_refresh_targets_from_domain_changes(changes),
    })
}

fn next_active_version_id_from_domain_changes<Change: TrackedDomainChangeView>(
    changes: &[Change],
) -> Result<Option<String>, LixError> {
    for change in changes.iter().rev() {
        if change.schema_key() != active_version_schema_key()
            || change.file_id() != Some(active_version_file_id())
            || change.version_id() != active_version_storage_version_id()
        {
            continue;
        }

        let Some(snapshot_content) = change.snapshot_content() else {
            continue;
        };
        return parse_active_version_snapshot(snapshot_content).map(Some);
    }

    Ok(None)
}

fn file_cache_refresh_targets_from_domain_changes<Change: TrackedDomainChangeView>(
    changes: &[Change],
) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| change.file_id() != Some("lix"))
        .filter(|change| change.schema_key() != "lix_file_descriptor")
        .filter(|change| change.schema_key() != "lix_directory_descriptor")
        .filter_map(|change| {
            change
                .file_id()
                .map(|file_id| (file_id.to_string(), change.version_id().to_string()))
        })
        .collect()
}

fn planned_row_optional_json_text_value<'a>(
    row: &'a crate::sql::logical_plan::public_ir::PlannedStateRow,
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
    surface_binding: &crate::sql::catalog::SurfaceBinding,
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
        Statement::Explain { statement, .. } => top_level_write_target_name(statement.as_ref()),
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

fn build_public_write_invariant_trace(planned_write: &PlannedWrite) -> PublicWriteInvariantTrace {
    let mut batch_local_checks = Vec::new();
    let mut commit_time_checks = vec![
        "write_lane.head_precondition".to_string(),
        "idempotency_key.recheck".to_string(),
    ];
    let mut physical_checks = Vec::new();

    if planned_write.command.operation_kind
        == crate::sql::logical_plan::public_ir::WriteOperationKind::Update
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
                partition.execution_mode
                    != crate::sql::logical_plan::public_ir::WriteMode::Untracked
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
    use super::{
        prepare_public_execution, prepare_public_read, prepare_public_read_strict,
        PreparedPublicExecution, PreparedPublicReadExecution,
    };
    use crate::read::models::StateHistoryRootScope;
    use crate::sql::routing::delay_broad_routing_for_test;
    use crate::sql::{
        binder::{bind_public_read_statement, forbid_broad_binding_for_test},
        catalog::{SurfaceReadFreshness, SurfaceRegistry},
        explain::ExplainTimingCollector,
        logical_plan::{DependencyPrecision, DirectPublicReadPlan},
        semantic_ir::ExecutionContext,
    };
    use crate::{LixBackend, LixError, QueryResult, Session, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Default)]
    struct FakeBackend {
        registered_schema_rows: HashMap<String, String>,
        registered_schema_delay: Option<Duration>,
        registered_schema_query_count: Arc<AtomicUsize>,
    }

    impl FakeBackend {
        fn with_registered_schema_delay(mut self, delay: Duration) -> Self {
            self.registered_schema_delay = Some(delay);
            self
        }

        fn registered_schema_query_count(&self) -> usize {
            self.registered_schema_query_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_registered_schema_bootstrap") {
                self.registered_schema_query_count
                    .fetch_add(1, Ordering::SeqCst);
                if let Some(delay) = self.registered_schema_delay {
                    sleep(delay).await;
                }
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
            if sql.contains("FROM lix_internal_workspace_metadata") {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: vec!["value".to_string()],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    fn parse_one(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&GenericDialect {}, sql).expect("SQL should parse")
    }

    fn extract_sql_string_filter(sql: &str, column: &str) -> Option<String> {
        let marker = format!("{column} = '");
        let start = sql.find(&marker)? + marker.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    fn stage_duration_us(
        prepared: &super::PreparedPublicRead,
        stage: crate::sql::explain::ExplainStage,
    ) -> Option<u64> {
        prepared
            .explain
            .stage_timings
            .iter()
            .find(|timing| timing.stage == stage)
            .map(|timing| timing.duration_us)
    }

    fn message_registered_schema_snapshot() -> String {
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
        .to_string()
    }

    async fn boot_real_backend() -> (crate::test_support::TestSqliteBackend, Session) {
        let (backend, _engine, session) = crate::test_support::boot_test_engine()
            .await
            .expect("test engine should boot");
        (backend, session)
    }

    async fn active_version_fixture() -> (
        crate::test_support::TestSqliteBackend,
        Session,
        String,
        String,
    ) {
        let (backend, session) = boot_real_backend().await;
        let version_id = session.active_version_id();
        let commit_id = active_version_commit_id(&session, &version_id).await;
        (backend, session, version_id, commit_id)
    }

    async fn active_version_commit_id(session: &Session, version_id: &str) -> String {
        let result = session
            .execute(
                "SELECT commit_id FROM lix_version WHERE id = $1 LIMIT 1",
                &[Value::Text(version_id.to_string())],
            )
            .await
            .expect("active version query should succeed");
        let row = result.statements[0]
            .rows
            .first()
            .expect("active version should exist");
        match &row[0] {
            Value::Text(commit_id) => commit_id.clone(),
            other => panic!("expected active version commit id text, got {other:?}"),
        }
    }

    fn run_with_large_stack<T, F>(run: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(run)
            .expect("test thread should spawn")
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    #[tokio::test]
    async fn records_surface_read_freshness_contracts() {
        let backend = FakeBackend::default();

        let derived_read = prepare_public_read(
            &backend,
            &parse_one("SELECT key FROM lix_key_value"),
            &[],
            "main",
            None,
        )
        .await
        .expect("derived entity read should prepare");
        assert_eq!(
            derived_read.freshness_contract,
            SurfaceReadFreshness::RequiresFreshProjection
        );

        let canonical_read = prepare_public_read(
            &backend,
            &parse_one("SELECT COUNT(*) FROM lix_change"),
            &[],
            "main",
            None,
        )
        .await
        .expect("canonical change read should prepare");
        assert_eq!(
            canonical_read.freshness_contract,
            SurfaceReadFreshness::AllowsStaleProjection
        );

        let admin_read = prepare_public_read(
            &backend,
            &parse_one("SELECT id, commit_id FROM lix_version WHERE name = 'main'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("canonical admin read should prepare");
        assert_eq!(
            admin_read.freshness_contract,
            SurfaceReadFreshness::AllowsStaleProjection
        );
    }

    #[test]
    fn stale_live_state_blocks_projection_reads_but_not_canonical_change_reads() {
        run_with_large_stack(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime should build");
            runtime.block_on(async {
                let (backend, session) = boot_real_backend().await;
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('freshness-check', '1')",
                        &[],
                    )
                    .await
                    .expect("tracked write should succeed before staleness test");
                crate::live_state::mark_mode_with_backend(
                    &backend,
                    crate::live_state::LiveStateMode::NeedsRebuild,
                )
                .await
                .expect("marking live_state stale should succeed");

                let stale_error = session
                    .execute(
                        "SELECT key FROM lix_key_value WHERE key = 'freshness-check'",
                        &[],
                    )
                    .await
                    .expect_err("projection-backed state read should reject stale live_state");
                assert_eq!(
                    stale_error.code,
                    crate::errors::ErrorCode::LiveStateNotReady.as_str()
                );
                assert!(
                    stale_error.description.contains("lix_key_value"),
                    "stale read error should name the projection-backed surface: {}",
                    stale_error.description
                );

                let canonical_result = session
                    .execute("SELECT COUNT(*) FROM lix_change", &[])
                    .await
                    .expect("canonical change read should still succeed while live_state is stale");
                let count = match &canonical_result.statements[0].rows[0][0] {
                    Value::Integer(value) => *value,
                    other => panic!("expected integer row count, got {other:?}"),
                };
                assert!(count > 0, "canonical change read should still return rows");

                let admin_result = session
                    .execute(
                        "SELECT id, commit_id FROM lix_version WHERE name = 'main'",
                        &[],
                    )
                    .await
                    .expect("canonical admin read should still succeed while live_state is stale");
                assert_eq!(admin_result.statements[0].rows.len(), 1);
            });
        });
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_key_value"]);
        assert_eq!(
            prepared
                .explain
                .routing_passes
                .iter()
                .map(|pass| pass.name)
                .collect::<Vec<_>>(),
            vec!["public-read.route-execution-strategy"]
        );
        assert_eq!(
            prepared
                .dependency_spec()
                .expect("dependency spec should be derived")
                .schema_keys
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
        assert!(prepared
            .dependency_spec()
            .expect("dependency spec should be derived")
            .session_dependencies
            .contains(&crate::session::contracts::SessionDependency::ActiveVersion));
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
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            vec!["key = 'hello'".to_string()]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
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
        backend
            .registered_schema_rows
            .insert("message".to_string(), message_registered_schema_snapshot());

        let prepared = prepare_public_read(
            &backend,
            &parse_one("SELECT body FROM message WHERE id = 'm1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("registered-schema entity read should canonicalize");

        assert_eq!(prepared.surface_bindings(), vec!["message"]);
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
        assert!(prepared.dependency_spec().is_some());
        assert!(prepared.effective_state_plan().is_some());
        let lowered_sql = prepared
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("registered-schema entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_live_v1_message"));
    }

    #[tokio::test]
    async fn explain_specialized_registered_schema_reads_charge_layout_loading_to_capability_resolution(
    ) {
        let delay = Duration::from_millis(150);
        let mut backend = FakeBackend::default().with_registered_schema_delay(delay);
        backend
            .registered_schema_rows
            .insert("message".to_string(), message_registered_schema_snapshot());

        let prepared = prepare_public_read(
            &backend,
            &parse_one("SELECT body FROM message WHERE id = 'm1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("registered-schema specialized read should prepare");

        assert!(
            prepared.structured_read().is_some(),
            "specialized registered-schema read should stay on the structured path"
        );
        assert!(
            backend.registered_schema_query_count() > 0,
            "registered-schema read should fetch schema state from the backend"
        );

        let capability_resolution = stage_duration_us(
            &prepared,
            crate::sql::explain::ExplainStage::CapabilityResolution,
        )
        .expect("specialized lowered read should record capability_resolution");
        let routing = stage_duration_us(&prepared, crate::sql::explain::ExplainStage::Routing)
            .expect("specialized lowered read should record routing timing");

        assert!(
            capability_resolution >= (delay.as_micros() / 2) as u64,
            "capability_resolution should absorb the injected schema-load delay: {capability_resolution}us"
        );
        assert!(
            routing < (delay.as_micros() / 2) as u64,
            "routing should stay below the injected schema-load delay when capability loading is timed separately: {routing}us"
        );
    }

    #[tokio::test]
    async fn explain_broad_registered_schema_reads_charge_layout_loading_to_capability_resolution()
    {
        let delay = Duration::from_millis(150);
        let mut backend = FakeBackend::default().with_registered_schema_delay(delay);
        backend
            .registered_schema_rows
            .insert("message".to_string(), message_registered_schema_snapshot());

        let prepared = prepare_public_read_strict(
            &backend,
            &parse_one(
                "SELECT body, COUNT(*) \
                 FROM message \
                 WHERE id = 'm1' \
                 GROUP BY body \
                 HAVING COUNT(*) > 0 \
                 ORDER BY body",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("registered-schema broad read should not error")
        .expect("registered-schema broad read should prepare through surface lowering");

        assert!(
            prepared.structured_read().is_none(),
            "group-by/having registered-schema read should route through broad lowering"
        );
        assert!(
            backend.registered_schema_query_count() > 0,
            "broad registered-schema read should fetch schema state from the backend"
        );

        let capability_resolution = stage_duration_us(
            &prepared,
            crate::sql::explain::ExplainStage::CapabilityResolution,
        )
        .expect("broad lowered read should record capability_resolution");
        let routing = stage_duration_us(&prepared, crate::sql::explain::ExplainStage::Routing)
            .expect("broad lowered read should record routing timing");

        assert!(
            capability_resolution >= (delay.as_micros() / 2) as u64,
            "capability_resolution should absorb the injected schema-load delay: {capability_resolution}us"
        );
        assert!(
            routing < (delay.as_micros() / 2) as u64,
            "routing should stay below the injected schema-load delay when capability loading is timed separately: {routing}us"
        );
    }

    #[tokio::test]
    async fn explain_broad_reads_charge_routing_delay_to_routing_stage() {
        let delay = Duration::from_millis(150);
        let backend = FakeBackend::default();
        let _routing_delay_guard = delay_broad_routing_for_test(delay);

        let prepared = prepare_public_read_strict(
            &backend,
            &parse_one(
                "SELECT \
                   (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent') AS parent_change_id, \
                   EXISTS (SELECT 1 FROM lix_directory WHERE id = 'dir-stable-child') AS has_child_dir, \
                   'file-stable-child' IN (SELECT id FROM lix_file WHERE path = '/hello.txt') AS has_file",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("broad read with nested subqueries should not error")
        .expect("broad read with nested subqueries should prepare");

        let routing = stage_duration_us(&prepared, crate::sql::explain::ExplainStage::Routing)
            .expect("broad explain should record routing timing");
        let physical_planning = stage_duration_us(
            &prepared,
            crate::sql::explain::ExplainStage::PhysicalPlanning,
        )
        .expect("broad explain should record physical_planning timing");

        assert!(
            routing >= (delay.as_micros() / 2) as u64,
            "routing should absorb the injected routing delay: {routing}us"
        );
        assert!(
            physical_planning < (delay.as_micros() / 2) as u64,
            "physical_planning should stay below the injected routing delay when routing work is timed separately: {physical_planning}us"
        );
    }

    #[test]
    fn lowers_backend_registered_public_queries_with_public_surface_lowering() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let (backend, session) = boot_real_backend().await;
                    session
                        .execute(
                            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                            &[Value::Json(json!({
                                "x-lix-key": "message",
                                "x-lix-version": "1",
                                "type": "object",
                                "properties": {
                                    "id": { "type": "string" },
                                    "body": { "type": "string" }
                                },
                                "required": ["id", "body"],
                                "additionalProperties": false
                            }))],
                        )
                        .await
                        .expect("schema registration write should succeed");
                    let prepared = prepare_public_read_strict(
                        &backend,
                        &parse_one("SELECT body FROM message WHERE id = 'm1'"),
                        &[],
                        &session.active_version_id(),
                        None,
                    )
                    .await
                    .expect("registered-schema derived public query should prepare through backend registry")
                    .expect("registered-schema derived public query should lower through backend registry");
                    let lowered_sql = prepared
                        .explain.executor_artifacts.lowered_sql
                        .first()
                        .expect("registered-schema derived public query should lower");

                    assert_eq!(
                        prepared
                            .dependency_spec()
                            .expect("dependency spec should be recorded")
                            .schema_keys
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>(),
                        vec!["message".to_string()]
                    );
                    assert!(lowered_sql.contains("lix_internal_live_v1_message"));
                    assert!(!lowered_sql.contains("FROM message"));
                })
        });
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
            prepared.surface_bindings(),
            vec!["lix_key_value_by_version"]
        );
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            vec![
                "key = 'hello'".to_string(),
                "lixcol_version_id = 'version-a'".to_string()
            ]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("entity by-version read should lower");
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("version_id AS lixcol_version_id"));
    }

    #[test]
    fn prepares_builtin_entity_history_reads() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let (backend, _session, active_version_id, active_commit_id) =
                        active_version_fixture().await;
                    let prepared = prepare_public_read(
                        &backend,
                        &parse_one(
                            "SELECT key, value, lixcol_commit_id, lixcol_depth \
                             FROM lix_key_value_history \
                             WHERE key = 'hello' \
                             ORDER BY lixcol_depth ASC",
                        ),
                        &[],
                        &active_version_id,
                        None,
                    )
                    .await
                    .expect("builtin entity history read should canonicalize");

                    assert_eq!(prepared.surface_bindings(), vec!["lix_key_value_history"]);
                    assert_eq!(
                        prepared
                            .explain
                            .routing_passes
                            .iter()
                            .map(|pass| pass.name)
                            .collect::<Vec<_>>(),
                        vec!["public-read.route-execution-strategy"]
                    );
                    assert_eq!(
                        prepared
                            .explain
                            .executor_artifacts
                            .pushdown
                            .as_ref()
                            .expect("pushdown trace should be recorded")
                            .residual_predicates
                            .clone(),
                        vec!["key = 'hello'".to_string()]
                    );
                    match &prepared.execution {
                        PreparedPublicReadExecution::Direct(
                            DirectPublicReadPlan::EntityHistory(plan),
                        ) => {
                            assert_eq!(
                                plan.request.root_scope,
                                StateHistoryRootScope::RequestedRoots(vec![active_commit_id])
                            );
                            assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
                        }
                        _ => {
                            panic!("entity history read should use direct entity-history execution")
                        }
                    }
                })
        });
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_change"]);
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .dependency_spec()
                .expect("change read should derive dependency spec")
                .relations,
            ["lix_change".to_string()].into_iter().collect()
        );
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            vec!["entity_id = 'entity-1'".to_string()]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_working_changes"]);
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .dependency_spec()
                .expect("working-changes dependency spec should be recorded")
                .precision,
            DependencyPrecision::Conservative
        );
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("working-changes read should lower");
        assert!(lowered_sql.contains("FROM lix_internal_last_checkpoint"));
        assert!(lowered_sql.contains("tip_ancestry_walk AS"));
        assert!(lowered_sql.contains("baseline_ancestry_walk AS"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_commit_edge"));
        assert!(!lowered_sql.contains("lix_internal_live_state_status"));
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_file"]);
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .dependency_spec()
                .expect("filesystem dependency spec should be recorded")
                .schema_keys
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "lix_binary_blob_ref".to_string(),
                "lix_directory_descriptor".to_string(),
                "lix_file_descriptor".to_string(),
            ]
        );
        assert!(prepared
            .dependency_spec()
            .expect("filesystem dependency spec should be recorded")
            .session_dependencies
            .contains(&crate::session::contracts::SessionDependency::ActiveVersion));
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            vec!["id = 'file-1'".to_string()]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
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
            prepared.surface_bindings(),
            vec!["lix_directory_by_version"]
        );
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            vec![
                "id = 'dir-1'".to_string(),
                "lixcol_version_id = 'version-a'".to_string()
            ]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_file_history"]);
        assert!(prepared.effective_state_request().is_none());
        assert!(prepared.effective_state_plan().is_none());
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
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
                assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
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
            prepared.surface_bindings(),
            vec!["lix_file_history_by_version"]
        );
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
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
                assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_directory_history"]);
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
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
                assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
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

    #[test]
    fn binds_active_root_commit_for_filesystem_history_reads_without_explicit_root() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let (backend, _session, active_version_id, active_commit_id) =
                        active_version_fixture().await;

                    let prepared = prepare_public_read(
                        &backend,
                        &parse_one(
                            "SELECT id, path, lixcol_commit_id, lixcol_depth \
                             FROM lix_file_history \
                             WHERE id = 'file-1' \
                             ORDER BY lixcol_depth ASC",
                        ),
                        &[],
                        &active_version_id,
                        None,
                    )
                    .await
                    .expect("filesystem history read should canonicalize");

                    match &prepared.execution {
                        PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(
                            plan,
                        )) => {
                            assert_eq!(
                                plan.request.root_scope,
                                crate::filesystem::history::FileHistoryRootScope::RequestedRoots(
                                    vec![active_commit_id]
                                )
                            );
                        }
                        PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(
                            _,
                        )) => {
                            panic!("filesystem history read should not use state-history direct plan")
                        }
                        PreparedPublicReadExecution::Direct(DirectPublicReadPlan::EntityHistory(
                            _,
                        )) => {
                            panic!("filesystem history read should not use entity-history direct plan")
                        }
                        PreparedPublicReadExecution::Direct(
                            DirectPublicReadPlan::DirectoryHistory(_),
                        ) => {
                            panic!("filesystem history read should not use directory-history direct plan")
                        }
                        PreparedPublicReadExecution::LoweredSql(_) => {
                            panic!("filesystem history read should not use lowered SQL")
                        }
                    }
                })
        });
    }

    #[test]
    fn binds_active_root_commit_for_entity_history_reads_without_explicit_root() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let (backend, _session, active_version_id, active_commit_id) =
                        active_version_fixture().await;

                    let prepared = prepare_public_read(
                        &backend,
                        &parse_one(
                            "SELECT key, value, lixcol_commit_id, lixcol_depth \
                             FROM lix_key_value_history \
                             WHERE key = 'hello' \
                             ORDER BY lixcol_depth ASC",
                        ),
                        &[],
                        &active_version_id,
                        None,
                    )
                    .await
                    .expect("entity history read should canonicalize");

                    match &prepared.execution {
                        PreparedPublicReadExecution::Direct(
                            DirectPublicReadPlan::EntityHistory(plan),
                        ) => {
                            assert_eq!(
                                plan.request.root_scope,
                                StateHistoryRootScope::RequestedRoots(vec![active_commit_id])
                            );
                            assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
                        }
                        _ => {
                            panic!("entity history read should use direct entity-history execution")
                        }
                    }
                })
        });
    }

    #[test]
    fn prepares_explain_over_history_surfaces_without_backend_rewrite() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let backend = FakeBackend::default();
                    let prepared = prepare_public_execution(
                        &backend,
                        &parse_one(
                            "EXPLAIN SELECT key FROM lix_key_value_history \
                             WHERE root_commit_id = 'root-1' AND key = 'hello'",
                        ),
                        &[],
                        "main",
                        &[],
                        None,
                    )
                    .await
                    .expect("history EXPLAIN should prepare")
                    .expect("history EXPLAIN should route through public execution");

                    match prepared {
                        PreparedPublicExecution::Read(prepared) => {
                            assert_eq!(prepared.surface_bindings(), vec!["lix_key_value_history"]);
                            assert!(
                                prepared.explain.executor_artifacts.lowered_sql.is_empty(),
                                "history EXPLAIN should stay on direct execution instead of lowering backend SQL"
                            );
                            assert!(
                                stage_duration_us(
                                    &prepared,
                                    crate::sql::explain::ExplainStage::CapabilityResolution,
                                )
                                .is_none(),
                                "direct-history EXPLAIN should not record capability_resolution timing"
                            );
                            assert!(
                                stage_duration_us(
                                    &prepared,
                                    crate::sql::explain::ExplainStage::ExecutorPreparation,
                                )
                                .is_none(),
                                "direct-history EXPLAIN should not record executor_preparation timing"
                            );
                        }
                        PreparedPublicExecution::Write(_) => {
                            panic!("history EXPLAIN must not route through public write execution")
                        }
                    }
                })
        });
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

        assert_eq!(prepared.surface_bindings(), vec!["lix_state"]);
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("explain state read should lower");
        assert!(!lowered_sql.starts_with("EXPLAIN SELECT"));
        assert!(prepared.explain.request.is_some());
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
    }

    #[test]
    fn classifies_public_reads_through_public_execution() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let backend = FakeBackend::default();
                    let prepared = prepare_public_execution(
                        &backend,
                        &parse_one(
                            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
                        ),
                        &[],
                        "main",
                        &[],
                        None,
                    )
                    .await
                    .expect("public read classification should succeed");

                    assert!(matches!(prepared, Some(PreparedPublicExecution::Read(_))));
                })
        });
    }

    #[test]
    fn classifies_public_writes_through_public_execution() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let (backend, session) = boot_real_backend().await;
                    let active_version_id = session.active_version_id();
                    let prepared = prepare_public_execution(
                        &backend,
                        &parse_one(
                            "INSERT INTO lix_key_value (key, value) VALUES ('phase1-boundary', 'ok')",
                        ),
                        &[],
                        &active_version_id,
                        &[],
                        None,
                    )
                    .await
                    .expect("public write classification should succeed");

                    assert!(matches!(prepared, Some(PreparedPublicExecution::Write(_))));
                })
        });
    }

    #[test]
    fn read_only_public_writes_are_owned_by_public_lowering_and_rejected_semantically() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let backend = FakeBackend::default();
                    let error = prepare_public_execution(
                        &backend,
                        &parse_one(
                            "INSERT INTO lix_change (id, entity_id, schema_key, schema_version, file_id, plugin_key, created_at) \
                             VALUES ('c1', 'e1', 's1', '1', 'lix', 'lix', '2026-01-01T00:00:00Z')",
                        ),
                        &[],
                        "main",
                        &[],
                        None,
                    )
                    .await
                    .expect_err("read-only public write should be rejected by public lowering");

                    assert_eq!(error.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
                })
        });
    }

    #[test]
    fn commit_and_change_set_public_writes_are_rejected_semantically() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let backend = FakeBackend::default();

                    for sql in [
                        "INSERT INTO lix_commit (id, change_set_id) VALUES ('c1', 'cs1')",
                        "INSERT INTO lix_change_set (id) VALUES ('cs1')",
                    ] {
                        let error = prepare_public_execution(
                            &backend,
                            &parse_one(sql),
                            &[],
                            "main",
                            &[],
                            None,
                        )
                        .await
                        .expect_err("read-only public write should be rejected by public lowering");
                        assert_eq!(error.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
                    }
                })
        });
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

        assert!(prepared.structured_read().is_none());
        assert!(prepared.dependency_spec().is_some());
        assert_eq!(
            prepared.surface_bindings(),
            vec!["lix_state", "lix_state_by_version"]
        );
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .bound_public_leaves
                .iter()
                .map(|leaf| leaf.public_name.as_str())
                .collect::<Vec<_>>(),
            vec!["lix_state", "lix_state_by_version"]
        );
        assert_eq!(
            prepared
                .dependency_spec()
                .expect("broad read should derive dependency spec")
                .precision,
            DependencyPrecision::Conservative
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("surface-expanded read should lower");
        assert!(!lowered_sql.contains("FROM lix_state "));
        assert!(!lowered_sql.contains("JOIN lix_state_by_version"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[tokio::test]
    async fn broad_surface_lowering_uses_prebound_broad_statement() {
        let backend = FakeBackend::default();
        let statement = parse_one(
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
        )
        .into_iter()
        .next()
        .expect("statement should exist");
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let bound = bind_public_read_statement(
            statement,
            Vec::new(),
            ExecutionContext {
                dialect: Some(SqlDialect::Sqlite),
                writer_key: None,
                requested_version_id: Some("main".to_string()),
                active_account_ids: Vec::new(),
            },
            &registry,
        )
        .expect("public read bind should succeed");

        let _binding_guard = forbid_broad_binding_for_test();
        let prepared = super::read::prepare_public_read_via_surface_lowering(
            &backend,
            bound.bound_statement,
            bound.broad_statement,
            None,
            &registry,
            false,
            None,
            ExplainTimingCollector::new(Some(Duration::ZERO)),
        )
        .await
        .expect("surface lowering should reuse the prebound broad statement")
        .expect("broad public read should still prepare");

        assert!(prepared.structured_read().is_none());
        assert_eq!(
            prepared.surface_bindings(),
            vec!["lix_state", "lix_state_by_version"]
        );
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

        assert!(prepared.structured_read().is_none());
        assert_eq!(prepared.surface_bindings(), vec!["lix_state"]);
        let lowered_sql = prepared
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("group-by/having read should lower");
        assert!(!lowered_sql.contains("FROM lix_state"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("GROUP BY"));
        assert!(lowered_sql.contains("HAVING"));
    }

    #[test]
    fn cte_shadowing_public_surface_names_stays_non_public() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let backend = FakeBackend::default();
                    let prepared = prepare_public_execution(
                        &backend,
                        &parse_one(
                            "WITH lix_state AS (SELECT 'shadow' AS entity_id) SELECT entity_id FROM lix_state",
                        ),
                        &[],
                        "main",
                        &[],
                        None,
                    )
                    .await
                    .expect("cte shadowing should classify cleanly");

                    assert!(prepared.is_none());
                })
        });
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
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            prepared
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .residual_predicates
                .clone(),
            Vec::<String>::new()
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
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
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
            vec![
                "version_id = 'v1'".to_string(),
                "schema_key = 'lix_key_value'".to_string()
            ]
        );
        let lowered_sql = prepared
            .explain
            .executor_artifacts
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
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
            vec!["root_commit_id = 'commit-1'".to_string()]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    StateHistoryRootScope::RequestedRoots(vec!["commit-1".to_string()])
                );
                assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
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
                .explain
                .executor_artifacts
                .pushdown
                .as_ref()
                .expect("pushdown trace should be recorded")
                .accepted_predicates
                .clone(),
            vec!["root_commit_id = 'commit-1'".to_string()]
        );
        match &prepared.execution {
            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::StateHistory(plan)) => {
                assert_eq!(
                    plan.request.root_scope,
                    StateHistoryRootScope::RequestedRoots(vec!["commit-1".to_string()])
                );
                assert!(prepared.explain.executor_artifacts.lowered_sql.is_empty());
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
            .explain
            .executor_artifacts
            .lowered_sql
            .first()
            .expect("nested filesystem subquery should lower");
        assert!(!lowered_sql.contains("FROM lix_file"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
    }

    #[test]
    fn prepares_session_runtime_functions_without_active_surfaces() {
        run_with_large_stack(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
                .block_on(async move {
                    let (backend, session) = boot_real_backend().await;
                    let active_version_id = session.active_version_id();
                    let prepared = prepare_public_read(
                        &backend,
                        &parse_one(
                            "SELECT lix_active_version_id() AS version_id \
                             FROM lix_version v \
                             WHERE v.id = lix_active_version_id() \
                               AND v.commit_id IS NOT NULL",
                        ),
                        &[],
                        &active_version_id,
                        None,
                    )
                    .await
                    .expect("session runtime function read should prepare through public lowering");

                    assert_eq!(prepared.surface_bindings(), vec!["lix_version"]);
                    if let Some(lowered_sql) =
                        prepared.explain.executor_artifacts.lowered_sql.first()
                    {
                        assert!(!lowered_sql.contains("FROM lix_version"));
                        assert!(lowered_sql.contains("FROM lix_internal_change c"));
                        assert!(lowered_sql.contains("lix_version_descriptor"));
                        assert!(lowered_sql.contains("current_refs"));
                        assert!(!lowered_sql.contains("lix_internal_live_v1_lix_version_ref"));
                    }
                })
        });
    }
}
