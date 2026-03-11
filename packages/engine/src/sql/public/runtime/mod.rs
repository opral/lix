use crate::errors::schema_not_registered_error;
use crate::errors::{
    file_data_expects_bytes_error, mixed_public_internal_query_error, read_only_view_write_error,
};
use crate::filesystem::pending_file_writes::PendingFileWrite;
use crate::sql::analysis::state_resolution::canonical::statement_targets_table_name;
use crate::sql::ast::lowering::lower_statement;
use crate::sql::common::dependency_spec::DependencySpec;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::planned_statement::SchemaRegistration;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::intent::authoritative_pending_file_write_targets;
use crate::sql::public::backend::PushdownDecision;
use crate::sql::public::catalog::{
    SurfaceCapability, SurfaceFamily, SurfaceRegistry, SurfaceVariant,
};
use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
use crate::sql::public::planner::backend::lowerer::{
    lower_read_for_execution, rewrite_supported_public_read_surfaces_in_statement_with_registry,
    LoweredReadProgram,
};
use crate::sql::public::planner::canonicalize::{
    canonicalize_read, canonicalize_write, CanonicalizedRead, CanonicalizedWrite,
};
use crate::sql::public::planner::ir::{
    CommitPreconditions, PlannedWrite, ResolvedWritePlan, SchemaProof, ScopeProof, TargetSetProof,
    WriteCommand, WriteOperationKind,
};
use crate::sql::public::planner::semantics::dependency_spec::{
    derive_dependency_spec_from_bound_public_surface_bindings,
    derive_dependency_spec_from_canonicalized_read,
};
use crate::sql::public::planner::semantics::domain_changes::{
    build_domain_change_batch, derive_commit_preconditions, DomainChangeBatch,
};
use crate::sql::public::planner::semantics::effective_state_resolver::{
    build_effective_state, EffectiveStatePlan, EffectiveStateRequest,
};
use crate::sql::public::planner::semantics::proof_engine::prove_write;
use crate::sql::public::planner::semantics::write_resolver::resolve_write_plan;
use crate::state::commit::{
    load_committed_version_tip_commit_id, AppendCommitPreconditions, AppendExpectedTip,
    AppendWriteLane, ProposedDomainChange,
};
use crate::state::stream::{
    state_commit_stream_changes_from_domain_changes, state_commit_stream_changes_from_planned_rows,
    StateCommitStreamOperation,
};
use crate::state::timeline::ensure_history_timeline_materialized_for_root;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, LimitClause, ObjectNamePart, OrderBy, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue, Visit, Visitor,
};
use std::collections::BTreeSet;
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
pub(crate) struct Sql2DebugTrace {
    pub(crate) bound_statements: Vec<BoundStatement>,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) bound_public_leaves: Vec<Sql2BoundPublicLeaf>,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
    pub(crate) pushdown_decision: Option<PushdownDecision>,
    pub(crate) write_command: Option<WriteCommand>,
    pub(crate) scope_proof: Option<ScopeProof>,
    pub(crate) schema_proof: Option<SchemaProof>,
    pub(crate) target_set_proof: Option<TargetSetProof>,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlan>,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) commit_preconditions: Option<CommitPreconditions>,
    pub(crate) invariant_trace: Option<Sql2InvariantTrace>,
    pub(crate) write_phase_trace: Vec<String>,
    pub(crate) lowered_sql: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Sql2InvariantTrace {
    pub(crate) batch_local_checks: Vec<String>,
    pub(crate) append_time_checks: Vec<String>,
    pub(crate) physical_checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2PreparedRead {
    pub(crate) canonicalized: Option<CanonicalizedRead>,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
    pub(crate) lowered_read: Option<LoweredReadProgram>,
    pub(crate) debug_trace: Sql2DebugTrace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sql2BoundPublicLeaf {
    pub(crate) public_name: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) capability: SurfaceCapability,
    pub(crate) requires_effective_state: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2PreparedWrite {
    pub(crate) canonicalized: CanonicalizedWrite,
    pub(crate) planned_write: PlannedWrite,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) execution: Option<Sql2WriteExecution>,
    pub(crate) debug_trace: Sql2DebugTrace,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Sql2WriteExecution {
    Tracked(Sql2TrackedWriteExecution),
    Untracked(Sql2UntrackedWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2TrackedWriteExecution {
    pub(crate) schema_registrations: Vec<SchemaRegistration>,
    pub(crate) domain_change_batch: DomainChangeBatch,
    pub(crate) append_preconditions: AppendCommitPreconditions,
    pub(crate) semantic_effects: PlanEffects,
    pub(crate) persist_filesystem_payloads_before_write: bool,
    pub(crate) filesystem_payload_changes_committed_by_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2UntrackedWriteExecution {
    pub(crate) intended_post_state: Vec<crate::sql::public::planner::ir::PlannedStateRow>,
    pub(crate) semantic_effects: PlanEffects,
    pub(crate) persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Sql2PreparedPublicExecution {
    Read(Sql2PreparedRead),
    Write(Sql2PreparedWrite),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BoundPublicReadSummary {
    bound_surface_bindings: Vec<crate::sql::public::catalog::SurfaceBinding>,
    internal_relations: Vec<String>,
    external_relations: Vec<String>,
}

mod read;

pub(crate) async fn prepare_sql2_public_execution(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<Sql2PreparedPublicExecution>, LixError> {
    prepare_sql2_public_execution_with_internal_access(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        false,
    )
    .await
}

pub(crate) async fn prepare_sql2_public_execution_with_internal_access(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<Sql2PreparedPublicExecution>, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    if !statements_reference_public_surface(&registry, parsed_statements) {
        return Ok(None);
    }

    if let Some(target_name) = public_write_target_name(&registry, parsed_statements) {
        let prepared = try_prepare_sql2_write(
            backend,
            parsed_statements,
            params,
            active_version_id,
            writer_key,
        )
        .await?;
        return prepared
            .map(Sql2PreparedPublicExecution::Write)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("public write target '{target_name}' must route through sql2"),
                )
            })
            .map(Some);
    }

    if parsed_statements.len() != 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public read statement batches must route through sql2 one statement at a time",
        ));
    }

    try_prepare_sql2_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
    )
    .await?
    .map(Sql2PreparedPublicExecution::Read)
    .ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public read statements must route through sql2",
        )
    })
    .map(Some)
}

pub(crate) fn statement_references_public_sql2_surface(statement: &Statement) -> bool {
    statement_references_public_surface(&SurfaceRegistry::with_builtin_surfaces(), statement)
}

pub(crate) async fn statement_references_public_sql2_surface_with_backend(
    backend: &dyn LixBackend,
    statement: &Statement,
) -> bool {
    let registry = match SurfaceRegistry::bootstrap_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => return statement_references_public_sql2_surface(statement),
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
    rewrite_supported_public_read_surfaces_in_statement_with_registry(statement, registry)?;
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

pub(crate) async fn lower_public_read_query_with_sql2_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    read::lower_public_read_query_with_sql2_backend(backend, query, params).await
}

async fn try_prepare_sql2_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<Sql2PreparedRead>, LixError> {
    read::try_prepare_sql2_read_with_internal_access(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
    )
    .await
}

fn sql2_public_read_preflight_error(
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

pub(crate) async fn prepare_sql2_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<Sql2PreparedRead> {
    read::prepare_sql2_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}

pub(crate) async fn prepare_sql2_read_strict(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<Sql2PreparedRead>, LixError> {
    read::prepare_sql2_read_strict(
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
    summarize_bound_public_read_query(registry, query)
}

fn summarize_bound_public_read_query(
    registry: &SurfaceRegistry,
    query: &Query,
) -> BoundPublicReadSummary {
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
    }
}

fn sql2_bound_public_leaf(
    binding: &crate::sql::public::catalog::SurfaceBinding,
) -> Sql2BoundPublicLeaf {
    Sql2BoundPublicLeaf {
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

fn bound_public_surface_names(registry: &SurfaceRegistry, statement: &Statement) -> Vec<String> {
    summarize_bound_public_read_statement(registry, statement)
        .bound_surface_bindings
        .into_iter()
        .map(|binding| binding.descriptor.public_name)
        .collect()
}

async fn load_active_version_id_for_sql2_read(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    let result = backend
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_untracked \
             WHERE schema_key = $1 \
               AND file_id = $2 \
               AND version_id = $3 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[
                Value::Text(crate::version::active_version_schema_key().to_string()),
                Value::Text(crate::version::active_version_file_id().to_string()),
                Value::Text(crate::version::active_version_storage_version_id().to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(crate::version::DEFAULT_ACTIVE_VERSION_NAME.to_string());
    };
    let snapshot_content = row.first().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "active version query row is missing snapshot_content",
        )
    })?;
    let snapshot_content = match snapshot_content {
        Value::Text(value) => value.as_str(),
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("active version snapshot_content must be text, got {other:?}"),
            ))
        }
    };
    crate::version::parse_active_version_snapshot(snapshot_content)
}

async fn maybe_bind_active_history_root(
    backend: &dyn LixBackend,
    canonicalized: CanonicalizedRead,
    active_version_id: &str,
    registry: &SurfaceRegistry,
) -> Option<CanonicalizedRead> {
    let descriptor = &canonicalized.surface_binding.descriptor;
    let public_name = descriptor.public_name.as_str();
    let uses_active_history_root = descriptor.surface_variant == SurfaceVariant::History
        && matches!(
            descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
        )
        && !public_name.ends_with("_history_by_version");
    if !uses_active_history_root {
        return Some(canonicalized);
    }
    if statement_has_root_commit_predicate(&canonicalized.bound_statement.statement) {
        return Some(canonicalized);
    }

    let mut executor = backend;
    let root_commit_id = load_committed_version_tip_commit_id(&mut executor, active_version_id)
        .await
        .ok()??;
    let mut rebound = canonicalized.bound_statement.clone();
    let Statement::Query(query) = &mut rebound.statement else {
        return Some(canonicalized);
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Some(canonicalized);
    };
    let root_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("lixcol_root_commit_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            SqlValue::SingleQuotedString(root_commit_id).into(),
        )),
    };
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(root_predicate),
        },
        None => root_predicate,
    });
    canonicalize_read(rebound, registry).ok()
}

fn statement_has_root_commit_predicate(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select
        .selection
        .as_ref()
        .map(expr_has_root_commit_predicate)
        .unwrap_or(false)
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

async fn ensure_sql2_history_timeline_roots(
    backend: &dyn LixBackend,
    statement: &Statement,
) -> Result<(), LixError> {
    for root_commit_id in requested_history_root_commit_ids(statement) {
        ensure_history_timeline_materialized_for_root(backend, &root_commit_id, 512).await?;
    }
    Ok(())
}

fn requested_history_root_commit_ids(statement: &Statement) -> Vec<String> {
    let Statement::Query(query) = statement else {
        return Vec::new();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };
    let mut roots = std::collections::BTreeSet::new();
    if let Some(selection) = select.selection.as_ref() {
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
    canonicalized: &CanonicalizedRead,
    dependency_spec: Option<DependencySpec>,
) -> Option<DependencySpec> {
    let dependency_spec = dependency_spec?;
    augment_dependency_spec_for_broad_public_read(registry, Some(dependency_spec)).map(
        |mut dependency_spec| {
            let has_state_schema_keys = dependency_spec
                .schema_keys
                .iter()
                .any(|schema_key| schema_key != "lix_active_version");
            if canonicalized.surface_binding.descriptor.surface_family == SurfaceFamily::State
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
    }
}

pub(crate) async fn try_prepare_sql2_write(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<Sql2PreparedWrite>, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(None);
    }

    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
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
                    if let Some(error) = sql2_public_write_preparation_error_for_surface(
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
                    return Err(sql2_filesystem_write_error(target_name, &error.message));
                }
                None => return Ok(None),
            }
        }
    };
    let mut planned_write = match prove_write(&canonicalized) {
        Ok(planned_write) => planned_write,
        Err(error) => {
            if let Some(error) = sql2_public_write_preparation_error(&canonicalized, &error.message)
            {
                return Err(error);
            }
            return Ok(None);
        }
    };
    let resolved_write_plan = match resolve_write_plan(backend, &planned_write).await {
        Ok(resolved_write_plan) => resolved_write_plan,
        Err(error) => match sql2_authoritative_write_error(&canonicalized, error.message) {
            Some(error) => return Err(error),
            None => return Ok(None),
        },
    };
    planned_write.resolved_write_plan = Some(resolved_write_plan.clone());
    let domain_change_batch = match build_domain_change_batch(&planned_write) {
        Ok(domain_change_batch) => domain_change_batch,
        Err(error) => {
            if let Some(error) = sql2_public_write_preparation_error(&canonicalized, &error.message)
            {
                return Err(error);
            }
            return Ok(None);
        }
    };
    let commit_preconditions = match derive_commit_preconditions(backend, &planned_write).await {
        Ok(commit_preconditions) => commit_preconditions,
        Err(error) => {
            if let Some(error) = sql2_public_write_preparation_error(&canonicalized, &error.message)
            {
                return Err(error);
            }
            return Ok(None);
        }
    };
    planned_write.commit_preconditions = commit_preconditions.clone();
    let invariant_trace = Some(build_sql2_invariant_trace(&planned_write));
    let execution = build_sql2_write_execution(
        &bound_statement.statement,
        &planned_write,
        domain_change_batch.as_ref(),
        commit_preconditions.as_ref(),
    )?;

    Ok(Some(Sql2PreparedWrite {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            bound_public_leaves: vec![sql2_bound_public_leaf(&canonicalized.surface_binding)],
            dependency_spec: None,
            effective_state_request: None,
            effective_state_plan: None,
            pushdown_decision: None,
            write_command: Some(canonicalized.write_command.clone()),
            scope_proof: Some(planned_write.scope_proof.clone()),
            schema_proof: Some(planned_write.schema_proof.clone()),
            target_set_proof: planned_write.target_set_proof.clone(),
            resolved_write_plan: Some(resolved_write_plan),
            domain_change_batch: domain_change_batch.clone(),
            commit_preconditions: commit_preconditions.clone(),
            invariant_trace,
            write_phase_trace: sql2_write_phase_trace(),
            lowered_sql: Vec::new(),
        },
        planned_write,
        domain_change_batch,
        execution,
        canonicalized,
    }))
}

pub(crate) async fn prepare_sql2_write(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<Sql2PreparedWrite> {
    try_prepare_sql2_write(
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

fn build_sql2_write_execution(
    statement: &Statement,
    planned_write: &PlannedWrite,
    domain_change_batch: Option<&DomainChangeBatch>,
    commit_preconditions: Option<&CommitPreconditions>,
) -> Result<Option<Sql2WriteExecution>, LixError> {
    if !matches!(
        write_result_contract(statement),
        ResultContract::DmlNoReturning
    ) {
        return Ok(None);
    }

    let Some(resolved) = planned_write.resolved_write_plan.as_ref() else {
        return Ok(None);
    };
    match resolved.execution_mode {
        crate::sql::public::planner::ir::WriteMode::Tracked => {
            let Some(domain_change_batch) = domain_change_batch.cloned() else {
                return Ok(None);
            };
            let Some(commit_preconditions) = commit_preconditions else {
                return Ok(None);
            };
            if !tracked_sql2_operation_supported(planned_write) {
                return Ok(None);
            }

            Ok(Some(Sql2WriteExecution::Tracked(
                Sql2TrackedWriteExecution {
                    schema_registrations: sql2_schema_registrations_from_planned_write(
                        planned_write,
                    ),
                    append_preconditions: append_commit_preconditions_for_sql2_write(
                        planned_write,
                        &domain_change_batch,
                        commit_preconditions,
                    )?,
                    semantic_effects: semantic_plan_effects_from_domain_changes(
                        &domain_change_batch.changes,
                        state_commit_stream_operation(planned_write.command.operation_kind),
                    )?,
                    persist_filesystem_payloads_before_write: sql2_persists_filesystem_payloads(
                        planned_write,
                    ),
                    filesystem_payload_changes_committed_by_write:
                        sql2_commits_filesystem_payload_domain_changes(planned_write),
                    domain_change_batch,
                },
            )))
        }
        crate::sql::public::planner::ir::WriteMode::Untracked => {
            if !sql2_untracked_operation_supported(planned_write) {
                return Ok(None);
            }
            Ok(Some(Sql2WriteExecution::Untracked(
                Sql2UntrackedWriteExecution {
                    intended_post_state: resolved.intended_post_state.clone(),
                    semantic_effects: PlanEffects::default(),
                    persist_filesystem_payloads_before_write: sql2_persists_filesystem_payloads(
                        planned_write,
                    ),
                },
            )))
        }
    }
}

pub(crate) fn finalize_sql2_write_execution(
    execution: &mut Sql2WriteExecution,
    planned_write: &PlannedWrite,
    pending_file_writes: &[PendingFileWrite],
    pending_file_delete_targets: &BTreeSet<(String, String)>,
) -> Result<(), LixError> {
    if let Sql2WriteExecution::Untracked(untracked) = execution {
        untracked.semantic_effects = semantic_plan_effects_from_untracked_sql2_write(
            planned_write,
            &untracked.intended_post_state,
            pending_file_writes,
            pending_file_delete_targets,
        )?;
    }
    Ok(())
}

fn write_result_contract(statement: &Statement) -> ResultContract {
    match statement {
        Statement::Insert(insert) => {
            if insert.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Statement::Update(update) => {
            if update.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Statement::Delete(delete) => {
            if delete.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        _ => ResultContract::Other,
    }
}

fn sql2_schema_registrations_from_planned_write(
    planned_write: &PlannedWrite,
) -> Vec<SchemaRegistration> {
    let mut schema_keys = BTreeSet::new();
    let Some(resolved) = planned_write.resolved_write_plan.as_ref() else {
        return Vec::new();
    };
    for row in &resolved.intended_post_state {
        if row.schema_key != "lix_stored_schema" {
            schema_keys.insert(row.schema_key.clone());
        }

        if row.schema_key != "lix_stored_schema" || row.tombstone {
            continue;
        }

        let Some(snapshot_content) = planned_row_optional_json_text_value(row, "snapshot_content")
        else {
            continue;
        };
        let Ok(snapshot) = serde_json::from_str(&snapshot_content) else {
            continue;
        };
        let Ok((schema_key, _)) = crate::schema::schema_from_stored_snapshot(&snapshot) else {
            continue;
        };
        schema_keys.insert(schema_key.schema_key);
    }

    schema_keys
        .into_iter()
        .map(|schema_key| SchemaRegistration { schema_key })
        .collect()
}

fn tracked_sql2_operation_supported(planned_write: &PlannedWrite) -> bool {
    match planned_write.command.operation_kind {
        WriteOperationKind::Insert => true,
        WriteOperationKind::Update | WriteOperationKind::Delete => matches!(
            planned_write
                .commit_preconditions
                .as_ref()
                .map(|preconditions| &preconditions.write_lane),
            Some(crate::sql::public::planner::ir::WriteLane::SingleVersion(_))
                | Some(crate::sql::public::planner::ir::WriteLane::ActiveVersion)
                | Some(crate::sql::public::planner::ir::WriteLane::GlobalAdmin)
        ),
    }
}

fn sql2_untracked_operation_supported(planned_write: &PlannedWrite) -> bool {
    matches!(
        planned_write.command.target.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
    ) || matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_active_version" | "lix_active_account"
    )
}

fn sql2_commits_filesystem_payload_domain_changes(planned_write: &PlannedWrite) -> bool {
    matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        planned_write
            .resolved_write_plan
            .as_ref()
            .map(|plan| plan.execution_mode),
        Some(crate::sql::public::planner::ir::WriteMode::Tracked)
    )
}

fn sql2_persists_filesystem_payloads(planned_write: &PlannedWrite) -> bool {
    matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        planned_write
            .resolved_write_plan
            .as_ref()
            .map(|plan| plan.execution_mode),
        Some(crate::sql::public::planner::ir::WriteMode::Tracked)
            | Some(crate::sql::public::planner::ir::WriteMode::Untracked)
    )
}

fn state_commit_stream_operation(operation_kind: WriteOperationKind) -> StateCommitStreamOperation {
    match operation_kind {
        WriteOperationKind::Insert => StateCommitStreamOperation::Insert,
        WriteOperationKind::Update => StateCommitStreamOperation::Update,
        WriteOperationKind::Delete => StateCommitStreamOperation::Delete,
    }
}

fn append_commit_preconditions_for_sql2_write(
    planned_write: &PlannedWrite,
    batch: &DomainChangeBatch,
    commit_preconditions: &CommitPreconditions,
) -> Result<AppendCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        crate::sql::public::planner::ir::WriteLane::SingleVersion(version_id) => {
            AppendWriteLane::Version(version_id.clone())
        }
        crate::sql::public::planner::ir::WriteLane::ActiveVersion => {
            let version_id = batch
                .changes
                .first()
                .map(|change| change.version_id.clone())
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
                        "sql2 append execution requires a concrete active version id",
                    )
                })?;
            AppendWriteLane::Version(version_id)
        }
        crate::sql::public::planner::ir::WriteLane::GlobalAdmin => AppendWriteLane::GlobalAdmin,
    };
    let expected_tip = match &commit_preconditions.expected_tip {
        crate::sql::public::planner::ir::ExpectedTip::CommitId(commit_id) => {
            AppendExpectedTip::CommitId(commit_id.clone())
        }
        crate::sql::public::planner::ir::ExpectedTip::CreateIfMissing => {
            AppendExpectedTip::CreateIfMissing
        }
    };

    Ok(AppendCommitPreconditions {
        write_lane,
        expected_tip,
        idempotency_key: commit_preconditions.idempotency_key.0.clone(),
    })
}

fn semantic_plan_effects_from_untracked_sql2_write(
    planned_write: &PlannedWrite,
    intended_post_state: &[crate::sql::public::planner::ir::PlannedStateRow],
    pending_file_writes: &[PendingFileWrite],
    pending_file_delete_targets: &BTreeSet<(String, String)>,
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
        effects.file_cache_refresh_targets =
            authoritative_pending_file_write_targets(pending_file_writes);
        effects
            .file_cache_refresh_targets
            .extend(pending_file_delete_targets.iter().cloned());
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

fn semantic_plan_effects_from_domain_changes(
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
                .map(|file_id| (file_id.clone(), change.version_id.clone()))
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

fn sql2_authoritative_write_error(
    canonicalized: &CanonicalizedWrite,
    message: String,
) -> Option<LixError> {
    sql2_public_write_preparation_error(canonicalized, &message)
}

fn sql2_public_write_preparation_error(
    canonicalized: &CanonicalizedWrite,
    message: &str,
) -> Option<LixError> {
    sql2_public_write_preparation_error_for_surface(
        &canonicalized.surface_binding,
        canonicalized.write_command.operation_kind,
        message,
    )
}

fn sql2_public_write_preparation_error_for_surface(
    surface_binding: &crate::sql::public::catalog::SurfaceBinding,
    operation_kind: WriteOperationKind,
    message: &str,
) -> Option<LixError> {
    let public_name = surface_binding.descriptor.public_name.as_str();
    if surface_binding.descriptor.capability == SurfaceCapability::ReadOnly
        || message.contains("is not writable in sql2")
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
    if message.contains("write proof requires version_id") && public_name.ends_with("_by_version") {
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
        SurfaceFamily::Filesystem => Some(sql2_filesystem_write_error(public_name, message)),
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
            .strip_prefix("sql2 version ")
            .map(|suffix| std::borrow::Cow::Owned(format!("{public_name} {suffix}")))
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

fn sql2_filesystem_write_error(target_name: &str, message: &str) -> LixError {
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

fn sql2_write_phase_trace() -> Vec<String> {
    vec![
        "canonicalize_write".to_string(),
        "prove_write".to_string(),
        "resolve_authoritative_pre_state".to_string(),
        "build_domain_change_batch".to_string(),
        "derive_commit_preconditions".to_string(),
        "validate_batch_local_write".to_string(),
        "append_time_invariant_recheck".to_string(),
        "append_commit_if_preconditions_hold".to_string(),
    ]
}

fn build_sql2_invariant_trace(planned_write: &PlannedWrite) -> Sql2InvariantTrace {
    let mut batch_local_checks = Vec::new();
    let mut append_time_checks = vec![
        "write_lane.tip_precondition".to_string(),
        "idempotency_key.recheck".to_string(),
    ];
    let mut physical_checks = Vec::new();

    if planned_write.command.operation_kind
        == crate::sql::public::planner::ir::WriteOperationKind::Update
    {
        append_time_checks.push("schema_mutability.recheck".to_string());
    }

    if let Some(resolved) = planned_write.resolved_write_plan.as_ref() {
        let mut saw_snapshot_validation = false;
        let mut saw_primary_key_consistency = false;
        let mut saw_stored_schema_definition = false;
        let mut saw_stored_schema_bootstrap_identity = false;

        for row in &resolved.intended_post_state {
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
            if row.schema_key == "lix_stored_schema" {
                if !saw_stored_schema_definition {
                    batch_local_checks.push("stored_schema.definition_validation".to_string());
                    saw_stored_schema_definition = true;
                }
                if !saw_stored_schema_bootstrap_identity {
                    batch_local_checks.push("stored_schema.bootstrap_identity".to_string());
                    saw_stored_schema_bootstrap_identity = true;
                }
            }
        }
    }

    if planned_write
        .resolved_write_plan
        .as_ref()
        .map(|plan| plan.execution_mode != crate::sql::public::planner::ir::WriteMode::Untracked)
        .unwrap_or(true)
    {
        physical_checks.push("backend_constraints.defense_in_depth".to_string());
    }

    Sql2InvariantTrace {
        batch_local_checks,
        append_time_checks,
        physical_checks,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        lower_public_read_query_with_sql2_backend, prepare_sql2_public_execution,
        prepare_sql2_public_execution_with_internal_access, prepare_sql2_read,
        prepare_sql2_read_strict, prepare_sql2_write, Sql2PreparedPublicExecution,
        Sql2WriteExecution,
    };
    use crate::sql::public::catalog::SurfaceRegistry;
    use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql::public::planner::canonicalize::canonicalize_write;
    use crate::sql::public::planner::ir::{
        ExpectedTip, ScopeProof, WriteLane, WriteMode, WriteModeRequest,
    };
    use crate::sql::public::planner::semantics::domain_changes::{
        build_domain_change_batch, derive_commit_preconditions,
    };
    use crate::sql::public::planner::semantics::proof_engine::prove_write;
    use crate::sql::public::planner::semantics::write_resolver::resolve_write_plan;
    use crate::state::commit::AppendWriteLane;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::{json, to_string};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeBackend {
        stored_schema_rows: HashMap<String, String>,
        version_descriptor_rows: HashMap<String, String>,
        version_pointer_rows: HashMap<String, String>,
        active_version_rows: Vec<(String, String)>,
        active_account_rows: Vec<String>,
        change_rows: Vec<Vec<Value>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_stored_schema_bootstrap") {
                let rows = self
                    .stored_schema_rows
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
            if sql.contains("FROM lix_internal_state_untracked")
                && sql.contains("schema_key = 'lix_active_version'")
            {
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
            if sql.contains("FROM lix_internal_state_untracked")
                && sql.contains("schema_key = 'lix_active_account'")
            {
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
            if sql.contains("FROM lix_internal_state_materialized_v1_lix_version_descriptor") {
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
                && sql.contains("c.schema_key = 'lix_version_pointer'")
            {
                let rows = self
                    .version_pointer_rows
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
            if sql.contains("FROM lix_internal_state_materialized_v1_lix_version_pointer") {
                let rows = self
                    .version_pointer_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(_, snapshot)| {
                        if sql.contains("change_id") {
                            vec![
                                Value::Text(snapshot.clone()),
                                Value::Text("pointer-change".to_string()),
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
            if sql.contains("FROM lix_internal_state_materialized_v1_lix_version_pointer")
                && sql.contains("entity_id = 'global'")
            {
                let rows = self
                    .version_pointer_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(_, snapshot)| vec![Value::Text(snapshot.clone())])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_pointer'")
                && sql.contains("c.entity_id = 'global'")
            {
                let rows = self
                    .version_pointer_rows
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
            "parent_commit_ids": [],
            "meta_change_ids": []
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
                Value::Text("lix_version_pointer".to_string()),
                Value::Text("1".to_string()),
                Value::Text(crate::version::version_pointer_file_id().to_string()),
                Value::Text(crate::version::version_pointer_plugin_key().to_string()),
                Value::Text(pointer_snapshot),
                Value::Null,
                Value::Text("2026-03-06T18:00:02Z".to_string()),
            ],
        ]
    }

    #[tokio::test]
    async fn prepares_builtin_schema_derived_entity_reads() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
                .expect("dependency spec should be derived")
                .schema_keys
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_key_value".to_string()
            ]
        );
        assert_eq!(
            prepared
                .effective_state_request
                .expect("effective-state request should be built")
                .schema_set
                .into_iter()
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
            .expect("live sql2 entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
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
    async fn prepares_stored_schema_derived_entity_reads() {
        let mut backend = FakeBackend::default();
        backend.stored_schema_rows.insert(
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

        let prepared = prepare_sql2_read(
            &backend,
            &parse_one("SELECT body FROM message WHERE id = 'm1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("stored-schema entity read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["message"]);
        assert_eq!(
            prepared
                .canonicalized
                .as_ref()
                .expect("stored-schema entity read should use canonicalized path")
                .surface_binding
                .implicit_overrides
                .fixed_schema_key
                .as_deref(),
            Some("message")
        );
        assert!(prepared.dependency_spec.is_some());
        assert!(prepared.effective_state_plan.is_some());
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("stored-schema entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_message"));
    }

    #[tokio::test]
    async fn lowers_backend_registered_public_queries_with_sql2_surface_expansion() {
        let mut backend = FakeBackend::default();
        backend.stored_schema_rows.insert(
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

        let lowered = lower_public_read_query_with_sql2_backend(&backend, *query, &[])
            .await
            .expect("stored-schema derived public query should lower through backend registry");
        let lowered_sql = lowered.to_string();

        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_message"));
        assert!(!lowered_sql.contains("FROM message"));
    }

    #[tokio::test]
    async fn prepares_builtin_entity_by_version_reads() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(lowered_sql.contains("version_id AS lixcol_version_id"));
    }

    #[tokio::test]
    async fn prepares_builtin_entity_history_reads() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            crate::version::version_pointer_snapshot_content("main", "commit-active-root"),
        );
        let prepared = prepare_sql2_read(
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
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("entity history read should lower");
        assert!(lowered_sql.contains("FROM lix_internal_state_materialized_v1_lix_commit"));
        assert!(lowered_sql.contains("c.id = 'commit-active-root'"));
        assert!(lowered_sql.contains("commit_id AS lixcol_commit_id"));
        assert!(lowered_sql.contains("depth AS lixcol_depth"));
    }

    #[tokio::test]
    async fn prepares_lix_change_reads_without_effective_state_artifacts() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(prepared.effective_state_request.is_none());
        assert!(prepared.effective_state_plan.is_none());
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
        let prepared = prepare_sql2_read(
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
        assert!(prepared.effective_state_request.is_none());
        assert!(prepared.effective_state_plan.is_none());
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
        assert!(lowered_sql.contains("lix_internal_commit_ancestry"));
    }

    #[tokio::test]
    async fn prepares_filesystem_reads_through_internal_projection_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one("SELECT id, path, data FROM lix_file WHERE id = 'file-1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("filesystem read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_file"]);
        assert!(prepared.effective_state_request.is_none());
        assert!(prepared.effective_state_plan.is_none());
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_file_descriptor"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_directory_descriptor"));
        assert!(lowered_sql.contains("lix_internal_binary_blob_store"));
        assert!(!lowered_sql.contains("FROM lix_file_by_version"));
    }

    #[tokio::test]
    async fn prepares_filesystem_by_version_reads_with_residual_version_filter() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(prepared.effective_state_request.is_none());
        assert!(prepared.effective_state_plan.is_none());
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_directory_descriptor"));
        assert!(!lowered_sql.contains("FROM lix_directory_by_version"));
    }

    #[tokio::test]
    async fn prepares_filesystem_history_reads_through_internal_history_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(prepared.effective_state_request.is_none());
        assert!(prepared.effective_state_plan.is_none());
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec![
                "id = 'file-1'".to_string(),
                "lixcol_root_commit_id = 'commit-1'".to_string()
            ]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("filesystem history read should lower");
        assert!(lowered_sql.contains("lix_internal_commit_ancestry"));
        assert!(lowered_sql.contains("lix_internal_change ch"));
        assert!(lowered_sql.contains("lix_internal_file_history_data_cache"));
        assert!(!lowered_sql.contains("FROM lix_file_history"));
    }

    #[tokio::test]
    async fn prepares_filesystem_history_by_version_reads_through_internal_history_sources() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("filesystem by-version history read should lower");
        assert!(lowered_sql.contains("lix_internal_commit_ancestry"));
        assert!(lowered_sql.contains("lix_internal_change ch"));
        assert!(lowered_sql.contains("lix_internal_file_history_data_cache"));
        assert!(!lowered_sql.contains("FROM lix_file_history_by_version"));
    }

    #[tokio::test]
    async fn binds_active_root_commit_for_filesystem_history_reads_without_explicit_root() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            crate::version::version_pointer_snapshot_content("main", "commit-active-root"),
        );

        let prepared = prepare_sql2_read(
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

        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("filesystem history read should lower");
        assert!(lowered_sql.contains("c.entity_id = 'commit-active-root'"));
    }

    #[tokio::test]
    async fn binds_active_root_commit_for_entity_history_reads_without_explicit_root() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            crate::version::version_pointer_snapshot_content("main", "commit-active-root"),
        );

        let prepared = prepare_sql2_read(
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

        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("entity history read should lower");
        assert!(lowered_sql.contains("c.id = 'commit-active-root'"));
    }

    #[tokio::test]
    async fn prepares_explain_over_state_reads_with_sql2_lowered_query() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
    }

    #[tokio::test]
    async fn classifies_public_reads_through_sql2_public_execution() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_public_execution(
            &backend,
            &parse_one("SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("public read classification should succeed");

        assert!(matches!(
            prepared,
            Some(Sql2PreparedPublicExecution::Read(_))
        ));
    }

    #[tokio::test]
    async fn classifies_public_writes_through_sql2_public_execution() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            crate::version::version_pointer_snapshot_content("main", "commit-active-root"),
        );
        let prepared = prepare_sql2_public_execution(
            &backend,
            &parse_one("INSERT INTO lix_key_value (key, value) VALUES ('phase1-boundary', 'ok')"),
            &[],
            "main",
            None,
        )
        .await
        .expect("public write classification should succeed");

        assert!(matches!(
            prepared,
            Some(Sql2PreparedPublicExecution::Write(_))
        ));
    }

    #[tokio::test]
    async fn read_only_public_writes_are_owned_by_sql2_and_rejected_semantically() {
        let backend = FakeBackend::default();
        let error = prepare_sql2_public_execution(
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
        .expect_err("read-only public write should be rejected by sql2");

        assert_eq!(error.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }

    #[tokio::test]
    async fn prepares_bindable_cte_join_group_by_reads_via_surface_expansion() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read_strict(
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
        .expect("bindable cte/join/group-by read should prepare through sql2");

        assert!(prepared.canonicalized.is_none());
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[tokio::test]
    async fn prepares_group_by_having_reads_via_surface_expansion() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read_strict(
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
        .expect("group-by/having public read should prepare through sql2");

        assert!(prepared.canonicalized.is_none());
        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_state"]);
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("group-by/having read should lower");
        assert!(!lowered_sql.contains("FROM lix_state"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(lowered_sql.contains("GROUP BY"));
        assert!(lowered_sql.contains("HAVING"));
    }

    #[tokio::test]
    async fn cte_shadowing_public_surface_names_stays_non_public() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_public_execution(
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
        let prepared = prepare_sql2_read(
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
        .expect("joined admin read should prepare through sql2");

        assert!(prepared.canonicalized.is_none());
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
        assert!(lowered_sql.contains("lix_internal_state_untracked"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_version_descriptor"));
    }

    #[tokio::test]
    async fn prepares_public_reads_joined_with_backend_real_tables() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read_strict(
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
        .expect("public/external mixed read should prepare through sql2");

        assert!(prepared.canonicalized.is_none());
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
        assert!(lowered_sql.contains("lix_internal_state_untracked"));
    }

    #[tokio::test]
    async fn rejects_public_reads_mixed_with_internal_engine_tables() {
        let backend = FakeBackend::default();
        let error = prepare_sql2_read_strict(
            &backend,
            &parse_one(
                "SELECT av.version_id \
                 FROM lix_active_version av \
                 JOIN lix_internal_state_untracked u ON u.entity_id = av.id",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect_err("public/internal mixed read should be rejected");

        assert_eq!(error.code, "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED");
        assert!(error.description.contains("lix_internal_state_untracked"));
    }

    #[tokio::test]
    async fn allows_public_reads_mixed_with_internal_engine_tables_when_internal_access_enabled() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_public_execution_with_internal_access(
            &backend,
            &parse_one(
                "SELECT av.version_id \
                 FROM lix_active_version av \
                 JOIN lix_internal_state_untracked u ON u.entity_id = av.id",
            ),
            &[],
            "main",
            None,
            true,
        )
        .await
        .expect("public/internal mixed read should prepare when internal access is enabled")
        .expect("public/internal mixed read should return a prepared sql2 read");

        let Sql2PreparedPublicExecution::Read(prepared) = prepared else {
            panic!("expected prepared public read");
        };

        assert!(prepared.canonicalized.is_none());
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("public/internal mixed read should lower");
        assert!(lowered_sql.contains("lix_internal_state_untracked"));
    }

    #[tokio::test]
    async fn prepares_state_reads_with_explicit_residual_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
    }

    #[tokio::test]
    async fn prepares_state_by_version_reads_with_version_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[tokio::test]
    async fn prepares_state_history_reads_with_root_commit_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("state-history read should lower");
        assert!(lowered_sql.contains("FROM lix_internal_state_materialized_v1_lix_commit"));
        assert!(lowered_sql.contains("c.id = 'commit-1'"));
    }

    #[tokio::test]
    async fn prepares_nested_filesystem_subqueries_through_sql2_lowering() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
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
        .expect("nested filesystem subquery should prepare through sql2");

        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("nested filesystem subquery should lower");
        assert!(!lowered_sql.contains("FROM lix_file"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_file_descriptor"));
    }

    #[tokio::test]
    async fn prepares_state_by_version_inserts_into_planned_writes() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "version-a".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "version-a".to_string(),
                commit_id: "commit-123".to_string(),
            })
            .expect("version pointer JSON"),
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1'\
                 )",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("state insert should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_state_by_version"]
        );
        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            prepared.planned_write.command.requested_mode,
            WriteModeRequest::Auto
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .execution_mode,
            WriteMode::Tracked
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-123".to_string())
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            prepared
                .domain_change_batch
                .as_ref()
                .expect("tracked write should include a domain change batch")
                .write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert!(matches!(
            prepared.execution.as_ref(),
            Some(Sql2WriteExecution::Tracked(execution))
                if execution.append_preconditions.write_lane
                    == AppendWriteLane::Version("version-a".to_string())
        ));
        assert_eq!(
            prepared
                .debug_trace
                .invariant_trace
                .as_ref()
                .expect("write debug trace should include invariant checks")
                .batch_local_checks,
            vec![
                "snapshot_content.schema_validation".to_string(),
                "entity_id.primary_key_consistency".to_string()
            ]
        );
        assert_eq!(
            prepared.debug_trace.write_phase_trace,
            vec![
                "canonicalize_write".to_string(),
                "prove_write".to_string(),
                "resolve_authoritative_pre_state".to_string(),
                "build_domain_change_batch".to_string(),
                "derive_commit_preconditions".to_string(),
                "validate_batch_local_write".to_string(),
                "append_time_invariant_recheck".to_string(),
                "append_commit_if_preconditions_hold".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn prepares_active_version_state_inserts_with_active_lane() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "main".to_string(),
                commit_id: "commit-main".to_string(),
            })
            .expect("version pointer JSON"),
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1'\
                 )",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("active-version state insert should prepare through sql2");

        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::ActiveVersion
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::ActiveVersion)
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-main".to_string())
        );
    }

    #[tokio::test]
    async fn prepares_lix_version_inserts_with_global_admin_lane() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "global".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "global".to_string(),
                commit_id: "commit-global".to_string(),
            })
            .expect("version pointer JSON"),
        );

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_version (id, name, hidden, commit_id) \
                 VALUES ('version-a', 'Version A', false, 'commit-a')",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("lix_version insert should prepare through sql2");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_version"]);
        assert_eq!(prepared.planned_write.scope_proof, ScopeProof::GlobalAdmin);
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::GlobalAdmin)
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-global".to_string())
        );
        let rows = &prepared
            .planned_write
            .resolved_write_plan
            .as_ref()
            .expect("resolved write plan should exist")
            .intended_post_state;
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|row| row.schema_key == crate::version::version_descriptor_schema_key()));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == crate::version::version_pointer_schema_key()));
    }

    #[tokio::test]
    async fn prepares_active_version_updates_as_untracked_admin_writes() {
        let backend = FakeBackend {
            active_version_rows: vec![(
                "active-row".to_string(),
                crate::version::active_version_snapshot_content("active-row", "main"),
            )],
            version_descriptor_rows: HashMap::from([(
                "version-b".to_string(),
                crate::version::version_descriptor_snapshot_content(
                    "version-b",
                    "Version B",
                    false,
                ),
            )]),
            ..FakeBackend::default()
        };

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("UPDATE lix_active_version SET version_id = 'version-b'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("active version update should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_active_version"]
        );
        assert_eq!(
            prepared.planned_write.command.requested_mode,
            WriteModeRequest::ForceUntracked
        );
        assert!(matches!(
            prepared.execution.as_ref(),
            Some(Sql2WriteExecution::Untracked(_))
        ));
        assert!(prepared.planned_write.commit_preconditions.is_none());
        assert!(prepared.domain_change_batch.is_none());
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .version_id
                .as_deref(),
            Some(crate::version::active_version_storage_version_id())
        );
    }

    #[tokio::test]
    async fn prepares_active_account_deletes_as_untracked_admin_writes() {
        let backend = FakeBackend {
            active_account_rows: vec!["acct-1".to_string()],
            ..FakeBackend::default()
        };

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("DELETE FROM lix_active_account WHERE account_id = 'acct-1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("active account delete should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_active_account"]
        );
        assert_eq!(
            prepared.planned_write.command.requested_mode,
            WriteModeRequest::ForceUntracked
        );
        assert!(matches!(
            prepared.execution.as_ref(),
            Some(Sql2WriteExecution::Untracked(execution))
                if !execution.persist_filesystem_payloads_before_write
        ));
        assert!(prepared.planned_write.commit_preconditions.is_none());
        assert!(prepared.domain_change_batch.is_none());
        assert!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .tombstone
        );
    }

    #[tokio::test]
    async fn prepares_untracked_filesystem_file_by_version_inserts_with_payload_rows() {
        let prepared = prepare_sql2_write(
            &FakeBackend::default(),
            &parse_one(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id, lixcol_untracked) \
                 VALUES ('file-u', '/docs/untracked.md', lix_text_encode('hello'), 'version-a', true)",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("untracked file insert should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_file_by_version"]
        );
        assert_eq!(
            prepared.planned_write.command.requested_mode,
            WriteModeRequest::ForceUntracked
        );
        assert!(prepared.planned_write.commit_preconditions.is_none());
        assert!(prepared.domain_change_batch.is_none());
        let rows = &prepared
            .planned_write
            .resolved_write_plan
            .as_ref()
            .expect("resolved write plan should exist")
            .intended_post_state;
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "lix_file_descriptor"));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "lix_binary_blob_ref"));
    }

    #[tokio::test]
    async fn prepares_untracked_directory_by_version_inserts_without_commit_artifacts() {
        let prepared = prepare_sql2_write(
            &FakeBackend::default(),
            &parse_one(
                "INSERT INTO lix_directory_by_version (id, path, lixcol_version_id, lixcol_untracked) \
                 VALUES ('dir-u', '/docs/guides/', 'version-a', true)",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("untracked directory insert should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_directory_by_version"]
        );
        assert_eq!(
            prepared.planned_write.command.requested_mode,
            WriteModeRequest::ForceUntracked
        );
        assert!(prepared.planned_write.commit_preconditions.is_none());
        assert!(prepared.domain_change_batch.is_none());
        let rows = &prepared
            .planned_write
            .resolved_write_plan
            .as_ref()
            .expect("resolved write plan should exist")
            .intended_post_state;
        assert!(rows
            .iter()
            .all(|row| row.schema_key == "lix_directory_descriptor"));
        assert!(
            rows.len() >= 2,
            "missing ancestor directory rows should be planned"
        );
    }

    #[tokio::test]
    async fn prepares_stored_schema_invariant_trace_for_sql2_writes() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "global".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "global".to_string(),
                commit_id: "commit-global".to_string(),
            })
            .expect("version pointer JSON"),
        );

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'schema-a~1', 'lix_stored_schema', 'lix', 'global', 'lix', '{\"value\":{\"x-lix-key\":\"schema-a\",\"x-lix-version\":\"1\",\"type\":\"object\"}}', '1'\
                 )",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("stored schema write should prepare through sql2");

        let invariant_trace = prepared
            .debug_trace
            .invariant_trace
            .as_ref()
            .expect("stored schema write should expose invariant trace");
        assert!(invariant_trace
            .batch_local_checks
            .contains(&"stored_schema.definition_validation".to_string()));
        assert!(invariant_trace
            .batch_local_checks
            .contains(&"stored_schema.bootstrap_identity".to_string()));
        assert!(invariant_trace
            .append_time_checks
            .contains(&"write_lane.tip_precondition".to_string()));
        assert_eq!(
            invariant_trace.physical_checks,
            vec!["backend_constraints.defense_in_depth".to_string()]
        );
    }

    #[tokio::test]
    async fn prepares_builtin_entity_inserts_into_tracked_write_artifacts() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "main".to_string(),
                commit_id: "commit-main".to_string(),
            })
            .expect("version pointer JSON"),
        );
        let registry = SurfaceRegistry::bootstrap_with_backend(&backend)
            .await
            .expect("registry should bootstrap");
        let bound = BoundStatement::from_statement(
            parse_one("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')")
                .into_iter()
                .next()
                .expect("single statement"),
            Vec::new(),
            ExecutionContext {
                dialect: Some(SqlDialect::Sqlite),
                requested_version_id: Some("main".to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("entity insert should canonicalize");
        let mut planned_write = prove_write(&canonicalized).expect("entity insert should prove");
        let resolved_write_plan = resolve_write_plan(&backend, &planned_write)
            .await
            .expect("entity insert should resolve");
        planned_write.resolved_write_plan = Some(resolved_write_plan);
        let _ = build_domain_change_batch(&planned_write)
            .expect("domain-change batch should build")
            .expect("tracked entity insert should produce a batch");
        let _ = derive_commit_preconditions(&backend, &planned_write)
            .await
            .expect("commit preconditions should derive")
            .expect("tracked entity insert should produce commit preconditions");

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')"),
            &[],
            "main",
            None,
        )
        .await
        .expect("builtin entity insert should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_key_value".to_string()]
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .values
                .get("snapshot_content"),
            Some(&Value::Text("{\"key\":\"k\",\"value\":\"v\"}".to_string()))
        );
    }

    #[tokio::test]
    async fn returns_none_for_entity_writes_that_need_legacy_global_override_handling() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "main".to_string(),
                commit_id: "commit-main".to_string(),
            })
            .expect("version pointer JSON"),
        );
        backend.stored_schema_rows.insert(
            "message".to_string(),
            json!({
                "value": {
                    "x-lix-key": "message",
                    "x-lix-version": "1",
                    "x-lix-primary-key": ["/id"],
                    "x-lix-override-lixcols": {
                        "lixcol_file_id": "\"lix\"",
                        "lixcol_plugin_key": "\"lix\"",
                        "lixcol_global": "true"
                    },
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "body": { "type": "string" }
                    }
                }
            })
            .to_string(),
        );

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("INSERT INTO message (id, body) VALUES ('m1', 'hello')"),
            &[],
            "main",
            None,
        )
        .await;

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepares_state_by_version_updates_into_tracked_write_artifacts() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "version-a".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "version-a".to_string(),
                commit_id: "commit-456".to_string(),
            })
            .expect("version pointer JSON"),
        );
        backend.change_rows = build_committed_state_change_rows(
            "entity-1",
            "version-a",
            "{\"value\":\"before\"}",
            Some("{\"m\":1}"),
            "change-1",
            "commit-456",
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("state update should prepare through sql2");

        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .values
                .get("file_id"),
            Some(&Value::Text("lix".to_string()))
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-456".to_string())
        );
    }

    #[tokio::test]
    async fn prepares_state_by_version_deletes_into_tracked_write_artifacts() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "version-a".to_string(),
            to_string(&crate::schema::builtin::types::LixVersionPointer {
                id: "version-a".to_string(),
                commit_id: "commit-789".to_string(),
            })
            .expect("version pointer JSON"),
        );
        backend.change_rows = build_committed_state_change_rows(
            "entity-1",
            "version-a",
            "{\"value\":\"before\"}",
            None,
            "change-1",
            "commit-789",
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("state delete should prepare through sql2");

        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .tombstone,
            true
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .tombstones
                .len(),
            1
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-789".to_string())
        );
    }
}
