use std::sync::Arc;

use crate::cel::CelEvaluator;
use crate::default_values::apply_vtable_insert_defaults;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::sql2::planner::backend::lowerer::rewrite_supported_public_read_surfaces_in_statement;
use crate::sql2::planner::backend::lowerer::lower_read_for_execution;
use crate::sql2::catalog::SurfaceRegistry;
use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
use crate::sql2::planner::canonicalize::canonicalize_read;
use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
use crate::sql2::planner::semantics::effective_state_resolver::build_effective_state;
use crate::sql2::runtime::prepare_sql2_read;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot, DEFAULT_ACTIVE_VERSION_NAME,
};
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::super::ast::lowering::lower_statement;
use super::super::ast::utils::parse_sql_statements;
use super::super::contracts::planned_statement::{
    MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use crate::internal_state::PostprocessPlan;
use super::super::contracts::prepared_statement::PreparedStatement;
use super::bind_once::{bind_statements_with_appended_params_once, StatementWithAppendedParams};
use super::inline_functions::inline_lix_functions_with_provider;
use super::param_context::normalize_statement_placeholders_in_batch;
use crate::internal_state::{
    materialize::materialize_vtable_insert_select_sources, requires_single_statement_postprocess,
    rewrite_internal_state_query_read, rewrite_internal_state_query_read_with_backend,
    rewrite_statement, rewrite_statement_with_backend, statement_references_internal_state_vtable,
    RewriteOutput,
};
use crate::internal_state::script::coalesce_vtable_inserts_in_transactions;
use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, ObjectNamePart, Query, Statement, TableFactor, Visit, Visitor};

#[derive(Debug, Clone)]
struct StatementRewriteOutput {
    statements: Vec<Statement>,
    params: Vec<Value>,
    registrations: Vec<SchemaRegistration>,
    postprocess: Option<PostprocessPlan>,
    mutations: Vec<MutationRow>,
    update_validations: Vec<UpdateValidationPlan>,
}

pub(crate) fn rewrite_public_read_statement_to_lowered_sql(
    statement: &mut Statement,
    dialect: SqlDialect,
) -> Result<Statement, LixError> {
    rewrite_supported_public_read_surfaces_in_statement(statement)?;
    lower_statement(statement.clone(), dialect)
}

pub(crate) fn statement_references_public_sql2_surface(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => query_references_builtin_public_sql2_surface(query),
        Statement::Explain { statement, .. } => statement_references_public_sql2_surface(statement),
        _ => false,
    }
}

pub(crate) async fn statement_references_public_sql2_surface_with_backend(
    backend: &dyn LixBackend,
    statement: &Statement,
) -> bool {
    let query = match statement {
        Statement::Query(query) => query,
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(query) => query,
            _ => return false,
        },
        _ => return false,
    };

    let relation_names = collect_query_relation_names(query);
    if relation_names.is_empty() {
        return false;
    }
    if relation_names
        .iter()
        .all(|name| name.starts_with("lix_internal_"))
    {
        return false;
    }

    let registry = match SurfaceRegistry::bootstrap_with_backend(backend).await {
        Ok(registry) => registry,
        Err(_) => return query_references_builtin_public_sql2_surface(query),
    };

    relation_names
        .iter()
        .any(|name| registry.bind_relation_name(name).is_some())
}

fn query_references_builtin_public_sql2_surface(query: &Query) -> bool {
    let registry = SurfaceRegistry::with_builtin_surfaces();
    collect_query_relation_names(query)
        .into_iter()
        .any(|name| registry.bind_relation_name(&name).is_some())
}

fn collect_query_relation_names(query: &Query) -> BTreeSet<String> {
    struct Collector {
        relation_names: BTreeSet<String>,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, .. } = table_factor {
                if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                    self.relation_names
                        .insert(identifier.value.to_ascii_lowercase());
                }
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        relation_names: BTreeSet::new(),
    };
    let _ = query.visit(&mut collector);
    collector.relation_names
}

pub(crate) fn rewrite_public_read_query_to_lowered_sql(
    query: sqlparser::ast::Query,
    dialect: SqlDialect,
) -> Result<sqlparser::ast::Query, LixError> {
    let mut statement = Statement::Query(Box::new(query));
    match rewrite_public_read_statement_to_lowered_sql(&mut statement, dialect)? {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected lowered read query to remain a SELECT query".to_string(),
        }),
    }
}

pub(crate) async fn lower_public_read_query_with_sql2_backend(
    backend: &dyn LixBackend,
    query: sqlparser::ast::Query,
    params: &[Value],
) -> Result<sqlparser::ast::Query, LixError> {
    let active_version_id = load_active_version_id_for_sql2_read(backend).await?;
    let parsed = vec![Statement::Query(Box::new(query.clone()))];
    let prepared = prepare_sql2_read(backend, &parsed, params, &active_version_id, None)
        .await;
    let lowered = if let Some(prepared) = prepared {
        prepared.lowered_read.ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 read subquery did not lower to executable SQL".to_string(),
        })?
    } else {
        let rewritten = rewrite_public_read_query_to_lowered_sql(query.clone(), backend.dialect())?;
        if rewritten != query {
            return Ok(rewritten);
        }
        let registry = SurfaceRegistry::bootstrap_with_backend(backend).await?;
        let bound_statement = BoundStatement::from_statement(
            Statement::Query(Box::new(query)),
            params.to_vec(),
            ExecutionContext {
                dialect: Some(backend.dialect()),
                writer_key: None,
                requested_version_id: Some(active_version_id),
            },
        );
        let canonicalized = canonicalize_read(bound_statement, &registry).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("sql2 read subquery canonicalization failed: {}", error.message),
        })?;
        let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized);
        let effective_state = build_effective_state(&canonicalized, dependency_spec.as_ref());
        lower_read_for_execution(
            &canonicalized,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
        )?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 could not prepare read subquery".to_string(),
        })?
    };
    let statement = lowered
        .statements
        .into_iter()
        .next()
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 read subquery lowered to no statements".to_string(),
        })?;
    let statement = lower_statement(statement, backend.dialect())?;
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected lowered subquery to remain a SELECT query".to_string(),
        }),
    }
}

async fn load_active_version_id_for_sql2_read(backend: &dyn LixBackend) -> Result<String, LixError> {
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
                Value::Text(active_version_schema_key().to_string()),
                Value::Text(active_version_file_id().to_string()),
                Value::Text(active_version_storage_version_id().to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(DEFAULT_ACTIVE_VERSION_NAME.to_string());
    };
    let snapshot_content = row.first().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "active version query row is missing snapshot_content".to_string(),
    })?;
    let snapshot_content = match snapshot_content {
        Value::Text(value) => value.as_str(),
        other => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("active version snapshot_content must be text, got {other:?}"),
            })
        }
    };
    parse_active_version_snapshot(snapshot_content)
}

struct RewrittenStatementBinding {
    statement: Statement,
    appended_params: Arc<Vec<Value>>,
}

pub(crate) fn preprocess_statements_with_provider_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    preprocess_statements_with_provider_and_writer_key(statements, params, provider, dialect, None)
}

fn preprocess_statements_with_provider_and_writer_key<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError> {
    let mut statements = statements;
    normalize_statement_placeholders_in_batch(&mut statements)?;

    let mut rewritten = Vec::with_capacity(statements.len());
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut mutations: Vec<MutationRow> = Vec::new();
    let mut update_validations: Vec<UpdateValidationPlan> = Vec::new();

    for statement in statements {
        let output = if let Some(output) =
            rewrite_top_level_read_statement_sync(statement.clone(), SqlDialect::Sqlite)?
        {
            output
        } else {
            rewrite_statement(statement, params, writer_key, provider)?
        };
        accumulate_rewrite_output(
            from_rewrite_output(output),
            provider,
            dialect,
            &mut rewritten,
            &mut registrations,
            &mut postprocess,
            &mut mutations,
            &mut update_validations,
        )?;
    }

    if requires_single_statement_postprocess(postprocess.as_ref()) && rewritten.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, dialect)?;

    Ok(PlannedStatementSet {
        sql: normalized_sql,
        prepared_statements,
        registrations,
        postprocess,
        mutations,
        update_validations,
    })
}

async fn preprocess_statements_with_provider_and_backend<P>(
    backend: &dyn LixBackend,
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut mutations: Vec<MutationRow> = Vec::new();
    let mut update_validations: Vec<UpdateValidationPlan> = Vec::new();

    for (statement_index, statement) in statements.into_iter().enumerate() {
        // Keep this async rewrite future boxed to avoid infinitely sized
        // futures in recursive rewrite call paths.
        let output = if let Some(output) =
            rewrite_top_level_read_statement_backend(backend, statement.clone(), params).await?
        {
            output
        } else {
            Box::pin(rewrite_statement_with_backend(
                backend,
                statement,
                params,
                writer_key,
                provider,
            ))
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "preprocess_with_surfaces_to_plan backend rewrite failed for statement {}: {}",
                    statement_index, error.description
                ),
            })?
        };

        accumulate_rewrite_output(
            from_rewrite_output(output),
            provider,
            backend.dialect(),
            &mut rewritten,
            &mut registrations,
            &mut postprocess,
            &mut mutations,
            &mut update_validations,
        )?;
    }

    if requires_single_statement_postprocess(postprocess.as_ref()) && rewritten.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, backend.dialect())?;

    Ok(PlannedStatementSet {
        sql: normalized_sql,
        prepared_statements,
        registrations,
        postprocess,
        mutations,
        update_validations,
    })
}

pub(crate) async fn preprocess_sql_to_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    let functions = SharedFunctionProvider::new(SystemFunctionProvider);
    preprocess_sql_with_provider(backend, evaluator, sql_text, params, functions).await
}

async fn preprocess_sql_with_provider<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    preprocess_sql_with_provider_and_writer_key(
        backend, evaluator, sql_text, params, functions, None,
    )
    .await
}

async fn preprocess_sql_with_provider_and_writer_key<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    preprocess_with_surfaces_to_plan(
        backend,
        evaluator,
        parse_sql_statements(sql_text)?,
        params,
        functions,
        writer_key,
    )
    .await
}

pub(crate) async fn preprocess_with_surfaces_to_plan<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let params = params.to_vec();
    let mut statements = coalesce_vtable_inserts_in_transactions(statements)?;
    normalize_statement_placeholders_in_batch(&mut statements)?;

    materialize_vtable_insert_select_sources(backend, &mut statements, &params)
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "preprocess_with_surfaces_to_plan insert-select materialization failed: {}",
                error.description
            ),
        })?;

    apply_vtable_insert_defaults(
        backend,
        evaluator,
        &mut statements,
        &params,
        functions.clone(),
    )
    .await
    .map_err(|error| LixError {
        code: error.code,
        description: format!(
            "preprocess_with_surfaces_to_plan insert default application failed: {}",
            error.description
        ),
    })?;

    for statement in &mut statements {
        if matches!(statement, Statement::Query(_) | Statement::Explain { .. }) {
            continue;
        }
        rewrite_supported_public_read_surfaces_in_statement(statement).map_err(|error| LixError {
            code: error.code,
            description: format!(
                "preprocess_with_surfaces_to_plan sql2 public-surface lowering failed: {}",
                error.description
            ),
        })?;
    }

    let mut provider = functions.clone();
    preprocess_statements_with_provider_and_backend(
        backend,
        statements,
        &params,
        &mut provider,
        writer_key,
    )
    .await
}

fn accumulate_rewrite_output<P: LixFunctionProvider>(
    output: StatementRewriteOutput,
    provider: &mut P,
    dialect: SqlDialect,
    rewritten: &mut Vec<RewrittenStatementBinding>,
    registrations: &mut Vec<SchemaRegistration>,
    postprocess: &mut Option<PostprocessPlan>,
    mutations: &mut Vec<MutationRow>,
    update_validations: &mut Vec<UpdateValidationPlan>,
) -> Result<(), LixError> {
    registrations.extend(output.registrations);
    if let Some(plan) = output.postprocess {
        if postprocess.is_some() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "only one postprocess rewrite is supported per query".to_string(),
            });
        }
        *postprocess = Some(plan);
    }
    mutations.extend(output.mutations);
    update_validations.extend(output.update_validations);

    let appended_params = Arc::new(output.params);
    for statement in output.statements {
        let inlined = inline_lix_functions_with_provider(statement, provider);
        rewritten.push(RewrittenStatementBinding {
            statement: lower_statement(inlined, dialect)?,
            appended_params: Arc::clone(&appended_params),
        });
    }

    Ok(())
}

fn render_statements_with_params(
    statements: &[RewrittenStatementBinding],
    base_params: &[Value],
    dialect: SqlDialect,
) -> Result<(String, Vec<PreparedStatement>), LixError> {
    let statement_sql = statements
        .iter()
        .map(|statement| statement.statement.to_string())
        .collect::<Vec<_>>();
    let statement_inputs = statements
        .iter()
        .zip(statement_sql.iter())
        .map(|(statement, sql)| StatementWithAppendedParams {
            sql: sql.as_str(),
            appended_params: statement.appended_params.as_slice(),
        })
        .collect::<Vec<_>>();
    let bound_statements =
        bind_statements_with_appended_params_once(&statement_inputs, base_params, dialect)
            .map_err(LixError::from)?;

    let mut rendered = Vec::with_capacity(bound_statements.len());
    let mut prepared_statements = Vec::with_capacity(bound_statements.len());
    for (sql, params) in bound_statements {
        rendered.push(sql.clone());
        prepared_statements.push(PreparedStatement { sql, params });
    }

    let normalized_sql = rendered.join("; ");
    Ok((normalized_sql, prepared_statements))
}

fn from_rewrite_output(output: crate::internal_state::RewriteOutput) -> StatementRewriteOutput {
    StatementRewriteOutput {
        statements: output.statements,
        params: output.params,
        registrations: output.registrations,
        postprocess: output.postprocess,
        mutations: output.mutations,
        update_validations: output.update_validations,
    }
}

fn rewrite_top_level_read_statement_sync(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Option<RewriteOutput>, LixError> {
    match statement {
        Statement::Query(query) => rewrite_query_read_sync(*query, dialect),
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            let rewritten_statement = match *statement {
                Statement::Query(query) => match rewrite_query_read_sync(*query, dialect)? {
                    Some(output) => Statement::Query(Box::new(
                        output
                            .statements
                            .into_iter()
                            .next()
                            .and_then(|stmt| match stmt {
                                Statement::Query(query) => Some(*query),
                                _ => None,
                            })
                            .ok_or_else(|| LixError {
                                code: "LIX_ERROR_UNKNOWN".to_string(),
                                description: "expected rewritten read query to remain a SELECT query".to_string(),
                            })?,
                    )),
                    None => return Ok(None),
                },
                _ => return Ok(None),
            };
            Ok(Some(RewriteOutput {
                statements: vec![Statement::Explain {
                    describe_alias,
                    analyze,
                    verbose,
                    query_plan,
                    estimate,
                    statement: Box::new(rewritten_statement),
                    format,
                    options,
                }],
                ..empty_rewrite_output()
            }))
        }
        _ => Ok(None),
    }
}

async fn rewrite_top_level_read_statement_backend(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
) -> Result<Option<RewriteOutput>, LixError> {
    match statement {
        Statement::Query(query) => rewrite_query_read_backend(backend, *query, params).await,
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            let rewritten_statement = match *statement {
                Statement::Query(query) => match rewrite_query_read_backend(backend, *query, params).await? {
                    Some(output) => Statement::Query(Box::new(
                        output
                            .statements
                            .into_iter()
                            .next()
                            .and_then(|stmt| match stmt {
                                Statement::Query(query) => Some(*query),
                                _ => None,
                            })
                            .ok_or_else(|| LixError {
                                code: "LIX_ERROR_UNKNOWN".to_string(),
                                description: "expected rewritten backend read query to remain a SELECT query".to_string(),
                            })?,
                    )),
                    None => return Ok(None),
                },
                _ => return Ok(None),
            };
            Ok(Some(RewriteOutput {
                statements: vec![Statement::Explain {
                    describe_alias,
                    analyze,
                    verbose,
                    query_plan,
                    estimate,
                    statement: Box::new(rewritten_statement),
                    format,
                    options,
                }],
                ..empty_rewrite_output()
            }))
        }
        _ => Ok(None),
    }
}

fn rewrite_query_read_sync(
    query: Query,
    dialect: SqlDialect,
) -> Result<Option<RewriteOutput>, LixError> {
    let statement = Statement::Query(Box::new(query.clone()));
    if statement_references_internal_state_vtable(&statement) {
        let rewritten = rewrite_internal_state_query_read(query, &[])?;
        return Ok(Some(rewrite_output_from_statement(Statement::Query(Box::new(
            rewritten,
        )))));
    }
    if !statement_references_public_sql2_surface(&statement) {
        return Ok(None);
    }
    let rewritten = rewrite_public_read_query_to_lowered_sql(query, dialect)?;
    Ok(Some(rewrite_output_from_statement(Statement::Query(Box::new(
        rewritten,
    )))))
}

async fn rewrite_query_read_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Option<RewriteOutput>, LixError> {
    let statement = Statement::Query(Box::new(query.clone()));
    if statement_references_internal_state_vtable(&statement) {
        let rewritten = rewrite_internal_state_query_read_with_backend(backend, query, params)
            .await?;
        return Ok(Some(rewrite_output_from_statement(Statement::Query(Box::new(
            rewritten,
        )))));
    }
    if !statement_references_public_sql2_surface_with_backend(backend, &statement).await {
        return Ok(None);
    }
    let rewritten = lower_public_read_query_with_sql2_backend(backend, query, params).await?;
    Ok(Some(rewrite_output_from_statement(Statement::Query(Box::new(
        rewritten,
    )))))
}

fn rewrite_output_from_statement(statement: Statement) -> RewriteOutput {
    RewriteOutput {
        statements: vec![statement],
        ..empty_rewrite_output()
    }
}

fn empty_rewrite_output() -> RewriteOutput {
    RewriteOutput {
        statements: Vec::new(),
        effect_only: false,
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}
