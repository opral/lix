pub(crate) mod bind_once;
mod canonical_write;
pub(crate) mod defaults;
pub(crate) mod followup;
pub(crate) mod inline_functions;
pub(crate) mod materialize;
pub(crate) mod param_context;
pub(crate) mod postprocess;
pub(crate) mod registered_schema;
pub(crate) mod script;
pub(crate) mod vtable_read;
pub(crate) mod vtable_write;

use crate::cel::CelEvaluator;
use crate::functions::LixFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::sql::common::ast::parse_sql_statements;
use crate::sql::execution::contracts::planned_statement::PlannedStatementSet;
use crate::state::internal::bind_once::{
    bind_statements_with_appended_params_once, StatementWithAppendedParams,
};
use crate::state::internal::defaults::apply_vtable_insert_defaults;
use crate::state::internal::inline_functions::inline_lix_functions_with_provider;
use crate::state::internal::param_context::normalize_statement_placeholders_in_batch;
use crate::{LixBackend, LixError, SqlDialect, Value};
use sqlparser::ast::{ObjectNamePart, Query, Statement, TableFactor, Visit, Visitor};
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use crate::sql::ast::lowering::lower_statement;
pub(crate) use crate::sql::ast::utils::PlaceholderState;
pub(crate) use crate::sql::ast::utils::{
    resolve_expr_cell_with_state, ResolvedCell, RowSourceResolver,
};
pub(crate) use crate::sql::ast::walk::object_name_matches;
pub(crate) type SchemaLiveTableRequirement =
    crate::sql::execution::contracts::planned_statement::SchemaLiveTableRequirement;
pub(crate) type MutationOperation =
    crate::sql::execution::contracts::planned_statement::MutationOperation;
pub(crate) type MutationRow = crate::sql::execution::contracts::planned_statement::MutationRow;
pub(crate) type UpdateValidationPlan =
    crate::sql::execution::contracts::planned_statement::UpdateValidationPlan;
pub(crate) type PreparedStatement =
    crate::sql::execution::contracts::prepared_statement::PreparedStatement;
use canonical_write as canonical;
pub(crate) use postprocess::{PostprocessPlan, VtableDeletePlan, VtableUpdatePlan};

#[derive(Debug, Clone)]
pub(crate) struct InternalStatePlan {
    pub(crate) postprocess: Option<PostprocessPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct RewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) effect_only: bool,
    pub(crate) params: Vec<Value>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreprocessOutput {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) internal_state: Option<InternalStatePlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
struct StatementRewriteOutput {
    statements: Vec<Statement>,
    params: Vec<Value>,
    live_table_requirements: Vec<SchemaLiveTableRequirement>,
    internal_state: Option<InternalStatePlan>,
    mutations: Vec<MutationRow>,
    update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
struct RewrittenStatementBinding {
    statement: Statement,
    appended_params: Arc<Vec<Value>>,
}

impl From<PreprocessOutput> for PlannedStatementSet {
    fn from(output: PreprocessOutput) -> Self {
        Self {
            sql: output.sql,
            prepared_statements: output.prepared_statements,
            live_table_requirements: output.live_table_requirements,
            internal_state: output.internal_state,
            mutations: output.mutations,
            update_validations: output.update_validations,
        }
    }
}

pub(crate) fn internal_state_plan_from_postprocess(
    postprocess: Option<PostprocessPlan>,
) -> Option<InternalStatePlan> {
    postprocess.map(|postprocess| InternalStatePlan {
        postprocess: Some(postprocess),
    })
}

pub(crate) fn parse_single_query(sql: &str) -> Result<sqlparser::ast::Query, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single SELECT statement".to_string(),
        });
    }
    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT statement".to_string(),
        }),
    }
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn rewrite_internal_state_query_read(
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let original = query.clone();
    Ok(vtable_read::rewrite_query(query, params)?.unwrap_or(original))
}

pub(crate) async fn rewrite_internal_state_query_read_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let original = query.clone();
    Ok(
        vtable_read::rewrite_query_with_backend(backend, query, params)
            .await?
            .unwrap_or(original),
    )
}

pub(crate) fn statement_references_internal_state_vtable(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => {
            collect_query_relation_names(query).contains("lix_internal_state_vtable")
        }
        Statement::Explain { statement, .. } => {
            statement_references_internal_state_vtable(statement)
        }
        _ => false,
    }
}

pub(crate) fn requires_single_statement_postprocess(plan: Option<&PostprocessPlan>) -> bool {
    matches!(plan, Some(PostprocessPlan::VtableDelete(_)))
}

pub(crate) fn requires_single_statement_internal_state_plan(
    plan: Option<&InternalStatePlan>,
) -> bool {
    requires_single_statement_postprocess(plan.and_then(|plan| plan.postprocess.as_ref()))
}

pub(crate) fn validate_internal_state_plan(
    plan: Option<&InternalStatePlan>,
) -> Result<(), LixError> {
    let Some(plan) = plan else {
        return Ok(());
    };
    let Some(postprocess) = plan.postprocess.as_ref() else {
        return Ok(());
    };
    let schema_key = match postprocess {
        PostprocessPlan::VtableUpdate(update) => &update.schema_key,
        PostprocessPlan::VtableDelete(delete) => &delete.schema_key,
    };
    if !schema_key.trim().is_empty()
        && !schema_key.contains(char::is_whitespace)
        && !schema_key.contains('\'')
    {
        return Ok(());
    }
    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "vtable postprocess plan requires a valid schema_key".to_string(),
    })
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
                let relation_name = name
                    .0
                    .iter()
                    .map(|part| match part {
                        ObjectNamePart::Identifier(identifier) => identifier.value.clone(),
                        ObjectNamePart::Function(function) => function.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(".");
                self.relation_names.insert(relation_name);
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        relation_names: BTreeSet::new(),
    };
    let _ = query.visit(&mut collector);
    collector.relation_names
}

pub(crate) fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError> {
    let output = if let Some(output) =
        canonical::rewrite_sync_statement(statement.clone(), params, writer_key, provider)?
    {
        output
    } else {
        passthrough_output(statement)
    };
    validate_statement_output(&output)?;
    Ok(output)
}

pub(crate) async fn rewrite_statement_with_backend<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let output = if let Some(output) = canonical::rewrite_backend_statement(
        backend,
        statement.clone(),
        params,
        writer_key,
        provider,
    )
    .await?
    {
        output
    } else {
        passthrough_output(statement)
    };
    validate_statement_output(&output)?;
    Ok(output)
}

fn passthrough_output(statement: Statement) -> RewriteOutput {
    RewriteOutput {
        statements: vec![statement],
        effect_only: false,
        params: Vec::new(),
        live_table_requirements: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}

fn validate_statement_output(output: &RewriteOutput) -> Result<(), LixError> {
    if output.statements.is_empty()
        && !(output.effect_only
            && output.postprocess.is_none()
            && output.mutations.is_empty()
            && output.update_validations.is_empty())
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "statement rewrite produced no statements".to_string(),
        });
    }
    if requires_single_statement_postprocess(output.postprocess.as_ref())
        && output.statements.len() != 1
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }
    if output.postprocess.is_some() && !output.mutations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites cannot emit mutation rows".to_string(),
        });
    }
    if !output.mutations.is_empty() && !output.update_validations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !output.update_validations.is_empty()
        && !output.statements.iter().all(|statement| {
            matches!(
                statement,
                sqlparser::ast::Statement::Update(_) | sqlparser::ast::Statement::Delete(_)
            )
        })
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "update validations require an UPDATE or DELETE statement output"
                .to_string(),
        });
    }
    if let Some(postprocess) = &output.postprocess {
        match postprocess {
            PostprocessPlan::VtableUpdate(_) => {
                if !matches!(
                    output.statements.last(),
                    Some(sqlparser::ast::Statement::Update(_))
                ) {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: "vtable update postprocess requires an UPDATE statement"
                            .to_string(),
                    });
                }
            }
            PostprocessPlan::VtableDelete(_) => {
                if !matches!(
                    output.statements[0],
                    sqlparser::ast::Statement::Update(_) | sqlparser::ast::Statement::Delete(_)
                ) {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description:
                            "vtable delete postprocess requires an UPDATE or DELETE statement"
                                .to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn prepare_statements_sync_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError> {
    let mut statements = statements;
    normalize_statement_placeholders_in_batch(&mut statements)?;

    let mut rewritten = Vec::with_capacity(statements.len());
    let mut live_table_requirements: Vec<SchemaLiveTableRequirement> = Vec::new();
    let mut internal_state: Option<InternalStatePlan> = None;
    let mut mutations: Vec<MutationRow> = Vec::new();
    let mut update_validations: Vec<UpdateValidationPlan> = Vec::new();

    for statement in statements {
        let output = if let Some(output) = rewrite_top_level_internal_state_read_statement_sync(
            statement.clone(),
            SqlDialect::Sqlite,
        )? {
            output
        } else {
            rewrite_statement(statement, params, writer_key, provider)?
        };
        accumulate_rewrite_output(
            from_rewrite_output(output),
            provider,
            dialect,
            &mut rewritten,
            &mut live_table_requirements,
            &mut internal_state,
            &mut mutations,
            &mut update_validations,
        )?;
    }

    if requires_single_statement_postprocess(
        internal_state
            .as_ref()
            .and_then(|plan| plan.postprocess.as_ref()),
    ) && rewritten.len() != 1
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, dialect)?;

    Ok(PreprocessOutput {
        sql: normalized_sql,
        prepared_statements,
        live_table_requirements,
        internal_state,
        mutations,
        update_validations,
    })
}

pub(crate) async fn prepare_statements_with_backend_to_plan<P>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let params = params.to_vec();
    let mut statements = script::coalesce_vtable_inserts_in_transactions(statements)?;
    normalize_statement_placeholders_in_batch(&mut statements)?;

    materialize::materialize_vtable_insert_select_sources(backend, &mut statements, &params)
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

    let mut provider = functions.clone();
    prepare_rewritten_statements_with_backend(
        backend,
        statements,
        &params,
        &mut provider,
        writer_key,
    )
    .await
}

async fn prepare_rewritten_statements_with_backend<P>(
    backend: &dyn LixBackend,
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut live_table_requirements: Vec<SchemaLiveTableRequirement> = Vec::new();
    let mut internal_state: Option<InternalStatePlan> = None;
    let mut mutations: Vec<MutationRow> = Vec::new();
    let mut update_validations: Vec<UpdateValidationPlan> = Vec::new();

    for (statement_index, statement) in statements.into_iter().enumerate() {
        let output = if let Some(output) = rewrite_top_level_internal_state_read_statement_backend(
            backend,
            statement.clone(),
            params,
        )
        .await?
        {
            output
        } else {
            Box::pin(rewrite_statement_with_backend(
                backend, statement, params, writer_key, provider,
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
            &mut live_table_requirements,
            &mut internal_state,
            &mut mutations,
            &mut update_validations,
        )?;
    }

    if requires_single_statement_postprocess(
        internal_state
            .as_ref()
            .and_then(|plan| plan.postprocess.as_ref()),
    ) && rewritten.len() != 1
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, backend.dialect())?;

    Ok(PreprocessOutput {
        sql: normalized_sql,
        prepared_statements,
        live_table_requirements,
        internal_state,
        mutations,
        update_validations,
    })
}

fn accumulate_rewrite_output<P: LixFunctionProvider>(
    output: StatementRewriteOutput,
    provider: &mut P,
    dialect: SqlDialect,
    rewritten: &mut Vec<RewrittenStatementBinding>,
    live_table_requirements: &mut Vec<SchemaLiveTableRequirement>,
    internal_state: &mut Option<InternalStatePlan>,
    mutations: &mut Vec<MutationRow>,
    update_validations: &mut Vec<UpdateValidationPlan>,
) -> Result<(), LixError> {
    live_table_requirements.extend(output.live_table_requirements);
    if let Some(plan) = output.internal_state {
        if internal_state.is_some() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "only one postprocess rewrite is supported per query".to_string(),
            });
        }
        *internal_state = Some(plan);
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

    Ok((rendered.join("; "), prepared_statements))
}

fn from_rewrite_output(output: RewriteOutput) -> StatementRewriteOutput {
    StatementRewriteOutput {
        statements: output.statements,
        params: output.params,
        live_table_requirements: output.live_table_requirements,
        internal_state: internal_state_plan_from_postprocess(output.postprocess),
        mutations: output.mutations,
        update_validations: output.update_validations,
    }
}

fn rewrite_top_level_internal_state_read_statement_sync(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Option<RewriteOutput>, LixError> {
    match statement {
        Statement::Query(query) => rewrite_internal_state_query_read_sync(*query, dialect),
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
                Statement::Query(query) => {
                    match rewrite_internal_state_query_read_sync(*query, dialect)? {
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
                                    description:
                                        "expected rewritten read query to remain a SELECT query"
                                            .to_string(),
                                })?,
                        )),
                        None => return Ok(None),
                    }
                }
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

async fn rewrite_top_level_internal_state_read_statement_backend(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
) -> Result<Option<RewriteOutput>, LixError> {
    match statement {
        Statement::Query(query) => {
            rewrite_internal_state_query_read_backend(backend, *query, params).await
        }
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
                Statement::Query(query) => {
                    match rewrite_internal_state_query_read_backend(backend, *query, params).await?
                    {
                        Some(output) => Statement::Query(Box::new(
                            output
                                .statements
                                .into_iter()
                                .next()
                                .and_then(|stmt| match stmt {
                                    Statement::Query(query) => Some(*query),
                                    _ => None,
                                })
                                .ok_or_else(|| {
                                    LixError {
                                code: "LIX_ERROR_UNKNOWN".to_string(),
                                description:
                                    "expected rewritten backend read query to remain a SELECT query"
                                        .to_string(),
                            }
                                })?,
                        )),
                        None => return Ok(None),
                    }
                }
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

fn rewrite_internal_state_query_read_sync(
    query: Query,
    _dialect: SqlDialect,
) -> Result<Option<RewriteOutput>, LixError> {
    let statement = Statement::Query(Box::new(query.clone()));
    if statement_references_internal_state_vtable(&statement) {
        let rewritten = rewrite_internal_state_query_read(query, &[])?;
        return Ok(Some(rewrite_output_from_statement(Statement::Query(
            Box::new(rewritten),
        ))));
    }
    Ok(None)
}

async fn rewrite_internal_state_query_read_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Option<RewriteOutput>, LixError> {
    let statement = Statement::Query(Box::new(query.clone()));
    if statement_references_internal_state_vtable(&statement) {
        let rewritten =
            rewrite_internal_state_query_read_with_backend(backend, query, params).await?;
        return Ok(Some(rewrite_output_from_statement(Statement::Query(
            Box::new(rewritten),
        ))));
    }
    Ok(None)
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
        live_table_requirements: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}
