use sqlparser::ast::{Expr, Insert, Query, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::backend::SqlDialect;
use crate::cel::CelEvaluator;
use crate::default_values::apply_vtable_insert_defaults;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::sql::bind_sql;
use crate::sql::lowering::lower_statement;
use crate::sql::materialize_vtable_insert_select_sources;
use crate::sql::object_name_matches;
use crate::sql::steps::inline_lix_functions::inline_lix_functions_with_provider;
use crate::sql::types::{PostprocessPlan, PreprocessOutput, SchemaRegistration};
use crate::sql::DetectedFileDomainChange;
use crate::{LixBackend, LixError, Value};

pub(crate) mod context;
pub(crate) mod query_engine;
pub(crate) mod registry;
pub(crate) mod rules;
pub(crate) mod statement_pipeline;
pub(crate) mod validator;
pub(crate) mod walker;

use self::statement_pipeline::StatementPipeline;

pub fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
        message: err.to_string(),
    })
}

pub fn preprocess_statements(
    statements: Vec<Statement>,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<PreprocessOutput, LixError> {
    let mut provider = SystemFunctionProvider;
    preprocess_statements_with_provider(statements, params, &mut provider, dialect)
}

pub fn preprocess_statements_with_provider<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PreprocessOutput, LixError> {
    preprocess_statements_with_provider_and_writer_key(statements, params, provider, dialect, None)
}

pub fn preprocess_statements_with_provider_and_writer_key<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError> {
    let statement_pipeline = StatementPipeline::new(params, writer_key);
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut mutations = Vec::new();
    let mut update_validations = Vec::new();
    for statement in statements {
        let output = statement_pipeline.rewrite_statement(statement, provider)?;
        registrations.extend(output.registrations);
        if let Some(plan) = output.postprocess {
            if postprocess.is_some() {
                return Err(LixError {
                    message: "only one postprocess rewrite is supported per query".to_string(),
                });
            }
            postprocess = Some(plan);
        }
        mutations.extend(output.mutations);
        update_validations.extend(output.update_validations);
        for rewritten_statement in output.statements {
            let inlined = inline_lix_functions_with_provider(rewritten_statement, provider);
            rewritten.push(lower_statement(inlined, dialect)?);
        }
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, params) = render_statements_with_params(&rewritten, params, dialect)?;

    Ok(PreprocessOutput {
        sql: normalized_sql,
        params,
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
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let statement_pipeline = StatementPipeline::new(params, writer_key);
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut mutations = Vec::new();
    let mut update_validations = Vec::new();
    for (statement_index, statement) in statements.into_iter().enumerate() {
        let statement_detected_file_domain_changes = detected_file_domain_changes_by_statement
            .get(statement_index)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        // Keep this large async rewrite future on the heap to avoid excessive
        // stack growth in callers that process many rewrite layers.
        let output = Box::pin(statement_pipeline.rewrite_statement_with_backend(
            backend,
            statement,
            provider,
            statement_detected_file_domain_changes,
        ))
        .await?;
        registrations.extend(output.registrations);
        if let Some(plan) = output.postprocess {
            if postprocess.is_some() {
                return Err(LixError {
                    message: "only one postprocess rewrite is supported per query".to_string(),
                });
            }
            postprocess = Some(plan);
        }
        mutations.extend(output.mutations);
        update_validations.extend(output.update_validations);
        for rewritten_statement in output.statements {
            let inlined = inline_lix_functions_with_provider(rewritten_statement, provider);
            rewritten.push(lower_statement(inlined, backend.dialect())?);
        }
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, params) =
        render_statements_with_params(&rewritten, params, backend.dialect())?;

    Ok(PreprocessOutput {
        sql: normalized_sql,
        params,
        registrations,
        postprocess,
        mutations,
        update_validations,
    })
}

#[allow(dead_code)]
pub async fn preprocess_sql(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql: &str,
    params: &[Value],
) -> Result<PreprocessOutput, LixError> {
    let functions = SharedFunctionProvider::new(SystemFunctionProvider);
    preprocess_sql_with_provider(backend, evaluator, sql, params, functions).await
}

pub async fn preprocess_sql_with_provider<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    preprocess_sql_with_provider_and_detected_file_domain_changes(
        backend,
        evaluator,
        sql,
        params,
        functions,
        &[],
        None,
    )
    .await
}

pub async fn preprocess_sql_with_provider_and_detected_file_domain_changes<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    preprocess_parsed_statements_with_provider_and_detected_file_domain_changes(
        backend,
        evaluator,
        parse_sql_statements(sql)?,
        params,
        functions,
        detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await
}

pub async fn preprocess_parsed_statements_with_provider_and_detected_file_domain_changes<
    P: LixFunctionProvider,
>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let params = params.to_vec();
    let mut statements = coalesce_vtable_inserts_in_transactions(statements)?;
    materialize_vtable_insert_select_sources(backend, &mut statements, &params).await?;
    apply_vtable_insert_defaults(
        backend,
        evaluator,
        &mut statements,
        &params,
        functions.clone(),
    )
    .await?;
    let mut provider = functions.clone();
    preprocess_statements_with_provider_and_backend(
        backend,
        statements,
        &params,
        &mut provider,
        detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await
}

#[allow(dead_code)]
pub fn preprocess_sql_rewrite_only(sql: &str) -> Result<PreprocessOutput, LixError> {
    preprocess_statements(parse_sql_statements(sql)?, &[], SqlDialect::Sqlite)
}

fn render_statements_with_params(
    statements: &[Statement],
    params: &[Value],
    dialect: SqlDialect,
) -> Result<(String, Vec<Value>), LixError> {
    let mut rendered = Vec::with_capacity(statements.len());
    let mut bound_params = Vec::new();
    let mut statement_with_params_count = 0usize;

    for statement in statements {
        let bound = bind_sql(&statement.to_string(), params, dialect)?;
        let _placeholder_state = bound.state;
        if !bound.params.is_empty() {
            statement_with_params_count += 1;
            bound_params = bound.params;
        }
        rendered.push(bound.sql);
    }

    if statement_with_params_count > 1 || (statement_with_params_count == 1 && statements.len() > 1)
    {
        return Err(LixError {
            message: "multiple statements with placeholders are not supported".to_string(),
        });
    }

    Ok((rendered.join("; "), bound_params))
}

fn coalesce_vtable_inserts_in_transactions(
    statements: Vec<Statement>,
) -> Result<Vec<Statement>, LixError> {
    let mut result = Vec::with_capacity(statements.len());
    let mut in_transaction = false;
    let mut pending_insert: Option<Insert> = None;

    for statement in statements {
        match statement {
            Statement::StartTransaction { .. } => {
                flush_pending_insert(&mut result, &mut pending_insert);
                in_transaction = true;
                result.push(statement);
            }
            Statement::Commit { .. } | Statement::Rollback { .. } => {
                flush_pending_insert(&mut result, &mut pending_insert);
                in_transaction = false;
                result.push(statement);
            }
            Statement::Insert(insert) if in_transaction => {
                if let Some(existing) = pending_insert.as_mut() {
                    if can_merge_vtable_insert(existing, &insert) {
                        append_insert_rows(existing, &insert)?;
                    } else {
                        flush_pending_insert(&mut result, &mut pending_insert);
                        pending_insert = Some(insert);
                    }
                } else {
                    pending_insert = Some(insert);
                }
            }
            other => {
                flush_pending_insert(&mut result, &mut pending_insert);
                result.push(other);
            }
        }
    }

    flush_pending_insert(&mut result, &mut pending_insert);
    Ok(result)
}

pub(crate) fn coalesce_vtable_inserts_in_statement_list(
    statements: Vec<Statement>,
) -> Result<Vec<Statement>, LixError> {
    let mut result = Vec::with_capacity(statements.len());
    let mut pending_insert: Option<Insert> = None;

    for statement in statements {
        match statement {
            Statement::Insert(insert) => {
                if let Some(existing) = pending_insert.as_mut() {
                    if can_merge_vtable_insert(existing, &insert) {
                        append_insert_rows(existing, &insert)?;
                    } else {
                        flush_pending_insert(&mut result, &mut pending_insert);
                        pending_insert = Some(insert);
                    }
                } else {
                    pending_insert = Some(insert);
                }
            }
            other => {
                flush_pending_insert(&mut result, &mut pending_insert);
                result.push(other);
            }
        }
    }

    flush_pending_insert(&mut result, &mut pending_insert);
    Ok(result)
}

fn flush_pending_insert(result: &mut Vec<Statement>, pending_insert: &mut Option<Insert>) {
    if let Some(insert) = pending_insert.take() {
        result.push(Statement::Insert(insert));
    }
}

fn can_merge_vtable_insert(left: &Insert, right: &Insert) -> bool {
    if !insert_targets_vtable(left) || !insert_targets_vtable(right) {
        return false;
    }
    if left.columns != right.columns {
        return false;
    }

    // Conservative merge policy: only plain VALUES inserts with no dialect-specific modifiers.
    if left.or.is_some()
        || right.or.is_some()
        || left.ignore
        || right.ignore
        || left.overwrite
        || right.overwrite
        || !left.assignments.is_empty()
        || !right.assignments.is_empty()
        || left.partitioned.is_some()
        || right.partitioned.is_some()
        || !left.after_columns.is_empty()
        || !right.after_columns.is_empty()
        || left.on.is_some()
        || right.on.is_some()
        || left.returning.is_some()
        || right.returning.is_some()
        || left.replace_into
        || right.replace_into
        || left.priority.is_some()
        || right.priority.is_some()
        || left.insert_alias.is_some()
        || right.insert_alias.is_some()
        || left.settings.is_some()
        || right.settings.is_some()
        || left.format_clause.is_some()
        || right.format_clause.is_some()
    {
        return false;
    }

    if left.table.to_string() != right.table.to_string() {
        return false;
    }
    if left.table_alias != right.table_alias {
        return false;
    }
    if left.into != right.into || left.has_table_keyword != right.has_table_keyword {
        return false;
    }

    plain_values_rows(left).is_some() && plain_values_rows(right).is_some()
}

fn append_insert_rows(target: &mut Insert, incoming: &Insert) -> Result<(), LixError> {
    let incoming_rows = plain_values_rows(incoming)
        .ok_or_else(|| LixError {
            message: "transaction insert coalescing expected VALUES rows".to_string(),
        })?
        .to_vec();

    let target_rows = plain_values_rows_mut(target).ok_or_else(|| LixError {
        message: "transaction insert coalescing expected mutable VALUES rows".to_string(),
    })?;
    target_rows.extend(incoming_rows);
    Ok(())
}

fn insert_targets_vtable(insert: &Insert) -> bool {
    match &insert.table {
        sqlparser::ast::TableObject::TableName(name) => {
            object_name_matches(name, "lix_internal_state_vtable")
        }
        _ => false,
    }
}

fn plain_values_rows(insert: &Insert) -> Option<&Vec<Vec<Expr>>> {
    let source = insert.source.as_ref()?;
    if !query_is_plain_values(source) {
        return None;
    }
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    Some(&values.rows)
}

fn plain_values_rows_mut(insert: &mut Insert) -> Option<&mut Vec<Vec<Expr>>> {
    let source = insert.source.as_mut()?;
    if !query_is_plain_values(source) {
        return None;
    }
    let SetExpr::Values(values) = source.body.as_mut() else {
        return None;
    };
    Some(&mut values.rows)
}

fn query_is_plain_values(query: &Query) -> bool {
    query.with.is_none()
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && query.fetch.is_none()
        && query.locks.is_empty()
        && query.for_clause.is_none()
        && query.settings.is_none()
        && query.format_clause.is_none()
        && query.pipe_operators.is_empty()
}

#[cfg(test)]
mod tests {
    use super::{parse_sql_statements, preprocess_sql_rewrite_only, preprocess_statements};
    use crate::backend::SqlDialect;

    #[test]
    fn rewrite_only_path_lowers_lix_json_text_functions() {
        let rewritten = preprocess_sql_rewrite_only("SELECT version_id FROM lix_active_version")
            .expect("rewrite should succeed");

        assert!(
            !rewritten.sql.contains("lix_json_text("),
            "rewrite-only path must lower logical lix_json_text() calls"
        );
        assert!(
            rewritten.sql.contains("json_extract("),
            "rewrite-only sqlite lowering should emit json_extract()"
        );
    }

    #[test]
    fn preprocess_statements_uses_requested_dialect() {
        let statements = parse_sql_statements("SELECT version_id FROM lix_active_version")
            .expect("parse should succeed");
        let rewritten = preprocess_statements(statements, &[], SqlDialect::Postgres)
            .expect("rewrite should succeed");

        assert!(
            !rewritten.sql.contains("lix_json_text("),
            "preprocess path must lower logical lix_json_text() calls"
        );
        assert!(
            rewritten.sql.contains("jsonb_extract_path_text("),
            "postgres lowering should emit jsonb_extract_path_text()"
        );
    }

    #[test]
    fn rewrite_only_rewrites_lix_active_version_in_nested_subquery() {
        let rewritten = preprocess_sql_rewrite_only(
            "SELECT COUNT(*) \
             FROM lix_state_by_version \
             WHERE schema_key = 'bench_schema' \
               AND version_id IN (SELECT version_id FROM lix_active_version) \
               AND snapshot_content IS NOT NULL",
        )
        .expect("rewrite should succeed");

        assert!(
            !rewritten.sql.contains("FROM lix_active_version"),
            "nested lix_active_version should be rewritten"
        );
        assert!(
            rewritten.sql.contains("lix_internal_state_vtable"),
            "rewritten query should route through vtable reads"
        );
    }
}
