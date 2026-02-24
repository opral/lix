use std::sync::Arc;
use std::{fmt::Write as _, string::String};

use sqlparser::ast::{Expr, Insert, Query, SetExpr, Statement, Value as SqlAstValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::backend::SqlDialect;
use crate::cel::CelEvaluator;
use crate::default_values::apply_vtable_insert_defaults;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::engine::sql2::planning::rewrite_engine::lowering::lower_statement;
use crate::engine::sql2::planning::rewrite_engine::materialize_vtable_insert_select_sources;
use crate::engine::sql2::planning::rewrite_engine::object_name_matches;
use crate::engine::sql2::planning::rewrite_engine::steps::inline_lix_functions::inline_lix_functions_with_provider;
use crate::engine::sql2::planning::rewrite_engine::types::{PostprocessPlan, PreparedStatement, PreprocessOutput, SchemaRegistration};
use crate::engine::sql2::planning::rewrite_engine::DetectedFileDomainChange;
use crate::engine::sql2::planning::rewrite_engine::{bind_sql_with_state_and_appended_params, PlaceholderState};
use crate::{LixBackend, LixError, Value};

pub(crate) mod context;
pub(crate) mod query_engine;
pub(crate) mod registry;
pub(crate) mod rules;
pub(crate) mod statement_pipeline;
pub(crate) mod validator;
pub(crate) mod walker;

use self::statement_pipeline::StatementPipeline;

struct RewrittenStatementBinding {
    statement: Statement,
    appended_params: Arc<Vec<Value>>,
}

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
        let appended_params = Arc::new(output.params);
        for rewritten_statement in output.statements {
            let inlined = inline_lix_functions_with_provider(rewritten_statement, provider);
            rewritten.push(RewrittenStatementBinding {
                statement: lower_statement(inlined, dialect)?,
                appended_params: Arc::clone(&appended_params),
            });
        }
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, dialect)?;

    Ok(PreprocessOutput {
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
        let appended_params = Arc::new(output.params);
        for rewritten_statement in output.statements {
            let inlined = inline_lix_functions_with_provider(rewritten_statement, provider);
            rewritten.push(RewrittenStatementBinding {
                statement: lower_statement(inlined, backend.dialect())?,
                appended_params: Arc::clone(&appended_params),
            });
        }
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, backend.dialect())?;

    Ok(PreprocessOutput {
        sql: normalized_sql,
        prepared_statements,
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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn preprocess_plan_fingerprint(output: &PreprocessOutput) -> String {
    let mut serialized = String::new();

    serialized.push_str("sql:");
    serialized.push_str(&output.sql);
    serialized.push('\u{1f}');

    for statement in &output.prepared_statements {
        serialized.push_str("stmt:");
        serialized.push_str(&statement.sql);
        serialized.push('\u{1e}');
        for value in &statement.params {
            let encoded =
                serde_json::to_string(value).expect("serializing statement params must succeed");
            serialized.push_str(&encoded);
            serialized.push('\u{1d}');
        }
        serialized.push('\u{1f}');
    }

    for registration in &output.registrations {
        serialized.push_str("registration:");
        serialized.push_str(&registration.schema_key);
        serialized.push('\u{1f}');
    }

    match &output.postprocess {
        None => serialized.push_str("postprocess:none"),
        Some(PostprocessPlan::VtableUpdate(plan)) => {
            serialized.push_str("postprocess:vtable_update:");
            serialized.push_str(&plan.schema_key);
            serialized.push('\u{1e}');
            match &plan.explicit_writer_key {
                Some(Some(value)) => {
                    serialized.push_str("writer:");
                    serialized.push_str(value);
                }
                Some(None) => serialized.push_str("writer:null"),
                None => serialized.push_str("writer:implicit"),
            }
            serialized.push('\u{1e}');
            let _ = write!(
                serialized,
                "writer_assignment:{}",
                plan.writer_key_assignment_present
            );
        }
        Some(PostprocessPlan::VtableDelete(plan)) => {
            serialized.push_str("postprocess:vtable_delete:");
            serialized.push_str(&plan.schema_key);
            serialized.push('\u{1e}');
            let _ = write!(
                serialized,
                "scope_fallback:{}",
                plan.effective_scope_fallback
            );
            serialized.push('\u{1e}');
            match &plan.effective_scope_selection_sql {
                Some(sql) => serialized.push_str(sql),
                None => serialized.push_str("scope_sql:none"),
            }
        }
    }
    serialized.push('\u{1f}');

    for mutation in &output.mutations {
        let _ = write!(
            serialized,
            "mutation:{:?}|{}|{}|{}|{}|{}|{}|{}|",
            mutation.operation,
            mutation.entity_id,
            mutation.schema_key,
            mutation.schema_version,
            mutation.file_id,
            mutation.version_id,
            mutation.plugin_key,
            mutation.untracked
        );
        match &mutation.snapshot_content {
            Some(snapshot) => serialized.push_str(&snapshot.to_string()),
            None => serialized.push_str("snapshot:none"),
        }
        serialized.push('\u{1f}');
    }

    for validation in &output.update_validations {
        serialized.push_str("validation:");
        serialized.push_str(&validation.table);
        serialized.push('\u{1e}');
        match &validation.where_clause {
            Some(clause) => serialized.push_str(&clause.to_string()),
            None => serialized.push_str("where:none"),
        }
        serialized.push('\u{1e}');
        match &validation.snapshot_content {
            Some(snapshot) => serialized.push_str(&snapshot.to_string()),
            None => serialized.push_str("snapshot_content:none"),
        }
        serialized.push('\u{1e}');
        match &validation.snapshot_patch {
            Some(patch) => serialized.push_str(
                &serde_json::to_string(patch).expect("snapshot patch serialization must succeed"),
            ),
            None => serialized.push_str("snapshot_patch:none"),
        }
        serialized.push('\u{1f}');
    }

    blake3::hash(serialized.as_bytes()).to_hex().to_string()
}

fn render_statements_with_params(
    statements: &[RewrittenStatementBinding],
    base_params: &[Value],
    dialect: SqlDialect,
) -> Result<(String, Vec<PreparedStatement>), LixError> {
    let mut rendered = Vec::with_capacity(statements.len());
    let mut prepared_statements = Vec::with_capacity(statements.len());
    let mut placeholder_state = PlaceholderState::new();

    for statement in statements {
        let bound = bind_sql_with_state_and_appended_params(
            &statement.statement.to_string(),
            base_params,
            statement.appended_params.as_slice(),
            dialect,
            placeholder_state,
        )?;
        placeholder_state = bound.state;
        rendered.push(bound.sql.clone());
        prepared_statements.push(PreparedStatement {
            sql: bound.sql,
            params: bound.params,
        });
    }

    let normalized_sql = rendered.join("; ");
    Ok((normalized_sql, prepared_statements))
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
    if insert_targets_stored_schema(left) || insert_targets_stored_schema(right) {
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

fn insert_targets_stored_schema(insert: &Insert) -> bool {
    let schema_key_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("schema_key"));
    let Some(schema_key_index) = schema_key_index else {
        return false;
    };

    let Some(rows) = plain_values_rows(insert) else {
        return false;
    };

    rows.iter().any(|row| {
        row.get(schema_key_index)
            .is_some_and(expr_is_stored_schema_literal)
    })
}

fn expr_is_stored_schema_literal(expr: &Expr) -> bool {
    let Expr::Value(value) = expr else {
        return false;
    };
    let literal = match &value.value {
        SqlAstValue::SingleQuotedString(text) | SqlAstValue::DoubleQuotedString(text) => text,
        _ => return false,
    };
    literal.eq_ignore_ascii_case("lix_stored_schema")
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
    use super::{
        coalesce_vtable_inserts_in_statement_list, parse_sql_statements, plain_values_rows,
        preprocess_plan_fingerprint, preprocess_sql_rewrite_only, preprocess_statements,
    };
    use crate::backend::SqlDialect;
    use crate::Value;
    use sqlparser::ast::Statement;

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

    #[test]
    fn preprocess_multi_statement_sqlite_anonymous_placeholders_keep_ordinal_progression() {
        let statements = parse_sql_statements("SELECT ?; SELECT ?").expect("parse should succeed");
        let rewritten = preprocess_statements(
            statements,
            &[Value::Integer(1), Value::Integer(2)],
            SqlDialect::Sqlite,
        )
        .expect("rewrite should succeed");

        assert_eq!(rewritten.prepared_statements.len(), 2);
        assert_eq!(
            rewritten.prepared_statements[0].params,
            vec![Value::Integer(1)]
        );
        assert_eq!(
            rewritten.prepared_statements[1].params,
            vec![Value::Integer(2)]
        );
    }

    #[test]
    fn preprocess_output_uses_prepared_statement_params() {
        let statements = parse_sql_statements("SELECT ?").expect("parse should succeed");
        let rewritten =
            preprocess_statements(statements, &[Value::Integer(7)], SqlDialect::Sqlite)
                .expect("rewrite should succeed");

        assert_eq!(
            rewritten.prepared_statements[0].params,
            vec![Value::Integer(7)]
        );
    }

    #[test]
    fn preprocess_plan_fingerprint_is_stable_for_identical_inputs() {
        let statements_a =
            parse_sql_statements("SELECT ?; SELECT ?2; SELECT ?").expect("parse A should succeed");
        let output_a = preprocess_statements(
            statements_a,
            &[Value::Integer(10), Value::Integer(20), Value::Integer(30)],
            SqlDialect::Sqlite,
        )
        .expect("rewrite A should succeed");

        let statements_b =
            parse_sql_statements("SELECT ?; SELECT ?2; SELECT ?").expect("parse B should succeed");
        let output_b = preprocess_statements(
            statements_b,
            &[Value::Integer(10), Value::Integer(20), Value::Integer(30)],
            SqlDialect::Sqlite,
        )
        .expect("rewrite B should succeed");

        assert_eq!(
            preprocess_plan_fingerprint(&output_a),
            preprocess_plan_fingerprint(&output_b),
            "fingerprint must be deterministic for identical planner output"
        );
    }

    #[test]
    fn preprocess_plan_fingerprint_changes_when_param_binding_changes() {
        let statements =
            parse_sql_statements("SELECT ?; SELECT ?2; SELECT ?").expect("parse should succeed");
        let output_a = preprocess_statements(
            statements.clone(),
            &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
            SqlDialect::Sqlite,
        )
        .expect("rewrite A should succeed");
        let output_b = preprocess_statements(
            statements,
            &[Value::Integer(1), Value::Integer(2), Value::Integer(4)],
            SqlDialect::Sqlite,
        )
        .expect("rewrite B should succeed");

        assert_ne!(
            preprocess_plan_fingerprint(&output_a),
            preprocess_plan_fingerprint(&output_b),
            "fingerprint must change when bound statement params change"
        );
    }

    #[test]
    fn coalesce_vtable_inserts_keeps_stored_schema_rows_separate() {
        let statements = parse_sql_statements(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES \
             ('lix_stored_schema', '{\"value\":{\"x-lix-key\":\"schema_a\",\"x-lix-version\":\"1\"}}'); \
             INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES \
             ('lix_stored_schema', '{\"value\":{\"x-lix-key\":\"schema_b\",\"x-lix-version\":\"1\"}}')",
        )
        .expect("parse should succeed");

        let coalesced = coalesce_vtable_inserts_in_statement_list(statements)
            .expect("coalescing should succeed");

        assert_eq!(
            coalesced.len(),
            2,
            "stored schema inserts must remain separate statements"
        );
    }

    #[test]
    fn coalesce_vtable_inserts_merges_non_stored_schema_rows() {
        let statements = parse_sql_statements(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ('entity-a', 'test_schema', 'file-1', 'version-main', 'lix', '{}', '1'); \
             INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ('entity-b', 'test_schema', 'file-1', 'version-main', 'lix', '{}', '1')",
        )
        .expect("parse should succeed");

        let coalesced = coalesce_vtable_inserts_in_statement_list(statements)
            .expect("coalescing should succeed");
        assert_eq!(coalesced.len(), 1, "non-stored schema inserts should merge");

        let Statement::Insert(insert) = &coalesced[0] else {
            panic!("expected merged insert statement");
        };
        let rows = plain_values_rows(insert).expect("merged statement should keep VALUES rows");
        assert_eq!(rows.len(), 2, "merged insert should contain both rows");
    }
}
