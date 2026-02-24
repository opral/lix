use std::sync::Arc;

use crate::cel::CelEvaluator;
use crate::default_values::apply_vtable_insert_defaults;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::super::super::sql_preprocess_runtime::{
    legacy_rewrite_statement_with_backend, legacy_rewrite_statement_with_provider,
    LegacyRewriteOutput,
};
use super::super::ast::lowering::lower_statement;
use super::super::ast::nodes::Statement;
use super::super::ast::utils::{
    bind_sql_with_state_and_appended_params, parse_sql_statements, PlaceholderState,
};
use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::planned_statement::{
    MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use super::super::contracts::postprocess_actions::PostprocessPlan;
use super::super::contracts::prepared_statement::PreparedStatement;
use super::inline_functions::inline_lix_functions_with_provider;
use super::materialize::materialize_vtable_insert_select_sources;
use super::script::coalesce_vtable_inserts_in_transactions;

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
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut mutations: Vec<MutationRow> = Vec::new();
    let mut update_validations: Vec<UpdateValidationPlan> = Vec::new();

    for statement in statements {
        let output =
            legacy_rewrite_statement_with_provider(params, writer_key, statement, provider)?;
        accumulate_rewrite_output(
            output,
            provider,
            dialect,
            &mut rewritten,
            &mut registrations,
            &mut postprocess,
            &mut mutations,
            &mut update_validations,
        )?;
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
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
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
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
        let statement_detected_file_domain_changes = detected_file_domain_changes_by_statement
            .get(statement_index)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        // Keep this async rewrite future boxed to avoid infinitely sized
        // futures in recursive rewrite call paths.
        let output = Box::pin(legacy_rewrite_statement_with_backend(
            backend,
            params,
            writer_key,
            statement,
            provider,
            statement_detected_file_domain_changes,
        ))
        .await?;

        accumulate_rewrite_output(
            output,
            provider,
            backend.dialect(),
            &mut rewritten,
            &mut registrations,
            &mut postprocess,
            &mut mutations,
            &mut update_validations,
        )?;
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
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
    preprocess_sql_with_provider_and_detected_file_domain_changes(
        backend,
        evaluator,
        sql_text,
        params,
        functions,
        &[],
        None,
    )
    .await
}

async fn preprocess_sql_with_provider_and_detected_file_domain_changes<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
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
        detected_file_domain_changes_by_statement,
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
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
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

fn accumulate_rewrite_output<P: LixFunctionProvider>(
    output: LegacyRewriteOutput,
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
                message: "only one postprocess rewrite is supported per query".to_string(),
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
