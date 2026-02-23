use sqlparser::ast::Statement;

use crate::cel::CelEvaluator;
use crate::default_values::apply_vtable_insert_defaults;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::{
    materialize_vtable_insert_select_sources, normalize_statement_placeholders_with_state,
    PlaceholderState,
};
use crate::{LixBackend, LixError, Value};

use super::catalog::PlannerCatalogSnapshot;
use super::emit::statement::emit_physical_statement_plan_with_state;
use super::ir::logical::LogicalStatementOperation;
use super::rewrite::statement::rewrite_statement_to_logical_plan_with_backend;
use super::types::CompiledStatementPlan;
use super::validate::{
    ensure_postprocess_single_statement, ensure_single_statement_plan,
    PostprocessSingleStatementContext,
};

pub(crate) async fn compile_statement_with_state<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    catalog_snapshot: &PlannerCatalogSnapshot,
    evaluator: &CelEvaluator,
    statement: Statement,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
    placeholder_state: PlaceholderState,
) -> Result<(CompiledStatementPlan, PlaceholderState), LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    compile_statement_plan_with_state(
        backend,
        catalog_snapshot,
        evaluator,
        statement,
        params,
        functions,
        writer_key,
        placeholder_state,
    )
    .await
}

async fn compile_statement_plan_with_state<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    catalog_snapshot: &PlannerCatalogSnapshot,
    evaluator: &CelEvaluator,
    statement: Statement,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
    initial_placeholder_state: PlaceholderState,
) -> Result<(CompiledStatementPlan, PlaceholderState), LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let mut statement = statement;
    let next_placeholder_state = normalize_statement_placeholders_with_state(
        &mut statement,
        params.len(),
        backend.dialect(),
        initial_placeholder_state,
    )?;

    let mut statements = vec![statement];
    materialize_vtable_insert_select_sources(backend, &mut statements, params).await?;
    apply_vtable_insert_defaults(
        backend,
        evaluator,
        &mut statements,
        params,
        functions.clone(),
    )
    .await?;
    ensure_single_statement_plan(statements.len())?;
    let statement = statements.remove(0);

    let mut provider = functions.clone();
    let logical_plan = rewrite_statement_to_logical_plan_with_backend(
        backend,
        catalog_snapshot,
        statement,
        params,
        writer_key,
        &mut provider,
        &[],
    )
    .await?;
    ensure_postprocess_single_statement(
        logical_plan.postprocess.is_some(),
        logical_plan.planned_statements.len(),
        PostprocessSingleStatementContext::CompilePlan,
    )?;

    let (prepared_statements, _) = emit_physical_statement_plan_with_state(
        &logical_plan,
        params,
        backend.dialect(),
        &mut provider,
        initial_placeholder_state,
    )?;
    if matches!(
        logical_plan.operation,
        LogicalStatementOperation::CanonicalWrite
    ) && prepared_statements.is_empty()
    {
        return Err(LixError {
            message: "planner canonical write emitted no executable statements".to_string(),
        });
    }

    Ok((
        CompiledStatementPlan {
            prepared_statements,
            registrations: logical_plan.registrations,
            maintenance_requirements: logical_plan.maintenance_requirements,
            postprocess: logical_plan.postprocess,
            mutations: logical_plan.mutations,
            update_validations: logical_plan.update_validations,
        },
        next_placeholder_state,
    ))
}
