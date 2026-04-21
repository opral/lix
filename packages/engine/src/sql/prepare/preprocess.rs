use super::statement_references_public_surface;
use crate::catalog::SurfaceRegistry;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::logical_plan::{result_contract_for_statements, DirectLogicalPlan, ResultContract};
use crate::sql::semantic_ir::prepare_direct_statements_to_plan;
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

pub(crate) async fn preprocess_with_surfaces_to_logical_plan<P: LixFunctionProvider>(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    origin_key: Option<&str>,
) -> Result<DirectLogicalPlan, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    reject_public_surface_statements(registry, &statements)?;
    let result_contract = result_contract_for_statements(&statements);
    let normalized_statements =
        prepare_direct_statements_to_plan(dialect, statements, params, functions, origin_key)
            .await?;
    let logical_plan = DirectLogicalPlan {
        normalized_statements,
        result_contract,
    };
    ensure_internal_preprocess_plan_has_execution(&logical_plan)?;
    Ok(logical_plan)
}

fn ensure_internal_preprocess_plan_has_execution(plan: &DirectLogicalPlan) -> Result<(), LixError> {
    if plan.normalized_statements.prepared_statements.is_empty()
        && !matches!(plan.result_contract, ResultContract::DmlNoReturning)
        && plan.normalized_statements.mutations.is_empty()
        && plan.normalized_statements.update_validations.is_empty()
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler produced an internal execution plan without statements or explicit internal effects",
        ));
    }

    Ok(())
}

fn reject_public_surface_statements(
    registry: &SurfaceRegistry,
    statements: &[Statement],
) -> Result<(), LixError> {
    for statement in statements {
        if statement_references_public_surface(registry, statement) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public surface statements must route through public lowering",
            ));
        }
    }
    Ok(())
}
