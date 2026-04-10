use super::statement_references_public_surface;
use crate::catalog::SurfaceRegistry;
use crate::contracts::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::logical_plan::{
    result_contract_for_statements, verify_logical_plan, InternalLogicalPlan, LogicalPlan,
};
use crate::sql::semantic_ir::prepare_internal_statements_to_plan;
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

pub(crate) async fn preprocess_with_surfaces_to_logical_plan<P: LixFunctionProvider>(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<InternalLogicalPlan, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    reject_public_surface_statements(registry, &statements)?;
    let result_contract = result_contract_for_statements(&statements);
    let normalized_statements =
        prepare_internal_statements_to_plan(dialect, statements, params, functions, writer_key)
            .await?;
    let logical_plan = InternalLogicalPlan {
        normalized_statements,
        result_contract,
    };
    verify_logical_plan(&LogicalPlan::Internal(logical_plan.clone())).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "internal logical plan verification failed during preprocess: {}",
                error.message
            ),
        )
    })?;
    Ok(logical_plan)
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
