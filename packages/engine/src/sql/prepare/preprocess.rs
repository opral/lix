use super::statement_references_public_surface;
use crate::catalog::SurfaceRegistry;
use crate::contracts::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::logical_plan::{
    result_contract_for_statements, verify_direct_logical_plan, DirectLogicalPlan,
};
use crate::sql::semantic_ir::prepare_direct_statements_to_plan;
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

pub(crate) async fn preprocess_with_surfaces_to_logical_plan<P: LixFunctionProvider>(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<DirectLogicalPlan, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    reject_public_surface_statements(registry, &statements)?;
    let result_contract = result_contract_for_statements(&statements);
    let normalized_statements =
        prepare_direct_statements_to_plan(dialect, statements, params, functions, writer_key)
            .await?;
    let logical_plan = DirectLogicalPlan {
        normalized_statements,
        result_contract,
    };
    verify_direct_logical_plan(&logical_plan).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "direct logical plan verification failed during preprocess: {}",
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
