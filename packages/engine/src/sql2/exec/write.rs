//! Write execution for bound sql2 plans.

use serde_json::json;

use datafusion::sql::parser::Statement as DataFusionStatement;

use super::SqlLogicalPlan;
use crate::sql2::parse::parse_statement;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::SqlWriteExecutionContext;
use crate::{LixError, Value};

#[allow(dead_code)]
pub(crate) struct WriteLogicalPlan {
    pub(super) plan: LogicalWritePlan,
}

#[allow(dead_code)]
pub(crate) async fn create_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
) -> Result<SqlLogicalPlan, LixError> {
    let statement = parse_statement(sql)?;
    create_write_logical_plan_from_parsed(ctx, statement).await
}

pub(crate) async fn create_write_logical_plan_from_parsed(
    ctx: &mut dyn SqlWriteExecutionContext,
    statement: DataFusionStatement,
) -> Result<SqlLogicalPlan, LixError> {
    let visible_schemas = ctx.list_visible_schemas()?;
    let bound_statement =
        crate::sql2::bind_statement(&statement, &visible_schemas, ctx.active_version_id())?;
    let crate::sql2::BoundStatement::Write(bound_write) = bound_statement else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "expected SQL write statement after binding",
        ));
    };
    let logical_write = crate::sql2::plan_write(bound_write)?;
    Ok(SqlLogicalPlan::Write(WriteLogicalPlan {
        plan: logical_write,
    }))
}

pub(crate) async fn execute_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<u64, LixError> {
    let SqlLogicalPlan::Write(write_plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "expected SQL write logical plan",
        ));
    };
    validate_write_parameter_count(&write_plan.plan, params.len())?;

    if let Some(fast_plan) =
        crate::sql2::optimize::simple_write::try_make_fast_write_plan(&write_plan.plan)?
    {
        return crate::sql2::exec::fast_write::try_execute_simple_write(ctx, fast_plan, params)
            .await;
    }

    super::datafusion::execute_datafusion_write_logical_plan(ctx, &write_plan.plan, params).await
}

fn validate_write_parameter_count(
    plan: &LogicalWritePlan,
    param_count: usize,
) -> Result<(), LixError> {
    let expected_count = plan.bound.params.params.keys().copied().max().unwrap_or(0);
    if param_count == expected_count {
        return Ok(());
    }

    Err(LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!(
            "SQL expected {expected_count} parameter(s), but {param_count} parameter(s) were provided"
        ),
    )
    .with_details(json!({
        "operation": "execute",
        "expected_param_count": expected_count,
        "provided_param_count": param_count,
        "placeholders": plan
            .bound
            .params
            .params
            .keys()
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>(),
    })))
}
