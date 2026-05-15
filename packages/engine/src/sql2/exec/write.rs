//! Write execution for bound sql2 plans.

use serde_json::json;

use super::datafusion::SqlLogicalPlan;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::SqlWriteExecutionContext;
use crate::{LixError, Value};

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
        if let Some(rows_affected) =
            crate::sql2::exec::fast_write::try_execute_simple_write(ctx, fast_plan, params).await?
        {
            return Ok(rows_affected);
        }
    }

    Err(LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "sql2 DataFusion write execution from bound plans is not wired yet",
    ))
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
