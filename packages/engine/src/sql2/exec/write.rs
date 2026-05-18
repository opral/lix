//! Write execution for bound sql2 plans.

use serde_json::json;

use datafusion::sql::parser::Statement as DataFusionStatement;

use super::SqlLogicalPlan;
use crate::sql2::parse::parse_statement;
use crate::sql2::plan::version_scope::VersionScope;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::SqlWriteExecutionContext;
use crate::{LixError, Value};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteExecutorMode {
    Auto,
    ForceDataFusion,
    ForceFast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteExecutorPath {
    Fast,
    DataFusion,
}

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
    let bound_write =
        crate::sql2::bind_statement(&statement, &visible_schemas, ctx.active_version_id())?;
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
    execute_write_logical_plan_with_mode_inner(ctx, plan, params, WriteExecutorModeInner::Auto)
        .await
        .map(|(rows_affected, _path)| rows_affected)
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan_with_mode(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    mode: WriteExecutorMode,
) -> Result<u64, LixError> {
    execute_write_logical_plan_with_mode_and_trace(ctx, plan, params, mode)
        .await
        .map(|(rows_affected, _path)| rows_affected)
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan_with_mode_and_trace(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    mode: WriteExecutorMode,
) -> Result<(u64, WriteExecutorPath), LixError> {
    let mode = match mode {
        WriteExecutorMode::Auto => WriteExecutorModeInner::Auto,
        WriteExecutorMode::ForceDataFusion => WriteExecutorModeInner::ForceDataFusion,
        WriteExecutorMode::ForceFast => WriteExecutorModeInner::ForceFast,
    };
    execute_write_logical_plan_with_mode_inner(ctx, plan, params, mode).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteExecutorModeInner {
    Auto,
    ForceDataFusion,
    ForceFast,
}

async fn execute_write_logical_plan_with_mode_inner(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    mode: WriteExecutorModeInner,
) -> Result<(u64, WriteExecutorPath), LixError> {
    let SqlLogicalPlan::Write(write_plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "expected SQL write logical plan",
        ));
    };
    let write_plan = resolve_parameterized_version_scope(write_plan.plan, params)?;
    validate_write_parameter_count(&write_plan, params.len())?;

    if mode != WriteExecutorModeInner::ForceDataFusion
        && super::bound_public_write::supports_bound_public_write(&write_plan)
    {
        let rows_affected =
            super::bound_public_write::execute_bound_public_write(ctx, &write_plan, params)
                .await
                .map_err(normalize_bound_public_write_error)?;
        return Ok((rows_affected, WriteExecutorPath::Fast));
    }

    if mode != WriteExecutorModeInner::ForceDataFusion {
        super::datafusion::validate_datafusion_write_logical_plan(ctx, &write_plan, params).await?;
        if let Some(fast_plan) =
            crate::sql2::optimize::simple_write::try_make_fast_write_plan(&write_plan)?
        {
            let rows_affected =
                crate::sql2::exec::fast_write::try_execute_simple_write(ctx, fast_plan, params)
                    .await?;
            return Ok((rows_affected, WriteExecutorPath::Fast));
        }
        if mode == WriteExecutorModeInner::ForceFast {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "SQL write plan is not eligible for fast execution",
            ));
        }
    }

    let rows_affected =
        super::datafusion::execute_datafusion_write_logical_plan(ctx, &write_plan, params).await?;
    Ok((rows_affected, WriteExecutorPath::DataFusion))
}

fn resolve_parameterized_version_scope(
    mut plan: LogicalWritePlan,
    params: &[Value],
) -> Result<LogicalWritePlan, LixError> {
    plan.bound.version_scope = match plan.bound.version_scope {
        VersionScope::ExplicitDynamic {
            mut version_ids,
            param_indexes,
        } => {
            insert_version_param_values(&mut version_ids, &param_indexes, params)?;
            if version_ids.is_empty() {
                VersionScope::Empty
            } else {
                VersionScope::Explicit { version_ids }
            }
        }
        VersionScope::ExplicitRequiredDynamic {
            mut version_ids,
            param_indexes,
        } => {
            insert_version_param_values(&mut version_ids, &param_indexes, params)?;
            if version_ids.is_empty() {
                VersionScope::Empty
            } else {
                VersionScope::ExplicitRequired { version_ids }
            }
        }
        scope => scope,
    };
    Ok(plan)
}

fn insert_version_param_values(
    version_ids: &mut std::collections::BTreeSet<String>,
    param_indexes: &std::collections::BTreeSet<usize>,
    params: &[Value],
) -> Result<(), LixError> {
    for index in param_indexes {
        match params.get(index.saturating_sub(1)) {
            Some(Value::Text(version_id)) => {
                version_ids.insert(version_id.clone());
            }
            Some(_) => {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "by-version SQL write selectors require text version-id parameters",
                ));
            }
            None => {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("SQL version selector parameter ${index} was not provided"),
                ));
            }
        }
    }
    Ok(())
}

fn normalize_bound_public_write_error(error: LixError) -> LixError {
    if error.code == LixError::CODE_SCHEMA_DEFINITION
        && error.message.to_ascii_lowercase().contains("system schema")
    {
        return LixError {
            code: LixError::CODE_INVALID_PARAM.to_string(),
            ..error
        };
    }
    error
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
