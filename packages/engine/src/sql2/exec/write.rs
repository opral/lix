//! Write execution for bound sql2 plans.

use std::collections::BTreeSet;

use serde_json::json;

use datafusion::sql::parser::Statement as DataFusionStatement;

use super::{SqlLogicalPlan, SqlWriteResult};
use crate::common::ExecuteStatementMetadata;
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::BoundWriteTarget;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;
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

#[cfg(test)]
pub(crate) async fn create_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
) -> Result<SqlLogicalPlan, LixError> {
    let statement = crate::sql2::parse::parse_statement(sql)?;
    create_write_logical_plan_from_parsed(ctx, statement).await
}

#[expect(clippy::needless_pass_by_ref_mut)]
#[cfg(test)]
async fn create_write_logical_plan_from_parsed(
    ctx: &mut dyn SqlWriteExecutionContext,
    statement: DataFusionStatement,
) -> Result<SqlLogicalPlan, LixError> {
    let visible_schemas = ctx.list_visible_schemas()?;
    let bound_write =
        crate::sql2::bind_statement(&statement, &visible_schemas, ctx.active_branch_id())?;
    let logical_write = crate::sql2::plan_write(bound_write)?;
    Ok(create_write_logical_plan_from_template(logical_write))
}

pub(crate) fn create_write_plan_template_from_parsed(
    statement: &DataFusionStatement,
    catalog: &crate::sql2::PublicCatalog,
    active_branch_id: &str,
) -> Result<LogicalWritePlan, LixError> {
    let bound_write =
        crate::sql2::bind_statement_with_catalog(statement, catalog, active_branch_id)?;
    crate::sql2::plan_write(bound_write)
}

pub(crate) fn create_write_logical_plan_from_template(
    logical_write: LogicalWritePlan,
) -> SqlLogicalPlan {
    SqlLogicalPlan::Write(WriteLogicalPlan {
        plan: logical_write,
    })
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<u64, LixError> {
    execute_write_logical_plan_result(ctx, plan, params)
        .await
        .map(|result| result.rows_affected)
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan_result(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    execute_write_logical_plan_result_with_metadata(
        ctx,
        plan,
        params,
        &ExecuteStatementMetadata::default(),
    )
    .await
}

pub(crate) async fn execute_write_logical_plan_result_with_metadata(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    metadata: &ExecuteStatementMetadata,
) -> Result<SqlWriteResult, LixError> {
    execute_write_logical_plan_with_mode_inner(
        ctx,
        plan,
        params,
        metadata,
        WriteExecutorModeInner::Auto,
    )
    .await
    .map(|(result, _path)| result)
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
        .map(|(result, _path)| result)
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan_with_mode_result(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    mode: WriteExecutorMode,
) -> Result<SqlWriteResult, LixError> {
    execute_write_logical_plan_with_mode_and_trace_result(ctx, plan, params, mode)
        .await
        .map(|(result, _path)| result)
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan_with_mode_and_trace(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    mode: WriteExecutorMode,
) -> Result<(u64, WriteExecutorPath), LixError> {
    execute_write_logical_plan_with_mode_and_trace_result(ctx, plan, params, mode)
        .await
        .map(|(result, path)| (result.rows_affected, path))
}

#[cfg(test)]
pub(crate) async fn execute_write_logical_plan_with_mode_and_trace_result(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
    mode: WriteExecutorMode,
) -> Result<(SqlWriteResult, WriteExecutorPath), LixError> {
    let mode = match mode {
        WriteExecutorMode::Auto => WriteExecutorModeInner::Auto,
        WriteExecutorMode::ForceDataFusion => WriteExecutorModeInner::ForceDataFusion,
        WriteExecutorMode::ForceFast => WriteExecutorModeInner::ForceFast,
    };
    execute_write_logical_plan_with_mode_inner(
        ctx,
        plan,
        params,
        &ExecuteStatementMetadata::default(),
        mode,
    )
    .await
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
    metadata: &ExecuteStatementMetadata,
    mode: WriteExecutorModeInner,
) -> Result<(SqlWriteResult, WriteExecutorPath), LixError> {
    let SqlLogicalPlan::Write(write_plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "expected SQL write logical plan",
        ));
    };
    let write_plan = resolve_parameterized_branch_scope(write_plan.plan, params)?;
    validate_write_parameter_count(&write_plan, params.len())?;

    if mode != WriteExecutorModeInner::ForceDataFusion {
        match super::bound_public_write::try_execute_bound_public_write(
            ctx,
            &write_plan,
            params,
            metadata,
        )
        .await
        .map_err(normalize_bound_public_write_error)?
        {
            super::bound_public_write::BoundPublicWriteExecution::Executed(result) => {
                return Ok((result, WriteExecutorPath::Fast));
            }
            super::bound_public_write::BoundPublicWriteExecution::Unsupported => {}
        }
    }

    if mode == WriteExecutorModeInner::ForceFast {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL write plan is not eligible for fast execution",
        ));
    }

    let result =
        super::datafusion::execute_datafusion_write_logical_plan(ctx, &write_plan, params).await?;
    Ok((result, WriteExecutorPath::DataFusion))
}

fn resolve_parameterized_branch_scope(
    mut plan: LogicalWritePlan,
    params: &[Value],
) -> Result<LogicalWritePlan, LixError> {
    plan.bound.branch_scope = match plan.bound.branch_scope {
        BranchScope::ExplicitDynamic {
            mut branch_ids,
            param_indexes,
        } => {
            insert_branch_param_values(
                &mut branch_ids,
                &param_indexes,
                params,
                BranchParamNullPolicy::Reject,
            )?;
            if branch_ids.is_empty() {
                BranchScope::Empty
            } else {
                BranchScope::Explicit { branch_ids }
            }
        }
        BranchScope::ExplicitRequiredDynamic {
            mut branch_ids,
            param_indexes,
        } => match branch_column_for_target(&plan.bound.target) {
            Some(branch_column) => {
                match resolved_predicate_branch_selector(
                    &plan.bound.predicate,
                    branch_column,
                    params,
                )? {
                    ResolvedBranchSelector::Static(branch_ids) if branch_ids.is_empty() => {
                        BranchScope::Empty
                    }
                    ResolvedBranchSelector::Static(branch_ids) => {
                        BranchScope::ExplicitRequired { branch_ids }
                    }
                    ResolvedBranchSelector::Missing => {
                        insert_branch_param_values(
                            &mut branch_ids,
                            &param_indexes,
                            params,
                            BranchParamNullPolicy::Ignore,
                        )?;
                        if branch_ids.is_empty() {
                            BranchScope::Empty
                        } else {
                            BranchScope::ExplicitRequired { branch_ids }
                        }
                    }
                }
            }
            None => {
                insert_branch_param_values(
                    &mut branch_ids,
                    &param_indexes,
                    params,
                    BranchParamNullPolicy::Ignore,
                )?;
                if branch_ids.is_empty() {
                    BranchScope::Empty
                } else {
                    BranchScope::ExplicitRequired { branch_ids }
                }
            }
        },
        scope => scope,
    };
    Ok(plan)
}

fn branch_column_for_target(target: &BoundWriteTarget) -> Option<&'static str> {
    match target {
        BoundWriteTarget::Entity(crate::sql2::bind::write::EntityWriteSurface::ByBranch {
            ..
        })
        | BoundWriteTarget::File(crate::sql2::bind::write::FileWriteSurface::ByBranch)
        | BoundWriteTarget::Directory(crate::sql2::bind::write::DirectoryWriteSurface::ByBranch) => {
            Some("lixcol_branch_id")
        }
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedBranchSelector {
    Missing,
    Static(BTreeSet<String>),
}

impl ResolvedBranchSelector {
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Missing, _) | (_, Self::Missing) => Self::Missing,
            (Self::Static(mut left), Self::Static(right)) => {
                left.extend(right);
                Self::Static(left)
            }
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Static(left), Self::Static(right)) => {
                Self::Static(left.intersection(&right).cloned().collect())
            }
        }
    }
}

fn resolved_predicate_branch_selector(
    predicate: &BoundPredicate,
    branch_column: &str,
    params: &[Value],
) -> Result<ResolvedBranchSelector, LixError> {
    match predicate {
        BoundPredicate::True => Ok(ResolvedBranchSelector::Missing),
        BoundPredicate::False => Ok(ResolvedBranchSelector::Static(BTreeSet::new())),
        BoundPredicate::And(predicates) => {
            let mut result = ResolvedBranchSelector::Missing;
            for predicate in predicates {
                result = result.intersect(resolved_predicate_branch_selector(
                    predicate,
                    branch_column,
                    params,
                )?);
            }
            Ok(result)
        }
        BoundPredicate::Or(predicates) => {
            let mut result = ResolvedBranchSelector::Static(BTreeSet::new());
            for predicate in predicates {
                result = result.union(resolved_predicate_branch_selector(
                    predicate,
                    branch_column,
                    params,
                )?);
            }
            Ok(result)
        }
        BoundPredicate::Eq(left, right) => {
            resolved_branch_selector_from_binary_exprs(left, right, branch_column, params)
                .or_else(|| {
                    resolved_branch_selector_from_binary_exprs(right, left, branch_column, params)
                })
                .transpose()
                .map(|selector| selector.unwrap_or(ResolvedBranchSelector::Missing))
        }
        BoundPredicate::Like { .. } | BoundPredicate::IsNull(_) | BoundPredicate::IsNotNull(_) => {
            Ok(ResolvedBranchSelector::Missing)
        }
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(ResolvedBranchSelector::Missing);
            };
            if column.name != branch_column {
                return Ok(ResolvedBranchSelector::Missing);
            }
            let mut result = ResolvedBranchSelector::Static(BTreeSet::new());
            for value in values {
                result = result.union(resolved_value_branch_selector(value, params)?);
            }
            Ok(result)
        }
    }
}

fn resolved_branch_selector_from_binary_exprs(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
    branch_column: &str,
    params: &[Value],
) -> Option<Result<ResolvedBranchSelector, LixError>> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name != branch_column {
        return None;
    }
    Some(resolved_value_branch_selector(value_expr, params))
}

fn resolved_value_branch_selector(
    expr: &BoundExpr,
    params: &[Value],
) -> Result<ResolvedBranchSelector, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(branch_id)) => {
            Ok(ResolvedBranchSelector::Static(BTreeSet::from([
                branch_id.clone()
            ])))
        }
        BoundExpr::Literal(BoundLiteral::Null) => {
            Ok(ResolvedBranchSelector::Static(BTreeSet::new()))
        }
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Text(branch_id)) => Ok(ResolvedBranchSelector::Static(BTreeSet::from([
                branch_id.clone(),
            ]))),
            Some(Value::Null) => Ok(ResolvedBranchSelector::Static(BTreeSet::new())),
            Some(_) => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "by-branch SQL write selectors require text branch-id parameters",
            )),
            None => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "SQL branch selector parameter ${} was not provided",
                    param.index
                ),
            )),
        },
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "by-branch SQL write predicates require string branch ids",
        )),
    }
}

fn insert_branch_param_values(
    branch_ids: &mut BTreeSet<String>,
    param_indexes: &BTreeSet<usize>,
    params: &[Value],
    null_policy: BranchParamNullPolicy,
) -> Result<(), LixError> {
    for index in param_indexes {
        match params.get(index.saturating_sub(1)) {
            Some(Value::Text(branch_id)) => {
                branch_ids.insert(branch_id.clone());
            }
            Some(Value::Null) if null_policy == BranchParamNullPolicy::Ignore => {}
            Some(Value::Null) => {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "INSERT into a by-branch SQL surface requires non-null text branch-id parameters",
                ));
            }
            Some(_) => {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "by-branch SQL write selectors require text branch-id parameters",
                ));
            }
            None => {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("SQL branch selector parameter ${index} was not provided"),
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BranchParamNullPolicy {
    Reject,
    Ignore,
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
