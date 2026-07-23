//! Write execution for bound sql2 plans.

use std::collections::BTreeSet;

use serde_json::json;

use datafusion::sql::parser::Statement as DataFusionStatement;

use super::{SqlLogicalPlan, SqlWriteResult};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{BoundWriteInput, BoundWriteTarget};
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;
use crate::{GLOBAL_BRANCH_ID, LixError, Value};

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

pub(crate) async fn execute_write_logical_plan_result(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    execute_write_logical_plan_with_mode_inner(ctx, plan, params, WriteExecutorModeInner::Auto)
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
        match super::bound_public_write::try_execute_bound_public_write(ctx, &write_plan, params)
            .await
            .map_err(normalize_bound_public_write_error)?
        {
            super::bound_public_write::BoundPublicWriteExecution::Executed(result) => {
                return Ok((result, WriteExecutorPath::Fast));
            }
            super::bound_public_write::BoundPublicWriteExecution::Unsupported => {}
        }
    }

    if mode != WriteExecutorModeInner::ForceDataFusion {
        let fast_plan = crate::sql2::optimize::simple_write::try_make_fast_write_plan(&write_plan)?;
        if requires_standalone_datafusion_validation(&write_plan, fast_plan.is_some(), mode) {
            super::datafusion::validate_datafusion_write_logical_plan(ctx, &write_plan, params)
                .await?;
        }
        if let Some(fast_plan) = fast_plan {
            let rows_affected =
                crate::sql2::exec::fast_write::try_execute_simple_write(ctx, fast_plan, params)
                    .await?;
            return Ok((
                SqlWriteResult::affected(rows_affected),
                WriteExecutorPath::Fast,
            ));
        }
        if mode == WriteExecutorModeInner::ForceFast {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "SQL write plan is not eligible for fast execution",
            ));
        }
    }

    let result =
        super::datafusion::execute_datafusion_write_logical_plan(ctx, &write_plan, params).await?;
    Ok((result, WriteExecutorPath::DataFusion))
}

/// Fast executors rely on the DataFusion writer to preserve validation
/// behavior that is outside their deliberately narrow implementations.
///
/// A regular DataFusion fallback does not need this separate pass: its
/// executor performs the same input, session, provider, expression, and
/// filter validation immediately before execution. Empty scopes and upserts
/// retain the standalone pass because their execution paths can return early.
fn requires_standalone_datafusion_validation(
    plan: &LogicalWritePlan,
    has_fast_plan: bool,
    mode: WriteExecutorModeInner,
) -> bool {
    has_fast_plan
        || mode == WriteExecutorModeInner::ForceFast
        || plan.bound.branch_scope == BranchScope::Empty
        || plan.bound.conflict.is_some()
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
    normalize_lix_state_by_branch_scope(&mut plan, params)?;
    Ok(plan)
}

fn branch_column_for_target(target: &BoundWriteTarget) -> Option<&'static str> {
    match target {
        BoundWriteTarget::LixStateByBranch => Some("branch_id"),
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

fn normalize_lix_state_by_branch_scope(
    plan: &mut LogicalWritePlan,
    params: &[Value],
) -> Result<(), LixError> {
    if !matches!(plan.bound.target, BoundWriteTarget::LixStateByBranch) {
        return Ok(());
    }
    let (BranchScope::Explicit { branch_ids } | BranchScope::ExplicitRequired { branch_ids }) =
        &plan.bound.branch_scope
    else {
        return Ok(());
    };
    let explicit_global = explicit_lix_state_global_value(&plan.bound.input, params)?.or(
        predicate_lix_state_global_value(&plan.bound.predicate, params)?,
    );
    if branch_ids.len() > 1 {
        if explicit_global == Some(true) || branch_ids.contains(GLOBAL_BRANCH_ID) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "lix_state_by_branch writes cannot mix global and branch-specific rows",
            ));
        }
        return Ok(());
    }
    let is_global_branch = branch_ids.contains(GLOBAL_BRANCH_ID);
    if explicit_global == Some(true) && !is_global_branch {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "lix_state_by_branch writes cannot combine global = true with non-global branch_id",
        ));
    }
    if !is_global_branch {
        return Ok(());
    }
    match explicit_global {
        Some(false) => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "lix_state_by_branch writes cannot combine global = false with global branch_id",
        )),
        Some(true) | None => {
            plan.bound.branch_scope = BranchScope::Global;
            Ok(())
        }
    }
}

fn explicit_lix_state_global_value(
    input: &BoundWriteInput,
    params: &[Value],
) -> Result<Option<bool>, LixError> {
    let BoundWriteInput::Values(values) = input else {
        return Ok(None);
    };
    let Some(global_index) = values.column_index("global") else {
        return Ok(None);
    };
    let mut explicit = None;
    for row in &values.rows {
        let value = match &row[global_index] {
            BoundExpr::Literal(BoundLiteral::Bool(value)) => *value,
            BoundExpr::Literal(BoundLiteral::Null) => continue,
            BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
                Some(Value::Boolean(value)) => *value,
                Some(_) => {
                    return Err(LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        "lix_state_by_branch global selectors must be boolean parameters",
                    ));
                }
                None => {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("missing SQL parameter ${}", param.index),
                    ));
                }
            },
            _ => {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "lix_state_by_branch global selectors must be static booleans",
                ));
            }
        };
        if explicit.is_some_and(|prior| prior != value) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "lix_state_by_branch writes cannot mix global and branch-specific rows",
            ));
        }
        explicit = Some(value);
    }
    Ok(explicit)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolvedGlobalSelector {
    Missing,
    Empty,
    Static(bool),
    Mixed,
}

impl ResolvedGlobalSelector {
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Mixed, _) | (_, Self::Mixed) => Self::Mixed,
            (Self::Missing | Self::Empty, selector) | (selector, Self::Missing | Self::Empty) => {
                selector
            }
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Mixed,
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (Self::Missing | Self::Mixed, selector) | (selector, Self::Missing | Self::Mixed) => {
                selector
            }
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Empty,
        }
    }
}

fn predicate_lix_state_global_value(
    predicate: &BoundPredicate,
    params: &[Value],
) -> Result<Option<bool>, LixError> {
    match resolved_predicate_global_selector(predicate, params)? {
        ResolvedGlobalSelector::Static(value) => Ok(Some(value)),
        ResolvedGlobalSelector::Mixed => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "lix_state_by_branch writes cannot mix global and branch-specific rows",
        )),
        ResolvedGlobalSelector::Missing | ResolvedGlobalSelector::Empty => Ok(None),
    }
}

fn resolved_predicate_global_selector(
    predicate: &BoundPredicate,
    params: &[Value],
) -> Result<ResolvedGlobalSelector, LixError> {
    match predicate {
        BoundPredicate::True => Ok(ResolvedGlobalSelector::Missing),
        BoundPredicate::False => Ok(ResolvedGlobalSelector::Empty),
        BoundPredicate::And(predicates) => {
            let mut result = ResolvedGlobalSelector::Missing;
            for predicate in predicates {
                result = result.intersect(resolved_predicate_global_selector(predicate, params)?);
            }
            Ok(result)
        }
        BoundPredicate::Or(predicates) => {
            let mut result = ResolvedGlobalSelector::Empty;
            let mut has_missing_branch = false;
            for predicate in predicates {
                let selector = resolved_predicate_global_selector(predicate, params)?;
                if selector == ResolvedGlobalSelector::Missing {
                    has_missing_branch = true;
                    continue;
                }
                result = result.union(selector);
            }
            if has_missing_branch {
                if result == ResolvedGlobalSelector::Empty {
                    Ok(ResolvedGlobalSelector::Missing)
                } else {
                    Ok(ResolvedGlobalSelector::Mixed)
                }
            } else {
                Ok(result)
            }
        }
        BoundPredicate::Eq(left, right) => global_value_from_binary_exprs(left, right)
            .or_else(|| global_value_from_binary_exprs(right, left))
            .map(|expr| global_selector_value(expr, params))
            .transpose()
            .map(|selector| selector.unwrap_or(ResolvedGlobalSelector::Missing)),
        BoundPredicate::Like { .. } | BoundPredicate::IsNull(_) | BoundPredicate::IsNotNull(_) => {
            Ok(ResolvedGlobalSelector::Missing)
        }
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(ResolvedGlobalSelector::Missing);
            };
            if column.name != "global" {
                return Ok(ResolvedGlobalSelector::Missing);
            }
            let mut result = ResolvedGlobalSelector::Missing;
            for value in values {
                result = result.union(global_selector_value(value, params)?);
            }
            Ok(result)
        }
    }
}

fn global_value_from_binary_exprs<'a>(
    column_expr: &BoundExpr,
    value_expr: &'a BoundExpr,
) -> Option<&'a BoundExpr> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "global" {
        return None;
    }
    Some(value_expr)
}

fn global_selector_value(
    expr: &BoundExpr,
    params: &[Value],
) -> Result<ResolvedGlobalSelector, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Bool(value)) => Ok(ResolvedGlobalSelector::Static(*value)),
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Boolean(value)) => Ok(ResolvedGlobalSelector::Static(*value)),
            Some(Value::Null) => Ok(ResolvedGlobalSelector::Missing),
            Some(_) => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "lix_state global predicates require boolean parameters",
            )),
            None => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("missing SQL parameter ${}", param.index),
            )),
        },
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "lix_state global predicates require boolean literals",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_datafusion_fallback_uses_execution_validation() {
        let plan = plan_sql("UPDATE lix_file SET data = $1 WHERE id = $2 AND data = $3");
        let has_fast_plan = crate::sql2::optimize::simple_write::try_make_fast_write_plan(&plan)
            .expect("fast-plan eligibility should be computable")
            .is_some();
        assert!(!has_fast_plan);

        assert!(!requires_standalone_datafusion_validation(
            &plan,
            has_fast_plan,
            WriteExecutorModeInner::Auto,
        ));
    }

    #[test]
    fn early_or_fast_paths_keep_standalone_datafusion_validation() {
        let fallback = plan_sql("UPDATE lix_file SET data = $1 WHERE id = $2 AND data = $3");
        assert!(requires_standalone_datafusion_validation(
            &fallback,
            false,
            WriteExecutorModeInner::ForceFast,
        ));

        let empty_scope = plan_sql(
            "UPDATE lix_state_by_branch SET metadata = '{}' \
             WHERE branch_id = 'branch-a' AND branch_id = 'branch-b'",
        );
        assert_eq!(empty_scope.bound.branch_scope, BranchScope::Empty);
        assert!(requires_standalone_datafusion_validation(
            &empty_scope,
            false,
            WriteExecutorModeInner::Auto,
        ));

        let conflict = plan_sql(
            "INSERT INTO lix_file (path, data) VALUES ('/readme.md', X'41') \
             ON CONFLICT (path) DO NOTHING",
        );
        assert!(conflict.bound.conflict.is_some());
        assert!(requires_standalone_datafusion_validation(
            &conflict,
            false,
            WriteExecutorModeInner::Auto,
        ));

        let fast = plan_sql("DELETE FROM lix_state WHERE false");
        let has_fast_plan = crate::sql2::optimize::simple_write::try_make_fast_write_plan(&fast)
            .expect("fast-plan eligibility should be computable")
            .is_some();
        assert!(has_fast_plan);
        assert!(requires_standalone_datafusion_validation(
            &fast,
            has_fast_plan,
            WriteExecutorModeInner::Auto,
        ));
    }

    fn plan_sql(sql: &str) -> LogicalWritePlan {
        let statement = crate::sql2::parse_statement(sql).expect("SQL should parse");
        let bound =
            crate::sql2::bind_statement(&statement, &[], "branch-a").expect("SQL should bind");
        crate::sql2::plan_write(bound).expect("SQL write should plan")
    }
}
