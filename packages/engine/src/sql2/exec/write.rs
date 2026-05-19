//! Write execution for bound sql2 plans.

use std::collections::BTreeSet;

use serde_json::json;

use datafusion::sql::parser::Statement as DataFusionStatement;

use super::SqlLogicalPlan;
use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{BoundWriteInput, BoundWriteTarget};
use crate::sql2::parse::parse_statement;
use crate::sql2::plan::predicate::BoundPredicate;
use crate::sql2::plan::version_scope::VersionScope;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::SqlWriteExecutionContext;
use crate::{LixError, Value, GLOBAL_VERSION_ID};

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
        } => match version_column_for_target(&plan.bound.target) {
            Some(version_column) => {
                match resolved_predicate_version_selector(
                    &plan.bound.predicate,
                    version_column,
                    params,
                )? {
                    ResolvedVersionSelector::Static(version_ids) if version_ids.is_empty() => {
                        VersionScope::Empty
                    }
                    ResolvedVersionSelector::Static(version_ids) => {
                        VersionScope::ExplicitRequired { version_ids }
                    }
                    ResolvedVersionSelector::Missing => {
                        insert_version_param_values(&mut version_ids, &param_indexes, params)?;
                        if version_ids.is_empty() {
                            VersionScope::Empty
                        } else {
                            VersionScope::ExplicitRequired { version_ids }
                        }
                    }
                }
            }
            None => {
                insert_version_param_values(&mut version_ids, &param_indexes, params)?;
                if version_ids.is_empty() {
                    VersionScope::Empty
                } else {
                    VersionScope::ExplicitRequired { version_ids }
                }
            }
        },
        scope => scope,
    };
    normalize_lix_state_by_version_scope(&mut plan, params)?;
    Ok(plan)
}

fn version_column_for_target(target: &BoundWriteTarget) -> Option<&'static str> {
    match target {
        BoundWriteTarget::LixStateByVersion => Some("version_id"),
        BoundWriteTarget::Entity(crate::sql2::bind::write::EntityWriteSurface::ByVersion {
            ..
        })
        | BoundWriteTarget::File(crate::sql2::bind::write::FileWriteSurface::ByVersion)
        | BoundWriteTarget::Directory(crate::sql2::bind::write::DirectoryWriteSurface::ByVersion) => {
            Some("lixcol_version_id")
        }
        _ => None,
    }
}

fn normalize_lix_state_by_version_scope(
    plan: &mut LogicalWritePlan,
    params: &[Value],
) -> Result<(), LixError> {
    if !matches!(plan.bound.target, BoundWriteTarget::LixStateByVersion) {
        return Ok(());
    }
    let version_ids = match &plan.bound.version_scope {
        VersionScope::Explicit { version_ids } | VersionScope::ExplicitRequired { version_ids } => {
            version_ids
        }
        _ => return Ok(()),
    };
    let explicit_global = explicit_lix_state_global_value(&plan.bound.input, params)?.or(
        predicate_lix_state_global_value(&plan.bound.predicate, params)?,
    );
    if version_ids.len() > 1 {
        if explicit_global == Some(true) || version_ids.contains(GLOBAL_VERSION_ID) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "lix_state_by_version writes cannot mix global and version-specific rows",
            ));
        }
        return Ok(());
    }
    let is_global_version = version_ids.contains(GLOBAL_VERSION_ID);
    if explicit_global == Some(true) && !is_global_version {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "lix_state_by_version writes cannot combine global = true with non-global version_id",
        ));
    }
    if !is_global_version {
        return Ok(());
    }
    match explicit_global {
        Some(false) => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "lix_state_by_version writes cannot combine global = false with global version_id",
        )),
        Some(true) | None => {
            plan.bound.version_scope = VersionScope::Global;
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
                        "lix_state_by_version global selectors must be boolean parameters",
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
                    "lix_state_by_version global selectors must be static booleans",
                ));
            }
        };
        if explicit.is_some_and(|prior| prior != value) {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "lix_state_by_version writes cannot mix global and version-specific rows",
            ));
        }
        explicit = Some(value);
    }
    Ok(explicit)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedVersionSelector {
    Missing,
    Static(BTreeSet<String>),
}

impl ResolvedVersionSelector {
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

fn resolved_predicate_version_selector(
    predicate: &BoundPredicate,
    version_column: &str,
    params: &[Value],
) -> Result<ResolvedVersionSelector, LixError> {
    match predicate {
        BoundPredicate::True => Ok(ResolvedVersionSelector::Missing),
        BoundPredicate::False => Ok(ResolvedVersionSelector::Static(BTreeSet::new())),
        BoundPredicate::And(predicates) => {
            let mut result = ResolvedVersionSelector::Missing;
            for predicate in predicates {
                result = result.intersect(resolved_predicate_version_selector(
                    predicate,
                    version_column,
                    params,
                )?);
            }
            Ok(result)
        }
        BoundPredicate::Or(predicates) => {
            let mut result = ResolvedVersionSelector::Static(BTreeSet::new());
            for predicate in predicates {
                result = result.union(resolved_predicate_version_selector(
                    predicate,
                    version_column,
                    params,
                )?);
            }
            Ok(result)
        }
        BoundPredicate::Eq(left, right) => {
            resolved_version_selector_from_binary_exprs(left, right, version_column, params)
                .or_else(|| {
                    resolved_version_selector_from_binary_exprs(right, left, version_column, params)
                })
                .transpose()
                .map(|selector| selector.unwrap_or(ResolvedVersionSelector::Missing))
        }
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(ResolvedVersionSelector::Missing);
            };
            if column.name != version_column {
                return Ok(ResolvedVersionSelector::Missing);
            }
            let mut result = ResolvedVersionSelector::Static(BTreeSet::new());
            for value in values {
                result = result.union(resolved_value_version_selector(value, params)?);
            }
            Ok(result)
        }
    }
}

fn resolved_version_selector_from_binary_exprs(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
    version_column: &str,
    params: &[Value],
) -> Option<Result<ResolvedVersionSelector, LixError>> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name != version_column {
        return None;
    }
    Some(resolved_value_version_selector(value_expr, params))
}

fn resolved_value_version_selector(
    expr: &BoundExpr,
    params: &[Value],
) -> Result<ResolvedVersionSelector, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(version_id)) => {
            Ok(ResolvedVersionSelector::Static(BTreeSet::from([
                version_id.clone(),
            ])))
        }
        BoundExpr::Literal(BoundLiteral::Null) => {
            Ok(ResolvedVersionSelector::Static(BTreeSet::new()))
        }
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Text(version_id)) => Ok(ResolvedVersionSelector::Static(BTreeSet::from([
                version_id.clone(),
            ]))),
            Some(Value::Null) => Ok(ResolvedVersionSelector::Static(BTreeSet::new())),
            Some(_) => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "by-version SQL write selectors require text version-id parameters",
            )),
            None => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "SQL version selector parameter ${} was not provided",
                    param.index
                ),
            )),
        },
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "by-version SQL write predicates require string version ids",
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
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Empty, selector) | (selector, Self::Empty) => selector,
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Mixed,
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Mixed, selector) | (selector, Self::Mixed) => selector,
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
            "lix_state_by_version writes cannot mix global and version-specific rows",
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
            Some(Value::Null) => {}
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
