//! Write execution for bound sql2 plans.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::json;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::sql::parser::Statement as DataFusionStatement;

use super::{SqlLogicalPlan, SqlWriteResult};
use crate::PreparedDmlParameterBatch;
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

pub(crate) fn diff_command_query(
    plan: &SqlLogicalPlan,
) -> Option<(
    crate::sql2::DiffCommand,
    String,
    Option<crate::sql2::bind::write::BoundReturning>,
)> {
    let SqlLogicalPlan::Write(write) = plan else {
        return None;
    };
    let BoundWriteTarget::DiffCommand(command) = write.plan.bound.target else {
        return None;
    };
    let crate::sql2::bind::write::BoundWriteInput::Query { query, .. } = &write.plan.bound.input
    else {
        return None;
    };
    Some((
        command,
        query.query.to_string(),
        write.plan.bound.returning.clone(),
    ))
}

/// Returns whether an explicit transaction needs a statement checkpoint
/// before executing this `RETURNING` write. Generic providers construct their
/// result from a staged postimage. Direct row writes are the one fast path
/// that can safely evaluate ordinary visible columns before staging.
pub(crate) fn write_plan_requires_post_stage_returning_checkpoint(plan: &SqlLogicalPlan) -> bool {
    let SqlLogicalPlan::Write(write) = plan else {
        return false;
    };
    write.plan.bound.returning.is_some()
        && !super::bound_public_write::row_returning_projects_before_stage(&write.plan)
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

/// Transposes row-oriented public `executeBatch` parameters into the
/// column-oriented carrier used by the physical batch executor.
///
/// A mixed-type column is deliberately not coerced: its statements retain the
/// ordinary sequential path and therefore the exact per-statement type errors.
pub(crate) fn parameter_record_batch(rows: &[&[Value]]) -> Result<Option<RecordBatch>, LixError> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    if rows.iter().any(|row| row.len() != first.len()) {
        return Ok(None);
    }
    if first.is_empty() {
        // An empty-schema RecordBatch has no array from which Arrow can infer
        // row cardinality. Keep parameterless statements on sequential
        // execution rather than silently turning a non-empty batch into zero
        // rows.
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(first.len());
    let mut columns = Vec::with_capacity(first.len());
    for column_index in 0..first.len() {
        let Some(kind) = rows
            .iter()
            .find_map(|row| ParameterKind::from_value(&row[column_index]))
        else {
            // Arrow's untyped Null column cannot retain the SQL parameter's
            // eventual type. Keep an all-null column on sequential execution.
            return Ok(None);
        };
        if rows.iter().any(|row| {
            ParameterKind::from_value(&row[column_index]).is_some_and(|candidate| candidate != kind)
        }) {
            return Ok(None);
        }
        let scalars = rows
            .iter()
            .map(|row| kind.scalar(&row[column_index]))
            .collect::<Result<Vec<_>, _>>()?;
        let array = ScalarValue::iter_to_array(scalars).map_err(|error| {
            LixError::unknown(format!("failed to lower SQL parameter column: {error}"))
        })?;
        let field = Field::new(
            format!("${}", column_index + 1),
            kind.data_type(),
            rows.iter()
                .any(|row| matches!(row[column_index], Value::Null)),
        );
        fields.push(if kind == ParameterKind::Json {
            crate::sql2::result_metadata::mark_json_field(field)
        } else {
            field
        });
        columns.push(array);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map(Some)
        .map_err(|error| {
            LixError::unknown(format!("failed to construct SQL parameter batch: {error}"))
        })
}

pub(crate) fn parameter_row(batch: &RecordBatch, row_index: usize) -> Result<Vec<Value>, LixError> {
    if row_index >= batch.num_rows() {
        return Err(LixError::unknown(format!(
            "SQL parameter row {row_index} is outside a {} row batch",
            batch.num_rows()
        )));
    }
    batch
        .columns()
        .iter()
        .enumerate()
        .map(|(column_index, array)| {
            let scalar = ScalarValue::try_from_array(array, row_index).map_err(|error| {
                LixError::unknown(format!(
                    "failed to read SQL parameter ${} from Arrow batch: {error}",
                    column_index + 1
                ))
            })?;
            scalar_parameter_value(
                scalar,
                crate::sql2::result_metadata::field_is_json(batch.schema().field(column_index)),
            )
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParameterKind {
    Boolean,
    Integer,
    Real,
    Text,
    Json,
    Timestamp,
    Blob,
}

impl ParameterKind {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => None,
            Value::Boolean(_) => Some(Self::Boolean),
            Value::Integer(_) => Some(Self::Integer),
            Value::Real(_) => Some(Self::Real),
            Value::Text(_) => Some(Self::Text),
            Value::Json(_) => Some(Self::Json),
            Value::Timestamp(_) => Some(Self::Timestamp),
            Value::Blob(_) => Some(Self::Blob),
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Integer => DataType::Int64,
            Self::Real => DataType::Float64,
            Self::Text | Self::Json => DataType::Utf8,
            Self::Timestamp => DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            Self::Blob => DataType::LargeBinary,
        }
    }

    fn scalar(self, value: &Value) -> Result<ScalarValue, LixError> {
        match (self, value) {
            (Self::Boolean, Value::Boolean(value)) => Ok(ScalarValue::Boolean(Some(*value))),
            (Self::Integer, Value::Integer(value)) => Ok(ScalarValue::Int64(Some(*value))),
            (Self::Real, Value::Real(value)) => Ok(ScalarValue::Float64(Some(*value))),
            (Self::Text, Value::Text(value)) => Ok(ScalarValue::Utf8(Some(value.clone()))),
            (Self::Json, Value::Json(value)) => Ok(ScalarValue::Utf8(Some(value.to_string()))),
            (Self::Timestamp, Value::Timestamp(value)) => Ok(
                ScalarValue::TimestampMicrosecond(Some(*value), Some("UTC".into())),
            ),
            (Self::Blob, Value::Blob(value)) => Ok(ScalarValue::LargeBinary(Some(value.to_vec()))),
            (Self::Boolean, Value::Null) => Ok(ScalarValue::Boolean(None)),
            (Self::Integer, Value::Null) => Ok(ScalarValue::Int64(None)),
            (Self::Real, Value::Null) => Ok(ScalarValue::Float64(None)),
            (Self::Text | Self::Json, Value::Null) => Ok(ScalarValue::Utf8(None)),
            (Self::Timestamp, Value::Null) => Ok(ScalarValue::TimestampMicrosecond(
                None,
                Some("UTC".into()),
            )),
            (Self::Blob, Value::Null) => Ok(ScalarValue::LargeBinary(None)),
            _ => Err(LixError::unknown(
                "heterogeneous SQL parameter column reached Arrow lowering",
            )),
        }
    }
}

fn scalar_parameter_value(scalar: ScalarValue, is_json: bool) -> Result<Value, LixError> {
    match scalar {
        ScalarValue::Boolean(Some(value)) => Ok(Value::Boolean(value)),
        ScalarValue::Int64(Some(value)) => Ok(Value::Integer(value)),
        ScalarValue::Float64(Some(value)) => Ok(Value::Real(value)),
        ScalarValue::TimestampMicrosecond(Some(value), _) => Ok(Value::Timestamp(value)),
        ScalarValue::Utf8(Some(value)) if is_json => serde_json::from_str(&value)
            .map(Value::Json)
            .map_err(|error| {
                LixError::unknown(format!(
                    "invalid JSON value in SQL parameter batch: {error}"
                ))
            }),
        ScalarValue::Utf8(Some(value)) => Ok(Value::Text(value)),
        ScalarValue::LargeUtf8(Some(value)) if is_json => serde_json::from_str(&value)
            .map(Value::Json)
            .map_err(|error| {
                LixError::unknown(format!(
                    "invalid JSON value in SQL parameter batch: {error}"
                ))
            }),
        ScalarValue::LargeUtf8(Some(value)) => Ok(Value::Text(value)),
        ScalarValue::LargeBinary(Some(value)) => Ok(Value::Blob(value.into())),
        ScalarValue::Boolean(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::Null => Ok(Value::Null),
        value => Err(LixError::unknown(format!(
            "unsupported Arrow SQL parameter value {value:?}"
        ))),
    }
}

/// Attempts a certified physical parameter-batch route.
///
/// `None` means the logical statements are not independent and must retain
/// public `executeBatch`'s sequential execution semantics.
pub(crate) async fn execute_write_logical_plan_parameter_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: SqlLogicalPlan,
    parameter_batch: &RecordBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let SqlLogicalPlan::Write(write_plan) = plan else {
        return Ok(None);
    };
    validate_write_parameter_count(&write_plan.plan, parameter_batch.num_columns())?;
    if let Some(results) = super::bound_public_write::try_execute_row_insert_parameter_batch(
        ctx,
        &write_plan.plan,
        parameter_batch,
    )
    .await
    .map_err(normalize_bound_public_write_error)?
    {
        return Ok(Some(results));
    }
    super::bound_public_write::try_execute_row_update_parameter_batch(
        ctx,
        &write_plan.plan,
        parameter_batch,
    )
    .await
    .map_err(normalize_bound_public_write_error)
}

pub(crate) async fn execute_write_logical_plan_prepared_dml_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &SqlLogicalPlan,
    parameter_batch: &PreparedDmlParameterBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let SqlLogicalPlan::Write(write_plan) = plan else {
        return Ok(None);
    };
    validate_write_parameter_count(&write_plan.plan, parameter_batch.column_count())?;
    if let Some(results) = super::bound_public_write::try_execute_file_prepared_batch(
        ctx,
        &write_plan.plan,
        parameter_batch,
    )
    .await
    .map_err(normalize_bound_public_write_error)?
    {
        PreparedDmlParameterBatch::record_execution(parameter_batch.row_count());
        return Ok(Some(results));
    }
    if let Some(results) = super::bound_public_write::try_execute_row_insert_prepared_batch(
        ctx,
        &write_plan.plan,
        parameter_batch,
    )
    .await
    .map_err(normalize_bound_public_write_error)?
    {
        PreparedDmlParameterBatch::record_execution(parameter_batch.row_count());
        return Ok(Some(results));
    }
    let results = super::bound_public_write::try_execute_row_update_prepared_batch(
        ctx,
        &write_plan.plan,
        parameter_batch,
    )
    .await
    .map_err(normalize_bound_public_write_error)?;
    if results.is_some() {
        PreparedDmlParameterBatch::record_execution(parameter_batch.row_count());
    }
    Ok(results)
}

pub(crate) async fn execute_write_logical_plan_value_batch<'a>(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &SqlLogicalPlan,
    parameter_rows: &'a [&'a [Value]],
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let SqlLogicalPlan::Write(write_plan) = plan else {
        return Ok(None);
    };
    let Some(first) = parameter_rows.first() else {
        return Ok(None);
    };
    if parameter_rows.iter().any(|row| row.len() != first.len()) {
        return Ok(None);
    }
    validate_write_parameter_count(&write_plan.plan, first.len())?;
    if let Some(results) = super::bound_public_write::try_execute_row_insert_value_batch(
        ctx,
        &write_plan.plan,
        parameter_rows,
    )
    .await
    .map_err(normalize_bound_public_write_error)?
    {
        return Ok(Some(results));
    }
    super::bound_public_write::try_execute_row_update_value_batch(
        ctx,
        &write_plan.plan,
        parameter_rows,
    )
    .await
    .map_err(normalize_bound_public_write_error)
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
        BoundWriteTarget::Row(crate::sql2::bind::write::RowWriteSurface::ByBranch {
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
