use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::metadata::{FieldMetadata, ScalarAndMetadata};
use datafusion::common::{Column, DFSchema, ParamValues, ScalarValue};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::{BinaryExpr, InList, ScalarFunction};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{BoundInsertValues, FileWriteSurface};
use crate::sql2::bind::write::{
    BoundWriteInput, BoundWriteOp, BoundWriteTarget, DirectoryWriteSurface,
};
use crate::sql2::parse::parse_statement;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;
use crate::sql2::plan::LogicalWritePlan;
use crate::{LixError, LixNotice, SqlQueryResult, Value, GLOBAL_BRANCH_ID};

use crate::sql2::predicate_typecheck::{
    json_predicate_placeholder_indexes_with_dfschema, validate_json_predicate_expr_with_dfschema,
};
use crate::sql2::result_metadata::{
    field_is_json, LIX_VALUE_TYPE_JSON, LIX_VALUE_TYPE_METADATA_KEY,
};
use crate::sql2::session::{
    build_read_session, build_transaction_read_session, build_write_session_with_options,
    SqlWriteSessionOptions,
};
use crate::sql2::write_normalization::lix_file_data_type_lix_error;
use crate::sql2::{SqlExecutionContext, SqlWriteExecutionContext};

use super::{SqlDataFusionLogicalPlan, SqlLogicalPlan};

pub(crate) const LIX_INSERT_COLUMN_OMITTED_METADATA_KEY: &str = "lix_insert_column_omitted";

pub(crate) struct DataFusionLogicalPlan {
    pub(super) session: SessionContext,
    pub(super) plan: LogicalPlan,
    pub(super) notices: Vec<LixNotice>,
    pub(super) json_predicate_params: BTreeSet<usize>,
}

/// Minimal top-level sql2 entrypoint.
///
/// The final implementation will build the DataFusion session from the
/// execution context and source rows from `live_state()`.
///
/// `catalog()` is intentionally omitted from the MVP boundary for now.
#[allow(dead_code)]
pub(crate) async fn execute_sql<C>(
    ctx: &C,
    sql: &str,
    params: &[Value],
) -> Result<SqlQueryResult, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let plan = create_logical_plan(ctx, sql).await?;
    execute_logical_plan(plan, params).await
}

pub(crate) async fn create_logical_plan<C>(ctx: &C, sql: &str) -> Result<SqlLogicalPlan, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let statement = parse_statement(sql)?;
    create_logical_plan_from_parsed(ctx, sql, statement).await
}

pub(crate) async fn create_logical_plan_from_parsed<C>(
    ctx: &C,
    sql: &str,
    statement: DataFusionStatement,
) -> Result<SqlLogicalPlan, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    crate::sql2::bind_read_statement(sql, &statement)?;
    let session = build_read_session(ctx).await?;
    let plan = create_logical_plan_from_statement(&session, statement).await?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&plan);
    let notices = history_filter_notices(&plan);

    Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
        session,
        plan,
        notices,
        json_predicate_params,
    }))
}

pub(crate) async fn create_transaction_read_logical_plan_from_parsed(
    read_ctx: &impl SqlExecutionContext,
    write_ctx: &mut dyn SqlWriteExecutionContext,
    sql: &str,
    statement: DataFusionStatement,
) -> Result<SqlLogicalPlan, LixError> {
    crate::sql2::bind_read_statement(sql, &statement)?;
    let session = build_transaction_read_session(read_ctx, write_ctx).await?;
    let plan = create_logical_plan_from_statement(&session, statement).await?;
    validate_supported_logical_plan(&plan)?;
    validate_json_predicates_in_logical_plan(&plan)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&plan);
    let notices = history_filter_notices(&plan);

    Ok(SqlLogicalPlan::DataFusion(SqlDataFusionLogicalPlan {
        session,
        plan,
        notices,
        json_predicate_params,
    }))
}

async fn create_logical_plan_from_statement(
    session: &SessionContext,
    statement: DataFusionStatement,
) -> Result<LogicalPlan, LixError> {
    session
        .state()
        .statement_to_plan(statement)
        .await
        .map_err(datafusion_error_to_lix_error)
}

fn validate_json_predicates_in_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    for expr in plan.expressions() {
        validate_json_predicate_expr_with_dfschema(plan.schema(), &expr)?;
    }
    match plan {
        LogicalPlan::Filter(filter) => {
            validate_json_predicate_expr_with_dfschema(filter.input.schema(), &filter.predicate)?;
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                validate_json_predicate_expr_with_dfschema(scan.projected_schema.as_ref(), filter)?;
            }
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_json_predicates_in_logical_plan(input)?;
    }

    Ok(())
}

fn json_predicate_params_in_logical_plan(plan: &LogicalPlan) -> BTreeSet<usize> {
    let mut params = BTreeSet::new();
    for expr in plan.expressions() {
        params.extend(json_predicate_placeholder_indexes_with_dfschema(
            plan.schema(),
            &expr,
        ));
    }
    match plan {
        LogicalPlan::Filter(filter) => {
            params.extend(json_predicate_placeholder_indexes_with_dfschema(
                filter.input.schema(),
                &filter.predicate,
            ));
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                params.extend(json_predicate_placeholder_indexes_with_dfschema(
                    scan.projected_schema.as_ref(),
                    filter,
                ));
            }
        }
        _ => {}
    }

    for input in plan.inputs() {
        params.extend(json_predicate_params_in_logical_plan(input));
    }
    params
}

pub(crate) async fn execute_logical_plan(
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<SqlQueryResult, LixError> {
    let SqlLogicalPlan::DataFusion(plan) = plan else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 bound write execution is not wired yet",
        ));
    };
    let SqlDataFusionLogicalPlan {
        session,
        plan,
        notices,
        json_predicate_params,
    } = plan;
    validate_parameter_count(&plan, params.len())?;
    validate_json_predicate_params(&json_predicate_params, params)?;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }

    let result_fields = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let batches = crate::sql2::runtime::collect_dataframe(dataframe)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let mut result = query_result_from_batches(&result_fields, &batches)?;
    result.notices = notices;
    Ok(result)
}

pub(crate) async fn execute_datafusion_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<u64, LixError> {
    validate_bound_write_input(plan, params)?;
    let session = build_write_session_with_options(ctx, write_session_options(plan)).await?;
    let table_name = write_target_table_name(plan)?;
    let table = session
        .table_provider(table_name)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let table_schema = table.schema();
    let state = session.state();

    let exec = match plan.bound.op {
        BoundWriteOp::Insert => {
            let input =
                insert_input_plan(&session, std::sync::Arc::clone(&table_schema), plan, params)
                    .await?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return Ok(0);
            }
            table.insert_into(&state, input, InsertOp::Append).await
        }
        BoundWriteOp::Update => {
            let assignments =
                datafusion_assignments(&session, table_schema.as_ref(), plan, params)?;
            let filters = datafusion_write_filters(&session, table_schema.as_ref(), plan, params)?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return Ok(0);
            }
            table.update(&state, assignments, filters).await
        }
        BoundWriteOp::Delete => {
            let filters = datafusion_write_filters(&session, table_schema.as_ref(), plan, params)?;
            if plan.bound.branch_scope == BranchScope::Empty {
                return Ok(0);
            }
            table.delete_from(&state, filters).await
        }
    }
    .map_err(datafusion_error_to_lix_error)?;

    let batches = crate::sql2::runtime::collect_input_plan(exec, session.task_ctx())
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let result =
        query_result_from_batches(&[Field::new("count", DataType::UInt64, false)], &batches)?;

    affected_rows_from_query_result(result)
}

pub(crate) async fn validate_datafusion_write_logical_plan(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<(), LixError> {
    validate_bound_write_input(plan, params)?;
    let session = build_write_session_with_options(ctx, write_session_options(plan)).await?;
    let table_name = write_target_table_name(plan)?;
    let table = session
        .table_provider(table_name)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let table_schema = table.schema();
    let state = session.state();

    match plan.bound.op {
        BoundWriteOp::Insert => {
            let input = insert_input_plan(&session, table_schema, plan, params).await?;
            let _ = table
                .insert_into(&state, input, InsertOp::Append)
                .await
                .map_err(datafusion_error_to_lix_error)?;
        }
        BoundWriteOp::Update => {
            let assignments =
                datafusion_assignments(&session, table_schema.as_ref(), plan, params)?;
            let filters = datafusion_write_filters(&session, table_schema.as_ref(), plan, params)?;
            let _ = table
                .update(&state, assignments, filters)
                .await
                .map_err(datafusion_error_to_lix_error)?;
        }
        BoundWriteOp::Delete => {
            let filters = datafusion_write_filters(&session, table_schema.as_ref(), plan, params)?;
            let _ = table
                .delete_from(&state, filters)
                .await
                .map_err(datafusion_error_to_lix_error)?;
        }
    }

    Ok(())
}

async fn insert_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    match &plan.bound.input {
        BoundWriteInput::Values(values) => {
            insert_values_input_plan(session, schema, plan, params, values).await
        }
        BoundWriteInput::Query { query, columns } => {
            insert_query_input_plan(session, schema, query, columns, params).await
        }
        BoundWriteInput::None => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "INSERT source is required",
        )),
    }
}

async fn insert_values_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    plan: &LogicalWritePlan,
    params: &[Value],
    values: &BoundInsertValues,
) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    if values.rows.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 DataFusion reference writer cannot execute empty INSERT",
        ));
    }
    let nullable_schema = std::sync::Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let df_schema = std::sync::Arc::new(
        DFSchema::try_from(nullable_schema).map_err(datafusion_error_to_lix_error)?,
    );
    let field_source_indexes = schema
        .fields()
        .iter()
        .map(|field| values.column_index(field.name()))
        .collect::<Vec<_>>();
    let rows = values
        .rows
        .iter()
        .map(|row| {
            schema
                .fields()
                .iter()
                .zip(field_source_indexes.iter())
                .map(|(field, source_index)| {
                    insert_field_expr(
                        session,
                        row,
                        *source_index,
                        field.name(),
                        field.data_type(),
                        plan,
                        params,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let projection = schema
        .fields()
        .iter()
        .zip(field_source_indexes.iter())
        .enumerate()
        .map(|(index, (field, source_index))| {
            let metadata = if source_index.is_none() {
                Some(FieldMetadata::new(BTreeMap::from([(
                    LIX_INSERT_COLUMN_OMITTED_METADATA_KEY.to_string(),
                    "true".to_string(),
                )])))
            } else {
                None
            };
            Expr::Column(Column::from_name(format!("column{}", index + 1)))
                .alias_with_metadata(field.name(), metadata)
        })
        .collect::<Vec<_>>();
    let logical_plan = LogicalPlanBuilder::values_with_schema(rows, &df_schema)
        .map_err(datafusion_error_to_lix_error)?
        .project(projection)
        .map_err(datafusion_error_to_lix_error)?
        .build()
        .map_err(datafusion_error_to_lix_error)?;
    session
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .map_err(datafusion_error_to_lix_error)
}

async fn insert_query_input_plan(
    session: &SessionContext,
    schema: SchemaRef,
    query: &crate::sql2::bind::read::BoundRead,
    columns: &[crate::sql2::bind::expr::BoundColumnRef],
    params: &[Value],
) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>, LixError> {
    let input = session
        .state()
        .statement_to_plan(DataFusionStatement::Statement(Box::new(
            datafusion::sql::sqlparser::ast::Statement::Query(query.query.clone()),
        )))
        .await
        .map_err(datafusion_error_to_lix_error)?;
    validate_supported_logical_plan(&input)?;
    validate_json_predicates_in_logical_plan(&input)?;
    let json_predicate_params = json_predicate_params_in_logical_plan(&input);
    validate_parameter_count(&input, params.len())?;
    validate_json_predicate_params(&json_predicate_params, params)?;
    if input.schema().fields().len() != columns.len() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "INSERT has {} target columns but query returns {} columns",
                columns.len(),
                input.schema().fields().len()
            ),
        ));
    }

    let input_schema = input.schema().clone();
    let projection = schema
        .fields()
        .iter()
        .map(|field| {
            let expr = columns
                .iter()
                .position(|column| column.name == *field.name())
                .map(|index| {
                    let (qualifier, source_field) = input_schema.qualified_field(index);
                    Expr::Column(Column::new(qualifier.cloned(), source_field.name().clone()))
                })
                .unwrap_or_else(|| {
                    Expr::Literal(ScalarValue::try_new_null(field.data_type()).unwrap(), None)
                });
            Ok(expr
                .cast_to(field.data_type(), input_schema.as_ref())
                .map_err(datafusion_error_to_lix_error)?
                .alias(field.name()))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let mut dataframe = session
        .execute_logical_plan(input)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(ParamValues::List(
                params.iter().map(scalar_value_from_lix_value).collect(),
            ))
            .map_err(datafusion_error_to_lix_error)?;
    }
    let logical_plan = LogicalPlanBuilder::from(
        dataframe
            .into_optimized_plan()
            .map_err(datafusion_error_to_lix_error)?,
    )
    .project(projection)
    .map_err(datafusion_error_to_lix_error)?
    .build()
    .map_err(datafusion_error_to_lix_error)?;
    session
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .map_err(datafusion_error_to_lix_error)
}

fn insert_column_is_omitted(values: &BoundInsertValues, field_name: &str) -> bool {
    values.column_index(field_name).is_none()
}

fn validate_bound_write_input(plan: &LogicalWritePlan, params: &[Value]) -> Result<(), LixError> {
    if !matches!(
        plan.bound.target,
        BoundWriteTarget::File(FileWriteSurface::Base | FileWriteSurface::ByBranch)
    ) || plan.bound.op != BoundWriteOp::Insert
    {
        return Ok(());
    }

    match &plan.bound.input {
        BoundWriteInput::Values(values) => {
            if let Some(column_index) = values.column_index("data") {
                for row in &values.rows {
                    validate_lix_file_data_insert_expr(&row[column_index], params)?;
                }
            }
            Ok(())
        }
        BoundWriteInput::Query { columns, .. } => {
            if columns.iter().any(|column| column.name == "data") {
                Err(lix_file_data_type_lix_error())
            } else {
                Ok(())
            }
        }
        BoundWriteInput::None => Ok(()),
    }
}

fn validate_lix_file_data_insert_expr(expr: &BoundExpr, params: &[Value]) -> Result<(), LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Blob(_)) => Ok(()),
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Blob(_)) => Ok(()),
            _ => Err(lix_file_data_type_lix_error()),
        },
        BoundExpr::Function { .. } => Ok(()),
        _ => Err(lix_file_data_type_lix_error()),
    }
}

fn write_session_options(plan: &LogicalWritePlan) -> SqlWriteSessionOptions {
    let mut omitted_insert_columns = BTreeSet::new();
    if let BoundWriteInput::Values(values) = &plan.bound.input {
        if insert_column_is_omitted(values, "data") {
            omitted_insert_columns.insert("data".to_string());
        }
    }
    SqlWriteSessionOptions {
        omitted_insert_columns,
    }
}

fn insert_field_expr(
    session: &SessionContext,
    row: &[BoundExpr],
    source_index: Option<usize>,
    field_name: &str,
    data_type: &DataType,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Expr, LixError> {
    if plan.bound.branch_scope == BranchScope::Global && field_name == "global" {
        let has_explicit_global = source_index.is_some();
        if !has_explicit_global {
            return Ok(Expr::Literal(ScalarValue::Boolean(Some(true)), None));
        }
    }

    source_index
        .map(|column_index| datafusion_expr_from_bound_expr(session, &row[column_index], params))
        .unwrap_or_else(|| {
            ScalarValue::try_new_null(data_type)
                .map(|value| Expr::Literal(value, None))
                .map_err(datafusion_error_to_lix_error)
        })
}

fn datafusion_assignments(
    session: &SessionContext,
    schema: &datafusion::arrow::datatypes::Schema,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Vec<(String, Expr)>, LixError> {
    let df_schema = DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
    plan.bound
        .assignments
        .iter()
        .map(|assignment| {
            let field = schema
                .field_with_name(&assignment.column.name)
                .map_err(|error| LixError::unknown(format!("unknown update column: {error}")))?;
            let expr = datafusion_expr_from_bound_expr(session, &assignment.value, params)?
                .cast_to(field.data_type(), &df_schema)
                .map_err(datafusion_error_to_lix_error)?;
            Ok((assignment.column.name.clone(), expr))
        })
        .collect()
}

fn datafusion_write_filters(
    session: &SessionContext,
    schema: &datafusion::arrow::datatypes::Schema,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<Vec<Expr>, LixError> {
    let mut filters =
        datafusion_filters_from_predicate(session, schema, &plan.bound.predicate, params)?;
    if plan.bound.branch_scope == BranchScope::Global {
        let branch_column = if schema.field_with_name("branch_id").is_ok() {
            Some("branch_id")
        } else if schema.field_with_name("lixcol_branch_id").is_ok() {
            Some("lixcol_branch_id")
        } else {
            None
        };
        let Some(branch_column) = branch_column else {
            let df_schema =
                DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
            for filter in &filters {
                validate_json_predicate_expr_with_dfschema(&df_schema, filter)?;
            }
            return Ok(filters);
        };
        filters.push(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(branch_column))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(GLOBAL_BRANCH_ID.to_string())),
                None,
            )),
        )));
    }
    let df_schema = DFSchema::try_from(schema.clone()).map_err(datafusion_error_to_lix_error)?;
    for filter in &filters {
        validate_json_predicate_expr_with_dfschema(&df_schema, filter)?;
    }
    Ok(filters)
}

fn datafusion_filters_from_predicate(
    session: &SessionContext,
    schema: &datafusion::arrow::datatypes::Schema,
    predicate: &BoundPredicate,
    params: &[Value],
) -> Result<Vec<Expr>, LixError> {
    match predicate {
        BoundPredicate::True => Ok(Vec::new()),
        BoundPredicate::False => Ok(vec![Expr::Literal(ScalarValue::Boolean(Some(false)), None)]),
        BoundPredicate::And(predicates) => {
            let mut filters = Vec::new();
            for predicate in predicates {
                filters.extend(datafusion_filters_from_predicate(
                    session, schema, predicate, params,
                )?);
            }
            Ok(filters)
        }
        BoundPredicate::Or(predicates) => {
            let mut iter = predicates.iter();
            let Some(first) = iter.next() else {
                return Ok(Vec::new());
            };
            let mut expr = datafusion_single_filter_from_predicate(session, schema, first, params)?;
            for predicate in iter {
                expr = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(expr),
                    Operator::Or,
                    Box::new(datafusion_single_filter_from_predicate(
                        session, schema, predicate, params,
                    )?),
                ));
            }
            Ok(vec![expr])
        }
        BoundPredicate::Eq(left, right) => {
            let left_is_json = bound_expr_is_json(left, schema);
            let right_is_json = bound_expr_is_json(right, schema);
            Ok(vec![Expr::BinaryExpr(BinaryExpr::new(
                Box::new(datafusion_filter_expr_from_bound_expr(
                    session,
                    left,
                    params,
                    right_is_json,
                    is_identity_json_bound_expr(right),
                )?),
                Operator::Eq,
                Box::new(datafusion_filter_expr_from_bound_expr(
                    session,
                    right,
                    params,
                    left_is_json,
                    is_identity_json_bound_expr(left),
                )?),
            ))])
        }
        BoundPredicate::IsNull(expr) => Ok(vec![Expr::IsNull(Box::new(
            datafusion_filter_expr_from_bound_expr(session, expr, params, false, false)?,
        ))]),
        BoundPredicate::IsNotNull(expr) => Ok(vec![Expr::IsNotNull(Box::new(
            datafusion_filter_expr_from_bound_expr(session, expr, params, false, false)?,
        ))]),
        BoundPredicate::In { expr, values } => {
            let expr_is_json = bound_expr_is_json(expr, schema);
            let values_include_json = values.iter().any(|value| bound_expr_is_json(value, schema));
            let expr_is_identity_json = is_identity_json_bound_expr(expr);
            let values_include_identity_json = values.iter().any(is_identity_json_bound_expr);
            Ok(vec![Expr::InList(InList::new(
                Box::new(datafusion_filter_expr_from_bound_expr(
                    session,
                    expr,
                    params,
                    values_include_json,
                    values_include_identity_json,
                )?),
                values
                    .iter()
                    .map(|value| {
                        datafusion_filter_expr_from_bound_expr(
                            session,
                            value,
                            params,
                            expr_is_json,
                            expr_is_identity_json,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                false,
            ))])
        }
    }
}

fn datafusion_single_filter_from_predicate(
    session: &SessionContext,
    schema: &datafusion::arrow::datatypes::Schema,
    predicate: &BoundPredicate,
    params: &[Value],
) -> Result<Expr, LixError> {
    let filters = datafusion_filters_from_predicate(session, schema, predicate, params)?;
    let mut iter = filters.into_iter();
    let mut expr = iter
        .next()
        .unwrap_or_else(|| Expr::Literal(ScalarValue::Boolean(Some(true)), None));
    for filter in iter {
        expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expr),
            Operator::And,
            Box::new(filter),
        ));
    }
    Ok(expr)
}

fn datafusion_filter_expr_from_bound_expr(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
    json_comparison_context: bool,
    identity_json_comparison_context: bool,
) -> Result<Expr, LixError> {
    match expr {
        BoundExpr::Param(param) if json_comparison_context => {
            let Some(value) = params.get(param.index - 1) else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                ));
            };
            let ScalarAndMetadata { value, metadata } = scalar_value_from_lix_value(value);
            if identity_json_comparison_context {
                if let ScalarValue::Utf8(Some(raw)) = &value {
                    return Ok(Expr::Literal(
                        ScalarValue::Utf8(Some(canonical_json_text(raw)?)),
                        Some(json_field_metadata()),
                    ));
                }
            }
            let metadata = metadata.or_else(|| match &value {
                ScalarValue::Utf8(Some(_)) => Some(json_field_metadata()),
                _ => None,
            });
            Ok(Expr::Literal(value, metadata))
        }
        BoundExpr::Literal(BoundLiteral::Text(value))
            if json_comparison_context && identity_json_comparison_context =>
        {
            Ok(Expr::Literal(
                ScalarValue::Utf8(Some(canonical_json_text(value)?)),
                Some(json_field_metadata()),
            ))
        }
        _ => datafusion_expr_from_bound_expr(session, expr, params),
    }
}

fn datafusion_expr_from_bound_expr(
    session: &SessionContext,
    expr: &BoundExpr,
    params: &[Value],
) -> Result<Expr, LixError> {
    match expr {
        BoundExpr::Column(column) => Ok(Expr::Column(Column::from_name(column.name.clone()))),
        BoundExpr::Literal(literal) => Ok(Expr::Literal(
            scalar_from_bound_literal(literal)?,
            bound_literal_metadata(literal),
        )),
        BoundExpr::Param(param) => {
            let Some(value) = params.get(param.index - 1) else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                ));
            };
            let ScalarAndMetadata { value, metadata } = scalar_value_from_lix_value(value);
            Ok(Expr::Literal(value, metadata))
        }
        BoundExpr::Function { name, args } => {
            let udf = session.udf(name).map_err(datafusion_error_to_lix_error)?;
            let args = args
                .iter()
                .map(|arg| datafusion_expr_from_bound_expr(session, arg, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)))
        }
    }
}

fn scalar_from_bound_literal(literal: &BoundLiteral) -> Result<ScalarValue, LixError> {
    Ok(match literal {
        BoundLiteral::Null => ScalarValue::Null,
        BoundLiteral::Bool(value) => ScalarValue::Boolean(Some(*value)),
        BoundLiteral::Integer(value) => ScalarValue::Int64(Some(*value)),
        BoundLiteral::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        BoundLiteral::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
        BoundLiteral::Blob(value) => ScalarValue::Binary(Some(value.clone())),
    })
}

fn bound_literal_metadata(literal: &BoundLiteral) -> Option<FieldMetadata> {
    match literal {
        BoundLiteral::Json(_) => Some(json_field_metadata()),
        _ => None,
    }
}

fn bound_expr_is_json(expr: &BoundExpr, schema: &datafusion::arrow::datatypes::Schema) -> bool {
    match expr {
        BoundExpr::Column(column) => schema
            .fields()
            .iter()
            .find(|field| field.name() == &column.name)
            .is_some_and(|field| field_is_json(field.as_ref())),
        BoundExpr::Literal(BoundLiteral::Json(_)) => true,
        BoundExpr::Function { name, .. } => matches!(name.as_str(), "lix_json" | "lix_json_get"),
        _ => false,
    }
}

fn is_identity_json_bound_expr(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Column(column)
            if matches!(column.name.as_str(), "entity_pk" | "lixcol_entity_pk")
    )
}

fn canonical_json_text(raw: &str) -> Result<String, LixError> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(|value| value.to_string())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("JSON comparison value is not valid JSON: {error}"),
            )
        })
}

fn write_target_table_name(plan: &LogicalWritePlan) -> Result<&'static str, LixError> {
    match &plan.bound.target {
        BoundWriteTarget::LixState
            if plan.bound.branch_scope == BranchScope::Global
                && plan.bound.op != BoundWriteOp::Insert =>
        {
            Ok("lix_state_by_branch")
        }
        BoundWriteTarget::LixState => Ok("lix_state"),
        BoundWriteTarget::LixStateByBranch => Ok("lix_state_by_branch"),
        BoundWriteTarget::File(FileWriteSurface::Base) => Ok("lix_file"),
        BoundWriteTarget::File(FileWriteSurface::ByBranch) => Ok("lix_file_by_branch"),
        BoundWriteTarget::Directory(DirectoryWriteSurface::Base) => Ok("lix_directory"),
        BoundWriteTarget::Directory(DirectoryWriteSurface::ByBranch) => {
            Ok("lix_directory_by_branch")
        }
        BoundWriteTarget::Branch => Ok("lix_branch"),
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "sql2 DataFusion reference writer currently supports only lix_state writes",
        )),
    }
}

fn affected_rows_from_query_result(result: SqlQueryResult) -> Result<u64, LixError> {
    let Some(first_row) = result.rows.first() else {
        return Ok(0);
    };
    let Some(first_value) = first_row.first() else {
        return Ok(0);
    };
    match first_value {
        Value::Integer(value) if *value >= 0 => Ok(*value as u64),
        Value::Text(value) => value.parse::<u64>().map_err(|error| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("failed to parse affected row count from SQL result: {error}"),
            )
        }),
        other => Err(LixError::new(
            LixError::CODE_UNKNOWN,
            format!("expected affected row count, got {other:?}"),
        )),
    }
}

fn validate_json_predicate_params(
    json_predicate_params: &BTreeSet<usize>,
    params: &[Value],
) -> Result<(), LixError> {
    for index in json_predicate_params {
        let Some(value) = params.get(index - 1) else {
            continue;
        };
        if !matches!(value, Value::Json(_) | Value::Null) {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "JSON columns can only be compared with JSON expressions",
            )
            .with_hint("Use lix_json(...) or pass a JSON parameter value instead of bare text."));
        }
    }
    Ok(())
}

fn validate_parameter_count(plan: &LogicalPlan, param_count: usize) -> Result<(), LixError> {
    let parameter_names = plan
        .get_parameter_names()
        .map_err(datafusion_error_to_lix_error)?;
    let expected_count = expected_positional_parameter_count(&parameter_names)?;
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
        "placeholders": sorted_parameter_names(&parameter_names),
    })))
}

fn expected_positional_parameter_count(
    parameter_names: &HashSet<String>,
) -> Result<usize, LixError> {
    let mut max_index = 0usize;
    for name in parameter_names {
        let Some(index) = name
            .strip_prefix('$')
            .and_then(|raw| raw.parse::<usize>().ok())
        else {
            return Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("unsupported SQL parameter placeholder '{name}'"),
            )
            .with_hint("Use placeholders like ?, ? or numbered placeholders like $1, $2, ...")
            .with_details(json!({
                "operation": "execute",
                "placeholder": name,
            })));
        };
        if index == 0 {
            return Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                "SQL parameter placeholders are 1-indexed",
            )
            .with_hint("Use placeholders like ?, ? or numbered placeholders like $1, $2, ...")
            .with_details(json!({
                "operation": "execute",
                "placeholder": name,
            })));
        }
        max_index = max_index.max(index);
    }
    Ok(max_index)
}

fn sorted_parameter_names(parameter_names: &HashSet<String>) -> Vec<String> {
    let mut names = parameter_names.iter().cloned().collect::<Vec<_>>();
    names.sort();
    names
}

fn validate_supported_logical_plan(plan: &LogicalPlan) -> Result<(), LixError> {
    match plan {
        LogicalPlan::Ddl(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "DDL statements are not supported by Lix SQL",
            )
            .with_hint(
                "Use Lix entity surfaces such as lix_registered_schema, lix_branch, lix_file, and lix_key_value instead of CREATE/DROP statements.",
            ));
        }
        LogicalPlan::Statement(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "SQL utility statements are not supported by Lix SQL",
            ));
        }
        LogicalPlan::Copy(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "COPY statements are not supported by Lix SQL",
            ));
        }
        LogicalPlan::RecursiveQuery(_) => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "recursive CTEs are not supported by Lix SQL",
            )
            .with_hint(
                "Use explicit commit graph surfaces such as lix_commit, lix_commit_edge, and lix_state_history instead of WITH RECURSIVE.",
            ));
        }
        _ => {}
    }

    for input in plan.inputs() {
        validate_supported_logical_plan(input)?;
    }

    Ok(())
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarAndMetadata {
    match value {
        Value::Null => ScalarValue::Null.into(),
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)).into(),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)).into(),
        Value::Real(value) => ScalarValue::Float64(Some(*value)).into(),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())).into(),
        Value::Json(value) => ScalarAndMetadata::new(
            ScalarValue::Utf8(Some(value.to_string())),
            Some(json_field_metadata()),
        ),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())).into(),
    }
}

fn json_field_metadata() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSON.to_string(),
    )]))
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    crate::sql2::error::datafusion_error_to_lix_error(error)
}

fn query_result_from_batches(
    result_fields: &[Field],
    batches: &[RecordBatch],
) -> Result<SqlQueryResult, LixError> {
    let result_columns = result_fields
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for (column_index, array) in batch.columns().iter().enumerate() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                let field = result_fields.get(column_index);
                row.push(scalar_value_to_lix_value(&scalar, field)?);
            }
            rows.push(row);
        }
    }

    Ok(SqlQueryResult {
        rows,
        columns: result_columns.to_vec(),
        notices: Vec::new(),
    })
}

fn history_filter_notices(plan: &LogicalPlan) -> Vec<LixNotice> {
    let mut observations = Vec::new();
    collect_notice_observations(plan, &Vec::new(), &mut observations);

    let mut notices = Vec::new();
    let mut emitted_codes = HashSet::<String>::new();
    for observation in observations {
        for rule in HISTORY_NOTICE_RULES {
            if observation.table_name != rule.table_name {
                continue;
            }
            if !observation.references_any(rule.payload_columns)
                || observation.references_any(rule.identity_columns)
            {
                continue;
            }

            let code = format!("LIX_HISTORY_NON_IDENTITY_FILTER:{}", rule.table_name);
            if emitted_codes.insert(code) {
                notices.push(history_non_identity_filter_notice(rule.table_name));
            }
        }
    }
    notices
}

#[derive(Debug)]
struct NoticeObservation {
    table_name: String,
    filter_columns: HashSet<String>,
}

impl NoticeObservation {
    fn references_any(&self, columns: &[&str]) -> bool {
        columns
            .iter()
            .any(|column| self.filter_columns.contains(*column))
    }
}

struct HistoryNoticeRule {
    table_name: &'static str,
    payload_columns: &'static [&'static str],
    identity_columns: &'static [&'static str],
}

const HISTORY_NOTICE_RULES: &[HistoryNoticeRule] = &[
    HistoryNoticeRule {
        table_name: "lix_file_history",
        payload_columns: &["path", "directory_id", "name", "hidden", "data"],
        identity_columns: &["id", "lixcol_entity_pk"],
    },
    HistoryNoticeRule {
        table_name: "lix_directory_history",
        payload_columns: &["path", "parent_id", "name", "hidden"],
        identity_columns: &["id", "lixcol_entity_pk"],
    },
];

fn collect_notice_observations(
    plan: &LogicalPlan,
    active_filter_columns: &Vec<HashSet<String>>,
    observations: &mut Vec<NoticeObservation>,
) {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut next_filters = active_filter_columns.clone();
            next_filters.push(expr_column_names(&filter.predicate));
            collect_notice_observations(&filter.input, &next_filters, observations);
        }
        LogicalPlan::TableScan(scan) => {
            let mut filter_columns = HashSet::new();
            for columns in active_filter_columns {
                filter_columns.extend(columns.iter().cloned());
            }
            for filter in &scan.filters {
                filter_columns.extend(expr_column_names(filter));
            }
            if !filter_columns.is_empty() {
                observations.push(NoticeObservation {
                    table_name: table_reference_name(&scan.table_name),
                    filter_columns,
                });
            }
        }
        other => {
            for input in other.inputs() {
                collect_notice_observations(input, active_filter_columns, observations);
            }
        }
    }
}

fn expr_column_names(expr: &Expr) -> HashSet<String> {
    expr.column_refs()
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

fn table_reference_name(table: &datafusion::common::TableReference) -> String {
    match table {
        datafusion::common::TableReference::Bare { table } => table.to_string(),
        datafusion::common::TableReference::Partial { table, .. } => table.to_string(),
        datafusion::common::TableReference::Full { table, .. } => table.to_string(),
    }
}

fn history_non_identity_filter_notice(view_name: &str) -> LixNotice {
    LixNotice {
        code: "LIX_HISTORY_NON_IDENTITY_FILTER".to_string(),
        message: format!("{view_name} was filtered without an identity predicate."),
        hint: Some(
            "Filter by id or lixcol_entity_pk to include tombstones and renamed history."
                .to_string(),
        ),
    }
}

fn scalar_value_to_lix_value(
    value: &ScalarValue,
    field: Option<&Field>,
) -> Result<Value, LixError> {
    match value {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(Some(value)) => Ok(Value::Boolean(*value)),
        ScalarValue::Boolean(None) => Ok(Value::Null),
        ScalarValue::Int8(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::Int8(None) => Ok(Value::Null),
        ScalarValue::Int16(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::Int16(None) => Ok(Value::Null),
        ScalarValue::Int32(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::Int32(None) => Ok(Value::Null),
        ScalarValue::Int64(Some(value)) => Ok(Value::Integer(*value)),
        ScalarValue::Int64(None) => Ok(Value::Null),
        ScalarValue::UInt8(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::UInt8(None) => Ok(Value::Null),
        ScalarValue::UInt16(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::UInt16(None) => Ok(Value::Null),
        ScalarValue::UInt32(Some(value)) => Ok(Value::Integer(i64::from(*value))),
        ScalarValue::UInt32(None) => Ok(Value::Null),
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Ok(Value::Integer(value)),
            Err(_) => Ok(Value::Text(value.to_string())),
        },
        ScalarValue::UInt64(None) => Ok(Value::Null),
        ScalarValue::Float32(Some(value)) => Ok(Value::Real(f64::from(*value))),
        ScalarValue::Float32(None) => Ok(Value::Null),
        ScalarValue::Float64(Some(value)) => Ok(Value::Real(*value)),
        ScalarValue::Float64(None) => Ok(Value::Null),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => string_scalar_to_lix_value(value, field),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Ok(Value::Null)
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Ok(Value::Blob(value.clone()))
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Ok(Value::Null),
        other => Ok(Value::Text(other.to_string())),
    }
}

fn string_scalar_to_lix_value(value: &str, field: Option<&Field>) -> Result<Value, LixError> {
    if field.is_some_and(field_is_json) {
        return serde_json::from_str::<serde_json::Value>(value)
            .map(Value::Json)
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_INVALID_JSON",
                    format!(
                        "column '{}' is marked as JSON but contains invalid JSON: {error}",
                        field
                            .map(|field| field.name().as_str())
                            .unwrap_or("<unknown>")
                    ),
                )
            });
    }
    Ok(Value::Text(value.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;
    use serde_json::Value as JsonValue;

    use super::{execute_sql, SqlExecutionContext, SqlWriteExecutionContext};
    use crate::binary_cas::BlobDataReader;
    use crate::branch::BranchRefReader;
    use crate::commit_graph::{
        CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphCommit,
        CommitGraphReader, ReachableCommitGraphCommit,
    };
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::json_store::JsonStoreContext;
    use crate::live_state::{
        LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::sql2::{
        bind_statement, create_write_logical_plan, execute_write_logical_plan, parse_statement,
        plan_write,
    };
    use crate::sql2::{
        ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlHistoryQuerySource,
    };
    use crate::storage::{
        InMemoryStorageBackend, InMemoryStorageRead, StorageContext, StorageReadOptions,
        StorageReadScope,
    };
    use crate::transaction::types::{
        TransactionWrite, TransactionWriteOutcome, TransactionWriteRow,
    };
    use crate::{Engine, ExecuteResult, SessionContext};
    use crate::{LixError, NullableKeyFilter, Value, GLOBAL_BRANCH_ID};

    struct DummyBlobReader;
    struct StaticBlobReader {
        bytes: Vec<u8>,
    }
    struct DummyLiveStateReader;
    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }
    struct DummyCommitGraphReader;
    struct DummyBranchRefReader;
    fn test_read_scope(
        storage: &StorageContext<InMemoryStorageBackend>,
    ) -> StorageReadScope<InMemoryStorageRead> {
        storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open")
    }

    #[allow(dead_code)]
    fn test_functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(
            Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct CapturingStagedWrites {
        deltas: Vec<CapturedStageWrite>,
    }

    #[derive(Clone)]
    struct CapturedStageWrite {
        rows: Vec<TransactionWriteRow>,
    }

    impl CapturedStageWrite {
        fn pending_write_overlay(&self) -> Result<CapturedStageOverlay, LixError> {
            Ok(CapturedStageOverlay {
                rows: self.rows.clone(),
            })
        }
    }

    struct CapturedStageOverlay {
        rows: Vec<TransactionWriteRow>,
    }

    impl CapturedStageOverlay {
        fn visible_semantic_rows(
            &self,
            include_tombstones: bool,
            schema_key: &str,
        ) -> Vec<CapturedStageRow> {
            self.visible_all_semantic_rows()
                .into_iter()
                .filter(|row| row.schema_key == schema_key)
                .filter(|row| include_tombstones || !row.tombstone)
                .collect()
        }

        fn visible_all_semantic_rows(&self) -> Vec<CapturedStageRow> {
            self.rows
                .iter()
                .cloned()
                .map(CapturedStageRow::from)
                .collect()
        }
    }

    struct CapturedStageRow {
        entity_pk: String,
        schema_key: String,
        branch_id: String,
        file_id: Option<String>,
        snapshot_content: Option<String>,
        metadata: Option<String>,
        global: bool,
        untracked: bool,
        tombstone: bool,
    }

    impl From<TransactionWriteRow> for CapturedStageRow {
        fn from(row: TransactionWriteRow) -> Self {
            Self {
                entity_pk: row
                    .entity_pk
                    .expect("captured staged row should carry entity_pk")
                    .as_json_array_text()
                    .expect("captured staged row should project entity_pk"),
                schema_key: row.schema_key,
                branch_id: row.branch_id,
                file_id: row.file_id,
                global: row.global,
                untracked: row.untracked,
                tombstone: row.snapshot.is_none(),
                snapshot_content: row.snapshot.map(|snapshot| snapshot.to_string()),
                metadata: row.metadata.map(|metadata| metadata.to_string()),
            }
        }
    }

    struct DummySqlExecutionContext<'a> {
        active_branch_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateReader>,
        schema_definitions: Vec<JsonValue>,
    }

    impl<'a> SqlExecutionContext for DummySqlExecutionContext<'a> {
        type ReadStore = StorageReadScope<InMemoryStorageRead>;

        fn active_branch_id(&self) -> &str {
            self.active_branch_id
        }

        fn live_state(&self) -> Arc<dyn LiveStateReader> {
            Arc::clone(&self.live_state)
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn history_query_source(&self) -> SqlHistoryQuerySource<Self::ReadStore> {
            let storage = StorageContext::new(InMemoryStorageBackend::new());
            let read_scope = test_read_scope(&storage);
            HistoryQuerySource {
                json_reader: JsonStoreContext::new().reader(read_scope.store()),
            }
        }

        fn changelog_query_source(&self) -> SqlChangelogQuerySource<Self::ReadStore> {
            let storage = StorageContext::new(InMemoryStorageBackend::new());
            let read_scope = test_read_scope(&storage);
            ChangelogQuerySource {
                store: read_scope.clone(),
                json_reader: JsonStoreContext::new().reader(read_scope.store()),
            }
        }

        fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
            Box::new(DummyCommitGraphReader)
        }

        fn branch_ref(&self) -> Arc<dyn BranchRefReader> {
            Arc::new(DummyBranchRefReader)
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }
    }

    struct DummySqlWriteExecutionContext<'a> {
        active_branch_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateReader>,
        staged_writes: Arc<Mutex<CapturingStagedWrites>>,
        schema_definitions: Vec<JsonValue>,
    }

    #[async_trait]
    impl SqlWriteExecutionContext for DummySqlWriteExecutionContext<'_> {
        fn active_branch_id(&self) -> &str {
            self.active_branch_id
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schema_definitions.clone())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            self.blob_reader.load_bytes_many(hashes).await
        }

        async fn scan_live_state(
            &mut self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.live_state.scan_rows(request).await
        }

        async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<String>, LixError> {
            if branch_id == "missing-branch" {
                return Ok(None);
            }
            Ok(Some(format!("commit-{branch_id}")))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            let count = match &write {
                TransactionWrite::Rows { rows, .. } => rows.len() as u64,
                TransactionWrite::RowsWithFileData { count, .. } => *count,
            };
            let rows = match write {
                TransactionWrite::Rows { rows, .. } => rows,
                TransactionWrite::RowsWithFileData { rows, .. } => rows,
            };
            self.staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .push(CapturedStageWrite { rows });
            Ok(TransactionWriteOutcome { count })
        }
    }

    async fn execute_write_sql(
        ctx: &mut dyn SqlWriteExecutionContext,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::SqlQueryResult, LixError> {
        let plan = create_write_logical_plan(ctx, sql).await?;
        let count = execute_write_logical_plan(ctx, plan, params).await?;
        Ok(crate::SqlQueryResult {
            columns: vec!["count".to_string()],
            rows: vec![vec![Value::Integer(count as i64)]],
            notices: Vec::new(),
        })
    }

    #[async_trait]
    impl BranchRefReader for DummyBranchRefReader {
        async fn load_head(
            &self,
            branch_id: &str,
        ) -> Result<Option<crate::branch::BranchHead>, LixError> {
            if branch_id == "missing-branch" {
                return Ok(None);
            }
            Ok(Some(crate::branch::BranchHead {
                branch_id: branch_id.to_string(),
                commit_id: format!("commit-{branch_id}"),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<crate::branch::BranchHead>, LixError> {
            Ok(["branch-a", "branch-b"]
                .into_iter()
                .map(|branch_id| crate::branch::BranchHead {
                    branch_id: branch_id.to_string(),
                    commit_id: format!("commit-{branch_id}"),
                })
                .collect())
        }
    }

    #[async_trait]
    impl CommitGraphReader for DummyCommitGraphReader {
        async fn load_commit(
            &mut self,
            _commit_id: &str,
        ) -> Result<Option<CommitGraphCommit>, LixError> {
            Ok(None)
        }

        async fn reachable_commits(
            &mut self,
            _head_commit_id: &str,
        ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &str,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl LiveStateReader for DummyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            if matches!(
                request.filter.rows,
                crate::live_state::LiveStateRowFilter::None
            ) {
                return Ok(Vec::new());
            }
            let mut rows = self
                .rows
                .iter()
                .filter(|row| {
                    (request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key))
                        && (request.filter.entity_pks.is_empty()
                            || request.filter.entity_pks.contains(&row.entity_pk))
                        && (request.filter.branch_ids.is_empty()
                            || request.filter.branch_ids.contains(&row.branch_id))
                        && request
                            .filter
                            .untracked
                            .is_none_or(|untracked| row.untracked == untracked)
                        && (request.filter.include_tombstones || !row.deleted)
                        && (request.filter.file_ids.is_empty()
                            || request.filter.file_ids.iter().any(|filter| match filter {
                                NullableKeyFilter::Any => true,
                                NullableKeyFilter::Null => row.file_id.is_none(),
                                NullableKeyFilter::Value(file_id) => {
                                    row.file_id.as_ref() == Some(file_id)
                                }
                            }))
                })
                .cloned()
                .collect::<Vec<_>>();
            if let Some(limit) = request.limit {
                rows.truncate(limit);
            }
            Ok(rows)
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BlobDataReader for DummyBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl BlobDataReader for StaticBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                Some(
                    self.bytes.clone()
                );
                hashes.len()
            ]))
        }
    }

    fn live_lix_state_row(entity_pk: &str, metadata: Option<&str>) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: metadata.map(str::to_string),
            deleted: false,
            branch_id: "branch-a".to_string(),
            change_id: Some(format!("change-{entity_pk}")),
            commit_id: Some(format!("commit-{entity_pk}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn global_lix_state_row(entity_pk: &str, metadata: Option<&str>) -> MaterializedLiveStateRow {
        let mut row = live_lix_state_row(entity_pk, metadata);
        row.branch_id = GLOBAL_BRANCH_ID.to_string();
        row.global = true;
        row
    }

    fn live_entity_row(entity_pk: &str, branch_id: &str, value: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "test_state_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: Some(json!({ "source": entity_pk }).to_string()),
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(format!("change-{entity_pk}")),
            commit_id: Some(format!("commit-{entity_pk}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_directory_row(
        entity_pk: &str,
        branch_id: &str,
        parent_id: Option<&str>,
        name: &str,
        hidden: bool,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": entity_pk,
                    "parent_id": parent_id,
                    "name": name,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(json!({ "source": entity_pk }).to_string()),
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(format!("change-{entity_pk}")),
            commit_id: Some(format!("commit-{entity_pk}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_file_row(
        entity_pk: &str,
        branch_id: &str,
        directory_id: Option<&str>,
        name: &str,
        hidden: bool,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": entity_pk,
                    "directory_id": directory_id,
                    "name": name,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(json!({ "source": entity_pk }).to_string()),
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(format!("change-{entity_pk}")),
            commit_id: Some(format!("commit-{entity_pk}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_blob_ref_row(
        entity_pk: &str,
        branch_id: &str,
        bytes: &[u8],
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "lix_binary_blob_ref".to_string(),
            file_id: Some(entity_pk.to_string()),
            snapshot_content: Some(
                json!({
                    "id": entity_pk,
                    "blob_hash": crate::common::stable_content_fingerprint_hex(bytes),
                    "size_bytes": bytes.len()
                })
                .to_string(),
            ),
            metadata: Some(json!({ "source": entity_pk }).to_string()),
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(format!("change-{entity_pk}-blob")),
            commit_id: Some(format!("commit-{entity_pk}-blob")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn sql_execution_context_exposes_live_state_and_blob_reader() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "branch-a",
            blob_reader: Arc::clone(&blob_reader),
            live_state: Arc::clone(&live_state) as Arc<dyn LiveStateReader>,
            schema_definitions: vec![],
        };

        let actual = ctx.live_state();
        let expected = live_state as Arc<dyn LiveStateReader>;
        assert_eq!(ctx.active_branch_id(), "branch-a");
        assert!(Arc::ptr_eq(&actual, &expected));
        assert!(Arc::ptr_eq(&ctx.blob_reader(), &blob_reader));
    }

    #[tokio::test]
    async fn execute_sql_uses_execution_context_boundary() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1", &[])
            .await
            .expect("sql2 execute should support literal-only queries");
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }

    #[tokio::test]
    async fn execute_sql_collects_union_all_partitions() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1 UNION ALL SELECT 2", &[])
            .await
            .expect("sql2 execute should collect UNION ALL partitions");
        assert_eq!(
            result.rows,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
    }

    #[tokio::test]
    async fn execute_sql_rejects_extra_parameters() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let error = execute_sql(
            &ctx,
            "SELECT $1 AS value",
            &[Value::Integer(1), Value::Integer(2)],
        )
        .await
        .expect_err("extra params should fail instead of being ignored");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(
            error.message,
            "SQL expected 1 parameter(s), but 2 parameter(s) were provided"
        );
        assert_eq!(
            error.details,
            Some(json!({
                "operation": "execute",
                "expected_param_count": 1,
                "provided_param_count": 2,
                "placeholders": ["$1"],
            }))
        );
    }

    #[tokio::test]
    async fn execute_sql_exposes_datafusion_information_schema() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let ctx = DummySqlExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            schema_definitions: vec![],
        };

        let information_schema_result = execute_sql(
            &ctx,
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'lix_state'",
            &[],
        )
        .await
        .expect("information_schema.tables should be enabled");
        assert_eq!(
            information_schema_result.rows,
            vec![vec![Value::Text("lix_state".to_string())]]
        );

        let tables_result = execute_sql(
            &ctx,
            "SELECT table_name FROM information_schema.tables",
            &[],
        )
        .await
        .expect("information_schema.tables should list registered tables");
        assert!(tables_result.rows.iter().any(|row| {
            row.iter()
                .any(|value| matches!(value, Value::Text(value) if value == "lix_state"))
        }));
    }

    async fn setup_engine_history_fixture() -> Result<(SessionContext, String), LixError> {
        let backend = crate::storage::InMemoryStorageBackend::new();
        let init_receipt = Engine::initialize(backend.clone()).await?;
        let engine = Engine::new(backend).await?;
        let session = engine.open_session(init_receipt.main_branch_id).await?;

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"test_state_schema\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"}},\"required\":[\"value\",\"count\"],\"additionalProperties\":false}'),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO test_state_schema \
	             (lixcol_entity_pk, value, count, lixcol_metadata, lixcol_untracked) \
	             VALUES (lix_json('[\"entity-history\"]'), 'A', 7, '{\"source\":\"history\"}', false)",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_directory (id, path, hidden) \
                 VALUES ('dir-docs', '/docs/', false)",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
                 VALUES ('file-a', '/docs/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await?;

        let active_branch_id = session.active_branch_id().await?;
        let head_commit_id = engine
            .load_branch_head_commit_id(&active_branch_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "history fixture expected the session branch to have a head commit",
                )
            })?;
        Ok((session, head_commit_id))
    }

    #[tokio::test]
    async fn lix_file_path_predicates_canonicalize_bound_values_like_writes() {
        let backend = crate::storage::InMemoryStorageBackend::new();
        let init_receipt = Engine::initialize(backend.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(backend).await.expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_branch_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-nfc', $1, X'41')",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NFD path insert should canonicalize");

        let nfd_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NFD path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(nfd_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let percent_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/%43afe%CC%81.txt'",
                &[],
            )
            .await
            .expect("percent-encoded path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(percent_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let reversed_result = session
            .execute(
                "SELECT id FROM lix_file WHERE $1 = path",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("reversed path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(reversed_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let or_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1 OR id = 'missing'",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("OR path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(or_result).1,
            vec![vec![Value::Text("file-nfc".to_string())]]
        );

        let not_result = session
            .execute(
                "SELECT id FROM lix_file WHERE NOT (path = $1)",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NOT path predicate should canonicalize");
        assert!(rows_from_execute_result(not_result).1.is_empty());

        let not_in_result = session
            .execute(
                "SELECT id FROM lix_file WHERE path NOT IN ($1)",
                &[Value::Text("/%43afe%CC%81.txt".to_string())],
            )
            .await
            .expect("NOT IN path predicate should canonicalize");
        assert!(rows_from_execute_result(not_in_result).1.is_empty());

        let update_result = session
            .execute(
                "UPDATE lix_file SET hidden = true WHERE path = $1 OR id = 'missing'",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("update predicate should canonicalize");
        assert_eq!(update_result.rows_affected(), 1);

        let delete_result = session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[Value::Text("/%43afe%CC%81.txt".to_string())],
            )
            .await
            .expect("delete predicate should canonicalize");
        assert_eq!(delete_result.rows_affected(), 1);
    }

    #[tokio::test]
    async fn lix_file_path_predicates_reject_non_literal_path_values() {
        let backend = crate::storage::InMemoryStorageBackend::new();
        let init_receipt = Engine::initialize(backend.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(backend).await.expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_branch_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-nfc', $1, X'41')",
                &[Value::Text("/Cafe\u{301}.txt".to_string())],
            )
            .await
            .expect("NFD path insert should canonicalize");

        let error = session
            .execute("SELECT id FROM lix_file WHERE path = id", &[])
            .await
            .expect_err("computed path predicate values should be rejected");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("filesystem path predicates only support literal path values"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn lix_directory_path_predicates_canonicalize_bound_values_like_writes() {
        let backend = crate::storage::InMemoryStorageBackend::new();
        let init_receipt = Engine::initialize(backend.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(backend).await.expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_branch_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-nfc', $1)",
                &[Value::Text("/Cafe\u{301}/".to_string())],
            )
            .await
            .expect("NFD directory path insert should canonicalize");

        let result = session
            .execute(
                "SELECT id FROM lix_directory WHERE path IN ($1)",
                &[Value::Text("/%43afe%CC%81/".to_string())],
            )
            .await
            .expect("directory path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(result).1,
            vec![vec![Value::Text("dir-nfc".to_string())]]
        );

        let or_result = session
            .execute(
                "SELECT id FROM lix_directory WHERE id = 'missing' OR path = $1",
                &[Value::Text("/Cafe\u{301}/".to_string())],
            )
            .await
            .expect("directory OR path predicate should canonicalize");
        assert_eq!(
            rows_from_execute_result(or_result).1,
            vec![vec![Value::Text("dir-nfc".to_string())]]
        );

        let not_in_result = session
            .execute(
                "SELECT id FROM lix_directory WHERE path NOT IN ($1)",
                &[Value::Text("/%43afe%CC%81/".to_string())],
            )
            .await
            .expect("directory NOT IN path predicate should canonicalize");
        assert!(rows_from_execute_result(not_in_result).1.is_empty());
    }

    #[tokio::test]
    async fn lix_directory_path_predicates_reject_non_literal_path_values() {
        let backend = crate::storage::InMemoryStorageBackend::new();
        let init_receipt = Engine::initialize(backend.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(backend).await.expect("engine should open");
        let session = engine
            .open_session(init_receipt.main_branch_id)
            .await
            .expect("session should open");

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES ('dir-nfc', $1)",
                &[Value::Text("/Cafe\u{301}/".to_string())],
            )
            .await
            .expect("NFD directory path insert should canonicalize");

        let error = session
            .execute("SELECT id FROM lix_directory WHERE path IN (id)", &[])
            .await
            .expect_err("computed directory path predicate values should be rejected");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("filesystem path predicates only support literal path values"),
            "{error:?}"
        );
    }

    fn rows_from_execute_result(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
        let rows = result;
        (
            rows.columns().to_vec(),
            rows.rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect(),
        )
    }

    #[tokio::test]
    async fn execute_sql_reads_lix_state_history_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT entity_pk, snapshot_content, metadata, depth, start_commit_id \
	             FROM lix_state_history \
	             WHERE schema_key = 'test_state_schema' \
	               AND entity_pk = lix_json('[\"entity-history\"]') \
	               AND start_commit_id = '{head_commit_id}' \
	               AND depth >= 0"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read lix_state_history through real engine context");
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "entity_pk",
                "snapshot_content",
                "metadata",
                "depth",
                "start_commit_id"
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Json(json!(["entity-history"])));
        assert_eq!(rows[0][1], Value::Json(json!({"count": 7, "value": "A"})));
        assert_eq!(rows[0][2], Value::Json(json!({"source": "history"})));
        assert!(matches!(rows[0][3], Value::Integer(_)));
        assert_eq!(rows[0][4], Value::Text(head_commit_id.clone()));
    }

    #[tokio::test]
    async fn execute_sql_reads_entity_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT value, count, lixcol_entity_pk, lixcol_start_commit_id, lixcol_depth \
	             FROM test_state_schema_history \
	             WHERE lixcol_start_commit_id = '{head_commit_id}' \
	               AND lixcol_entity_pk = lix_json('[\"entity-history\"]')"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read entity history through real engine context");
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "value",
                "count",
                "lixcol_entity_pk",
                "lixcol_start_commit_id",
                "lixcol_depth",
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("A".to_string()));
        assert_eq!(rows[0][1], Value::Integer(7));
        assert_eq!(rows[0][2], Value::Json(json!(["entity-history"])));
        assert_eq!(rows[0][3], Value::Text(head_commit_id));
        assert!(matches!(rows[0][4], Value::Integer(_)));
    }

    #[tokio::test]
    async fn execute_sql_reads_directory_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT id, parent_id, name, path, hidden, lixcol_start_commit_id, lixcol_depth \
             FROM lix_directory_history \
             WHERE id = 'dir-docs' AND lixcol_start_commit_id = '{head_commit_id}'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read directory history through real engine context");
        assert!(
            result.notices().is_empty(),
            "identity-filtered directory history should not emit soft notices"
        );
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "id",
                "parent_id",
                "name",
                "path",
                "hidden",
                "lixcol_start_commit_id",
                "lixcol_depth",
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("dir-docs".to_string()));
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[0][2], Value::Text("docs".to_string()));
        assert_eq!(rows[0][3], Value::Text("/docs/".to_string()));
        assert_eq!(rows[0][4], Value::Boolean(false));
        assert_eq!(rows[0][5], Value::Text(head_commit_id.clone()));
        assert!(matches!(rows[0][6], Value::Integer(_)));

        let name_filtered_result = session
            .execute(
                &format!(
                    "SELECT id \
             FROM lix_directory_history \
             WHERE name = 'docs' \
               AND lixcol_start_commit_id = '{head_commit_id}'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should attach notices to name-filtered directory history reads");
        assert_eq!(name_filtered_result.notices().len(), 1);
        assert_eq!(
            name_filtered_result.notices()[0].code,
            "LIX_HISTORY_NON_IDENTITY_FILTER"
        );
    }

    #[tokio::test]
    async fn execute_sql_reads_file_history_view_from_history_context() {
        let (session, head_commit_id) = setup_engine_history_fixture()
            .await
            .expect("history fixture should initialize");
        let result = session
            .execute(
                &format!(
                    "SELECT id, path, data, hidden, lixcol_start_commit_id, lixcol_depth \
             FROM lix_file_history \
             WHERE id = 'file-a' \
               AND lixcol_start_commit_id = '{head_commit_id}' \
               AND data IS NOT NULL \
             ORDER BY lixcol_depth",
                ),
                &[],
            )
            .await
            .expect("sql2 execute should read file history through real engine context");
        assert!(
            result.notices().is_empty(),
            "identity-filtered file history should not emit soft notices"
        );
        let (columns, rows) = rows_from_execute_result(result);

        assert_eq!(
            columns,
            vec![
                "id",
                "path",
                "data",
                "hidden",
                "lixcol_start_commit_id",
                "lixcol_depth",
            ]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("file-a".to_string()));
        assert_eq!(rows[0][1], Value::Text("/docs/readme.md".to_string()));
        assert_eq!(rows[0][2], Value::Blob(b"hello".to_vec()));
        assert_eq!(rows[0][3], Value::Boolean(false));
        assert_eq!(rows[0][4], Value::Text(head_commit_id.clone()));
        assert!(matches!(rows[0][5], Value::Integer(_)));

        let path_filtered_result = session
            .execute(
                &format!(
                    "SELECT id \
             FROM lix_file_history \
             WHERE path = '/docs/readme.md' \
               AND lixcol_start_commit_id = '{head_commit_id}'"
                ),
                &[],
            )
            .await
            .expect("sql2 execute should attach notices to path-filtered file history reads");
        assert_eq!(path_filtered_result.notices().len(), 1);
        assert_eq!(
            path_filtered_result.notices()[0].code,
            "LIX_HISTORY_NON_IDENTITY_FILTER"
        );
    }

    #[tokio::test]
    async fn execute_sql_rejects_writes_to_history_views_before_planning() {
        for sql in [
            "DELETE FROM lix_state_history",
            "DELETE FROM LIX_STATE_HISTORY",
        ] {
            let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
            let live_state = Arc::new(DummyLiveStateReader);
            let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
            let mut ctx = DummySqlWriteExecutionContext {
                active_branch_id: "branch-a",
                blob_reader,
                live_state,
                staged_writes,
                schema_definitions: vec![],
            };

            let error = execute_write_sql(&mut ctx, sql, &[])
                .await
                .expect_err("history views are read-only");

            assert_eq!(error.code, LixError::CODE_READ_ONLY, "{sql}");
            assert_eq!(
                error.message, "DML cannot write read-only SQL table 'lix_state_history'",
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_values_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (\
		         entity_pk, schema_key, file_id, snapshot_content, metadata, global, untracked\
		         ) VALUES (\
		         lix_json('[\"entity-1\"]'), 'lix_key_value', NULL, '{\"key\":\"hello\",\"value\":\"world\"}', '{\"source\":\"sql\"}', false, false\
		         )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state VALUES should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-1\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"sql\"}"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_stages_explicit_branch_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, global\
             ) VALUES (\
             lix_json('[\"entity-b\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"branch-b\"}', 'branch-b', false\
             )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state_by_branch should stage explicit-branch write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-b\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(!rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"branch-b\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_global_branch_defaults_global_true() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id\
             ) VALUES (\
             lix_json('[\"entity-global\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"global\"}', 'global'\
             )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state_by_branch with global branch should stage global row");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-global\"]");
        assert_eq!(rows[0].branch_id, GLOBAL_BRANCH_ID);
        assert!(rows[0].global);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_parameterized_global_branch_defaults_global_true(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id\
             ) VALUES (\
             lix_json('[\"entity-global-param\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"global-param\"}', $1\
             )",
            &[Value::Text(GLOBAL_BRANCH_ID.to_string())],
        )
        .await
        .expect("parameterized global branch should stage global row");

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-global-param\"]");
        assert_eq!(rows[0].branch_id, GLOBAL_BRANCH_ID);
        assert!(rows[0].global);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_parameterized_branch_stays_non_global() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id\
             ) VALUES (\
             lix_json('[\"entity-branch-param\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"branch-param\"}', $1\
             )",
            &[Value::Text("branch-b".to_string())],
        )
        .await
        .expect("parameterized non-global branch should stage non-global row");

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-branch-param\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(!rows[0].global);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_parameterized_multi_branch_global_false() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, global\
             ) VALUES (\
             lix_json('[\"entity-branch-param-b\"]'), 'lix_key_value', '{\"key\":\"hello-b\",\"value\":\"branch-b\"}', $1, false\
             ), (\
             lix_json('[\"entity-branch-param-c\"]'), 'lix_key_value', '{\"key\":\"hello-c\",\"value\":\"branch-c\"}', $2, false\
             )",
            &[
                Value::Text("branch-b".to_string()),
                Value::Text("branch-c".to_string()),
            ],
        )
        .await
        .expect("all-non-global parameterized branches should be accepted");

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.branch_id == "branch-b"));
        assert!(rows.iter().any(|row| row.branch_id == "branch-c"));
        assert!(rows.iter().all(|row| !row.global));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_parameterized_global_selector() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, global\
             ) VALUES (\
             lix_json('[\"entity-branch-param-global-param\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"branch-param\"}', $1, $2\
             )",
            &[Value::Text("branch-b".to_string()), Value::Boolean(false)],
        )
        .await
        .expect("parameterized global=false selector should stage non-global row");

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(!rows[0].global);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_rejects_parameterized_global_null_global_branch(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, global\
             ) VALUES (\
             lix_json('[\"entity-global-param-null\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"global-param-null\"}', $1, $2\
             )",
            &[Value::Text(GLOBAL_BRANCH_ID.to_string()), Value::Null],
        )
        .await
        .expect_err("explicit parameterized NULL global selector should be rejected");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error
            .message
            .contains("global selectors must be boolean parameters"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_rejects_parameterized_global_false_global_branch(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, global\
             ) VALUES (\
             lix_json('[\"entity-global-param-false\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"global-param-false\"}', $1, false\
             )",
            &[Value::Text(GLOBAL_BRANCH_ID.to_string())],
        )
        .await
        .expect_err("global=false cannot target parameterized global branch");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = false with global branch_id"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_by_branch_rejects_parameterized_global_true_non_global_branch(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, global\
             ) VALUES (\
             lix_json('[\"entity-branch-param-true\"]'), 'lix_key_value', '{\"key\":\"hello\",\"value\":\"branch-param-true\"}', $1, true\
             )",
            &[Value::Text("branch-b".to_string())],
        )
        .await
        .expect_err("global=true cannot target parameterized non-global branch");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = true with non-global branch_id"));
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_by_branch_rejects_parameterized_global_mixed_branches() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let mut branch_b_row = live_lix_state_row("entity-b", Some("{\"source\":\"branch\"}"));
        branch_b_row.branch_id = "branch-b".to_string();
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                global_lix_state_row("entity-global", Some("{\"source\":\"global\"}")),
                branch_b_row,
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state_by_branch \
             SET metadata = '{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}' \
             WHERE branch_id IN ($1, $2) AND schema_key = 'lix_key_value'",
            &[
                Value::Text(GLOBAL_BRANCH_ID.to_string()),
                Value::Text("branch-b".to_string()),
            ],
        )
        .await
        .expect_err("parameterized UPDATE should reject mixed global/non-global scopes");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot mix global and branch-specific rows"));
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_by_branch_rejects_parameterized_global_mixed_branches() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let mut branch_b_row = live_lix_state_row("entity-b", Some("{\"source\":\"branch\"}"));
        branch_b_row.branch_id = "branch-b".to_string();
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                global_lix_state_row("entity-global", Some("{\"source\":\"global\"}")),
                branch_b_row,
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_state_by_branch \
             WHERE branch_id IN ($1, $2) AND schema_key = 'lix_key_value'",
            &[
                Value::Text(GLOBAL_BRANCH_ID.to_string()),
                Value::Text("branch-b".to_string()),
            ],
        )
        .await
        .expect_err("parameterized DELETE should reject mixed global/non-global scopes");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot mix global and branch-specific rows"));
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_by_branch_parameterized_conjunctive_mismatch_is_noop() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let mut branch_b_row = live_lix_state_row("entity-b", Some("{\"source\":\"branch\"}"));
        branch_b_row.branch_id = "branch-b".to_string();
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                global_lix_state_row("entity-global", Some("{\"source\":\"global\"}")),
                branch_b_row,
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_state_by_branch \
             WHERE branch_id = $1 AND branch_id = $2 AND schema_key = 'lix_key_value'",
            &[
                Value::Text(GLOBAL_BRANCH_ID.to_string()),
                Value::Text("branch-b".to_string()),
            ],
        )
        .await
        .expect("conjunctive parameterized branch mismatch should be a no-op");

        assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
        assert!(staged_writes
            .lock()
            .expect("staged writes lock")
            .deltas
            .is_empty());
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_by_branch_parameterized_null_branch_is_noop() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![global_lix_state_row(
                "entity-global",
                Some("{\"source\":\"global\"}"),
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_state_by_branch \
             WHERE branch_id = $1 AND schema_key = 'lix_key_value'",
            &[Value::Null],
        )
        .await
        .expect("NULL parameterized branch predicate should be a no-op");

        assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
        assert!(staged_writes
            .lock()
            .expect("staged writes lock")
            .deltas
            .is_empty());
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_by_branch_rejects_parameterized_global_true_non_global_predicate(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let mut branch_b_row = live_lix_state_row("entity-b", Some("{\"source\":\"branch\"}"));
        branch_b_row.branch_id = "branch-b".to_string();
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![branch_b_row],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state_by_branch \
             SET metadata = '{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}' \
             WHERE branch_id = $1 AND global = true AND schema_key = 'lix_key_value'",
            &[Value::Text("branch-b".to_string())],
        )
        .await
        .expect_err("global=true predicate cannot target parameterized non-global branch");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = true with non-global branch_id"));
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_by_branch_rejects_parameterized_global_predicate_true_non_global_branch(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let mut branch_b_row = live_lix_state_row("entity-b", Some("{\"source\":\"branch\"}"));
        branch_b_row.branch_id = "branch-b".to_string();
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![branch_b_row],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state_by_branch \
             SET metadata = '{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}' \
             WHERE branch_id = $1 AND global = $2 AND schema_key = 'lix_key_value'",
            &[Value::Text("branch-b".to_string()), Value::Boolean(true)],
        )
        .await
        .expect_err("global=true parameter cannot target parameterized non-global branch");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = true with non-global branch_id"));
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_by_branch_rejects_parameterized_global_false_global_predicate(
    ) {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![global_lix_state_row(
                "entity-global",
                Some("{\"source\":\"global\"}"),
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_state_by_branch \
             WHERE branch_id = $1 AND global = false AND schema_key = 'lix_key_value'",
            &[Value::Text(GLOBAL_BRANCH_ID.to_string())],
        )
        .await
        .expect_err("global=false predicate cannot target parameterized global branch");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = false with global branch_id"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_defaults_global_and_untracked_to_false() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
			&mut ctx,
			"INSERT INTO lix_state (\
	         entity_pk, schema_key, file_id, snapshot_content, metadata\
	         ) VALUES (\
	         lix_json('[\"entity-defaults\"]'), 'lix_key_value', NULL, '{\"key\":\"hello\",\"value\":\"defaults\"}', NULL\
	         )",
			&[],
		)
        .await
        .expect("INSERT INTO lix_state should default bookkeeping flags");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-defaults\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_casts_values_to_target_columns() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (\
             entity_pk, schema_key, snapshot_content\
             ) VALUES (\
             lix_json('[\"entity-numeric\"]'), 'lix_key_value', -1\
             )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state should cast values to target columns");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-numeric\"]");
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("-1"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_select_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (\
		         entity_pk, schema_key, file_id, snapshot_content, metadata, global, untracked\
		         ) \
	         SELECT \
	         lix_json('[\"entity-from-select\"]') AS entity_pk, \
	         'lix_key_value' AS schema_key, \
	         NULL AS file_id, \
             '{\"key\":\"hello\",\"value\":\"from-select\"}' AS snapshot_content, \
             '{\"source\":\"select\"}' AS metadata, \
             false AS global, \
             false AS untracked",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state SELECT should stage write rows");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-from-select\"]");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"from-select\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"select\"}"));
        assert_eq!(rows[0].branch_id, "branch-a");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_select_binds_params_positionally_and_casts() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (schema_key, entity_pk, snapshot_content) \
             SELECT 'lix_key_value' AS looks_like_entity_pk, \
                    lix_json($1) AS looks_like_schema_key, \
                    -2 AS looks_like_metadata",
            &[Value::Text("[\"entity-select-param\"]".to_string())],
        )
        .await
        .expect("INSERT INTO lix_state SELECT should bind params and map outputs by ordinal");

        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-select-param\"]");
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("-2"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_select_applies_read_validation() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
             SELECT entity_pk, schema_key, snapshot_content \
             FROM lix_state \
             WHERE entity_pk = '[\"state-latest\"]'",
            &[],
        )
        .await
        .expect_err("query source should apply read JSON predicate validation");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(
            error.hint().is_some_and(|hint| hint.contains("lix_json")),
            "expected lix_json hint: {error}"
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_select_validates_json_join_predicate_params() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_lix_state_row("state-latest", None)],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
             SELECT left_state.entity_pk, left_state.schema_key, left_state.snapshot_content \
             FROM lix_state AS left_state \
             JOIN lix_state AS right_state \
             ON left_state.entity_pk = $1",
            &[Value::Text("[\"state-latest\"]".to_string())],
        )
        .await
        .expect_err("query source join predicates should apply JSON param validation");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(
            error.hint().is_some_and(|hint| hint.contains("lix_json")),
            "expected lix_json hint: {error}"
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_file_select_without_data_stages_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file (id, path) SELECT 'file-from-select', '/docs/from-select.txt'",
            &[],
        )
        .await
        .expect("lix_file INSERT SELECT without data should execute");

        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"file-from-select\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_entity_by_branch_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema_by_branch (\
	     lixcol_entity_pk, lixcol_branch_id, value\
	     ) VALUES (lix_json('[\"entity-c\"]'), 'branch-b', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO entity by-branch surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-c\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_entity_by_branch_accepts_parameterized_branch_id() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema_by_branch (\
             lixcol_entity_pk, lixcol_branch_id, value\
             ) VALUES (lix_json('[\"entity-c\"]'), $1, 'C')",
            &[Value::Text("branch-b".to_string())],
        )
        .await
        .expect("parameterized by-branch entity insert should stage write");

        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-c\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_entity_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (lixcol_entity_pk, value) \
	     VALUES (lix_json('[\"entity-c\"]'), 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO active entity surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-c\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_entity_rejects_missing_active_head() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "missing-branch",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let error = execute_write_sql(
            &mut ctx,
            "INSERT INTO test_state_schema (lixcol_entity_pk, value) \
             VALUES (lix_json('[\"entity-c\"]'), 'C')",
            &[],
        )
        .await
        .expect_err("missing active head should fail before staging");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert!(error
            .message
            .contains("branch 'missing-branch' was not found"));
    }

    #[tokio::test]
    async fn execute_sql_noop_active_entity_write_rejects_missing_active_head() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "missing-branch",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        for sql in [
            "UPDATE test_state_schema SET value = 'D' WHERE false",
            "DELETE FROM test_state_schema WHERE false",
        ] {
            let error = execute_write_sql(&mut ctx, sql, &[])
                .await
                .expect_err("missing active head should fail even for no-op writes");

            assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND, "{sql}");
            assert!(
                error
                    .message
                    .contains("branch 'missing-branch' was not found"),
                "{sql}: {}",
                error.message
            );
        }
    }

    #[tokio::test]
    async fn execute_sql_insert_into_directory_by_branch_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_directory_by_branch (\
             id, parent_id, name, hidden, lixcol_branch_id\
             ) VALUES ('dir-docs', NULL, 'docs', false, 'branch-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory_by_branch should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"dir-docs\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"hidden\":false,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_directory_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"dir-docs\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_update_directory_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "branch-a", None, "docs", false),
                live_directory_row("dir-guides", "branch-a", Some("dir-docs"), "guides", false),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_directory \
             SET hidden = true, lixcol_metadata = '{\"source\":\"directory-update\"}' \
             WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect("UPDATE lix_directory should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"dir-docs\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"hidden\":true,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"directory-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_directory_rejects_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_directory_row(
                "dir-docs", "branch-a", None, "docs", false,
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_directory SET path = '/renamed/' WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect_err("path should remain read-only");

        assert!(
            error.message.contains("not writable")
                || error.message.contains("read-only column 'path'"),
            "unexpected error: {error:?}"
        );
        assert!(staged_writes
            .lock()
            .expect("staged writes lock")
            .deltas
            .is_empty());
    }

    #[tokio::test]
    async fn execute_sql_delete_directory_by_branch_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "branch-a", None, "docs", false),
                live_directory_row("dir-guides", "branch-b", Some("dir-docs"), "guides", false),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_directory_by_branch \
             WHERE id = 'dir-guides' AND lixcol_branch_id = 'branch-b'",
            &[],
        )
        .await
        .expect("DELETE lix_directory_by_branch should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"dir-guides\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_by_branch_stages_descriptor_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file_by_branch (\
             id, directory_id, name, hidden, lixcol_branch_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme.md', false, 'branch-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_branch should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"file-readme\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme.md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_file_defaults_active_branch() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file (id, directory_id, name, hidden) \
             VALUES ('file-readme', 'dir-docs', 'readme.md', false)",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"file-readme\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(!rows[0].global);
        assert!(!rows[0].untracked);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_with_data_stages_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateReader);
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "INSERT INTO lix_file_by_branch (\
             id, directory_id, name, hidden, data, lixcol_branch_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme.md', false, X'4142', 'branch-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_branch should stage descriptor and data writes");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_rows.len(), 1);
        assert_eq!(descriptor_rows[0].entity_pk, "[\"file-readme\"]");
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_pk, "[\"file-readme\"]");
        assert_eq!(blob_ref_rows[0].file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref_rows[0].branch_id, "branch-b");
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 2);
        assert!(snapshot["blob_hash"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "branch-a", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "branch-a",
                    Some("dir-docs"),
                    "readme.md",
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "branch-a",
                    Some("dir-docs"),
                    "guide.md",
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file \
             SET name = 'readme-updated.txt', hidden = true, lixcol_metadata = '{\"source\":\"file-update\"}' \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"file-readme\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme-updated.txt");
        assert_eq!(snapshot["hidden"], true);
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"file-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_data_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "branch-a", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "branch-a",
                    Some("dir-docs"),
                    "readme.md",
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file SET data = X'4142' WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage data write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        assert!(overlay
            .visible_semantic_rows(false, "lix_file_descriptor")
            .is_empty());
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_pk, "[\"file-readme\"]");
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 2);
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "branch-a", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "branch-a",
                    Some("dir-docs"),
                    "readme.md",
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_file SET path = '/docs/renamed.md' WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("path update should stage descriptor rewrite");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[tokio::test]
    async fn execute_sql_delete_file_by_branch_stages_descriptor_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_directory_row("dir-docs", "branch-a", None, "docs", false),
                live_directory_row("dir-docs", "branch-b", None, "docs", false),
                live_file_row(
                    "file-readme",
                    "branch-a",
                    Some("dir-docs"),
                    "readme.md",
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "branch-b",
                    Some("dir-docs"),
                    "guide.md",
                    false,
                ),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_file_by_branch \
             WHERE id = 'file-guide' AND lixcol_branch_id = 'branch-b'",
            &[],
        )
        .await
        .expect("DELETE lix_file_by_branch should stage descriptor tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"file-guide\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_entity_surface_stages_rewritten_snapshot() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_entity_row("entity-a", "branch-a", "A"),
                live_entity_row("entity-b", "branch-a", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE test_state_schema \
             SET value = 'updated', lixcol_metadata = '{\"source\":\"entity-update\"}' \
             WHERE value = 'A'",
            &[],
        )
        .await
        .expect("UPDATE entity surface should stage rewritten row");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-a\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"entity-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_entity_by_branch_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_entity_row("entity-a", "branch-a", "A"),
                live_entity_row("entity-b", "branch-b", "B"),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM test_state_schema_by_branch \
             WHERE lixcol_branch_id = $1",
            &[Value::Text("branch-b".to_string())],
        )
        .await
        .expect("parameterized DELETE entity by-branch surface should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-b\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_stages_rewritten_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"match\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET snapshot_content = '{\"key\":\"hello\",\"value\":\"updated\"}', \
                 metadata = '{\"schema_key\":\"lix_key_value\"}' \
             WHERE metadata = lix_json('{ \"source\" : \"match\" }')",
            &[],
        )
        .await
        .expect("UPDATE lix_state should stage rewritten rows");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-1\"]");
        assert_eq!(rows[0].branch_id, "branch-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"schema_key\":\"lix_key_value\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_complex_predicate_declines_fast_path_and_executes() {
        let sql = "UPDATE lix_state \
             SET snapshot_content = '{\"key\":\"hello\",\"value\":\"updated\"}', \
                 metadata = '{\"schema_key\":\"lix_key_value\"}' \
             WHERE metadata = lix_json('{ \"source\" : \"match\" }')";
        let statement = parse_statement(sql).expect("SQL parses");
        let bound_write = bind_statement(&statement, &[], "branch-a").expect("SQL binds");
        let plan = plan_write(bound_write).expect("write plans");
        assert_eq!(
            crate::sql2::optimize::simple_write::try_make_fast_write_plan(&plan)
                .expect("fast optimization should not fail"),
            None
        );

        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_lix_state_row(
                "entity-1",
                Some("{\"source\":\"match\"}"),
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(&mut ctx, sql, &[])
            .await
            .expect("declined fast path should fall through to reference write execution");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
        assert_eq!(
            staged_writes
                .lock()
                .expect("staged writes lock")
                .deltas
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn bound_public_write_supports_only_supported_entity_shapes() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let supported_plan = create_write_logical_plan(
            &mut ctx,
            "UPDATE test_state_schema SET value = 'updated' WHERE value = 'A'",
        )
        .await
        .expect("supported entity update should plan");
        let crate::sql2::exec::SqlLogicalPlan::Write(supported_plan) = supported_plan else {
            panic!("expected write plan");
        };
        assert!(
            crate::sql2::exec::bound_public_write::supports_bound_public_write(
                &supported_plan.plan
            )
        );

        let mut unsupported_plan = supported_plan.plan.clone();
        unsupported_plan.bound.op = crate::sql2::bind::write::BoundWriteOp::Insert;
        assert!(
            !crate::sql2::exec::bound_public_write::supports_bound_public_write(&unsupported_plan)
        );
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_preserves_json_param_metadata() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"match\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET metadata = lix_json('{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}') \
             WHERE entity_pk = $1",
            &[Value::Json(json!(["entity-1"]))],
        )
        .await
        .expect("UPDATE lix_state should preserve JSON parameter metadata");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-1\"]");
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_accepts_text_param_for_json_predicate() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"match\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET metadata = lix_json('{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}') \
             WHERE entity_pk = $1",
            &[Value::Text("[\"entity-1\"]".to_string())],
        )
        .await
        .expect("UPDATE lix_state should allow text params in JSON predicates");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-1\"]");
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_casts_assignments_to_target_columns() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_lix_state_row(
                "entity-1",
                Some("{\"source\":\"match\"}"),
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET snapshot_content = -1 \
             WHERE entity_pk = lix_json('[\"entity-1\"]')",
            &[],
        )
        .await
        .expect("UPDATE lix_state should cast assignments to target columns");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-1\"]");
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("-1"));
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_rejects_extra_parameters() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_lix_state_row(
                "entity-1",
                Some("{\"source\":\"match\"}"),
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET metadata = lix_json('{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}') \
             WHERE schema_key = $1",
            &[
                Value::Text("lix_key_value".to_string()),
                Value::Text("ignored".to_string()),
            ],
        )
        .await
        .expect_err("extra write params should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(
            error.message,
            "SQL expected 1 parameter(s), but 2 parameter(s) were provided"
        );
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_by_branch_stages_explicit_branch_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let mut branch_b_row = live_lix_state_row("entity-b", Some("{\"source\":\"match\"}"));
        branch_b_row.branch_id = "branch-b".to_string();
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-a", Some("{\"source\":\"skip\"}")),
                branch_b_row,
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state_by_branch \
             SET metadata = '{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}' \
             WHERE branch_id = 'branch-b' AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("UPDATE lix_state_by_branch should stage explicit-branch rows");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-b\"]");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"schema_key\":\"lix_key_value\",\"source\":\"updated\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_global_predicate_stages_global_row() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-active", Some("{\"source\":\"active\"}")),
                global_lix_state_row("entity-global", Some("{\"source\":\"global\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET metadata = '{\"schema_key\":\"lix_key_value\",\"source\":\"updated-global\"}' \
             WHERE global = true AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("UPDATE lix_state global predicate should stage global rows");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-global\"]");
        assert_eq!(rows[0].branch_id, GLOBAL_BRANCH_ID);
        assert!(rows[0].global);
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"schema_key\":\"lix_key_value\",\"source\":\"updated-global\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_by_branch_global_predicate_stages_global_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-active", Some("{\"source\":\"active\"}")),
                global_lix_state_row("entity-global", Some("{\"source\":\"global\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(
            &mut ctx,
            "DELETE FROM lix_state_by_branch \
             WHERE global = true AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("DELETE lix_state_by_branch global predicate should stage global tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, "[\"entity-global\"]");
        assert_eq!(rows[0].branch_id, GLOBAL_BRANCH_ID);
        assert!(rows[0].global);
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_by_branch_false_predicate_is_noop() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![live_lix_state_row(
                "entity-active",
                Some("{\"source\":\"active\"}"),
            )],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result =
            execute_write_sql(&mut ctx, "DELETE FROM lix_state_by_branch WHERE false", &[])
                .await
                .expect("empty by-branch scope should execute as a no-op");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(0)]]);
        assert!(staged_writes
            .lock()
            .expect("staged writes lock")
            .deltas
            .is_empty());
    }

    #[tokio::test]
    async fn execute_sql_delete_unsupported_target_contradiction_still_falls_back_and_errors() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let plan = create_write_logical_plan(
            &mut ctx,
            "DELETE FROM test_state_schema WHERE value = 'A' AND value = 'B'",
        )
        .await
        .expect("registered entity write should bind before reference writer selection");
        let error = crate::sql2::execute_write_logical_plan_with_mode(
            &mut ctx,
            plan,
            &[],
            crate::sql2::WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("unsupported reference writer target should not become a fast no-op");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("currently supports only lix_state writes"));
    }

    #[tokio::test]
    async fn execute_sql_delete_unsupported_target_false_predicate_still_errors() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let plan = create_write_logical_plan(&mut ctx, "DELETE FROM test_state_schema WHERE false")
            .await
            .expect("registered entity write should bind before reference writer selection");
        let error = crate::sql2::execute_write_logical_plan_with_mode(
            &mut ctx,
            plan,
            &[],
            crate::sql2::WriteExecutorMode::ForceDataFusion,
        )
        .await
        .expect_err("unsupported target with empty scope should not become a no-op");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("currently supports only lix_state writes"));
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_contradiction_still_validates_json_predicates() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET metadata = lix_json('{}') \
             WHERE metadata = 'not-json-typed' \
               AND schema_key = 'a' \
               AND schema_key = 'b'",
            &[],
        )
        .await
        .expect_err("column contradiction should not bypass JSON predicate validation");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error
            .message
            .contains("JSON columns can only be compared with JSON expressions"));
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_false_predicate_still_validates_json_predicates() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state \
             SET metadata = lix_json('{}') \
             WHERE false \
               AND metadata = 'not-json-typed'",
            &[],
        )
        .await
        .expect_err("false predicate should not bypass JSON predicate validation");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error
            .message
            .contains("JSON columns can only be compared with JSON expressions"));
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_by_branch_empty_scope_still_validates_json_predicates() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader { rows: vec![] });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes,
            schema_definitions: vec![],
        };

        let error = execute_write_sql(
            &mut ctx,
            "UPDATE lix_state_by_branch \
             SET metadata = lix_json('{}') \
             WHERE branch_id = 'branch-a' \
               AND branch_id = 'branch-b' \
               AND metadata = 'not-json-typed'",
            &[],
        )
        .await
        .expect_err("empty branch scope should not bypass JSON predicate validation");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error
            .message
            .contains("JSON columns can only be compared with JSON expressions"));
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_without_where_stages_all_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"one\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"two\"}")),
            ],
        });
        let staged_writes = Arc::new(Mutex::new(CapturingStagedWrites::default()));
        let mut ctx = DummySqlWriteExecutionContext {
            active_branch_id: "branch-a",
            blob_reader,
            live_state,
            staged_writes: Arc::clone(&staged_writes),
            schema_definitions: vec![],
        };

        let result = execute_write_sql(&mut ctx, "DELETE FROM lix_state", &[])
            .await
            .expect("DELETE FROM lix_state should follow DataFusion delete-all semantics");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);

        let staged_writes = staged_writes.lock().expect("staged writes lock");
        assert_eq!(staged_writes.deltas.len(), 1);
        let overlay = staged_writes.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.tombstone));
        assert!(rows.iter().all(|row| row.snapshot_content.is_none()));
        assert!(rows.iter().any(|row| row.entity_pk == "[\"entity-1\"]"));
        assert!(rows.iter().any(|row| row.entity_pk == "[\"entity-2\"]"));
    }

    async fn setup_sql2_state_fixture() -> Result<DummySqlExecutionContext<'static>, crate::LixError>
    {
        let schema_definition = json!({
            "x-lix-key": "test_state_schema",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        });
        Ok(DummySqlExecutionContext {
            active_branch_id: "branch-a",
            blob_reader: Arc::new(StaticBlobReader {
                bytes: vec![0x41, 0x42],
            }),
            live_state: Arc::new(RowsLiveStateReader {
                rows: vec![
                    live_entity_row("entity-a", "branch-a", "A"),
                    live_entity_row("entity-b", "branch-b", "B"),
                    live_directory_row("dir-docs", "branch-a", None, "docs", false),
                    live_file_row("file-a", "branch-a", Some("dir-docs"), "readme.md", false),
                    live_blob_ref_row("file-a", "branch-a", &[0x41, 0x42]),
                ],
            }),
            schema_definitions: vec![schema_definition],
        })
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-execute-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn execute_sql_reads_lix_state_by_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_pk, branch_id, snapshot_content, commit_id \
                     FROM lix_state_by_branch \
                     WHERE branch_id = 'branch-b' AND schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state_by_branch");

                assert_eq!(
                    result.columns,
                    vec!["entity_pk", "branch_id", "snapshot_content", "commit_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Json(json!(["entity-b"])));
                assert_eq!(result.rows[0][1], Value::Text("branch-b".to_string()));
                assert_eq!(result.rows[0][2], Value::Json(json!({"value": "B"})));
                match &result.rows[0][3] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected non-null commit_id text, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_sql_supports_broad_lix_state_by_branch_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_pk FROM lix_state_by_branch WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("broad by-branch read should succeed");

                assert!(
					result.rows.iter().any(|row| row[0] == Value::Json(json!(["entity-a"])))
						&& result.rows.iter().any(|row| row[0] == Value::Json(json!(["entity-b"]))),
					"expected broad by-branch read to include rows from multiple visible branches: {:?}",
					result.rows
				);
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_state_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_pk, snapshot_content \
                     FROM lix_state \
                     WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state");

                assert_eq!(result.columns, vec!["entity_pk", "snapshot_content"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Json(json!(["entity-a"])));
                assert_eq!(result.rows[0][1], Value::Json(json!({"value": "A"})));
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_view_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_entity_pk \
                     FROM test_state_schema",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity view");

                assert_eq!(result.columns, vec!["value", "lixcol_entity_pk"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("A".to_string()));
                assert_eq!(result.rows[0][1], Value::Json(json!(["entity-a"])));
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_by_branch_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_branch_id \
                     FROM test_state_schema_by_branch \
                     WHERE lixcol_branch_id = 'branch-b'",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity by-branch view");

                assert_eq!(result.columns, vec!["value", "lixcol_branch_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("B".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("branch-b".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_by_branch_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, lixcol_branch_id \
                     FROM lix_directory_by_branch \
                     WHERE id = 'dir-docs' AND lixcol_branch_id = 'branch-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory_by_branch");

                assert_eq!(result.columns, vec!["path", "name", "lixcol_branch_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs/".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("branch-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name \
                     FROM lix_directory \
                     WHERE id = 'dir-docs'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory");

                assert_eq!(result.columns, vec!["path", "name"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs/".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_by_branch_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, data, lixcol_branch_id \
                     FROM lix_file_by_branch \
                     WHERE id = 'file-a' AND lixcol_branch_id = 'branch-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file_by_branch");

                assert_eq!(
                    result.columns,
                    vec!["path", "name", "data", "lixcol_branch_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme.md".to_string()));
                assert_eq!(result.rows[0][2], Value::Blob(vec![0x41, 0x42]));
                assert_eq!(result.rows[0][3], Value::Text("branch-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_from_active_branch() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let ctx = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, data \
                     FROM lix_file \
                     WHERE id = 'file-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file");

                assert_eq!(result.columns, vec!["path", "name", "data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme.md".to_string()));
                assert_eq!(result.rows[0][2], Value::Blob(vec![0x41, 0x42]));
            })
        });
    }
}
