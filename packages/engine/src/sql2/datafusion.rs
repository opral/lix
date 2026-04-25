use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion::{
    datasource::{MemTable, TableType},
    physical_plan::SendableRecordBatchStream,
};
use futures_util::{stream, TryStreamExt};
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Expr as SqlExpr, GroupByExpr, Ident, Query as SqlQuery, Select as SqlSelect, SelectFlavor,
    SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, Statement as SqlStatement,
    TableAlias as SqlTableAlias, TableFactor as SqlTableFactor,
    TableWithJoins as SqlTableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tokio::sync::oneshot;

use super::entity_view::{
    PreparedSql2EntityViewPlan, Sql2EntityViewBaseRelation, VARIANT_FIELD_METADATA_KEY,
    VARIANT_FIELD_METADATA_VALUE,
};
use super::filesystem_view::{PreparedSql2FilesystemViewPlan, Sql2FilesystemViewBaseRelation};
use super::udf::{register_sql2_udfs, system_sql2_function_provider};
use crate::binary_cas::BlobDataReader;
use crate::catalog::SurfaceColumnType;
use crate::catalog::{
    open_change_surface_snapshot, open_change_surface_snapshot_with_shared_backend,
    open_version_surface_snapshot, open_version_surface_snapshot_with_shared_backend,
    open_working_changes_surface_snapshot,
    open_working_changes_surface_snapshot_with_shared_backend, ChangeSurfaceColumn,
    ChangeSurfaceFilter, ChangeSurfaceRow, ChangeSurfaceScanRequest, ChangeSurfaceSnapshot,
    VersionSurfaceColumn, VersionSurfaceRow, VersionSurfaceScanRequest, VersionSurfaceSnapshot,
    WorkingChangesSurfaceColumn, WorkingChangesSurfaceFilter, WorkingChangesSurfaceRow,
    WorkingChangesSurfaceScanRequest, WorkingChangesSurfaceSnapshot,
};
use crate::history::{
    CommittedStateHistoryReader, StateHistoryContentMode, StateHistoryLineageScope,
    StateHistoryRequest, StateHistoryRootScope, StateHistoryRow, StateHistoryVersionScope,
};
use crate::live_state::{
    open_state_by_version_snapshot, open_state_by_version_snapshot_with_shared_backend,
    open_visible_state_by_version_snapshot, StateByVersionScanRequest, StateByVersionSnapshot,
    StateSurfaceColumn, StateSurfaceFilter,
};
use crate::sql::diagnostics::sql_unknown_column_error;
use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedSql2ReadArtifact {
    pub(crate) sql: String,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) active_version_id: String,
    pub(crate) surface_names: Vec<String>,
    pub(crate) entity_views: BTreeMap<String, PreparedSql2EntityViewPlan>,
    #[allow(dead_code)]
    pub(crate) filesystem_views: BTreeMap<String, PreparedSql2FilesystemViewPlan>,
}

pub(crate) async fn execute_read_with_backend(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read_with_borrowed_backend(backend, artifact).await?;
    collect_query_result_from_ctx(ctx, artifact, Some(backend)).await
}

pub(crate) async fn execute_read_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read_with_shared_backend(backend.clone(), artifact).await?;
    collect_query_result_from_ctx(ctx, artifact, Some(backend.as_ref())).await
}

async fn collect_query_result_from_ctx(
    ctx: SessionContext,
    artifact: &PreparedSql2ReadArtifact,
    backend: Option<&dyn LixBackend>,
) -> Result<QueryResult, LixError> {
    let sql = normalize_sql2_query_shape(&artifact.sql)?;
    validate_variant_text_coercions(&sql, artifact)?;
    let mut dataframe = ctx
        .sql(&sql)
        .await
        .map_err(|error| datafusion_error_to_lix_error_with_artifact(error, artifact))?;
    if !artifact.bound_parameters.is_empty() {
        dataframe = dataframe
            .with_param_values(
                artifact
                    .bound_parameters
                    .iter()
                    .map(scalar_value_from_lix_value)
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| datafusion_error_to_lix_error_with_artifact(error, artifact))?;
    }
    let result_schema_fields = dataframe.schema().fields().iter().collect::<Vec<_>>();
    let result_columns = result_schema_fields
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let variant_result_columns = result_schema_fields
        .iter()
        .map(|field| {
            field
                .metadata()
                .get(VARIANT_FIELD_METADATA_KEY)
                .is_some_and(|value| value == VARIANT_FIELD_METADATA_VALUE)
        })
        .collect::<Vec<_>>();
    let batches = dataframe
        .collect()
        .await
        .map_err(|error| datafusion_error_to_lix_error_with_artifact(error, artifact))?;
    let mut result = query_result_from_batches(&result_columns, &variant_result_columns, &batches)?;
    if let Some(backend) = backend {
        hydrate_filesystem_blob_columns(backend, artifact, &mut result).await?;
    }
    Ok(result)
}

fn normalize_sql2_query_shape(sql: &str) -> Result<String, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 query parse failed during normalization: {error}"),
        )
    })?;
    if statements.len() != 1 {
        return Ok(sql.to_string());
    }

    let Some(SqlStatement::Query(query)) = statements.get_mut(0) else {
        return Ok(sql.to_string());
    };
    let SqlSetExpr::Select(select) = query.body.as_mut() else {
        return Ok(sql.to_string());
    };
    if !select.from.is_empty() {
        return Ok(sql.to_string());
    }

    select.from.push(single_row_source_table());
    Ok(statements.remove(0).to_string())
}

fn validate_variant_text_coercions(
    sql: &str,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<(), LixError> {
    if artifact.entity_views.is_empty() {
        return Ok(());
    }

    let statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 query parse failed during variant validation: {error}"),
        )
    })?;
    for statement in &statements {
        if let SqlStatement::Query(query) = statement {
            validate_variant_text_coercions_in_query(query, artifact, &BTreeMap::new())?;
        }
    }
    Ok(())
}

fn validate_variant_text_coercions_in_query(
    query: &SqlQuery,
    artifact: &PreparedSql2ReadArtifact,
    outer_scope: &BTreeMap<String, String>,
) -> Result<(), LixError> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            validate_variant_text_coercions_in_query(&cte.query, artifact, outer_scope)?;
        }
    }

    validate_variant_text_coercions_in_set_expr(&query.body, artifact, outer_scope)?;

    if let Some(order_by) = &query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            for order in exprs {
                validate_variant_text_coercions_in_expr(&order.expr, artifact, outer_scope)?;
            }
        }
    }

    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } => {
                if let Some(limit) = limit {
                    validate_variant_text_coercions_in_expr(limit, artifact, outer_scope)?;
                }
                if let Some(offset) = offset {
                    validate_variant_text_coercions_in_expr(&offset.value, artifact, outer_scope)?;
                }
            }
            sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                validate_variant_text_coercions_in_expr(offset, artifact, outer_scope)?;
                validate_variant_text_coercions_in_expr(limit, artifact, outer_scope)?;
            }
        }
    }

    Ok(())
}

fn validate_variant_text_coercions_in_set_expr(
    set_expr: &SqlSetExpr,
    artifact: &PreparedSql2ReadArtifact,
    outer_scope: &BTreeMap<String, String>,
) -> Result<(), LixError> {
    match set_expr {
        SqlSetExpr::Select(select) => {
            let scope = sql2_variant_scope_for_select(select, artifact, outer_scope)?;
            for projection in &select.projection {
                match projection {
                    SqlSelectItem::UnnamedExpr(expr) => {
                        validate_variant_text_coercions_in_expr(expr, artifact, &scope)?
                    }
                    SqlSelectItem::ExprWithAlias { expr, .. } => {
                        validate_variant_text_coercions_in_expr(expr, artifact, &scope)?
                    }
                    _ => {}
                }
            }
            if let Some(selection) = &select.selection {
                validate_variant_text_coercions_in_expr(selection, artifact, &scope)?;
            }
            match &select.group_by {
                GroupByExpr::Expressions(exprs, _modifiers) => {
                    for expr in exprs {
                        validate_variant_text_coercions_in_expr(expr, artifact, &scope)?;
                    }
                }
                GroupByExpr::All(_) => {}
            }
            if let Some(having) = &select.having {
                validate_variant_text_coercions_in_expr(having, artifact, &scope)?;
            }
            if let Some(qualify) = &select.qualify {
                validate_variant_text_coercions_in_expr(qualify, artifact, &scope)?;
            }
            Ok(())
        }
        SqlSetExpr::Query(query) => {
            validate_variant_text_coercions_in_query(query, artifact, outer_scope)
        }
        SqlSetExpr::SetOperation { left, right, .. } => {
            validate_variant_text_coercions_in_set_expr(left, artifact, outer_scope)?;
            validate_variant_text_coercions_in_set_expr(right, artifact, outer_scope)
        }
        _ => Ok(()),
    }
}

fn sql2_variant_scope_for_select(
    select: &SqlSelect,
    artifact: &PreparedSql2ReadArtifact,
    outer_scope: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, LixError> {
    let mut scope = outer_scope.clone();
    for table_with_joins in &select.from {
        collect_variant_scope_from_table_factor(&table_with_joins.relation, artifact, &mut scope)?;
        for join in &table_with_joins.joins {
            collect_variant_scope_from_table_factor(&join.relation, artifact, &mut scope)?;
            match &join.join_operator {
                sqlparser::ast::JoinOperator::Inner(constraint)
                | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                | sqlparser::ast::JoinOperator::RightOuter(constraint)
                | sqlparser::ast::JoinOperator::FullOuter(constraint)
                | sqlparser::ast::JoinOperator::Semi(constraint)
                | sqlparser::ast::JoinOperator::LeftSemi(constraint)
                | sqlparser::ast::JoinOperator::RightSemi(constraint)
                | sqlparser::ast::JoinOperator::Anti(constraint)
                | sqlparser::ast::JoinOperator::LeftAnti(constraint)
                | sqlparser::ast::JoinOperator::RightAnti(constraint)
                | sqlparser::ast::JoinOperator::StraightJoin(constraint) => {
                    validate_variant_text_coercions_in_join_constraint(
                        constraint, artifact, &scope,
                    )?;
                }
                sqlparser::ast::JoinOperator::CrossJoin(_)
                | sqlparser::ast::JoinOperator::CrossApply
                | sqlparser::ast::JoinOperator::OuterApply => {}
                _ => {}
            }
        }
    }
    Ok(scope)
}

fn collect_variant_scope_from_table_factor(
    relation: &SqlTableFactor,
    artifact: &PreparedSql2ReadArtifact,
    scope: &mut BTreeMap<String, String>,
) -> Result<(), LixError> {
    match relation {
        SqlTableFactor::Table { name, alias, .. } => {
            let relation_name = name.to_string();
            if artifact.entity_views.contains_key(&relation_name) {
                scope.insert(relation_name.clone(), relation_name.clone());
                if let Some(alias) = alias {
                    scope.insert(alias.name.value.clone(), relation_name);
                }
            }
        }
        SqlTableFactor::Derived { subquery, .. } => {
            validate_variant_text_coercions_in_query(subquery, artifact, &BTreeMap::new())?;
        }
        SqlTableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            collect_variant_scope_from_table_factor(&table_with_joins.relation, artifact, scope)?;
            if let Some(alias) = alias {
                let relation_name = alias.name.value.clone();
                scope.insert(relation_name.clone(), relation_name);
            }
            for join in &table_with_joins.joins {
                collect_variant_scope_from_table_factor(&join.relation, artifact, scope)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_variant_text_coercions_in_join_constraint(
    constraint: &sqlparser::ast::JoinConstraint,
    artifact: &PreparedSql2ReadArtifact,
    scope: &BTreeMap<String, String>,
) -> Result<(), LixError> {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => {
            validate_variant_text_coercions_in_expr(expr, artifact, scope)
        }
        sqlparser::ast::JoinConstraint::Using(_idents) => Ok(()),
        sqlparser::ast::JoinConstraint::Natural | sqlparser::ast::JoinConstraint::None => Ok(()),
    }
}

fn validate_variant_text_coercions_in_expr(
    expr: &SqlExpr,
    artifact: &PreparedSql2ReadArtifact,
    scope: &BTreeMap<String, String>,
) -> Result<(), LixError> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => {
            if binary_operator_wants_text(op)
                && ((expr_is_string_like_literal(left)
                    && expr_is_bare_variant_column(right, artifact, scope))
                    || (expr_is_string_like_literal(right)
                        && expr_is_bare_variant_column(left, artifact, scope)))
            {
                return Err(variant_text_coercion_error(expr));
            }
            validate_variant_text_coercions_in_expr(left, artifact, scope)?;
            validate_variant_text_coercions_in_expr(right, artifact, scope)
        }
        SqlExpr::Like { expr, pattern, .. }
        | SqlExpr::ILike { expr, pattern, .. }
        | SqlExpr::SimilarTo { expr, pattern, .. }
        | SqlExpr::RLike { expr, pattern, .. } => {
            if expr_is_bare_variant_column(expr, artifact, scope)
                && expr_is_string_like_literal(pattern)
            {
                return Err(variant_text_coercion_error(expr));
            }
            validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
            validate_variant_text_coercions_in_expr(pattern, artifact, scope)
        }
        SqlExpr::InList { expr, list, .. } => {
            if expr_is_bare_variant_column(expr, artifact, scope)
                && list.iter().any(expr_is_string_like_literal)
            {
                return Err(variant_text_coercion_error(expr));
            }
            validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
            for item in list {
                validate_variant_text_coercions_in_expr(item, artifact, scope)?;
            }
            Ok(())
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            if expr_is_bare_variant_column(expr, artifact, scope)
                && (expr_is_string_like_literal(low) || expr_is_string_like_literal(high))
            {
                return Err(variant_text_coercion_error(expr));
            }
            validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
            validate_variant_text_coercions_in_expr(low, artifact, scope)?;
            validate_variant_text_coercions_in_expr(high, artifact, scope)
        }
        SqlExpr::Nested(expr)
        | SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::IsFalse(expr)
        | SqlExpr::IsNotFalse(expr)
        | SqlExpr::IsTrue(expr)
        | SqlExpr::IsNotTrue(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::IsUnknown(expr)
        | SqlExpr::IsNotUnknown(expr) => {
            validate_variant_text_coercions_in_expr(expr, artifact, scope)
        }
        SqlExpr::IsDistinctFrom(left, right) | SqlExpr::IsNotDistinctFrom(left, right) => {
            validate_variant_text_coercions_in_expr(left, artifact, scope)?;
            validate_variant_text_coercions_in_expr(right, artifact, scope)
        }
        SqlExpr::Cast { expr, .. } | SqlExpr::Convert { expr, .. } => {
            validate_variant_text_coercions_in_expr(expr, artifact, scope)
        }
        SqlExpr::Function(function) => {
            match &function.args {
                sqlparser::ast::FunctionArguments::None => {}
                sqlparser::ast::FunctionArguments::Subquery(query) => {
                    validate_variant_text_coercions_in_query(query, artifact, scope)?;
                }
                sqlparser::ast::FunctionArguments::List(args) => {
                    for arg in &args.args {
                        match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(expr),
                            )
                            | sqlparser::ast::FunctionArg::Named {
                                arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                                ..
                            }
                            | sqlparser::ast::FunctionArg::ExprNamed {
                                arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                                ..
                            } => validate_variant_text_coercions_in_expr(expr, artifact, scope)?,
                            _ => {}
                        }
                    }
                    for clause in &args.clauses {
                        match clause {
                            sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) => {
                                for order in order_by {
                                    validate_variant_text_coercions_in_expr(
                                        &order.expr,
                                        artifact,
                                        scope,
                                    )?;
                                }
                            }
                            sqlparser::ast::FunctionArgumentClause::Limit(expr) => {
                                validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(())
        }
        SqlExpr::Position { expr, r#in } => {
            validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
            validate_variant_text_coercions_in_expr(r#in, artifact, scope)
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
            if let Some(from) = substring_from {
                validate_variant_text_coercions_in_expr(from, artifact, scope)?;
            }
            if let Some(for_expr) = substring_for {
                validate_variant_text_coercions_in_expr(for_expr, artifact, scope)?;
            }
            Ok(())
        }
        SqlExpr::Trim {
            expr, trim_what, ..
        } => {
            validate_variant_text_coercions_in_expr(expr, artifact, scope)?;
            if let Some(trim_what) = trim_what {
                validate_variant_text_coercions_in_expr(trim_what, artifact, scope)?;
            }
            Ok(())
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                validate_variant_text_coercions_in_expr(operand, artifact, scope)?;
            }
            for condition in conditions {
                validate_variant_text_coercions_in_expr(&condition.condition, artifact, scope)?;
                validate_variant_text_coercions_in_expr(&condition.result, artifact, scope)?;
            }
            if let Some(else_result) = else_result {
                validate_variant_text_coercions_in_expr(else_result, artifact, scope)?;
            }
            Ok(())
        }
        SqlExpr::Exists { subquery, .. } | SqlExpr::Subquery(subquery) => {
            validate_variant_text_coercions_in_query(subquery, artifact, scope)
        }
        _ => Ok(()),
    }
}

fn binary_operator_wants_text(op: &sqlparser::ast::BinaryOperator) -> bool {
    matches!(
        op,
        sqlparser::ast::BinaryOperator::Eq
            | sqlparser::ast::BinaryOperator::NotEq
            | sqlparser::ast::BinaryOperator::Lt
            | sqlparser::ast::BinaryOperator::LtEq
            | sqlparser::ast::BinaryOperator::Gt
            | sqlparser::ast::BinaryOperator::GtEq
    )
}

fn expr_is_string_like_literal(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Value(value) => matches!(
            value.value,
            SqlValue::SingleQuotedString(_)
                | SqlValue::DollarQuotedString(_)
                | SqlValue::TripleSingleQuotedString(_)
                | SqlValue::TripleDoubleQuotedString(_)
                | SqlValue::SingleQuotedByteStringLiteral(_)
                | SqlValue::DoubleQuotedByteStringLiteral(_)
                | SqlValue::TripleSingleQuotedByteStringLiteral(_)
                | SqlValue::TripleDoubleQuotedByteStringLiteral(_)
                | SqlValue::EscapedStringLiteral(_)
                | SqlValue::UnicodeStringLiteral(_)
        ),
        SqlExpr::Nested(expr) => expr_is_string_like_literal(expr),
        _ => false,
    }
}

fn expr_is_bare_variant_column(
    expr: &SqlExpr,
    artifact: &PreparedSql2ReadArtifact,
    scope: &BTreeMap<String, String>,
) -> bool {
    match expr {
        SqlExpr::Identifier(ident) => {
            variant_column_type_for_reference(None, &ident.value, artifact, scope).is_some()
        }
        SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => {
            variant_column_type_for_reference(
                Some(&idents[0].value),
                &idents[1].value,
                artifact,
                scope,
            )
            .is_some()
        }
        SqlExpr::Nested(expr) => expr_is_bare_variant_column(expr, artifact, scope),
        _ => false,
    }
}

fn variant_column_type_for_reference(
    qualifier: Option<&str>,
    column_name: &str,
    artifact: &PreparedSql2ReadArtifact,
    scope: &BTreeMap<String, String>,
) -> Option<SurfaceColumnType> {
    // Only explicit owner-chosen Variant columns should reach this path.
    // Schema-derived mixed JSON kinds must stay Json and therefore must not
    // participate in variant detection or binary-output behavior.
    if let Some(qualifier) = qualifier {
        let relation_name = scope.get(qualifier)?;
        let plan = artifact.entity_views.get(relation_name)?;
        let column_type = plan.column_types.get(column_name)?;
        return (*column_type == SurfaceColumnType::Variant).then_some(*column_type);
    }

    let mut variant_matches = artifact.entity_views.values().filter_map(|plan| {
        plan.column_types
            .get(column_name)
            .copied()
            .filter(|column_type| *column_type == SurfaceColumnType::Variant)
    });
    let first = variant_matches.next()?;
    variant_matches.next().is_none().then_some(first)
}

fn variant_text_coercion_error(expr: &SqlExpr) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "variant payload expression '{expr}' requires an explicit cast or extraction before it can be used as text"
        ),
    )
    .with_hint("use CAST(... AS TEXT), lix_text_decode(...), or lix_json_extract(...) explicitly")
}

fn single_row_source_table() -> SqlTableWithJoins {
    SqlTableWithJoins {
        relation: SqlTableFactor::Derived {
            lateral: false,
            subquery: Box::new(SqlQuery {
                with: None,
                body: Box::new(SqlSetExpr::Select(Box::new(SqlSelect {
                    select_token: AttachedToken::empty(),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![SqlSelectItem::ExprWithAlias {
                        expr: SqlExpr::Value(SqlValue::Number("1".to_string(), false).into()),
                        alias: Ident::new("__lix_single_row"),
                    }],
                    exclude: None,
                    into: None,
                    from: Vec::new(),
                    lateral_views: Vec::new(),
                    prewhere: None,
                    selection: None,
                    group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
                    cluster_by: Vec::new(),
                    distribute_by: Vec::new(),
                    sort_by: Vec::new(),
                    having: None,
                    named_window: Vec::new(),
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: SelectFlavor::Standard,
                }))),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            }),
            alias: Some(SqlTableAlias {
                explicit: true,
                name: Ident::new("__lix_single_row_source"),
                columns: Vec::new(),
            }),
        },
        joins: Vec::new(),
    }
}

async fn build_session_for_read_with_borrowed_backend(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    register_sql2_udfs(&ctx, system_sql2_function_provider());
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                let snapshot =
                    open_state_by_version_snapshot(backend, &artifact.active_version_id).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::State,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_state_by_version" => {
                let snapshot = open_visible_state_by_version_snapshot(backend).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::StateByVersion,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_state_history" => {
                let rows = load_materialized_state_history_rows(
                    backend,
                    &artifact.active_version_id,
                    &artifact.sql,
                )
                .await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateHistoryProvider::new_materialized(
                        artifact.active_version_id.clone(),
                        rows,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_file" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_borrowed_backend(
                    &ctx,
                    backend,
                    artifact,
                    spec,
                    surface_name,
                )
                .await?;
            }
            "lix_file_by_version" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_borrowed_backend(
                    &ctx,
                    backend,
                    artifact,
                    spec,
                    surface_name,
                )
                .await?;
            }
            "lix_directory" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_borrowed_backend(
                    &ctx,
                    backend,
                    artifact,
                    spec,
                    surface_name,
                )
                .await?;
            }
            "lix_directory_by_version" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_borrowed_backend(
                    &ctx,
                    backend,
                    artifact,
                    spec,
                    surface_name,
                )
                .await?;
            }
            "lix_file_history" | "lix_file_history_by_version" | "lix_directory_history" => {
                if let Some(spec) = artifact.filesystem_views.get(surface_name) {
                    let provider: Arc<dyn TableProvider> =
                        Arc::new(LixStateHistoryProvider::new_materialized(
                            artifact.active_version_id.clone(),
                            load_materialized_state_history_rows(
                                backend,
                                &artifact.active_version_id,
                                &artifact.sql,
                            )
                            .await?,
                        ));
                    register_filesystem_history_view_with_state_history_provider(
                        &ctx,
                        artifact,
                        spec,
                        surface_name,
                        provider,
                    )
                    .await?;
                } else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled filesystem history surface '{surface_name}'"
                        ),
                    ));
                }
            }
            "lix_version" => {
                let snapshot = open_version_surface_snapshot(backend).await?;
                ctx.register_table(surface_name, Arc::new(LixVersionProvider::new(snapshot)))
                    .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_working_changes" => {
                let snapshot =
                    open_working_changes_surface_snapshot(backend, &artifact.active_version_id)
                        .await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixWorkingChangesProvider::new(snapshot)),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_change" => {
                let snapshot = open_change_surface_snapshot(backend).await?;
                ctx.register_table(surface_name, Arc::new(LixChangeProvider::new(snapshot)))
                    .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                let Some(spec) = artifact.entity_views.get(other) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("sql2 phase-2 does not support surface '{other}' yet"),
                    ));
                };
                register_entity_view_with_borrowed_backend(
                    &ctx,
                    backend,
                    artifact,
                    spec,
                    surface_name,
                )
                .await?;
            }
        }
    }
    Ok(ctx)
}

async fn build_session_for_read_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    register_sql2_udfs(&ctx, system_sql2_function_provider());
    let shared_state_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| matches!(surface.as_str(), "lix_state" | "lix_state_by_version"))
        || !artifact.entity_views.is_empty()
        || !artifact.filesystem_views.is_empty()
    {
        Some(open_state_by_version_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    let shared_version_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| surface.as_str() == "lix_version")
    {
        Some(open_version_surface_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    let shared_change_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| surface.as_str() == "lix_change")
    {
        Some(open_change_surface_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    let shared_working_changes_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| surface.as_str() == "lix_working_changes")
    {
        Some(
            open_working_changes_surface_snapshot_with_shared_backend(
                Arc::clone(&backend),
                &artifact.active_version_id,
            )
            .await?,
        )
    } else {
        None
    };
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::State,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_state_snapshot
                                .as_ref()
                                .expect("state surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_state_by_version" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::StateByVersion,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_state_snapshot
                                .as_ref()
                                .expect("state surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_state_history" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateHistoryProvider::new_shared_backend(
                        artifact.active_version_id.clone(),
                        Arc::clone(&backend),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_file" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled shared-backend filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_shared_snapshot(
                    &ctx,
                    backend.as_ref(),
                    artifact,
                    spec,
                    surface_name,
                    Arc::clone(
                        shared_state_snapshot
                            .as_ref()
                            .expect("state surface snapshot should exist"),
                    ),
                )
                .await?;
            }
            "lix_file_by_version" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled shared-backend filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_shared_snapshot(
                    &ctx,
                    backend.as_ref(),
                    artifact,
                    spec,
                    surface_name,
                    Arc::clone(
                        shared_state_snapshot
                            .as_ref()
                            .expect("state surface snapshot should exist"),
                    ),
                )
                .await?;
            }
            "lix_directory" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled shared-backend filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_shared_snapshot(
                    &ctx,
                    backend.as_ref(),
                    artifact,
                    spec,
                    surface_name,
                    Arc::clone(
                        shared_state_snapshot
                            .as_ref()
                            .expect("state surface snapshot should exist"),
                    ),
                )
                .await?;
            }
            "lix_directory_by_version" => {
                let Some(spec) = artifact.filesystem_views.get(surface_name) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled shared-backend filesystem surface '{surface_name}'"
                        ),
                    ));
                };
                register_filesystem_view_with_shared_snapshot(
                    &ctx,
                    backend.as_ref(),
                    artifact,
                    spec,
                    surface_name,
                    Arc::clone(
                        shared_state_snapshot
                            .as_ref()
                            .expect("state surface snapshot should exist"),
                    ),
                )
                .await?;
            }
            "lix_file_history" | "lix_file_history_by_version" | "lix_directory_history" => {
                if let Some(spec) = artifact.filesystem_views.get(surface_name) {
                    let provider: Arc<dyn TableProvider> =
                        Arc::new(LixStateHistoryProvider::new_materialized(
                            artifact.active_version_id.clone(),
                            backend
                                .load_committed_state_history_rows(&state_history_request(
                                    &artifact.active_version_id,
                                    &state_history_route_from_sql(&artifact.sql)?,
                                ))
                                .await?,
                        ));
                    register_filesystem_history_view_with_state_history_provider(
                        &ctx,
                        artifact,
                        spec,
                        surface_name,
                        provider,
                    )
                    .await?;
                } else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 does not support uncompiled shared-backend filesystem history surface '{surface_name}'"
                        ),
                    ));
                }
            }
            "lix_version" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixVersionProvider::new(Arc::clone(
                        shared_version_snapshot
                            .as_ref()
                            .expect("version surface snapshot should exist"),
                    ))),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_change" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixChangeProvider::new(Arc::clone(
                        shared_change_snapshot
                            .as_ref()
                            .expect("change surface snapshot should exist"),
                    ))),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_working_changes" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixWorkingChangesProvider::new(Arc::clone(
                        shared_working_changes_snapshot
                            .as_ref()
                            .expect("working changes surface snapshot should exist"),
                    ))),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                let Some(spec) = artifact.entity_views.get(other) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("sql2 phase-2 does not support surface '{other}' yet"),
                    ));
                };
                match spec.base_relation {
                    Sql2EntityViewBaseRelation::LixStateHistory => {
                        register_entity_view_with_shared_history_backend(
                            &ctx,
                            artifact,
                            spec,
                            surface_name,
                            Arc::clone(&backend),
                        )?;
                    }
                    Sql2EntityViewBaseRelation::LixState
                    | Sql2EntityViewBaseRelation::LixStateByVersion => {
                        register_entity_view_with_shared_snapshot(
                            &ctx,
                            artifact,
                            spec,
                            surface_name,
                            Arc::clone(
                                shared_state_snapshot
                                    .as_ref()
                                    .expect("state snapshot should exist for entity surfaces"),
                            ),
                        )?;
                    }
                }
            }
        }
    }
    Ok(ctx)
}

async fn register_entity_view_with_borrowed_backend(
    ctx: &SessionContext,
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2EntityViewPlan,
    surface_name: &str,
) -> Result<(), LixError> {
    let provider: Arc<dyn TableProvider> = match spec.base_relation {
        Sql2EntityViewBaseRelation::LixState => Arc::new(LixStateProvider::new(
            LixStateSurfaceKind::State,
            artifact.active_version_id.clone(),
            open_state_by_version_snapshot(backend, &artifact.active_version_id).await?,
        )),
        Sql2EntityViewBaseRelation::LixStateByVersion => Arc::new(LixStateProvider::new(
            LixStateSurfaceKind::StateByVersion,
            artifact.active_version_id.clone(),
            open_visible_state_by_version_snapshot(backend).await?,
        )),
        Sql2EntityViewBaseRelation::LixStateHistory => {
            Arc::new(LixStateHistoryProvider::new_materialized(
                artifact.active_version_id.clone(),
                load_materialized_state_history_rows(
                    backend,
                    &artifact.active_version_id,
                    &artifact.sql,
                )
                .await?,
            ))
        }
    };
    register_entity_view_provider(ctx, provider, spec, surface_name)
}

fn register_entity_view_with_shared_snapshot(
    ctx: &SessionContext,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2EntityViewPlan,
    surface_name: &str,
    snapshot: Arc<dyn StateByVersionSnapshot>,
) -> Result<(), LixError> {
    let provider: Arc<dyn TableProvider> = Arc::new(LixStateProvider::new(
        match spec.base_relation {
            Sql2EntityViewBaseRelation::LixState => LixStateSurfaceKind::State,
            Sql2EntityViewBaseRelation::LixStateByVersion => LixStateSurfaceKind::StateByVersion,
            Sql2EntityViewBaseRelation::LixStateHistory => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "sql2 entity view '{}' must register history-backed surfaces through the history provider",
                        spec.public_name
                    ),
                ))
            }
        },
        artifact.active_version_id.clone(),
        snapshot,
    ));
    register_entity_view_provider(ctx, provider, spec, surface_name)
}

fn register_entity_view_with_shared_history_backend(
    ctx: &SessionContext,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2EntityViewPlan,
    surface_name: &str,
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Result<(), LixError> {
    let provider: Arc<dyn TableProvider> = Arc::new(LixStateHistoryProvider::new_shared_backend(
        artifact.active_version_id.clone(),
        backend,
    ));
    register_entity_view_provider(ctx, provider, spec, surface_name)
}

async fn register_filesystem_view_with_borrowed_backend(
    ctx: &SessionContext,
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2FilesystemViewPlan,
    surface_name: &str,
) -> Result<(), LixError> {
    let state_snapshot = open_visible_state_by_version_snapshot(backend).await?;
    register_filesystem_view_with_state_snapshot(
        ctx,
        backend,
        artifact,
        spec,
        surface_name,
        state_snapshot,
    )
    .await
}

async fn register_filesystem_view_with_shared_snapshot(
    ctx: &SessionContext,
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2FilesystemViewPlan,
    surface_name: &str,
    state_snapshot: Arc<dyn StateByVersionSnapshot>,
) -> Result<(), LixError> {
    register_filesystem_view_with_state_snapshot(
        ctx,
        backend,
        artifact,
        spec,
        surface_name,
        state_snapshot,
    )
    .await
}

async fn register_filesystem_view_with_state_snapshot(
    ctx: &SessionContext,
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2FilesystemViewPlan,
    surface_name: &str,
    state_snapshot: Arc<dyn StateByVersionSnapshot>,
) -> Result<(), LixError> {
    let state_provider: Arc<dyn TableProvider> = Arc::new(LixStateProvider::new(
        LixStateSurfaceKind::StateByVersion,
        artifact.active_version_id.clone(),
        state_snapshot,
    ));
    let mut winner_providers = BTreeMap::new();
    for (relation, base_plan) in &spec.base_relation_plans {
        let compile_ctx = SessionContext::new();
        register_sql2_udfs(&compile_ctx, system_sql2_function_provider());
        let base_provider =
            base_plan.compiled_view_provider(&compile_ctx, Arc::clone(&state_provider))?;
        let winner_provider = base_plan
            .compiled_ranked_winner_view_provider(&compile_ctx, base_provider)
            .await?;
        winner_providers.insert(*relation, winner_provider);
    }

    match surface_name {
        "lix_file" | "lix_file_by_version" => {
            let file_relation = if spec
                .base_relation_plans
                .contains_key(&Sql2FilesystemViewBaseRelation::FileDescriptorRows)
            {
                Sql2FilesystemViewBaseRelation::FileDescriptorRows
            } else {
                Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows
            };
            let directory_relation = if spec
                .base_relation_plans
                .contains_key(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows)
            {
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows
            } else {
                Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows
            };
            let blob_relation = if spec
                .base_relation_plans
                .contains_key(&Sql2FilesystemViewBaseRelation::BinaryBlobRefRows)
            {
                Sql2FilesystemViewBaseRelation::BinaryBlobRefRows
            } else {
                Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows
            };
            let file_provider = winner_providers
                .get(&file_relation)
                .cloned()
                .expect("filesystem file view should have file winner provider");
            let directory_provider = winner_providers
                .get(&directory_relation)
                .cloned()
                .expect("filesystem file view should have directory winner provider");
            let blob_provider = winner_providers
                .get(&blob_relation)
                .cloned()
                .expect("filesystem file view should have blob winner provider");
            let file_data_provider =
                materialize_live_file_data_provider(backend, Arc::clone(&blob_provider)).await?;
            let final_ctx = SessionContext::new();
            register_sql2_udfs(&final_ctx, system_sql2_function_provider());
            ctx.register_table(
                surface_name,
                spec.compiled_lix_file_view_provider(
                    &final_ctx,
                    &artifact.active_version_id,
                    file_provider,
                    directory_provider,
                    file_data_provider,
                )
                .await?,
            )
            .map_err(datafusion_error_to_lix_error)?;
        }
        "lix_directory" | "lix_directory_by_version" => {
            let directory_provider = winner_providers
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorRows)
                .cloned()
                .expect("filesystem directory view should have directory winner provider");
            let final_ctx = SessionContext::new();
            register_sql2_udfs(&final_ctx, system_sql2_function_provider());
            ctx.register_table(
                surface_name,
                spec.compiled_lix_directory_view_provider(
                    &final_ctx,
                    &artifact.active_version_id,
                    directory_provider,
                )
                .await?,
            )
            .map_err(datafusion_error_to_lix_error)?;
        }
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sql2 does not support filesystem surface '{other}' yet"),
            ));
        }
    }
    Ok(())
}

async fn materialize_live_file_data_provider(
    backend: &dyn LixBackend,
    blob_winner_provider: Arc<dyn TableProvider>,
) -> Result<Arc<dyn TableProvider>, LixError> {
    let ctx = SessionContext::new();
    let batches = ctx
        .read_table(blob_winner_provider)
        .map_err(datafusion_error_to_lix_error)?
        .select(vec![
            datafusion::logical_expr::col("id"),
            datafusion::logical_expr::col("version_id"),
            datafusion::logical_expr::col("blob_hash"),
            datafusion::logical_expr::col("size_bytes"),
        ])
        .map_err(datafusion_error_to_lix_error)?
        .collect()
        .await
        .map_err(datafusion_error_to_lix_error)?;

    let mut ids = Vec::<Option<String>>::new();
    let mut version_ids = Vec::<Option<String>>::new();
    let mut blob_hashes = Vec::<Option<String>>::new();
    let mut payloads = Vec::<Option<Vec<u8>>>::new();
    let mut size_bytes = Vec::<Option<i64>>::new();

    for batch in &batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "sql2 expected string id column while materializing live file data",
                )
            })?;
        let version_id_array = batch
            .column_by_name("version_id")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "sql2 expected string version_id column while materializing live file data",
                )
            })?;
        let blob_hash_array = batch
            .column_by_name("blob_hash")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "sql2 expected string blob_hash column while materializing live file data",
                )
            })?;
        let size_bytes_array = batch
            .column_by_name("size_bytes")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "sql2 expected int64 size_bytes column while materializing live file data",
                )
            })?;

        for row_index in 0..batch.num_rows() {
            let id = (!id_array.is_null(row_index)).then(|| id_array.value(row_index).to_string());
            let version_id = (!version_id_array.is_null(row_index))
                .then(|| version_id_array.value(row_index).to_string());
            let blob_hash = (!blob_hash_array.is_null(row_index))
                .then(|| blob_hash_array.value(row_index).to_string());
            let data = match (&id, &version_id, &blob_hash) {
                (Some(file_id), Some(version_id), Some(blob_hash)) => {
                    load_live_file_payload_bytes(backend, file_id, version_id, blob_hash).await?
                }
                _ => None,
            };
            let size =
                (!size_bytes_array.is_null(row_index)).then(|| size_bytes_array.value(row_index));

            ids.push(id);
            version_ids.push(version_id);
            blob_hashes.push(blob_hash);
            payloads.push(data);
            size_bytes.push(size);
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("version_id", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("blob_hash", DataType::Utf8, true),
        Field::new("size_bytes", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(version_ids)) as ArrayRef,
            Arc::new(BinaryArray::from(
                payloads
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(blob_hashes)) as ArrayRef,
            Arc::new(Int64Array::from(size_bytes)) as ArrayRef,
        ],
    )
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build live file data batch: {error}"),
        )
    })?;

    Ok(Arc::new(
        MemTable::try_new(schema, vec![vec![batch]]).map_err(datafusion_error_to_lix_error)?,
    ))
}

async fn load_live_file_payload_bytes(
    backend: &dyn LixBackend,
    _file_id: &str,
    _version_id: &str,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    crate::binary_cas::load_blob_data_by_hash(backend, blob_hash).await
}

async fn register_filesystem_history_view_with_state_history_provider(
    ctx: &SessionContext,
    artifact: &PreparedSql2ReadArtifact,
    spec: &PreparedSql2FilesystemViewPlan,
    surface_name: &str,
    state_history_provider: Arc<dyn TableProvider>,
) -> Result<(), LixError> {
    let compile_ctx = SessionContext::new();
    register_sql2_udfs(&compile_ctx, system_sql2_function_provider());

    let mut base_relation_providers: BTreeMap<
        Sql2FilesystemViewBaseRelation,
        Arc<dyn TableProvider>,
    > = BTreeMap::new();
    for base_relation in &spec.base_relations {
        let base_relation_plan = spec.base_relation_plans.get(base_relation).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "filesystem history view '{}' is missing base relation plan for {:?}",
                    spec.public_name, base_relation
                ),
            )
        })?;
        let base_provider_ctx = SessionContext::new();
        register_sql2_udfs(&base_provider_ctx, system_sql2_function_provider());
        let provider = base_relation_plan
            .compiled_view_provider(&base_provider_ctx, Arc::clone(&state_history_provider))?;
        base_relation_providers.insert(*base_relation, provider);
    }

    match surface_name {
        "lix_file_history" | "lix_file_history_by_version" => {
            let file_history_rows_provider = base_relation_providers
                .get(&Sql2FilesystemViewBaseRelation::FileDescriptorHistoryRows)
                .cloned()
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "filesystem history view '{}' is missing file descriptor history rows",
                            spec.public_name
                        ),
                    )
                })?;
            let directory_history_rows_provider = base_relation_providers
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
                .cloned()
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "filesystem history view '{}' is missing directory descriptor history rows",
                            spec.public_name
                        ),
                    )
                })?;
            let blob_history_rows_provider = base_relation_providers
                .get(&Sql2FilesystemViewBaseRelation::BinaryBlobRefHistoryRows)
                .cloned()
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "filesystem history view '{}' is missing blob ref history rows",
                            spec.public_name
                        ),
                    )
                })?;
            ctx.register_table(
                surface_name,
                spec.compiled_lix_file_history_view_provider(
                    &compile_ctx,
                    file_history_rows_provider,
                    directory_history_rows_provider,
                    blob_history_rows_provider,
                )
                .await?,
            )
            .map_err(datafusion_error_to_lix_error)?;
        }
        "lix_directory_history" => {
            let directory_history_rows_provider = base_relation_providers
                .get(&Sql2FilesystemViewBaseRelation::DirectoryDescriptorHistoryRows)
                .cloned()
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "filesystem history view '{}' is missing directory descriptor history rows",
                            spec.public_name
                        ),
                    )
                })?;
            ctx.register_table(
                surface_name,
                spec.compiled_lix_directory_history_view_provider(
                    &compile_ctx,
                    &artifact.active_version_id,
                    directory_history_rows_provider,
                )
                .await?,
            )
            .map_err(datafusion_error_to_lix_error)?;
        }
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sql2 does not support filesystem history surface '{other}' yet"),
            ));
        }
    }

    Ok(())
}

fn register_entity_view_provider(
    ctx: &SessionContext,
    provider: Arc<dyn TableProvider>,
    spec: &PreparedSql2EntityViewPlan,
    surface_name: &str,
) -> Result<(), LixError> {
    ctx.register_table(surface_name, spec.compiled_view_provider(ctx, provider)?)
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn datafusion_error_to_lix_error_with_artifact(
    error: DataFusionError,
    artifact: &PreparedSql2ReadArtifact,
) -> LixError {
    let error_text = error.to_string();
    if let Some(column_name) = parse_datafusion_unknown_column_name(&error_text) {
        let table_name = artifact.surface_names.first().map(String::as_str);
        let available_columns = artifact
            .surface_names
            .first()
            .and_then(|surface_name| artifact.entity_views.get(surface_name))
            .map(|spec| {
                spec.column_order
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        return sql_unknown_column_error(&column_name, table_name, &available_columns, None);
    }

    datafusion_error_to_lix_error(error)
}

fn parse_datafusion_unknown_column_name(message: &str) -> Option<String> {
    for needle in [
        "No field named ",
        "field named ",
        "Column '",
        "column '",
        "column `",
    ] {
        let Some(start) = message.find(needle) else {
            continue;
        };
        let rest = &message[start + needle.len()..];
        let candidate = rest
            .trim_start_matches(['`', '\'', '"'])
            .split(|ch: char| {
                ch == '`'
                    || ch == '\''
                    || ch == '"'
                    || ch == '.'
                    || ch == ','
                    || ch == ' '
                    || ch == '\n'
                    || ch == '\r'
            })
            .next()
            .unwrap_or_default()
            .trim();
        if !candidate.is_empty() {
            return Some(candidate.to_string());
        }
    }
    None
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarValue {
    match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)),
        Value::Real(value) => ScalarValue::Float64(Some(*value)),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        Value::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())),
    }
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 live_state error: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LixStateSurfaceKind {
    State,
    StateByVersion,
}

#[derive(Clone)]
enum LixStateHistorySource {
    Materialized(Arc<Vec<StateHistoryRow>>),
    SharedBackend(Arc<dyn LixBackend + Send + Sync>),
}

impl std::fmt::Debug for LixStateHistorySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialized(rows) => f.debug_tuple("Materialized").field(&rows.len()).finish(),
            Self::SharedBackend(_) => f.write_str("SharedBackend(..)"),
        }
    }
}

impl LixStateSurfaceKind {
    fn schema(self) -> SchemaRef {
        match self {
            Self::State => Arc::new(Schema::new(vec![
                Field::new("entity_id", DataType::Utf8, false),
                Field::new("schema_key", DataType::Utf8, false),
                Field::new("file_id", DataType::Utf8, true),
                Field::new("plugin_key", DataType::Utf8, true),
                Field::new("snapshot_content", DataType::Utf8, true),
                Field::new("metadata", DataType::Utf8, true),
                Field::new("schema_version", DataType::Utf8, true),
                Field::new("created_at", DataType::Utf8, true),
                Field::new("updated_at", DataType::Utf8, true),
                Field::new("global", DataType::Boolean, false),
                Field::new("change_id", DataType::Utf8, true),
                Field::new("commit_id", DataType::Utf8, true),
                Field::new("untracked", DataType::Boolean, false),
            ])),
            Self::StateByVersion => Arc::new(Schema::new(vec![
                Field::new("entity_id", DataType::Utf8, false),
                Field::new("schema_key", DataType::Utf8, false),
                Field::new("file_id", DataType::Utf8, true),
                Field::new("plugin_key", DataType::Utf8, true),
                Field::new("snapshot_content", DataType::Utf8, true),
                Field::new("metadata", DataType::Utf8, true),
                Field::new("schema_version", DataType::Utf8, true),
                Field::new("created_at", DataType::Utf8, true),
                Field::new("updated_at", DataType::Utf8, true),
                Field::new("global", DataType::Boolean, false),
                Field::new("change_id", DataType::Utf8, true),
                Field::new("commit_id", DataType::Utf8, true),
                Field::new("untracked", DataType::Boolean, false),
                Field::new("version_id", DataType::Utf8, false),
            ])),
        }
    }
}

#[derive(Debug, Clone)]
struct LixStateProvider {
    surface_kind: LixStateSurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn StateByVersionSnapshot>,
}

impl LixStateProvider {
    fn new(
        surface_kind: LixStateSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: surface_kind.schema(),
            snapshot,
        }
    }
}

fn lix_state_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("snapshot_content", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("change_id", DataType::Utf8, false),
        Field::new("commit_id", DataType::Utf8, false),
        Field::new("commit_created_at", DataType::Utf8, false),
        Field::new("root_commit_id", DataType::Utf8, false),
        Field::new("depth", DataType::Int64, false),
        Field::new("version_id", DataType::Utf8, false),
    ]))
}

#[derive(Debug, Clone)]
struct LixStateHistoryProvider {
    active_version_id: String,
    schema: SchemaRef,
    source: LixStateHistorySource,
}

impl LixStateHistoryProvider {
    fn new_materialized(active_version_id: String, rows: Vec<StateHistoryRow>) -> Self {
        Self {
            active_version_id,
            schema: lix_state_history_schema(),
            source: LixStateHistorySource::Materialized(Arc::new(rows)),
        }
    }

    fn new_shared_backend(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
    ) -> Self {
        Self {
            active_version_id,
            schema: lix_state_history_schema(),
            source: LixStateHistorySource::SharedBackend(backend),
        }
    }
}

#[async_trait]
impl TableProvider for LixStateHistoryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_state_history_filter(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(LixStateHistoryScanExec::new(
            self.active_version_id.clone(),
            self.source.clone(),
            projected_schema,
            projection.cloned(),
            StateHistoryRoute::from_filters(filters),
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixStateHistoryScanExec {
    active_version_id: String,
    source: LixStateHistorySource,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: StateHistoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixStateHistoryScanExec {
    fn new(
        active_version_id: String,
        source: LixStateHistorySource,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: StateHistoryRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            active_version_id,
            source,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateHistoryScanExec(active_version_id={}, limit={:?}, route={:?})",
                    self.active_version_id, self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateHistoryScanExec"),
        }
    }
}

impl ExecutionPlan for LixStateHistoryScanExec {
    fn name(&self) -> &str {
        "LixStateHistoryScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixStateHistoryScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixStateHistoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let active_version_id = self.active_version_id.clone();
        let source = self.source.clone();
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let limit = self.limit;
        let route = self.route.clone();
        let zero_column_projection = self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.is_empty());
        let stream = stream::once(async move {
            let request = state_history_request(&active_version_id, &route);
            let rows = if request_contradictory(&request) {
                Vec::new()
            } else {
                match source {
                    LixStateHistorySource::Materialized(rows) => rows.as_ref().clone(),
                    LixStateHistorySource::SharedBackend(backend) => {
                        enqueue_state_history_scan(backend, request).await?
                    }
                }
            };
            let rows = if let Some(limit) = limit {
                rows.into_iter().take(limit).collect::<Vec<_>>()
            } else {
                rows
            };
            let batches = if zero_column_projection {
                let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
                vec![RecordBatch::try_new_with_options(
                    Arc::clone(&stream_schema),
                    vec![],
                    &options,
                )
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "sql2 failed to build zero-column lix_state_history batch: {error}"
                    ))
                })?]
            } else {
                state_history_record_batches(Arc::clone(&stream_schema), &rows)?
            };
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct StateHistoryScanJob {
    backend: Arc<dyn LixBackend + Send + Sync>,
    request: StateHistoryRequest,
    reply: oneshot::Sender<std::result::Result<Vec<StateHistoryRow>, LixError>>,
}

fn state_history_scan_worker() -> &'static mpsc::Sender<StateHistoryScanJob> {
    static WORKER: OnceLock<mpsc::Sender<StateHistoryScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<StateHistoryScanJob>();
        thread::Builder::new()
            .name("sql2-state-history-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 state-history runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime.block_on(async move {
                        job.backend
                            .as_ref()
                            .load_committed_state_history_rows(&job.request)
                            .await
                    });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 state-history worker thread should spawn");
        tx
    })
}

async fn enqueue_state_history_scan(
    backend: Arc<dyn LixBackend + Send + Sync>,
    request: StateHistoryRequest,
) -> Result<Vec<StateHistoryRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state_history_scan_worker()
        .send(StateHistoryScanJob {
            backend,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue state-history scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 state-history scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

async fn load_materialized_state_history_rows(
    backend: &dyn LixBackend,
    active_version_id: &str,
    sql: &str,
) -> Result<Vec<StateHistoryRow>, LixError> {
    backend
        .load_committed_state_history_rows(&state_history_request(
            active_version_id,
            &state_history_route_from_sql(sql)?,
        ))
        .await
}

fn canonical_state_history_column_name(name: &str) -> Option<&str> {
    match name {
        "root_commit_id" | "lixcol_root_commit_id" => Some("root_commit_id"),
        "entity_id" | "lixcol_entity_id" => Some("entity_id"),
        "schema_key" | "lixcol_schema_key" => Some("schema_key"),
        "version_id" | "lixcol_version_id" => Some("version_id"),
        "depth" | "lixcol_depth" => Some("depth"),
        _ => None,
    }
}
fn state_history_request(
    active_version_id: &str,
    route: &StateHistoryRoute,
) -> StateHistoryRequest {
    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        lineage_version_id: Some(active_version_id.to_string()),
        content_mode: StateHistoryContentMode::IncludeSnapshotContent,
        ..StateHistoryRequest::default()
    };

    if !route.root_commit_ids.is_empty() {
        request.root_scope = StateHistoryRootScope::RequestedRoots(route.root_commit_ids.clone());
    }
    if !route.entity_ids.is_empty() {
        request.entity_ids = route.entity_ids.clone();
    }
    if !route.schema_keys.is_empty() {
        request.schema_keys = route.schema_keys.clone();
    }
    if !route.version_ids.is_empty() {
        request.version_scope =
            StateHistoryVersionScope::RequestedVersions(route.version_ids.clone());
    }
    request.min_depth = route.min_depth;
    request.max_depth = route.max_depth;
    request
}

fn request_contradictory(request: &StateHistoryRequest) -> bool {
    request
        .min_depth
        .zip(request.max_depth)
        .is_some_and(|(min, max)| min > max)
        || matches!(request.root_scope, StateHistoryRootScope::RequestedRoots(ref roots) if roots.is_empty())
        || matches!(request.version_scope, StateHistoryVersionScope::RequestedVersions(ref versions) if versions.is_empty())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StateHistoryRoute {
    root_commit_ids: Vec<String>,
    entity_ids: Vec<String>,
    schema_keys: Vec<String>,
    version_ids: Vec<String>,
    min_depth: Option<i64>,
    max_depth: Option<i64>,
}

impl StateHistoryRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            apply_state_history_filter(filter, &mut route);
        }
        route
    }
}

fn parse_state_history_filter(expr: &Expr) -> Option<()> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    match binary_expr.op {
        Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {}
        _ => return None,
    }

    let Expr::Column(column) = &*binary_expr.left else {
        return None;
    };
    let Expr::Literal(_, _) = &*binary_expr.right else {
        return None;
    };

    canonical_state_history_column_name(column.name.as_str()).and_then(|column_name| {
        match column_name {
            "root_commit_id" | "entity_id" | "schema_key" | "version_id" | "depth" => Some(()),
            _ => None,
        }
    })
}

fn apply_state_history_filter(expr: &Expr, route: &mut StateHistoryRoute) {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return;
    };
    let Expr::Column(column) = &*binary_expr.left else {
        return;
    };
    let Some(column_name) = canonical_state_history_column_name(column.name.as_str()) else {
        return;
    };
    let right = &*binary_expr.right;
    match (column_name, &binary_expr.op, right) {
        ("root_commit_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("entity_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("schema_key", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("version_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _)) => {
            let bucket = match column_name {
                "root_commit_id" => &mut route.root_commit_ids,
                "entity_id" => &mut route.entity_ids,
                "schema_key" => &mut route.schema_keys,
                "version_id" => &mut route.version_ids,
                _ => unreachable!(),
            };
            if !bucket.contains(value) {
                bucket.push(value.clone());
            }
        }
        ("depth", Operator::Eq, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.min_depth = Some(value);
                route.max_depth = Some(value);
            }
        }
        ("depth", Operator::Gt, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.min_depth = Some(
                    route
                        .min_depth
                        .map_or(value + 1, |current| current.max(value + 1)),
                );
            }
        }
        ("depth", Operator::GtEq, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.min_depth = Some(route.min_depth.map_or(value, |current| current.max(value)));
            }
        }
        ("depth", Operator::Lt, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.max_depth = Some(
                    route
                        .max_depth
                        .map_or(value - 1, |current| current.min(value - 1)),
                );
            }
        }
        ("depth", Operator::LtEq, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.max_depth = Some(route.max_depth.map_or(value, |current| current.min(value)));
            }
        }
        _ => {}
    }
}

fn scalar_i64_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn state_history_route_from_sql(sql: &str) -> Result<StateHistoryRoute, LixError> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 query parse failed during state-history route extraction: {error}"),
        )
    })?;
    let mut route = StateHistoryRoute::default();
    for statement in &statements {
        if let SqlStatement::Query(query) = statement {
            collect_state_history_route_from_query(query, &mut route);
        }
    }
    Ok(route)
}

fn collect_state_history_route_from_query(query: &SqlQuery, route: &mut StateHistoryRoute) {
    if let SqlSetExpr::Select(select) = query.body.as_ref() {
        if let Some(selection) = &select.selection {
            collect_state_history_route_from_sql_expr(selection, route);
        }
    }
}

fn collect_state_history_route_from_sql_expr(expr: &SqlExpr, route: &mut StateHistoryRoute) {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => {
            if *op == sqlparser::ast::BinaryOperator::And {
                collect_state_history_route_from_sql_expr(left, route);
                collect_state_history_route_from_sql_expr(right, route);
                return;
            }
            match (history_column_name(left), op) {
                (Some("root_commit_id"), sqlparser::ast::BinaryOperator::Eq) => {
                    let Some(value) = sql_expr_string_literal(right) else {
                        return;
                    };
                    if !route.root_commit_ids.contains(&value.to_string()) {
                        route.root_commit_ids.push(value.to_string());
                    }
                }
                (Some("entity_id"), sqlparser::ast::BinaryOperator::Eq) => {
                    let Some(value) = sql_expr_string_literal(right) else {
                        return;
                    };
                    if !route.entity_ids.contains(&value.to_string()) {
                        route.entity_ids.push(value.to_string());
                    }
                }
                (Some("schema_key"), sqlparser::ast::BinaryOperator::Eq) => {
                    let Some(value) = sql_expr_string_literal(right) else {
                        return;
                    };
                    if !route.schema_keys.contains(&value.to_string()) {
                        route.schema_keys.push(value.to_string());
                    }
                }
                (Some("version_id"), sqlparser::ast::BinaryOperator::Eq) => {
                    let Some(value) = sql_expr_string_literal(right) else {
                        return;
                    };
                    if !route.version_ids.contains(&value.to_string()) {
                        route.version_ids.push(value.to_string());
                    }
                }
                (Some("depth"), sqlparser::ast::BinaryOperator::Eq) => {
                    if let Some(value) = sql_expr_i64_literal(right) {
                        route.min_depth = Some(value);
                        route.max_depth = Some(value);
                    }
                }
                (Some("depth"), sqlparser::ast::BinaryOperator::Gt) => {
                    if let Some(value) = sql_expr_i64_literal(right) {
                        route.min_depth = Some(
                            route
                                .min_depth
                                .map_or(value + 1, |current| current.max(value + 1)),
                        );
                    }
                }
                (Some("depth"), sqlparser::ast::BinaryOperator::GtEq) => {
                    if let Some(value) = sql_expr_i64_literal(right) {
                        route.min_depth =
                            Some(route.min_depth.map_or(value, |current| current.max(value)));
                    }
                }
                (Some("depth"), sqlparser::ast::BinaryOperator::Lt) => {
                    if let Some(value) = sql_expr_i64_literal(right) {
                        route.max_depth = Some(
                            route
                                .max_depth
                                .map_or(value - 1, |current| current.min(value - 1)),
                        );
                    }
                }
                (Some("depth"), sqlparser::ast::BinaryOperator::LtEq) => {
                    if let Some(value) = sql_expr_i64_literal(right) {
                        route.max_depth =
                            Some(route.max_depth.map_or(value, |current| current.min(value)));
                    }
                }
                _ => {}
            }
        }
        SqlExpr::Nested(inner) => collect_state_history_route_from_sql_expr(inner, route),
        SqlExpr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Some("root_commit_id") = history_column_name(expr) {
                for item in list {
                    if let Some(value) = sql_expr_string_literal(item) {
                        if !route.root_commit_ids.contains(&value.to_string()) {
                            route.root_commit_ids.push(value.to_string());
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn history_column_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.as_str()),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
    .and_then(canonical_state_history_column_name)
}

fn sql_expr_string_literal(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Value(value) => match value.value {
            SqlValue::SingleQuotedString(ref inner) => Some(inner.as_str()),
            _ => None,
        },
        _ => None,
    }
}

fn sql_expr_i64_literal(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(number, _) => number.parse::<i64>().ok(),
            _ => None,
        },
        SqlExpr::UnaryOp { op, expr } if matches!(op, sqlparser::ast::UnaryOperator::Minus) => {
            sql_expr_i64_literal(expr).map(|value| -value)
        }
        _ => None,
    }
}

#[async_trait]
impl TableProvider for LixStateProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_route_filter(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        let route = LixStateRoute::from_filters(filters);
        Ok(Arc::new(LixStateScanExec::new(
            self.surface_kind,
            self.default_version_id.clone(),
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixStateScanExec {
    surface_kind: LixStateSurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn StateByVersionSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixStateRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixStateScanExec {
    fn new(
        surface_kind: LixStateSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixStateRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            surface_kind,
            default_version_id,
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateScanExec(surface={:?}, version_id={}, limit={:?}, route={:?})",
                    self.surface_kind, self.default_version_id, self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateScanExec"),
        }
    }
}

impl ExecutionPlan for LixStateScanExec {
    fn name(&self) -> &str {
        "LixStateScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixStateScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixStateScanExec only exposes one partition, got {partition}"
            )));
        }

        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let zero_column_projection = self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.is_empty());
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                let batches = enqueue_state_by_version_scan_batches(
                    snapshot,
                    state_by_version_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    )?,
                )
                .await?;
                if zero_column_projection {
                    batches
                        .iter()
                        .map(|batch| {
                            let options =
                                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                            RecordBatch::try_new_with_options(
                                Arc::clone(&stream_schema),
                                vec![],
                                &options,
                            )
                            .map_err(|error| {
                                DataFusionError::Execution(format!(
                                    "sql2 failed to build zero-column lix_state batch: {error}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    batches
                }
            };
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct StateByVersionScanJob {
    snapshot: Arc<dyn StateByVersionSnapshot>,
    request: StateByVersionScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<RecordBatch>, LixError>>,
}

fn state_by_version_scan_worker() -> &'static mpsc::Sender<StateByVersionScanJob> {
    static WORKER: OnceLock<mpsc::Sender<StateByVersionScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<StateByVersionScanJob>();
        thread::Builder::new()
            .name("sql2-live-state-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 live-state runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime.block_on(async move {
                        job.snapshot
                            .scan_state_by_version_batches(&job.request)
                            .await
                    });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 live-state worker thread should spawn");
        tx
    })
}

async fn enqueue_state_by_version_scan_batches(
    snapshot: Arc<dyn StateByVersionSnapshot>,
    request: StateByVersionScanRequest,
) -> Result<Vec<RecordBatch>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state_by_version_scan_worker()
        .send(StateByVersionScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue live_state scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 live_state scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn state_history_record_batches(
    schema: SchemaRef,
    rows: &[StateHistoryRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![state_history_record_batch(schema, rows)?])
}

fn state_history_record_batch(schema: SchemaRef, rows: &[StateHistoryRow]) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(match field.name().as_str() {
                "entity_id" => string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
                "schema_key" => string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
                "file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
                "plugin_key" => string_array(rows.iter().map(|row| row.plugin_key.as_deref())),
                "snapshot_content" => {
                    string_array(rows.iter().map(|row| row.snapshot_content.as_deref()))
                }
                "metadata" => string_array(rows.iter().map(|row| row.metadata.as_deref())),
                "schema_version" => {
                    string_array(rows.iter().map(|row| Some(row.schema_version.as_str())))
                }
                "change_id" => string_array(rows.iter().map(|row| Some(row.change_id.as_str()))),
                "commit_id" => string_array(rows.iter().map(|row| Some(row.commit_id.as_str()))),
                "commit_created_at" => {
                    string_array(rows.iter().map(|row| Some(row.commit_created_at.as_str())))
                }
                "root_commit_id" => {
                    string_array(rows.iter().map(|row| Some(row.root_commit_id.as_str())))
                }
                "depth" => Arc::new(datafusion::arrow::array::Int64Array::from(
                    rows.iter().map(|row| row.depth).collect::<Vec<_>>(),
                )) as ArrayRef,
                "version_id" => string_array(rows.iter().map(|row| Some(row.version_id.as_str()))),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "sql2 does not support lix_state_history column '{other}'"
                    )))
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 failed to build lix_state_history batch: {error}"
        ))
    })
}

fn lix_change_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("untracked", DataType::Boolean, false),
        Field::new("snapshot_content", DataType::Utf8, true),
    ]))
}

#[derive(Debug, Clone)]
struct LixChangeProvider {
    schema: SchemaRef,
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
}

impl LixChangeProvider {
    fn new(snapshot: Arc<dyn ChangeSurfaceSnapshot>) -> Self {
        Self {
            schema: lix_change_schema(),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixChangeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_change_route_filter(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        let route = LixChangeRoute::from_filters(filters);
        Ok(Arc::new(LixChangeScanExec::new(
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixChangeScanExec {
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixChangeRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixChangeScanExec {
    fn new(
        snapshot: Arc<dyn ChangeSurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixChangeRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixChangeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixChangeScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixChangeScanExec"),
        }
    }
}

impl ExecutionPlan for LixChangeScanExec {
    fn name(&self) -> &str {
        "LixChangeScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixChangeScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixChangeScanExec only exposes one partition, got {partition}"
            )));
        }

        if self.route.contradictory {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::iter(Vec::<Result<RecordBatch>>::new()),
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let scan_projection = change_projection_for_scan(projection.as_ref());
            let rows = enqueue_change_surface_scan(
                snapshot,
                ChangeSurfaceScanRequest {
                    projection: scan_projection.clone(),
                    filters: change_filters_for_route(&route),
                    limit,
                },
            )
            .await?;
            let batches = change_surface_record_batches(scan_projection, &rows)?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct ChangeSurfaceScanJob {
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
    request: ChangeSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<ChangeSurfaceRow>, LixError>>,
}

fn change_surface_scan_worker() -> &'static mpsc::Sender<ChangeSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<ChangeSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<ChangeSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-change-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 change-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_changes(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 change-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_change_surface_scan(
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
    request: ChangeSurfaceScanRequest,
) -> Result<Vec<ChangeSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    change_surface_scan_worker()
        .send(ChangeSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue change surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 change surface scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn change_surface_record_batches(
    projection: Vec<ChangeSurfaceColumn>,
    rows: &[ChangeSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![change_surface_record_batch(&projection, rows)?])
}

fn change_surface_record_batch(
    projection: &[ChangeSurfaceColumn],
    rows: &[ChangeSurfaceRow],
) -> Result<RecordBatch> {
    if projection.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(
            change_surface_schema(projection),
            vec![],
            &options,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to build zero-column lix_change batch: {error}"
            ))
        });
    }

    let arrays = projection
        .iter()
        .map(|column| match column {
            ChangeSurfaceColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            ChangeSurfaceColumn::EntityId => {
                string_array(rows.iter().map(|row| Some(row.entity_id.as_str())))
            }
            ChangeSurfaceColumn::SchemaKey => {
                string_array(rows.iter().map(|row| Some(row.schema_key.as_str())))
            }
            ChangeSurfaceColumn::SchemaVersion => {
                string_array(rows.iter().map(|row| Some(row.schema_version.as_str())))
            }
            ChangeSurfaceColumn::FileId => {
                string_array(rows.iter().map(|row| row.file_id.as_deref()))
            }
            ChangeSurfaceColumn::PluginKey => {
                string_array(rows.iter().map(|row| row.plugin_key.as_deref()))
            }
            ChangeSurfaceColumn::Metadata => {
                string_array(rows.iter().map(|row| row.metadata.as_deref()))
            }
            ChangeSurfaceColumn::CreatedAt => {
                string_array(rows.iter().map(|row| Some(row.created_at.as_str())))
            }
            ChangeSurfaceColumn::Untracked => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.untracked).collect::<Vec<_>>(),
            )) as ArrayRef,
            ChangeSurfaceColumn::SnapshotContent => {
                string_array(rows.iter().map(|row| row.snapshot_content.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(change_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_change batch: {error}"))
    })
}

fn change_surface_schema(projection: &[ChangeSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                ChangeSurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                ChangeSurfaceColumn::EntityId => Field::new("entity_id", DataType::Utf8, false),
                ChangeSurfaceColumn::SchemaKey => Field::new("schema_key", DataType::Utf8, false),
                ChangeSurfaceColumn::SchemaVersion => {
                    Field::new("schema_version", DataType::Utf8, false)
                }
                ChangeSurfaceColumn::FileId => Field::new("file_id", DataType::Utf8, true),
                ChangeSurfaceColumn::PluginKey => Field::new("plugin_key", DataType::Utf8, true),
                ChangeSurfaceColumn::Metadata => Field::new("metadata", DataType::Utf8, true),
                ChangeSurfaceColumn::CreatedAt => Field::new("created_at", DataType::Utf8, false),
                ChangeSurfaceColumn::Untracked => Field::new("untracked", DataType::Boolean, false),
                ChangeSurfaceColumn::SnapshotContent => {
                    Field::new("snapshot_content", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn change_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<ChangeSurfaceColumn> {
    let all_columns = vec![
        ChangeSurfaceColumn::Id,
        ChangeSurfaceColumn::EntityId,
        ChangeSurfaceColumn::SchemaKey,
        ChangeSurfaceColumn::SchemaVersion,
        ChangeSurfaceColumn::FileId,
        ChangeSurfaceColumn::PluginKey,
        ChangeSurfaceColumn::Metadata,
        ChangeSurfaceColumn::CreatedAt,
        ChangeSurfaceColumn::Untracked,
        ChangeSurfaceColumn::SnapshotContent,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn lix_working_changes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("before_change_id", DataType::Utf8, true),
        Field::new("after_change_id", DataType::Utf8, true),
        Field::new("before_commit_id", DataType::Utf8, true),
        Field::new("after_commit_id", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
    ]))
}

#[derive(Debug, Clone)]
struct LixWorkingChangesProvider {
    schema: SchemaRef,
    snapshot: Arc<dyn WorkingChangesSurfaceSnapshot>,
}

impl LixWorkingChangesProvider {
    fn new(snapshot: Arc<dyn WorkingChangesSurfaceSnapshot>) -> Self {
        Self {
            schema: lix_working_changes_schema(),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixWorkingChangesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        let route = LixWorkingChangesRoute::from_filters(filters);
        Ok(Arc::new(LixWorkingChangesScanExec::new(
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixWorkingChangesScanExec {
    snapshot: Arc<dyn WorkingChangesSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixWorkingChangesRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixWorkingChangesScanExec {
    fn new(
        snapshot: Arc<dyn WorkingChangesSurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixWorkingChangesRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixWorkingChangesScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixWorkingChangesScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixWorkingChangesScanExec"),
        }
    }
}

impl ExecutionPlan for LixWorkingChangesScanExec {
    fn name(&self) -> &str {
        "LixWorkingChangesScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixWorkingChangesScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixWorkingChangesScanExec only exposes one partition, got {partition}"
            )));
        }

        if self.route.contradictory {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::iter(Vec::<Result<RecordBatch>>::new()),
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let scan_projection = working_changes_projection_for_scan(projection.as_ref());
            let rows = enqueue_working_changes_surface_scan(
                snapshot,
                WorkingChangesSurfaceScanRequest {
                    projection: scan_projection.clone(),
                    filters: route.working_changes_filters(),
                    limit,
                },
            )
            .await?;
            let batches = working_changes_surface_record_batches(scan_projection, &rows)?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct WorkingChangesSurfaceScanJob {
    snapshot: Arc<dyn WorkingChangesSurfaceSnapshot>,
    request: WorkingChangesSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<WorkingChangesSurfaceRow>, LixError>>,
}

fn working_changes_surface_scan_worker() -> &'static mpsc::Sender<WorkingChangesSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<WorkingChangesSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<WorkingChangesSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-working-changes-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 working-changes-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime.block_on(async move {
                        job.snapshot.scan_working_changes(&job.request).await
                    });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 working-changes-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_working_changes_surface_scan(
    snapshot: Arc<dyn WorkingChangesSurfaceSnapshot>,
    request: WorkingChangesSurfaceScanRequest,
) -> Result<Vec<WorkingChangesSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    working_changes_surface_scan_worker()
        .send(WorkingChangesSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue working changes surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution(
                "sql2 working changes surface scan worker dropped reply".to_string(),
            )
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn working_changes_surface_record_batches(
    projection: Vec<WorkingChangesSurfaceColumn>,
    rows: &[WorkingChangesSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![working_changes_surface_record_batch(
        &projection,
        rows,
    )?])
}

fn working_changes_surface_record_batch(
    projection: &[WorkingChangesSurfaceColumn],
    rows: &[WorkingChangesSurfaceRow],
) -> Result<RecordBatch> {
    if projection.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(
            working_changes_surface_schema(projection),
            vec![],
            &options,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to build zero-column lix_working_changes batch: {error}"
            ))
        });
    }

    let arrays = projection
        .iter()
        .map(|column| match column {
            WorkingChangesSurfaceColumn::EntityId => {
                string_array(rows.iter().map(|row| Some(row.entity_id.as_str())))
            }
            WorkingChangesSurfaceColumn::SchemaKey => {
                string_array(rows.iter().map(|row| Some(row.schema_key.as_str())))
            }
            WorkingChangesSurfaceColumn::FileId => {
                string_array(rows.iter().map(|row| row.file_id.as_deref()))
            }
            WorkingChangesSurfaceColumn::LixcolGlobal => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.lixcol_global).collect::<Vec<_>>(),
            )) as ArrayRef,
            WorkingChangesSurfaceColumn::BeforeChangeId => {
                string_array(rows.iter().map(|row| row.before_change_id.as_deref()))
            }
            WorkingChangesSurfaceColumn::AfterChangeId => {
                string_array(rows.iter().map(|row| row.after_change_id.as_deref()))
            }
            WorkingChangesSurfaceColumn::BeforeCommitId => {
                string_array(rows.iter().map(|row| row.before_commit_id.as_deref()))
            }
            WorkingChangesSurfaceColumn::AfterCommitId => {
                string_array(rows.iter().map(|row| row.after_commit_id.as_deref()))
            }
            WorkingChangesSurfaceColumn::Status => {
                string_array(rows.iter().map(|row| Some(row.status.as_str())))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(working_changes_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 failed to build lix_working_changes batch: {error}"
        ))
    })
}

fn working_changes_surface_schema(projection: &[WorkingChangesSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                WorkingChangesSurfaceColumn::EntityId => {
                    Field::new("entity_id", DataType::Utf8, false)
                }
                WorkingChangesSurfaceColumn::SchemaKey => {
                    Field::new("schema_key", DataType::Utf8, false)
                }
                WorkingChangesSurfaceColumn::FileId => Field::new("file_id", DataType::Utf8, true),
                WorkingChangesSurfaceColumn::LixcolGlobal => {
                    Field::new("lixcol_global", DataType::Boolean, false)
                }
                WorkingChangesSurfaceColumn::BeforeChangeId => {
                    Field::new("before_change_id", DataType::Utf8, true)
                }
                WorkingChangesSurfaceColumn::AfterChangeId => {
                    Field::new("after_change_id", DataType::Utf8, true)
                }
                WorkingChangesSurfaceColumn::BeforeCommitId => {
                    Field::new("before_commit_id", DataType::Utf8, true)
                }
                WorkingChangesSurfaceColumn::AfterCommitId => {
                    Field::new("after_commit_id", DataType::Utf8, true)
                }
                WorkingChangesSurfaceColumn::Status => Field::new("status", DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ))
}

fn working_changes_projection_for_scan(
    projection: Option<&Vec<usize>>,
) -> Vec<WorkingChangesSurfaceColumn> {
    let all_columns = vec![
        WorkingChangesSurfaceColumn::EntityId,
        WorkingChangesSurfaceColumn::SchemaKey,
        WorkingChangesSurfaceColumn::FileId,
        WorkingChangesSurfaceColumn::LixcolGlobal,
        WorkingChangesSurfaceColumn::BeforeChangeId,
        WorkingChangesSurfaceColumn::AfterChangeId,
        WorkingChangesSurfaceColumn::BeforeCommitId,
        WorkingChangesSurfaceColumn::AfterCommitId,
        WorkingChangesSurfaceColumn::Status,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn lix_version_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("commit_id", DataType::Utf8, false),
    ]))
}

#[derive(Debug, Clone)]
struct LixVersionProvider {
    schema: SchemaRef,
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
}

impl LixVersionProvider {
    fn new(snapshot: Arc<dyn VersionSurfaceSnapshot>) -> Self {
        Self {
            schema: lix_version_schema(),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixVersionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(LixVersionScanExec::new(
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
        )))
    }
}

#[derive(Debug)]
struct LixVersionScanExec {
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl LixVersionScanExec {
    fn new(
        snapshot: Arc<dyn VersionSurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            snapshot,
            schema,
            projection,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixVersionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixVersionScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixVersionScanExec"),
        }
    }
}

impl ExecutionPlan for LixVersionScanExec {
    fn name(&self) -> &str {
        "LixVersionScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixVersionScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixVersionScanExec only exposes one partition, got {partition}"
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let rows = enqueue_version_surface_scan(
                snapshot,
                VersionSurfaceScanRequest {
                    projection: version_projection_for_scan(projection.as_ref()),
                    limit: None,
                },
            )
            .await?;
            let batches = version_surface_record_batches(
                version_projection_for_scan(projection.as_ref()),
                &rows,
            )?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct VersionSurfaceScanJob {
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
    request: VersionSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<VersionSurfaceRow>, LixError>>,
}

fn version_surface_scan_worker() -> &'static mpsc::Sender<VersionSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<VersionSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<VersionSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-version-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 version-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_versions(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 version-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_version_surface_scan(
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
    request: VersionSurfaceScanRequest,
) -> Result<Vec<VersionSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    version_surface_scan_worker()
        .send(VersionSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue version surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 version surface scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn version_surface_record_batches(
    projection: Vec<VersionSurfaceColumn>,
    rows: &[VersionSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![version_surface_record_batch(&projection, rows)?])
}

fn version_surface_record_batch(
    projection: &[VersionSurfaceColumn],
    rows: &[VersionSurfaceRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            VersionSurfaceColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            VersionSurfaceColumn::Name => {
                string_array(rows.iter().map(|row| Some(row.name.as_str())))
            }
            VersionSurfaceColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            VersionSurfaceColumn::CommitId => {
                string_array(rows.iter().map(|row| Some(row.commit_id.as_str())))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(version_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_version batch: {error}"))
    })
}

fn version_surface_schema(projection: &[VersionSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                VersionSurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                VersionSurfaceColumn::Name => Field::new("name", DataType::Utf8, false),
                VersionSurfaceColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                VersionSurfaceColumn::CommitId => Field::new("commit_id", DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ))
}

fn version_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<VersionSurfaceColumn> {
    let all_columns = vec![
        VersionSurfaceColumn::Id,
        VersionSurfaceColumn::Name,
        VersionSurfaceColumn::Hidden,
        VersionSurfaceColumn::CommitId,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixChangeRoute {
    id: Option<String>,
    entity_id: Option<String>,
    schema_key: Option<String>,
    file_id: Option<String>,
    plugin_key: Option<String>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl LixChangeRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_change_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Untracked => &mut route.untracked,
                        _ => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::Id => &mut route.id,
                        RouteStringField::EntityId => &mut route.entity_id,
                        RouteStringField::SchemaKey => &mut route.schema_key,
                        RouteStringField::FileId => &mut route.file_id,
                        RouteStringField::PluginKey => &mut route.plugin_key,
                        _ => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }
}

fn change_filters_for_route(route: &LixChangeRoute) -> Vec<ChangeSurfaceFilter> {
    let mut filters = Vec::new();
    if let Some(id) = &route.id {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::Id,
            Value::Text(id.clone()),
        ));
    }
    if let Some(entity_id) = &route.entity_id {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::EntityId,
            Value::Text(entity_id.clone()),
        ));
    }
    if let Some(schema_key) = &route.schema_key {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::SchemaKey,
            Value::Text(schema_key.clone()),
        ));
    }
    if let Some(file_id) = &route.file_id {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::FileId,
            Value::Text(file_id.clone()),
        ));
    }
    if let Some(plugin_key) = &route.plugin_key {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::PluginKey,
            Value::Text(plugin_key.clone()),
        ));
    }
    if let Some(untracked) = route.untracked {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::Untracked,
            Value::Boolean(untracked),
        ));
    }
    filters
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixWorkingChangesRoute {
    entity_id: Option<String>,
    schema_key: Option<String>,
    file_id: Option<String>,
    status: Option<String>,
    contradictory: bool,
}

impl LixWorkingChangesRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Expr::BinaryExpr(binary_expr) = filter else {
                continue;
            };
            if binary_expr.op != Operator::Eq {
                continue;
            }

            let predicate = parse_working_changes_route_column_literal_filter(
                &binary_expr.left,
                &binary_expr.right,
            )
            .or_else(|| {
                parse_working_changes_route_column_literal_filter(
                    &binary_expr.right,
                    &binary_expr.left,
                )
            });
            let Some((field, value)) = predicate else {
                continue;
            };

            let slot = match field {
                "entity_id" => &mut route.entity_id,
                "schema_key" => &mut route.schema_key,
                "file_id" => &mut route.file_id,
                "status" => &mut route.status,
                _ => continue,
            };
            assign_route_slot(slot, value, &mut route.contradictory);
        }
        route
    }

    fn working_changes_filters(&self) -> Vec<WorkingChangesSurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(entity_id) = &self.entity_id {
            filters.push(WorkingChangesSurfaceFilter::Eq(
                WorkingChangesSurfaceColumn::EntityId,
                Value::Text(entity_id.clone()),
            ));
        }
        if let Some(schema_key) = &self.schema_key {
            filters.push(WorkingChangesSurfaceFilter::Eq(
                WorkingChangesSurfaceColumn::SchemaKey,
                Value::Text(schema_key.clone()),
            ));
        }
        if let Some(file_id) = &self.file_id {
            filters.push(WorkingChangesSurfaceFilter::Eq(
                WorkingChangesSurfaceColumn::FileId,
                Value::Text(file_id.clone()),
            ));
        }
        if let Some(status) = &self.status {
            filters.push(WorkingChangesSurfaceFilter::Eq(
                WorkingChangesSurfaceColumn::Status,
                Value::Text(status.clone()),
            ));
        }
        filters
    }
}

fn parse_working_changes_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<(&'static str, String)> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    let value = match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => value.clone(),
        _ => return None,
    };

    match column.name.as_str() {
        "entity_id" => Some(("entity_id", value)),
        "schema_key" => Some(("schema_key", value)),
        "file_id" => Some(("file_id", value)),
        "status" => Some(("status", value)),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixStateRoute {
    version_id: Option<String>,
    schema_key: Option<String>,
    entity_id: Option<String>,
    file_id: Option<String>,
    global: Option<bool>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl LixStateRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Global => &mut route.global,
                        RouteBooleanField::Untracked => &mut route.untracked,
                        #[cfg(test)]
                        RouteBooleanField::Hidden
                        | RouteBooleanField::LixcolGlobal
                        | RouteBooleanField::LixcolUntracked => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::VersionId => &mut route.version_id,
                        RouteStringField::SchemaKey => &mut route.schema_key,
                        RouteStringField::EntityId => &mut route.entity_id,
                        RouteStringField::FileId => &mut route.file_id,
                        _ => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn state_filters(&self) -> Vec<StateSurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(schema_key) = &self.schema_key {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::SchemaKey,
                Value::Text(schema_key.clone()),
            ));
        }
        if let Some(entity_id) = &self.entity_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::EntityId,
                Value::Text(entity_id.clone()),
            ));
        }
        if let Some(file_id) = &self.file_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::FileId,
                Value::Text(file_id.clone()),
            ));
        }
        if let Some(global) = self.global {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Global,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.untracked {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Untracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

fn state_by_version_scan_request(
    surface_kind: LixStateSurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixStateRoute,
    limit: Option<usize>,
) -> Result<StateByVersionScanRequest> {
    let version_id = match surface_kind {
        LixStateSurfaceKind::State => Some(default_version_id.to_string()),
        LixStateSurfaceKind::StateByVersion => route.version_id.clone(),
    };
    Ok(StateByVersionScanRequest {
        version_id,
        projection: state_projection_for_scan(surface_kind, projection),
        filters: route.state_filters(),
        limit,
    })
}

fn assign_route_slot<T: PartialEq>(slot: &mut Option<T>, value: T, contradictory: &mut bool) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RoutePredicate {
    Boolean {
        field: RouteBooleanField,
        value: bool,
    },
    String {
        field: RouteStringField,
        value: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteBooleanField {
    Global,
    Untracked,
    #[cfg(test)]
    Hidden,
    #[cfg(test)]
    LixcolGlobal,
    #[cfg(test)]
    LixcolUntracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteStringField {
    VersionId,
    #[cfg(test)]
    LixcolVersionId,
    SchemaKey,
    EntityId,
    FileId,
    PluginKey,
    Id,
    #[cfg(test)]
    Path,
}

fn parse_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "version_id" => parse_string_route(literal, RouteStringField::VersionId),
        "schema_key" => parse_string_route(literal, RouteStringField::SchemaKey),
        "entity_id" => parse_string_route(literal, RouteStringField::EntityId),
        "file_id" => parse_string_route(literal, RouteStringField::FileId),
        "global" => parse_boolean_route(literal, RouteBooleanField::Global),
        "untracked" => parse_boolean_route(literal, RouteBooleanField::Untracked),
        _ => None,
    }
}

#[cfg(test)]
fn parse_file_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_file_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_file_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

#[cfg(test)]
fn parse_directory_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_directory_route_column_literal_filter(&binary_expr.left, &binary_expr.right).or_else(
        || parse_directory_route_column_literal_filter(&binary_expr.right, &binary_expr.left),
    )
}

fn parse_change_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_change_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_change_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

#[cfg(test)]
fn parse_directory_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "id" => parse_string_route(literal, RouteStringField::Id),
        "path" => parse_string_route(literal, RouteStringField::Path),
        "lixcol_version_id" => parse_string_route(literal, RouteStringField::LixcolVersionId),
        "hidden" => parse_boolean_route(literal, RouteBooleanField::Hidden),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
        _ => None,
    }
}

fn parse_change_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "id" => parse_string_route(literal, RouteStringField::Id),
        "entity_id" => parse_string_route(literal, RouteStringField::EntityId),
        "schema_key" => parse_string_route(literal, RouteStringField::SchemaKey),
        "file_id" => parse_string_route(literal, RouteStringField::FileId),
        "plugin_key" => parse_string_route(literal, RouteStringField::PluginKey),
        "untracked" => parse_boolean_route(literal, RouteBooleanField::Untracked),
        _ => None,
    }
}

#[cfg(test)]
fn parse_file_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "id" => parse_string_route(literal, RouteStringField::Id),
        "path" => parse_string_route(literal, RouteStringField::Path),
        "lixcol_version_id" => parse_string_route(literal, RouteStringField::LixcolVersionId),
        "hidden" => parse_boolean_route(literal, RouteBooleanField::Hidden),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
        _ => None,
    }
}

fn parse_string_route(literal: &ScalarValue, field: RouteStringField) -> Option<RoutePredicate> {
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(RoutePredicate::String {
            field,
            value: value.clone(),
        }),
        _ => None,
    }
}

fn parse_boolean_route(literal: &ScalarValue, field: RouteBooleanField) -> Option<RoutePredicate> {
    match literal {
        ScalarValue::Boolean(Some(value)) => Some(RoutePredicate::Boolean {
            field,
            value: *value,
        }),
        _ => None,
    }
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };

    let projected = schema.project(projection).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to project lix_state schema: {error}"))
    })?;
    Ok(Arc::new(projected))
}

fn state_projection_for_scan(
    surface_kind: LixStateSurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<StateSurfaceColumn> {
    let all_columns = match surface_kind {
        LixStateSurfaceKind::State => vec![
            StateSurfaceColumn::EntityId,
            StateSurfaceColumn::SchemaKey,
            StateSurfaceColumn::FileId,
            StateSurfaceColumn::PluginKey,
            StateSurfaceColumn::SnapshotContent,
            StateSurfaceColumn::Metadata,
            StateSurfaceColumn::SchemaVersion,
            StateSurfaceColumn::CreatedAt,
            StateSurfaceColumn::UpdatedAt,
            StateSurfaceColumn::Global,
            StateSurfaceColumn::ChangeId,
            StateSurfaceColumn::CommitId,
            StateSurfaceColumn::Untracked,
        ],
        LixStateSurfaceKind::StateByVersion => vec![
            StateSurfaceColumn::EntityId,
            StateSurfaceColumn::SchemaKey,
            StateSurfaceColumn::FileId,
            StateSurfaceColumn::PluginKey,
            StateSurfaceColumn::SnapshotContent,
            StateSurfaceColumn::Metadata,
            StateSurfaceColumn::SchemaVersion,
            StateSurfaceColumn::CreatedAt,
            StateSurfaceColumn::UpdatedAt,
            StateSurfaceColumn::Global,
            StateSurfaceColumn::ChangeId,
            StateSurfaceColumn::CommitId,
            StateSurfaceColumn::Untracked,
            StateSurfaceColumn::VersionId,
        ],
    };
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn query_result_from_batches(
    result_columns: &[String],
    variant_result_columns: &[bool],
    batches: &[RecordBatch],
) -> Result<QueryResult, LixError> {
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for (column_index, array) in batch.columns().iter().enumerate() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                row.push(scalar_value_to_lix_value(
                    &scalar,
                    variant_result_columns
                        .get(column_index)
                        .copied()
                        .unwrap_or(false),
                ));
            }
            rows.push(row);
        }
    }

    Ok(QueryResult {
        rows,
        columns: result_columns.to_vec(),
    })
}

async fn hydrate_filesystem_blob_columns(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
    result: &mut QueryResult,
) -> Result<(), LixError> {
    if !artifact.surface_names.iter().any(|name| {
        matches!(
            name.as_str(),
            "lix_file" | "lix_file_by_version" | "lix_file_history" | "lix_file_history_by_version"
        )
    }) {
        return Ok(());
    }

    let data_column_indexes = result
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, name)| (name == "data").then_some(index))
        .collect::<Vec<_>>();
    if data_column_indexes.is_empty() {
        return Ok(());
    }

    let mut required_blob_hashes = BTreeSet::new();
    for row in &result.rows {
        for &column_index in &data_column_indexes {
            if let Some(Value::Text(blob_hash)) = row.get(column_index) {
                required_blob_hashes.insert(blob_hash.clone());
            }
        }
    }
    if required_blob_hashes.is_empty() {
        return Ok(());
    }

    let mut blob_data_by_hash = BTreeMap::new();
    for blob_hash in required_blob_hashes {
        blob_data_by_hash.insert(
            blob_hash.clone(),
            backend.load_blob_data_by_hash(&blob_hash).await?,
        );
    }

    for row in &mut result.rows {
        for &column_index in &data_column_indexes {
            let hydrated = match row.get(column_index) {
                Some(Value::Text(blob_hash)) => blob_data_by_hash
                    .get(blob_hash)
                    .cloned()
                    .unwrap_or(None)
                    .map(Value::Blob)
                    .unwrap_or(Value::Null),
                _ => continue,
            };
            row[column_index] = hydrated;
        }
    }

    Ok(())
}

fn scalar_value_to_lix_value(value: &ScalarValue, variant_output: bool) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(value)) => Value::Boolean(*value),
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Int8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int8(None) => Value::Null,
        ScalarValue::Int16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int16(None) => Value::Null,
        ScalarValue::Int32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int32(None) => Value::Null,
        ScalarValue::Int64(Some(value)) => Value::Integer(*value),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::UInt8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt8(None) => Value::Null,
        ScalarValue::UInt16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt16(None) => Value::Null,
        ScalarValue::UInt32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt32(None) => Value::Null,
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Value::Integer(value),
            Err(_) => Value::Text(value.to_string()),
        },
        ScalarValue::UInt64(None) => Value::Null,
        ScalarValue::Float32(Some(value)) => Value::Real(f64::from(*value)),
        ScalarValue::Float32(None) => Value::Null,
        ScalarValue::Float64(Some(value)) => Value::Real(*value),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => {
            if variant_output {
                serde_json::from_str::<serde_json::Value>(value)
                    .map(Value::Json)
                    .unwrap_or_else(|_| Value::Text(value.clone()))
            } else {
                Value::Text(value.clone())
            }
        }
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Value::Null
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            if variant_output {
                serde_json::from_slice::<serde_json::Value>(value)
                    .map(Value::Json)
                    .unwrap_or_else(|_| Value::Blob(value.clone()))
            } else {
                Value::Blob(value.clone())
            }
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Null,
        other => Value::Text(other.to_string()),
    }
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use datafusion::datasource::ViewTable;

    use super::{
        build_session_for_read_with_borrowed_backend, build_session_for_read_with_shared_backend,
        execute_read_with_backend, execute_read_with_shared_backend, parse_directory_route_filter,
        parse_file_route_filter, parse_route_filter, state_history_route_from_sql,
        validate_variant_text_coercions, LixStateHistoryProvider, PreparedSql2ReadArtifact,
        RouteBooleanField, RoutePredicate, RouteStringField,
    };
    use crate::catalog::SurfaceColumnType;
    use crate::live_state::{
        open_state_by_version_snapshot_with_shared_backend, StateByVersionScanRequest,
        StateSurfaceColumn, StateSurfaceFilter,
    };
    use crate::session::AdditionalSessionOptions;
    use crate::sql2::{
        prepared_entity_view_plans_for_registry, prepared_filesystem_view_plans_for_registry,
    };
    use crate::test_support::{boot_test_engine, TestSqliteBackendEvent};
    use crate::{CreateVersionOptions, LixBackend, TransactionBeginMode, Value};
    use serde_json::json;

    async fn setup_sql2_state_fixture(
    ) -> Result<(crate::test_support::TestSqliteBackend, crate::Session), crate::LixError> {
        let (backend, _lix, session) = boot_test_engine().await?;
        session
            .register_schema(&json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;
        session
            .register_schema(&json!({
                "x-lix-key": "other_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;
        session
            .register_schema(&json!({
                "x-lix-key": "stable_scalar_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "count": { "type": "integer" },
                    "score": { "type": "number" },
                    "enabled": { "type": "boolean" }
                },
                "required": ["name", "count", "score", "enabled"],
                "additionalProperties": false
            }))
            .await?;

        session
            .create_version(CreateVersionOptions {
                id: Some("version-a".to_string()),
                name: Some("version-a".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-b".to_string()),
                name: Some("version-b".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_state_schema', NULL, 'version-a', NULL, '{\"value\":\"A\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-b', 'test_state_schema', NULL, 'version-b', NULL, '{\"value\":\"B\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'scalar-a', 'stable_scalar_schema', NULL, 'version-a', NULL, '{\"name\":\"alpha\",\"count\":7,\"score\":3.5,\"enabled\":true}', '1'\
                 )",
                &[],
            )
            .await?;
        let sql2_session = session
            .open_additional_session(AdditionalSessionOptions {
                active_version_id: Some("version-a".to_string()),
                origin_key: Some("engine:sql2".to_string()),
                ..AdditionalSessionOptions::default()
            })
            .await?;
        sql2_session
            .execute(
                "INSERT INTO lix_file (id, path, data, lixcol_metadata) VALUES ('file-a', '/hello.txt', X'68656C6C6F', '{\"kind\":\"text\"}')",
                &[],
            )
            .await?;
        sql2_session
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) VALUES ('dir-a', '/docs/', NULL, 'docs')",
                &[],
            )
            .await?;
        Ok((backend, sql2_session))
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-datafusion-test".to_string())
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

    fn force_entity_view_column_to_variant(
        artifact: &mut PreparedSql2ReadArtifact,
        surface_name: &str,
        column_name: &str,
    ) {
        let plan = artifact
            .entity_views
            .get_mut(surface_name)
            .expect("surface should have an entity-view plan");
        *plan
            .column_types
            .get_mut(column_name)
            .expect("surface should expose the requested column type") = SurfaceColumnType::Variant;
        plan.column_plans
            .get_mut(column_name)
            .expect("surface should expose the requested column plan")
            .column_type = SurfaceColumnType::Variant;
    }

    #[test]
    fn parses_string_route_filters() {
        let filter =
            datafusion::logical_expr::col("schema_key").eq(datafusion::logical_expr::lit("demo"));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::SchemaKey,
                value: "demo".to_string(),
            })
        );
    }

    #[test]
    fn parses_version_id_route_filters() {
        let filter =
            datafusion::logical_expr::col("version_id").eq(datafusion::logical_expr::lit("v1"));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::VersionId,
                value: "v1".to_string(),
            })
        );
    }

    #[test]
    fn parses_boolean_route_filters() {
        let filter =
            datafusion::logical_expr::col("untracked").eq(datafusion::logical_expr::lit(true));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::Boolean {
                field: RouteBooleanField::Untracked,
                value: true,
            })
        );
    }

    #[test]
    fn parses_file_route_filters() {
        let filter =
            datafusion::logical_expr::col("path").eq(datafusion::logical_expr::lit("/hello.txt"));

        assert_eq!(
            parse_file_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::Path,
                value: "/hello.txt".to_string(),
            })
        );
    }

    #[test]
    fn parses_file_by_version_route_filters() {
        let filter = datafusion::logical_expr::col("lixcol_version_id")
            .eq(datafusion::logical_expr::lit("version-a"));

        assert_eq!(
            parse_file_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::LixcolVersionId,
                value: "version-a".to_string(),
            })
        );
    }

    #[test]
    fn parses_directory_route_filters() {
        let filter =
            datafusion::logical_expr::col("path").eq(datafusion::logical_expr::lit("/docs/"));

        assert_eq!(
            parse_directory_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::Path,
                value: "/docs/".to_string(),
            })
        );
    }

    #[test]
    fn parses_directory_by_version_route_filters() {
        let filter = datafusion::logical_expr::col("lixcol_version_id")
            .eq(datafusion::logical_expr::lit("version-a"));

        assert_eq!(
            parse_directory_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::LixcolVersionId,
                value: "version-a".to_string(),
            })
        );
    }

    #[test]
    fn builds_session_and_executes_lix_state_query() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' ORDER BY entity_id".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("session should build");
                let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
                let batches = dataframe.collect().await.expect("query should execute");
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
            })
        });
    }

    #[test]
    fn shared_backend_path_defers_state_reads_until_execution() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' ORDER BY entity_id".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                backend.clear_query_log();
                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend session should build");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .all(|sql| !sql.contains("lix_registered_schema")
                            && !sql.contains("change_commit_by_change_id")
                            && !sql.contains("lix_internal_live")),
                    "session setup should not query live_state on shared-backend path"
                );

                let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
                let _batches = dataframe.collect().await.expect("query should execute");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("test_state_schema")),
                    "execution should query live_state on shared-backend path"
                );
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .all(|sql| !sql.contains("other_state_schema")),
                    "schema_key pushdown should avoid scanning unrelated state schemas"
                );
            })
        });
    }

    #[test]
    fn borrowed_backend_path_registers_entity_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value FROM test_state_schema".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["test_state_schema".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["test_state_schema".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("entity session should build");
                let provider = ctx
                    .table_provider("test_state_schema")
                    .await
                    .expect("entity surface should be registered");

                assert!(
                    provider.as_any().is::<ViewTable>(),
                    "entity surfaces should register as native DataFusion views"
                );
                assert!(
                    provider.get_logical_plan().is_some(),
                    "registered entity view should expose a logical plan"
                );
            })
        });
    }

    #[test]
    fn borrowed_backend_path_registers_filesystem_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id FROM lix_file".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec![
                        "lix_file".to_string(),
                        "lix_file_by_version".to_string(),
                        "lix_directory".to_string(),
                        "lix_directory_by_version".to_string(),
                    ],
                    entity_views: BTreeMap::new(),
                    filesystem_views: prepared_filesystem_view_plans_for_registry(
                        &registry,
                        &[
                            "lix_file".to_string(),
                            "lix_file_by_version".to_string(),
                            "lix_directory".to_string(),
                            "lix_directory_by_version".to_string(),
                        ],
                    ),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("filesystem session should build");

                for surface_name in &artifact.surface_names {
                    let provider = ctx
                        .table_provider(surface_name)
                        .await
                        .expect("filesystem surface should be registered");
                    assert!(
                        provider.as_any().is::<ViewTable>(),
                        "filesystem surfaces should register as native DataFusion views"
                    );
                    assert!(
                        provider.get_logical_plan().is_some(),
                        "registered filesystem view should expose a logical plan"
                    );
                }
            })
        });
    }

    #[test]
    fn borrowed_backend_path_registers_history_entity_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value FROM test_state_schema_history".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["test_state_schema_history".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["test_state_schema_history".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("history entity session should build");
                let provider = ctx
                    .table_provider("test_state_schema_history")
                    .await
                    .expect("history entity surface should be registered");

                assert!(
                    provider.as_any().is::<ViewTable>(),
                    "history entity surfaces should register as native DataFusion views"
                );
                assert!(
                    provider.get_logical_plan().is_some(),
                    "registered history entity view should expose a logical plan"
                );
                let schema = provider.schema();
                let field_names = schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>();
                assert!(
                    field_names.contains(&"value"),
                    "history entity view should expose schema-defined payload columns"
                );
                assert!(
                    field_names.contains(&"lixcol_entity_id"),
                    "history entity view should expose derived history state columns"
                );
                assert!(
                    field_names.contains(&"lixcol_root_commit_id"),
                    "history entity view should expose derived history root columns"
                );
                assert!(
                    field_names.contains(&"lixcol_version_id"),
                    "history entity view should expose derived history version columns"
                );
                assert!(
                    field_names.contains(&"lixcol_depth"),
                    "history entity view should expose derived history depth columns"
                );
            })
        });
    }

    #[test]
    fn borrowed_backend_path_registers_history_filesystem_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id FROM lix_file_history".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec![
                        "lix_file_history".to_string(),
                        "lix_file_history_by_version".to_string(),
                        "lix_directory_history".to_string(),
                    ],
                    entity_views: BTreeMap::new(),
                    filesystem_views: prepared_filesystem_view_plans_for_registry(
                        &registry,
                        &[
                            "lix_file_history".to_string(),
                            "lix_file_history_by_version".to_string(),
                            "lix_directory_history".to_string(),
                        ],
                    ),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("history filesystem session should build");

                for surface_name in &artifact.surface_names {
                    let provider = ctx
                        .table_provider(surface_name)
                        .await
                        .expect("history filesystem surface should be registered");
                    assert!(
                        provider.as_any().is::<ViewTable>(),
                        "history filesystem surfaces should register as native DataFusion views"
                    );
                    assert!(
                        provider.get_logical_plan().is_some(),
                        "registered history filesystem view should expose a logical plan"
                    );
                }
            })
        });
    }

    #[test]
    fn shared_backend_path_registers_history_entity_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value FROM test_state_schema_history".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["test_state_schema_history".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["test_state_schema_history".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend history entity session should build");
                let provider = ctx
                    .table_provider("test_state_schema_history")
                    .await
                    .expect("shared-backend history entity surface should be registered");

                assert!(
                    provider.as_any().is::<ViewTable>(),
                    "shared-backend history entity surfaces should register as native DataFusion views"
                );
                assert!(
                    provider.get_logical_plan().is_some(),
                    "shared-backend registered history entity view should expose a logical plan"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_registers_filesystem_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id FROM lix_file".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec![
                        "lix_file".to_string(),
                        "lix_file_by_version".to_string(),
                        "lix_directory".to_string(),
                        "lix_directory_by_version".to_string(),
                    ],
                    entity_views: BTreeMap::new(),
                    filesystem_views: prepared_filesystem_view_plans_for_registry(
                        &registry,
                        &[
                            "lix_file".to_string(),
                            "lix_file_by_version".to_string(),
                            "lix_directory".to_string(),
                            "lix_directory_by_version".to_string(),
                        ],
                    ),
                };

                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend filesystem session should build");

                for surface_name in &artifact.surface_names {
                    let provider = ctx
                        .table_provider(surface_name)
                        .await
                        .expect("filesystem surface should be registered");
                    assert!(
                        provider.as_any().is::<ViewTable>(),
                        "shared-backend filesystem surfaces should register as native DataFusion views"
                    );
                    assert!(
                        provider.get_logical_plan().is_some(),
                        "shared-backend registered filesystem view should expose a logical plan"
                    );
                }
            })
        });
    }

    #[test]
    fn shared_backend_path_registers_history_filesystem_surfaces_as_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id FROM lix_file_history".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec![
                        "lix_file_history".to_string(),
                        "lix_file_history_by_version".to_string(),
                        "lix_directory_history".to_string(),
                    ],
                    entity_views: BTreeMap::new(),
                    filesystem_views: prepared_filesystem_view_plans_for_registry(
                        &registry,
                        &[
                            "lix_file_history".to_string(),
                            "lix_file_history_by_version".to_string(),
                            "lix_directory_history".to_string(),
                        ],
                    ),
                };

                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend history filesystem session should build");

                for surface_name in &artifact.surface_names {
                    let provider = ctx
                        .table_provider(surface_name)
                        .await
                        .expect("shared-backend history filesystem surface should be registered");
                    assert!(
                        provider.as_any().is::<ViewTable>(),
                        "shared-backend history filesystem surfaces should register as native DataFusion views"
                    );
                    assert!(
                        provider.get_logical_plan().is_some(),
                        "shared-backend registered history filesystem view should expose a logical plan"
                    );
                }
            })
        });
    }

    #[test]
    fn borrowed_backend_path_registers_lix_state_history_as_native_relation() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state_history".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_history".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("state-history session should build");
                let provider = ctx
                    .table_provider("lix_state_history")
                    .await
                    .expect("lix_state_history should be registered");

                assert!(
                    provider.as_any().is::<LixStateHistoryProvider>(),
                    "lix_state_history should register as a native sql2 base relation"
                );
                assert!(
                    !provider.as_any().is::<ViewTable>(),
                    "lix_state_history should not register as a DataFusion view"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_opens_read_transaction_for_query_snapshot() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                backend.clear_query_log();
                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let _ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend session should build");

                let begin_modes = backend
                    .recorded_events()
                    .into_iter()
                    .filter_map(|event| match event {
                        TestSqliteBackendEvent::BeginTransaction { mode } => Some(mode),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    begin_modes,
                    vec![TransactionBeginMode::Read],
                    "shared-backend sql2 path should open one read transaction as the query snapshot"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_pushes_entity_constraint_into_source_scan() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                backend.clear_query_log();
                let _result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                        .await
                        .expect("sql2 shared-backend read should execute");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("\"entity_id\" = 'entity-a'")
                            || sql.contains("entity_id = 'entity-a'")),
                    "entity_id filter should be pushed into live_state source scans"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_derives_required_columns_from_projection() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                backend.clear_query_log();
                let _result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                        .await
                        .expect("sql2 shared-backend read should execute");

                let state_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| sql.contains("lix_internal_live_v1_test_state_schema"))
                    .collect::<Vec<_>>();
                assert!(
                    !state_scan_sql.is_empty(),
                    "expected sql2 read to scan the test_state_schema live table"
                );
                assert!(
                    state_scan_sql.iter().all(|sql| !sql.contains("\"value\"")),
                    "entity-only projection should avoid loading dynamic state columns: {state_scan_sql:?}"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_pushes_limit_only_for_safe_untracked_scans() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-untracked-a', 'test_state_schema', NULL, NULL, '{\"value\":\"UA\"}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("first untracked row should insert");
                session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-untracked-b', 'test_state_schema', NULL, NULL, '{\"value\":\"UB\"}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("second untracked row should insert");

                let untracked_snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::new(backend.clone()))
                        .await
                        .expect("shared-backend snapshot should open");

                backend.clear_query_log();
                let _batches = untracked_snapshot
                    .scan_state_by_version_batches(&StateByVersionScanRequest {
                        version_id: Some(crate::version::GLOBAL_VERSION_ID.to_string()),
                        projection: vec![StateSurfaceColumn::EntityId],
                        filters: vec![
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::SchemaKey,
                                Value::Text("test_state_schema".to_string()),
                            ),
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::Untracked,
                                Value::Boolean(true),
                            ),
                        ],
                        limit: Some(1),
                    })
                    .await
                    .expect("untracked state-surface read should execute");

                let untracked_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| {
                        sql.contains("lix_internal_live_v1_test_state_schema")
                            && sql.contains("untracked = true")
                    })
                    .collect::<Vec<_>>();
                assert!(
                    untracked_scan_sql.iter().any(|sql| sql.contains("LIMIT 1")),
                    "single-lane untracked scan should receive the pushed limit: {untracked_scan_sql:?}"
                );
                drop(untracked_snapshot);

                let tracked_snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::new(backend.clone()))
                        .await
                        .expect("shared-backend snapshot should open");

                backend.clear_query_log();
                let _batches = tracked_snapshot
                    .scan_state_by_version_batches(&StateByVersionScanRequest {
                        version_id: Some("version-a".to_string()),
                        projection: vec![StateSurfaceColumn::EntityId],
                        filters: vec![
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::SchemaKey,
                                Value::Text("test_state_schema".to_string()),
                            ),
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::Untracked,
                                Value::Boolean(false),
                            ),
                        ],
                        limit: Some(1),
                    })
                    .await
                    .expect("tracked state-surface read should execute");

                let tracked_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| {
                        sql.contains("lix_internal_live_v1_test_state_schema")
                            && sql.contains("untracked = false")
                            && sql.contains("is_tombstone = 0")
                    })
                    .collect::<Vec<_>>();
                assert!(
                    tracked_scan_sql.iter().all(|sql| !sql.contains("LIMIT 1")),
                    "tracked scans should keep source-side limits disabled: {tracked_scan_sql:?}"
                );
            })
        });
    }

    #[test]
    fn execute_read_uses_active_version_snapshot_for_lix_state() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.columns, vec!["entity_id", "snapshot_content"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-a".to_string()),
                        Value::Text("{\"value\":\"A\"}".to_string())
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_supports_top_level_select_without_from_over_scalar_subquery() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT COALESCE((SELECT snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'missing-entity' LIMIT 1), 'missing')".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0], vec![Value::Text("missing".to_string())]);
            })
        });
    }

    #[test]
    fn execute_read_exposes_commit_id_for_tracked_lix_state_rows() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT commit_id FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.rows.len(), 1);
                match &result.rows[0][0] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected text commit_id, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_uses_execution_time_state_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                backend.clear_query_log();
                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend read should execute");
                assert_eq!(result.rows.len(), 1);
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("lix_registered_schema")),
                    "shared-backend execution should query live_state at execution time"
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_state_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, version_id, snapshot_content FROM lix_state_by_version WHERE version_id = 'version-b' AND schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_by_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend by-version read should execute");
                assert_eq!(
                    result.columns,
                    vec!["entity_id", "version_id", "snapshot_content"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-b".to_string()),
                        Value::Text("version-b".to_string()),
                        Value::Text("{\"value\":\"B\"}".to_string()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_broad_lix_state_by_version_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state_by_version WHERE schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_by_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("broad by-version read should succeed");
                assert!(
                    result.rows.iter().any(|row| row[0] == Value::Text("entity-a".to_string()))
                        && result.rows.iter().any(|row| row[0] == Value::Text("entity-b".to_string())),
                    "expected broad by-version read to include rows from multiple visible versions: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_state_history() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, root_commit_id, depth \
                          FROM lix_state_history \
                         WHERE schema_key = 'test_state_schema' \
                         ORDER BY entity_id, depth"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_history".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend state-history read should execute");
                assert_eq!(result.columns, vec!["entity_id", "root_commit_id", "depth"]);
                assert!(
                    !result.rows.is_empty(),
                    "expected lix_state_history rows for the active version lineage"
                );
                assert!(
                    result
                        .rows
                        .iter()
                        .any(|row| row.first() == Some(&Value::Text("entity-a".to_string()))),
                    "expected version-a history row in lix_state_history results: {:?}",
                    result.rows
                );
                assert!(
                    result
                        .rows
                        .iter()
                        .all(|row| matches!(row.get(1), Some(Value::Text(_)))
                            && matches!(row.get(2), Some(Value::Integer(_)))),
                    "expected root_commit_id text and depth integer values: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_preserves_payload_projection_and_filtering_over_history_entity_views(
    ) {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value, lixcol_depth \
                          FROM test_state_schema_history \
                         WHERE value = 'A' \
                         ORDER BY lixcol_depth"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["test_state_schema_history".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["test_state_schema_history".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("history entity payload read should execute through sql2");
                assert_eq!(result.columns, vec!["value", "lixcol_depth"]);
                assert!(
                    !result.rows.is_empty(),
                    "expected history entity payload filtering to return rows"
                );
                for row in &result.rows {
                    assert_eq!(row.len(), 2);
                    assert_eq!(row[0], Value::Text("A".to_string()));
                    assert!(
                        matches!(row[1], Value::Integer(_)),
                        "expected projected history depth integer, got {:?}",
                        row[1]
                    );
                }
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_returns_json_payload_columns_as_json_values() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('variant-read', 'value-a')",
                        &[],
                    )
                    .await
                    .expect("seed insert should succeed");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value FROM lix_key_value WHERE key = 'variant-read'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_key_value".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["lix_key_value".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend JSON read should execute");
                assert_eq!(result.columns, vec!["value"]);
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Text("\"value-a\"".to_string())]]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_preserves_json_results_through_aliases() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('variant-alias', 'value-a')",
                        &[],
                    )
                    .await
                    .expect("seed insert should succeed");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value AS payload FROM lix_key_value WHERE key = 'variant-alias'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_key_value".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["lix_key_value".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend aliased JSON read should execute");
                assert_eq!(result.columns, vec!["payload"]);
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Text("\"value-a\"".to_string())]]
                );
            })
        });
    }

    #[test]
    fn mixed_json_kind_entity_views_do_not_trigger_variant_text_validation() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .register_schema(&json!({
                        "x-lix-key": "json_union_schema",
                        "x-lix-version": "1",
                        "type": "object",
                        "properties": {
                            "value": {
                                "anyOf": [
                                    { "type": "string" },
                                    { "type": "object" }
                                ]
                            }
                        },
                        "additionalProperties": false
                    }))
                    .await
                    .expect("schema registration should succeed");

                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value FROM json_union_schema WHERE value = 'hello'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["json_union_schema".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["json_union_schema".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                validate_variant_text_coercions(&artifact.sql, &artifact)
                    .expect("schema-derived JSON unions should not be treated as variant text");

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("schema-derived JSON query should execute");
                assert_eq!(result.columns, vec!["value"]);
                assert!(result.rows.is_empty());
            })
        });
    }

    #[test]
    fn explicit_variant_columns_require_explicit_cast_for_text_comparison() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('variant-compare', 'value-a')",
                        &[],
                    )
                    .await
                    .expect("seed insert should succeed");
                let registry = session.public_surface_registry();
                let mut artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT key FROM lix_key_value WHERE value = 'value-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_key_value".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["lix_key_value".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };
                force_entity_view_column_to_variant(&mut artifact, "lix_key_value", "value");

                let result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact).await;
                assert!(
                    result.is_err(),
                    "explicit variant text comparison should require an explicit cast or extraction"
                );
            })
        });
    }

    #[test]
    fn explicit_variant_columns_allow_explicit_text_decode() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('variant-decode', 'value-a')",
                        &[],
                    )
                    .await
                    .expect("seed insert should succeed");
                let registry = session.public_surface_registry();
                let mut artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT lix_text_decode(value) AS payload_text \
                          FROM lix_key_value \
                         WHERE key = 'variant-decode'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_key_value".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["lix_key_value".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };
                force_entity_view_column_to_variant(&mut artifact, "lix_key_value", "value");

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("explicit text decode over explicit variant should execute");
                assert_eq!(result.columns, vec!["payload_text"]);
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Text("\"value-a\"".to_string())]]
                );
            })
        });
    }

    #[test]
    fn explicit_variant_columns_allow_explicit_json_extract() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) \
                         VALUES ('variant-object', lix_json('{\"kind\":\"greeting\",\"text\":\"hello\"}'))",
                        &[],
                    )
                    .await
                    .expect("seed insert should succeed");
                let registry = session.public_surface_registry();
                let mut artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT lix_json_extract(value, 'text') AS payload_text \
                          FROM lix_key_value \
                         WHERE key = 'variant-object'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_key_value".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["lix_key_value".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };
                force_entity_view_column_to_variant(&mut artifact, "lix_key_value", "value");

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("explicit json extract over explicit variant should execute");
                assert_eq!(result.columns, vec!["payload_text"]);
                assert_eq!(result.rows, vec![vec![Value::Text("hello".to_string())]]);
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_preserves_json_null_values() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_state_by_version (\
                         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'variant-null', 'lix_key_value', NULL, 'version-a', NULL, '{\"key\":\"variant-null\",\"value\":null}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("seed insert should succeed");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT value FROM lix_key_value WHERE key = 'variant-null'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_key_value".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["lix_key_value".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend JSON null read should execute");
                assert_eq!(result.columns, vec!["value"]);
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Json(serde_json::Value::Null)]]
                );
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Json(serde_json::Value::Null)]]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_preserves_table_like_scalar_payload_behavior() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT name, count, score, enabled \
                          FROM stable_scalar_schema \
                         WHERE name = 'alpha' \
                           AND count = 7 \
                           AND score = 3.5 \
                           AND enabled = true"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["stable_scalar_schema".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["stable_scalar_schema".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("stable scalar entity query should execute");
                assert_eq!(
                    result,
                    crate::QueryResult {
                        columns: vec![
                            "name".to_string(),
                            "count".to_string(),
                            "score".to_string(),
                            "enabled".to_string(),
                        ],
                        rows: vec![vec![
                            Value::Text("alpha".to_string()),
                            Value::Integer(7),
                            Value::Real(3.5),
                            Value::Boolean(true),
                        ]],
                    }
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_state_history() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT COUNT(*) AS total \
                          FROM lix_state_history \
                         WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_history".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 borrowed-backend state-history read should execute");
                assert_eq!(result.columns, vec!["total"]);
                assert!(
                    matches!(result.rows.first(), Some(row) if matches!(row.first(), Some(Value::Integer(value)) if *value > 0)),
                    "expected positive lix_state_history row count, got {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_file() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, data FROM lix_file WHERE path = '/hello.txt'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 file read should execute");
                assert_eq!(result.columns, vec!["id", "path", "data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("file-a".to_string()),
                        Value::Text("/hello.txt".to_string()),
                        Value::Blob(b"hello".to_vec()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_scans_lix_file() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path FROM lix_file ORDER BY path".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend file read should execute");
                assert_eq!(result.columns, vec!["id", "path"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("file-a".to_string()),
                            Value::Text("/hello.txt".to_string()),
                        ]),
                    "expected inserted file in lix_file results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_file_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, lixcol_version_id \
                          FROM lix_file_by_version \
                         WHERE lixcol_version_id = 'version-a' \
                         ORDER BY path"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file_by_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend file-by-version read should execute");
                assert_eq!(result.columns, vec!["id", "path", "lixcol_version_id"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("file-a".to_string()),
                            Value::Text("/hello.txt".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected inserted file in lix_file_by_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_hydrates_file_history_data_from_blob_hashes() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT data FROM lix_file_history WHERE id = 'file-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file_history".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: prepared_filesystem_view_plans_for_registry(
                        &registry,
                        &["lix_file_history".to_string()],
                    ),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend file history read should execute");
                assert_eq!(result.columns, vec!["data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0], vec![Value::Blob(b"hello".to_vec())]);
            })
        });
    }

    #[test]
    fn state_history_route_from_sql_recognizes_lixcol_root_and_depth_aliases() {
        let route = state_history_route_from_sql(
            "SELECT data \
             FROM lix_file_history \
             WHERE lixcol_root_commit_id = 'root-123' \
               AND lixcol_depth >= 1 \
               AND lixcol_depth <= 3",
        )
        .expect("route extraction should succeed");

        assert_eq!(route.root_commit_ids, vec!["root-123".to_string()]);
        assert_eq!(route.min_depth, Some(1));
        assert_eq!(route.max_depth, Some(3));
    }

    #[test]
    fn execute_read_with_backend_reads_lix_directory() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, name FROM lix_directory WHERE path = '/docs/'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_directory".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 directory read should execute");
                assert_eq!(result.columns, vec!["id", "path", "name"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("dir-a".to_string()),
                        Value::Text("/docs/".to_string()),
                        Value::Text("docs".to_string()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_scans_lix_directory() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path FROM lix_directory ORDER BY path".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_directory".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend directory read should execute");
                assert_eq!(result.columns, vec!["id", "path"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("dir-a".to_string()),
                            Value::Text("/docs/".to_string()),
                        ]),
                    "expected inserted directory in lix_directory results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_directory_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, lixcol_version_id \
                          FROM lix_directory_by_version \
                         WHERE lixcol_version_id = 'version-a' \
                         ORDER BY path"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_directory_by_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend directory-by-version read should execute");
                assert_eq!(result.columns, vec!["id", "path", "lixcol_version_id"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("dir-a".to_string()),
                            Value::Text("/docs/".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected inserted directory in lix_directory_by_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, name FROM lix_version WHERE id = 'version-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 version read should execute");
                assert_eq!(result.columns, vec!["id", "name"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("version-a".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected version-a in lix_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_scans_lix_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, hidden FROM lix_version ORDER BY id".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend version read should execute");
                assert_eq!(result.columns, vec!["id", "hidden"]);
                assert!(
                    result
                        .rows
                        .iter()
                        .any(|row| row.first() == Some(&Value::Text("version-a".to_string()))),
                    "expected version-a in lix_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_counts_lix_change() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT COUNT(*) AS c FROM lix_change".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_change".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 change count should execute");
                assert_eq!(result.columns, vec!["c"]);
                let count = result.rows.first().and_then(|row| row.first()).cloned();
                assert!(
                    matches!(count, Some(Value::Integer(value)) if value > 0),
                    "expected positive lix_change count, got {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_supports_literal_only_lix_change_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_change".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 literal-only change read should execute");
                assert_eq!(result.columns, vec!["marker"]);
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Text("observe-shared-sentinel".to_string())]]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_change_by_id() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let raw = backend
                    .execute(
                        "SELECT id FROM lix_internal_change WHERE entity_id = 'file-a' ORDER BY created_at DESC LIMIT 1",
                        &[],
                    )
                    .await
                    .expect("raw change lookup should execute");
                let change_id = match raw.rows.first().and_then(|row| row.first()) {
                    Some(Value::Text(value)) => value.clone(),
                    other => panic!("expected raw file-a change id, got {other:?}"),
                };
                let artifact = PreparedSql2ReadArtifact {
                    sql: format!(
                        "SELECT id, entity_id, schema_key FROM lix_change WHERE id = '{change_id}'"
                    ),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_change".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 change read should execute");
                assert_eq!(result.columns, vec!["id", "entity_id", "schema_key"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text(change_id));
                assert_eq!(result.rows[0][1], Value::Text("file-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_mixed_file_queries() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT f.id, fbv.lixcol_version_id \
                          FROM lix_file f \
                          JOIN lix_file_by_version fbv \
                            ON f.id = fbv.id \
                         WHERE fbv.lixcol_version_id = 'version-a'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file".to_string(), "lix_file_by_version".to_string()],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend mixed file read should execute");
                assert_eq!(result.columns, vec!["id", "lixcol_version_id"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("file-a".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected joined active/by-version file row: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_count_exists_and_filters_over_entity_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT COUNT(*) AS total \
                          FROM test_state_schema \
                         WHERE value = 'A' \
                           AND EXISTS(SELECT 1 FROM test_state_schema WHERE value = 'A')"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["test_state_schema".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["test_state_schema".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend entity-view aggregate read should execute");
                assert_eq!(result.columns, vec!["total"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0], vec![Value::Integer(1)]);
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_joins_over_entity_views() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let registry = session.public_surface_registry();
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT s.value, st.entity_id \
                          FROM test_state_schema s \
                          JOIN lix_state st \
                            ON s.lixcol_entity_id = st.entity_id \
                         WHERE s.value = 'A' \
                           AND st.schema_key = 'test_state_schema'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["test_state_schema".to_string(), "lix_state".to_string()],
                    entity_views: prepared_entity_view_plans_for_registry(
                        &registry,
                        &["test_state_schema".to_string(), "lix_state".to_string()],
                    ),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend entity-view join read should execute");
                assert_eq!(result.columns, vec!["value", "entity_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("A".to_string()),
                        Value::Text("entity-a".to_string()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_mixed_state_queries() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT s.entity_id, sbv.version_id \
                          FROM lix_state s \
                          JOIN lix_state_by_version sbv \
                            ON s.entity_id = sbv.entity_id \
                         WHERE s.schema_key = 'test_state_schema' \
                           AND sbv.schema_key = 'test_state_schema' \
                           AND sbv.version_id = 'version-a'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec![
                        "lix_state".to_string(),
                        "lix_state_by_version".to_string(),
                    ],
                    entity_views: BTreeMap::new(),
                    filesystem_views: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend mixed read should execute");
                assert_eq!(result.columns, vec!["entity_id", "version_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-a".to_string()),
                        Value::Text("version-a".to_string()),
                    ]
                );
            })
        });
    }
}
