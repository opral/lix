use super::{
    required_bool_value_index, required_text_value_index, text_from_value,
    write_resolve_backend_error, WriteResolveError,
};
use crate::sql::public::planner::ir::{CanonicalStateRowKey, CanonicalStateSelector, PlannedWrite};
use crate::sql::public::planner::semantics::surface_semantics::{
    public_selector_column_name, public_selector_version_column,
};
use crate::sql::public::runtime::execute_public_read_query_strict;
use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select,
    SelectFlavor, SelectItem, SetExpr, TableFactor, TableWithJoins, Value as SqlValue,
    ValueWithSpan,
};

pub(super) async fn query_entity_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let mut selector_columns = vec!["lixcol_entity_id"];
    if let Some(version_column) = selector.version_column.as_deref() {
        selector_columns.push(version_column);
    }
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector,
            &selector_columns,
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut selector_rows = Vec::new();
    for row in query_result.rows {
        let selector_row = CanonicalStateRowKey {
            entity_id: required_text_value_index(&row, 0, "lixcol_entity_id")?,
            file_id: None,
            plugin_key: None,
            schema_version: None,
            version_id: selector
                .version_column
                .as_deref()
                .map(|version_column| required_text_value_index(&row, 1, version_column))
                .transpose()?,
            global: None,
            untracked: None,
            writer_key: None,
        };
        if !selector_rows
            .iter()
            .any(|existing| existing == &selector_row)
        {
            selector_rows.push(selector_row);
        }
    }
    Ok(selector_rows)
}

pub(super) async fn query_text_selector_values_for_write_selector(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    selector_column: &str,
    error_message: &str,
) -> Result<Vec<String>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector,
            &[selector_column],
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut values = Vec::new();
    for row in query_result.rows {
        let Some(value) = row.first().and_then(text_from_value) else {
            return Err(WriteResolveError {
                message: error_message.to_string(),
            });
        };
        if !values.iter().any(|existing| existing == &value) {
            values.push(value);
        }
    }
    Ok(values)
}

pub(super) async fn query_state_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let mut selector_columns = vec!["entity_id", "file_id", "plugin_key", "schema_version"];
    if let Some(version_column) = selector.version_column.as_deref() {
        selector_columns.push(version_column);
    }
    selector_columns.push("global");
    selector_columns.push("untracked");
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector,
            &selector_columns,
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut selector_rows = Vec::new();
    for row in query_result.rows {
        let version_offset = usize::from(selector.version_column.is_some());
        let selector_row = CanonicalStateRowKey {
            entity_id: required_text_value_index(&row, 0, "entity_id")?,
            file_id: Some(required_text_value_index(&row, 1, "file_id")?),
            plugin_key: Some(required_text_value_index(&row, 2, "plugin_key")?),
            schema_version: Some(required_text_value_index(&row, 3, "schema_version")?),
            version_id: selector
                .version_column
                .as_deref()
                .map(|version_column| required_text_value_index(&row, 4, version_column))
                .transpose()?,
            global: Some(required_bool_value_index(
                &row,
                4 + version_offset,
                "global",
            )?),
            untracked: Some(required_bool_value_index(
                &row,
                5 + version_offset,
                "untracked",
            )?),
            writer_key: None,
        };
        if !selector_rows
            .iter()
            .any(|existing| existing == &selector_row)
        {
            selector_rows.push(selector_row);
        }
    }
    Ok(selector_rows)
}

pub(super) fn canonical_state_selector(planned_write: &PlannedWrite) -> CanonicalStateSelector {
    let predicates = if planned_write.command.selector.exact_only {
        exact_selector_predicates(planned_write)
            .unwrap_or_else(|| planned_write.command.selector.residual_predicates.clone())
    } else {
        planned_write.command.selector.residual_predicates.clone()
    };
    let version_column = planned_write
        .command
        .target
        .implicit_overrides
        .expose_version_id
        .then(|| {
            public_selector_version_column(planned_write.command.target.descriptor.surface_family)
                .to_string()
        });
    CanonicalStateSelector {
        predicates,
        version_column,
    }
}

pub(super) fn assign_state_row_key_value(
    row_key: &mut CanonicalStateRowKey,
    column: &str,
    value: &Value,
) -> Result<(), WriteResolveError> {
    match column {
        "entity_id" => {
            row_key.entity_id = exact_text_value(
                value,
                "public state row key requires text-compatible 'entity_id'",
            )?;
        }
        "file_id" => {
            row_key.file_id = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'file_id'",
            )?);
        }
        "plugin_key" => {
            row_key.plugin_key = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'plugin_key'",
            )?);
        }
        "schema_version" => {
            row_key.schema_version = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'schema_version'",
            )?);
        }
        "version_id" => {
            row_key.version_id = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'version_id'",
            )?);
        }
        "writer_key" => {
            row_key.writer_key = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'writer_key'",
            )?);
        }
        "global" => {
            row_key.global = Some(exact_bool_value(
                value,
                "public state row key requires boolean-compatible 'global'",
            )?);
        }
        "untracked" => {
            row_key.untracked = Some(exact_bool_value(
                value,
                "public state row key requires boolean-compatible 'untracked'",
            )?);
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn exact_filter_text(
    filters: &std::collections::BTreeMap<String, Value>,
    key: &str,
    error_message: &str,
) -> Result<Option<String>, WriteResolveError> {
    filters
        .get(key)
        .map(|value| exact_text_value(value, error_message))
        .transpose()
}

fn exact_selector_predicates(planned_write: &PlannedWrite) -> Option<Vec<Expr>> {
    let mut predicates = Vec::with_capacity(planned_write.command.selector.exact_filters.len());
    for (column, value) in &planned_write.command.selector.exact_filters {
        let public_column = public_selector_column_name(
            planned_write.command.target.descriptor.surface_family,
            column,
        )?;
        predicates.push(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(public_column))),
            op: BinaryOperator::Eq,
            right: Box::new(engine_value_to_sql_expr(value)),
        });
    }
    Some(predicates)
}

fn exact_text_value(value: &Value, error_message: &str) -> Result<String, WriteResolveError> {
    text_from_value(value).ok_or_else(|| WriteResolveError {
        message: error_message.to_string(),
    })
}

fn exact_bool_value(value: &Value, error_message: &str) -> Result<bool, WriteResolveError> {
    super::bool_from_value(value).ok_or_else(|| WriteResolveError {
        message: error_message.to_string(),
    })
}

fn engine_value_to_sql_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Value(ValueWithSpan::from(SqlValue::Null)),
        Value::Boolean(value) => Expr::Value(ValueWithSpan::from(SqlValue::Boolean(*value))),
        Value::Text(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.clone(),
        ))),
        Value::Json(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.to_string(),
        ))),
        Value::Integer(value) => Expr::Value(ValueWithSpan::from(SqlValue::Number(
            value.to_string(),
            false,
        ))),
        Value::Real(value) => Expr::Value(ValueWithSpan::from(SqlValue::Number(
            value.to_string(),
            false,
        ))),
        Value::Blob(value) => Expr::Value(ValueWithSpan::from(
            SqlValue::SingleQuotedByteStringLiteral(String::from_utf8_lossy(value).to_string()),
        )),
    }
}

fn build_public_selector_query(
    surface_name: &str,
    selector: &CanonicalStateSelector,
    selector_columns: &[&str],
) -> Query {
    let selection = selector
        .predicates
        .iter()
        .cloned()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        });

    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: selector_columns
                .iter()
                .map(|column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(*column))))
                .collect(),
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(surface_name))]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    with_ordinality: false,
                    partitions: vec![],
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
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
    }
}

async fn execute_public_selector_query_strict(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    query: Query,
) -> Result<QueryResult, LixError> {
    execute_public_read_query_strict(backend, query, &planned_write.command.bound_parameters).await
}
