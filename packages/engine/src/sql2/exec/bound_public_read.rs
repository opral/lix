//! Strict native reads for common public entity shapes.
//!
//! Lix keeps the full SQL surface in DataFusion.  This module intentionally
//! recognizes only a small shape which has a direct, row-oriented execution
//! equivalent.  A rejected shape is not an error: callers must use the normal
//! SQL path so that unsupported syntax never receives a partial native
//! interpretation.

use std::collections::BTreeSet;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, OrderByKind, Query, Select, SelectFlavor, SetExpr,
    Statement as SqlStatement, TableFactor, Value as SqlValue,
};
use serde_json::Value as JsonValue;

use crate::entity_pk::EntityPk;
use crate::live_state::{LiveStateFilter, LiveStateProjection, LiveStateScanRequest};
use crate::sql2::SqlExecutionContext;
use crate::sql2::catalog::{EntityColumnType, PublicSurfaceKind};
use crate::sql2::entity_projection::{
    EntityProjectionDecoder, entity_projection_error_to_lix_error,
};
use crate::{LixError, SqlQueryResult, Value};

/// Attempts the one public entity read shape that can be evaluated directly
/// against live state. `None` means the statement belongs to the general SQL
/// executor.
pub(crate) async fn try_execute_bound_public_read<C>(
    ctx: &C,
    sql: &str,
    statement: &DataFusionStatement,
    params: &[Value],
) -> Result<Option<SqlQueryResult>, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let Some(shape) = strict_entity_primary_key_read(statement, params)
        .map(StrictEntityRead::PrimaryKey)
        .or_else(|| strict_entity_broad_read(statement, params).map(StrictEntityRead::Broad))
    else {
        return Ok(None);
    };

    // A native route must retain all public SQL-surface validation. This runs
    // after structural recognition so normal queries do not pay an additional
    // validation pass before their existing DataFusion path.
    crate::sql2::bind_read_statement(sql, statement)?;

    let catalog = ctx.public_catalog().await?;
    let Some(surface) = catalog.surface(shape.table_name()) else {
        return Ok(None);
    };
    let PublicSurfaceKind::EntityBase { schema_key } = &surface.kind else {
        return Ok(None);
    };
    let Some(spec) = catalog.entity_spec(schema_key) else {
        return Ok(None);
    };
    let Some(primary_key_column) = single_top_level_string_primary_key(spec) else {
        return Ok(None);
    };
    match shape {
        StrictEntityRead::PrimaryKey(shape) => {
            if primary_key_column != shape.primary_key_column
                || shape.projection.iter().any(|column| {
                    !matches!(
                        spec.visible_column(column).map(|column| column.column_type),
                        Some(EntityColumnType::String | EntityColumnType::Json)
                    )
                })
            {
                return Ok(None);
            }

            let entity_pks = shape
                .primary_key_values
                .into_iter()
                .map(EntityPk::single)
                .collect::<Vec<_>>();
            let mut rows = ctx
                .live_state()
                .scan_rows(&LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec![schema_key.clone()],
                        entity_pks,
                        branch_ids: vec![ctx.active_branch_id().to_string()],
                        include_tombstones: false,
                        ..LiveStateFilter::default()
                    },
                    projection: LiveStateProjection {
                        columns: vec!["snapshot_content".to_string()],
                    },
                    limit: None,
                })
                .await?;

            // The accepted ORDER BY is the complete one-column primary key.
            // Retain multiple file-backed identities for one logical primary
            // key; `file_id` is a first-class row identity and must not be
            // collapsed by this route.
            rows.sort_by(|left, right| left.entity_pk.cmp(&right.entity_pk));
            let result_rows = rows
                .iter()
                .map(|row| {
                    materialize_row(spec, &shape.projection, row.snapshot_content.as_deref())
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Some(SqlQueryResult {
                columns: shape.projection,
                rows: result_rows,
                notices: Vec::new(),
            }))
        }
        StrictEntityRead::Broad(shape) => {
            if primary_key_column != shape.primary_key_column
                || shape
                    .projection
                    .iter()
                    .any(|column| spec.visible_column(column).is_none())
            {
                return Ok(None);
            }
            let Some(reader) = ctx.entity_snapshot_reader() else {
                return Ok(None);
            };
            let Some(snapshots) = reader
                .scan_entity_snapshots(LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec![schema_key.clone()],
                        branch_ids: vec![ctx.active_branch_id().to_string()],
                        include_tombstones: false,
                        ..LiveStateFilter::default()
                    },
                    projection: LiveStateProjection {
                        columns: vec!["snapshot_content".to_string()],
                    },
                    limit: None,
                })
                .await?
            else {
                return Ok(None);
            };
            let decoder =
                EntityProjectionDecoder::new(spec, shape.projection.iter().map(String::as_str))
                    .map_err(entity_projection_error_to_lix_error)?;
            let rows = decoder
                .decode_value_rows(snapshots.iter().map(Option::as_deref))
                .map_err(entity_projection_error_to_lix_error)?;
            Ok(Some(SqlQueryResult {
                columns: shape.projection,
                rows,
                notices: Vec::new(),
            }))
        }
    }
}

fn single_top_level_string_primary_key(
    spec: &crate::sql2::catalog::EntitySurfaceSpec,
) -> Option<&str> {
    let [primary_key] = spec.primary_key_paths.as_slice() else {
        return None;
    };
    let [column] = primary_key.as_slice() else {
        return None;
    };
    (spec.visible_column(column)?.column_type == EntityColumnType::String).then_some(column)
}

fn materialize_row(
    spec: &crate::sql2::catalog::EntitySurfaceSpec,
    projection: &[String],
    snapshot_content: Option<&str>,
) -> Result<Vec<Value>, LixError> {
    let snapshot = snapshot_content
        .map(|content| {
            serde_json::from_str::<JsonValue>(content).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sql2 entity provider expected valid snapshot_content JSON: {error}"),
                )
            })
        })
        .transpose()?;
    projection
        .iter()
        .map(|column| {
            let spec_column = spec.visible_column(column).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "direct entity read selected unknown column '{}' from schema '{}'",
                        column, spec.schema_key
                    ),
                )
            })?;
            let value = snapshot.as_ref().and_then(|snapshot| snapshot.get(column));
            materialize_value(spec_column.column_type, value)
        })
        .collect()
}

fn materialize_value(
    column_type: EntityColumnType,
    value: Option<&JsonValue>,
) -> Result<Value, LixError> {
    match (column_type, value) {
        (_, None | Some(JsonValue::Null)) => Ok(Value::Null),
        (EntityColumnType::String, Some(JsonValue::String(value))) => {
            Ok(Value::Text(value.clone()))
        }
        (EntityColumnType::String, Some(JsonValue::Bool(value))) => {
            Ok(Value::Text(value.to_string()))
        }
        (EntityColumnType::String, Some(value)) => serde_json::to_string(value)
            .map(Value::Text)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to render string entity value: {error}"),
                )
            }),
        (EntityColumnType::Json, Some(value)) => Ok(Value::Json(value.clone())),
        // Numeric and boolean direct projection is deliberately left to
        // DataFusion until this route shares the provider's exact coercion
        // contracts. The common document CRUD shape uses string/JSON fields.
        (EntityColumnType::Integer | EntityColumnType::Number | EntityColumnType::Boolean, _) => {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "direct entity read accepted an unsupported projected column type",
            ))
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StrictEntityPrimaryKeyRead {
    table_name: String,
    projection: Vec<String>,
    primary_key_column: String,
    primary_key_values: Vec<String>,
}

enum StrictEntityRead {
    PrimaryKey(StrictEntityPrimaryKeyRead),
    Broad(StrictEntityBroadRead),
}

impl StrictEntityRead {
    fn table_name(&self) -> &str {
        match self {
            Self::PrimaryKey(shape) => &shape.table_name,
            Self::Broad(shape) => &shape.table_name,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StrictEntityBroadRead {
    table_name: String,
    projection: Vec<String>,
    primary_key_column: String,
}

/// Recognizes a single, active entity table with an `IN`/`=` primary-key
/// predicate and canonical ascending primary-key order. It intentionally does
/// not inspect catalog metadata: catalog mismatches are handled by the caller
/// as a safe fallback.
fn strict_entity_primary_key_read(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<StrictEntityPrimaryKeyRead> {
    let (query, select, table_name) = strict_single_table_select(statement)?;
    if query.order_by.is_none()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || select.selection.is_none()
    {
        return None;
    }
    let projection = strict_projection(&select.projection)?;
    let (primary_key_column, primary_key_values) =
        strict_primary_key_values(select.selection.as_ref()?, params)?;
    if !canonical_primary_key_order(query, &primary_key_column) {
        return None;
    }
    Some(StrictEntityPrimaryKeyRead {
        table_name,
        projection,
        primary_key_column,
        primary_key_values,
    })
}

/// Recognizes the canonical broad tracked entity read. The packed head index
/// already stores one schema in primary-key order, so this shape can return
/// final public rows without routing an otherwise no-op sort through
/// DataFusion. Any clause that needs general SQL semantics remains a normal
/// DataFusion query.
fn strict_entity_broad_read(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<StrictEntityBroadRead> {
    if !params.is_empty() {
        return None;
    }
    let (query, select, table_name) = strict_single_table_select(statement)?;
    if query.limit_clause.is_some() || query.fetch.is_some() || select.selection.is_some() {
        return None;
    }
    Some(StrictEntityBroadRead {
        table_name,
        projection: strict_projection(&select.projection)?,
        primary_key_column: canonical_ascending_order_column(query)?,
    })
}

fn strict_single_table_select(
    statement: &DataFusionStatement,
) -> Option<(&Query, &Select, String)> {
    let DataFusionStatement::Statement(statement) = statement else {
        return None;
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return None;
    };
    if query.with.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.flavor != SelectFlavor::Standard
        || select.optimizer_hint.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !group_by_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return None;
    }
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from.relation
    else {
        return None;
    };
    if alias.is_some()
        || args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || name.0.len() != 1
    {
        return None;
    }
    let table_identifier = name.0.first()?.as_ident()?;
    if table_identifier.quote_style.is_some() {
        return None;
    }
    Some((query, select, table_identifier.value.to_ascii_lowercase()))
}

fn strict_projection(
    projection: &[datafusion::sql::sqlparser::ast::SelectItem],
) -> Option<Vec<String>> {
    let columns =
        projection
            .iter()
            .map(|item| match item {
                datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(Expr::Identifier(
                    column,
                )) if column.quote_style.is_none() => Some(column.value.to_ascii_lowercase()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
    let unique_count = columns.iter().collect::<BTreeSet<_>>().len();
    (unique_count == columns.len()).then_some(columns)
}

fn strict_primary_key_values(expression: &Expr, params: &[Value]) -> Option<(String, Vec<String>)> {
    let (column, expressions) = match expression {
        Expr::InList {
            expr,
            list,
            negated: false,
        } if !list.is_empty() => (strict_identifier(expr)?, list.iter().collect::<Vec<_>>()),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match (strict_identifier(left), strict_identifier(right)) {
            (Some(column), None) => (column, vec![right.as_ref()]),
            (None, Some(column)) => (column, vec![left.as_ref()]),
            _ => return None,
        },
        _ => return None,
    };

    let mut highest_parameter = 0;
    let mut values = BTreeSet::new();
    for expression in expressions {
        let value = strict_text_value(expression, params, &mut highest_parameter)?;
        values.insert(value);
    }
    (params.len() == highest_parameter).then_some((column, values.into_iter().collect()))
}

fn strict_text_value(
    expression: &Expr,
    params: &[Value],
    highest_parameter: &mut usize,
) -> Option<String> {
    let Expr::Value(value) = expression else {
        return None;
    };
    match &value.value {
        SqlValue::SingleQuotedString(value) => Some(value.clone()),
        SqlValue::Placeholder(placeholder) => {
            let index = placeholder
                .strip_prefix('$')?
                .parse::<usize>()
                .ok()
                .filter(|index| *index > 0)?;
            *highest_parameter = (*highest_parameter).max(index);
            let Value::Text(value) = params.get(index - 1)? else {
                return None;
            };
            Some(value.clone())
        }
        _ => None,
    }
}

fn canonical_primary_key_order(query: &Query, primary_key_column: &str) -> bool {
    canonical_ascending_order_column(query).as_deref() == Some(primary_key_column)
}

fn canonical_ascending_order_column(query: &Query) -> Option<String> {
    let Some(order_by) = &query.order_by else {
        return None;
    };
    if order_by.interpolate.is_some() {
        return None;
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return None;
    };
    let [order] = expressions.as_slice() else {
        return None;
    };
    (order.with_fill.is_none()
        && order.options.asc != Some(false)
        && order.options.nulls_first.is_none())
    .then(|| strict_identifier(&order.expr))?
}

fn strict_identifier(expression: &Expr) -> Option<String> {
    let Expr::Identifier(identifier) = expression else {
        return None;
    };
    (identifier.quote_style.is_none()).then(|| identifier.value.to_ascii_lowercase())
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers)
        if expressions.is_empty() && modifiers.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> DataFusionStatement {
        crate::sql2::parse_statement(sql).expect("test SQL should parse")
    }

    #[test]
    fn recognizes_benchmark_primary_key_read() {
        let shape = strict_entity_primary_key_read(
            &parse(
                "SELECT path, value FROM json_pointer WHERE path IN ('/a', '/b', '/a') ORDER BY path",
            ),
            &[],
        )
        .expect("benchmark query should use direct route");

        assert_eq!(shape.table_name, "json_pointer");
        assert_eq!(shape.projection, ["path", "value"]);
        assert_eq!(shape.primary_key_column, "path");
        assert_eq!(shape.primary_key_values, ["/a", "/b"]);
    }

    #[test]
    fn recognizes_numbered_text_parameters() {
        let shape = strict_entity_primary_key_read(
            &parse("SELECT path FROM json_pointer WHERE path IN ($2, $1) ORDER BY path"),
            &[Value::Text("/a".to_string()), Value::Text("/b".to_string())],
        )
        .expect("numbered text params should use direct route");

        assert_eq!(shape.primary_key_values, ["/a", "/b"]);
    }

    #[test]
    fn recognizes_benchmark_broad_read() {
        let shape = strict_entity_broad_read(
            &parse("SELECT path, value FROM json_pointer ORDER BY path"),
            &[],
        )
        .expect("benchmark query should use the broad direct route");

        assert_eq!(shape.table_name, "json_pointer");
        assert_eq!(shape.projection, ["path", "value"]);
        assert_eq!(shape.primary_key_column, "path");
    }

    #[test]
    fn rejects_noncanonical_or_ambiguous_sql() {
        for sql in [
            "SELECT path, value FROM json_pointer WHERE path IN ('/a')",
            "SELECT path, value FROM json_pointer WHERE path IN ('/a') ORDER BY path DESC",
            "SELECT path, value FROM json_pointer WHERE path IN ('/a') OR value = 'x' ORDER BY path",
            "SELECT path + 'x' FROM json_pointer WHERE path IN ('/a') ORDER BY path",
            "SELECT path FROM json_pointer AS p WHERE path IN ('/a') ORDER BY path",
        ] {
            assert!(
                strict_entity_primary_key_read(&parse(sql), &[]).is_none(),
                "{sql} must fall back"
            );
        }
    }

    #[test]
    fn broad_read_rejects_any_clause_that_needs_general_sql() {
        for sql in [
            "SELECT path, value FROM json_pointer",
            "SELECT path, value FROM json_pointer ORDER BY path DESC",
            "SELECT path, value FROM json_pointer WHERE true ORDER BY path",
            "SELECT path, value FROM json_pointer ORDER BY path LIMIT 1",
            "SELECT path, value FROM json_pointer AS p ORDER BY path",
            "SELECT * FROM json_pointer ORDER BY path",
            "SELECT path, path FROM json_pointer ORDER BY path",
        ] {
            assert!(
                strict_entity_broad_read(&parse(sql), &[]).is_none(),
                "{sql} must fall back"
            );
        }
        assert!(
            strict_entity_broad_read(
                &parse("SELECT path, value FROM json_pointer ORDER BY path"),
                &[Value::Text("unused".to_string())],
            )
            .is_none(),
            "an unused parameter must retain normal SQL validation"
        );
    }
}
