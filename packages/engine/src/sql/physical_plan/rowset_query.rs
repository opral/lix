use crate::contracts::artifacts::{
    PendingViewFilter, PendingViewOrderClause, PendingViewProjection, ReadTimeProjectionRead,
    ReadTimeProjectionReadQuery, ReadTimeProjectionSurface,
};
use crate::sql::logical_plan::public_ir::StructuredPublicRead;
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::Value;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    LimitClause, OrderBy, OrderByExpr, SelectItem, UnaryOperator, Value as SqlValue,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompiledPublicRowsetQuery {
    pub(crate) projections: Vec<PendingViewProjection>,
    pub(crate) filters: Vec<PendingViewFilter>,
    pub(crate) order_by: Vec<PendingViewOrderClause>,
    pub(crate) limit: Option<usize>,
}

pub(crate) fn compile_public_rowset_query(
    structured_read: &StructuredPublicRead,
) -> Option<CompiledPublicRowsetQuery> {
    let table_alias = structured_read
        .query
        .source_alias
        .as_ref()
        .map(|alias| alias.name.value.as_str());
    let mut placeholder_state = PlaceholderState::new();
    let bound_parameters = &structured_read.bound_parameters;

    Some(CompiledPublicRowsetQuery {
        projections: structured_read
            .query
            .projection
            .iter()
            .map(|item| rowset_projection_from_select_item(item, table_alias))
            .collect::<Option<Vec<_>>>()?,
        filters: structured_read
            .query
            .selection_predicates
            .iter()
            .map(|predicate| {
                rowset_filter_from_expr(
                    predicate,
                    table_alias,
                    bound_parameters,
                    &mut placeholder_state,
                )
            })
            .collect::<Option<Vec<_>>>()?,
        order_by: structured_read
            .query
            .order_by
            .as_ref()
            .map(|order_by| rowset_order_by_from_clause(order_by, table_alias))
            .flatten()
            .unwrap_or_default(),
        limit: rowset_limit_from_clause(structured_read.query.limit_clause.as_ref())?,
    })
}

pub(crate) fn try_compile_read_time_projection_read(
    structured_read: &StructuredPublicRead,
) -> Option<ReadTimeProjectionRead> {
    let surface = ReadTimeProjectionSurface::from_public_name(
        &structured_read.surface_binding.descriptor.public_name,
    )?;
    match &structured_read.query.group_by {
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty() => {}
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => return None,
    }
    if structured_read.query.having.is_some() {
        return None;
    }

    let query = compile_public_rowset_query(structured_read)?;
    let uses_count_all = query
        .projections
        .iter()
        .any(|projection| matches!(projection, PendingViewProjection::CountAll { .. }));
    if uses_count_all
        && !query
            .projections
            .iter()
            .all(|projection| matches!(projection, PendingViewProjection::CountAll { .. }))
    {
        return None;
    }

    Some(ReadTimeProjectionRead {
        surface,
        requested_version_id: structured_read.requested_version_id.clone(),
        query: ReadTimeProjectionReadQuery {
            projections: query.projections,
            filters: query.filters,
            order_by: query.order_by,
            limit: query.limit,
        },
    })
}

fn rowset_projection_from_select_item(
    item: &SelectItem,
    table_alias: Option<&str>,
) -> Option<PendingViewProjection> {
    match item {
        SelectItem::UnnamedExpr(expr) => rowset_projection_from_expr(
            expr,
            table_alias,
            rowset_identifier_name(expr, table_alias).unwrap_or_else(|| expr.to_string()),
        ),
        SelectItem::ExprWithAlias { expr, alias } => {
            rowset_projection_from_expr(expr, table_alias, alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

fn rowset_projection_from_expr(
    expr: &Expr,
    table_alias: Option<&str>,
    output_column: String,
) -> Option<PendingViewProjection> {
    if rowset_expr_is_count_all(expr) {
        return Some(PendingViewProjection::CountAll { output_column });
    }

    Some(PendingViewProjection::Column {
        source_column: rowset_identifier_name(expr, table_alias)?,
        output_column,
    })
}

fn rowset_filter_from_expr(
    expr: &Expr,
    table_alias: Option<&str>,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<PendingViewFilter> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Some(PendingViewFilter::And(vec![
            rowset_filter_from_expr(left, table_alias, params, placeholder_state)?,
            rowset_filter_from_expr(right, table_alias, params, placeholder_state)?,
        ])),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Some(PendingViewFilter::Or(vec![
            rowset_filter_from_expr(left, table_alias, params, placeholder_state)?,
            rowset_filter_from_expr(right, table_alias, params, placeholder_state)?,
        ])),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match (
            left.as_ref(),
            rowset_value_from_expr(right, params, placeholder_state),
            right.as_ref(),
            rowset_value_from_expr(left, params, placeholder_state),
        ) {
            (left, Some(value), _, _) => Some(PendingViewFilter::Equals(
                rowset_identifier_name(left, table_alias)?,
                value,
            )),
            (_, _, right, Some(value)) => Some(PendingViewFilter::Equals(
                rowset_identifier_name(right, table_alias)?,
                value,
            )),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => Some(PendingViewFilter::In(
            rowset_identifier_name(expr, table_alias)?,
            list.iter()
                .map(|expr| rowset_value_from_expr(expr, params, placeholder_state))
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::IsNull(expr) => Some(PendingViewFilter::IsNull(rowset_identifier_name(
            expr,
            table_alias,
        )?)),
        Expr::IsNotNull(expr) => Some(PendingViewFilter::IsNotNull(rowset_identifier_name(
            expr,
            table_alias,
        )?)),
        Expr::Like {
            expr,
            pattern,
            negated: false,
            ..
        } => Some(PendingViewFilter::Like {
            column: rowset_identifier_name(expr, table_alias)?,
            pattern: rowset_value_from_expr(pattern, params, placeholder_state)
                .and_then(|value| rowset_filter_text(&value))?,
            case_insensitive: false,
        }),
        Expr::ILike {
            expr,
            pattern,
            negated: false,
            ..
        } => Some(PendingViewFilter::Like {
            column: rowset_identifier_name(expr, table_alias)?,
            pattern: rowset_value_from_expr(pattern, params, placeholder_state)
                .and_then(|value| rowset_filter_text(&value))?,
            case_insensitive: true,
        }),
        Expr::Nested(inner) => {
            rowset_filter_from_expr(inner, table_alias, params, placeholder_state)
        }
        _ => None,
    }
}

fn rowset_order_by_from_clause(
    order_by: &OrderBy,
    table_alias: Option<&str>,
) -> Option<Vec<PendingViewOrderClause>> {
    let sqlparser::ast::OrderByKind::Expressions(expressions) = &order_by.kind else {
        return None;
    };
    expressions
        .iter()
        .map(|expr| rowset_order_clause_from_expr(expr, table_alias))
        .collect()
}

fn rowset_order_clause_from_expr(
    expr: &OrderByExpr,
    table_alias: Option<&str>,
) -> Option<PendingViewOrderClause> {
    Some(PendingViewOrderClause {
        column: rowset_identifier_name(&expr.expr, table_alias)?,
        descending: expr.options.asc == Some(false),
    })
}

fn rowset_limit_from_clause(limit_clause: Option<&LimitClause>) -> Option<Option<usize>> {
    let Some(limit_clause) = limit_clause else {
        return Some(None);
    };
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if offset.is_some() || !limit_by.is_empty() {
                return None;
            }
            let Some(limit) = limit.as_ref() else {
                return Some(None);
            };
            let Expr::Value(value) = limit else {
                return None;
            };
            match &value.value {
                SqlValue::Number(value, _) => value.parse::<usize>().ok().map(Some),
                _ => None,
            }
        }
        LimitClause::OffsetCommaLimit { .. } => None,
    }
}

fn rowset_identifier_name(expr: &Expr, table_alias: Option<&str>) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = parts[0].value.as_str();
            let column = parts[1].value.clone();
            match table_alias {
                Some(alias) if alias.eq_ignore_ascii_case(qualifier) => Some(column),
                None => Some(column),
                _ => None,
            }
        }
        _ => None,
    }
}

fn rowset_expr_is_count_all(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    function.name.to_string().eq_ignore_ascii_case("count")
        && matches!(
            &function.args,
            FunctionArguments::List(list)
                if list.args.len() == 1
                    && matches!(
                        &list.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    )
        )
}

fn rowset_value_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<Value> {
    match expr {
        Expr::Nested(inner) => rowset_value_from_expr(inner, params, placeholder_state),
        Expr::UnaryOp { op, expr } => {
            let value = rowset_value_from_expr(expr, params, placeholder_state)?;
            match (op, value) {
                (UnaryOperator::Minus, Value::Integer(value)) => Some(Value::Integer(-value)),
                (UnaryOperator::Minus, Value::Real(value)) => Some(Value::Real(-value)),
                (UnaryOperator::Plus, value) => Some(value),
                _ => None,
            }
        }
        Expr::Value(value) => match &value.value {
            SqlValue::Placeholder(token) => {
                let index =
                    resolve_placeholder_index(token, params.len(), placeholder_state).ok()?;
                params.get(index).cloned()
            }
            _ => sql_value_as_engine_value(value),
        },
        _ => None,
    }
}

fn sql_value_as_engine_value(value: &sqlparser::ast::ValueWithSpan) -> Option<Value> {
    match &value.value {
        SqlValue::Null => Some(Value::Null),
        SqlValue::Boolean(value) => Some(Value::Boolean(*value)),
        SqlValue::SingleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::DollarQuotedString(sqlparser::ast::DollarQuotedString {
            value: text, ..
        }) => Some(Value::Text(text.clone())),
        SqlValue::Number(value, _) => value
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| value.parse::<f64>().map(Value::Real))
            .ok(),
        _ => None,
    }
}

fn rowset_filter_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(if *value { "1" } else { "0" }.to_string()),
        Value::Real(value) => Some(value.to_string()),
        Value::Json(value) => Some(value.to_string()),
        Value::Null | Value::Blob(_) => None,
    }
}
