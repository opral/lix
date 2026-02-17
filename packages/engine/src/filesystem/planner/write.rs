use sqlparser::ast::{
    Delete, Expr, FromTable, ObjectName, ObjectNamePart, TableFactor, TableObject, Update,
    Value as AstValue, ValueWithSpan,
};

use super::read::{FILE_BY_VERSION_VIEW, FILE_VIEW};
use crate::sql::{bind_sql_with_state, resolve_expr_cell_with_state, PlaceholderState};
use crate::{LixError, SqlDialect, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileWriteScope {
    ActiveVersion,
    ExplicitVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsupportedPredicateBehavior {
    Ignore,
    ConsumePlaceholders { dialect: SqlDialect },
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ExactFileUpdateSelection {
    pub(crate) file_id: Option<String>,
    pub(crate) explicit_version_id: Option<String>,
    pub(crate) invalid: bool,
}

pub(crate) fn infer_file_write_scope_from_table_object(
    table: &TableObject,
) -> Option<FileWriteScope> {
    let TableObject::TableName(name) = table else {
        return None;
    };
    let table_name = object_name_terminal(name)?;
    infer_file_write_scope_from_name(&table_name)
}

pub(crate) fn infer_file_write_scope_from_update(update: &Update) -> Option<FileWriteScope> {
    if !update.table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &update.table.relation else {
        return None;
    };
    let table_name = object_name_terminal(name)?;
    infer_file_write_scope_from_name(&table_name)
}

pub(crate) fn infer_file_write_scope_from_delete(delete: &Delete) -> Option<FileWriteScope> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return None;
    }
    if !tables[0].joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &tables[0].relation else {
        return None;
    };
    let table_name = object_name_terminal(name)?;
    infer_file_write_scope_from_name(&table_name)
}

pub(crate) fn infer_file_write_scope_from_name(table_name: &str) -> Option<FileWriteScope> {
    if table_name.eq_ignore_ascii_case(FILE_VIEW) {
        Some(FileWriteScope::ActiveVersion)
    } else if table_name.eq_ignore_ascii_case(FILE_BY_VERSION_VIEW) {
        Some(FileWriteScope::ExplicitVersion)
    } else {
        None
    }
}

pub(crate) fn extract_exact_file_update_selection(
    selection: Option<&Expr>,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    unsupported_behavior: UnsupportedPredicateBehavior,
) -> Result<Option<ExactFileUpdateSelection>, LixError> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut out = ExactFileUpdateSelection::default();
    if !collect_exact_file_update_predicates(
        selection,
        params,
        placeholder_state,
        &mut out,
        unsupported_behavior,
    )? {
        return Ok(None);
    }
    if out.invalid || out.file_id.is_none() {
        return Ok(None);
    }
    Ok(Some(out))
}

fn collect_exact_file_update_predicates(
    selection: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    out: &mut ExactFileUpdateSelection,
    unsupported_behavior: UnsupportedPredicateBehavior,
) -> Result<bool, LixError> {
    match selection {
        Expr::Nested(inner) => collect_exact_file_update_predicates(
            inner,
            params,
            placeholder_state,
            out,
            unsupported_behavior,
        ),
        Expr::BinaryOp { left, op, right } => {
            if op.to_string().eq_ignore_ascii_case("AND") {
                let left_ok = collect_exact_file_update_predicates(
                    left,
                    params,
                    placeholder_state,
                    out,
                    unsupported_behavior,
                )?;
                let right_ok = collect_exact_file_update_predicates(
                    right,
                    params,
                    placeholder_state,
                    out,
                    unsupported_behavior,
                )?;
                return Ok(left_ok && right_ok);
            }
            if op.to_string().eq_ignore_ascii_case("=") {
                if let Some(column) = expr_column_name(left) {
                    if let Some(value) =
                        expr_string_literal_or_placeholder(right, params, placeholder_state)?
                    {
                        if apply_exact_file_update_predicate(&column, &value, out) {
                            return Ok(true);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                if let Some(column) = expr_column_name(right) {
                    if let Some(value) =
                        expr_string_literal_or_placeholder(left, params, placeholder_state)?
                    {
                        if apply_exact_file_update_predicate(&column, &value, out) {
                            return Ok(true);
                        }
                    } else {
                        return Ok(false);
                    }
                }
            }
            consume_unsupported_predicate(
                selection,
                params,
                placeholder_state,
                unsupported_behavior,
            )?;
            Ok(false)
        }
        _ => {
            consume_unsupported_predicate(
                selection,
                params,
                placeholder_state,
                unsupported_behavior,
            )?;
            Ok(false)
        }
    }
}

fn apply_exact_file_update_predicate(
    column: &str,
    value: &str,
    out: &mut ExactFileUpdateSelection,
) -> bool {
    if column.eq_ignore_ascii_case("id")
        || column.eq_ignore_ascii_case("lixcol_entity_id")
        || column.eq_ignore_ascii_case("lixcol_file_id")
    {
        if let Some(existing) = out.file_id.as_ref() {
            if existing != value {
                out.invalid = true;
            }
        } else {
            out.file_id = Some(value.to_string());
        }
        return true;
    }

    if column.eq_ignore_ascii_case("lixcol_version_id") || column.eq_ignore_ascii_case("version_id")
    {
        if let Some(existing) = out.explicit_version_id.as_ref() {
            if existing != value {
                out.invalid = true;
            }
        } else {
            out.explicit_version_id = Some(value.to_string());
        }
        return true;
    }

    false
}

fn consume_unsupported_predicate(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    unsupported_behavior: UnsupportedPredicateBehavior,
) -> Result<(), LixError> {
    match unsupported_behavior {
        UnsupportedPredicateBehavior::Ignore => Ok(()),
        UnsupportedPredicateBehavior::ConsumePlaceholders { dialect } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)
        }
    }
}

fn consume_placeholders_in_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::Placeholder(_),
            ..
        }) => {
            let _ = resolve_expr_cell_with_state(expr, params, placeholder_state)?;
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            consume_placeholders_in_expr(left, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(right, params, placeholder_state, dialect)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)
        }
        Expr::InList { expr, list, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            for item in list {
                consume_placeholders_in_expr(item, params, placeholder_state, dialect)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(low, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(high, params, placeholder_state, dialect)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(pattern, params, placeholder_state, dialect)
        }
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                for arg in &list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => {
                            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?
                        }
                        sqlparser::ast::FunctionArg::Named { arg, .. }
                        | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                                consume_placeholders_in_expr(
                                    expr,
                                    params,
                                    placeholder_state,
                                    dialect,
                                )?;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        },
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            consume_placeholders_in_expr(left, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(right, params, placeholder_state, dialect)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            consume_placeholders_in_query(subquery, params, placeholder_state, dialect)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            consume_placeholders_in_query(subquery, params, placeholder_state, dialect)
        }
        _ => Ok(()),
    }
}

fn consume_placeholders_in_query(
    query: &sqlparser::ast::Query,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    let probe_sql = format!("SELECT 1 WHERE EXISTS ({query})");
    let bound = bind_sql_with_state(&probe_sql, params, dialect, *placeholder_state)?;
    *placeholder_state = bound.state;
    Ok(())
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn expr_string_literal(expr: &Expr) -> Option<String> {
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return None;
    };

    match value {
        AstValue::SingleQuotedString(value)
        | AstValue::DoubleQuotedString(value)
        | AstValue::TripleSingleQuotedString(value)
        | AstValue::TripleDoubleQuotedString(value)
        | AstValue::EscapedStringLiteral(value)
        | AstValue::UnicodeStringLiteral(value)
        | AstValue::NationalStringLiteral(value)
        | AstValue::HexStringLiteral(value)
        | AstValue::SingleQuotedRawStringLiteral(value)
        | AstValue::DoubleQuotedRawStringLiteral(value)
        | AstValue::TripleSingleQuotedRawStringLiteral(value)
        | AstValue::TripleDoubleQuotedRawStringLiteral(value)
        | AstValue::SingleQuotedByteStringLiteral(value)
        | AstValue::DoubleQuotedByteStringLiteral(value)
        | AstValue::TripleSingleQuotedByteStringLiteral(value)
        | AstValue::TripleDoubleQuotedByteStringLiteral(value) => Some(value.clone()),
        AstValue::DollarQuotedString(value) => Some(value.value.clone()),
        AstValue::Number(value, _) => Some(value.clone()),
        AstValue::Boolean(value) => Some(if *value {
            "1".to_string()
        } else {
            "0".to_string()
        }),
        AstValue::Null | AstValue::Placeholder(_) => None,
    }
}

fn expr_string_literal_or_placeholder(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<String>, LixError> {
    if let Some(value) = expr_string_literal(expr) {
        return Ok(Some(value));
    }

    let resolved = resolve_expr_cell_with_state(expr, params, placeholder_state)?;
    let Some(value) = resolved.value else {
        return Ok(None);
    };
    match value {
        Value::Text(value) => Ok(Some(value)),
        Value::Integer(value) => Ok(Some(value.to_string())),
        Value::Real(value) => Ok(Some(value.to_string())),
        Value::Null => Ok(None),
        Value::Blob(_) => Ok(None),
    }
}
