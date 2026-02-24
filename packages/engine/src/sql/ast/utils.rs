use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Insert, SetExpr, Value as SqlValue, VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{LixError, SqlDialect, Value};

use sqlparser::ast::Statement;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceholderState {
    next_ordinal: usize,
}

impl PlaceholderState {
    pub(crate) fn new() -> Self {
        Self { next_ordinal: 0 }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BoundSql {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) state: PlaceholderState,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedCell {
    pub(crate) value: Option<Value>,
    pub(crate) placeholder_index: Option<usize>,
}

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })
}

pub(crate) fn bind_sql(
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<BoundSql, LixError> {
    bind_sql_with_state(sql, params, dialect, PlaceholderState::new())
}

pub(crate) fn bind_sql_with_state(
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
    state: PlaceholderState,
) -> Result<BoundSql, LixError> {
    bind_sql_with_state_and_appended_params(sql, params, &[], dialect, state)
}

pub(crate) fn bind_sql_with_state_and_appended_params(
    sql: &str,
    base_params: &[Value],
    appended_params: &[Value],
    dialect: SqlDialect,
    mut state: PlaceholderState,
) -> Result<BoundSql, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    let mut used_source_indices = Vec::new();
    let mut source_to_dense: HashMap<usize, usize> = HashMap::new();
    let total_params_len = base_params.len() + appended_params.len();

    for statement in &mut statements {
        let mut visitor = PlaceholderBinder {
            params_len: total_params_len,
            dialect,
            state: &mut state,
            source_to_dense: &mut source_to_dense,
            used_source_indices: &mut used_source_indices,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
    }

    let bound_params = used_source_indices
        .into_iter()
        .map(|source_index| clone_param_from_sources(source_index, base_params, appended_params))
        .collect();

    Ok(BoundSql {
        sql: statements_to_sql(&statements),
        params: bound_params,
        state,
    })
}

pub(crate) fn resolve_values_rows(
    rows: &[Vec<Expr>],
    params: &[Value],
) -> Result<Vec<Vec<ResolvedCell>>, LixError> {
    let mut state = PlaceholderState::default();
    let mut resolved_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut resolved = Vec::with_capacity(row.len());
        for expr in row {
            resolved.push(resolve_expr(expr, params, &mut state)?);
        }
        resolved_rows.push(resolved);
    }

    Ok(resolved_rows)
}

pub(crate) fn resolve_expr_cell_with_state(
    expr: &Expr,
    params: &[Value],
    state: &mut PlaceholderState,
) -> Result<ResolvedCell, LixError> {
    resolve_expr(expr, params, state)
}

pub(crate) fn resolve_insert_rows(
    insert: &Insert,
    params: &[Value],
) -> Result<Option<Vec<Vec<ResolvedCell>>>, LixError> {
    let Some(source) = &insert.source else {
        return Ok(None);
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(None);
    };

    resolve_values_rows(&values.rows, params).map(Some)
}

pub(crate) fn insert_values_rows_mut(insert: &mut Insert) -> Option<&mut [Vec<Expr>]> {
    let source = insert.source.as_mut()?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return None;
    };
    Some(values.rows.as_mut_slice())
}

fn clone_param_from_sources(
    source_index: usize,
    base_params: &[Value],
    appended_params: &[Value],
) -> Value {
    if source_index < base_params.len() {
        return base_params[source_index].clone();
    }

    appended_params[source_index - base_params.len()].clone()
}

struct PlaceholderBinder<'a> {
    params_len: usize,
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
    source_to_dense: &'a mut HashMap<usize, usize>,
    used_source_indices: &'a mut Vec<usize>,
}

impl VisitorMut for PlaceholderBinder<'_> {
    type Break = LixError;

    fn pre_visit_value(&mut self, value: &mut SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };
        let source_index = match resolve_placeholder_index(token, self.params_len, self.state) {
            Ok(index) => index,
            Err(error) => return ControlFlow::Break(error),
        };
        let dense_index =
            dense_index_for_source(source_index, self.source_to_dense, self.used_source_indices);
        *value = SqlValue::Placeholder(placeholder_for_dialect(self.dialect, dense_index + 1));
        ControlFlow::Continue(())
    }
}

pub(crate) fn resolve_placeholder_index(
    token: &str,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<usize, LixError> {
    let trimmed = token.trim();

    let source_index = if trimmed.is_empty() || trimmed == "?" {
        let source_index = state.next_ordinal;
        state.next_ordinal += 1;
        source_index
    } else if let Some(numeric) = trimmed.strip_prefix('?') {
        let parsed = parse_1_based_index(trimmed, numeric)?;
        state.next_ordinal = state.next_ordinal.max(parsed);
        parsed - 1
    } else if let Some(numeric) = trimmed.strip_prefix('$') {
        let parsed = parse_1_based_index(trimmed, numeric)?;
        state.next_ordinal = state.next_ordinal.max(parsed);
        parsed - 1
    } else {
        return Err(LixError {
            message: format!("unsupported SQL placeholder format '{trimmed}'"),
        });
    };

    if source_index >= params_len {
        return Err(LixError {
            message: format!(
                "placeholder '{trimmed}' references parameter {} but only {} parameters were provided",
                source_index + 1,
                params_len
            ),
        });
    }

    Ok(source_index)
}

fn dense_index_for_source(
    source_index: usize,
    source_to_dense: &mut HashMap<usize, usize>,
    used_source_indices: &mut Vec<usize>,
) -> usize {
    if let Some(existing) = source_to_dense.get(&source_index) {
        return *existing;
    }
    let dense_index = used_source_indices.len();
    used_source_indices.push(source_index);
    source_to_dense.insert(source_index, dense_index);
    dense_index
}

fn placeholder_for_dialect(dialect: SqlDialect, dense_index_1_based: usize) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("?{dense_index_1_based}"),
        SqlDialect::Postgres => format!("${dense_index_1_based}"),
    }
}

fn parse_1_based_index(token: &str, numeric: &str) -> Result<usize, LixError> {
    let parsed = numeric.parse::<usize>().map_err(|_| LixError {
        message: format!("invalid SQL placeholder '{token}'"),
    })?;
    if parsed == 0 {
        return Err(LixError {
            message: format!("invalid SQL placeholder '{token}'"),
        });
    }
    Ok(parsed)
}

fn resolve_expr(
    expr: &Expr,
    params: &[Value],
    state: &mut PlaceholderState,
) -> Result<ResolvedCell, LixError> {
    let Expr::Value(value) = expr else {
        return Ok(ResolvedCell {
            value: None,
            placeholder_index: None,
        });
    };

    match &value.value {
        SqlValue::Placeholder(token) => {
            let index = resolve_placeholder_index(token, params.len(), state)?;
            Ok(ResolvedCell {
                value: Some(params[index].clone()),
                placeholder_index: Some(index),
            })
        }
        other => Ok(ResolvedCell {
            value: Some(sql_literal_to_engine_value(other)?),
            placeholder_index: None,
        }),
    }
}

fn sql_literal_to_engine_value(value: &SqlValue) -> Result<Value, LixError> {
    match value {
        SqlValue::Number(raw, _) => {
            if let Ok(int) = raw.parse::<i64>() {
                Ok(Value::Integer(int))
            } else if let Ok(real) = raw.parse::<f64>() {
                Ok(Value::Real(real))
            } else {
                Err(LixError {
                    message: format!("unsupported numeric literal '{raw}'"),
                })
            }
        }
        SqlValue::SingleQuotedString(text)
        | SqlValue::DoubleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::TripleDoubleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::UnicodeStringLiteral(text)
        | SqlValue::NationalStringLiteral(text)
        | SqlValue::SingleQuotedRawStringLiteral(text)
        | SqlValue::DoubleQuotedRawStringLiteral(text)
        | SqlValue::TripleSingleQuotedRawStringLiteral(text)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(text)
        | SqlValue::SingleQuotedByteStringLiteral(text)
        | SqlValue::DoubleQuotedByteStringLiteral(text)
        | SqlValue::TripleSingleQuotedByteStringLiteral(text)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(text) => Ok(Value::Text(text.clone())),
        SqlValue::HexStringLiteral(text) => Ok(Value::Blob(parse_hex_literal(text)?)),
        SqlValue::DollarQuotedString(text) => Ok(Value::Text(text.value.clone())),
        SqlValue::Boolean(value) => Ok(Value::Integer(if *value { 1 } else { 0 })),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Placeholder(token) => Err(LixError {
            message: format!("unexpected placeholder '{token}' while resolving row"),
        }),
    }
}

fn parse_hex_literal(text: &str) -> Result<Vec<u8>, LixError> {
    if text.len() % 2 != 0 {
        return Err(LixError {
            message: format!(
                "hex literal must contain an even number of digits, got {}",
                text.len()
            ),
        });
    }

    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index < bytes.len() {
        let hi = hex_nibble(bytes[index])?;
        let lo = hex_nibble(bytes[index + 1])?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, LixError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(LixError {
            message: format!("invalid hex digit '{}'", char::from(byte)),
        }),
    }
}

fn statements_to_sql(statements: &[Statement]) -> String {
    statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
pub(crate) fn is_transaction_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
