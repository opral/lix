use std::ops::ControlFlow;

use crate::{LixError, Value as EngineValue};
use sqlparser::ast::{
    BinaryOperator, Expr, Query, Statement, Value as SqlValue, ValueWithSpan, VisitMut, VisitorMut,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceholderOrdinalState {
    next_ordinal: usize,
}

impl PlaceholderOrdinalState {
    pub(crate) fn new() -> Self {
        Self { next_ordinal: 0 }
    }
}

pub(crate) fn normalize_statement_placeholders_in_batch(
    statements: &mut [Statement],
) -> Result<(), LixError> {
    let mut state = PlaceholderOrdinalState::new();
    for statement in statements {
        normalize_statement_placeholders(statement, &mut state)?;
    }
    Ok(())
}

pub(crate) fn normalize_statement_placeholders(
    statement: &mut Statement,
    state: &mut PlaceholderOrdinalState,
) -> Result<(), LixError> {
    let mut canonicalizer = PlaceholderCanonicalizer { state };
    if let ControlFlow::Break(error) = statement.visit(&mut canonicalizer) {
        return Err(error);
    }
    Ok(())
}

pub(crate) fn normalize_query_placeholders(
    query: &mut Query,
    state: &mut PlaceholderOrdinalState,
) -> Result<(), LixError> {
    let mut canonicalizer = PlaceholderCanonicalizer { state };
    if let ControlFlow::Break(error) = query.visit(&mut canonicalizer) {
        return Err(error);
    }
    Ok(())
}

pub(crate) fn extract_string_column_values_from_expr(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
    params: &[EngineValue],
) -> Option<Vec<String>> {
    extract_column_values_from_expr(expr, is_target_column, params, resolve_string_scalar)
}

#[cfg(test)]
pub(crate) fn extract_bool_column_values_from_expr(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
    params: &[EngineValue],
) -> Option<Vec<bool>> {
    extract_column_values_from_expr(expr, is_target_column, params, resolve_bool_scalar)
}

pub(crate) fn match_bool_column_equality(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
    params: &[EngineValue],
) -> Option<bool> {
    match unwrap_wrappers(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_target_column(left) {
                return resolve_bool_scalar(right, params);
            }
            if is_target_column(right) {
                return resolve_bool_scalar(left, params);
            }
            None
        }
        _ => None,
    }
}

pub(crate) fn expr_last_identifier_eq(expr: &Expr, target: &str) -> bool {
    match unwrap_wrappers(expr) {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(target),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(target))
            .unwrap_or(false),
        _ => false,
    }
}

struct PlaceholderCanonicalizer<'a> {
    state: &'a mut PlaceholderOrdinalState,
}

impl VisitorMut for PlaceholderCanonicalizer<'_> {
    type Break = LixError;

    fn pre_visit_value(&mut self, value: &mut SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };

        let index_1_based = match normalize_placeholder_token(token, self.state) {
            Ok(index) => index,
            Err(error) => return ControlFlow::Break(error),
        };
        *value = SqlValue::Placeholder(format!("?{index_1_based}"));
        ControlFlow::Continue(())
    }
}

fn normalize_placeholder_token(
    token: &str,
    state: &mut PlaceholderOrdinalState,
) -> Result<usize, LixError> {
    let trimmed = token.trim();

    if trimmed.is_empty() || trimmed == "?" {
        state.next_ordinal += 1;
        return Ok(state.next_ordinal);
    }

    let explicit_1_based = if let Some(numeric) = trimmed.strip_prefix('?') {
        parse_1_based_placeholder(trimmed, numeric)?
    } else if let Some(numeric) = trimmed.strip_prefix('$') {
        parse_1_based_placeholder(trimmed, numeric)?
    } else {
        return Err(LixError::unknown(format!(
            "unsupported SQL placeholder format '{trimmed}'"
        )));
    };

    state.next_ordinal = state.next_ordinal.max(explicit_1_based);
    Ok(explicit_1_based)
}

fn parse_1_based_placeholder(token: &str, numeric: &str) -> Result<usize, LixError> {
    let parsed = numeric
        .parse::<usize>()
        .map_err(|_| LixError::unknown(format!("invalid SQL placeholder '{token}'")))?;
    if parsed == 0 {
        return Err(LixError::unknown(format!(
            "invalid SQL placeholder '{token}'"
        )));
    }
    Ok(parsed)
}

fn extract_column_values_from_expr<T>(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
    params: &[EngineValue],
    resolve_scalar: fn(&Expr, &[EngineValue]) -> Option<T>,
) -> Option<Vec<T>>
where
    T: Clone + PartialEq,
{
    match unwrap_wrappers(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_target_column(left) {
                return resolve_scalar(right, params).map(|value| vec![value]);
            }
            if is_target_column(right) {
                return resolve_scalar(left, params).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            extract_column_values_from_expr(left, is_target_column, params, resolve_scalar),
            extract_column_values_from_expr(right, is_target_column, params, resolve_scalar),
        ) {
            (Some(left), Some(right)) => {
                let intersection = intersect_values(&left, &right);
                if intersection.is_empty() {
                    None
                } else {
                    Some(intersection)
                }
            }
            (Some(values), None) | (None, Some(values)) => Some(values),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => match (
            extract_column_values_from_expr(left, is_target_column, params, resolve_scalar),
            extract_column_values_from_expr(right, is_target_column, params, resolve_scalar),
        ) {
            (Some(left), Some(right)) => Some(union_values(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !is_target_column(expr) {
                return None;
            }
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = resolve_scalar(item, params)?;
                values.push(value);
            }
            if values.is_empty() {
                None
            } else {
                Some(dedup_values(values))
            }
        }
        _ => None,
    }
}

fn unwrap_wrappers(mut expr: &Expr) -> &Expr {
    loop {
        match expr {
            Expr::Nested(inner) => expr = inner,
            Expr::Cast { expr: inner, .. } => expr = inner,
            _ => return expr,
        }
    }
}

fn resolve_string_scalar(expr: &Expr, params: &[EngineValue]) -> Option<String> {
    match unwrap_wrappers(expr) {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Placeholder(token),
            ..
        }) => resolve_explicit_placeholder_value(token, params).and_then(|value| match value {
            EngineValue::Text(text) => Some(text.clone()),
            _ => None,
        }),
        Expr::Value(value) => value.value.clone().into_string(),
        Expr::Identifier(ident) if ident.quote_style == Some('"') => Some(ident.value.clone()),
        _ => None,
    }
}

fn resolve_bool_scalar(expr: &Expr, params: &[EngineValue]) -> Option<bool> {
    match unwrap_wrappers(expr) {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Placeholder(token),
            ..
        }) => resolve_explicit_placeholder_value(token, params).and_then(|value| match value {
            EngineValue::Boolean(boolean) => Some(*boolean),
            _ => None,
        }),
        Expr::Value(ValueWithSpan {
            value: SqlValue::Boolean(boolean),
            ..
        }) => Some(*boolean),
        _ => None,
    }
}

fn resolve_explicit_placeholder_value<'a>(
    token: &str,
    params: &'a [EngineValue],
) -> Option<&'a EngineValue> {
    let numeric = token
        .strip_prefix('?')
        .or_else(|| token.strip_prefix('$'))?;
    let index = numeric.parse::<usize>().ok()?;
    index
        .checked_sub(1)
        .and_then(|zero_based| params.get(zero_based))
}

fn intersect_values<T>(left: &[T], right: &[T]) -> Vec<T>
where
    T: Clone + PartialEq,
{
    left.iter()
        .filter(|value| right.contains(value))
        .cloned()
        .collect::<Vec<_>>()
}

fn union_values<T>(left: &[T], right: &[T]) -> Vec<T>
where
    T: Clone + PartialEq,
{
    let mut values = left.to_vec();
    for value in right {
        if !values.contains(value) {
            values.push(value.clone());
        }
    }
    values
}

fn dedup_values<T>(values: Vec<T>) -> Vec<T>
where
    T: Clone + PartialEq,
{
    let mut deduped = Vec::with_capacity(values.len());
    for value in values {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
    }
    deduped
}
