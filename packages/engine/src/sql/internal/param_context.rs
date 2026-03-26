use std::ops::ControlFlow;

use crate::LixError;
use sqlparser::ast::{Statement, Value as SqlValue, VisitMut, VisitorMut};

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
