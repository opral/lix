use crate::LixError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceholderState {
    next_ordinal: usize,
}

impl PlaceholderState {
    pub(crate) fn new() -> Self {
        Self { next_ordinal: 0 }
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("unsupported SQL placeholder format '{trimmed}'"),
        });
    };

    if source_index >= params_len {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "placeholder '{trimmed}' references parameter {} but only {} parameters were provided",
                source_index + 1,
                params_len
            ),
        });
    }

    Ok(source_index)
}

fn parse_1_based_index(token: &str, numeric: &str) -> Result<usize, LixError> {
    let parsed = numeric.parse::<usize>().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("invalid SQL placeholder '{token}'"),
    })?;
    if parsed == 0 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("invalid SQL placeholder '{token}'"),
        });
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{resolve_placeholder_index, PlaceholderState};

    #[test]
    fn advances_ordinal_placeholders() {
        let mut state = PlaceholderState::new();

        assert_eq!(resolve_placeholder_index("?", 3, &mut state).unwrap(), 0);
        assert_eq!(resolve_placeholder_index("?", 3, &mut state).unwrap(), 1);
        assert_eq!(resolve_placeholder_index("?3", 3, &mut state).unwrap(), 2);
    }

    #[test]
    fn rejects_out_of_range_placeholders() {
        let mut state = PlaceholderState::new();
        let error = resolve_placeholder_index("$2", 1, &mut state).unwrap_err();

        assert!(error.description.contains("references parameter 2"));
    }
}
