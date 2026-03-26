use crate::LixError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceholderState {
    next_ordinal: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaceholderRef {
    Next,
    Explicit(usize),
}

impl PlaceholderState {
    pub(crate) fn new() -> Self {
        Self { next_ordinal: 0 }
    }
}

pub(crate) fn parse_placeholder_ref(token: &str) -> Result<PlaceholderRef, LixError> {
    let trimmed = token.trim();

    if trimmed.is_empty() || trimmed == "?" {
        return Ok(PlaceholderRef::Next);
    }

    if let Some(numeric) = trimmed.strip_prefix('?') {
        return Ok(PlaceholderRef::Explicit(parse_1_based_index(
            trimmed, numeric,
        )?));
    }

    if let Some(numeric) = trimmed.strip_prefix('$') {
        return Ok(PlaceholderRef::Explicit(parse_1_based_index(
            trimmed, numeric,
        )?));
    }

    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("unsupported SQL placeholder format '{trimmed}'"),
    })
}

pub(crate) fn resolve_placeholder_ref(
    placeholder: PlaceholderRef,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<usize, LixError> {
    let source_index = match placeholder {
        PlaceholderRef::Next => {
            let source_index = state.next_ordinal;
            state.next_ordinal += 1;
            source_index
        }
        PlaceholderRef::Explicit(index_1_based) => {
            state.next_ordinal = state.next_ordinal.max(index_1_based);
            index_1_based - 1
        }
    };

    if source_index >= params_len {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "placeholder {:?} references parameter {} but only {} parameters were provided",
                placeholder,
                source_index + 1,
                params_len
            ),
        });
    }

    Ok(source_index)
}

pub(crate) fn resolve_placeholder_index(
    token: &str,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<usize, LixError> {
    let placeholder = parse_placeholder_ref(token)?;
    resolve_placeholder_ref(placeholder, params_len, state)
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
    use super::{
        parse_placeholder_ref, resolve_placeholder_index, resolve_placeholder_ref, PlaceholderRef,
        PlaceholderState,
    };

    #[test]
    fn parses_placeholder_kinds() {
        assert_eq!(parse_placeholder_ref("?").unwrap(), PlaceholderRef::Next);
        assert_eq!(
            parse_placeholder_ref(" ?3 ").unwrap(),
            PlaceholderRef::Explicit(3)
        );
        assert_eq!(
            parse_placeholder_ref("$2").unwrap(),
            PlaceholderRef::Explicit(2)
        );
    }

    #[test]
    fn advances_ordinal_placeholders() {
        let mut state = PlaceholderState::new();

        assert_eq!(
            resolve_placeholder_ref(PlaceholderRef::Next, 3, &mut state).unwrap(),
            0
        );
        assert_eq!(
            resolve_placeholder_ref(PlaceholderRef::Next, 3, &mut state).unwrap(),
            1
        );
        assert_eq!(
            resolve_placeholder_ref(PlaceholderRef::Explicit(3), 3, &mut state).unwrap(),
            2
        );
    }

    #[test]
    fn preserves_legacy_token_entrypoint() {
        let mut state = PlaceholderState::new();

        assert_eq!(resolve_placeholder_index("?", 3, &mut state).unwrap(), 0);
        assert_eq!(resolve_placeholder_index("?", 3, &mut state).unwrap(), 1);
        assert_eq!(resolve_placeholder_index("?3", 3, &mut state).unwrap(), 2);
    }

    #[test]
    fn rejects_out_of_range_placeholders() {
        let mut state = PlaceholderState::new();
        let error = resolve_placeholder_index("$2", 1, &mut state).unwrap_err();

        assert!(error.description.contains("parameter 2"));
    }
}
