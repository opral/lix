use crate::LixError;

pub(crate) fn cel_parse_error(expression: &str, error: impl std::fmt::Display) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("failed to parse CEL expression '{expression}': {error}"),
        hint: None,
        details: None,
    }
}

pub(crate) fn cel_runtime_error(expression: &str, error: impl std::fmt::Display) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("failed to evaluate CEL expression '{expression}': {error}"),
        hint: None,
        details: None,
    }
}
