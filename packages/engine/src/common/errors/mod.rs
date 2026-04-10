use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ErrorCode {
    AlreadyInitialized,
    NotInitialized,
    LiveStateNotReady,
    UnexpectedStatementCount,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyInitialized => "LIX_ERROR_ALREADY_INITIALIZED",
            Self::NotInitialized => "LIX_ERROR_NOT_INITIALIZED",
            Self::LiveStateNotReady => "LIX_ERROR_LIVE_STATE_NOT_READY",
            Self::UnexpectedStatementCount => "LIX_ERROR_UNEXPECTED_STATEMENT_COUNT",
        }
    }
}

fn build_error(code: ErrorCode, description: &str) -> LixError {
    LixError::new(code.as_str(), description)
}

pub(crate) fn already_initialized_error() -> LixError {
    build_error(
        ErrorCode::AlreadyInitialized,
        "Lix is already initialized in this backend target.",
    )
}

pub(crate) fn not_initialized_error() -> LixError {
    build_error(
        ErrorCode::NotInitialized,
        "Lix is not initialized in this backend target. Initialize it before opening.",
    )
}

pub(crate) fn live_state_not_ready_error() -> LixError {
    build_error(
        ErrorCode::LiveStateNotReady,
        "Lix live state is not ready. Rebuild live state before opening or executing tracked operations.",
    )
}

pub(crate) fn unexpected_statement_count_error(
    context: &str,
    expected: usize,
    actual: usize,
) -> LixError {
    build_error(
        ErrorCode::UnexpectedStatementCount,
        &format!("{context}: expected {expected} statement result(s), got {actual}"),
    )
}
