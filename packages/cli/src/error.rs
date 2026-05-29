use lix_sdk::LixError;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CliError {
    InvalidArgs(&'static str),
    Message(String),
    Io {
        context: &'static str,
        source: std::io::Error,
    },
    Lix {
        context: &'static str,
        source: LixError,
    },
}

impl CliError {
    pub fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    pub fn from_lix(context: &'static str, source: LixError) -> Self {
        Self::Lix { context, source }
    }

    pub fn hint(&self) -> Option<&str> {
        match self {
            Self::Lix { source, .. } => source.hint.as_deref(),
            _ => None,
        }
    }
}

impl Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgs(message) => write!(f, "invalid arguments: {message}"),
            Self::Message(message) => write!(f, "{message}"),
            Self::Io { context, source } => write!(f, "{context}: {source}"),
            Self::Lix { context, source } => {
                write!(f, "{context}: {}", source.message)
            }
        }
    }
}

impl std::error::Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_returns_none_for_non_lix_variants() {
        assert_eq!(CliError::InvalidArgs("bad").hint(), None);
        assert_eq!(CliError::msg("oops").hint(), None);
        let io_err = CliError::io(
            "reading",
            std::io::Error::new(std::io::ErrorKind::Other, "boom"),
        );
        assert_eq!(io_err.hint(), None);
    }

    #[test]
    fn hint_returns_lix_hint_when_attached() {
        let lix_err = LixError::new("LIX_ERROR_FOO", "desc").with_hint("try lix_json(...)");
        let cli_err = CliError::from_lix("sql execution failed", lix_err);
        assert_eq!(cli_err.hint(), Some("try lix_json(...)"));
    }

    #[test]
    fn hint_returns_none_when_lix_error_has_no_hint() {
        let lix_err = LixError::new("LIX_ERROR_FOO", "desc");
        let cli_err = CliError::from_lix("sql execution failed", lix_err);
        assert_eq!(cli_err.hint(), None);
    }

    #[test]
    fn display_format_omits_hint_line() {
        // hints are rendered separately via `render_hints`, not via Display
        let lix_err = LixError::new("LIX_ERROR_FOO", "boom").with_hint("fix it");
        let cli_err = CliError::from_lix("sql execution failed", lix_err);
        assert_eq!(cli_err.to_string(), "sql execution failed: boom");
    }
}
