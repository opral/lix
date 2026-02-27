use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CliError {
    InvalidArgs(&'static str),
    Message(String),
    Io {
        context: &'static str,
        source: std::io::Error,
    },
}

impl CliError {
    pub fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

impl Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgs(message) => write!(f, "invalid arguments: {message}"),
            Self::Message(message) => write!(f, "{message}"),
            Self::Io { context, source } => write!(f, "{context}: {source}"),
        }
    }
}

impl std::error::Error for CliError {}
