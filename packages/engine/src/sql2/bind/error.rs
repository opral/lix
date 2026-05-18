use crate::LixError;

pub(crate) fn unsupported(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_UNSUPPORTED_SQL, message.into())
}
