use crate::LixError;

pub(crate) fn unsupported(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_UNSUPPORTED_SQL, message.into())
}

/// Binding cannot publish a commit; report the phase rather than infer from codes.
pub(crate) fn rejected(mut error: LixError) -> LixError {
    let mut details = match error.details.take().map(|details| *details) {
        Some(serde_json::Value::Object(fields)) => fields,
        Some(value) => serde_json::Map::from_iter([("cause".into(), value)]),
        None => serde_json::Map::new(),
    };
    details.insert("outcome".into(), "not_committed".into());
    details.insert("executionPhase".into(), "binding".into());
    error.with_details(serde_json::Value::Object(details))
}
