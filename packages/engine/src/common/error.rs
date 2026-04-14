#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LixError {
    pub code: String,
    pub description: String,
}

impl LixError {
    pub fn new(code: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            description: description.into(),
        }
    }

    pub fn unknown(description: impl Into<String>) -> Self {
        Self::new("LIX_ERROR_UNKNOWN", description)
    }

    pub fn format(&self) -> String {
        format!("code: {}\ndescription: {}", self.code, self.description)
    }
}

pub(crate) fn is_missing_relation_error(err: &LixError) -> bool {
    if err.code == "LIX_ERROR_SQL_UNKNOWN_TABLE" || err.code == "LIX_ERROR_TABLE_NOT_FOUND" {
        return true;
    }
    let lower = err.description.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}

impl std::fmt::Display for LixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format())
    }
}

impl std::error::Error for LixError {}
