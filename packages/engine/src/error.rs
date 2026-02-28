#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LixError {
    pub code: String,
    pub title: String,
    pub description: String,
}

impl LixError {
    pub fn new(
        code: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            title: title.into(),
            description: description.into(),
        }
    }

    pub fn unknown(description: impl Into<String>) -> Self {
        Self::new("LIX_ERROR_UNKNOWN", "Unknown error", description)
    }

    pub fn format(&self) -> String {
        format!(
            "code: {}\ntitle: {}\ndescription: {}",
            self.code, self.title, self.description
        )
    }
}

impl std::fmt::Display for LixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format())
    }
}

impl std::error::Error for LixError {}
