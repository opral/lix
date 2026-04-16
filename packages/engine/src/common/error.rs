/// Structured error type surfaced by Lix to every SDK binding.
///
/// Carries a machine-readable [`code`](Self::code), a human-readable
/// [`description`](Self::description), and an optional
/// [`hint`](Self::hint) suggesting how to recover. Hints follow the
/// Postgres/rustc convention: `description` states what went wrong in
/// factual terms, and `hint` offers a possible fix when one is known.
///
/// ```
/// use lix_engine::LixError;
///
/// let err = LixError::new(
///     "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION",
///     "json(...) is not supported",
/// )
/// .with_hint("use lix_json('...') instead");
///
/// assert_eq!(err.hint(), Some("use lix_json('...') instead"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LixError {
    pub code: String,
    pub description: String,
    pub hint: Option<String>,
}

impl LixError {
    pub fn new(code: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            description: description.into(),
            hint: None,
        }
    }

    pub fn unknown(description: impl Into<String>) -> Self {
        Self::new("LIX_ERROR_UNKNOWN", description)
    }

    /// Attach a hint to this error. Consumers render hints alongside the
    /// primary message (e.g. a CLI prints them as `hint: <text>`).
    ///
    /// ```
    /// use lix_engine::LixError;
    ///
    /// let err = LixError::new("CODE", "boom").with_hint("try this");
    /// assert_eq!(err.hint(), Some("try this"));
    /// ```
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Return the attached hint, if any.
    ///
    /// Returns `None` when no hint was attached at the error's producer
    /// site. This is the accessor SDK consumers should prefer over
    /// reading the `hint` field directly — it returns `Option<&str>`,
    /// avoiding the need for `.as_deref()` at the call site.
    ///
    /// ```
    /// use lix_engine::LixError;
    ///
    /// let without_hint = LixError::new("CODE", "boom");
    /// assert_eq!(without_hint.hint(), None);
    ///
    /// let with_hint = LixError::new("CODE", "boom").with_hint("fix it");
    /// assert_eq!(with_hint.hint(), Some("fix it"));
    /// ```
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn format(&self) -> String {
        let mut s = format!("code: {}\ndescription: {}", self.code, self.description);
        if let Some(hint) = &self.hint {
            s.push_str(&format!("\nhint: {hint}"));
        }
        s
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_without_hint_omits_hint_line() {
        let err = LixError::new("LIX_ERROR_FOO", "something went wrong");
        assert_eq!(
            err.format(),
            "code: LIX_ERROR_FOO\ndescription: something went wrong"
        );
        assert!(err.hint.is_none());
    }

    #[test]
    fn format_with_hint_appends_hint_line() {
        let err = LixError::new("LIX_ERROR_FOO", "something went wrong").with_hint("try the fix");
        assert_eq!(
            err.format(),
            "code: LIX_ERROR_FOO\ndescription: something went wrong\nhint: try the fix"
        );
    }

    #[test]
    fn with_hint_is_chainable_and_replaces_prior_hint() {
        let err = LixError::new("LIX_ERROR_FOO", "desc")
            .with_hint("first")
            .with_hint("second");
        assert_eq!(err.hint.as_deref(), Some("second"));
    }

    #[test]
    fn new_defaults_hint_to_none() {
        let err = LixError::new("CODE", "desc");
        assert_eq!(err.hint, None);
    }

    #[test]
    fn unknown_defaults_hint_to_none() {
        let err = LixError::unknown("desc");
        assert_eq!(err.code, "LIX_ERROR_UNKNOWN");
        assert_eq!(err.hint, None);
    }
}
