use std::fmt::Write as _;

use serde_json::{Value as JsonValue, json};

/// Structured error type surfaced by Lix to every SDK binding.
///
/// Carries a machine-readable [`code`](Self::code), a human-readable
/// [`message`](Self::message), and an optional [`hint`](Self::hint)
/// suggesting how to recover. Hints follow the Postgres/rustc convention:
/// `message` states what went wrong in factual terms, and `hint` offers a
/// possible fix when one is known.
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
    pub message: String,
    pub hint: Option<String>,
    pub details: Option<JsonValue>,
}

impl LixError {
    /// True fallback — use when no more specific category fits. Producing
    /// sites should prefer the categorized codes below whenever possible;
    /// the SDK contract is that `LIX_ERROR_UNKNOWN` is the *last* resort,
    /// never the default.
    pub const CODE_UNKNOWN: &'static str = "LIX_ERROR_UNKNOWN";

    /// SQL text could not be parsed.
    pub const CODE_PARSE_ERROR: &'static str = "LIX_PARSE_ERROR";

    /// A SQL function name could not be resolved.
    pub const CODE_UDF_NOT_FOUND: &'static str = "LIX_UDF_NOT_FOUND";

    /// A SQL expression or function argument had an incompatible type.
    pub const CODE_TYPE_MISMATCH: &'static str = "LIX_TYPE_MISMATCH";

    /// A Lix JSON path argument used another dialect's path language instead
    /// of Lix's canonical variadic key/index segments.
    pub const CODE_INVALID_JSON_PATH: &'static str = "LIX_INVALID_JSON_PATH";

    /// SQL syntax belongs to another dialect and is outside the Lix SQL
    /// surface.
    pub const CODE_DIALECT_UNSUPPORTED: &'static str = "LIX_DIALECT_UNSUPPORTED";

    /// SQL parameters could not be bound to placeholders.
    pub const CODE_BINDING_ERROR: &'static str = "LIX_BINDING_ERROR";

    /// A caller supplied an invalid SQL parameter value or parameter list.
    pub const CODE_INVALID_PARAM: &'static str = "LIX_INVALID_PARAM";

    /// A SQL table or view name could not be resolved.
    pub const CODE_TABLE_NOT_FOUND: &'static str = "LIX_TABLE_NOT_FOUND";

    /// A SQL column name could not be resolved in the available projection.
    pub const CODE_COLUMN_NOT_FOUND: &'static str = "LIX_COLUMN_NOT_FOUND";

    /// A SQL write violated a primary-key, unique, NOT NULL, or other
    /// relational constraint.
    pub const CODE_CONSTRAINT_VIOLATION: &'static str = "LIX_CONSTRAINT_VIOLATION";

    /// A SQL write targeted a read-only internal/component surface.
    pub const CODE_READ_ONLY: &'static str = "LIX_ERROR_READ_ONLY";

    /// A history table was queried without an explicit commit/branch range.
    pub const CODE_HISTORY_FILTER_REQUIRED: &'static str = "LIX_HISTORY_FILTER_REQUIRED";

    /// SQL syntax is valid, but the feature is intentionally outside the Lix
    /// SQL surface.
    pub const CODE_UNSUPPORTED_SQL: &'static str = "LIX_UNSUPPORTED_SQL";

    /// SQL planning succeeded far enough to produce a physical runtime shape
    /// that the current engine target cannot execute safely.
    pub const CODE_UNSUPPORTED_SQL_RUNTIME_PLAN: &'static str = "LIX_UNSUPPORTED_SQL_RUNTIME_PLAN";

    /// Storage I/O failed.
    pub const CODE_STORAGE_ERROR: &'static str = "LIX_STORAGE_ERROR";

    /// An internal engine invariant failed.
    pub const CODE_INTERNAL_ERROR: &'static str = "LIX_INTERNAL_ERROR";

    /// A plugin ZIP package or manifest is malformed, unsafe, or exceeds the
    /// static resource bounds accepted by the engine. Invalid embedded Lix
    /// schema definitions retain [`Self::CODE_SCHEMA_DEFINITION`].
    pub const CODE_INVALID_PLUGIN: &'static str = "LIX_ERROR_INVALID_PLUGIN";

    /// A file is materialized as durable plugin state, but the plugin needed
    /// to render that state is not installed on the file's branch.
    pub const CODE_PLUGIN_UNAVAILABLE: &'static str = "LIX_ERROR_PLUGIN_UNAVAILABLE";

    /// Write-time failure where user data did not conform to a registered
    /// schema (type mismatch, missing required field, pattern violation,
    /// additionalProperties, etc.). Raised from the JSON-Schema validator
    /// run over a candidate row's snapshot.
    pub const CODE_SCHEMA_VALIDATION: &'static str = "LIX_ERROR_SCHEMA_VALIDATION";

    /// A foreign-key constraint could not be satisfied. Covers both the
    /// insert-side "no matching target row" failure and the delete-side
    /// "still referenced" (restrict) failure.
    pub const CODE_FOREIGN_KEY: &'static str = "LIX_ERROR_FOREIGN_KEY";

    /// A row references a non-null `file_id` that has no matching `lix_file`
    /// descriptor in the same effective branch scope.
    pub const CODE_FILE_NOT_FOUND: &'static str = "LIX_ERROR_FILE_NOT_FOUND";

    /// A primary-key or `x-lix-unique` constraint was violated — another
    /// row already owns the value(s) for the declared pointer group.
    pub const CODE_UNIQUE: &'static str = "LIX_ERROR_UNIQUE";

    /// An `INSERT ... VALUES (...)` expression is not supported by the
    /// public write surface (e.g. `json(...)`, subqueries, arbitrary SQL
    /// expressions). Users should wrap inline JSON with `lix_json(...)`.
    pub const CODE_UNSUPPORTED_WRITE_EXPRESSION: &'static str =
        "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION";

    /// The schema JSON itself (the *definition*, not a row against it) is
    /// malformed — a missing `x-lix-key`, a JSON-Pointer without the
    /// leading slash, a reserved-namespace collision, or any other
    /// meta-schema validation failure.
    pub const CODE_SCHEMA_DEFINITION: &'static str = "LIX_ERROR_SCHEMA_DEFINITION";

    /// The logical Lix handle/session has been closed and cannot run further
    /// operations. Close is a resource-release lifecycle boundary, not a
    /// durability boundary.
    pub const CODE_CLOSED: &'static str = "LIX_ERROR_CLOSED";

    /// An operation is incompatible with the current session mode or state.
    pub const CODE_INVALID_SESSION_STATE: &'static str = "LIX_INVALID_SESSION_STATE";

    /// A merge found incompatible changes to the same tracked-state identity.
    pub const CODE_MERGE_CONFLICT: &'static str = "LIX_MERGE_CONFLICT";

    /// A caller referenced a branch id that has no matching branch ref.
    pub const CODE_BRANCH_NOT_FOUND: &'static str = "LIX_BRANCH_NOT_FOUND";

    /// A caller referenced a commit id that has no matching commit record.
    pub const CODE_COMMIT_NOT_FOUND: &'static str = "LIX_COMMIT_NOT_FOUND";

    /// A staged row's storage scope flags disagree, such as a global row not
    /// using the reserved global branch id.
    pub const CODE_INVALID_STORAGE_SCOPE: &'static str = "LIX_ERROR_INVALID_STORAGE_SCOPE";

    /// Merge graph analysis found multiple equally valid merge bases.
    pub const CODE_AMBIGUOUS_MERGE_BASE: &'static str = "LIX_AMBIGUOUS_MERGE_BASE";

    /// A merge request is well-formed but nonsensical for the commit graph,
    /// such as merging a branch into itself.
    pub const CODE_INVALID_MERGE: &'static str = "LIX_INVALID_MERGE";

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            hint: None,
            details: None,
        }
    }

    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new("LIX_ERROR_UNKNOWN", message)
    }

    pub fn branch_not_found(
        branch_id: impl Into<String>,
        operation: impl Into<String>,
        role: impl Into<String>,
    ) -> Self {
        let branch_id = branch_id.into();
        let operation = operation.into();
        let role = role.into();
        Self::new(
            Self::CODE_BRANCH_NOT_FOUND,
            format!("branch '{branch_id}' was not found"),
        )
        .with_details(json!({
            "branch_id": branch_id,
            "operation": operation,
            "role": role,
        }))
    }

    pub fn commit_not_found(
        commit_id: impl Into<String>,
        operation: impl Into<String>,
        role: impl Into<String>,
    ) -> Self {
        let commit_id = commit_id.into();
        let operation = operation.into();
        let role = role.into();
        Self::new(
            Self::CODE_COMMIT_NOT_FOUND,
            format!("commit '{commit_id}' was not found"),
        )
        .with_details(json!({
            "commit_id": commit_id,
            "operation": operation,
            "role": role,
        }))
    }

    pub fn ambiguous_merge_base(
        left_commit_id: impl Into<String>,
        right_commit_id: impl Into<String>,
        candidates: Vec<String>,
    ) -> Self {
        let left_commit_id = left_commit_id.into();
        let right_commit_id = right_commit_id.into();
        Self::new(
            Self::CODE_AMBIGUOUS_MERGE_BASE,
            format!("ambiguous merge base between '{left_commit_id}' and '{right_commit_id}'"),
        )
        .with_details(json!({
            "left_commit_id": left_commit_id,
            "right_commit_id": right_commit_id,
            "candidates": candidates,
        }))
    }

    pub fn invalid_self_merge(branch_id: impl Into<String>) -> Self {
        let branch_id = branch_id.into();
        Self::new(
            Self::CODE_INVALID_MERGE,
            format!("cannot merge branch '{branch_id}' into itself"),
        )
        .with_details(json!({
            "operation": "merge_branch",
            "target_branch_id": branch_id,
            "source_branch_id": branch_id,
        }))
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

    /// Attach machine-readable details to this error.
    pub fn with_details(mut self, details: JsonValue) -> Self {
        self.details = Some(details);
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
        let mut s = format!("code: {}\nmessage: {}", self.code, self.message);
        if let Some(hint) = &self.hint {
            let _ = write!(s, "\nhint: {hint}");
        }
        s
    }
}

impl From<crate::storage_adapter::StorageError> for LixError {
    fn from(error: crate::storage_adapter::StorageError) -> Self {
        Self::new(Self::CODE_STORAGE_ERROR, error.to_string())
    }
}

impl From<crate::storage_adapter::StorageWriteSetError> for LixError {
    fn from(error: crate::storage_adapter::StorageWriteSetError) -> Self {
        Self::new(Self::CODE_STORAGE_ERROR, error.to_string())
    }
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
            "code: LIX_ERROR_FOO\nmessage: something went wrong"
        );
        assert!(err.hint.is_none());
    }

    #[test]
    fn format_with_hint_appends_hint_line() {
        let err = LixError::new("LIX_ERROR_FOO", "something went wrong").with_hint("try the fix");
        assert_eq!(
            err.format(),
            "code: LIX_ERROR_FOO\nmessage: something went wrong\nhint: try the fix"
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
