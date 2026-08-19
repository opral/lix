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
/// use lix::LixError;
///
/// let err = LixError::new(
///     "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION",
///     "json(...) is not supported",
/// )
/// .with_hint("cast the value with ::jsonb instead");
///
/// assert_eq!(err.hint(), Some("cast the value with ::jsonb instead"));
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

    /// Cedar denied an authorization request for the active account.
    pub const CODE_PERMISSION_DENIED: &'static str = "LIX_PERMISSION_DENIED";

    /// Repository-owned Cedar schema, policy, or entity data is invalid.
    pub const CODE_INVALID_PERMISSION_POLICY: &'static str =
        "LIX_INVALID_PERMISSION_POLICY";

    /// SQL syntax is valid, but the feature is intentionally outside the Lix
    /// SQL surface.
    pub const CODE_UNSUPPORTED_SQL: &'static str = "LIX_UNSUPPORTED_SQL";

    /// SQL planning succeeded far enough to produce a physical runtime shape
    /// that the current engine target cannot execute safely.
    pub const CODE_UNSUPPORTED_SQL_RUNTIME_PLAN: &'static str = "LIX_UNSUPPORTED_SQL_RUNTIME_PLAN";

    /// Storage I/O failed.
    pub const CODE_STORAGE_ERROR: &'static str = "LIX_STORAGE_ERROR";

    /// A coherent storage read was invalidated by a concurrent commit.
    /// Auto-commit read surfaces consume this internally by reopening the
    /// complete read/query against a fresh snapshot.
    pub const CODE_STORAGE_READ_EXPIRED: &'static str = "LIX_STORAGE_READ_EXPIRED";

    /// The selected storage cannot prove the requested persistence boundary.
    pub const CODE_STORAGE_DURABILITY_UNAVAILABLE: &'static str =
        "LIX_STORAGE_DURABILITY_UNAVAILABLE";

    /// A newer storage client fenced this writer, so this Lix instance can no
    /// longer serve requests.
    pub const CODE_STORAGE_FENCED: &'static str = "LIX_STORAGE_FENCED";

    /// The backing storage instance stopped and this Lix instance can no
    /// longer serve requests.
    pub const CODE_STORAGE_CLOSED: &'static str = "LIX_STORAGE_CLOSED";

    /// A storage commit may have been applied, but its caller did not receive
    /// a definitive result.
    pub const CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN: &'static str =
        "LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN";

    /// A server SQL mutation did not provide the required replay identity.
    pub const CODE_IDEMPOTENCY_KEY_REQUIRED: &'static str = "LIX_IDEMPOTENCY_KEY_REQUIRED";

    /// A replay identity was reused for a different logical mutation.
    pub const CODE_IDEMPOTENCY_KEY_REUSED: &'static str = "LIX_IDEMPOTENCY_KEY_REUSED";

    /// A mutation response cannot be retained safely for idempotent replay.
    pub const CODE_IDEMPOTENCY_RESPONSE_TOO_LARGE: &'static str =
        "LIX_IDEMPOTENCY_RESPONSE_TOO_LARGE";

    /// Optimistic transaction publication lost a race with a newer commit.
    pub const CODE_TRANSACTION_CONFLICT: &'static str = "LIX_TRANSACTION_CONFLICT";

    /// An internal engine invariant failed.
    pub const CODE_INTERNAL_ERROR: &'static str = "LIX_INTERNAL_ERROR";

    /// A plugin ZIP package or manifest is malformed, unsafe, or exceeds the
    /// static resource bounds accepted by the engine. Invalid embedded Lix
    /// schema definitions retain [`Self::CODE_SCHEMA_DEFINITION`].
    pub const CODE_INVALID_PLUGIN: &'static str = "LIX_ERROR_INVALID_PLUGIN";

    /// A file is materialized as durable plugin state, but the plugin needed
    /// to render that state is not installed on the file's branch.
    pub const CODE_PLUGIN_UNAVAILABLE: &'static str = "LIX_ERROR_PLUGIN_UNAVAILABLE";

    /// An incremental plugin write did not carry an exact, still-current
    /// private document observation. The client must re-read the file; the
    /// engine never guesses identity authority from equal byte hashes.
    pub const CODE_PLUGIN_OBSERVATION_STALE: &'static str = "LIX_ERROR_PLUGIN_OBSERVATION_STALE";

    /// Creating another live plugin Store would exceed the repository-wide
    /// runtime admission limit configured for this Engine.
    pub const CODE_PLUGIN_RESOURCE_LIMIT: &'static str = "LIX_ERROR_PLUGIN_RESOURCE_LIMIT";

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
    /// expressions). Users should cast inline JSON with `::jsonb`.
    pub const CODE_UNSUPPORTED_WRITE_EXPRESSION: &'static str =
        "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION";

    /// The schema JSON itself (the *definition*, not a row against it) is
    /// malformed — a missing `key`, an invalid primary-key column, or another
    /// leading slash, a reserved-namespace collision, or any other
    /// meta-schema validation failure.
    pub const CODE_SCHEMA_DEFINITION: &'static str = "LIX_ERROR_SCHEMA_DEFINITION";

    /// A public runtime schema registration attempted to use the `lix_*`
    /// namespace reserved for schemas owned and bootstrapped by Lix.
    pub const CODE_RESERVED_SCHEMA_NAMESPACE: &'static str = "LIX_RESERVED_SCHEMA_NAMESPACE";

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

    /// The selected branch has no ordinary commit above its undo boundary.
    pub const CODE_NOTHING_TO_UNDO: &'static str = "LIX_NOTHING_TO_UNDO";

    /// The selected branch has no abandoned action available to replay.
    pub const CODE_NOTHING_TO_REDO: &'static str = "LIX_NOTHING_TO_REDO";

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
    /// use lix::LixError;
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
    /// use lix::LixError;
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
        match error {
            crate::storage_adapter::StorageError::WriteConflict
            | crate::storage_adapter::StorageError::PreconditionFailed(_) => Self::new(
                Self::CODE_TRANSACTION_CONFLICT,
                "transaction snapshot is stale because tracked state changed before commit",
            )
            .with_hint("Retry the transaction against the latest committed state."),
            crate::storage_adapter::StorageError::Fenced => Self::new(
                Self::CODE_STORAGE_FENCED,
                "the storage writer was fenced by a newer client",
            )
            .with_hint(
                "Do not automatically retry this request; a mutation may still have completed.",
            )
            .with_details(json!({
                "retryable": false,
                "outcome": "unknown",
            })),
            crate::storage_adapter::StorageError::Closed(_) => Self::new(
                Self::CODE_STORAGE_CLOSED,
                "the storage instance closed and must be reopened",
            )
            .with_hint(
                "Do not automatically retry this request; a mutation may still have completed.",
            )
            .with_details(json!({
                "retryable": false,
                "outcome": "unknown",
            })),
            crate::storage_adapter::StorageError::CommitOutcomeUnknown(message) => Self::new(
                Self::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN,
                format!("the storage commit outcome is unknown: {message}"),
            )
            .with_hint(
                "Do not automatically retry this request; a mutation may still have completed.",
            )
            .with_details(json!({
                "retryable": false,
                "outcome": "unknown",
            })),
            crate::storage_adapter::StorageError::Durability => Self::new(
                Self::CODE_STORAGE_DURABILITY_UNAVAILABLE,
                "the storage backend cannot prove the requested durability boundary",
            ),
            crate::storage_adapter::StorageError::ReadExpired => Self::new(
                Self::CODE_STORAGE_READ_EXPIRED,
                "the coherent storage read was invalidated by a concurrent commit",
            )
            .with_details(json!({
                "retryable": true,
            })),
            error => Self::new(Self::CODE_STORAGE_ERROR, error.to_string()),
        }
    }
}

impl From<crate::storage_adapter::StorageWriteSetError> for LixError {
    fn from(error: crate::storage_adapter::StorageWriteSetError) -> Self {
        match error {
            crate::storage_adapter::StorageWriteSetError::Storage(error) => error.into(),
            error => Self::new(Self::CODE_STORAGE_ERROR, error.to_string()),
        }
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

    #[test]
    fn fenced_storage_error_is_terminal_and_not_retryable() {
        let error = LixError::from(crate::storage::StorageError::Fenced);

        assert_eq!(error.code, LixError::CODE_STORAGE_FENCED);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "retryable": false,
                "outcome": "unknown",
            }))
        );
    }

    #[test]
    fn unknown_commit_outcome_is_not_retryable() {
        let error = LixError::from(crate::storage::StorageError::CommitOutcomeUnknown(
            "storage reply was lost after commit".to_string(),
        ));

        assert_eq!(error.code, LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "retryable": false,
                "outcome": "unknown",
            }))
        );
        assert_eq!(
            error.hint(),
            Some("Do not automatically retry this request; a mutation may still have completed.")
        );
    }

    #[test]
    fn fenced_storage_write_set_error_preserves_the_terminal_code() {
        let error = LixError::from(crate::storage_adapter::StorageWriteSetError::Storage(
            crate::storage::StorageError::Fenced,
        ));

        assert_eq!(error.code, LixError::CODE_STORAGE_FENCED);
    }

    #[test]
    fn closed_storage_error_is_terminal_and_not_retryable() {
        let error = LixError::from(crate::storage::StorageError::Closed(
            "background worker panicked".to_string(),
        ));

        assert_eq!(error.code, LixError::CODE_STORAGE_CLOSED);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "retryable": false,
                "outcome": "unknown",
            }))
        );
    }
}
