use crate::surfaces::{
    builtin_relation_inventory, protected_builtin_public_surface_names,
    relation_policy_choice_summary,
};
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ErrorCode {
    AlreadyInitialized,
    NotInitialized,
    LiveStateNotReady,
    TableNotFound,
    SchemaNotRegistered,
    SqlUnknownTable,
    SqlUnknownColumn,
    InternalTableAccessDenied,
    PublicCreateTableDenied,
    ReadOnlyViewWriteDenied,
    TransactionControlStatementDenied,
    FileDataExpectsBytes,
    UnexpectedStatementCount,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyInitialized => "LIX_ERROR_ALREADY_INITIALIZED",
            Self::NotInitialized => "LIX_ERROR_NOT_INITIALIZED",
            Self::LiveStateNotReady => "LIX_ERROR_LIVE_STATE_NOT_READY",
            Self::TableNotFound => "LIX_ERROR_TABLE_NOT_FOUND",
            Self::SchemaNotRegistered => "LIX_ERROR_SCHEMA_NOT_REGISTERED",
            Self::SqlUnknownTable => "LIX_ERROR_SQL_UNKNOWN_TABLE",
            Self::SqlUnknownColumn => "LIX_ERROR_SQL_UNKNOWN_COLUMN",
            Self::InternalTableAccessDenied => "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED",
            Self::PublicCreateTableDenied => "LIX_ERROR_PUBLIC_CREATE_TABLE_DENIED",
            Self::ReadOnlyViewWriteDenied => "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED",
            Self::TransactionControlStatementDenied => {
                "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED"
            }
            Self::FileDataExpectsBytes => "LIX_ERROR_FILE_DATA_EXPECTS_BYTES",
            Self::UnexpectedStatementCount => "LIX_ERROR_UNEXPECTED_STATEMENT_COUNT",
        }
    }

    #[cfg(test)]
    pub const fn all() -> &'static [Self] {
        &[
            Self::AlreadyInitialized,
            Self::NotInitialized,
            Self::LiveStateNotReady,
            Self::TableNotFound,
            Self::SchemaNotRegistered,
            Self::SqlUnknownTable,
            Self::SqlUnknownColumn,
            Self::InternalTableAccessDenied,
            Self::PublicCreateTableDenied,
            Self::ReadOnlyViewWriteDenied,
            Self::TransactionControlStatementDenied,
            Self::FileDataExpectsBytes,
            Self::UnexpectedStatementCount,
        ]
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

pub(crate) fn table_not_found_read_error() -> LixError {
    let available_tables = protected_builtin_public_surface_names().join(", ");
    build_error(
        ErrorCode::TableNotFound,
        &format!(
            "Table not found. Known Lix tables: {available_tables}. If you are querying custom backend tables, ensure they exist in the connected database."
        ),
    )
}

pub(crate) fn schema_not_registered_error(
    schema_key: &str,
    available_schema_keys: &[&str],
) -> LixError {
    let available = if available_schema_keys.is_empty() {
        "Available schemas: (none).".to_string()
    } else {
        format!("Available schemas: {}.", available_schema_keys.join(", "))
    };
    build_error(
        ErrorCode::SchemaNotRegistered,
        &format!(
            "Schema `{schema_key}` is not registered. Register or install the schema before querying it. {available} Inspect registered schemas via `SELECT * FROM lix_registered_schema`."
        ),
    )
}

pub(crate) fn sql_unknown_table_error(
    table_name: &str,
    available_tables: &[&str],
    offset: Option<usize>,
) -> LixError {
    let available_tables = if available_tables.is_empty() {
        "Available tables: (none).".to_string()
    } else {
        format!("Available tables: {}.", available_tables.join(", "))
    };
    let location = offset
        .map(|value| format!(" Location: SQL offset {value}."))
        .unwrap_or_default();
    build_error(
        ErrorCode::SqlUnknownTable,
        &format!("Table `{table_name}` does not exist. {available_tables}{location}"),
    )
}

pub(crate) fn sql_unknown_column_error(
    column_name: &str,
    table_name: Option<&str>,
    available_columns: &[&str],
    offset: Option<usize>,
) -> LixError {
    let table_segment = table_name
        .map(|table| format!(" on `{table}`"))
        .unwrap_or_else(|| " in this query".to_string());
    let available_columns = if available_columns.is_empty() {
        "Available columns: (unknown).".to_string()
    } else {
        format!("Available columns: {}.", available_columns.join(", "))
    };
    let location = offset
        .map(|value| format!(" Location: SQL offset {value}."))
        .unwrap_or_default();
    build_error(
        ErrorCode::SqlUnknownColumn,
        &format!(
            "Column `{column_name}` does not exist{table_segment}. {available_columns}{location}"
        ),
    )
}

pub(crate) fn internal_table_access_denied_error() -> LixError {
    let inventory = builtin_relation_inventory();
    let available_tables = inventory.protected_builtin_public_surfaces.join(", ");
    let internal_storage_namespaces = inventory
        .internal_relation_families
        .iter()
        .map(|family| format!("`{}*`", family.prefix))
        .collect::<Vec<_>>()
        .join(", ");
    build_error(
        ErrorCode::InternalTableAccessDenied,
        &format!(
            "Direct writes against internal storage relations can lead to data corruption. {policy_choice} Protected internal storage includes exact built-in tables plus managed relation families such as {internal_storage_namespaces}. DDL against internal storage and protected Lix system relations is also denied. Public SQL tables remain writable, including `lix_state` and `lix_state_by_version`. Public SQL tables: {available_tables}.",
            policy_choice = relation_policy_choice_summary(),
        ),
    )
}

pub(crate) fn public_create_table_denied_error() -> LixError {
    build_error(
        ErrorCode::PublicCreateTableDenied,
        "CREATE TABLE is not supported in public Lix SQL. Instead, store a schema definition in `lix_registered_schema`; registered schemas become queryable entity views.",
    )
}

pub(crate) fn mixed_public_internal_query_error(internal_tables: &[String]) -> LixError {
    let available_tables = protected_builtin_public_surface_names().join(", ");
    let internal_tables = if internal_tables.is_empty() {
        "`lix_internal_*`".to_string()
    } else {
        internal_tables
            .iter()
            .map(|table| format!("`{table}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    build_error(
        ErrorCode::InternalTableAccessDenied,
        &format!(
            "Queries that reference public Lix tables must not also access internal engine tables in the same statement. Internal tables referenced: {internal_tables}. Public SQL tables: {available_tables}."
        ),
    )
}

pub(crate) fn read_only_view_write_error(view_name: &str, operation: &str) -> LixError {
    let guidance = if let Some(base_view) = view_name.strip_suffix("_history") {
        format!("Use `{base_view}` or `{base_view}_by_version` for writes.")
    } else {
        "Use the corresponding writable view for writes.".to_string()
    };
    build_error(
        ErrorCode::ReadOnlyViewWriteDenied,
        &format!("`{view_name}` is read-only. `{operation}` is not supported. {guidance}"),
    )
}

pub(crate) fn transaction_control_statement_denied_error() -> LixError {
    build_error(
        ErrorCode::TransactionControlStatementDenied,
        "Standalone transaction control is not supported via execute(). Use a single BEGIN ... COMMIT script or the typed transaction API.",
    )
}

pub(crate) const FILE_DATA_EXPECTS_BYTES_MESSAGE: &str = "data expects bytes. To insert text use lix_text_encode(): INSERT INTO lix_file (path, data) VALUES ('file.txt', lix_text_encode('your text'))";

pub(crate) fn file_data_expects_bytes_error() -> LixError {
    build_error(
        ErrorCode::FileDataExpectsBytes,
        FILE_DATA_EXPECTS_BYTES_MESSAGE,
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

#[cfg(test)]
mod tests {
    use super::{
        already_initialized_error, file_data_expects_bytes_error,
        internal_table_access_denied_error, mixed_public_internal_query_error,
        not_initialized_error, public_create_table_denied_error, read_only_view_write_error,
        schema_not_registered_error, sql_unknown_column_error, sql_unknown_table_error,
        table_not_found_read_error, transaction_control_statement_denied_error,
        unexpected_statement_count_error, ErrorCode,
    };
    use std::collections::HashSet;

    #[test]
    fn error_code_strings_are_unique() {
        let mut seen = HashSet::new();
        for code in ErrorCode::all() {
            let inserted = seen.insert(code.as_str());
            assert!(inserted, "duplicate error code string: {}", code.as_str());
        }
    }

    #[test]
    fn constructors_include_code() {
        let already_initialized = already_initialized_error();
        assert_eq!(already_initialized.code, "LIX_ERROR_ALREADY_INITIALIZED");

        let table_not_found = table_not_found_read_error();
        assert_eq!(table_not_found.code, "LIX_ERROR_TABLE_NOT_FOUND");

        let not_initialized = not_initialized_error();
        assert_eq!(not_initialized.code, "LIX_ERROR_NOT_INITIALIZED");

        let schema_not_registered =
            schema_not_registered_error("markdown_v2_document", &["lix_key_value"]);
        assert_eq!(
            schema_not_registered.code,
            "LIX_ERROR_SCHEMA_NOT_REGISTERED"
        );

        let internal_access = internal_table_access_denied_error();
        assert_eq!(
            internal_access.code,
            "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED"
        );

        let create_table_denied = public_create_table_denied_error();
        assert_eq!(
            create_table_denied.code,
            "LIX_ERROR_PUBLIC_CREATE_TABLE_DENIED"
        );

        let mixed_public_internal = mixed_public_internal_query_error(&[String::from(
            "lix_internal_live_untracked_v1_lix_active_version",
        )]);
        assert_eq!(
            mixed_public_internal.code,
            "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED"
        );

        let unknown_table = sql_unknown_table_error("lix_sate", &["lix_state"], Some(11));
        assert_eq!(unknown_table.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");

        let unknown_column = sql_unknown_column_error(
            "plugin_key",
            Some("lix_working_changes"),
            &["schema_key", "status"],
            Some(47),
        );
        assert_eq!(unknown_column.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");

        let read_only = read_only_view_write_error("lix_state_history", "INSERT");
        assert_eq!(read_only.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

        let transaction_control_denied = transaction_control_statement_denied_error();
        assert_eq!(
            transaction_control_denied.code,
            "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED"
        );

        let file_data_expects_bytes = file_data_expects_bytes_error();
        assert_eq!(
            file_data_expects_bytes.code,
            "LIX_ERROR_FILE_DATA_EXPECTS_BYTES"
        );

        let unexpected_statement_count = unexpected_statement_count_error("unit test", 1, 2);
        assert_eq!(
            unexpected_statement_count.code,
            "LIX_ERROR_UNEXPECTED_STATEMENT_COUNT"
        );
    }

    #[test]
    fn agent_entrypoints_use_error_catalog_constructors() {
        let statement_support_src = include_str!("../../sql/support.rs");
        assert!(statement_support_src.contains("errors::internal_table_access_denied_error()"));

        let classification_src = include_str!("classification.rs");
        assert!(classification_src.contains("errors::sql_unknown_table_error("));
        assert!(classification_src.contains("errors::sql_unknown_column_error("));
        assert!(classification_src.contains("errors::table_not_found_read_error()"));

        let state_history_write_src = include_str!("../../sql/prepare/public_surface/mod.rs");
        assert!(state_history_write_src.contains("read_only_view_write_error("));

        let change_write_src = include_str!("../../sql/prepare/public_surface/mod.rs");
        assert!(change_write_src.contains("read_only_view_write_error("));

        let session_src = include_str!("../../session/mod.rs");
        assert!(session_src.contains("errors::transaction_control_statement_denied_error()"));
        assert!(session_src.contains("reject_public_create_table("));
    }
}
