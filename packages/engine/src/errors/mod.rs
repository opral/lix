use crate::lix_table_registry::public_lix_table_names;
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    TableNotFound,
    SqlUnknownTable,
    SqlUnknownColumn,
    InternalTableAccessDenied,
    ReadOnlyViewWriteDenied,
    VtableSchemaKeyRequired,
    TransactionControlStatementDenied,
    TransactionHandleNotFound,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TableNotFound => "LIX_ERROR_TABLE_NOT_FOUND",
            Self::SqlUnknownTable => "LIX_ERROR_SQL_UNKNOWN_TABLE",
            Self::SqlUnknownColumn => "LIX_ERROR_SQL_UNKNOWN_COLUMN",
            Self::InternalTableAccessDenied => "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED",
            Self::ReadOnlyViewWriteDenied => "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED",
            Self::VtableSchemaKeyRequired => "LIX_ERROR_VTABLE_SCHEMA_KEY_REQUIRED",
            Self::TransactionControlStatementDenied => {
                "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED"
            }
            Self::TransactionHandleNotFound => "LIX_ERROR_TRANSACTION_HANDLE_NOT_FOUND",
        }
    }

    pub const fn all() -> &'static [Self] {
        &[
            Self::TableNotFound,
            Self::SqlUnknownTable,
            Self::SqlUnknownColumn,
            Self::InternalTableAccessDenied,
            Self::ReadOnlyViewWriteDenied,
            Self::VtableSchemaKeyRequired,
            Self::TransactionControlStatementDenied,
            Self::TransactionHandleNotFound,
        ]
    }
}

fn build_error(code: ErrorCode, description: &str) -> LixError {
    LixError::new(code.as_str(), description)
}

pub(crate) fn table_not_found_read_error() -> LixError {
    let available_tables = public_lix_table_names().join(", ");
    build_error(
        ErrorCode::TableNotFound,
        &format!(
            "Read queries must target Lix views (`lix_*`) only. Available tables: {available_tables}. Schemas are available via `lix_stored_schema`."
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
    build_error(
        ErrorCode::InternalTableAccessDenied,
        "Queries against `lix_internal_*` are not allowed. Use public `lix_*` views.",
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

pub(crate) fn vtable_schema_key_required_error() -> LixError {
    build_error(
        ErrorCode::VtableSchemaKeyRequired,
        "This write targets a schema-scoped vtable. Add a WHERE predicate that resolves schema_key (for example: schema_key = 'markdown_v2_block' or schema_key = ?). This prevents accidental cross-schema updates/deletes.",
    )
}

pub(crate) fn transaction_control_statement_denied_error() -> LixError {
    build_error(
        ErrorCode::TransactionControlStatementDenied,
        "Use transaction APIs instead: beginTransaction(), transaction(), or executeTransaction().",
    )
}

pub(crate) fn transaction_handle_not_found_error() -> LixError {
    build_error(
        ErrorCode::TransactionHandleNotFound,
        "The transaction handle is invalid or already closed. Open a new transaction with beginTransaction().",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        internal_table_access_denied_error, read_only_view_write_error, sql_unknown_column_error,
        sql_unknown_table_error, table_not_found_read_error,
        transaction_control_statement_denied_error, transaction_handle_not_found_error,
        vtable_schema_key_required_error, ErrorCode,
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
        let table_not_found = table_not_found_read_error();
        assert_eq!(table_not_found.code, "LIX_ERROR_TABLE_NOT_FOUND");

        let internal_access = internal_table_access_denied_error();
        assert_eq!(
            internal_access.code,
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

        let schema_key_required = vtable_schema_key_required_error();
        assert_eq!(
            schema_key_required.code,
            "LIX_ERROR_VTABLE_SCHEMA_KEY_REQUIRED"
        );

        let transaction_control_denied = transaction_control_statement_denied_error();
        assert_eq!(
            transaction_control_denied.code,
            "LIX_ERROR_TRANSACTION_CONTROL_STATEMENT_DENIED"
        );

        let transaction_handle_not_found = transaction_handle_not_found_error();
        assert_eq!(
            transaction_handle_not_found.code,
            "LIX_ERROR_TRANSACTION_HANDLE_NOT_FOUND"
        );
    }

    #[test]
    fn agent_entrypoints_use_error_catalog_constructors() {
        let engine_src = include_str!("../engine.rs");
        assert!(engine_src.contains("errors::internal_table_access_denied_error()"));

        let preprocess_src = include_str!("../sql/planning/preprocess.rs");
        assert!(preprocess_src.contains("errors::table_not_found_read_error()"));

        let classification_src = include_str!("../error_classification.rs");
        assert!(classification_src.contains("errors::sql_unknown_table_error("));
        assert!(classification_src.contains("errors::sql_unknown_column_error("));
        assert!(classification_src.contains("errors::table_not_found_read_error()"));

        let state_history_write_src =
            include_str!("../sql/planning/rewrite_engine/steps/lix_state_history_view_write.rs");
        assert!(state_history_write_src.contains("errors::read_only_view_write_error("));

        let entity_view_write_src =
            include_str!("../sql/planning/rewrite_engine/entity_views/write.rs");
        assert!(entity_view_write_src.contains("errors::read_only_view_write_error("));

        let vtable_write_src = include_str!("../sql/planning/rewrite_engine/steps/vtable_write.rs");
        assert!(vtable_write_src.contains("errors::vtable_schema_key_required_error"));

        let api_src = include_str!("../api.rs");
        assert!(api_src.contains("errors::transaction_control_statement_denied_error()"));
    }
}
