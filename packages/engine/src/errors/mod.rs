use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    TableNotFound,
    InternalTableAccessDenied,
    ReadOnlyViewWriteDenied,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TableNotFound => "LIX_ERROR_TABLE_NOT_FOUND",
            Self::InternalTableAccessDenied => "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED",
            Self::ReadOnlyViewWriteDenied => "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED",
        }
    }

    pub const fn all() -> &'static [Self] {
        &[
            Self::TableNotFound,
            Self::InternalTableAccessDenied,
            Self::ReadOnlyViewWriteDenied,
        ]
    }
}

fn build_error(code: ErrorCode, title: &str, description: &str) -> LixError {
    LixError::new(code.as_str(), title, description)
}

pub(crate) fn table_not_found_read_error() -> LixError {
    build_error(
        ErrorCode::TableNotFound,
        "Table does not exist",
        "Read queries must target Lix views (`lix_*`) only. Try: lix_state, lix_state_by_version, lix_state_history, lix_file, lix_directory. Schemas are available via `lix_stored_schema`.",
    )
}

pub(crate) fn internal_table_access_denied_error() -> LixError {
    build_error(
        ErrorCode::InternalTableAccessDenied,
        "Internal table access denied",
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
        "View is read-only",
        &format!("`{view_name}` is read-only. `{operation}` is not supported. {guidance}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        internal_table_access_denied_error, read_only_view_write_error, table_not_found_read_error,
        ErrorCode,
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

        let read_only = read_only_view_write_error("lix_state_history", "INSERT");
        assert_eq!(read_only.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }

    #[test]
    fn agent_entrypoints_use_error_catalog_constructors() {
        let engine_src = include_str!("../engine.rs");
        assert!(engine_src.contains("errors::internal_table_access_denied_error()"));
        assert!(engine_src.contains("errors::table_not_found_read_error()"));

        let preprocess_src = include_str!("../sql/planning/preprocess.rs");
        assert!(preprocess_src.contains("errors::table_not_found_read_error()"));

        let state_history_write_src =
            include_str!("../sql/planning/rewrite_engine/steps/lix_state_history_view_write.rs");
        assert!(state_history_write_src.contains("errors::read_only_view_write_error("));

        let entity_view_write_src =
            include_str!("../sql/planning/rewrite_engine/entity_views/write.rs");
        assert!(entity_view_write_src.contains("errors::read_only_view_write_error("));
    }
}
