pub(crate) mod catalog;
pub(crate) mod classification;
mod r#type;

pub(crate) use catalog::ErrorCode;
pub(crate) use catalog::{
    already_initialized_error, file_data_expects_bytes_error, internal_table_access_denied_error,
    mixed_public_internal_query_error, not_initialized_error, read_only_view_write_error,
    schema_not_registered_error, sql_unknown_column_error, sql_unknown_table_error,
    table_not_found_read_error, transaction_control_statement_denied_error,
    unexpected_statement_count_error, vtable_schema_key_required_error,
};
pub use r#type::LixError;
