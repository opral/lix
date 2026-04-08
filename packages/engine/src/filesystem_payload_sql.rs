pub(crate) fn insert_filesystem_payload_domain_changes_sql(
    rows_sql: &str,
    untracked: bool,
) -> String {
    crate::backend::storage_sql::queries::state::insert_filesystem_payload_domain_changes_sql(
        rows_sql, untracked,
    )
}
