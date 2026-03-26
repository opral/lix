pub(crate) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
