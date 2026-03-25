pub(crate) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
