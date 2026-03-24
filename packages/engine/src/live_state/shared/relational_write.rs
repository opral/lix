use crate::live_state::constraints::sql_literal;
use crate::Value;

pub(crate) fn normalized_insert_columns_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| format!(", {}", crate::live_state::constraints::quote_ident(column)))
        .collect::<String>()
}

pub(crate) fn normalized_insert_values_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

pub(crate) fn normalized_update_assignments_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| {
            format!(
                ", {} = excluded.{}",
                crate::live_state::constraints::quote_ident(column),
                crate::live_state::constraints::quote_ident(column)
            )
        })
        .collect::<String>()
}
