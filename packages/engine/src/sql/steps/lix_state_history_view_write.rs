use sqlparser::ast::{
    Delete, FromTable, Insert, ObjectName, ObjectNamePart, TableFactor, TableObject, Update,
};

use crate::LixError;

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";

pub fn reject_insert(insert: &Insert) -> Result<(), LixError> {
    if table_object_is_lix_state_history(&insert.table) {
        return Err(read_only_error("INSERT"));
    }
    Ok(())
}

pub fn reject_update(update: &Update) -> Result<(), LixError> {
    if table_with_joins_is_lix_state_history(&update.table) {
        return Err(read_only_error("UPDATE"));
    }
    Ok(())
}

pub fn reject_delete(delete: &Delete) -> Result<(), LixError> {
    if delete_from_is_lix_state_history(delete) {
        return Err(read_only_error("DELETE"));
    }
    Ok(())
}

fn read_only_error(operation: &str) -> LixError {
    LixError {
        message: format!("lix_state_history is read-only; {operation} is not supported"),
    }
}

fn table_object_is_lix_state_history(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME),
        _ => false,
    }
}

fn table_with_joins_is_lix_state_history(table: &sqlparser::ast::TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME)
        )
}

fn delete_from_is_lix_state_history(delete: &Delete) -> bool {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            tables.iter().any(table_with_joins_is_lix_state_history)
        }
    }
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}
