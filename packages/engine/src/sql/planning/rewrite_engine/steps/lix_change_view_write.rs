use sqlparser::ast::{Delete, FromTable, Insert, TableFactor, TableObject, Update};

use crate::engine::sql::planning::rewrite_engine::object_name_matches;
use crate::{errors, LixError};

const LIX_CHANGE_VIEW_NAME: &str = "lix_change";

pub fn reject_insert(insert: &Insert) -> Result<(), LixError> {
    if table_object_is_lix_change(&insert.table) {
        return Err(read_only_error("INSERT"));
    }
    Ok(())
}

pub fn reject_update(update: &Update) -> Result<(), LixError> {
    if table_with_joins_is_lix_change(&update.table) {
        return Err(read_only_error("UPDATE"));
    }
    Ok(())
}

pub fn reject_delete(delete: &Delete) -> Result<(), LixError> {
    if delete_from_is_lix_change(delete) {
        return Err(read_only_error("DELETE"));
    }
    Ok(())
}

fn read_only_error(operation: &str) -> LixError {
    errors::read_only_view_write_error(LIX_CHANGE_VIEW_NAME, operation)
}

fn table_object_is_lix_change(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, LIX_CHANGE_VIEW_NAME),
        _ => false,
    }
}

fn table_with_joins_is_lix_change(table: &sqlparser::ast::TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_CHANGE_VIEW_NAME)
        )
}

fn delete_from_is_lix_change(delete: &Delete) -> bool {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            tables.iter().any(table_with_joins_is_lix_change)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{reject_delete, reject_insert, reject_update};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn rejects_insert_into_lix_change() {
        let statement = parse_statement(
            "INSERT INTO lix_change (id, entity_id, schema_key, schema_version, file_id, plugin_key, created_at) \
             VALUES ('c1', 'e1', 's1', '1', 'lix', 'lix', '2026-01-01T00:00:00Z')",
        );
        let Statement::Insert(insert) = statement else {
            panic!("expected INSERT");
        };
        let err = reject_insert(&insert).expect_err("lix_change should be read-only");
        assert_eq!(err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }

    #[test]
    fn rejects_update_lix_change() {
        let statement = parse_statement("UPDATE lix_change SET schema_key = 'x' WHERE id = 'c1'");
        let Statement::Update(update) = statement else {
            panic!("expected UPDATE");
        };
        let err = reject_update(&update).expect_err("lix_change should be read-only");
        assert_eq!(err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }

    #[test]
    fn rejects_delete_from_lix_change() {
        let statement = parse_statement("DELETE FROM lix_change WHERE id = 'c1'");
        let Statement::Delete(delete) = statement else {
            panic!("expected DELETE");
        };
        let err = reject_delete(&delete).expect_err("lix_change should be read-only");
        assert_eq!(err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }

    fn parse_statement(sql: &str) -> Statement {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        statements.remove(0)
    }
}
