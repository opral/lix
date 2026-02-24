use super::super::super::ast::nodes::Statement;
use sqlparser::ast::{FromTable, ObjectName, ObjectNamePart, TableObject, TableWithJoins};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CanonicalStateResolution {
    pub(crate) read_only_query: bool,
    pub(crate) should_invalidate_installed_plugins_cache: bool,
}

pub(crate) fn canonicalize_state_resolution(statements: &[Statement]) -> CanonicalStateResolution {
    CanonicalStateResolution {
        read_only_query: is_query_only_statements(statements),
        should_invalidate_installed_plugins_cache:
            should_invalidate_installed_plugins_cache_for_statements(statements),
    }
}

pub(crate) fn is_query_only_statements(statements: &[Statement]) -> bool {
    !statements.is_empty()
        && statements
            .iter()
            .all(|statement| matches!(statement, Statement::Query(_)))
}

pub(crate) fn should_invalidate_installed_plugins_cache_for_statements(
    statements: &[Statement],
) -> bool {
    statements
        .iter()
        .any(|statement| statement_targets_table_name(statement, "lix_internal_plugin"))
}

pub(crate) fn statement_targets_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Insert(insert) => table_object_targets_table_name(&insert.table, table_name),
        Statement::Update(update) => table_with_joins_targets_table_name(&update.table, table_name),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            tables
                .iter()
                .any(|table| table_with_joins_targets_table_name(table, table_name))
        }
        _ => false,
    }
}

pub(crate) fn table_object_targets_table_name(table: &TableObject, table_name: &str) -> bool {
    let TableObject::TableName(name) = table else {
        return false;
    };
    object_name_targets_table_name(name, table_name)
}

pub(crate) fn table_with_joins_targets_table_name(
    table: &TableWithJoins,
    table_name: &str,
) -> bool {
    let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation else {
        return false;
    };
    object_name_targets_table_name(name, table_name)
}

fn object_name_targets_table_name(name: &ObjectName, table_name: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
        .unwrap_or(false)
}
