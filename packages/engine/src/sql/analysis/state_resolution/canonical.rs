use crate::catalog::{builtin_catalog_compiler_facade, CatalogCompilerApi, FilesystemRelationKind};
use sqlparser::ast::Statement;
use sqlparser::ast::{FromTable, ObjectName, TableObject, TableWithJoins};

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
    !statements.is_empty() && statements.iter().all(statement_is_query_only)
}

fn statement_is_query_only(statement: &Statement) -> bool {
    match statement {
        Statement::Query(_) => true,
        Statement::Explain { statement, .. } => statement_is_query_only(statement.as_ref()),
        _ => false,
    }
}

pub(crate) fn should_invalidate_installed_plugins_cache_for_statements(
    statements: &[Statement],
) -> bool {
    statements
        .iter()
        .any(statement_targets_plugin_cache_invalidating_surface)
}

fn statement_targets_plugin_cache_invalidating_surface(statement: &Statement) -> bool {
    match statement {
        Statement::Insert(insert) => {
            table_object_targets_plugin_cache_invalidating_surface(&insert.table)
        }
        Statement::Update(update) => {
            table_with_joins_targets_plugin_cache_invalidating_surface(&update.table)
        }
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            tables
                .iter()
                .any(table_with_joins_targets_plugin_cache_invalidating_surface)
        }
        _ => false,
    }
}

fn table_object_targets_plugin_cache_invalidating_surface(table: &TableObject) -> bool {
    let TableObject::TableName(name) = table else {
        return false;
    };
    object_name_targets_plugin_cache_invalidating_surface(name)
}

fn table_with_joins_targets_plugin_cache_invalidating_surface(table: &TableWithJoins) -> bool {
    let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation else {
        return false;
    };
    object_name_targets_plugin_cache_invalidating_surface(name)
}

fn object_name_targets_plugin_cache_invalidating_surface(name: &ObjectName) -> bool {
    builtin_catalog_compiler_facade()
        .write_surface_semantics_for_object_name(name)
        .ok()
        .flatten()
        .is_some_and(|semantics| semantics.filesystem_kind == Some(FilesystemRelationKind::File))
}

#[cfg(test)]
mod tests {
    use super::{
        is_query_only_statements, should_invalidate_installed_plugins_cache_for_statements,
    };

    fn parse_one(sql: &str) -> sqlparser::ast::Statement {
        crate::sql::parser::parse_sql_script(sql)
            .expect("SQL should parse")
            .pop()
            .expect("single statement")
    }

    #[test]
    fn explain_wrapped_query_counts_as_read_only() {
        let statement = parse_one("EXPLAIN ANALYZE SELECT 1");
        assert!(is_query_only_statements(&[statement]));
    }

    #[test]
    fn explain_wrapped_insert_is_not_read_only() {
        let statement =
            parse_one("EXPLAIN INSERT INTO lix_active_version (version_id) VALUES ('main')");
        assert!(!is_query_only_statements(&[statement]));
    }

    #[test]
    fn file_surface_writes_invalidate_installed_plugins_cache_via_catalog_metadata() {
        let statements = vec![parse_one(
            "UPDATE lix_file SET data = X'01' WHERE id = 'file-1'",
        )];
        assert!(should_invalidate_installed_plugins_cache_for_statements(
            &statements
        ));
    }

    #[test]
    fn non_file_surface_writes_do_not_invalidate_installed_plugins_cache() {
        let statements = vec![parse_one(
            "UPDATE lix_directory SET name = 'docs' WHERE id = 'dir-1'",
        )];
        assert!(!should_invalidate_installed_plugins_cache_for_statements(
            &statements
        ));
    }
}
