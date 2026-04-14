use crate::catalog::{
    builtin_catalog_compiler_facade, CatalogCompilerApi, CatalogWriteTargetKind,
    FilesystemRelationKind,
};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::binder::insert_values_rows_mut;
use crate::LixError;
use sqlparser::ast::{Expr, Statement, TableObject, Value as AstValue, ValueWithSpan};

pub(crate) fn ensure_generated_filesystem_insert_ids<P: LixFunctionProvider>(
    statements: &mut [Statement],
    functions: &SharedFunctionProvider<P>,
) -> Result<(), LixError> {
    for statement in statements.iter_mut() {
        if !statement_requires_generated_filesystem_insert_id(statement) {
            continue;
        }
        let Statement::Insert(insert) = statement else {
            continue;
        };

        let current_column_count = insert.columns.len();
        insert.columns.push("id".into());
        let Some(rows) = insert_values_rows_mut(insert) else {
            continue;
        };
        for row in rows.iter_mut() {
            if row.len() != current_column_count {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "filesystem insert row length does not match column count"
                        .to_string(),
                });
            }
            row.push(string_literal_expr(functions.call_uuid_v7()));
        }
    }

    Ok(())
}

fn statement_requires_generated_filesystem_insert_id(statement: &Statement) -> bool {
    let Statement::Insert(insert) = statement else {
        return false;
    };
    if !statement_targets_generated_file_surface(&insert.table) {
        return false;
    }
    let data_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("data"));
    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("id"));
    data_index.is_some() && path_index.is_some() && id_index.is_none()
}

fn statement_targets_generated_file_surface(table: &TableObject) -> bool {
    let TableObject::TableName(name) = table else {
        return false;
    };
    builtin_catalog_compiler_facade()
        .write_surface_semantics_for_object_name(name)
        .ok()
        .flatten()
        .is_some_and(|semantics| {
            semantics.target_kind == CatalogWriteTargetKind::Filesystem
                && semantics.filesystem_kind == Some(FilesystemRelationKind::File)
        })
}

fn string_literal_expr(value: String) -> Expr {
    Expr::Value(ValueWithSpan::from(AstValue::SingleQuotedString(value)))
}

#[cfg(test)]
mod tests {
    use super::ensure_generated_filesystem_insert_ids;
    use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
    use crate::sql::parse_sql;
    use sqlparser::ast::Statement;

    struct FixedUuidProvider;

    impl LixFunctionProvider for FixedUuidProvider {
        fn uuid_v7(&mut self) -> String {
            "uuid-1".to_string()
        }

        fn timestamp(&mut self) -> String {
            "2026-01-01T00:00:00Z".to_string()
        }
    }

    fn parse_one(sql: &str) -> Statement {
        let mut statements = parse_sql(sql).expect("sql should parse");
        assert_eq!(statements.len(), 1);
        statements.remove(0)
    }

    #[test]
    fn generated_insert_ids_use_catalog_write_surface_metadata_for_file_surfaces() {
        let mut statements = vec![parse_one(
            "INSERT INTO lix_file (path, data) VALUES ('a', X'01')",
        )];
        let functions = SharedFunctionProvider::new(FixedUuidProvider);

        ensure_generated_filesystem_insert_ids(&mut statements, &functions)
            .expect("generated ids should succeed");

        let Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert");
        };
        assert!(insert
            .columns
            .iter()
            .any(|column| column.value.eq_ignore_ascii_case("id")));
    }

    #[test]
    fn generated_insert_ids_do_not_apply_to_directory_surfaces() {
        let mut statements = vec![parse_one("INSERT INTO lix_directory (path) VALUES ('a')")];
        let functions = SharedFunctionProvider::new(FixedUuidProvider);

        ensure_generated_filesystem_insert_ids(&mut statements, &functions)
            .expect("filesystem insert preprocessing should succeed");

        let Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert");
        };
        assert!(!insert
            .columns
            .iter()
            .any(|column| column.value.eq_ignore_ascii_case("id")));
    }
}
