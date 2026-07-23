use datafusion::sql::sqlparser::ast::ObjectName;

use crate::LixError;

use super::super::catalog::{PublicCatalog, PublicSurfaceContract};
use super::expr::BoundColumnRef;
use super::write::BoundWriteOp;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundTable {
    pub(crate) name: String,
    pub(crate) surface: PublicSurfaceContract,
}

pub(crate) fn bind_exact_table_name(name: &ObjectName) -> Result<String, LixError> {
    if name.0.len() != 1 {
        return Err(super::error::unsupported(
            "qualified SQL table names are not supported",
        ));
    }
    name.0
        .first()
        .and_then(|part| part.as_ident())
        .map(|ident| {
            if ident.quote_style.is_some() {
                ident.value.clone()
            } else {
                ident.value.to_ascii_lowercase()
            }
        })
        .ok_or_else(|| super::error::unsupported("unsupported SQL table name"))
}

pub(crate) fn bind_public_table(
    catalog: &PublicCatalog,
    name: &ObjectName,
) -> Result<BoundTable, LixError> {
    let table_name = bind_exact_table_name(name)?;
    let surface = catalog.require_surface(&table_name)?.clone();
    Ok(BoundTable {
        name: table_name,
        surface,
    })
}

pub(crate) fn require_public_column<'a>(
    table: &'a BoundTable,
    column_name: &str,
) -> Result<&'a super::super::catalog::PublicColumn, LixError> {
    if table.surface.public_column(column_name).is_some() {
        return Ok(table
            .surface
            .public_column(column_name)
            .expect("checked public column"));
    }
    if table.surface.column(column_name).is_some() {
        return Err(LixError::new(
            LixError::CODE_COLUMN_NOT_FOUND,
            format!(
                "column '{column_name}' is not part of public SQL surface '{}'",
                table.name
            ),
        ));
    }
    Err(LixError::new(
        LixError::CODE_COLUMN_NOT_FOUND,
        format!(
            "column '{column_name}' does not exist on SQL table '{}'",
            table.name
        ),
    ))
}

pub(crate) fn require_writable_column(
    table: &BoundTable,
    column_name: &str,
    op: BoundWriteOp,
) -> Result<BoundColumnRef, LixError> {
    let column = require_public_column(table, column_name)?;
    let allowed = match op {
        BoundWriteOp::Insert => column.is_insertable(),
        BoundWriteOp::Update => column.is_updatable(),
        BoundWriteOp::Delete => false,
    };
    if !allowed {
        if table.name == "lix_branch" && column_name == "id" && op == BoundWriteOp::Update {
            return Err(super::error::unsupported(
                "UPDATE lix_branch cannot change immutable column 'id'",
            ));
        }
        return Err(super::error::unsupported(format!(
            "column '{column_name}' is not writable on SQL table '{}'",
            table.name
        )));
    }
    Ok(BoundColumnRef {
        table: table.name.clone(),
        column_id: column.id,
        name: column.name.clone(),
    })
}

pub(crate) fn bind_public_column_ref(
    table: &BoundTable,
    column_name: &str,
) -> Result<BoundColumnRef, LixError> {
    let column = require_public_column(table, column_name)?;
    Ok(BoundColumnRef {
        table: table.name.clone(),
        column_id: column.id,
        name: column.name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement, TableFactor};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use serde_json::json;

    use super::*;
    use crate::sql2::catalog::PublicSurfaceKind;

    #[test]
    fn rejects_qualified_table_name_even_when_leaf_exists() {
        let catalog = catalog();
        let error = bind_public_table(&catalog, &table_name("SELECT * FROM foo.lix_state"))
            .expect_err("qualified table should be rejected");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    }

    #[test]
    fn rejects_unknown_table_name() {
        let catalog = catalog();
        let error = bind_public_table(&catalog, &table_name("SELECT * FROM missing"))
            .expect_err("unknown table should be rejected");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("unknown SQL table 'missing'"));
    }

    #[test]
    fn base_entity_table_does_not_expose_branch_column() {
        let catalog = catalog();
        let table = bind_public_table(&catalog, &table_name("SELECT * FROM test_state_schema"))
            .expect("base entity table should bind");

        assert!(matches!(
            table.surface.kind,
            PublicSurfaceKind::EntityBase { .. }
        ));
        assert!(require_public_column(&table, "name").is_ok());
        let error = require_public_column(&table, "lixcol_branch_id")
            .expect_err("base entity surface should not expose branch column");
        assert!(error.message.contains("does not exist"));
    }

    #[test]
    fn by_branch_entity_exposes_lixcol_branch_id_without_branch_id_alias() {
        let catalog = catalog();
        let table = bind_public_table(
            &catalog,
            &table_name("SELECT * FROM test_state_schema_by_branch"),
        )
        .expect("by-branch entity table should bind");

        assert!(matches!(
            table.surface.kind,
            PublicSurfaceKind::EntityByBranch { .. }
        ));
        assert!(require_public_column(&table, "lixcol_branch_id").is_ok());
        let error = require_public_column(&table, "branch_id")
            .expect_err("by-branch entity surface should not alias branch_id");
        assert!(error.message.contains("does not exist"));
    }

    #[test]
    fn quoted_table_names_are_case_sensitive() {
        let catalog = catalog();

        bind_public_table(&catalog, &table_name("SELECT * FROM \"lix_file\""))
            .expect("quoted exact case should bind");
        let error = bind_public_table(&catalog, &table_name("SELECT * FROM \"LIX_FILE\""))
            .expect_err("quoted mixed case should not be folded");

        assert!(error.message.contains("unknown SQL table 'LIX_FILE'"));
    }

    #[test]
    fn hidden_columns_cannot_bind_as_public_columns() {
        let catalog = catalog();
        let table = bind_public_table(&catalog, &table_name("SELECT * FROM lix_file"))
            .expect("lix_file should bind");

        let error = require_public_column(&table, "lixcol_schema_key")
            .expect_err("hidden column should not bind");
        assert!(error.message.contains("not part of public SQL surface"));
    }

    #[test]
    fn catalog_rejects_runtime_schema_in_reserved_namespace_before_surface_collision() {
        let error = PublicCatalog::from_visible_schemas(&[json!({
            "x-lix-key": "lix_file",
            "properties": {
                "id": { "type": "string" }
            }
        })])
        .expect_err("the complete lix_* runtime namespace should be rejected");

        assert_eq!(error.code, LixError::CODE_RESERVED_SCHEMA_NAMESPACE);
        assert!(error.message.contains("lix_file"));
    }

    #[test]
    fn catalog_uses_validated_entity_surface_derivation() {
        let catalog = PublicCatalog::from_visible_schemas(&[json!({
            "x-lix-key": "bad_entity",
            "properties": {
                "value": { "type": "null" }
            }
        })])
        .expect("invalid entity schemas should match provider behavior and be skipped");

        assert!(catalog.surface("bad_entity").is_none());
    }

    #[test]
    fn fixed_catalog_exposes_only_the_deliberate_lix_sql_contract() {
        let actual = PublicCatalog::fixed_system()
            .surfaces()
            .map(|surface| surface.name.as_str())
            .collect::<Vec<_>>();
        let expected = vec![
            "lix_account",
            "lix_account_by_branch",
            "lix_account_history",
            "lix_branch",
            "lix_branch_descriptor",
            "lix_branch_descriptor_by_branch",
            "lix_branch_descriptor_history",
            "lix_branch_ref",
            "lix_branch_ref_by_branch",
            "lix_branch_ref_history",
            "lix_change",
            "lix_change_author",
            "lix_change_author_by_branch",
            "lix_change_author_history",
            "lix_commit",
            "lix_commit_by_branch",
            "lix_commit_edge",
            "lix_commit_edge_by_branch",
            "lix_directory",
            "lix_directory_by_branch",
            "lix_directory_history",
            "lix_file",
            "lix_file_by_branch",
            "lix_file_history",
            "lix_key_value",
            "lix_key_value_by_branch",
            "lix_key_value_history",
            "lix_label",
            "lix_label_assignment",
            "lix_label_assignment_by_branch",
            "lix_label_assignment_history",
            "lix_label_by_branch",
            "lix_label_history",
            "lix_registered_schema",
            "lix_registered_schema_by_branch",
            "lix_registered_schema_history",
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn fixed_catalog_keeps_registry_surfaces_and_hides_storage_adapters() {
        let catalog = PublicCatalog::fixed_system();
        for surface_name in [
            "lix_key_value",
            "lix_key_value_by_branch",
            "lix_key_value_history",
            "lix_registered_schema",
            "lix_registered_schema_by_branch",
            "lix_registered_schema_history",
        ] {
            assert!(
                catalog.surface(surface_name).is_some(),
                "{surface_name} should remain public"
            );
        }
        for surface_name in [
            "lix_state",
            "lix_state_by_branch",
            "lix_state_history",
            "lix_binary_blob_ref",
            "lix_binary_blob_ref_by_branch",
            "lix_binary_blob_ref_history",
            "lix_directory_descriptor",
            "lix_directory_descriptor_by_branch",
            "lix_directory_descriptor_history",
            "lix_file_descriptor",
            "lix_file_descriptor_by_branch",
            "lix_file_descriptor_history",
        ] {
            assert!(
                catalog.surface(surface_name).is_none(),
                "{surface_name} should not be public"
            );
        }
    }

    #[test]
    fn runtime_schema_namespace_check_matches_unquoted_sql_normalization() {
        for schema_key in [
            "lix",
            "LIX",
            "lix_plugin_note",
            "LIX_PLUGIN_NOTE",
            "LiX_PlUgIn_NoTe",
        ] {
            assert!(
                PublicCatalog::runtime_schema_key_uses_reserved_namespace(schema_key),
                "{schema_key} should normalize into the reserved namespace"
            );
        }
        assert!(!PublicCatalog::runtime_schema_key_uses_reserved_namespace(
            "acme_lix_note"
        ));
    }

    #[test]
    fn dynamic_entity_history_surface_uses_provider_history_column_names() {
        let catalog = catalog();
        let table = bind_public_table(
            &catalog,
            &table_name("SELECT * FROM test_state_schema_history"),
        )
        .expect("entity history surface should bind");

        assert!(matches!(
            table.surface.kind,
            PublicSurfaceKind::EntityHistory { .. }
        ));
        assert!(require_public_column(&table, "lixcol_entity_pk").is_ok());
        assert!(require_public_column(&table, "lixcol_snapshot_content").is_ok());
    }

    #[test]
    fn dynamic_entity_file_id_is_public_and_insert_only() {
        let catalog = catalog();
        let table = bind_public_table(&catalog, &table_name("SELECT * FROM test_state_schema"))
            .expect("entity surface should bind");

        assert!(require_public_column(&table, "lixcol_file_id").is_ok());
        assert!(require_writable_column(&table, "lixcol_file_id", BoundWriteOp::Insert).is_ok());
        let error = require_writable_column(&table, "lixcol_file_id", BoundWriteOp::Update)
            .expect_err("entity file id should remain immutable after insert");
        assert!(error.message.contains("is not writable"));
    }

    fn catalog() -> PublicCatalog {
        PublicCatalog::from_visible_schemas(&[json!({
            "x-lix-key": "test_state_schema",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "lixcol_internal": { "type": "string" }
            }
        })])
        .expect("test catalog")
    }

    fn table_name(sql: &str) -> ObjectName {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let Some(Statement::Query(query)) = statements.pop() else {
            panic!("expected query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            panic!("expected table factor");
        };
        name.clone()
    }
}
