use std::collections::BTreeSet;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, FromTable, ObjectName, Statement as SqlStatement, TableFactor, TableObject,
    TableWithJoins,
};
use serde_json::Value as JsonValue;

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract};
use crate::LixError;

use super::read::BoundRead;
use super::table::{bind_public_table, require_public_column, BoundTable};
use super::write::{BoundWrite, BoundWriteOp};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundStatement {
    Read(BoundRead),
    Write(BoundWrite),
}

pub(crate) fn bind_statement(
    statement: &DataFusionStatement,
    visible_schemas: &[JsonValue],
    _active_version_id: &str,
) -> Result<BoundStatement, LixError> {
    let catalog = PublicCatalog::from_visible_schemas(visible_schemas)?;
    match statement {
        DataFusionStatement::Statement(statement) => bind_sql_statement(statement, &catalog),
        DataFusionStatement::Explain(explain) => bind_statement(
            explain.statement.as_ref(),
            visible_schemas,
            _active_version_id,
        ),
        _ => Err(super::error::unsupported(format!(
            "SQL statement is not supported by Lix SQL: {statement}"
        ))),
    }
}

fn bind_sql_statement(
    statement: &SqlStatement,
    catalog: &PublicCatalog,
) -> Result<BoundStatement, LixError> {
    match statement {
        SqlStatement::Insert(insert) => {
            let TableObject::TableName(name) = &insert.table else {
                return Err(super::error::unsupported("unsupported INSERT target"));
            };
            let table = bind_public_table(catalog, name)?;
            require_write_capability(&table.surface, BoundWriteOp::Insert)?;
            if insert.columns.is_empty() {
                return Err(super::error::unsupported(
                    "INSERT requires an explicit public column list",
                ));
            }
            let mut target_columns = BTreeSet::new();
            for column in &insert.columns {
                let column_name = normalize_identifier(column);
                require_public_column(&table, &column_name)?;
                reject_duplicate_target_column(&mut target_columns, &column_name)?;
            }
            Err(bound_write_not_wired_error())
        }
        SqlStatement::Update(update) => {
            let table = bind_table_with_joins(catalog, &update.table)?;
            require_write_capability(&table.surface, BoundWriteOp::Update)?;
            let mut target_columns = BTreeSet::new();
            for assignment in &update.assignments {
                let column_names = bind_assignment_target(&table, &assignment.target)?;
                for column_name in column_names {
                    reject_duplicate_target_column(&mut target_columns, &column_name)?;
                }
            }
            Err(bound_write_not_wired_error())
        }
        SqlStatement::Delete(delete) => {
            let table = bind_delete_target(catalog, &delete.from)?;
            require_write_capability(&table.surface, BoundWriteOp::Delete)?;
            Err(bound_write_not_wired_error())
        }
        SqlStatement::Explain { statement, .. } => bind_sql_statement(statement.as_ref(), catalog),
        _ => Err(super::error::unsupported(
            "sql2 bound statement pipeline is not wired yet",
        )),
    }
}

fn bind_table_with_joins(
    catalog: &PublicCatalog,
    table: &TableWithJoins,
) -> Result<BoundTable, LixError> {
    if !table.joins.is_empty() {
        return Err(super::error::unsupported(
            "joined DML targets are not supported",
        ));
    }
    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(super::error::unsupported("unsupported DML target"));
    };
    bind_public_table(catalog, name)
}

fn bind_delete_target(catalog: &PublicCatalog, from: &FromTable) -> Result<BoundTable, LixError> {
    let tables = match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(super::error::unsupported(
            "DELETE requires exactly one target table",
        ));
    }
    bind_table_with_joins(catalog, &tables[0])
}

fn bind_assignment_target(
    table: &BoundTable,
    target: &AssignmentTarget,
) -> Result<Vec<String>, LixError> {
    match target {
        AssignmentTarget::ColumnName(name) => {
            let column_name = bind_exact_column_name(name)?;
            require_public_column(table, &column_name)?;
            Ok(vec![column_name])
        }
        AssignmentTarget::Tuple(_) => Err(super::error::unsupported(
            "tuple UPDATE assignments are not supported",
        )),
    }
}

fn bind_exact_column_name(name: &ObjectName) -> Result<String, LixError> {
    if name.0.len() != 1 {
        return Err(super::error::unsupported(
            "qualified SQL column names are not supported",
        ));
    }
    name.0
        .first()
        .and_then(|part| part.as_ident())
        .map(normalize_identifier)
        .ok_or_else(|| super::error::unsupported("unsupported SQL column name"))
}

fn normalize_identifier(ident: &datafusion::sql::sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn reject_duplicate_target_column(
    target_columns: &mut BTreeSet<String>,
    column_name: &str,
) -> Result<(), LixError> {
    if target_columns.insert(column_name.to_string()) {
        Ok(())
    } else {
        Err(super::error::unsupported(format!(
            "duplicate write target column '{column_name}'"
        )))
    }
}

fn require_write_capability(
    surface: &PublicSurfaceContract,
    op: BoundWriteOp,
) -> Result<(), LixError> {
    let allowed = match op {
        BoundWriteOp::Insert => surface.capabilities.insert,
        BoundWriteOp::Update => surface.capabilities.update,
        BoundWriteOp::Delete => surface.capabilities.delete,
    };
    if allowed {
        Ok(())
    } else {
        Err(LixError::new(
            LixError::CODE_READ_ONLY,
            format!("DML cannot write read-only SQL table '{}'", surface.name),
        ))
    }
}

fn bound_write_not_wired_error() -> LixError {
    super::error::unsupported("sql2 bound write body pipeline is not wired yet")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::Statement as DataFusionStatement;

    #[test]
    fn bind_statement_uses_exact_table_binding_for_write_targets() {
        let statement = parse_statement("INSERT INTO foo.lix_file (id) VALUES ('file1')");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("qualified write target should be rejected by the binder");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("qualified SQL table names"));
    }

    #[test]
    fn bind_statement_rejects_hidden_insert_columns() {
        let statement = parse_statement(
            "INSERT INTO lix_file (id, path, directory_id, name, data, lixcol_schema_key) VALUES ('file1', '/a', null, 'a', null, 'schema')",
        );
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("hidden columns should not bind through statement binder");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("not part of public SQL surface"));
    }

    #[test]
    fn bind_statement_rejects_implicit_insert_columns() {
        let statement = parse_statement("INSERT INTO lix_file VALUES ('file1')");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("implicit insert column list should fail closed");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("INSERT requires an explicit public column list"));
    }

    #[test]
    fn bind_statement_rejects_duplicate_insert_columns() {
        let statement = parse_statement("INSERT INTO lix_file (id, id) VALUES ('file1', 'file2')");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("duplicate insert columns should be rejected");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("duplicate write target column 'id'"));
    }

    #[test]
    fn bind_statement_rejects_duplicate_update_columns() {
        let statement = parse_statement("UPDATE lix_file SET name = 'a', name = 'b'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("duplicate update columns should be rejected");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("duplicate write target column 'name'"));
    }

    #[test]
    fn bind_statement_rejects_read_only_history_writes() {
        let statement = parse_statement("DELETE FROM lix_file_history");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("history surfaces should be read-only");

        assert_eq!(error.code, LixError::CODE_READ_ONLY);
    }

    #[test]
    fn bind_statement_fails_closed_after_validating_write_target() {
        let statement = parse_statement(
            "UPDATE test_state_schema_by_version SET name = 'next' WHERE lixcol_version_id = 'version2'",
        );
        let error = bind_statement(
            &statement,
            &[serde_json::json!({
                "x-lix-key": "test_state_schema",
                "properties": {
                    "id": { "type": "string" },
                    "name": { "type": "string" }
                }
            })],
            "version1",
        )
        .expect_err("write body binding should fail closed until Phase 3");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("sql2 bound write body pipeline is not wired yet"));
    }

    #[test]
    fn bind_statement_fails_closed_instead_of_dropping_predicates() {
        let statement = parse_statement("DELETE FROM lix_file WHERE lixcol_schema_key = 'schema'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("unbound predicates should not produce a BoundWrite");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("sql2 bound write body pipeline is not wired yet"));
    }

    fn parse_statement(sql: &str) -> DataFusionStatement {
        crate::sql2::parse_statement(sql).expect("parse SQL")
    }
}
