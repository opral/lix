use super::super::super::ast::nodes::Statement;
use crate::sql::{object_name_matches, visit_query_selects, visit_table_factors_in_select};
use sqlparser::ast::{FromTable, ObjectName, TableFactor, TableObject, TableWithJoins};

const STATE_HISTORY_VIEW: &str = "lix_state_history";
const FILE_HISTORY_VIEW: &str = "lix_file_history";
const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

pub(crate) fn statement_targets_state_history(statement: &Statement) -> bool {
    statement_targets_relation(statement, STATE_HISTORY_VIEW)
}

pub(crate) fn statement_targets_file_history(statement: &Statement) -> bool {
    statement_targets_relation(statement, FILE_HISTORY_VIEW)
}

pub(crate) fn statement_targets_directory_history(statement: &Statement) -> bool {
    statement_targets_relation(statement, DIRECTORY_HISTORY_VIEW)
}

fn statement_targets_relation(statement: &Statement, relation: &str) -> bool {
    match statement {
        Statement::Insert(insert) => table_object_targets_relation(&insert.table, relation),
        Statement::Update(update) => table_with_joins_targets_relation(&update.table, relation),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            tables
                .iter()
                .any(|table| table_with_joins_targets_relation(table, relation))
        }
        Statement::Query(query) => query_targets_relation(query, relation),
        Statement::Explain {
            statement: inner, ..
        } => statement_targets_relation(inner, relation),
        _ => false,
    }
}

fn table_object_targets_relation(table: &TableObject, relation: &str) -> bool {
    match table {
        TableObject::TableName(name) => object_name_targets_relation(name, relation),
        _ => false,
    }
}

fn table_with_joins_targets_relation(table: &TableWithJoins, relation: &str) -> bool {
    matches!(
        &table.relation,
        TableFactor::Table { name, .. } if object_name_targets_relation(name, relation)
    )
}

fn query_targets_relation(query: &sqlparser::ast::Query, relation: &str) -> bool {
    let mut found = false;
    let visit = visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation_factor| {
            if let TableFactor::Table { name, .. } = relation_factor {
                if object_name_targets_relation(name, relation) {
                    found = true;
                }
            }
            Ok(())
        })
    });
    visit.is_ok() && found
}

fn object_name_targets_relation(name: &ObjectName, relation: &str) -> bool {
    object_name_matches(name, relation)
}
