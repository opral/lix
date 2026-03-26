use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Statement, TableFactor, TableObject, VisitMut, VisitorMut,
};

use crate::sql::ast::walk::object_name_matches;

pub(crate) const LEGACY_INTERNAL_STATE_VTABLE: &str = "lix_internal_state_vtable";
pub(crate) const STATE_BY_VERSION_SURFACE: &str = "lix_state_by_version";

pub(crate) fn normalize_legacy_internal_state_vtable_statements(
    statements: &mut [Statement],
) -> bool {
    let mut changed = false;
    for statement in statements {
        changed |= normalize_legacy_internal_state_vtable_statement(statement);
    }
    changed
}

pub(crate) fn normalize_legacy_internal_state_vtable_statement(statement: &mut Statement) -> bool {
    let mut normalizer = LegacyInternalStateVtableNormalizer { changed: false };
    let _ = statement.visit(&mut normalizer);
    normalizer.changed
}

struct LegacyInternalStateVtableNormalizer {
    changed: bool,
}

impl VisitorMut for LegacyInternalStateVtableNormalizer {
    type Break = ();

    fn post_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, .. } = table_factor {
            self.changed |= normalize_relation_name(name);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Insert(insert) => {
                self.changed |= normalize_table_object(&mut insert.table);
            }
            Statement::Update(update) => {
                if let TableFactor::Table { name, .. } = &mut update.table.relation {
                    self.changed |= normalize_relation_name(name);
                }
            }
            Statement::Delete(delete) => {
                let tables = match &mut delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
                };
                for table in tables {
                    if let TableFactor::Table { name, .. } = &mut table.relation {
                        self.changed |= normalize_relation_name(name);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn normalize_table_object(table: &mut TableObject) -> bool {
    match table {
        TableObject::TableName(name) => normalize_relation_name(name),
        _ => false,
    }
}

fn normalize_relation_name(name: &mut ObjectName) -> bool {
    if !object_name_matches(name, LEGACY_INTERNAL_STATE_VTABLE) {
        return false;
    }

    *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
        STATE_BY_VERSION_SURFACE,
    ))]);
    true
}
