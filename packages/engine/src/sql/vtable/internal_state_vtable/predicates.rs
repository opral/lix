use super::super::super::ast::nodes::Statement;
use super::super::super::ast::walk::{
    object_name_matches, visit_query_selects, visit_table_factors_in_select,
};
use sqlparser::ast::{
    BinaryOperator, Delete, Expr, FromTable, ObjectName, ObjectNamePart, TableFactor, TableObject,
    TableWithJoins, Value, ValueWithSpan,
};

const INTERNAL_STATE_VTABLE_NAME: &str = "lix_internal_state_vtable";
const INTERNAL_STATE_UNTRACKED_NAME: &str = "lix_internal_state_untracked";
const INTERNAL_STATE_MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub(crate) fn statement_targets_internal_state_vtable(statement: &Statement) -> bool {
    match statement {
        Statement::Insert(insert) => table_object_targets_internal_state_vtable(&insert.table),
        Statement::Update(update) => table_with_joins_targets_internal_state_vtable(&update.table),
        Statement::Delete(delete) => delete_targets_internal_state_vtable(delete),
        Statement::Query(query) => query_targets_internal_state_vtable(query),
        Statement::Explain {
            statement: inner, ..
        } => statement_targets_internal_state_vtable(inner),
        _ => false,
    }
}

pub(crate) fn statement_has_schema_key_predicate(statement: &Statement) -> bool {
    match statement {
        Statement::Update(update) => update
            .selection
            .as_ref()
            .is_some_and(expr_has_schema_key_predicate),
        Statement::Delete(delete) => delete
            .selection
            .as_ref()
            .is_some_and(expr_has_schema_key_predicate),
        Statement::Query(query) => query_has_schema_key_predicate(query),
        Statement::Explain {
            statement: inner, ..
        } => statement_has_schema_key_predicate(inner),
        _ => false,
    }
}

pub(crate) fn schema_key_is_valid(schema_key: &str) -> bool {
    !schema_key.trim().is_empty()
        && !schema_key.contains(char::is_whitespace)
        && !schema_key.contains('\'')
}

fn table_object_targets_internal_state_vtable(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_targets_internal_state_vtable(name),
        _ => false,
    }
}

fn table_with_joins_targets_internal_state_vtable(table: &TableWithJoins) -> bool {
    matches!(
        &table.relation,
        TableFactor::Table { name, .. } if object_name_targets_internal_state_vtable(name)
    )
}

fn delete_targets_internal_state_vtable(delete: &Delete) -> bool {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    tables.len() == 1 && table_with_joins_targets_internal_state_vtable(&tables[0])
}

fn query_targets_internal_state_vtable(query: &sqlparser::ast::Query) -> bool {
    let mut found = false;
    let visit = visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            if let TableFactor::Table { name, .. } = relation {
                if object_name_targets_internal_state_vtable(name) {
                    found = true;
                }
            }
            Ok(())
        })
    });
    visit.is_ok() && found
}

fn object_name_targets_internal_state_vtable(name: &ObjectName) -> bool {
    object_name_matches(name, INTERNAL_STATE_VTABLE_NAME)
        || object_name_matches(name, INTERNAL_STATE_UNTRACKED_NAME)
        || object_name_has_internal_state_materialized_prefix(name)
}

fn object_name_has_internal_state_materialized_prefix(name: &ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| {
            ident
                .value
                .to_ascii_lowercase()
                .starts_with(INTERNAL_STATE_MATERIALIZED_PREFIX)
        })
        .unwrap_or(false)
}

fn query_has_schema_key_predicate(query: &sqlparser::ast::Query) -> bool {
    let mut found = false;
    let visit = visit_query_selects(query, &mut |select| {
        if select
            .selection
            .as_ref()
            .is_some_and(expr_has_schema_key_predicate)
        {
            found = true;
        }
        Ok(())
    });
    visit.is_ok() && found
}

fn expr_has_schema_key_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            (expr_is_schema_key_column(left) && expr_is_string_literal(right))
                || (expr_is_schema_key_column(right) && expr_is_string_literal(left))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        }
        | Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => expr_has_schema_key_predicate(left) || expr_has_schema_key_predicate(right),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            expr_is_schema_key_column(expr)
                && !list.is_empty()
                && list.iter().all(expr_is_string_literal)
        }
        Expr::Nested(inner) => expr_has_schema_key_predicate(inner),
        _ => false,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("schema_key"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("schema_key"))
            .unwrap_or(false),
        _ => false,
    }
}

fn expr_is_string_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(_),
            ..
        })
    )
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::{
        schema_key_is_valid, statement_has_schema_key_predicate,
        statement_targets_internal_state_vtable,
    };

    #[test]
    fn detects_internal_state_vtable_target() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT * FROM lix_internal_state_vtable WHERE schema_key = 'x'",
        )
        .expect("parse SQL");
        assert!(statement_targets_internal_state_vtable(&statements[0]));
    }

    #[test]
    fn detects_schema_key_predicate() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{}' WHERE schema_key IN ('a')",
        )
        .expect("parse SQL");
        assert!(statement_has_schema_key_predicate(&statements[0]));
    }

    #[test]
    fn validates_schema_key_tokens() {
        assert!(schema_key_is_valid("lix_file_descriptor"));
        assert!(!schema_key_is_valid(""));
        assert!(!schema_key_is_valid(" "));
        assert!(!schema_key_is_valid("bad key"));
        assert!(!schema_key_is_valid("bad'key"));
    }
}
