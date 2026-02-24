use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Ident, Insert, ObjectName,
    ObjectNamePart, TableFactor, TableObject, TableWithJoins, Update,
};

use crate::engine::sql::planning::rewrite_engine::object_name_matches;
use crate::LixError;

const LIX_STATE_BY_VERSION_VIEW_NAME: &str = "lix_state_by_version";
const VTABLE_NAME: &str = "lix_internal_state_vtable";

pub fn rewrite_insert(mut insert: Insert) -> Result<Option<Insert>, LixError> {
    if !table_object_is_lix_state_by_version(&insert.table) {
        return Ok(None);
    }
    if insert.on.is_some() {
        return Err(LixError {
            message: "lix_state_by_version insert does not support ON CONFLICT".to_string(),
        });
    }
    if insert.columns.is_empty() {
        return Err(LixError {
            message: "lix_state_by_version insert requires explicit columns".to_string(),
        });
    }
    if insert.columns.iter().any(|column| {
        column
            .value
            .eq_ignore_ascii_case("inherited_from_version_id")
    }) {
        return Err(LixError {
            message:
                "lix_state_by_version insert cannot set inherited_from_version_id; it is computed"
                    .to_string(),
        });
    }
    if !insert
        .columns
        .iter()
        .any(|column| column.value.eq_ignore_ascii_case("version_id"))
    {
        return Err(LixError {
            message: "lix_state_by_version insert requires version_id".to_string(),
        });
    }

    insert.table = TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(
        Ident::new(VTABLE_NAME),
    )]));
    Ok(Some(insert))
}

pub fn rewrite_update(mut update: Update) -> Result<Option<Update>, LixError> {
    if !table_with_joins_is_lix_state_by_version(&update.table) {
        return Ok(None);
    }
    if update.assignments.iter().any(|assignment| {
        assignment_target_is_column(&assignment.target, "inherited_from_version_id")
    }) {
        return Err(LixError {
            message:
                "lix_state_by_version update cannot set inherited_from_version_id; it is computed"
                    .to_string(),
        });
    }

    let Some(existing_selection) = update.selection.take() else {
        return Err(LixError {
            message: "lix_state_by_version update requires a version_id predicate".to_string(),
        });
    };
    if !contains_column_reference(&existing_selection, "version_id") {
        return Err(LixError {
            message: "lix_state_by_version update requires a version_id predicate".to_string(),
        });
    }

    replace_table_with_vtable(&mut update.table)?;
    update.selection = Some(Expr::BinaryOp {
        left: Box::new(existing_selection),
        op: BinaryOperator::And,
        right: Box::new(snapshot_not_null_predicate_expr()),
    });
    Ok(Some(update))
}

pub fn rewrite_delete(mut delete: Delete) -> Result<Option<Delete>, LixError> {
    if !delete_from_is_lix_state_by_version(&delete) {
        return Ok(None);
    }

    let Some(existing_selection) = delete.selection.take() else {
        return Err(LixError {
            message: "lix_state_by_version delete requires a version_id predicate".to_string(),
        });
    };
    if !contains_column_reference(&existing_selection, "version_id") {
        return Err(LixError {
            message: "lix_state_by_version delete requires a version_id predicate".to_string(),
        });
    }

    replace_delete_from_vtable(&mut delete)?;
    delete.selection = Some(Expr::BinaryOp {
        left: Box::new(existing_selection),
        op: BinaryOperator::And,
        right: Box::new(snapshot_not_null_predicate_expr()),
    });
    Ok(Some(delete))
}

fn snapshot_not_null_predicate_expr() -> Expr {
    Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new("snapshot_content"))))
}

fn table_object_is_lix_state_by_version(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, LIX_STATE_BY_VERSION_VIEW_NAME),
        _ => false,
    }
}

fn table_with_joins_is_lix_state_by_version(table: &TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_STATE_BY_VERSION_VIEW_NAME)
        )
}

fn delete_from_is_lix_state_by_version(delete: &Delete) -> bool {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            tables.len() == 1 && table_with_joins_is_lix_state_by_version(&tables[0])
        }
    }
}

fn replace_table_with_vtable(table: &mut TableWithJoins) -> Result<(), LixError> {
    if !table.joins.is_empty() {
        return Err(LixError {
            message: "lix_state_by_version mutation does not support JOIN targets".to_string(),
        });
    }
    match &mut table.relation {
        TableFactor::Table { name, .. } => {
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(VTABLE_NAME))]);
            Ok(())
        }
        _ => Err(LixError {
            message: "lix_state_by_version mutation requires table target".to_string(),
        }),
    }
}

fn replace_delete_from_vtable(delete: &mut Delete) -> Result<(), LixError> {
    let tables = match &mut delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let Some(table) = tables.first_mut() else {
        return Err(LixError {
            message: "lix_state_by_version delete requires table target".to_string(),
        });
    };
    replace_table_with_vtable(table)
}

fn assignment_target_is_column(target: &AssignmentTarget, name: &str) -> bool {
    match target {
        AssignmentTarget::ColumnName(object_name) => object_name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.eq_ignore_ascii_case(name))
            .unwrap_or(false),
        AssignmentTarget::Tuple(_) => false,
    }
}

fn contains_column_reference(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(column))
            .unwrap_or(false),
        Expr::BinaryOp { left, right, .. } => {
            contains_column_reference(left, column) || contains_column_reference(right, column)
        }
        Expr::UnaryOp { expr, .. } => contains_column_reference(expr, column),
        Expr::Nested(inner) => contains_column_reference(inner, column),
        Expr::InList { expr, list, .. } => {
            contains_column_reference(expr, column)
                || list
                    .iter()
                    .any(|item| contains_column_reference(item, column))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_column_reference(expr, column)
                || contains_column_reference(low, column)
                || contains_column_reference(high, column)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            contains_column_reference(expr, column) || contains_column_reference(pattern, column)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_column_reference(inner, column),
        Expr::Cast { expr, .. } => contains_column_reference(expr, column),
        Expr::Function(_) => false,
        _ => false,
    }
}
