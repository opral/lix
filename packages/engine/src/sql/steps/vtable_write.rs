use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ConflictTarget, Delete, DoUpdate, Expr, Ident,
    ObjectName, ObjectNamePart, OnConflict, OnConflictAction, OnInsert, SetExpr, Statement,
    TableFactor, TableObject, TableWithJoins, Update, Value, ValueWithSpan,
};

use crate::LixError;

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";

pub fn rewrite_insert(insert: sqlparser::ast::Insert) -> Result<Option<Statement>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    if insert.on.is_some() {
        return Ok(None);
    }

    if insert.columns.is_empty() {
        return Ok(None);
    }

    let untracked_index = find_column_index(&insert.columns, "untracked");
    let untracked_index = match untracked_index {
        Some(index) => index,
        None => return Ok(None),
    };

    let source = match &insert.source {
        Some(source) => source,
        None => return Ok(None),
    };

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        _ => return Ok(None),
    };

    if !values.rows.iter().all(|row| {
        row.get(untracked_index)
            .map(is_untracked_true_literal)
            .unwrap_or(false)
    }) {
        return Ok(None);
    }

    let mut new_insert = insert.clone();
    new_insert.table = TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(
        Ident::new(UNTRACKED_TABLE),
    )]));

    let mut new_columns = insert.columns.clone();
    new_columns.remove(untracked_index);
    new_insert.columns = new_columns;

    let mut new_values = values.clone();
    for row in &mut new_values.rows {
        if row.len() > untracked_index {
            row.remove(untracked_index);
        }
    }

    let mut new_query = (*source.clone()).clone();
    new_query.body = Box::new(SetExpr::Values(new_values));
    new_insert.source = Some(Box::new(new_query));

    new_insert.on = Some(build_untracked_on_conflict());

    Ok(Some(Statement::Insert(new_insert)))
}

pub fn rewrite_update(update: Update) -> Result<Option<Statement>, LixError> {
    if !table_with_joins_is_vtable(&update.table) {
        return Ok(None);
    }

    let selection = match update.selection.as_ref() {
        Some(selection) if can_strip_untracked_predicate(selection) => selection,
        _ => return Ok(None),
    };

    let mut new_update = update.clone();
    replace_table_with_untracked(&mut new_update.table);
    new_update.assignments = update
        .assignments
        .into_iter()
        .filter(|assignment| !assignment_target_is_untracked(&assignment.target))
        .collect();
    new_update.selection = try_strip_untracked_predicate(selection).unwrap_or(None);

    Ok(Some(Statement::Update(new_update)))
}

pub fn rewrite_delete(delete: Delete) -> Result<Option<Statement>, LixError> {
    if !delete_from_is_vtable(&delete) {
        return Ok(None);
    }

    let selection = match delete.selection.as_ref() {
        Some(selection) if can_strip_untracked_predicate(selection) => selection,
        _ => return Ok(None),
    };

    let mut new_delete = delete.clone();
    replace_delete_from_untracked(&mut new_delete);
    new_delete.selection = try_strip_untracked_predicate(selection).unwrap_or(None);

    Ok(Some(Statement::Delete(new_delete)))
}

fn build_untracked_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("schema_key"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![Assignment {
                target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                    Ident::new("snapshot_content"),
                )])),
                value: Expr::CompoundIdentifier(vec![
                    Ident::new("excluded"),
                    Ident::new("snapshot_content"),
                ]),
            }],
            selection: None,
        }),
    })
}

fn table_object_is_vtable(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, VTABLE_NAME),
        _ => false,
    }
}

fn table_with_joins_is_vtable(table: &TableWithJoins) -> bool {
    matches!(
        &table.relation,
        TableFactor::Table { name, .. } if object_name_matches(name, VTABLE_NAME)
    )
}

fn delete_from_is_vtable(delete: &Delete) -> bool {
    match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return false;
            }
            table_with_joins_is_vtable(&tables[0])
        }
    }
}

fn replace_table_with_untracked(table: &mut TableWithJoins) {
    if let TableFactor::Table { name, .. } = &mut table.relation {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            UNTRACKED_TABLE,
        ))]);
    }
}

fn replace_delete_from_untracked(delete: &mut Delete) {
    let tables = match &mut delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };

    if let Some(table) = tables.first_mut() {
        replace_table_with_untracked(table);
    }
}

fn find_column_index(columns: &[Ident], column: &str) -> Option<usize> {
    columns
        .iter()
        .position(|ident| ident.value.eq_ignore_ascii_case(column))
}

fn assignment_target_is_untracked(target: &AssignmentTarget) -> bool {
    match target {
        AssignmentTarget::ColumnName(name) => object_name_matches(name, "untracked"),
        AssignmentTarget::Tuple(columns) => columns
            .iter()
            .any(|name| object_name_matches(name, "untracked")),
    }
}

fn contains_untracked_true(expr: &Expr) -> bool {
    if is_untracked_equals_true(expr) {
        return true;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                contains_untracked_true(left) || contains_untracked_true(right)
            }
            _ => false,
        },
        Expr::Nested(inner) => contains_untracked_true(inner),
        _ => false,
    }
}

fn can_strip_untracked_predicate(expr: &Expr) -> bool {
    contains_untracked_true(expr) && try_strip_untracked_predicate(expr).is_some()
}

fn try_strip_untracked_predicate(expr: &Expr) -> Option<Option<Expr>> {
    if is_untracked_equals_true(expr) {
        return Some(None);
    }

    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let left = try_strip_untracked_predicate(left)?;
            let right = try_strip_untracked_predicate(right)?;

            match (left, right) {
                (None, None) => Some(None),
                (Some(expr), None) | (None, Some(expr)) => Some(Some(expr)),
                (Some(left), Some(right)) => Some(Some(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                })),
            }
        }
        Expr::Nested(inner) => {
            let stripped = try_strip_untracked_predicate(inner)?;
            Some(stripped.map(|expr| Expr::Nested(Box::new(expr))))
        }
        _ => Some(Some(expr.clone())),
    }
}

fn is_untracked_equals_true(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            (expr_is_untracked_column(left) && is_untracked_true_literal(right))
                || (expr_is_untracked_column(right) && is_untracked_true_literal(left))
        }
        _ => false,
    }
}

fn expr_is_untracked_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("untracked"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("untracked"))
            .unwrap_or(false),
        _ => false,
    }
}

fn is_untracked_true_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(value, _),
            ..
        }) => value == "1",
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(value),
            ..
        }) => *value,
        _ => false,
    }
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}
