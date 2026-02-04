use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ConflictTarget, Delete, DoUpdate, Expr, Ident,
    ObjectName, ObjectNamePart, OnConflict, OnConflictAction, OnInsert, Query, Select, SetExpr,
    Statement, TableAlias, TableFactor, TableObject, TableWithJoins, Update, Value, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::LixError;

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

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

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let schema_key = match extract_schema_key_from_query(&query) {
        Some(key) => key,
        None => return Ok(None),
    };

    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(
        *query.body,
        &schema_key,
        &mut changed,
    )?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_set_expr(expr: SetExpr, schema_key: &str, changed: &mut bool) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(&mut select, schema_key, changed)?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            query.body = Box::new(rewrite_set_expr(*query.body, schema_key, changed)?);
            SetExpr::Query(Box::new(query))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => SetExpr::SetOperation {
            op,
            set_quantifier,
            left: Box::new(rewrite_set_expr(*left, schema_key, changed)?),
            right: Box::new(rewrite_set_expr(*right, schema_key, changed)?),
        },
        other => other,
    })
}

fn rewrite_select(select: &mut Select, schema_key: &str, changed: &mut bool) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, schema_key, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    schema_key: &str,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(&mut table.relation, schema_key, changed)?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, schema_key, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    schema_key: &str,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } if object_name_matches(name, VTABLE_NAME) => {
            let derived_query = build_untracked_union_query(schema_key)?;
            let derived_alias = alias.clone().or_else(|| Some(default_vtable_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        _ => {}
    }
    Ok(())
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
                target: AssignmentTarget::ColumnName(ObjectName(vec![
                    ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                ])),
                value: Expr::CompoundIdentifier(vec![
                    Ident::new("excluded"),
                    Ident::new("snapshot_content"),
                ]),
            }],
            selection: None,
        }),
    })
}

fn build_untracked_union_query(schema_key: &str) -> Result<Query, LixError> {
    let dialect = GenericDialect {};
    let schema_literal = escape_string_literal(schema_key);
    let materialized_table = format!("{MATERIALIZED_PREFIX}{schema_key}");
    let materialized_ident = quote_ident(&materialized_table);

    let sql = format!(
        "SELECT entity_id, schema_key, file_id, version_id, snapshot_content, untracked \
         FROM (\
             SELECT entity_id, schema_key, file_id, version_id, snapshot_content, untracked, \
                    ROW_NUMBER() OVER (PARTITION BY entity_id, file_id, version_id ORDER BY priority) AS rn \
             FROM (\
                 SELECT entity_id, schema_key, file_id, version_id, snapshot_content, \
                        1 AS untracked, 1 AS priority \
                 FROM {untracked} \
                 WHERE schema_key = '{schema_literal}' \
                 UNION ALL \
                 SELECT entity_id, schema_key, file_id, version_id, snapshot_content, \
                        0 AS untracked, 2 AS priority \
                 FROM {materialized} \
             ) AS lix_state_union\
         ) AS lix_state_ranked \
         WHERE rn = 1",
        untracked = UNTRACKED_TABLE,
        materialized = materialized_ident
    );

    let mut statements = Parser::parse_sql(&dialect, &sql).map_err(|err| LixError {
        message: err.to_string(),
    })?;

    if statements.len() != 1 {
        return Err(LixError {
            message: "expected single derived query statement".to_string(),
        });
    }

    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "derived query did not parse as SELECT".to_string(),
        }),
    }
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

fn extract_schema_key_from_query(query: &Query) -> Option<String> {
    extract_schema_key_from_set_expr(&query.body)
}

fn extract_schema_key_from_set_expr(expr: &SetExpr) -> Option<String> {
    match expr {
        SetExpr::Select(select) => extract_schema_key_from_select(select),
        SetExpr::Query(query) => extract_schema_key_from_set_expr(&query.body),
        SetExpr::SetOperation { left, right, .. } => {
            extract_schema_key_from_set_expr(left).or_else(|| extract_schema_key_from_set_expr(right))
        }
        _ => None,
    }
}

fn extract_schema_key_from_select(select: &Select) -> Option<String> {
    select
        .selection
        .as_ref()
        .and_then(extract_schema_key_from_expr)
}

fn extract_schema_key_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_schema_key_column(left) {
                return string_literal_value(right);
            }
            if expr_is_schema_key_column(right) {
                return string_literal_value(left);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => extract_schema_key_from_expr(left).or_else(|| extract_schema_key_from_expr(right)),
        Expr::Nested(inner) => extract_schema_key_from_expr(inner),
        _ => None,
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

fn string_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Some(value.clone()),
        _ => None,
    }
}

fn default_vtable_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(VTABLE_NAME),
        columns: Vec::new(),
    }
}

fn escape_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
