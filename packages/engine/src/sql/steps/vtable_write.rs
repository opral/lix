use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ConflictTarget, Delete, DoUpdate, Expr, Ident,
    ObjectName, ObjectNamePart, OnConflict, OnConflictAction, OnInsert, Query, SetExpr, Statement,
    TableFactor, TableObject, TableWithJoins, Update, Value, ValueWithSpan, Values,
};

use crate::functions::timestamp::timestamp;
use crate::functions::uuid_v7::uuid_v7;
use crate::sql::SchemaRegistration;
use crate::LixError;

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub struct VtableWriteRewrite {
    pub statements: Vec<Statement>,
    pub registrations: Vec<SchemaRegistration>,
}

pub fn rewrite_insert(
    insert: sqlparser::ast::Insert,
) -> Result<Option<VtableWriteRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    if insert.on.is_some() {
        return Err(LixError {
            message: "vtable insert does not support ON CONFLICT".to_string(),
        });
    }

    if insert.columns.is_empty() {
        return Err(LixError {
            message: "vtable insert requires explicit columns".to_string(),
        });
    }

    let source = match &insert.source {
        Some(source) => source,
        None => {
            return Err(LixError {
                message: "vtable insert requires a VALUES source".to_string(),
            })
        }
    };

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        _ => {
            return Err(LixError {
                message: "vtable insert requires VALUES rows".to_string(),
            })
        }
    };

    if values.rows.is_empty() {
        return Ok(None);
    }

    let untracked_index = find_column_index(&insert.columns, "untracked");
    let mut tracked_rows = Vec::new();
    let mut untracked_rows = Vec::new();

    for row in &values.rows {
        let untracked_value = untracked_index.and_then(|idx| row.get(idx)).cloned();

        let untracked = match untracked_value {
            None => false,
            Some(expr) if is_untracked_true_literal(&expr) => true,
            Some(expr) if is_untracked_false_literal(&expr) => false,
            Some(_) => {
                return Err(LixError {
                    message: "vtable insert requires literal untracked values".to_string(),
                })
            }
        };

        if untracked {
            untracked_rows.push(row.clone());
        } else {
            tracked_rows.push(row.clone());
        }
    }

    let mut statements: Vec<Statement> = Vec::new();
    let mut registrations: Vec<SchemaRegistration> = Vec::new();

    if !tracked_rows.is_empty() {
        let tracked = rewrite_tracked_rows(&insert, tracked_rows, &mut registrations)?;
        statements.extend(tracked);
    }

    if !untracked_rows.is_empty() {
        let untracked = build_untracked_insert(&insert, untracked_rows)?;
        statements.push(untracked);
    }

    Ok(Some(VtableWriteRewrite {
        statements,
        registrations,
    }))
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
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("snapshot_content"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("plugin_key")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("plugin_key"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("schema_version")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("schema_version"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("updated_at")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("updated_at"),
                    ]),
                },
            ],
            selection: None,
        }),
    })
}

fn build_materialized_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("snapshot_content"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("change_id")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("change_id"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("updated_at")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("updated_at"),
                    ]),
                },
            ],
            selection: None,
        }),
    })
}

fn rewrite_tracked_rows(
    insert: &sqlparser::ast::Insert,
    rows: Vec<Vec<Expr>>,
    registrations: &mut Vec<SchemaRegistration>,
) -> Result<Vec<Statement>, LixError> {
    let entity_idx = required_column_index(&insert.columns, "entity_id")?;
    let schema_idx = required_column_index(&insert.columns, "schema_key")?;
    let file_idx = required_column_index(&insert.columns, "file_id")?;
    let version_idx = required_column_index(&insert.columns, "version_id")?;
    let plugin_idx = required_column_index(&insert.columns, "plugin_key")?;
    let schema_version_idx = required_column_index(&insert.columns, "schema_version")?;
    let snapshot_idx = required_column_index(&insert.columns, "snapshot_content")?;
    let metadata_idx = find_column_index(&insert.columns, "metadata");

    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: std::collections::BTreeMap<String, Vec<Vec<Expr>>> =
        std::collections::BTreeMap::new();

    for row in rows {
        let schema_key_expr = row.get(schema_idx).ok_or_else(|| LixError {
            message: "vtable insert missing schema_key".to_string(),
        })?;
        let schema_key = literal_string(schema_key_expr)?;

        if !registrations.iter().any(|reg| reg.schema_key == schema_key) {
            registrations.push(SchemaRegistration {
                schema_key: schema_key.clone(),
            });
        }

        let snapshot_content = row.get(snapshot_idx).cloned().unwrap_or_else(null_expr);
        let snapshot_id = if is_null_literal(&snapshot_content) {
            ensure_no_content = true;
            "no-content".to_string()
        } else {
            let id = uuid_v7();
            snapshot_rows.push(vec![string_expr(&id), snapshot_content.clone()]);
            id
        };

        let change_id = uuid_v7();
        let created_at = timestamp();
        let updated_at = created_at.clone();

        let metadata_expr = metadata_idx
            .and_then(|idx| row.get(idx))
            .cloned()
            .unwrap_or_else(null_expr);

        change_rows.push(vec![
            string_expr(&change_id),
            row.get(entity_idx).cloned().unwrap_or_else(null_expr),
            row.get(schema_idx).cloned().unwrap_or_else(null_expr),
            row.get(schema_version_idx)
                .cloned()
                .unwrap_or_else(null_expr),
            row.get(file_idx).cloned().unwrap_or_else(null_expr),
            row.get(plugin_idx).cloned().unwrap_or_else(null_expr),
            string_expr(&snapshot_id),
            metadata_expr,
            string_expr(&created_at),
        ]);

        let materialized_row = vec![
            row.get(entity_idx).cloned().unwrap_or_else(null_expr),
            row.get(schema_idx).cloned().unwrap_or_else(null_expr),
            row.get(file_idx).cloned().unwrap_or_else(null_expr),
            row.get(version_idx).cloned().unwrap_or_else(null_expr),
            row.get(plugin_idx).cloned().unwrap_or_else(null_expr),
            snapshot_content,
            string_expr(&change_id),
            number_expr("0"),
            string_expr(&created_at),
            string_expr(&updated_at),
        ];

        materialized_by_schema
            .entry(schema_key)
            .or_default()
            .push(materialized_row);
    }

    let mut statements = Vec::new();

    if ensure_no_content {
        statements.push(make_insert_statement(
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            vec![vec![string_expr("no-content"), null_expr()]],
            Some(build_snapshot_on_conflict()),
        ));
    }

    if !snapshot_rows.is_empty() {
        statements.push(make_insert_statement(
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            snapshot_rows,
            Some(build_snapshot_on_conflict()),
        ));
    }

    if !change_rows.is_empty() {
        statements.push(make_insert_statement(
            CHANGE_TABLE,
            vec![
                Ident::new("id"),
                Ident::new("entity_id"),
                Ident::new("schema_key"),
                Ident::new("schema_version"),
                Ident::new("file_id"),
                Ident::new("plugin_key"),
                Ident::new("snapshot_id"),
                Ident::new("metadata"),
                Ident::new("created_at"),
            ],
            change_rows,
            None,
        ));
    }

    for (schema_key, rows) in materialized_by_schema {
        let table_name = format!("{}{}", MATERIALIZED_PREFIX, schema_key);
        statements.push(make_insert_statement(
            &table_name,
            vec![
                Ident::new("entity_id"),
                Ident::new("schema_key"),
                Ident::new("file_id"),
                Ident::new("version_id"),
                Ident::new("plugin_key"),
                Ident::new("snapshot_content"),
                Ident::new("change_id"),
                Ident::new("is_tombstone"),
                Ident::new("created_at"),
                Ident::new("updated_at"),
            ],
            rows,
            Some(build_materialized_on_conflict()),
        ));
    }

    Ok(statements)
}

fn build_snapshot_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![Ident::new("id")])),
        action: OnConflictAction::DoNothing,
    })
}

fn build_untracked_insert(
    insert: &sqlparser::ast::Insert,
    rows: Vec<Vec<Expr>>,
) -> Result<Statement, LixError> {
    let entity_idx = required_column_index(&insert.columns, "entity_id")?;
    let schema_idx = required_column_index(&insert.columns, "schema_key")?;
    let file_idx = required_column_index(&insert.columns, "file_id")?;
    let version_idx = required_column_index(&insert.columns, "version_id")?;
    let plugin_idx = required_column_index(&insert.columns, "plugin_key")?;
    let snapshot_idx = required_column_index(&insert.columns, "snapshot_content")?;
    let schema_version_idx = required_column_index(&insert.columns, "schema_version")?;

    let mut mapped_rows = Vec::new();
    for row in rows {
        let now = timestamp();
        mapped_rows.push(vec![
            row.get(entity_idx).cloned().unwrap_or_else(null_expr),
            row.get(schema_idx).cloned().unwrap_or_else(null_expr),
            row.get(file_idx).cloned().unwrap_or_else(null_expr),
            row.get(version_idx).cloned().unwrap_or_else(null_expr),
            row.get(plugin_idx).cloned().unwrap_or_else(null_expr),
            row.get(snapshot_idx).cloned().unwrap_or_else(null_expr),
            row.get(schema_version_idx)
                .cloned()
                .unwrap_or_else(null_expr),
            string_expr(&now),
            string_expr(&now),
        ]);
    }

    Ok(make_insert_statement(
        UNTRACKED_TABLE,
        vec![
            Ident::new("entity_id"),
            Ident::new("schema_key"),
            Ident::new("file_id"),
            Ident::new("version_id"),
            Ident::new("plugin_key"),
            Ident::new("snapshot_content"),
            Ident::new("schema_version"),
            Ident::new("created_at"),
            Ident::new("updated_at"),
        ],
        mapped_rows,
        Some(build_untracked_on_conflict()),
    ))
}

fn make_insert_statement(
    table: &str,
    columns: Vec<Ident>,
    rows: Vec<Vec<Expr>>,
    on: Option<OnInsert>,
) -> Statement {
    let values = Values {
        explicit_row: false,
        value_keyword: false,
        rows,
    };
    let query = Query {
        with: None,
        body: Box::new(SetExpr::Values(values)),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    Statement::Insert(sqlparser::ast::Insert {
        insert_token: AttachedToken::empty(),
        or: None,
        ignore: false,
        into: true,
        table: TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            table,
        ))])),
        table_alias: None,
        columns,
        overwrite: false,
        source: Some(Box::new(query)),
        assignments: Vec::new(),
        partitioned: None,
        after_columns: Vec::new(),
        has_table_keyword: false,
        on,
        returning: None,
        replace_into: false,
        priority: None,
        insert_alias: None,
        settings: None,
        format_clause: None,
    })
}

fn required_column_index(columns: &[Ident], name: &str) -> Result<usize, LixError> {
    find_column_index(columns, name).ok_or_else(|| LixError {
        message: format!("vtable insert requires {name}"),
    })
}

fn literal_string(expr: &Expr) -> Result<String, LixError> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Ok(value.clone()),
        _ => Err(LixError {
            message: "vtable insert requires literal schema_key".to_string(),
        }),
    }
}

fn string_expr(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
}

fn number_expr(value: &str) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).into())
}

fn null_expr() -> Expr {
    Expr::Value(Value::Null.into())
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: Value::Null,
            ..
        })
    )
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

fn is_untracked_false_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(value, _),
            ..
        }) => value == "0",
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(value),
            ..
        }) => !*value,
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

#[cfg(test)]
mod tests {
    use super::rewrite_insert;
    use sqlparser::ast::{
        Expr, ObjectNamePart, SetExpr, Statement, TableObject, Value, ValueWithSpan,
    };
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn rewrite_tracked_insert_emits_snapshot_change_and_materialized() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1')"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let rewrite = rewrite_insert(insert)
            .expect("rewrite ok")
            .expect("rewrite applied");

        assert_eq!(rewrite.statements.len(), 3);

        let snapshot_stmt = find_insert(&rewrite.statements, "lix_internal_snapshot");
        let change_stmt = find_insert(&rewrite.statements, "lix_internal_change");
        let materialized_stmt = find_insert(
            &rewrite.statements,
            "lix_internal_state_materialized_v1_test_schema",
        );

        let snapshot_id = extract_string_value(snapshot_stmt, "id");
        let change_snapshot_id = extract_string_value(change_stmt, "snapshot_id");
        assert_eq!(snapshot_id, change_snapshot_id);

        let change_id = extract_string_value(change_stmt, "id");
        let materialized_change_id = extract_string_value(materialized_stmt, "change_id");
        assert_eq!(change_id, materialized_change_id);
    }

    #[test]
    fn rewrite_tracked_insert_uses_no_content_snapshot() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', NULL, '1')"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let rewrite = rewrite_insert(insert)
            .expect("rewrite ok")
            .expect("rewrite applied");

        let change_stmt = find_insert(&rewrite.statements, "lix_internal_change");
        let snapshot_id = extract_string_value(change_stmt, "snapshot_id");
        assert_eq!(snapshot_id, "no-content");

        let snapshot_stmt = find_insert(&rewrite.statements, "lix_internal_snapshot");
        let ensured_id = extract_string_value(snapshot_stmt, "id");
        assert_eq!(ensured_id, "no-content");
    }

    #[test]
    fn rewrite_untracked_insert_routes_to_untracked_table() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', 1)"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let rewrite = rewrite_insert(insert)
            .expect("rewrite ok")
            .expect("rewrite applied");

        assert_eq!(rewrite.statements.len(), 1);
        let stmt = &rewrite.statements[0];
        assert_eq!(table_name(stmt), "lix_internal_state_untracked");
    }

    fn find_insert<'a>(statements: &'a [Statement], table: &str) -> &'a Statement {
        statements
            .iter()
            .find(|stmt| table_name(stmt) == table)
            .unwrap_or_else(|| panic!("missing insert into {table}"))
    }

    fn table_name(statement: &Statement) -> &str {
        match statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => name
                    .0
                    .last()
                    .and_then(ObjectNamePart::as_ident)
                    .map(|ident| ident.value.as_str())
                    .expect("table name ident"),
                _ => panic!("expected table name"),
            },
            _ => panic!("expected insert statement"),
        }
    }

    fn extract_string_value(statement: &Statement, column: &str) -> String {
        let (columns, rows) = insert_values(statement);
        let idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case(column))
            .expect("column present");
        let expr = rows.get(0).and_then(|row| row.get(idx)).expect("row value");
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(value),
                ..
            }) => value.clone(),
            _ => panic!("expected string literal"),
        }
    }

    fn insert_values(statement: &Statement) -> (Vec<String>, Vec<Vec<Expr>>) {
        match statement {
            Statement::Insert(insert) => {
                let columns = insert
                    .columns
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                let rows = match insert.source.as_ref().expect("insert source").body.as_ref() {
                    SetExpr::Values(values) => values.rows.clone(),
                    _ => panic!("expected values"),
                };
                (columns, rows)
            }
            _ => panic!("expected insert"),
        }
    }
}
