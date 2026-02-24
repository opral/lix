use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Ident, Insert, ObjectName,
    ObjectNamePart, SetExpr, TableFactor, TableObject, TableWithJoins, Update, Value,
};

use crate::engine::sql::planning::rewrite_engine::object_name_matches;
use crate::engine::sql::planning::rewrite_engine::bind_sql;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::{LixBackend, LixError, Value as EngineValue};

const LIX_STATE_VIEW_NAME: &str = "lix_state";
const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    mut insert: Insert,
) -> Result<Option<Insert>, LixError> {
    if !table_object_is_lix_state(&insert.table) {
        return Ok(None);
    }
    if insert.on.is_some() {
        return Err(LixError {
            message: "lix_state insert does not support ON CONFLICT".to_string(),
        });
    }
    if insert.columns.is_empty() {
        return Err(LixError {
            message: "lix_state insert requires explicit columns".to_string(),
        });
    }
    if insert
        .columns
        .iter()
        .any(|column| column.value.eq_ignore_ascii_case("version_id"))
    {
        return Err(LixError {
            message:
                "lix_state insert cannot set version_id; active version is resolved automatically"
                    .to_string(),
        });
    }

    let active_version_id = load_active_version_id(backend).await?;
    let expected_columns = insert.columns.len();
    let source = insert.source.as_mut().ok_or_else(|| LixError {
        message: "lix_state insert requires VALUES rows".to_string(),
    })?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return Err(LixError {
            message: "lix_state insert requires VALUES rows".to_string(),
        });
    };

    for row in &mut values.rows {
        if row.len() != expected_columns {
            return Err(LixError {
                message: "lix_state insert row length does not match column count".to_string(),
            });
        }
        row.push(Expr::Value(
            sqlparser::ast::Value::SingleQuotedString(active_version_id.clone()).into(),
        ));
    }

    insert.columns.push(Ident::new("version_id"));
    insert.table = TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(
        Ident::new(VTABLE_NAME),
    )]));
    Ok(Some(insert))
}

pub async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    mut update: Update,
    params: &[EngineValue],
) -> Result<Option<Update>, LixError> {
    if !table_with_joins_is_lix_state(&update.table) {
        return Ok(None);
    }
    if update
        .assignments
        .iter()
        .any(|assignment| assignment_target_is_column(&assignment.target, "version_id"))
    {
        return Err(LixError {
            message:
                "lix_state update cannot set version_id; active version is resolved automatically"
                    .to_string(),
        });
    }

    let active_version_id = load_active_version_id(backend).await?;
    let stripped_selection = update
        .selection
        .take()
        .map(strip_inherited_from_version_predicate)
        .transpose()?
        .flatten();
    let has_untracked_predicate = stripped_selection
        .as_ref()
        .map(|selection| contains_column_reference(selection, "untracked"))
        .unwrap_or(false);
    replace_table_with_vtable(&mut update.table)?;
    let mut selection = match stripped_selection {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(version_predicate_expr(&active_version_id)),
        },
        None => version_predicate_expr(&active_version_id),
    };

    if !has_untracked_predicate && matches_untracked_rows(backend, &selection, params).await? {
        selection = Expr::BinaryOp {
            left: Box::new(selection),
            op: BinaryOperator::And,
            right: Box::new(untracked_true_predicate_expr()),
        };
    }

    update.selection = Some(selection);
    Ok(Some(update))
}

async fn matches_untracked_rows(
    backend: &dyn LixBackend,
    selection: &Expr,
    params: &[EngineValue],
) -> Result<bool, LixError> {
    let sql = format!(
        "SELECT 1 \
         FROM {untracked_table} \
         WHERE ({selection}) \
         LIMIT 1",
        untracked_table = UNTRACKED_TABLE,
        selection = selection,
    );
    let bound = bind_sql(&sql, params, backend.dialect())?;
    let result = backend.execute(&bound.sql, &bound.params).await?;
    Ok(!result.rows.is_empty())
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

fn untracked_true_predicate_expr() -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("untracked"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number("1".to_string(), false).into())),
    }
}

pub async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    mut delete: Delete,
) -> Result<Option<Delete>, LixError> {
    if !delete_from_is_lix_state(&delete) {
        return Ok(None);
    }

    let active_version_id = load_active_version_id(backend).await?;
    replace_delete_from_vtable(&mut delete)?;
    let stripped_selection = delete
        .selection
        .take()
        .map(strip_inherited_from_version_predicate)
        .transpose()?
        .flatten();
    delete.selection = Some(match stripped_selection {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(version_predicate_expr(&active_version_id)),
        },
        None => version_predicate_expr(&active_version_id),
    });
    Ok(Some(delete))
}

async fn load_active_version_id(backend: &dyn LixBackend) -> Result<String, LixError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM {untracked_table} \
         WHERE schema_key = $1 \
           AND file_id = $2 \
           AND version_id = $3 \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        untracked_table = UNTRACKED_TABLE,
    );
    let result = backend
        .execute(
            &sql,
            &[
                EngineValue::Text(active_version_schema_key().to_string()),
                EngineValue::Text(active_version_file_id().to_string()),
                EngineValue::Text(active_version_storage_version_id().to_string()),
            ],
        )
        .await?;

    let row = result.rows.first().ok_or_else(|| LixError {
        message: "lix_state write requires an active version".to_string(),
    })?;
    let snapshot_content = row.first().ok_or_else(|| LixError {
        message: "active version query row is missing snapshot_content".to_string(),
    })?;
    let snapshot_content = match snapshot_content {
        EngineValue::Text(value) => value.as_str(),
        other => {
            return Err(LixError {
                message: format!("active version snapshot_content must be text, got {other:?}"),
            })
        }
    };
    parse_active_version_snapshot(snapshot_content)
}

fn table_object_is_lix_state(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, LIX_STATE_VIEW_NAME),
        _ => false,
    }
}

fn table_with_joins_is_lix_state(table: &TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_STATE_VIEW_NAME)
        )
}

fn delete_from_is_lix_state(delete: &Delete) -> bool {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            tables.len() == 1 && table_with_joins_is_lix_state(&tables[0])
        }
    }
}

fn replace_table_with_vtable(table: &mut TableWithJoins) -> Result<(), LixError> {
    if !table.joins.is_empty() {
        return Err(LixError {
            message: "lix_state update does not support JOIN targets".to_string(),
        });
    }
    match &mut table.relation {
        TableFactor::Table { name, .. } => {
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(VTABLE_NAME))]);
            Ok(())
        }
        _ => Err(LixError {
            message: "lix_state update requires table target".to_string(),
        }),
    }
}

fn replace_delete_from_vtable(delete: &mut Delete) -> Result<(), LixError> {
    let tables = match &mut delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let Some(table) = tables.first_mut() else {
        return Err(LixError {
            message: "lix_state delete requires table target".to_string(),
        });
    };
    replace_table_with_vtable(table)
}

fn version_predicate_expr(version_id: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("version_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            Value::SingleQuotedString(version_id.to_string()).into(),
        )),
    }
}

fn strip_inherited_from_version_predicate(expr: Expr) -> Result<Option<Expr>, LixError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left = strip_inherited_from_version_predicate(*left)?;
            let right = strip_inherited_from_version_predicate(*right)?;
            match (left, right) {
                (Some(left), Some(right)) => Ok(Some(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                })),
                (Some(expr), None) | (None, Some(expr)) => Ok(Some(expr)),
                (None, None) => Ok(None),
            }
        }
        Expr::IsNull(inner) if expr_is_inherited_from_version_column(&inner) => Ok(None),
        Expr::IsNotNull(inner) if expr_is_inherited_from_version_column(&inner) => {
            Ok(Some(false_predicate_expr()))
        }
        other if contains_column_reference(&other, "inherited_from_version_id") => Err(LixError {
            message:
                "lix_state mutation only supports inherited_from_version_id filters via IS NULL/IS NOT NULL"
                    .to_string(),
        }),
        other => Ok(Some(other)),
    }
}

fn expr_is_inherited_from_version_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident
            .value
            .eq_ignore_ascii_case("inherited_from_version_id"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| {
                ident
                    .value
                    .eq_ignore_ascii_case("inherited_from_version_id")
            })
            .unwrap_or(false),
        _ => false,
    }
}

fn false_predicate_expr() -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Value(Value::Number("1".to_string(), false).into())),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number("0".to_string(), false).into())),
    }
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
