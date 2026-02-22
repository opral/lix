use serde_json::Value as JsonValue;
use sqlparser::ast::{Delete, Expr, Insert, Statement, TableFactor, TableObject, TableWithJoins};

use crate::account::{
    active_account_file_id, active_account_plugin_key, active_account_schema_key,
    active_account_schema_version, active_account_snapshot_content,
    active_account_storage_version_id,
};
use crate::sql::planner::rewrite::query::execute_rewritten_read_sql_with_state;
use crate::sql::{object_name_matches, resolve_insert_rows, PlaceholderState};
use crate::{LixBackend, LixError, Value as EngineValue};

use super::insert_builder::{
    and_expr, column_expr, eq_expr, int_expr, make_delete, make_values_insert, string_expr,
};

const LIX_ACTIVE_ACCOUNT_VIEW_NAME: &str = "lix_active_account";
const VTABLE_NAME: &str = "lix_internal_state_vtable";

#[derive(Debug, Clone)]
struct InsertRow {
    entity_id: String,
    snapshot_content: JsonValue,
}

pub fn rewrite_insert(
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<Vec<Insert>>, LixError> {
    if !table_object_is_lix_active_account(&insert.table) {
        return Ok(None);
    }
    if insert.on.is_some() {
        return Err(LixError {
            message: "lix_active_account insert does not support ON CONFLICT".to_string(),
        });
    }
    if insert.columns.is_empty() {
        return Err(LixError {
            message: "lix_active_account insert requires explicit columns".to_string(),
        });
    }

    let resolved_rows = resolve_insert_rows(&insert, params)?.ok_or_else(|| LixError {
        message: "lix_active_account insert requires VALUES rows".to_string(),
    })?;
    let mut rows = Vec::with_capacity(resolved_rows.len());
    for resolved_row in resolved_rows {
        let mut account_id: Option<String> = None;

        for (index, column) in insert.columns.iter().enumerate() {
            let value = resolved_row
                .get(index)
                .and_then(|cell| cell.value.as_ref())
                .ok_or_else(|| LixError {
                    message: format!(
                        "lix_active_account insert '{}' must be literal or parameter",
                        column.value
                    ),
                })?;
            match column.value.to_ascii_lowercase().as_str() {
                "account_id" => account_id = Some(value_required_string(value, "account_id")?),
                other => {
                    return Err(LixError {
                        message: format!(
                            "lix_active_account insert does not support column '{other}'"
                        ),
                    })
                }
            }
        }

        let account_id = account_id.ok_or_else(|| LixError {
            message: "lix_active_account insert requires column 'account_id'".to_string(),
        })?;
        if account_id.is_empty() {
            return Err(LixError {
                message: "lix_active_account insert requires non-empty account_id".to_string(),
            });
        }

        rows.push(InsertRow {
            entity_id: account_id.clone(),
            snapshot_content: serde_json::from_str::<JsonValue>(&active_account_snapshot_content(
                &account_id,
            ))
            .map_err(|error| LixError {
                message: format!("failed to encode active account snapshot: {error}"),
            })?,
        });
    }

    Ok(Some(vec![build_vtable_insert(rows)?]))
}

pub async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[EngineValue],
) -> Result<Option<Statement>, LixError> {
    if !delete_from_is_lix_active_account(&delete) {
        return Ok(None);
    }
    if delete.using.is_some() {
        return Err(LixError {
            message: "lix_active_account delete does not support USING".to_string(),
        });
    }
    if delete.returning.is_some() {
        return Err(LixError {
            message: "lix_active_account delete does not support RETURNING".to_string(),
        });
    }
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Err(LixError {
            message: "lix_active_account delete does not support LIMIT or ORDER BY".to_string(),
        });
    }

    let entity_ids =
        query_entity_ids_for_delete(backend, delete.selection.as_ref(), params).await?;
    if entity_ids.is_empty() {
        return Ok(Some(build_noop_delete()?));
    }

    Ok(Some(build_vtable_delete(entity_ids)?))
}

fn value_required_string(value: &EngineValue, field: &str) -> Result<String, LixError> {
    match value {
        EngineValue::Text(text) => Ok(text.clone()),
        EngineValue::Null => Err(LixError {
            message: format!("lix_active_account field '{field}' cannot be NULL"),
        }),
        _ => Err(LixError {
            message: format!("lix_active_account field '{field}' must be a string"),
        }),
    }
}

async fn query_entity_ids_for_delete(
    backend: &dyn LixBackend,
    selection: Option<&Expr>,
    params: &[EngineValue],
) -> Result<Vec<String>, LixError> {
    let mut sql = "SELECT account_id FROM lix_active_account".to_string();
    if let Some(selection) = selection {
        sql.push_str(" WHERE ");
        sql.push_str(&selection.to_string());
    }
    let result = execute_rewritten_read_sql_with_state(
        backend,
        &sql,
        params,
        PlaceholderState::new(),
        "lix_active_account row loader query",
    )
    .await?;

    let mut entity_ids = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        if row.len() != 1 {
            return Err(LixError {
                message: "lix_active_account rewrite expected 1 column from row loader query"
                    .to_string(),
            });
        }
        entity_ids.push(value_required_string(&row[0], "account_id")?);
    }
    Ok(entity_ids)
}

fn build_vtable_insert(rows: Vec<InsertRow>) -> Result<Insert, LixError> {
    let values = rows
        .into_iter()
        .map(|row| {
            vec![
                string_expr(&row.entity_id),
                string_expr(active_account_schema_key()),
                string_expr(active_account_file_id()),
                string_expr(active_account_storage_version_id()),
                string_expr(active_account_plugin_key()),
                string_expr(&row.snapshot_content.to_string()),
                string_expr(active_account_schema_version()),
                int_expr(1),
            ]
        })
        .collect::<Vec<_>>();

    Ok(make_values_insert(
        VTABLE_NAME,
        &[
            "entity_id",
            "schema_key",
            "file_id",
            "version_id",
            "plugin_key",
            "snapshot_content",
            "schema_version",
            "untracked",
        ],
        values,
    ))
}

fn build_vtable_delete(entity_ids: Vec<String>) -> Result<Statement, LixError> {
    let entity_in_list = Expr::InList {
        expr: Box::new(column_expr("entity_id")),
        list: entity_ids
            .iter()
            .map(|entity_id| string_expr(entity_id))
            .collect(),
        negated: false,
    };
    let selection = and_expr(
        and_expr(
            and_expr(
                and_expr(
                    eq_expr(
                        column_expr("schema_key"),
                        string_expr(active_account_schema_key()),
                    ),
                    eq_expr(
                        column_expr("file_id"),
                        string_expr(active_account_file_id()),
                    ),
                ),
                eq_expr(
                    column_expr("version_id"),
                    string_expr(active_account_storage_version_id()),
                ),
            ),
            eq_expr(column_expr("untracked"), int_expr(1)),
        ),
        entity_in_list,
    );
    Ok(make_delete(VTABLE_NAME, selection))
}

fn build_noop_delete() -> Result<Statement, LixError> {
    Ok(make_delete(VTABLE_NAME, eq_expr(int_expr(1), int_expr(0))))
}

fn table_object_is_lix_active_account(table: &TableObject) -> bool {
    matches!(table, TableObject::TableName(name) if object_name_matches(name, LIX_ACTIVE_ACCOUNT_VIEW_NAME))
}

fn delete_from_is_lix_active_account(delete: &Delete) -> bool {
    match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
            tables.len() == 1 && table_with_joins_is_lix_active_account(&tables[0])
        }
    }
}

fn table_with_joins_is_lix_active_account(table: &TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_ACTIVE_ACCOUNT_VIEW_NAME)
        )
}
