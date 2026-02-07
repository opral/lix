use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Delete, Expr, Insert, ObjectName, ObjectNamePart, Statement, TableFactor, TableObject,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::account::{
    active_account_file_id, active_account_plugin_key, active_account_schema_key,
    active_account_schema_version, active_account_snapshot_content,
    active_account_storage_version_id,
};
use crate::sql::lowering::lower_statement;
use crate::sql::steps::{lix_active_account_view_read, vtable_read};
use crate::sql::{bind_sql_with_state, resolve_insert_rows, PlaceholderState};
use crate::{LixBackend, LixError, Value as EngineValue};

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

    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).map_err(|error| LixError {
        message: format!("failed to parse lix_active_account row loader query: {error}"),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement while querying lix_active_account rows"
                .to_string(),
        });
    }
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(LixError {
            message: "lix_active_account row loader query must be SELECT".to_string(),
        });
    };

    let query = *query;
    let query = lix_active_account_view_read::rewrite_query(query.clone())?.unwrap_or(query);
    let query = vtable_read::rewrite_query(query.clone())?.unwrap_or(query);
    let lowered = lower_statement(Statement::Query(Box::new(query)), backend.dialect())?;
    let Statement::Query(lowered_query) = lowered else {
        return Err(LixError {
            message: "lix_active_account row loader rewrite expected query statement".to_string(),
        });
    };
    let bound = bind_sql_with_state(
        &lowered_query.to_string(),
        params,
        backend.dialect(),
        PlaceholderState::new(),
    )?;
    let result = backend.execute(&bound.sql, &bound.params).await?;

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
        .iter()
        .map(|row| {
            format!(
                "('{entity_id}', '{schema_key}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', '{schema_version}', 1)",
                entity_id = escape_sql_string(&row.entity_id),
                schema_key = escape_sql_string(active_account_schema_key()),
                file_id = escape_sql_string(active_account_file_id()),
                version_id = escape_sql_string(active_account_storage_version_id()),
                plugin_key = escape_sql_string(active_account_plugin_key()),
                snapshot_content = escape_sql_string(&row.snapshot_content.to_string()),
                schema_version = escape_sql_string(active_account_schema_version()),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {vtable} \
         (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked) \
         VALUES {values}",
        vtable = VTABLE_NAME,
        values = values,
    );
    parse_insert(
        &sql,
        "lix_active_account rewrite expected generated INSERT statement",
    )
}

fn build_vtable_delete(entity_ids: Vec<String>) -> Result<Statement, LixError> {
    let in_values = entity_ids
        .iter()
        .map(|entity_id| format!("'{}'", escape_sql_string(entity_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "DELETE FROM {vtable} \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = 1 \
           AND entity_id IN ({in_values})",
        vtable = VTABLE_NAME,
        schema_key = escape_sql_string(active_account_schema_key()),
        file_id = escape_sql_string(active_account_file_id()),
        version_id = escape_sql_string(active_account_storage_version_id()),
    );
    parse_statement(
        &sql,
        "lix_active_account rewrite expected generated DELETE statement",
    )
}

fn build_noop_delete() -> Result<Statement, LixError> {
    parse_statement(
        &format!("DELETE FROM {VTABLE_NAME} WHERE 1 = 0"),
        "lix_active_account rewrite expected generated no-op DELETE statement",
    )
}

fn parse_insert(sql: &str, error_message: &str) -> Result<Insert, LixError> {
    let statement = parse_statement(sql, error_message)?;
    match statement {
        Statement::Insert(insert) => Ok(insert),
        _ => Err(LixError {
            message: error_message.to_string(),
        }),
    }
}

fn parse_statement(sql: &str, error_message: &str) -> Result<Statement, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: format!("failed to build lix_active_account rewrite statement: {error}"),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: error_message.to_string(),
        });
    }
    Ok(statements.remove(0))
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

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
