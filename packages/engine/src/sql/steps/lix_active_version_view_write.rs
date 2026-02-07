use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Insert, ObjectName, ObjectNamePart, Statement, TableFactor,
    TableWithJoins, Update,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::lowering::lower_statement;
use crate::sql::steps::{lix_active_version_view_read, vtable_read};
use crate::sql::{
    bind_sql_with_state, escape_sql_string, resolve_expr_cell_with_state, PlaceholderState,
};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id,
};
use crate::{LixBackend, LixError, Value as EngineValue};

const LIX_ACTIVE_VERSION_VIEW_NAME: &str = "lix_active_version";
const VTABLE_NAME: &str = "lix_internal_state_vtable";
const VERSION_DESCRIPTOR_TABLE: &str = "lix_internal_state_materialized_v1_lix_version_descriptor";

#[derive(Debug, Clone)]
struct ActiveVersionRow {
    id: String,
}

#[derive(Debug, Clone)]
struct UpdateAssignments {
    version_id: String,
}

#[derive(Debug, Clone)]
struct InsertSnapshotRow {
    entity_id: String,
    snapshot_content: JsonValue,
}

pub async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[EngineValue],
) -> Result<Option<Vec<Insert>>, LixError> {
    if !table_with_joins_is_lix_active_version(&update.table) {
        return Ok(None);
    }
    if update.from.is_some() {
        return Err(LixError {
            message: "lix_active_version update does not support FROM".to_string(),
        });
    }
    if update.returning.is_some() {
        return Err(LixError {
            message: "lix_active_version update does not support RETURNING".to_string(),
        });
    }

    let mut placeholder_state = PlaceholderState::new();
    let assignments =
        parse_update_assignments(&update.assignments, params, &mut placeholder_state)?;
    ensure_version_descriptor_exists(backend, &assignments.version_id).await?;

    let existing_rows = query_lix_active_version_rows(
        backend,
        update.selection.as_ref(),
        params,
        placeholder_state,
    )
    .await?;
    if existing_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let insert_rows = existing_rows
        .into_iter()
        .map(|row| {
            let snapshot_content = serde_json::from_str::<JsonValue>(
                &active_version_snapshot_content(&row.id, &assignments.version_id),
            )
            .map_err(|error| LixError {
                message: format!("failed to encode active version snapshot: {error}"),
            })?;
            Ok(InsertSnapshotRow {
                entity_id: row.id,
                snapshot_content,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;

    Ok(Some(vec![build_vtable_insert(insert_rows)?]))
}

fn parse_update_assignments(
    assignments: &[Assignment],
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
) -> Result<UpdateAssignments, LixError> {
    let mut version_id: Option<String> = None;
    for assignment in assignments {
        let column = assignment_target_column(&assignment.target).ok_or_else(|| LixError {
            message: "lix_active_version update requires single-column assignments".to_string(),
        })?;
        let resolved = resolve_expr_cell_with_state(&assignment.value, params, placeholder_state)?;
        let value = resolved.value.as_ref().ok_or_else(|| LixError {
            message: format!(
                "lix_active_version update assignment for '{column}' must be literal or parameter"
            ),
        })?;
        match column.as_str() {
            "id" => {
                return Err(LixError {
                    message: "lix_active_version update cannot modify id".to_string(),
                })
            }
            "version_id" => {
                let next_value = value_required_string(value, "version_id")?;
                if next_value.is_empty() {
                    return Err(LixError {
                        message: "lix_active_version update cannot set empty version_id"
                            .to_string(),
                    });
                }
                version_id = Some(next_value);
            }
            _ => {
                return Err(LixError {
                    message: format!(
                        "lix_active_version update does not support column '{column}'"
                    ),
                })
            }
        }
    }

    let version_id = version_id.ok_or_else(|| LixError {
        message: "lix_active_version update must set version_id".to_string(),
    })?;

    Ok(UpdateAssignments { version_id })
}

fn value_required_string(value: &EngineValue, field: &str) -> Result<String, LixError> {
    match value {
        EngineValue::Text(text) => Ok(text.clone()),
        EngineValue::Null => Err(LixError {
            message: format!("lix_active_version field '{field}' cannot be NULL"),
        }),
        _ => Err(LixError {
            message: format!("lix_active_version field '{field}' must be a string"),
        }),
    }
}

async fn ensure_version_descriptor_exists(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<(), LixError> {
    let result = backend
        .execute(
            &format!(
                "SELECT 1 \
                 FROM {table} \
                 WHERE entity_id = $1 \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                table = VERSION_DESCRIPTOR_TABLE,
            ),
            &[EngineValue::Text(version_id.to_string())],
        )
        .await?;

    if result.rows.is_empty() {
        return Err(LixError {
            message: format!(
                "Foreign key constraint violation: lix_active_version.version_id '{}' references missing lix_version_descriptor.id",
                version_id
            ),
        });
    }
    Ok(())
}

async fn query_lix_active_version_rows(
    backend: &dyn LixBackend,
    selection: Option<&Expr>,
    params: &[EngineValue],
    placeholder_state: PlaceholderState,
) -> Result<Vec<ActiveVersionRow>, LixError> {
    let mut sql = "SELECT id FROM lix_active_version".to_string();
    if let Some(selection) = selection {
        sql.push_str(" WHERE ");
        sql.push_str(&selection.to_string());
    }

    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).map_err(|error| LixError {
        message: format!("failed to parse lix_active_version row loader query: {error}"),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement while querying lix_active_version rows"
                .to_string(),
        });
    }
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(LixError {
            message: "lix_active_version row loader query must be SELECT".to_string(),
        });
    };

    let query = *query;
    let query = lix_active_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
    let query = vtable_read::rewrite_query(query.clone())?.unwrap_or(query);
    let lowered = lower_statement(Statement::Query(Box::new(query)), backend.dialect())?;
    let Statement::Query(lowered_query) = lowered else {
        return Err(LixError {
            message: "lix_active_version row loader rewrite expected query statement".to_string(),
        });
    };
    let bound = bind_sql_with_state(
        &lowered_query.to_string(),
        params,
        backend.dialect(),
        placeholder_state,
    )?;
    let result = backend.execute(&bound.sql, &bound.params).await?;

    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        if row.len() != 1 {
            return Err(LixError {
                message: "lix_active_version rewrite expected 1 column from row loader query"
                    .to_string(),
            });
        }
        rows.push(ActiveVersionRow {
            id: value_required_string(&row[0], "id")?,
        });
    }
    Ok(rows)
}

fn build_vtable_insert(rows: Vec<InsertSnapshotRow>) -> Result<Insert, LixError> {
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "('{entity_id}', '{schema_key}', '{file_id}', '{storage_version_id}', '{plugin_key}', '{snapshot_content}', '{schema_version}', 1)",
                entity_id = escape_sql_string(&row.entity_id),
                schema_key = escape_sql_string(active_version_schema_key()),
                file_id = escape_sql_string(active_version_file_id()),
                storage_version_id = escape_sql_string(active_version_storage_version_id()),
                plugin_key = escape_sql_string(active_version_plugin_key()),
                snapshot_content = escape_sql_string(&row.snapshot_content.to_string()),
                schema_version = escape_sql_string(active_version_schema_version()),
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
    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).map_err(|error| LixError {
        message: format!("failed to build vtable insert for lix_active_version rewrite: {error}"),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "lix_active_version rewrite expected one INSERT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    match statement {
        Statement::Insert(insert) => Ok(insert),
        _ => Err(LixError {
            message: "lix_active_version rewrite expected generated INSERT statement".to_string(),
        }),
    }
}

fn assignment_target_column(target: &AssignmentTarget) -> Option<String> {
    match target {
        AssignmentTarget::ColumnName(name) => name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.to_ascii_lowercase()),
        AssignmentTarget::Tuple(_) => None,
    }
}

fn table_with_joins_is_lix_active_version(table: &TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_ACTIVE_VERSION_VIEW_NAME)
        )
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}
