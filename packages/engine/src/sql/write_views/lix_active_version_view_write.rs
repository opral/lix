use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Insert, ObjectNamePart, TableFactor, TableWithJoins, Update,
};

use crate::sql::planner::rewrite::query::execute_rewritten_read_sql_with_state;
use crate::sql::{object_name_matches, resolve_expr_cell_with_state, PlaceholderState};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id,
};
use crate::{LixBackend, LixError, Value as EngineValue};

use super::insert_builder::{int_expr, make_values_insert, string_expr};

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
    let result = execute_rewritten_read_sql_with_state(
        backend,
        &sql,
        params,
        placeholder_state,
        "lix_active_version row loader query",
    )
    .await?;

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
        .into_iter()
        .map(|row| {
            vec![
                string_expr(&row.entity_id),
                string_expr(active_version_schema_key()),
                string_expr(active_version_file_id()),
                string_expr(active_version_storage_version_id()),
                string_expr(active_version_plugin_key()),
                string_expr(&row.snapshot_content.to_string()),
                string_expr(active_version_schema_version()),
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
