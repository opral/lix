use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, Ident, Insert, ObjectNamePart,
    Statement, TableFactor, TableObject, TableWithJoins, Update,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::sql::lowering::lower_statement;
use crate::sql::steps::{lix_version_view_read, vtable_read};
use crate::sql::{
    bind_sql_with_state, escape_sql_string, object_name_matches, resolve_expr_cell_with_state,
    PlaceholderState, RowSourceResolver,
};
use crate::version::{
    version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content,
    version_descriptor_storage_version_id, version_pointer_file_id, version_pointer_plugin_key,
    version_pointer_schema_key, version_pointer_schema_version, version_pointer_snapshot_content,
    version_pointer_storage_version_id,
};
use crate::{LixBackend, LixError, Value as EngineValue};

const LIX_VERSION_VIEW_NAME: &str = "lix_version";
const VTABLE_NAME: &str = "lix_internal_state_vtable";
const VERSION_POINTER_TABLE: &str = "lix_internal_state_materialized_v1_lix_version_pointer";

pub fn rewrite_insert(
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<Vec<Insert>>, LixError> {
    if !table_object_is_lix_version(&insert.table) {
        return Ok(None);
    }
    if insert.columns.is_empty() {
        return Err(LixError {
            message: "lix_version insert requires explicit columns".to_string(),
        });
    }
    if insert.on.is_some() {
        return Err(LixError {
            message: "lix_version insert does not support ON CONFLICT".to_string(),
        });
    }

    let field_map = insert_field_map(&insert.columns)?;
    let rows_source =
        RowSourceResolver::new(params).resolve_insert_required(&insert, "lix_version insert")?;
    let parsed_rows = parse_insert_rows(
        &field_map,
        rows_source.rows,
        rows_source.resolved_rows,
        "lix_version insert",
    )?;
    let descriptor_rows = parsed_rows
        .iter()
        .map(|row| row.descriptor_row.clone())
        .collect::<Vec<_>>();
    let tip_rows = parsed_rows
        .iter()
        .map(|row| row.tip_row.clone())
        .collect::<Vec<_>>();

    Ok(Some(build_vtable_inserts(descriptor_rows, tip_rows)?))
}

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<Vec<Insert>>, LixError> {
    if !table_object_is_lix_version(&insert.table) {
        return Ok(None);
    }
    if insert.columns.is_empty() {
        return Err(LixError {
            message: "lix_version insert requires explicit columns".to_string(),
        });
    }
    if insert.on.is_some() {
        return Err(LixError {
            message: "lix_version insert does not support ON CONFLICT".to_string(),
        });
    }

    let field_map = insert_field_map(&insert.columns)?;
    let rows_source =
        RowSourceResolver::new(params).resolve_insert_required(&insert, "lix_version insert")?;
    let parsed_rows = parse_insert_rows(
        &field_map,
        rows_source.rows,
        rows_source.resolved_rows,
        "lix_version insert",
    )?;
    validate_tip_working_commit_uniqueness(backend, &parsed_rows).await?;

    let descriptor_rows = parsed_rows
        .iter()
        .map(|row| row.descriptor_row.clone())
        .collect::<Vec<_>>();
    let tip_rows = parsed_rows
        .iter()
        .map(|row| row.tip_row.clone())
        .collect::<Vec<_>>();
    Ok(Some(build_vtable_inserts(descriptor_rows, tip_rows)?))
}

pub async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[EngineValue],
) -> Result<Option<Vec<Insert>>, LixError> {
    if !table_with_joins_is_lix_version(&update.table) {
        return Ok(None);
    }
    if update.from.is_some() {
        return Err(LixError {
            message: "lix_version update does not support FROM".to_string(),
        });
    }
    if update.returning.is_some() {
        return Err(LixError {
            message: "lix_version update does not support RETURNING".to_string(),
        });
    }
    let mut placeholder_state = PlaceholderState::new();
    let assignment_values =
        parse_update_assignments(&update.assignments, params, &mut placeholder_state)?;
    if !assignment_values.touches_descriptor() && !assignment_values.touches_tip() {
        return Err(LixError {
            message: "lix_version update must set at least one supported column".to_string(),
        });
    }

    let existing_rows = query_lix_version_rows(
        backend,
        update.selection.as_ref(),
        params,
        placeholder_state,
    )
    .await?;
    if existing_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut descriptor_rows = Vec::new();
    let mut tip_rows = Vec::new();
    for existing in existing_rows {
        if assignment_values.touches_descriptor() {
            let next_name = assignment_values
                .name
                .as_ref()
                .cloned()
                .unwrap_or(existing.name.clone());
            if next_name.is_empty() {
                return Err(LixError {
                    message: "lix_version update cannot set empty name".to_string(),
                });
            }
            let next_inherits = assignment_values
                .inherits_from_version_id
                .clone()
                .unwrap_or(existing.inherits_from_version_id.clone());
            let next_hidden = assignment_values.hidden.unwrap_or(existing.hidden);
            let snapshot = serde_json::from_str::<JsonValue>(&version_descriptor_snapshot_content(
                &existing.id,
                &next_name,
                next_inherits.as_deref(),
                next_hidden,
            ))
            .map_err(|error| LixError {
                message: format!("failed to encode updated version descriptor snapshot: {error}"),
            })?;
            descriptor_rows.push(InsertSnapshotRow {
                entity_id: existing.id.clone(),
                snapshot_content: Some(snapshot),
            });
        }

        if assignment_values.touches_tip() {
            let next_commit_id = assignment_values
                .commit_id
                .clone()
                .ok_or_else(|| LixError {
                    message:
                        "lix_version update must set both commit_id and working_commit_id together"
                            .to_string(),
                })?;
            if next_commit_id.is_empty() {
                return Err(LixError {
                    message: "lix_version update cannot set empty commit_id".to_string(),
                });
            }
            let next_working_commit_id =
                assignment_values
                    .working_commit_id
                    .clone()
                    .ok_or_else(|| {
                        LixError {
                    message:
                        "lix_version update must set both commit_id and working_commit_id together"
                            .to_string(),
                }
                    })?;
            if next_working_commit_id.is_empty() {
                return Err(LixError {
                    message: "lix_version update cannot set empty working_commit_id".to_string(),
                });
            }
            let snapshot = serde_json::from_str::<JsonValue>(&version_pointer_snapshot_content(
                &existing.id,
                &next_commit_id,
                &next_working_commit_id,
            ))
            .map_err(|error| LixError {
                message: format!("failed to encode updated version tip snapshot: {error}"),
            })?;
            tip_rows.push(InsertSnapshotRow {
                entity_id: existing.id,
                snapshot_content: Some(snapshot),
            });
        }
    }

    Ok(Some(build_vtable_inserts(descriptor_rows, tip_rows)?))
}

pub async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[EngineValue],
) -> Result<Option<Vec<Insert>>, LixError> {
    if !delete_from_is_lix_version(&delete) {
        return Ok(None);
    }
    if delete.using.is_some() {
        return Err(LixError {
            message: "lix_version delete does not support USING".to_string(),
        });
    }
    if delete.returning.is_some() {
        return Err(LixError {
            message: "lix_version delete does not support RETURNING".to_string(),
        });
    }
    if delete.limit.is_some() || !delete.order_by.is_empty() {
        return Err(LixError {
            message: "lix_version delete does not support LIMIT or ORDER BY".to_string(),
        });
    }

    let existing_rows = query_lix_version_rows(
        backend,
        delete.selection.as_ref(),
        params,
        PlaceholderState::new(),
    )
    .await?;
    if existing_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut descriptor_rows = Vec::new();
    let mut tip_rows = Vec::new();
    for row in existing_rows {
        descriptor_rows.push(InsertSnapshotRow {
            entity_id: row.id.clone(),
            snapshot_content: None,
        });
        tip_rows.push(InsertSnapshotRow {
            entity_id: row.id,
            snapshot_content: None,
        });
    }

    Ok(Some(build_vtable_inserts(descriptor_rows, tip_rows)?))
}

#[derive(Debug, Clone)]
struct InsertSnapshotRow {
    entity_id: String,
    snapshot_content: Option<JsonValue>,
}

#[derive(Debug, Clone)]
struct VersionRow {
    id: String,
    name: String,
    inherits_from_version_id: Option<String>,
    hidden: bool,
}

#[derive(Debug, Clone, Default)]
struct VersionAssignments {
    name: Option<String>,
    inherits_from_version_id: Option<Option<String>>,
    hidden: Option<bool>,
    commit_id: Option<String>,
    working_commit_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedInsertRow {
    entity_id: String,
    working_commit_id: String,
    descriptor_row: InsertSnapshotRow,
    tip_row: InsertSnapshotRow,
}

impl VersionAssignments {
    fn touches_descriptor(&self) -> bool {
        self.name.is_some() || self.inherits_from_version_id.is_some() || self.hidden.is_some()
    }

    fn touches_tip(&self) -> bool {
        self.commit_id.is_some() || self.working_commit_id.is_some()
    }
}

fn parse_update_assignments(
    assignments: &[Assignment],
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
) -> Result<VersionAssignments, LixError> {
    let mut parsed = VersionAssignments::default();
    for assignment in assignments {
        let column = assignment_target_column(&assignment.target).ok_or_else(|| LixError {
            message: "lix_version update requires single-column assignments".to_string(),
        })?;
        let resolved = resolve_expr_cell_with_state(&assignment.value, params, placeholder_state)?;
        let value = resolved.value.ok_or_else(|| LixError {
            message: format!(
                "lix_version update assignment for '{column}' must be literal or parameter"
            ),
        })?;

        match column.as_str() {
            "id" => {
                return Err(LixError {
                    message: "lix_version update cannot modify id".to_string(),
                });
            }
            "name" => {
                parsed.name = Some(value_required_string(&value, "name")?);
            }
            "inherits_from_version_id" => {
                parsed.inherits_from_version_id =
                    Some(value_optional_string(&value, "inherits_from_version_id")?);
            }
            "hidden" => {
                parsed.hidden = Some(value_bool(&value, "hidden")?);
            }
            "commit_id" => {
                parsed.commit_id = Some(value_required_string(&value, "commit_id")?);
            }
            "working_commit_id" => {
                parsed.working_commit_id =
                    Some(value_required_string(&value, "working_commit_id")?);
            }
            _ => {
                return Err(LixError {
                    message: format!("lix_version update does not support column '{column}'"),
                });
            }
        }
    }

    if parsed.commit_id.is_some() ^ parsed.working_commit_id.is_some() {
        return Err(LixError {
            message: "lix_version update must set both commit_id and working_commit_id together"
                .to_string(),
        });
    }

    Ok(parsed)
}

fn parse_insert_rows(
    field_map: &BTreeMap<String, usize>,
    rows: Vec<Vec<Expr>>,
    resolved_rows: Vec<Vec<crate::sql::ResolvedCell>>,
    operation_name: &str,
) -> Result<Vec<ParsedInsertRow>, LixError> {
    let mut parsed_rows = Vec::new();
    for (row, resolved_row) in rows.iter().zip(resolved_rows.iter()) {
        let id = field_required_string(field_map.get("id"), resolved_row, row, "id")?;
        let name = field_required_string(field_map.get("name"), resolved_row, row, "name")?;
        let inherits_from_version_id = field_optional_string(
            field_map.get("inherits_from_version_id"),
            resolved_row,
            row,
            "inherits_from_version_id",
        )?;
        let hidden = field_optional_bool(field_map.get("hidden"), resolved_row, row, "hidden")?
            .unwrap_or(false);

        let commit_id =
            field_required_string(field_map.get("commit_id"), resolved_row, row, "commit_id")?;
        let working_commit_id = field_required_string(
            field_map.get("working_commit_id"),
            resolved_row,
            row,
            "working_commit_id",
        )?;

        if id.is_empty() {
            return Err(LixError {
                message: format!("{operation_name} field 'id' cannot be empty"),
            });
        }
        if commit_id.is_empty() {
            return Err(LixError {
                message: format!("{operation_name} field 'commit_id' cannot be empty"),
            });
        }
        if working_commit_id.is_empty() {
            return Err(LixError {
                message: format!("{operation_name} field 'working_commit_id' cannot be empty"),
            });
        }

        let descriptor_snapshot =
            serde_json::from_str::<JsonValue>(&version_descriptor_snapshot_content(
                &id,
                &name,
                inherits_from_version_id.as_deref(),
                hidden,
            ))
            .map_err(|error| LixError {
                message: format!("failed to encode version descriptor snapshot: {error}"),
            })?;
        let tip_snapshot = serde_json::from_str::<JsonValue>(&version_pointer_snapshot_content(
            &id,
            &commit_id,
            &working_commit_id,
        ))
        .map_err(|error| LixError {
            message: format!("failed to encode version tip snapshot: {error}"),
        })?;

        parsed_rows.push(ParsedInsertRow {
            entity_id: id.clone(),
            working_commit_id,
            descriptor_row: InsertSnapshotRow {
                entity_id: id.clone(),
                snapshot_content: Some(descriptor_snapshot),
            },
            tip_row: InsertSnapshotRow {
                entity_id: id,
                snapshot_content: Some(tip_snapshot),
            },
        });
    }
    Ok(parsed_rows)
}

async fn validate_tip_working_commit_uniqueness(
    backend: &dyn LixBackend,
    parsed_rows: &[ParsedInsertRow],
) -> Result<(), LixError> {
    let mut incoming_working_to_entity = HashMap::<String, String>::new();
    for row in parsed_rows {
        if let Some(existing_entity_id) =
            incoming_working_to_entity.insert(row.working_commit_id.clone(), row.entity_id.clone())
        {
            if existing_entity_id != row.entity_id {
                return Err(LixError {
                    message: format!(
                        "Unique constraint violation: working_commit_id '{}' already used by version '{}'",
                        row.working_commit_id, existing_entity_id
                    ),
                });
            }
        }
    }

    if incoming_working_to_entity.is_empty() {
        return Ok(());
    }

    let result = backend
        .execute(
            &format!(
                "SELECT entity_id, snapshot_content \
                 FROM {table_name} \
                 WHERE schema_key = $1 \
                   AND version_id = $2 \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL",
                table_name = VERSION_POINTER_TABLE
            ),
            &[
                EngineValue::Text(version_pointer_schema_key().to_string()),
                EngineValue::Text(version_pointer_storage_version_id().to_string()),
            ],
        )
        .await?;

    let mut seen = HashSet::<String>::new();
    for row in result.rows {
        if row.len() < 2 {
            continue;
        }
        let entity_id = match &row[0] {
            EngineValue::Text(value) => value,
            _ => continue,
        };
        let snapshot_content = match &row[1] {
            EngineValue::Text(value) => value,
            _ => continue,
        };
        let snapshot: JsonValue = match serde_json::from_str(snapshot_content) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(working_commit_id) = snapshot
            .get("working_commit_id")
            .and_then(JsonValue::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        if !seen.insert(working_commit_id.clone()) {
            // Keep behavior stable when prior data already violates uniqueness.
            continue;
        }
        if let Some(incoming_entity_id) = incoming_working_to_entity.get(&working_commit_id) {
            if incoming_entity_id != entity_id {
                return Err(LixError {
                    message: format!(
                        "Unique constraint violation: working_commit_id '{}' already used by version '{}'",
                        working_commit_id, entity_id
                    ),
                });
            }
        }
    }

    Ok(())
}

async fn query_lix_version_rows(
    backend: &dyn LixBackend,
    selection: Option<&Expr>,
    params: &[EngineValue],
    placeholder_state: PlaceholderState,
) -> Result<Vec<VersionRow>, LixError> {
    let where_sql = selection
        .map(|expr| format!(" WHERE {expr}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT \
         id, name, inherits_from_version_id, hidden, commit_id, working_commit_id \
         FROM {view}{where_sql}",
        view = LIX_VERSION_VIEW_NAME
    );

    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement while querying lix_version rows"
                .to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            message: "version row loader query must be SELECT".to_string(),
        });
    };

    let query = *query;
    let query = lix_version_view_read::rewrite_query(query.clone())?.unwrap_or(query);
    let query = vtable_read::rewrite_query(query.clone())?.unwrap_or(query);
    let lowered = lower_statement(Statement::Query(Box::new(query)), backend.dialect())?;
    let bound = bind_sql_with_state(
        &lowered.to_string(),
        params,
        backend.dialect(),
        placeholder_state,
    )?;
    let result = backend.execute(&bound.sql, &bound.params).await?;

    let mut rows = Vec::new();
    for row in result.rows {
        if row.len() < 6 {
            return Err(LixError {
                message: "lix_version rewrite expected 6 columns from row loader query".to_string(),
            });
        }
        let id = value_required_string(&row[0], "id")?;
        let name = value_required_string(&row[1], "name")?;
        let inherits_from_version_id = value_optional_string(&row[2], "inherits_from_version_id")?;
        let hidden = value_bool(&row[3], "hidden")?;
        let _commit_id = value_required_string(&row[4], "commit_id")?;
        let _working_commit_id = value_required_string(&row[5], "working_commit_id")?;

        rows.push(VersionRow {
            id,
            name,
            inherits_from_version_id,
            hidden,
        });
    }

    Ok(rows)
}

fn build_vtable_inserts(
    descriptor_rows: Vec<InsertSnapshotRow>,
    tip_rows: Vec<InsertSnapshotRow>,
) -> Result<Vec<Insert>, LixError> {
    let mut inserts = Vec::new();
    if !descriptor_rows.is_empty() {
        inserts.push(build_vtable_insert_for_schema(
            version_descriptor_schema_key(),
            version_descriptor_file_id(),
            version_descriptor_storage_version_id(),
            version_descriptor_plugin_key(),
            version_descriptor_schema_version(),
            &descriptor_rows,
        )?);
    }
    if !tip_rows.is_empty() {
        inserts.push(build_vtable_insert_for_schema(
            version_pointer_schema_key(),
            version_pointer_file_id(),
            version_pointer_storage_version_id(),
            version_pointer_plugin_key(),
            version_pointer_schema_version(),
            &tip_rows,
        )?);
    }
    Ok(inserts)
}

fn build_vtable_insert_for_schema(
    schema_key: &str,
    file_id: &str,
    version_id: &str,
    plugin_key: &str,
    schema_version: &str,
    rows: &[InsertSnapshotRow],
) -> Result<Insert, LixError> {
    let values_sql = rows
        .iter()
        .map(|row| {
            let snapshot_sql = row
                .snapshot_content
                .as_ref()
                .map(|value| format!("'{}'", escape_sql_string(&value.to_string())))
                .unwrap_or_else(|| "NULL".to_string());
            format!(
                "('{entity_id}', '{schema_key}', '{file_id}', '{version_id}', '{plugin_key}', {snapshot}, '{schema_version}')",
                entity_id = escape_sql_string(&row.entity_id),
                schema_key = escape_sql_string(schema_key),
                file_id = escape_sql_string(file_id),
                version_id = escape_sql_string(version_id),
                plugin_key = escape_sql_string(plugin_key),
                snapshot = snapshot_sql,
                schema_version = escape_sql_string(schema_version),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT INTO {vtable} (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES {values}",
        vtable = VTABLE_NAME,
        values = values_sql
    );

    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "failed to build vtable insert for lix_version rewrite".to_string(),
        });
    }
    let Statement::Insert(insert) = statements.remove(0) else {
        return Err(LixError {
            message: "lix_version rewrite expected generated INSERT statement".to_string(),
        });
    };

    Ok(insert)
}

fn insert_field_map(columns: &[Ident]) -> Result<BTreeMap<String, usize>, LixError> {
    let allowed: BTreeSet<&str> = BTreeSet::from([
        "id",
        "name",
        "inherits_from_version_id",
        "hidden",
        "commit_id",
        "working_commit_id",
    ]);
    let mut map = BTreeMap::new();
    for (index, column) in columns.iter().enumerate() {
        let key = column.value.to_ascii_lowercase();
        if !allowed.contains(key.as_str()) {
            return Err(LixError {
                message: format!(
                    "lix_version insert does not support column '{}'",
                    column.value
                ),
            });
        }
        if map.insert(key.clone(), index).is_some() {
            return Err(LixError {
                message: format!("lix_version insert duplicated column '{}'", column.value),
            });
        }
    }
    Ok(map)
}

fn field_required_string(
    index: Option<&usize>,
    resolved_row: &[crate::sql::ResolvedCell],
    original_row: &[Expr],
    field: &str,
) -> Result<String, LixError> {
    let Some(index) = index else {
        return Err(LixError {
            message: format!("lix_version insert requires column '{field}'"),
        });
    };
    let value = resolved_row
        .get(*index)
        .and_then(|cell| cell.value.as_ref())
        .ok_or_else(|| LixError {
            message: format!("lix_version insert '{field}' must be literal or parameter"),
        })?;
    value_required_string(value, field).map_err(|error| {
        if matches!(original_row.get(*index), Some(Expr::Value(_))) {
            error
        } else {
            LixError {
                message: format!("lix_version insert '{field}' must be literal or parameter"),
            }
        }
    })
}

fn field_optional_string(
    index: Option<&usize>,
    resolved_row: &[crate::sql::ResolvedCell],
    _original_row: &[Expr],
    field: &str,
) -> Result<Option<String>, LixError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let value = resolved_row
        .get(*index)
        .and_then(|cell| cell.value.as_ref())
        .ok_or_else(|| LixError {
            message: format!("lix_version insert '{field}' must be literal or parameter"),
        })?;
    value_optional_string(value, field)
}

fn field_optional_bool(
    index: Option<&usize>,
    resolved_row: &[crate::sql::ResolvedCell],
    _original_row: &[Expr],
    field: &str,
) -> Result<Option<bool>, LixError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let value = resolved_row
        .get(*index)
        .and_then(|cell| cell.value.as_ref())
        .ok_or_else(|| LixError {
            message: format!("lix_version insert '{field}' must be literal or parameter"),
        })?;
    Ok(Some(value_bool(value, field)?))
}

fn value_required_string(value: &EngineValue, field: &str) -> Result<String, LixError> {
    match value {
        EngineValue::Text(text) => Ok(text.clone()),
        EngineValue::Null => Err(LixError {
            message: format!("lix_version field '{field}' cannot be NULL"),
        }),
        _ => Err(LixError {
            message: format!("lix_version field '{field}' must be a string"),
        }),
    }
}

fn value_optional_string(value: &EngineValue, field: &str) -> Result<Option<String>, LixError> {
    match value {
        EngineValue::Text(text) => Ok(Some(text.clone())),
        EngineValue::Null => Ok(None),
        _ => Err(LixError {
            message: format!("lix_version field '{field}' must be a string or NULL"),
        }),
    }
}

fn value_bool(value: &EngineValue, field: &str) -> Result<bool, LixError> {
    match value {
        EngineValue::Integer(number) => Ok(*number != 0),
        EngineValue::Real(number) => Ok(*number != 0.0),
        EngineValue::Text(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" => Ok(true),
                "0" | "false" => Ok(false),
                _ => Err(LixError {
                    message: format!(
                        "lix_version field '{field}' must be boolean-compatible, got '{text}'"
                    ),
                }),
            }
        }
        EngineValue::Null => Ok(false),
        _ => Err(LixError {
            message: format!("lix_version field '{field}' must be boolean-compatible"),
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

fn table_object_is_lix_version(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, LIX_VERSION_VIEW_NAME),
        _ => false,
    }
}

fn table_with_joins_is_lix_version(table: &TableWithJoins) -> bool {
    table.joins.is_empty()
        && matches!(
            &table.relation,
            TableFactor::Table { name, .. } if object_name_matches(name, LIX_VERSION_VIEW_NAME)
        )
}

fn delete_from_is_lix_version(delete: &Delete) -> bool {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return false;
            }
            table_with_joins_is_lix_version(&tables[0])
        }
    }
}
