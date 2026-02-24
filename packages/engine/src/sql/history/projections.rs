use crate::version::{
    version_pointer_file_id, version_pointer_schema_key, version_pointer_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::working_projection::WORKING_PROJECTION_METADATA;
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub(crate) async fn refresh_working_projection_for_read_query(
    backend: &dyn LixBackend,
    active_version_id: &str,
) -> Result<(), LixError> {
    match refresh_working_change_projection_with_backend(backend, active_version_id).await {
        Ok(()) => Ok(()),
        Err(error) if is_missing_internal_relation_error(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn refresh_working_change_projection_with_backend(
    backend: &dyn LixBackend,
    active_version_id: &str,
) -> Result<(), LixError> {
    let version_pointer_untracked = backend
        .execute(
            "SELECT \
               snapshot_content \
             FROM lix_internal_state_untracked \
             WHERE schema_key = $1 \
               AND entity_id = $2 \
               AND file_id = $3 \
               AND version_id = $4 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[
                Value::Text(version_pointer_schema_key().to_string()),
                Value::Text(active_version_id.to_string()),
                Value::Text(version_pointer_file_id().to_string()),
                Value::Text(version_pointer_storage_version_id().to_string()),
            ],
        )
        .await?;
    let working_commit_id =
        first_row_json_text_field(&version_pointer_untracked, "working_commit_id")?;
    let tip_commit_id = first_row_json_text_field(&version_pointer_untracked, "commit_id")?;
    let (working_commit_id, tip_commit_id) = if let (Some(working_commit_id), Some(tip_commit_id)) =
        (working_commit_id, tip_commit_id)
    {
        (working_commit_id, tip_commit_id)
    } else {
        let version_pointer_table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_pointer_schema_key()
        );
        let version_pointer_sql = format!(
            "SELECT \
                   snapshot_content \
                 FROM {table} \
                 WHERE schema_key = '{schema_key}' \
                   AND entity_id = $1 \
                   AND file_id = '{file_id}' \
                   AND version_id = '{version_id}' \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
            table = version_pointer_table,
            schema_key = escape_sql_string(version_pointer_schema_key()),
            file_id = escape_sql_string(version_pointer_file_id()),
            version_id = escape_sql_string(version_pointer_storage_version_id()),
        );
        let version_pointer = backend
            .execute(
                &version_pointer_sql,
                &[Value::Text(active_version_id.to_string())],
            )
            .await?;
        let Some(working_commit_id) =
            first_row_json_text_field(&version_pointer, "working_commit_id")?
        else {
            return Ok(());
        };
        let Some(tip_commit_id) = first_row_json_text_field(&version_pointer, "commit_id")? else {
            return Ok(());
        };
        (working_commit_id, tip_commit_id)
    };

    let change_set_untracked = backend
        .execute(
            "SELECT \
               snapshot_content \
             FROM lix_internal_state_untracked \
             WHERE schema_key = 'lix_commit' \
               AND entity_id = $1 \
               AND file_id = 'lix' \
               AND version_id = $2 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[
                Value::Text(working_commit_id.clone()),
                Value::Text(GLOBAL_VERSION_ID.to_string()),
            ],
        )
        .await?;
    let mut working_commit_snapshot_raw = first_row_text(&change_set_untracked)?;
    let working_change_set_id = first_row_json_text_field(&change_set_untracked, "change_set_id")?;
    let working_change_set_id = if let Some(working_change_set_id) = working_change_set_id {
        working_change_set_id
    } else {
        let commit_table = "lix_internal_state_materialized_v1_lix_commit";
        let change_set_sql = format!(
            "SELECT \
               snapshot_content \
             FROM {table} \
             WHERE schema_key = 'lix_commit' \
               AND entity_id = $1 \
               AND file_id = 'lix' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table = commit_table,
            version_id = escape_sql_string(GLOBAL_VERSION_ID),
        );
        let change_set_row = backend
            .execute(&change_set_sql, &[Value::Text(working_commit_id.clone())])
            .await?;
        working_commit_snapshot_raw = first_row_text(&change_set_row)?;
        let Some(working_change_set_id) =
            first_row_json_text_field(&change_set_row, "change_set_id")?
        else {
            return Ok(());
        };
        working_change_set_id
    };

    backend
        .execute(
            "DELETE FROM lix_internal_state_untracked \
             WHERE metadata = $1 \
               AND ( \
                 (schema_key = 'lix_change_set_element' AND entity_id LIKE $2) \
                 OR (schema_key = 'lix_change' AND entity_id LIKE $3) \
               )",
            &[
                Value::Text(WORKING_PROJECTION_METADATA.to_string()),
                Value::Text(format!("{working_change_set_id}~%")),
                Value::Text(format!("working_projection:%:{working_change_set_id}:%")),
            ],
        )
        .await?;

    let commit_rows = backend
        .execute(
            "SELECT \
               snapshot_content \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await?;
    let mut change_set_by_commit_id = BTreeMap::new();
    for row in &commit_rows.rows {
        let raw = text_column(row, 0, "snapshot_content")?;
        if let Some((commit_id, change_set_id)) = parse_commit_snapshot(&raw)? {
            change_set_by_commit_id.insert(commit_id, change_set_id);
        }
    }
    if change_set_by_commit_id.is_empty() {
        return Ok(());
    }

    let edge_rows = backend
        .execute(
            "SELECT \
               snapshot_content \
             FROM lix_internal_state_materialized_v1_lix_commit_edge \
             WHERE schema_key = 'lix_commit_edge' \
               AND file_id = 'lix' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await?;
    let mut parents_by_child: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in &edge_rows.rows {
        let raw = text_column(row, 0, "snapshot_content")?;
        if let Some((parent_id, child_id)) = parse_commit_edge_snapshot(&raw)? {
            parents_by_child
                .entry(child_id)
                .or_default()
                .push(parent_id);
        }
    }

    let baseline_commit_id = parents_by_child
        .get(&working_commit_id)
        .and_then(|parents| parents.first())
        .cloned()
        .unwrap_or_else(|| working_commit_id.clone());

    let mut depth_by_commit_id: BTreeMap<String, usize> = BTreeMap::new();
    let mut queue = VecDeque::new();
    queue.push_back((tip_commit_id.clone(), 0usize));
    while let Some((commit_id, depth)) = queue.pop_front() {
        if commit_id == baseline_commit_id {
            continue;
        }
        if let Some(existing_depth) = depth_by_commit_id.get(&commit_id) {
            if *existing_depth <= depth {
                continue;
            }
        }
        depth_by_commit_id.insert(commit_id.clone(), depth);
        if let Some(parents) = parents_by_child.get(&commit_id) {
            for parent_id in parents {
                queue.push_back((parent_id.clone(), depth + 1));
            }
        }
    }
    if depth_by_commit_id.is_empty() {
        return Ok(());
    }

    let cse_rows = backend
        .execute(
            "SELECT \
               snapshot_content, created_at \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND file_id = 'lix' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await?;
    let mut cse_by_change_set_id: BTreeMap<String, Vec<WorkingProjectionChangeSetElement>> =
        BTreeMap::new();
    for row in &cse_rows.rows {
        let raw = text_column(row, 0, "snapshot_content")?;
        let created_at = text_column(row, 1, "created_at")?;
        if let Some(cse) = parse_change_set_element_snapshot(&raw, created_at)? {
            cse_by_change_set_id
                .entry(cse.change_set_id.clone())
                .or_default()
                .push(cse);
        }
    }
    if cse_by_change_set_id.is_empty() {
        return Ok(());
    }

    let mut selected_by_entity: BTreeMap<
        (String, String, String),
        WorkingProjectionSelectedChange,
    > = BTreeMap::new();
    for (commit_id, depth) in &depth_by_commit_id {
        let Some(change_set_id) = change_set_by_commit_id.get(commit_id) else {
            continue;
        };
        let Some(cse_rows) = cse_by_change_set_id.get(change_set_id) else {
            continue;
        };
        for cse in cse_rows {
            if cse.change_id.starts_with("working_projection:") {
                continue;
            }
            let key = (
                cse.entity_id.clone(),
                cse.schema_key.clone(),
                cse.file_id.clone(),
            );
            let next = WorkingProjectionSelectedChange {
                change_id: cse.change_id.clone(),
                depth: *depth,
                created_at: cse.created_at.clone(),
            };
            match selected_by_entity.get(&key) {
                None => {
                    selected_by_entity.insert(key, next);
                }
                Some(existing) => {
                    if next.depth < existing.depth
                        || (next.depth == existing.depth
                            && next.created_at.as_str() > existing.created_at.as_str())
                    {
                        selected_by_entity.insert(key, next);
                    }
                }
            }
        }
    }
    if let Some(cse_rows) = cse_by_change_set_id.get(&working_change_set_id) {
        for cse in cse_rows {
            if cse.change_id.starts_with("working_projection:") {
                continue;
            }
            let key = (
                cse.entity_id.clone(),
                cse.schema_key.clone(),
                cse.file_id.clone(),
            );
            let next = WorkingProjectionSelectedChange {
                change_id: cse.change_id.clone(),
                depth: 0,
                created_at: cse.created_at.clone(),
            };
            match selected_by_entity.get(&key) {
                None => {
                    selected_by_entity.insert(key, next);
                }
                Some(existing) => {
                    if next.depth < existing.depth
                        || (next.depth == existing.depth
                            && next.created_at.as_str() > existing.created_at.as_str())
                    {
                        selected_by_entity.insert(key, next);
                    }
                }
            }
        }
    }
    if selected_by_entity.is_empty() {
        return Ok(());
    }

    let mut projected_change_ids = Vec::new();
    let mut projected_change_set = BTreeSet::new();
    for selected in selected_by_entity.values() {
        if projected_change_set.insert(selected.change_id.clone()) {
            projected_change_ids.push(selected.change_id.clone());
        }
    }
    let projection_updated_at = selected_by_entity
        .values()
        .map(|selected| selected.created_at.as_str())
        .max()
        .unwrap_or("1970-01-01T00:00:00.000Z")
        .to_string();
    let working_commit_snapshot = build_working_commit_projection_snapshot(
        working_commit_snapshot_raw.as_deref(),
        &working_commit_id,
        &working_change_set_id,
        &projected_change_ids,
    )?;
    upsert_working_projection_row(
        backend,
        &working_commit_id,
        "lix_commit",
        GLOBAL_VERSION_ID,
        "lix",
        &working_commit_snapshot,
        "1",
        &projection_updated_at,
    )
    .await?;

    let mut placeholders = Vec::new();
    let mut change_params = Vec::new();
    for (index, change_id) in projected_change_ids.iter().enumerate() {
        placeholders.push(format!("${}", index + 1));
        change_params.push(Value::Text(change_id.clone()));
    }
    let change_sql = format!(
        "SELECT \
           c.id, c.schema_version, c.plugin_key, c.created_at, c.metadata, s.content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s \
           ON s.id = c.snapshot_id \
         WHERE c.id IN ({})",
        placeholders.join(", ")
    );
    let change_rows = backend.execute(&change_sql, &change_params).await?;
    let mut change_by_id = BTreeMap::new();
    for row in &change_rows.rows {
        let id = text_column(row, 0, "id")?;
        let schema_version = text_column(row, 1, "schema_version")?;
        let plugin_key = text_column(row, 2, "plugin_key")?;
        let created_at = text_column(row, 3, "created_at")?;
        let metadata = optional_json_column(row.get(4), "metadata")?;
        let snapshot_content = optional_json_column(row.get(5), "snapshot_content")?;
        change_by_id.insert(
            id,
            WorkingProjectionChangeRow {
                schema_version,
                plugin_key,
                created_at,
                metadata,
                snapshot_content,
            },
        );
    }

    for ((entity_id, schema_key, file_id), selected) in &selected_by_entity {
        let Some(change_row) = change_by_id.get(&selected.change_id) else {
            continue;
        };

        let change_id = build_working_projection_change_id(
            active_version_id,
            &working_change_set_id,
            entity_id,
            schema_key,
            file_id,
        );
        let change_snapshot = serde_json::json!({
            "id": change_id,
            "entity_id": entity_id,
            "schema_key": schema_key,
            "schema_version": change_row.schema_version,
            "file_id": file_id,
            "plugin_key": change_row.plugin_key,
            "created_at": change_row.created_at,
            "snapshot_content": change_row.snapshot_content,
            "metadata": change_row.metadata,
        })
        .to_string();
        upsert_working_projection_row(
            backend,
            &change_id,
            "lix_change",
            GLOBAL_VERSION_ID,
            "lix",
            &change_snapshot,
            "1",
            &change_row.created_at,
        )
        .await?;

        let cse_entity_id = format!("{working_change_set_id}~{change_id}");
        let cse_snapshot = serde_json::json!({
            "change_set_id": working_change_set_id,
            "change_id": change_id,
            "entity_id": entity_id,
            "schema_key": schema_key,
            "file_id": file_id,
        })
        .to_string();
        upsert_working_projection_row(
            backend,
            &cse_entity_id,
            "lix_change_set_element",
            GLOBAL_VERSION_ID,
            "lix",
            &cse_snapshot,
            "1",
            &change_row.created_at,
        )
        .await?;
    }

    Ok(())
}

fn text_column(row: &[Value], index: usize, name: &str) -> Result<String, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            message: format!("working projection row is missing '{name}'"),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            message: format!("working projection '{name}' must be text, got {other:?}"),
        }),
    }
}

fn first_row_json_text_field(
    result: &QueryResult,
    field: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(value) = row.first() else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(raw) => parse_json_text_field(raw, field),
        other => Err(LixError {
            message: format!(
                "working projection expected JSON snapshot text for '{field}', got {other:?}"
            ),
        }),
    }
}

fn first_row_text(result: &QueryResult) -> Result<Option<String>, LixError> {
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(value) = row.first() else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Text(raw) => Ok(Some(raw.clone())),
        other => Err(LixError {
            message: format!("working projection expected JSON snapshot text, got {other:?}"),
        }),
    }
}

fn build_working_commit_projection_snapshot(
    raw_snapshot: Option<&str>,
    working_commit_id: &str,
    working_change_set_id: &str,
    projected_change_ids: &[String],
) -> Result<String, LixError> {
    let mut snapshot = if let Some(raw_snapshot) = raw_snapshot {
        serde_json::from_str::<JsonValue>(raw_snapshot).map_err(|error| LixError {
            message: format!("working projection commit snapshot invalid JSON: {error}"),
        })?
    } else {
        JsonValue::Object(serde_json::Map::new())
    };
    let Some(object) = snapshot.as_object_mut() else {
        return Err(LixError {
            message: "working projection commit snapshot must be a JSON object".to_string(),
        });
    };

    object.insert(
        "id".to_string(),
        JsonValue::String(working_commit_id.to_string()),
    );
    object.insert(
        "change_set_id".to_string(),
        JsonValue::String(working_change_set_id.to_string()),
    );
    object.insert(
        "change_ids".to_string(),
        JsonValue::Array(
            projected_change_ids
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );
    object
        .entry("parent_commit_ids".to_string())
        .or_insert_with(|| JsonValue::Array(Vec::new()));
    object
        .entry("author_account_ids".to_string())
        .or_insert_with(|| JsonValue::Array(Vec::new()));
    object
        .entry("meta_change_ids".to_string())
        .or_insert_with(|| JsonValue::Array(Vec::new()));

    Ok(snapshot.to_string())
}

fn parse_json_text_field(raw: &str, field: &str) -> Result<Option<String>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("working projection snapshot_content invalid JSON: {error}"),
    })?;
    let value = parsed
        .get(field)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    Ok(value)
}

#[derive(Debug, Clone)]
struct WorkingProjectionChangeSetElement {
    change_set_id: String,
    change_id: String,
    entity_id: String,
    schema_key: String,
    file_id: String,
    created_at: String,
}

#[derive(Debug, Clone)]
struct WorkingProjectionSelectedChange {
    change_id: String,
    depth: usize,
    created_at: String,
}

#[derive(Debug, Clone)]
struct WorkingProjectionChangeRow {
    schema_version: String,
    plugin_key: String,
    created_at: String,
    metadata: JsonValue,
    snapshot_content: JsonValue,
}

fn optional_json_column(value: Option<&Value>, name: &str) -> Result<JsonValue, LixError> {
    let Some(value) = value else {
        return Ok(JsonValue::Null);
    };
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Text(raw) => serde_json::from_str(raw).map_err(|error| LixError {
            message: format!("working projection '{name}' invalid JSON: {error}"),
        }),
        other => Err(LixError {
            message: format!(
                "working projection '{name}' must be JSON text or null, got {other:?}"
            ),
        }),
    }
}

fn parse_commit_snapshot(raw: &str) -> Result<Option<(String, String)>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("working projection commit snapshot invalid JSON: {error}"),
    })?;
    let commit_id = parsed
        .get("id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    let change_set_id = parsed
        .get("change_set_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    match (commit_id, change_set_id) {
        (Some(commit_id), Some(change_set_id)) => Ok(Some((commit_id, change_set_id))),
        _ => Ok(None),
    }
}

fn parse_commit_edge_snapshot(raw: &str) -> Result<Option<(String, String)>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("working projection commit_edge snapshot invalid JSON: {error}"),
    })?;
    let parent_id = parsed
        .get("parent_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    let child_id = parsed
        .get("child_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    match (parent_id, child_id) {
        (Some(parent_id), Some(child_id)) => Ok(Some((parent_id, child_id))),
        _ => Ok(None),
    }
}

fn parse_change_set_element_snapshot(
    raw: &str,
    created_at: String,
) -> Result<Option<WorkingProjectionChangeSetElement>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("working projection change_set_element snapshot invalid JSON: {error}"),
    })?;
    let change_set_id = parsed
        .get("change_set_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    let change_id = parsed
        .get("change_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    let entity_id = parsed
        .get("entity_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string);
    let schema_key = parsed
        .get("schema_key")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    let file_id = parsed
        .get("file_id")
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty());
    match (change_set_id, change_id, entity_id, schema_key, file_id) {
        (
            Some(change_set_id),
            Some(change_id),
            Some(entity_id),
            Some(schema_key),
            Some(file_id),
        ) => Ok(Some(WorkingProjectionChangeSetElement {
            change_set_id,
            change_id,
            entity_id,
            schema_key,
            file_id,
            created_at,
        })),
        _ => Ok(None),
    }
}

fn build_working_projection_change_id(
    active_version_id: &str,
    change_set_id: &str,
    entity_id: &str,
    schema_key: &str,
    file_id: &str,
) -> String {
    format!(
        "working_projection:{active_version_id}:{change_set_id}:{schema_key}:{file_id}:{entity_id}"
    )
}

fn is_missing_internal_relation_error(error: &LixError) -> bool {
    let message = error.message.to_ascii_lowercase();
    (message.contains("no such table") || message.contains("does not exist"))
        && (message.contains("lix_internal_state_materialized_v1_")
            || message.contains("lix_internal_state_untracked")
            || message.contains("lix_internal_state_vtable"))
}

async fn upsert_working_projection_row(
    backend: &dyn LixBackend,
    entity_id: &str,
    schema_key: &str,
    version_id: &str,
    plugin_key: &str,
    snapshot_content: &str,
    schema_version: &str,
    updated_at: &str,
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_state_untracked (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, created_at, updated_at\
             ) VALUES ($1, $2, 'lix', $3, $4, $5, $6, $7, $8, $8) \
             ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE SET \
             plugin_key = EXCLUDED.plugin_key, \
             snapshot_content = EXCLUDED.snapshot_content, \
             metadata = EXCLUDED.metadata, \
             schema_version = EXCLUDED.schema_version, \
             updated_at = EXCLUDED.updated_at",
            &[
                Value::Text(entity_id.to_string()),
                Value::Text(schema_key.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(plugin_key.to_string()),
                Value::Text(snapshot_content.to_string()),
                Value::Text(WORKING_PROJECTION_METADATA.to_string()),
                Value::Text(schema_version.to_string()),
                Value::Text(updated_at.to_string()),
            ],
        )
        .await?;
    Ok(())
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
