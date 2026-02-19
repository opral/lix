use crate::{Engine, EngineTransaction, ExecuteOptions, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateCheckpointResult {
    pub id: String,
    pub change_set_id: String,
}

pub async fn create_checkpoint(engine: &Engine) -> Result<CreateCheckpointResult, LixError> {
    engine
        .transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move { create_checkpoint_in_transaction(tx).await })
        })
        .await
}

async fn create_checkpoint_in_transaction(
    tx: &mut EngineTransaction<'_>,
) -> Result<CreateCheckpointResult, LixError> {
    let version_row = tx
        .execute(
            "SELECT av.version_id, v.commit_id, v.working_commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await?;
    let row = first_row(&version_row, "active version row")?;
    let version_id = text_at(row, 0, "version_id")?;
    let previous_tip_id = text_at(row, 1, "commit_id")?;
    let working_commit_id = text_at(row, 2, "working_commit_id")?;

    let working_commit = load_commit(tx, &working_commit_id)
        .await?
        .ok_or_else(|| LixError {
            message: format!("working commit '{working_commit_id}' is missing"),
        })?;
    let working_change_set_id = working_commit.change_set_id.clone();
    let mut merged_parents = working_commit.parent_commit_ids;
    if !merged_parents.iter().any(|id| id == &previous_tip_id) {
        merged_parents.push(previous_tip_id.clone());
    }
    let merged_parents = normalize_parent_commit_ids(merged_parents, working_commit_id.as_str());
    let merged_parents =
        filter_acyclic_parents(tx, working_commit_id.as_str(), merged_parents).await?;

    let has_working_elements =
        has_checkpointable_change_set_elements(tx, &working_change_set_id).await?;

    if !has_working_elements {
        if let Some(tip_commit) = load_commit(tx, &previous_tip_id).await? {
            return Ok(CreateCheckpointResult {
                id: previous_tip_id,
                change_set_id: tip_commit.change_set_id,
            });
        }
        return Ok(CreateCheckpointResult {
            id: previous_tip_id,
            change_set_id: working_change_set_id,
        });
    }

    let checkpoint_label_id = load_checkpoint_label_id(tx).await?;
    let checkpoint_snapshot = serde_json::json!({
        "id": working_commit_id.clone(),
        "change_set_id": working_change_set_id.clone(),
        "parent_commit_ids": merged_parents.clone(),
        "change_ids": [],
    })
    .to_string();
    tx.execute(
        "UPDATE lix_state_by_version \
         SET snapshot_content = $1 \
         WHERE entity_id = $2 \
           AND schema_key = 'lix_commit' \
           AND file_id = 'lix' \
           AND version_id = 'global'",
        &[
            Value::Text(checkpoint_snapshot),
            Value::Text(working_commit_id.clone()),
        ],
    )
    .await?;

    if merged_parents.iter().any(|id| id == &previous_tip_id) {
        ensure_commit_edge(tx, &previous_tip_id, &working_commit_id).await?;
    }

    let existing_label = tx
        .execute(
            "SELECT 1 \
             FROM lix_entity_label \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_commit' \
               AND file_id = 'lix' \
               AND label_id = $2 \
             LIMIT 1",
            &[
                Value::Text(working_commit_id.clone()),
                Value::Text(checkpoint_label_id.clone()),
            ],
        )
        .await?;
    if existing_label.rows.is_empty() {
        tx.execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ($1, 'lix_entity_label', 'lix', 'global', 'lix', $2, '1')",
            &[
                Value::Text(format!(
                    "{working_commit_id}~lix_commit~lix~{checkpoint_label_id}"
                )),
                Value::Text(
                    serde_json::json!({
                        "entity_id": working_commit_id.clone(),
                        "schema_key": "lix_commit",
                        "file_id": "lix",
                        "label_id": checkpoint_label_id.clone(),
                    })
                    .to_string(),
                ),
            ],
        )
        .await?;
    }

    let new_change_set_id = generate_uuid(tx).await?;
    let new_working_commit_id = generate_uuid(tx).await?;
    tx.execute(
        "INSERT INTO lix_change_set (id) VALUES ($1)",
        &[Value::Text(new_change_set_id.clone())],
    )
    .await?;

    tx.execute(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES ($1, 'lix_commit', 'lix', 'global', 'lix', $2, '1')",
        &[
            Value::Text(new_working_commit_id.clone()),
            Value::Text(
                serde_json::json!({
                    "id": new_working_commit_id.clone(),
                    "change_set_id": new_change_set_id.clone(),
                    "parent_commit_ids": [working_commit_id.clone()],
                    "change_ids": [],
                })
                .to_string(),
            ),
        ],
    )
    .await?;

    ensure_commit_edge(tx, &working_commit_id, &new_working_commit_id).await?;

    tx.execute(
        "UPDATE lix_version \
         SET commit_id = $1, working_commit_id = $2 \
         WHERE id = $3",
        &[
            Value::Text(working_commit_id.clone()),
            Value::Text(new_working_commit_id),
            Value::Text(version_id),
        ],
    )
    .await?;

    Ok(CreateCheckpointResult {
        id: working_commit_id,
        change_set_id: working_change_set_id,
    })
}

#[derive(Debug, Clone)]
struct CommitRow {
    change_set_id: String,
    parent_commit_ids: Vec<String>,
}

async fn load_commit(
    tx: &mut EngineTransaction<'_>,
    commit_id: &str,
) -> Result<Option<CommitRow>, LixError> {
    let result = tx
        .execute(
            "SELECT change_set_id, parent_commit_ids \
             FROM lix_commit \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(commit_id.to_string())],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let change_set_id = text_at(row, 0, "change_set_id")?;
    let parent_commit_ids = parse_parent_commit_ids(row.get(1), "parent_commit_ids")?;
    Ok(Some(CommitRow {
        change_set_id,
        parent_commit_ids,
    }))
}

async fn has_checkpointable_change_set_elements(
    tx: &mut EngineTransaction<'_>,
    change_set_id: &str,
) -> Result<bool, LixError> {
    let result = tx
        .execute(
            "SELECT 1 \
             FROM lix_change_set_element \
             WHERE change_set_id = $1 \
               AND schema_key NOT IN (\
                 'lix_version_pointer', \
                 'lix_commit', \
                 'lix_change_set_element', \
                 'lix_commit_edge', \
                 'lix_change_author', \
                 'lix_entity_label', \
                 'lix_version_descriptor'\
               ) \
             LIMIT 1",
            &[Value::Text(change_set_id.to_string())],
        )
        .await?;
    Ok(!result.rows.is_empty())
}

async fn load_checkpoint_label_id(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let result = tx
        .execute(
            "SELECT id FROM lix_label WHERE name = $1 LIMIT 1",
            &[Value::Text("checkpoint".to_string())],
        )
        .await?;
    let row = first_row(&result, "checkpoint label row")?;
    text_at(row, 0, "label_id")
}

async fn ensure_commit_edge(
    tx: &mut EngineTransaction<'_>,
    parent_id: &str,
    child_id: &str,
) -> Result<(), LixError> {
    if parent_id == child_id {
        return Ok(());
    }

    let exists = tx
        .execute(
            "SELECT 1 FROM lix_commit_edge WHERE parent_id = $1 AND child_id = $2 LIMIT 1",
            &[
                Value::Text(parent_id.to_string()),
                Value::Text(child_id.to_string()),
            ],
        )
        .await?;
    if !exists.rows.is_empty() {
        return Ok(());
    }

    tx.execute(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES ($1, 'lix_commit_edge', 'lix', 'global', 'lix', $2, '1')",
        &[
            Value::Text(format!("{parent_id}~{child_id}")),
            Value::Text(
                serde_json::json!({
                    "parent_id": parent_id,
                    "child_id": child_id,
                })
                .to_string(),
            ),
        ],
    )
    .await?;
    Ok(())
}

async fn filter_acyclic_parents(
    tx: &mut EngineTransaction<'_>,
    child_id: &str,
    parent_ids: Vec<String>,
) -> Result<Vec<String>, LixError> {
    let mut out = Vec::with_capacity(parent_ids.len());
    for parent_id in parent_ids {
        if !would_create_cycle(tx, &parent_id, child_id).await? {
            out.push(parent_id);
        }
    }
    Ok(out)
}

async fn would_create_cycle(
    tx: &mut EngineTransaction<'_>,
    parent_id: &str,
    child_id: &str,
) -> Result<bool, LixError> {
    if parent_id == child_id {
        return Ok(true);
    }

    let result = tx
        .execute(
            "WITH RECURSIVE reachable(id) AS ( \
               SELECT child_id \
               FROM lix_commit_edge \
               WHERE parent_id = $1 \
               UNION \
               SELECT ce.child_id \
               FROM lix_commit_edge ce \
               JOIN reachable r ON ce.parent_id = r.id \
             ) \
             SELECT 1 \
             FROM reachable \
             WHERE id = $2 \
             LIMIT 1",
            &[
                Value::Text(child_id.to_string()),
                Value::Text(parent_id.to_string()),
            ],
        )
        .await?;

    Ok(!result.rows.is_empty())
}

fn normalize_parent_commit_ids(
    mut parent_commit_ids: Vec<String>,
    self_commit_id: &str,
) -> Vec<String> {
    parent_commit_ids.retain(|id| !id.is_empty() && id != self_commit_id);
    parent_commit_ids.sort();
    parent_commit_ids.dedup();
    parent_commit_ids
}

async fn generate_uuid(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let result = tx.execute("SELECT lix_uuid_v7()", &[]).await?;
    let row = first_row(&result, "generated uuid row")?;
    text_at(row, 0, "lix_uuid_v7()")
}

fn first_row<'a>(result: &'a crate::QueryResult, label: &str) -> Result<&'a [Value], LixError> {
    result
        .rows
        .first()
        .map(|row| row.as_slice())
        .ok_or_else(|| LixError {
            message: format!("expected {label}, but query returned no rows"),
        })
}

fn text_at(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    let value = row.get(index).ok_or_else(|| LixError {
        message: format!("row is missing required field '{field}'"),
    })?;
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            message: format!("field '{field}' must be text, got {other:?}"),
        }),
    }
}

fn parse_parent_commit_ids(value: Option<&Value>, field: &str) -> Result<Vec<String>, LixError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Text(raw) if raw.trim().is_empty() => Ok(Vec::new()),
        Value::Text(raw) => {
            let parsed: serde_json::Value =
                serde_json::from_str(raw).map_err(|error| LixError {
                    message: format!("field '{field}' must be JSON array text: {error}"),
                })?;
            let array = parsed.as_array().ok_or_else(|| LixError {
                message: format!("field '{field}' must be a JSON array"),
            })?;
            let mut out = Vec::with_capacity(array.len());
            for item in array {
                let id = item.as_str().ok_or_else(|| LixError {
                    message: format!("field '{field}' must contain only string commit ids"),
                })?;
                out.push(id.to_string());
            }
            Ok(out)
        }
        other => Err(LixError {
            message: format!("field '{field}' must be text or null, got {other:?}"),
        }),
    }
}
