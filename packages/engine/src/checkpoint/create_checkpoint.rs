use crate::working_projection::WORKING_PROJECTION_METADATA;
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
    if working_change_set_id.trim().is_empty() {
        return Err(LixError {
            message: format!("working commit '{working_commit_id}' has empty change_set_id"),
        });
    }
    let working_change_ids = working_commit.change_ids.clone();
    let has_working_elements = !working_change_ids.is_empty();
    let effective_previous_tip_id = if has_working_elements {
        resolve_effective_previous_tip_id(
            tx,
            &previous_tip_id,
            &working_commit_id,
            &working_change_ids,
        )
        .await?
    } else {
        previous_tip_id.clone()
    };
    let mut merged_parents = working_commit.parent_commit_ids;
    if !merged_parents
        .iter()
        .any(|id| id == &effective_previous_tip_id)
    {
        merged_parents.push(effective_previous_tip_id.clone());
    }
    let merged_parents = normalize_parent_commit_ids(merged_parents, working_commit_id.as_str());

    if !has_working_elements {
        let tip_commit = load_commit(tx, &effective_previous_tip_id)
            .await?
            .ok_or_else(|| LixError {
                message: format!("tip commit '{effective_previous_tip_id}' is missing"),
            })?;
        return Ok(CreateCheckpointResult {
            id: effective_previous_tip_id,
            change_set_id: tip_commit.change_set_id,
        });
    }

    ensure_change_set_elements_for_checkpoint(tx, &working_change_set_id, &working_change_ids)
        .await?;

    let checkpoint_label_id = load_checkpoint_label_id(tx).await?;
    let mut checkpoint_change_ids = working_change_ids.clone();

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
    let label_change_id = generate_uuid(tx).await?;
    let label_change_created_at = generate_timestamp(tx).await?;
    let label_change_entity_id =
        format!("{working_commit_id}~lix_commit~lix~{checkpoint_label_id}");
    let label_state_snapshot = serde_json::json!({
        "entity_id": working_commit_id.clone(),
        "schema_key": "lix_commit",
        "file_id": "lix",
        "label_id": checkpoint_label_id.clone(),
    });
    tx.execute(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES ($1, 'lix_change', 'lix', 'global', 'lix', $2, '1')",
        &[
            Value::Text(label_change_id.clone()),
            Value::Text(
                serde_json::json!({
                    "id": label_change_id.clone(),
                    "entity_id": label_change_entity_id.clone(),
                    "schema_key": "lix_entity_label",
                    "schema_version": "1",
                    "file_id": "lix",
                    "plugin_key": "lix",
                    "created_at": label_change_created_at,
                    "snapshot_content": label_state_snapshot,
                })
                .to_string(),
            ),
        ],
    )
    .await?;
    tx.execute(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES ($1, 'lix_change_set_element', 'lix', 'global', 'lix', $2, '1')",
        &[
            Value::Text(format!("{working_change_set_id}~{label_change_id}")),
            Value::Text(
                serde_json::json!({
                    "change_set_id": working_change_set_id,
                    "change_id": label_change_id.clone(),
                    "entity_id": label_change_entity_id,
                    "schema_key": "lix_entity_label",
                    "file_id": "lix",
                })
                .to_string(),
            ),
        ],
    )
    .await?;
    if !checkpoint_change_ids
        .iter()
        .any(|id| id == &label_change_id)
    {
        checkpoint_change_ids.push(label_change_id);
    }

    let checkpoint_snapshot = serde_json::json!({
        "id": working_commit_id.clone(),
        "change_set_id": working_change_set_id.clone(),
        "parent_commit_ids": merged_parents.clone(),
        "change_ids": checkpoint_change_ids,
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
            Value::Text(checkpoint_snapshot.clone()),
            Value::Text(working_commit_id.clone()),
        ],
    )
    .await?;
    let checkpoint_commit_exists = tx
        .execute_internal(
            "SELECT 1 \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_commit' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND is_tombstone = 0 \
             LIMIT 1",
            &[Value::Text(working_commit_id.clone())],
        )
        .await?;
    if checkpoint_commit_exists.rows.is_empty() {
        tx.execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ($1, 'lix_commit', 'lix', 'global', 'lix', $2, '1')",
            &[
                Value::Text(working_commit_id.clone()),
                Value::Text(checkpoint_snapshot),
            ],
        )
        .await?;
    }
    tx.execute_internal(
        "DELETE FROM lix_internal_state_untracked \
         WHERE metadata = $1 \
           AND ( \
             (schema_key = 'lix_commit' AND entity_id = $2) \
             OR (schema_key = 'lix_change_set_element' AND entity_id LIKE $3) \
             OR (schema_key = 'lix_change' AND entity_id LIKE $4) \
           )",
        &[
            Value::Text(WORKING_PROJECTION_METADATA.to_string()),
            Value::Text(working_commit_id.clone()),
            Value::Text(format!(
                "{working_change_set_id}~working_projection:{version_id}:{working_change_set_id}:%"
            )),
            Value::Text(format!(
                "working_projection:{version_id}:{working_change_set_id}:%"
            )),
        ],
    )
    .await?;
    ensure_commit_edge(tx, &effective_previous_tip_id, &working_commit_id).await?;
    ensure_commit_ancestry(tx, &working_commit_id, &merged_parents).await?;

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
    ensure_commit_ancestry(
        tx,
        &new_working_commit_id,
        std::slice::from_ref(&working_commit_id),
    )
    .await?;

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
    change_ids: Vec<String>,
}

async fn load_commit(
    tx: &mut EngineTransaction<'_>,
    commit_id: &str,
) -> Result<Option<CommitRow>, LixError> {
    let result = tx
        .execute(
            "SELECT change_set_id, parent_commit_ids, change_ids \
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
    let change_ids = parse_change_ids(row.get(2), "change_ids")?;
    Ok(Some(CommitRow {
        change_set_id,
        parent_commit_ids,
        change_ids,
    }))
}

async fn load_checkpoint_label_id(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let result = tx
        .execute_internal(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_label' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await?;
    for row in &result.rows {
        let snapshot_content = text_at(row, 0, "snapshot_content")?;
        let parsed: serde_json::Value =
            serde_json::from_str(&snapshot_content).map_err(|error| LixError {
                message: format!("checkpoint label snapshot invalid JSON: {error}"),
            })?;
        if parsed.get("name").and_then(serde_json::Value::as_str) != Some("checkpoint") {
            continue;
        }
        if let Some(label_id) = parsed
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty())
        {
            return Ok(label_id.to_string());
        }
    }
    Err(LixError {
        message: "checkpoint label row is missing in global version".to_string(),
    })
}

async fn resolve_effective_previous_tip_id(
    tx: &mut EngineTransaction<'_>,
    previous_tip_id: &str,
    working_commit_id: &str,
    working_change_ids: &[String],
) -> Result<String, LixError> {
    if previous_tip_id != working_commit_id {
        return Ok(previous_tip_id.to_string());
    }
    if working_change_ids.is_empty() {
        return Ok(previous_tip_id.to_string());
    }
    let mut sql = String::from(
        "SELECT lixcol_commit_id \
         FROM lix_change \
         WHERE id IN (",
    );
    let mut params = Vec::with_capacity(working_change_ids.len() + 1);
    for (index, change_id) in working_change_ids.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("${}", index + 1));
        params.push(Value::Text(change_id.clone()));
    }
    sql.push_str(") AND lixcol_commit_id <> $");
    sql.push_str(&(working_change_ids.len() + 1).to_string());
    sql.push_str(" ORDER BY created_at DESC LIMIT 1");
    params.push(Value::Text(working_commit_id.to_string()));
    let result = tx.execute(&sql, &params).await?;
    let row = first_row(&result, "effective previous tip row").map_err(|_| LixError {
        message: format!(
            "working commit '{working_commit_id}' has changes but previous tip could not be resolved"
        ),
    })?;
    text_at(row, 0, "lixcol_commit_id")
}

async fn ensure_change_set_elements_for_checkpoint(
    tx: &mut EngineTransaction<'_>,
    change_set_id: &str,
    change_ids: &[String],
) -> Result<(), LixError> {
    if change_ids.is_empty() {
        return Ok(());
    }

    let working_elements = tx
        .execute(
            "SELECT change_id, entity_id, schema_key, file_id \
             FROM lix_change_set_element \
             WHERE change_set_id = $1",
            &[Value::Text(change_set_id.to_string())],
        )
        .await?;

    if working_elements.rows.is_empty() {
        return Err(LixError {
            message: format!(
                "working change_set '{change_set_id}' has change_ids but no change_set elements"
            ),
        });
    }

    let mut latest_working_change_by_entity =
        std::collections::BTreeMap::<(String, String, String), (String, String, bool)>::new();
    let mut raw_change_sql = String::from(
        "SELECT id, entity_id, schema_key, file_id, created_at, \
                CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS is_tombstone \
         FROM lix_change \
         WHERE id IN (",
    );
    let mut raw_change_params = Vec::with_capacity(change_ids.len());
    for (index, change_id) in change_ids.iter().enumerate() {
        if index > 0 {
            raw_change_sql.push_str(", ");
        }
        raw_change_sql.push_str(&format!("${}", index + 1));
        raw_change_params.push(Value::Text(change_id.clone()));
    }
    raw_change_sql.push(')');
    let raw_change_rows = tx.execute(&raw_change_sql, &raw_change_params).await?;
    for row in &raw_change_rows.rows {
        let raw_change_id = text_at(row, 0, "id")?;
        let entity_id = text_at(row, 1, "entity_id")?;
        let schema_key = text_at(row, 2, "schema_key")?;
        let file_id = text_at(row, 3, "file_id")?;
        let created_at = text_at(row, 4, "created_at")?;
        let is_tombstone = matches!(row.get(5), Some(Value::Integer(1)));
        let key = (entity_id, schema_key, file_id);
        match latest_working_change_by_entity.get(&key) {
            None => {
                latest_working_change_by_entity
                    .insert(key, (raw_change_id, created_at, is_tombstone));
            }
            Some((_, existing_created_at, existing_is_tombstone)) => {
                if created_at.as_str() > existing_created_at.as_str()
                    || (created_at == *existing_created_at
                        && is_tombstone
                        && !*existing_is_tombstone)
                {
                    latest_working_change_by_entity
                        .insert(key, (raw_change_id, created_at, is_tombstone));
                }
            }
        }
    }

    for row in &working_elements.rows {
        let view_change_id = text_at(row, 0, "change_id")?;
        let entity_id = text_at(row, 1, "entity_id")?;
        let schema_key = text_at(row, 2, "schema_key")?;
        let file_id = text_at(row, 3, "file_id")?;
        let selected_change_id = latest_working_change_by_entity
            .get(&(entity_id.clone(), schema_key.clone(), file_id.clone()))
            .map(|(change_id, _created_at, _is_tombstone)| change_id.clone())
            .unwrap_or(view_change_id);

        ensure_change_exists_for_checkpoint(tx, &selected_change_id).await?;
        tx.execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ($1, 'lix_change_set_element', 'lix', 'global', 'lix', $2, '1')",
            &[
                Value::Text(format!("{change_set_id}~{selected_change_id}")),
                Value::Text(
                    serde_json::json!({
                        "change_set_id": change_set_id,
                        "change_id": selected_change_id,
                        "entity_id": entity_id,
                        "schema_key": schema_key,
                        "file_id": file_id,
                    })
                    .to_string(),
                ),
            ],
        )
        .await?;
    }

    Ok(())
}

async fn ensure_change_exists_for_checkpoint(
    tx: &mut EngineTransaction<'_>,
    change_id: &str,
) -> Result<(), LixError> {
    let existing = tx
        .execute_internal(
            "SELECT 1 \
             FROM lix_internal_state_materialized_v1_lix_change \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_change' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND is_tombstone = 0 \
             LIMIT 1",
            &[Value::Text(change_id.to_string())],
        )
        .await?;
    if !existing.rows.is_empty() {
        return Ok(());
    }

    let row = tx
        .execute(
            "SELECT id, entity_id, schema_key, schema_version, file_id, plugin_key, created_at, snapshot_content, metadata \
             FROM lix_change \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(change_id.to_string())],
        )
        .await?;
    let Some(row) = row.rows.first() else {
        return Err(LixError {
            message: format!("change '{change_id}' is missing while checkpointing"),
        });
    };
    let id = text_at(row, 0, "id")?;
    let entity_id = text_at(row, 1, "entity_id")?;
    let schema_key = text_at(row, 2, "schema_key")?;
    let schema_version = text_at(row, 3, "schema_version")?;
    let file_id = text_at(row, 4, "file_id")?;
    let plugin_key = text_at(row, 5, "plugin_key")?;
    let created_at = text_at(row, 6, "created_at")?;
    let snapshot_content = match row.get(7) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => {
            let parsed: serde_json::Value =
                serde_json::from_str(value).unwrap_or(serde_json::Value::Null);
            if parsed.is_null() {
                None
            } else {
                Some(parsed)
            }
        }
        _ => None,
    };
    let metadata = match row.get(8) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => {
            let parsed: serde_json::Value =
                serde_json::from_str(value).unwrap_or(serde_json::Value::Null);
            if parsed.is_null() {
                None
            } else {
                Some(parsed)
            }
        }
        _ => None,
    };
    let mut change_snapshot = serde_json::Map::new();
    change_snapshot.insert(
        "id".to_string(),
        serde_json::Value::String(change_id.to_string()),
    );
    change_snapshot.insert(
        "entity_id".to_string(),
        serde_json::Value::String(entity_id),
    );
    change_snapshot.insert(
        "schema_key".to_string(),
        serde_json::Value::String(schema_key),
    );
    change_snapshot.insert(
        "schema_version".to_string(),
        serde_json::Value::String(schema_version),
    );
    change_snapshot.insert("file_id".to_string(), serde_json::Value::String(file_id));
    change_snapshot.insert(
        "plugin_key".to_string(),
        serde_json::Value::String(plugin_key),
    );
    change_snapshot.insert(
        "created_at".to_string(),
        serde_json::Value::String(created_at),
    );
    if let Some(snapshot_content) = snapshot_content {
        change_snapshot.insert("snapshot_content".to_string(), snapshot_content);
    }
    if let Some(metadata) = metadata {
        change_snapshot.insert("metadata".to_string(), metadata);
    }

    tx.execute(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES ($1, 'lix_change', 'lix', 'global', 'lix', $2, '1')",
        &[
            Value::Text(id),
            Value::Text(serde_json::Value::Object(change_snapshot).to_string()),
        ],
    )
    .await?;

    Ok(())
}

async fn ensure_commit_edge(
    tx: &mut EngineTransaction<'_>,
    parent_id: &str,
    child_id: &str,
) -> Result<(), LixError> {
    if parent_id == child_id {
        return Err(LixError {
            message: format!("refusing self-edge for commit '{parent_id}'"),
        });
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

async fn ensure_commit_ancestry(
    tx: &mut EngineTransaction<'_>,
    commit_id: &str,
    parent_ids: &[String],
) -> Result<(), LixError> {
    tx.execute_internal(
        "INSERT INTO lix_internal_commit_ancestry (commit_id, ancestor_id, depth) \
         VALUES ($1, $1, 0) \
         ON CONFLICT (commit_id, ancestor_id) DO NOTHING",
        &[Value::Text(commit_id.to_string())],
    )
    .await?;

    let normalized_parents = normalize_parent_commit_ids(parent_ids.to_vec(), commit_id);
    for parent_id in normalized_parents {
        tx.execute_internal(
            "INSERT INTO lix_internal_commit_ancestry (commit_id, ancestor_id, depth) \
             SELECT $1, candidate.ancestor_id, MIN(candidate.depth) AS depth \
             FROM ( \
               SELECT $2 AS ancestor_id, 1 AS depth \
               UNION ALL \
               SELECT ancestor_id, depth + 1 AS depth \
               FROM lix_internal_commit_ancestry \
               WHERE commit_id = $2 \
             ) AS candidate \
             GROUP BY candidate.ancestor_id \
             ON CONFLICT (commit_id, ancestor_id) DO UPDATE \
             SET depth = CASE \
               WHEN excluded.depth < lix_internal_commit_ancestry.depth THEN excluded.depth \
               ELSE lix_internal_commit_ancestry.depth \
             END",
            &[Value::Text(commit_id.to_string()), Value::Text(parent_id)],
        )
        .await?;
    }

    Ok(())
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

async fn generate_timestamp(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let result = tx.execute("SELECT lix_timestamp()", &[]).await?;
    let row = first_row(&result, "generated timestamp row")?;
    text_at(row, 0, "lix_timestamp()")
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
    parse_string_array(value, field)
}

fn parse_change_ids(value: Option<&Value>, field: &str) -> Result<Vec<String>, LixError> {
    parse_string_array(value, field)
}

fn parse_string_array(value: Option<&Value>, field: &str) -> Result<Vec<String>, LixError> {
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
