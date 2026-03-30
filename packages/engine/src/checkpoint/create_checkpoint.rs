use super::{checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot};
use crate::canonical::readers::load_commit_lineage_entry_by_id;
use crate::engine::TransactionBackendAdapter;
use crate::init::seed::{
    normalized_insert_columns_sql, normalized_insert_literals_sql, normalized_seed_values,
    quote_ident,
};
use crate::live_state::schema_access::tracked_relation_name;
use crate::sql::executor::runtime_state::ExecutionRuntimeState;
use crate::{ExecuteOptions, LixError, Session, SessionTransaction, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateCheckpointResult {
    pub id: String,
    pub change_set_id: String,
}

pub(crate) async fn create_checkpoint_in_session(
    session: &Session,
) -> Result<CreateCheckpointResult, LixError> {
    session
        .transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move { create_checkpoint_in_transaction(tx).await })
        })
        .await
}

async fn create_checkpoint_in_transaction(
    tx: &mut SessionTransaction<'_>,
) -> Result<CreateCheckpointResult, LixError> {
    let version_id = tx.context.active_version_id.clone();
    let local_commit_id = load_version_commit_id(tx, &version_id).await?;
    let global_commit_id = load_global_version_commit_id(tx).await?;

    let commit = load_commit(tx, &local_commit_id)
        .await?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("commit '{local_commit_id}' is missing"),
        })?;
    if commit.change_set_id.trim().is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("commit '{local_commit_id}' has empty change_set_id"),
        });
    }

    ensure_checkpoint_label_on_commit(tx, &local_commit_id).await?;
    if global_commit_id != local_commit_id {
        ensure_checkpoint_label_on_commit(tx, &global_commit_id).await?;
    }
    // Keep the derived checkpoint-history cache warm for the active version.
    super::history::upsert_last_checkpoint_for_version_in_transaction(
        tx.backend_transaction_mut()?,
        &version_id,
        &local_commit_id,
    )
    .await?;
    // The global lane mirrors the same derived cache contract and remains
    // rebuildable from canonical heads plus canonical checkpoint labels.
    super::history::upsert_last_checkpoint_for_version_in_transaction(
        tx.backend_transaction_mut()?,
        crate::version::GLOBAL_VERSION_ID,
        &global_commit_id,
    )
    .await?;

    Ok(CreateCheckpointResult {
        id: local_commit_id,
        change_set_id: commit.change_set_id,
    })
}

#[derive(Debug, Clone)]
struct CommitRow {
    change_set_id: String,
}

async fn load_commit(
    tx: &mut SessionTransaction<'_>,
    commit_id: &str,
) -> Result<Option<CommitRow>, LixError> {
    let mut executor = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    let Some(commit) = load_commit_lineage_entry_by_id(&mut executor, commit_id).await? else {
        return Ok(None);
    };
    Ok(Some(CommitRow {
        change_set_id: commit.change_set_id.unwrap_or_default(),
    }))
}

async fn load_global_version_commit_id(
    tx: &mut SessionTransaction<'_>,
) -> Result<String, LixError> {
    let mut executor = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    let Some(commit_id) = crate::canonical::refs::load_committed_version_head_commit_id(
        &mut executor,
        crate::version::GLOBAL_VERSION_ID,
    )
    .await?
    else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "hidden global version ref row is missing".to_string(),
        });
    };
    Ok(commit_id)
}

async fn load_version_commit_id(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
) -> Result<String, LixError> {
    let mut executor = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    let Some(commit_id) =
        crate::canonical::refs::load_committed_version_head_commit_id(&mut executor, version_id)
            .await?
    else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("version '{version_id}' is missing"),
        });
    };
    Ok(commit_id)
}

async fn ensure_checkpoint_label_on_commit(
    tx: &mut SessionTransaction<'_>,
    commit_id: &str,
) -> Result<(), LixError> {
    let state_entity_id = checkpoint_commit_label_entity_id(commit_id);
    let exists = tx
        .backend_transaction_mut()?
        .execute(
            "SELECT 1 \
             FROM lix_internal_change \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_entity_label' \
               AND file_id = 'lix' \
               AND plugin_key = 'lix' \
             LIMIT 1",
            &[Value::Text(state_entity_id.clone())],
        )
        .await?;
    if !exists.rows.is_empty() {
        return Ok(());
    }

    let snapshot_content = checkpoint_commit_label_snapshot(commit_id);
    let change_id = generate_runtime_uuid(tx).await?;
    let timestamp = generate_runtime_timestamp(tx).await?;
    insert_canonical_checkpoint_label_change(
        tx,
        &state_entity_id,
        &snapshot_content,
        &change_id,
        &timestamp,
    )
    .await?;

    let normalized_values = normalized_seed_values("lix_entity_label", Some(&snapshot_content))?;
    let tracked_table = quote_ident(&tracked_relation_name("lix_entity_label"));
    tx.backend_transaction_mut()?
        .execute(
            &format!(
                "INSERT INTO {tracked_table} (\
                 entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
                 ) VALUES (\
                 $1, 'lix_entity_label', '1', 'lix', $2, true, 'lix', $3, NULL, NULL, 0, $4, $4{normalized_literals}\
                 )",
                normalized_columns = normalized_insert_columns_sql(&normalized_values),
                normalized_literals = normalized_insert_literals_sql(&normalized_values),
            ),
            &[
                Value::Text(state_entity_id),
                Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
                Value::Text(change_id),
                Value::Text(timestamp),
            ],
        )
        .await?;
    Ok(())
}

async fn insert_canonical_checkpoint_label_change(
    tx: &mut SessionTransaction<'_>,
    entity_id: &str,
    snapshot_content: &str,
    change_id: &str,
    created_at: &str,
) -> Result<(), LixError> {
    let snapshot_id = format!("{change_id}~snapshot");
    tx.backend_transaction_mut()?
        .execute(
            "INSERT INTO lix_internal_snapshot (id, content) \
             SELECT $1, $2 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_snapshot WHERE id = $1)",
            &[
                Value::Text(snapshot_id.clone()),
                Value::Text(snapshot_content.to_string()),
            ],
        )
        .await?;
    tx.backend_transaction_mut()?
        .execute(
            "INSERT INTO lix_internal_change (\
             id, entity_id, schema_key, schema_version, file_id, plugin_key, snapshot_id, metadata, created_at\
             ) \
             SELECT $1, $2, 'lix_entity_label', '1', 'lix', 'lix', $3, NULL, $4 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_change WHERE id = $1)",
            &[
                Value::Text(change_id.to_string()),
                Value::Text(entity_id.to_string()),
                Value::Text(snapshot_id),
                Value::Text(created_at.to_string()),
            ],
        )
        .await?;
    Ok(())
}

async fn checkpoint_runtime_state(
    tx: &mut SessionTransaction<'_>,
) -> Result<ExecutionRuntimeState, LixError> {
    if let Some(runtime_state) = tx.context.execution_runtime_state().cloned() {
        return Ok(runtime_state);
    }

    let engine = tx.engine;
    let backend = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    let runtime_state = ExecutionRuntimeState::prepare(engine, &backend).await?;
    runtime_state
        .ensure_sequence_initialized_in_transaction(engine, tx.backend_transaction_mut()?)
        .await?;
    tx.context
        .set_execution_runtime_state(runtime_state.clone());
    Ok(runtime_state)
}

async fn generate_runtime_uuid(tx: &mut SessionTransaction<'_>) -> Result<String, LixError> {
    Ok(checkpoint_runtime_state(tx)
        .await?
        .provider()
        .call_uuid_v7())
}

async fn generate_runtime_timestamp(tx: &mut SessionTransaction<'_>) -> Result<String, LixError> {
    Ok(checkpoint_runtime_state(tx)
        .await?
        .provider()
        .call_timestamp())
}
