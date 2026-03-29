use super::{checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot};
use crate::canonical::readers::load_commit_lineage_entry_by_id;
use crate::engine::TransactionBackendAdapter;
use crate::init::seed::{
    normalized_insert_columns_sql, normalized_insert_literals_sql, normalized_seed_values,
    quote_ident,
};
use crate::live_state::schema_access::tracked_relation_name;
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
    // Keep the derived checkpoint pointer cache warm for the active version.
    tx.backend_transaction_mut()?
        .execute(
            "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES ($1, $2) \
         ON CONFLICT (version_id) DO UPDATE \
         SET checkpoint_commit_id = excluded.checkpoint_commit_id",
            &[
                Value::Text(version_id),
                Value::Text(local_commit_id.clone()),
            ],
        )
        .await?;
    // The global lane mirrors the same derived cache contract and remains
    // rebuildable from canonical heads plus checkpoint labels.
    tx.backend_transaction_mut()?
        .execute(
            "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES ($1, $2) \
         ON CONFLICT (version_id) DO UPDATE \
         SET checkpoint_commit_id = excluded.checkpoint_commit_id",
            &[
                Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
                Value::Text(global_commit_id.clone()),
            ],
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
    let tracked_table = quote_ident(&tracked_relation_name("lix_entity_label"));
    let exists = tx
        .backend_transaction_mut()?
        .execute(
            &format!(
                "SELECT 1 \
                 FROM {tracked_table} \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_entity_label' \
               AND file_id = 'lix' \
               AND version_id = $2 \
               AND is_tombstone = 0 \
             LIMIT 1"
            ),
            &[
                Value::Text(state_entity_id.clone()),
                Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
            ],
        )
        .await?;
    if !exists.rows.is_empty() {
        return Ok(());
    }

    let snapshot_content = checkpoint_commit_label_snapshot(commit_id);
    let normalized_values = normalized_seed_values("lix_entity_label", Some(&snapshot_content))?;
    tx.backend_transaction_mut()?
        .execute(
            &format!(
                "INSERT INTO {tracked_table} (\
                 entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
                 ) VALUES (\
                 $1, 'lix_entity_label', '1', 'lix', $2, true, 'lix', lix_uuid_v7(), NULL, NULL, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP{normalized_literals}\
                 )",
                normalized_columns = normalized_insert_columns_sql(&normalized_values),
                normalized_literals = normalized_insert_literals_sql(&normalized_values),
            ),
            &[
                Value::Text(state_entity_id),
                Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
            ],
        )
        .await?;
    Ok(())
}
