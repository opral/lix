use std::collections::BTreeSet;

use crate::backend::ddl::execute_ddl_batch;
use crate::common::text::escape_sql_string;
use crate::contracts::artifacts::{
    DomainChangeBatch, PreparedPublicWriteArtifact, PreparedWriteOperationKind,
};
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

pub(crate) const LAST_CHECKPOINT_TABLE: &str = "lix_internal_last_checkpoint";

const HISTORY_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_last_checkpoint (\
     version_id TEXT PRIMARY KEY,\
     checkpoint_commit_id TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_last_checkpoint_commit \
     ON lix_internal_last_checkpoint (checkpoint_commit_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_ddl_batch(backend, "checkpoint", HISTORY_INIT_STATEMENTS).await
}

pub(crate) async fn apply_public_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixBackendTransaction,
    public_write: &PreparedPublicWriteArtifact,
    batch: &DomainChangeBatch,
) -> Result<(), LixError> {
    if public_write.contract.target.descriptor.public_name != "lix_version" {
        return Ok(());
    }

    match public_write.contract.operation_kind {
        PreparedWriteOperationKind::Insert => {
            upsert_last_checkpoint_rows_in_transaction(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                true,
            )
            .await
        }
        PreparedWriteOperationKind::Update => {
            upsert_last_checkpoint_rows_in_transaction(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                false,
            )
            .await
        }
        PreparedWriteOperationKind::Delete => {
            let version_ids = version_ids_from_resolved_write(public_write, batch);
            delete_last_checkpoint_rows_in_transaction(transaction, &version_ids).await
        }
    }
}

pub(crate) async fn upsert_last_checkpoint_for_version_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
    checkpoint_commit_id: &str,
) -> Result<(), LixError> {
    upsert_last_checkpoint_rows_in_transaction(
        transaction,
        &[(version_id.to_string(), checkpoint_commit_id.to_string())],
        true,
    )
    .await
}

fn version_checkpoint_rows_from_resolved_write(
    public_write: &PreparedPublicWriteArtifact,
    batch: &DomainChangeBatch,
) -> Vec<(String, String)> {
    if let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() {
        let rows = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                row.schema_key == crate::version_state::version_ref_schema_key() && !row.tombstone
            })
            .filter_map(|row| {
                row.values
                    .get("snapshot_content")
                    .and_then(|value| match value {
                        Value::Text(snapshot) => {
                            serde_json::from_str::<serde_json::Value>(snapshot)
                                .ok()
                                .and_then(|snapshot| {
                                    snapshot
                                        .get("commit_id")
                                        .and_then(serde_json::Value::as_str)
                                        .map(|commit_id| {
                                            (row.entity_id.to_string(), commit_id.to_string())
                                        })
                                })
                        }
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return rows;
        }
    }

    batch
        .changes
        .iter()
        .filter(|change| change.schema_key == crate::version_state::version_ref_schema_key())
        .filter_map(|change| {
            change.snapshot_content.as_deref().and_then(|snapshot| {
                serde_json::from_str::<serde_json::Value>(snapshot)
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .get("commit_id")
                            .and_then(serde_json::Value::as_str)
                            .map(|commit_id| (change.entity_id.to_string(), commit_id.to_string()))
                    })
            })
        })
        .collect()
}

fn version_ids_from_resolved_write(
    public_write: &PreparedPublicWriteArtifact,
    batch: &DomainChangeBatch,
) -> Vec<String> {
    if let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() {
        let version_ids = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                matches!(
                    row.schema_key.as_str(),
                    "lix_version_ref" | "lix_version_descriptor"
                )
            })
            .map(|row| row.entity_id.to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !version_ids.is_empty() {
            return version_ids;
        }
    }

    batch
        .changes
        .iter()
        .map(|change| change.entity_id.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
}

async fn upsert_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[(String, String)],
    update_existing: bool,
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let values_sql = rows
        .iter()
        .map(|(version_id, checkpoint_commit_id)| {
            format!(
                "('{}', '{}')",
                escape_sql_string(version_id),
                escape_sql_string(checkpoint_commit_id)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let on_conflict = if update_existing {
        "DO UPDATE SET checkpoint_commit_id = excluded.checkpoint_commit_id"
    } else {
        "DO NOTHING"
    };
    let sql = format!(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES {values_sql} \
         ON CONFLICT (version_id) {on_conflict}"
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn delete_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_ids: &[String],
) -> Result<(), LixError> {
    if version_ids.is_empty() {
        return Ok(());
    }

    let in_list = version_ids
        .iter()
        .map(|id| format!("'{}'", escape_sql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM lix_internal_last_checkpoint WHERE version_id IN ({in_list})");
    transaction.execute(&sql, &[]).await?;
    Ok(())
}
