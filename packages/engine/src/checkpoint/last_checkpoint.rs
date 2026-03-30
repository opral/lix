use std::collections::BTreeSet;

use crate::sql::common::text::escape_sql_string;
use crate::sql::executor::PreparedPublicWrite;
use crate::sql::semantic_ir::semantics::domain_changes::DomainChangeBatch;
use crate::{LixBackendTransaction, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionHeadUpdate {
    version_id: String,
    commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VersionCheckpointEffect {
    Upsert {
        heads: Vec<VersionHeadUpdate>,
        update_existing: bool,
    },
    Delete {
        version_ids: Vec<String>,
    },
}

pub(crate) async fn apply_public_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixBackendTransaction,
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> Result<(), LixError> {
    // Public writes to `lix_version` keep the derived checkpoint pointer cache
    // in sync. This is convenience state only; canonical changes remain the
    // source of truth if the cache is rebuilt.
    if public_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_version"
    {
        return Ok(());
    }

    match version_checkpoint_effect_from_public_write(public_write, batch) {
        VersionCheckpointEffect::Upsert {
            heads,
            update_existing,
        } => {
            let rows = heads
                .into_iter()
                .map(|head| (head.version_id, head.commit_id))
                .collect::<Vec<_>>();
            upsert_last_checkpoint_rows(transaction, &rows, update_existing).await
        }
        VersionCheckpointEffect::Delete { version_ids } => {
            delete_last_checkpoint_rows(transaction, &version_ids).await
        }
    }
}

fn version_checkpoint_effect_from_public_write(
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> VersionCheckpointEffect {
    match public_write.planned_write.command.operation_kind {
        crate::sql::logical_plan::public_ir::WriteOperationKind::Insert => {
            VersionCheckpointEffect::Upsert {
                heads: version_head_updates_from_resolved_write(public_write, batch),
                update_existing: true,
            }
        }
        crate::sql::logical_plan::public_ir::WriteOperationKind::Update => {
            VersionCheckpointEffect::Upsert {
                heads: version_head_updates_from_resolved_write(public_write, batch),
                update_existing: false,
            }
        }
        crate::sql::logical_plan::public_ir::WriteOperationKind::Delete => {
            VersionCheckpointEffect::Delete {
                version_ids: version_ids_from_resolved_write(public_write, batch),
            }
        }
    }
}

fn version_head_updates_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> Vec<VersionHeadUpdate> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let heads = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                row.schema_key == crate::version::version_ref_schema_key() && !row.tombstone
            })
            .filter_map(version_head_update_from_planned_row)
            .collect::<Vec<_>>();
        if !heads.is_empty() {
            return heads;
        }
    }

    batch
        .changes
        .iter()
        .filter(|change| change.schema_key == crate::version::version_ref_schema_key())
        .filter_map(|change| {
            version_head_update_from_snapshot(
                change.entity_id.to_string(),
                change.snapshot_content.as_deref(),
            )
        })
        .collect()
}

fn version_head_update_from_planned_row(
    row: &crate::sql::logical_plan::public_ir::PlannedStateRow,
) -> Option<VersionHeadUpdate> {
    version_head_update_from_snapshot(
        row.entity_id.to_string(),
        row.values
            .get("snapshot_content")
            .and_then(|value| match value {
                Value::Text(snapshot) => Some(snapshot.as_str()),
                _ => None,
            }),
    )
}

fn version_head_update_from_snapshot(
    version_id: String,
    snapshot_content: Option<&str>,
) -> Option<VersionHeadUpdate> {
    snapshot_content.and_then(|snapshot| {
        serde_json::from_str::<serde_json::Value>(snapshot)
            .ok()
            .and_then(|snapshot| {
                snapshot
                    .get("commit_id")
                    .and_then(serde_json::Value::as_str)
                    .map(|commit_id| VersionHeadUpdate {
                        version_id,
                        commit_id: commit_id.to_string(),
                    })
            })
    })
}

fn version_ids_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> Vec<String> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
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

async fn upsert_last_checkpoint_rows(
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

async fn delete_last_checkpoint_rows(
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
