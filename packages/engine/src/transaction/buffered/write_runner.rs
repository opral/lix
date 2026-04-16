use crate::canonical::{
    append_changes, append_untracked_change_visibility_rows,
    canonical_untracked_visibility_write_from_change_visibility,
    compact_untracked_changes_for_touched_rows_in_transaction, CanonicalChangeWrite,
    CanonicalUntrackedVisibilityWrite,
};
use crate::functions::SystemFunctionProvider;
use crate::live_state::{
    finalize_live_state_after_immediate_write, write_live_rows, LiveRow, LiveWriteOperation,
    LiveWriteRow,
};
use crate::transaction::CommitOutcome;
use crate::{LixBackendTransaction, LixError};

use super::write_plan::WritePlan;

pub(crate) async fn apply_write_plan(
    transaction: &mut dyn LixBackendTransaction,
    plan: &WritePlan,
) -> Result<CommitOutcome, LixError> {
    let (tracked_writes, untracked_writes): (Vec<_>, Vec<_>) =
        plan.writes.iter().partition(|write| !write.untracked);

    if !tracked_writes.is_empty() {
        let live_rows = tracked_writes
            .into_iter()
            .map(live_row_from_write)
            .collect::<Vec<_>>();
        write_live_rows(transaction, &live_rows).await?;
    }

    if !untracked_writes.is_empty() {
        let mut functions = SystemFunctionProvider;
        let canonical_changes = canonical_changes_from_untracked_writes(&untracked_writes)?;
        let live_rows = untracked_writes
            .into_iter()
            .zip(canonical_changes.iter())
            .map(|(write, change)| live_row_from_untracked_write(write, change))
            .collect::<Vec<_>>();
        let visibility_rows = canonical_untracked_visibility_rows_from_untracked_live_rows(
            &live_rows,
            &canonical_changes,
        )?;
        append_changes(transaction, &canonical_changes, &mut functions).await?;
        append_untracked_change_visibility_rows(transaction, &visibility_rows).await?;
        write_live_rows(transaction, &live_rows).await?;
        finalize_live_state_after_immediate_write(transaction).await?;
        compact_untracked_changes_for_touched_rows_in_transaction(transaction, &visibility_rows)
            .await?;
    }

    Ok(CommitOutcome::from_write_plan(plan))
}

#[allow(dead_code)]
fn tracked_writes_summary(writes: &[LiveWriteRow]) -> (usize, usize) {
    let mut upserts = 0;
    let mut tombstones = 0;
    for write in writes {
        if write.untracked {
            continue;
        }
        match write.operation {
            LiveWriteOperation::Upsert => upserts += 1,
            LiveWriteOperation::Tombstone => tombstones += 1,
            LiveWriteOperation::Delete => {}
        }
    }
    (upserts, tombstones)
}

#[allow(dead_code)]
fn untracked_writes_summary(writes: &[LiveWriteRow]) -> (usize, usize) {
    let mut upserts = 0;
    let mut deletes = 0;
    for write in writes {
        if !write.untracked {
            continue;
        }
        match write.operation {
            LiveWriteOperation::Upsert => upserts += 1,
            LiveWriteOperation::Delete => deletes += 1,
            LiveWriteOperation::Tombstone => {}
        }
    }
    (upserts, deletes)
}

fn live_row_from_write(write: &LiveWriteRow) -> LiveRow {
    LiveRow {
        entity_id: write.entity_id.clone(),
        file_id: write.file_id.clone(),
        schema_key: write.schema_key.clone(),
        schema_version: write.schema_version.clone(),
        version_id: write.version_id.clone(),
        plugin_key: write.plugin_key.clone(),
        metadata: write.metadata.clone(),
        change_id: Some(write.change_id.clone()),
        global: write.global,
        untracked: write.untracked,
        created_at: write.created_at.clone(),
        updated_at: Some(write.updated_at.clone()),
        snapshot_content: write.snapshot_content.clone(),
    }
}

fn live_row_from_untracked_write(write: &LiveWriteRow, change: &CanonicalChangeWrite) -> LiveRow {
    LiveRow {
        entity_id: write.entity_id.clone(),
        file_id: write.file_id.clone(),
        schema_key: write.schema_key.clone(),
        schema_version: write.schema_version.clone(),
        version_id: write.version_id.clone(),
        plugin_key: write.plugin_key.clone(),
        metadata: write.metadata.clone(),
        change_id: Some(change.id.clone()),
        global: write.global,
        untracked: true,
        created_at: Some(change.created_at.clone()),
        updated_at: Some(change.created_at.clone()),
        snapshot_content: write.snapshot_content.clone(),
    }
}

fn canonical_changes_from_untracked_writes(
    writes: &[&LiveWriteRow],
) -> Result<Vec<CanonicalChangeWrite>, LixError> {
    writes
        .iter()
        .map(|write| {
            Ok(CanonicalChangeWrite {
                id: write.change_id.clone(),
                entity_id: write.entity_id.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid untracked write entity_id '{}'", write.entity_id),
                    )
                })?,
                schema_key: write.schema_key.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid untracked write schema_key '{}'", write.schema_key),
                    )
                })?,
                schema_version: write.schema_version.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "invalid untracked write schema_version '{}'",
                            write.schema_version
                        ),
                    )
                })?,
                file_id: write
                    .file_id
                    .clone()
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "invalid untracked write file_id {:?}",
                                write.file_id.as_deref()
                            ),
                        )
                    })?,
                plugin_key: write
                    .plugin_key
                    .clone()
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "invalid untracked write plugin_key {:?}",
                                write.plugin_key.as_deref()
                            ),
                        )
                    })?,
                snapshot_content: write
                    .snapshot_content
                    .clone()
                    .map(crate::canonical::CanonicalJson::from_text)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "invalid untracked write snapshot_content for '{}': {}",
                                write.schema_key, error.description
                            ),
                        )
                    })?,
                metadata: write
                    .metadata
                    .clone()
                    .map(crate::canonical::CanonicalJson::from_text)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "invalid untracked write metadata for '{}': {}",
                                write.schema_key, error.description
                            ),
                        )
                    })?,
                created_at: write
                    .created_at
                    .clone()
                    .unwrap_or_else(|| write.updated_at.clone()),
            })
        })
        .collect()
}

fn canonical_untracked_visibility_rows_from_untracked_live_rows(
    rows: &[LiveRow],
    canonical_changes: &[CanonicalChangeWrite],
) -> Result<Vec<CanonicalUntrackedVisibilityWrite>, LixError> {
    if rows.len() != canonical_changes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "expected {} canonical changes for {} untracked live rows when building untracked visibility",
                canonical_changes.len(),
                rows.len()
            ),
        ));
    }

    Ok(rows
        .iter()
        .zip(canonical_changes.iter())
        .map(|(row, change)| {
            canonical_untracked_visibility_write_from_change_visibility(
                change,
                &row.version_id,
                row.global,
                row.created_at.as_deref(),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::LixBackend;
    use crate::live_state::{load_exact_live_row, ExactLiveRowQuery, LiveRowSource};
    use crate::test_support::{init_test_backend_core, TestSqliteBackend};
    use crate::version::GLOBAL_VERSION_ID;
    use crate::{NullableKeyFilter, TransactionBeginMode, Value};

    fn value_as_i64(value: &Value) -> i64 {
        match value {
            Value::Integer(value) => *value,
            other => panic!("expected integer value, got {other:?}"),
        }
    }

    fn live_write(
        entity_id: &str,
        change_id: &str,
        untracked: bool,
        operation: LiveWriteOperation,
    ) -> LiveWriteRow {
        LiveWriteRow {
            entity_id: entity_id.to_string(),
            schema_key: "lix_key_value".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            version_id: GLOBAL_VERSION_ID.to_string(),
            global: true,
            untracked,
            plugin_key: None,
            metadata: None,
            change_id: change_id.to_string(),
            snapshot_content: Some(format!(
                "{{\"key\":\"{entity_id}\",\"value\":\"{}\"}}",
                if untracked { "untracked" } else { "tracked" }
            )),
            created_at: Some("2026-04-15T00:00:00Z".to_string()),
            updated_at: "2026-04-15T00:00:00Z".to_string(),
            operation,
        }
    }

    #[tokio::test]
    async fn apply_write_plan_handles_tracked_and_untracked_rows_in_one_pass() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");

        let plan = WritePlan {
            writes: vec![
                live_write(
                    "write-runner-tracked",
                    "tracked-change-1",
                    false,
                    LiveWriteOperation::Upsert,
                ),
                live_write(
                    "write-runner-untracked",
                    "untracked-change-1",
                    true,
                    LiveWriteOperation::Upsert,
                ),
            ],
        };

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        let outcome = apply_write_plan(transaction.as_mut(), &plan)
            .await
            .expect("write plan should apply");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(
            outcome,
            CommitOutcome {
                tracked_upserts: 1,
                tracked_tombstones: 0,
                untracked_upserts: 1,
                untracked_deletes: 0,
            }
        );

        let tracked_row = load_exact_live_row(
            &backend,
            &ExactLiveRowQuery {
                source: LiveRowSource::Tracked,
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: "write-runner-tracked".to_string(),
                file_id: NullableKeyFilter::Null,
                schema_version: None,
                plugin_key: NullableKeyFilter::Null,
                global: Some(true),
                untracked: Some(false),
                include_tombstones: false,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await
        .expect("tracked lookup should succeed")
        .expect("tracked row should exist");
        assert_eq!(tracked_row.change_id.as_deref(), Some("tracked-change-1"));

        let untracked_row = load_exact_live_row(
            &backend,
            &ExactLiveRowQuery {
                source: LiveRowSource::Untracked,
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: "write-runner-untracked".to_string(),
                file_id: NullableKeyFilter::Null,
                schema_version: None,
                plugin_key: NullableKeyFilter::Null,
                global: Some(true),
                untracked: Some(true),
                include_tombstones: false,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await
        .expect("untracked lookup should succeed")
        .expect("untracked row should exist");
        assert_eq!(
            untracked_row.change_id.as_deref(),
            Some("untracked-change-1")
        );

        let canonical_count = backend
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_untracked_change_visibility \
                 WHERE change_id = 'untracked-change-1' \
                   AND version_id = 'global' \
                   AND visibility_kind = 'global'",
                &[],
            )
            .await
            .expect("canonical visibility query should succeed");
        assert_eq!(value_as_i64(&canonical_count.rows[0][0]), 1);

        let scope_count = backend
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_untracked_change_visibility \
                 WHERE change_id = 'untracked-change-1' \
                   AND version_id = 'global' \
                   AND visibility_kind = 'global'",
                &[],
            )
            .await
            .expect("canonical scope query should succeed");
        assert_eq!(value_as_i64(&scope_count.rows[0][0]), 1);
    }
}
