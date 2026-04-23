use crate::canonical::{
    append_changes, checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot,
    CanonicalChangeWrite, CanonicalJson, CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
};
use crate::functions::LixFunctionProvider;
use crate::live_state::{finalize_live_state_after_immediate_write, write_live_rows, LiveRow};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

const CHECKPOINT_LABEL_SCHEMA_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointCommitLabelWrite {
    pub(crate) commit_id: String,
    pub(crate) change_id: String,
    pub(crate) created_at: String,
}

pub(crate) async fn append_checkpoint_commit_label_fact_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    functions: &mut dyn LixFunctionProvider,
    write: &CheckpointCommitLabelWrite,
) -> Result<(), LixError> {
    let snapshot_content = checkpoint_commit_label_snapshot(&write.commit_id);
    let canonical_change = checkpoint_commit_label_change(write, &snapshot_content)?;
    let live_row = checkpoint_commit_label_live_row(write, &snapshot_content);

    append_changes(
        transaction,
        std::slice::from_ref(&canonical_change),
        functions,
    )
    .await?;
    write_live_rows(transaction, &[live_row]).await?;
    finalize_live_state_after_immediate_write(transaction).await?;
    Ok(())
}

fn checkpoint_commit_label_change(
    write: &CheckpointCommitLabelWrite,
    snapshot_content: &str,
) -> Result<CanonicalChangeWrite, LixError> {
    Ok(CanonicalChangeWrite {
        id: write.change_id.clone(),
        entity_id: checkpoint_commit_label_entity_id(&write.commit_id)
            .try_into()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "invalid checkpoint commit label entity_id for commit '{}'",
                        write.commit_id
                    ),
                )
            })?,
        schema_key: CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY
            .to_string()
            .try_into()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "invalid checkpoint commit label schema key '{}'",
                        CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY
                    ),
                )
            })?,
        schema_version: CHECKPOINT_LABEL_SCHEMA_VERSION
            .to_string()
            .try_into()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "invalid checkpoint commit label schema version '{}'",
                        CHECKPOINT_LABEL_SCHEMA_VERSION
                    ),
                )
            })?,
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(CanonicalJson::from_text(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "generated checkpoint commit label snapshot is invalid canonical JSON: {}",
                    error.description
                ),
            )
        })?),
        metadata: None,
        created_at: write.created_at.clone(),
    })
}

fn checkpoint_commit_label_live_row(
    write: &CheckpointCommitLabelWrite,
    snapshot_content: &str,
) -> LiveRow {
    LiveRow {
        entity_id: checkpoint_commit_label_entity_id(&write.commit_id),
        file_id: None,
        schema_key: CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY.to_string(),
        schema_version: CHECKPOINT_LABEL_SCHEMA_VERSION.to_string(),
        version_id: GLOBAL_VERSION_ID.to_string(),
        plugin_key: None,
        metadata: None,
        change_id: Some(write.change_id.clone()),
        commit_id: Some(write.commit_id.clone()),
        global: true,
        untracked: false,
        created_at: Some(write.created_at.clone()),
        updated_at: Some(write.created_at.clone()),
        snapshot_content: Some(snapshot_content.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::SystemFunctionProvider;
    use crate::live_state::{load_exact_live_row, ExactLiveRowQuery, LiveRowSource};
    use crate::test_support::boot_test_engine;
    use crate::{LixBackend, NullableKeyFilter, TransactionBeginMode, Value};

    fn value_as_text(value: &Value) -> &str {
        match value {
            Value::Text(text) => text,
            other => panic!("expected text value, got {other:?}"),
        }
    }

    fn value_as_i64(value: &Value) -> i64 {
        match value {
            Value::Integer(number) => *number,
            other => panic!("expected integer value, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_checkpoint_commit_label_fact_writes_tracked_global_state() {
        let (backend, _lix, _session) = boot_test_engine()
            .await
            .expect("boot_test_engine should succeed");
        let write = CheckpointCommitLabelWrite {
            commit_id: "phase1-checkpoint-commit".to_string(),
            change_id: "phase1-checkpoint-change".to_string(),
            created_at: "2026-04-16T00:00:00Z".to_string(),
        };
        let expected_entity_id = checkpoint_commit_label_entity_id(&write.commit_id);
        let expected_snapshot = checkpoint_commit_label_snapshot(&write.commit_id);

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("write transaction should begin");
        let mut functions = SystemFunctionProvider;
        append_checkpoint_commit_label_fact_in_transaction(
            transaction.as_mut(),
            &mut functions,
            &write,
        )
        .await
        .expect("checkpoint commit label append should succeed");
        transaction
            .commit()
            .await
            .expect("write transaction should commit");

        let canonical_rows = backend
            .execute(
                "SELECT c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content, c.created_at \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.id = $1",
                &[Value::Text(write.change_id.clone())],
            )
            .await
            .expect("canonical change query should succeed");
        assert_eq!(canonical_rows.rows.len(), 1);
        let row = &canonical_rows.rows[0];
        assert_eq!(value_as_text(&row[0]), expected_entity_id);
        assert_eq!(value_as_text(&row[1]), CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY);
        assert_eq!(value_as_text(&row[2]), CHECKPOINT_LABEL_SCHEMA_VERSION);
        assert!(matches!(row[3], Value::Null));
        assert!(matches!(row[4], Value::Null));
        assert_eq!(value_as_text(&row[5]), expected_snapshot);
        assert_eq!(value_as_text(&row[6]), write.created_at);

        let live_row = load_exact_live_row(
            &backend,
            &ExactLiveRowQuery {
                source: LiveRowSource::Tracked,
                schema_key: CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: expected_entity_id.clone(),
                file_id: NullableKeyFilter::Null,
                schema_version: Some(CHECKPOINT_LABEL_SCHEMA_VERSION.to_string()),
                plugin_key: NullableKeyFilter::Null,
                global: Some(true),
                untracked: Some(false),
                include_tombstones: false,
                include_global_overlay: true,
                include_untracked_overlay: false,
            },
        )
        .await
        .expect("exact live row query should succeed")
        .expect("tracked live row should exist");
        assert_eq!(
            live_row.change_id.as_deref(),
            Some(write.change_id.as_str())
        );
        assert_eq!(
            live_row.snapshot_content.as_deref(),
            Some(expected_snapshot.as_str())
        );
        assert_eq!(live_row.version_id, GLOBAL_VERSION_ID);
        assert!(live_row.global);
        assert!(!live_row.untracked);

        let visibility_rows = backend
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_untracked_change_visibility \
                 WHERE change_id = $1",
                &[Value::Text(write.change_id.clone())],
            )
            .await
            .expect("visibility query should succeed");
        assert_eq!(visibility_rows.rows.len(), 1);
        assert_eq!(value_as_i64(&visibility_rows.rows[0][0]), 0);
    }
}
