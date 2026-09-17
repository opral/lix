//! Bound the v72 schema amendment to its one authorized live-row change.
//! History qualification is separate; this includes history-free working rows.
use std::collections::BTreeMap;

use crate::LixError;
use crate::branch::{BranchHeadControl, BranchHeadControlContext};
use crate::hot_state::{MaterializedHotStateRow, TrackedHeadContext};
use crate::storage_adapter::{Storage, StorageAdapter};
use crate::tracked_state::{TrackedStateKey, TrackedStateReadColumns, TrackedStateScanRequest};

use super::MigrationOptions;

type Rows =
    BTreeMap<(String, TrackedStateKey), (MaterializedHotStateRow, Option<serde_json::Value>)>;

pub(super) struct Witness {
    controls: BTreeMap<String, BranchHeadControl>,
    rows: Rows,
}

fn failure(message: &str) -> LixError {
    LixError::new("LIX_MIGRATION_PRESERVATION_FAILED", message)
}

pub(super) async fn capture<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
) -> Result<Witness, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let controls = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?;
    let mut rows = BTreeMap::new();
    let mut bytes = 0usize;
    if controls.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            "account amendment branch witness exceeds bounds",
        ));
    }
    for (branch, control) in &controls {
        let batch = TrackedHeadContext::new()
            .reader(read.clone())
            .scan_live_batch_for_retention(
                branch,
                *control,
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        include_tombstones: true,
                        ..Default::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec![
                            "snapshot".into(),
                            "metadata".into(),
                            "created_at".into(),
                            "updated_at".into(),
                            "change_id".into(),
                            "commit_id".into(),
                        ],
                    },
                    limit: Some(
                        options
                            .max_changes
                            .saturating_sub(rows.len())
                            .saturating_add(1),
                    ),
                    ..Default::default()
                },
                None,
            )
            .await?;
        for row in batch.iter() {
            let snapshot = row.snapshot_json_value()?;
            let mut owned = row.to_owned();
            // JSON/typed storage representations are interchangeable; compare
            // the logical snapshot below and every other public row property.
            owned.snapshot_content = None;
            bytes = bytes
                .saturating_add(branch.len())
                .saturating_add(owned.schema_key.len())
                .saturating_add(owned.file_id.as_ref().map_or(0, String::len))
                .saturating_add(format!("{:?}", owned.row_pk).len())
                .saturating_add(
                    owned
                        .metadata
                        .as_ref()
                        .map_or(0, |value| value.as_str().len()),
                )
                .saturating_add(
                    serde_json::to_vec(&snapshot)
                        .map_err(|_| failure("cannot encode amendment snapshot witness"))?
                        .len(),
                );
            if rows.len() >= options.max_changes || bytes > options.max_preflight_bytes {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                    "account amendment row witness exceeds bounds",
                ));
            }
            let key = TrackedStateKey {
                schema_key: owned.schema_key.clone(),
                file_id: owned.file_id.clone(),
                row_pk: owned.row_pk.clone(),
            };
            if rows
                .insert((branch.clone(), key), (owned, snapshot))
                .is_some()
            {
                return Err(failure(
                    "account amendment witness has duplicate live row identities",
                ));
            }
        }
    }
    read.finish()?;
    Ok(Witness {
        controls: controls.into_iter().collect(),
        rows,
    })
}

impl Witness {
    pub(super) async fn verify<S>(
        &self,
        adapter: &StorageAdapter<S>,
        options: MigrationOptions,
        target_schema: &serde_json::Value,
    ) -> Result<(), LixError>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let after = capture(adapter, options).await?;
        if self.controls.keys().ne(after.controls.keys()) || self.rows.keys().ne(after.rows.keys())
        {
            return Err(failure(
                "account amendment changed branch or live row identities",
            ));
        }
        for (branch, before) in &self.controls {
            let current = &after.controls[branch];
            if before.working_diff_checkpoint_commit_id != current.working_diff_checkpoint_commit_id
                || before.created_at != current.created_at
            {
                return Err(failure(
                    "account amendment changed a branch checkpoint or creation identity",
                ));
            }
        }
        for (key, (before, snapshot)) in &self.rows {
            let (current, current_snapshot) = &after.rows[key];
            if before == current && snapshot == current_snapshot {
                continue;
            }
            if before.schema_key != "lix_registered_schema"
                || before.file_id.is_some()
                || before.row_pk.as_single_string_owned().ok().as_deref() != Some("lix_account")
                || before.deleted
                || current.deleted
                || before.untracked
                || current.untracked
            {
                return Err(failure(
                    "account amendment changed an unrelated tracked or untracked live row",
                ));
            }
            let mut expected = snapshot
                .clone()
                .ok_or_else(|| failure("account schema source snapshot is missing"))?;
            let value = expected
                .as_object_mut()
                .and_then(|row| row.get_mut("value"))
                .ok_or_else(|| failure("account schema source value is missing"))?;
            *value = target_schema.clone();
            if current_snapshot.as_ref() != Some(&expected) {
                return Err(failure(
                    "account schema amendment differs from the exact authorized target",
                ));
            }
            let mut allowed = before.clone();
            allowed.updated_at = current.updated_at;
            allowed.change_id = current.change_id;
            allowed.commit_id = current.commit_id;
            if &allowed != current {
                return Err(failure(
                    "account amendment changed protected schema-row properties",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn amendment_witness_rejects_unrelated_tracked_and_untracked_changes() {
        for untracked in [false, true] {
            let storage = crate::storage_adapter::StorageSession::acquire(crate::Memory::new())
                .await
                .unwrap();
            let lix = crate::open_lix()
                .with_storage(storage.clone())
                .await
                .unwrap();
            lix.execute(
                "INSERT INTO lix_key_value(key,value,lixcol_untracked) VALUES('protected','original',$1)",
                &[crate::Value::Boolean(untracked)],
            ).await.unwrap();
            let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
                .await
                .unwrap();
            let options = MigrationOptions::default();
            let witness = capture(&adapter, options).await.unwrap();
            let target = crate::schema::seed_schema_definition("lix_account").unwrap();
            witness.verify(&adapter, options, target).await.unwrap();
            lix.execute(
                "UPDATE lix_key_value SET value='changed' WHERE key='protected'",
                &[],
            )
            .await
            .unwrap();
            let error = witness.verify(&adapter, options, target).await.unwrap_err();
            assert_eq!(error.code, "LIX_MIGRATION_PRESERVATION_FAILED");
            lix.close().await.unwrap();
        }
    }
}
