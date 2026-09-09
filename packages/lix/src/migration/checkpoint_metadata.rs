//! Upgrade the legacy global checkpoint inventory without rewriting history.
//!
//! The open path runs this work in a hidden epoch. The arity rewrite has its
//! own fence so direct/restarted migration cannot advertise v7 records to a
//! v77 reader. The final metadata publication and repository marker are atomic.
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::branch::BranchHeadControlContext;
use crate::changelog::{CommitId, CommitRecord};
use crate::storage_adapter::{
    SharedStorageAdapterRead, Storage, StorageAdapter, StorageBeginScanOptions,
    StorageCoreProjection, StorageKeyRange, StorageProjectedValue, StorageReadOptions,
};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns, TrackedStateScanRequest,
};
use crate::{LixError, storage_codec};

use super::api::MigrationOptions;
use super::publish::{PublicationPlan, publish};

/// Exact v6 packed arity. Never add defaults to the canonical v7 decoder:
/// legacy data is accepted only by the fenced migration path.
#[derive(Debug, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(super) struct CommitRecordV6 {
    pub(super) format_version: u32,
    pub(super) commit_id: CommitId,
    pub(super) generation: u64,
    pub(super) parent_commit_ids: Vec<CommitId>,
    #[musli(with = storage_codec::option)]
    pub(super) base_commit_id: Option<CommitId>,
    pub(super) first_parent_jump_commit_id: CommitId,
    pub(super) first_parent_jump_span: u64,
    pub(super) account_id: String,
    pub(super) created_at: crate::common::LixTimestamp,
    pub(super) touched_scope_digest: crate::changelog::CommitTouchedScopeDigest,
}

pub(super) fn decode_v6(bytes: &[u8]) -> Option<CommitRecord> {
    let record = storage_codec::decode::<CommitRecordV6>("legacy v6 commit record", bytes).ok()?;
    (record.format_version == 6).then(|| CommitRecord {
        format_version: crate::changelog::COMMIT_RECORD_FORMAT_VERSION,
        commit_id: record.commit_id,
        generation: record.generation,
        parent_commit_ids: record.parent_commit_ids,
        base_commit_id: record.base_commit_id,
        first_parent_jump_commit_id: record.first_parent_jump_commit_id,
        first_parent_jump_span: record.first_parent_jump_span,
        account_id: record.account_id,
        created_at: record.created_at,
        touched_scope_digest: record.touched_scope_digest,
        is_checkpoint: false,
    })
}

fn failure(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message)
}

fn limit() -> LixError {
    LixError::new(
        "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
        "checkpoint metadata migration exceeds configured bounds",
    )
}

/// Read record bytes directly; graph readers intentionally reject the old arity.
async fn records<S: crate::storage_adapter::StorageAdapterRead>(
    read: &S,
    options: MigrationOptions,
) -> Result<BTreeMap<CommitId, (Vec<u8>, CommitRecord)>, LixError> {
    let mut scan = read
        .begin_scan(
            crate::changelog::COMMIT_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..Default::default()
            },
        )
        .await?;
    let mut result = BTreeMap::new();
    let mut bytes = 0usize;
    while let Some(entries) = scan.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(failure("checkpoint migration commit scan omitted a value"));
            };
            bytes = bytes.checked_add(value.len()).ok_or_else(limit)?;
            if result.len() >= options.max_changes || bytes > options.max_preflight_bytes {
                return Err(limit());
            }
            let record = storage_codec::decode::<CommitRecord>("commit record", &value)
                .ok()
                .filter(|r| r.format_version == crate::changelog::COMMIT_RECORD_FORMAT_VERSION)
                .or_else(|| decode_v6(&value))
                .ok_or_else(|| {
                    failure("checkpoint migration encountered an invalid v6/v7 commit record")
                })?;
            let id = record.commit_id;
            if entry.key.0.as_ref() != crate::changelog::commit_key(id).as_slice() {
                return Err(failure(format!(
                    "checkpoint migration record key does not match commit {id}"
                )));
            }
            if result.insert(id, (entry.key.0.to_vec(), record)).is_some() {
                return Err(failure(format!(
                    "checkpoint migration found duplicate commit '{id}'"
                )));
            }
        }
    }
    Ok(result)
}

pub(super) async fn migrate<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
) -> Result<u64, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let marker = super::api::load_repository_protocol_marker(adapter)
        .await?
        .ok_or_else(|| failure("checkpoint migration has no repository marker"))?;
    if marker.as_ref() == crate::init::REPOSITORY_PROTOCOL_V77 {
        let read =
            SharedStorageAdapterRead::new(adapter.begin_read(StorageReadOptions::default()).await?);
        let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
        let records = records(&read, options).await?;
        let mut plan = PublicationPlan::bounded(options.max_changes, options.max_preflight_bytes);
        let replacements = records
            .into_values()
            .map(|(key, record)| {
                storage_codec::encode("commit record", &record).map(|value| (key, value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        plan.put_mutable(crate::changelog::COMMIT_SPACE, replacements)?;
        read.finish()?;
        publish(
            adapter,
            revision,
            crate::init::REPOSITORY_PROTOCOL_V77,
            crate::init::REPOSITORY_PROTOCOL_V77_CHECKPOINT_REWRITE,
            plan,
        )
        .await?;
    } else if marker.as_ref() != crate::init::REPOSITORY_PROTOCOL_V77_CHECKPOINT_REWRITE {
        return Err(failure(
            "checkpoint migration observed an unexpected repository marker",
        ));
    }

    let read =
        SharedStorageAdapterRead::new(adapter.begin_read(StorageReadOptions::default()).await?);
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    let mut records = records(&read, options).await?;
    let controls = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?;
    let global_head = controls
        .iter()
        .find(|(id, _)| id == crate::GLOBAL_BRANCH_ID)
        .ok_or_else(|| failure("checkpoint migration found no global branch"))?
        .1
        .head_commit_id;
    let mut reader = TrackedStateContext::new().reader(read.clone());
    // Marker PK is the checkpoint commit UUID. Identity/header-only reading
    // does not require registering the retired lix_checkpoint schema.
    let markers = reader
        .scan_batch_at_commit(
            &global_head.to_string(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec!["lix_checkpoint".to_owned()],
                    ..Default::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["row_pk".to_owned(), "created_at".to_owned()],
                },
                limit: Some(options.max_changes.saturating_add(1)),
            },
        )
        .await?
        .into_rows();
    if markers.len() > options.max_changes {
        return Err(limit());
    }
    let mut inventory = std::collections::BTreeSet::new();
    let mut timestamps = Vec::new();
    let mut changed_day_buckets = 0u64;
    for marker in markers {
        if marker.deleted {
            continue;
        }
        let parts = marker.row_pk.into_parts();
        let [id] = parts.as_slice() else {
            return Err(failure("legacy checkpoint marker has invalid identity"));
        };
        let commit_id = id
            .parse::<CommitId>()
            .map_err(|error| failure(format!("invalid checkpoint marker '{id}': {error}")))?;
        let (_, record) = records.get_mut(&commit_id).ok_or_else(|| failure(format!(
            "checkpoint marker references missing commit '{commit_id}'; hydrate the repository before retrying migration"
        )))?;
        let commit_created_at = record.created_at.to_string();
        if marker.created_at.get(..10) != commit_created_at.get(..10) {
            changed_day_buckets += 1;
        }
        timestamps.push(serde_json::json!({"commit_id": commit_id.to_string(), "legacy_marker_created_at": marker.created_at, "commit_created_at": commit_created_at}));
        record.is_checkpoint = true;
        inventory.insert(commit_id);
    }
    // Validate the immutable descriptors needed to interpret checkpoints,
    // deduplicating shared bases and captured sources. No state rows or file
    // bytes are materialized: this migration changes metadata, not snapshots.
    let mut pending = inventory.iter().copied().collect::<Vec<_>>();
    let mut validated = std::collections::BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !validated.insert(id) {
            continue;
        }
        if validated.len() > options.max_changes {
            return Err(limit());
        }
        let manifest = crate::tracked_state::load_commit_state_manifest(&read, id).await
            .map_err(|error| failure(format!("checkpoint dependency '{id}' is invalid: {error}")))?
            .ok_or_else(|| failure(format!("checkpoint state dependency '{id}' is missing; hydrate before retrying migration")))?;
        if let Some(record) = records.get(&id).map(|(_, record)| record) {
            if let Some(base) = record.base_commit_id {
                if !records.contains_key(&base) {
                    return Err(failure(format!(
                        "checkpoint semantic base '{base}' is missing; hydrate before retrying migration"
                    )));
                }
                pending.push(base);
            }
            if manifest.snapshot_root.is_none() {
                pending.extend(record.parent_commit_ids.first().copied());
            }
        }
        pending.extend(manifest.mutations.selected_source_commit_id());
    }
    // The flag itself is the durable retention root under v78. Do not drop
    // marker-derived state until every corresponding flag shares publication.
    let actual = records
        .values()
        .filter(|(_, r)| r.is_checkpoint)
        .map(|(_, r)| r.commit_id)
        .collect::<std::collections::BTreeSet<_>>();
    if actual != inventory {
        return Err(failure(
            "checkpoint flag inventory differs from the legacy live marker inventory",
        ));
    }
    let rewritten = records.len() as u64;
    let mut plan = PublicationPlan::bounded(
        options.max_changes.saturating_mul(2).saturating_add(1),
        options.max_preflight_bytes,
    );
    plan.put_mutable(crate::init::REPOSITORY_PROTOCOL_SPACE, vec![(b"checkpoint-migration.v78".to_vec(),
        serde_json::to_vec(&serde_json::json!({"checkpoint_count": inventory.len(), "changed_day_buckets": changed_day_buckets, "timestamps": timestamps})).map_err(|error| failure(error.to_string()))?
    )])?;
    plan.replace_mutable_space(
        crate::checkpoint::CHECKPOINT_INVENTORY_SPACE,
        inventory
            .into_iter()
            .map(|id| (id.as_uuid().as_bytes().to_vec(), Vec::new()))
            .collect(),
    )?;
    plan.put_mutable(
        crate::changelog::COMMIT_SPACE,
        records
            .into_values()
            .map(|(key, record)| {
                storage_codec::encode("commit record", &record).map(|value| (key, value))
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    drop(reader);
    read.finish()?;
    publish(
        adapter,
        revision,
        crate::init::REPOSITORY_PROTOCOL_V77_CHECKPOINT_REWRITE,
        crate::init::REPOSITORY_PROTOCOL_VALUE,
        plan,
    )
    .await?;
    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Memory;
    use crate::storage_adapter::{StorageAdapterRead as _, StorageWriteOptions};
    use crate::tracked_state::MaterializedTrackedStateRow;

    fn v6(record: &CommitRecord) -> CommitRecordV6 {
        CommitRecordV6 {
            format_version: 6,
            commit_id: record.commit_id,
            generation: record.generation,
            parent_commit_ids: record.parent_commit_ids.clone(),
            base_commit_id: record.base_commit_id,
            first_parent_jump_commit_id: record.first_parent_jump_commit_id,
            first_parent_jump_span: record.first_parent_jump_span,
            account_id: record.account_id.clone(),
            created_at: record.created_at,
            touched_scope_digest: record.touched_scope_digest.clone(),
        }
    }

    async fn normalize_fixture_records(adapter: &StorageAdapter<Memory>) {
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut scan = read
            .begin_scan(
                crate::changelog::COMMIT_SPACE,
                StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut replacement = Vec::new();
        while let Some(entries) = scan.next_chunk().await.unwrap() {
            for entry in entries {
                let StorageProjectedValue::FullValue(value) = entry.value else {
                    panic!("full value");
                };
                let mut record: CommitRecord =
                    storage_codec::decode("test record", &value).unwrap();
                record.format_version = crate::changelog::COMMIT_RECORD_FORMAT_VERSION;
                replacement.push((
                    entry.key.0.to_vec(),
                    storage_codec::encode("commit record", &record).unwrap(),
                ));
            }
        }
        drop(scan);
        drop(read);
        use crate::storage_adapter::{
            PutBatch, PutEntry, StorageKey, StorageValue, StorageWrite as _,
        };
        let mut write = adapter
            .begin_migration_write(StorageWriteOptions::default())
            .await
            .unwrap();
        write
            .put_many(
                crate::changelog::COMMIT_SPACE,
                PutBatch {
                    entries: replacement
                        .into_iter()
                        .map(|(key, value)| PutEntry {
                            key: StorageKey(key.into()),
                            value: StorageValue {
                                bytes: value.into(),
                            },
                        })
                        .collect(),
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
    }

    async fn downgrade(adapter: &StorageAdapter<Memory>) -> BTreeMap<CommitId, CommitRecord> {
        normalize_fixture_records(adapter).await;
        let read = SharedStorageAdapterRead::new(
            adapter
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
        );
        let records = records(&read, MigrationOptions::automatic()).await.unwrap();
        let before = records
            .iter()
            .map(|(id, (_, record))| (*id, record.clone()))
            .collect();
        let revision = crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap();
        let mut plan = PublicationPlan::default();
        plan.put_mutable(
            crate::changelog::COMMIT_SPACE,
            records
                .into_values()
                .map(|(key, record)| {
                    (
                        key,
                        storage_codec::encode("v6 commit record", &v6(&record)).unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap();
        read.finish().unwrap();
        publish(
            adapter,
            revision,
            crate::init::REPOSITORY_PROTOCOL_VALUE,
            crate::init::REPOSITORY_PROTOCOL_V77,
            plan,
        )
        .await
        .unwrap();
        before
    }

    async fn stage_empty(adapter: &StorageAdapter<Memory>, id: CommitId) {
        let mut read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut writes = adapter.new_write_set();
        crate::test_support::stage_tracked_root_from_materialized(
            &mut read,
            &mut writes,
            &TrackedStateContext::new(),
            &id.to_string(),
            None,
            &[],
        )
        .await
        .unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
    }

    async fn fixture(missing: bool) -> (StorageAdapter<Memory>, CommitId, CommitId) {
        let adapter = StorageAdapter::new(Memory::new());
        let checkpoint = CommitId::for_test_label("legacy-off-branch-checkpoint");
        let ordinary = CommitId::for_test_label("legacy-ordinary");
        if !missing {
            stage_empty(&adapter, checkpoint).await;
        }
        stage_empty(&adapter, ordinary).await;
        let global = CommitId::for_test_label("legacy-marker-publication");
        let marker = MaterializedTrackedStateRow {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(&checkpoint.to_string()).unwrap(),
            schema_key: "lix_checkpoint".to_owned(),
            file_id: None,
            snapshot_content: Some(
                serde_json::json!({"commit_id": checkpoint.to_string()})
                    .to_string()
                    .into(),
            ),
            decoded_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "2026-09-09T12:00:00.000Z".to_owned(),
            updated_at: "2026-09-09T12:00:00.000Z".to_owned(),
            change_id: crate::changelog::ChangeId::for_test_label("legacy-marker"),
            commit_id: global,
        };
        crate::test_support::seed_branch_head_with_rows(
            adapter.clone(),
            crate::GLOBAL_BRANCH_ID,
            &global.to_string(),
            &[marker],
        )
        .await;
        let mut writes = adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            crate::init::REPOSITORY_PROTOCOL_VALUE,
        );
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        (adapter, checkpoint, ordinary)
    }

    #[tokio::test]
    async fn v77_flags_preserve_ids_state_controls_and_off_branch_inventory() {
        let (adapter, checkpoint, ordinary) = fixture(false).await;
        let before = downgrade(&adapter).await;
        let read = SharedStorageAdapterRead::new(
            adapter
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
        );
        let controls = BranchHeadControlContext::new()
            .reader(read.clone())
            .scan()
            .await
            .unwrap();
        let manifests = crate::tracked_state::scan_commit_state_manifest_commit_ids(&read)
            .await
            .unwrap();
        read.finish().unwrap();
        migrate(&adapter, MigrationOptions::default())
            .await
            .unwrap();
        let read = SharedStorageAdapterRead::new(
            adapter
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
        );
        let after = records(&read, MigrationOptions::default()).await.unwrap();
        assert_eq!(after.len(), before.len());
        for (id, mut expected) in before {
            expected.is_checkpoint = id == checkpoint;
            assert_eq!(after[&id].1, expected, "only checkpoint membership changes");
        }
        assert!(!after[&ordinary].1.is_checkpoint);
        assert_eq!(
            BranchHeadControlContext::new()
                .reader(read.clone())
                .scan()
                .await
                .unwrap(),
            controls
        );
        assert_eq!(
            crate::tracked_state::scan_commit_state_manifest_commit_ids(&read)
                .await
                .unwrap(),
            manifests
        );
        assert_eq!(
            crate::checkpoint::checkpoint_commit_ids(&read)
                .await
                .unwrap(),
            [checkpoint].into_iter().collect()
        );
        let global = controls
            .iter()
            .find(|(id, _)| id == crate::GLOBAL_BRANCH_ID)
            .unwrap()
            .1
            .head_commit_id;
        let marker = TrackedStateContext::new()
            .reader(read.clone())
            .scan_batch_at_commit(
                &global.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["lix_checkpoint".to_owned()],
                        ..Default::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["row_pk".to_owned(), "created_at".to_owned()],
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_rows();
        assert_eq!(marker.len(), 1, "old immutable marker state remains inert");
        assert_eq!(marker[0].created_at, "2026-09-09T12:00:00.000Z");
        assert_ne!(
            after[&checkpoint].1.created_at.to_string(),
            marker[0].created_at
        );
        let audit = crate::storage_adapter::PointReadPlan::new(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            &[crate::storage_adapter::StorageKey(
                bytes::Bytes::from_static(b"checkpoint-migration.v78"),
            )],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .into_iter()
        .next()
        .flatten()
        .unwrap();
        let StorageProjectedValue::FullValue(audit) = audit else {
            panic!("audit full value");
        };
        let audit: serde_json::Value = serde_json::from_slice(&audit).unwrap();
        assert_eq!(audit["checkpoint_count"], 1);
        assert_eq!(audit["changed_day_buckets"], 1);
        assert_eq!(
            audit["timestamps"][0]["legacy_marker_created_at"],
            marker[0].created_at
        );
        assert_eq!(
            audit["timestamps"][0]["commit_created_at"],
            after[&checkpoint].1.created_at.to_string()
        );
    }

    #[tokio::test]
    async fn missing_checkpoint_fails_under_fence_and_resumes_after_hydration() {
        let (adapter, checkpoint, _) = fixture(true).await;
        downgrade(&adapter).await;
        let error = migrate(&adapter, MigrationOptions::default())
            .await
            .unwrap_err();
        assert!(error.message.contains("missing commit"));
        assert_eq!(
            super::super::api::load_repository_protocol_marker(&adapter)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            crate::init::REPOSITORY_PROTOCOL_V77_CHECKPOINT_REWRITE
        );
        // Recover the missing dependency without recopying already-upgraded bytes.
        stage_empty(&adapter, checkpoint).await;
        normalize_fixture_records(&adapter).await;
        migrate(&adapter, MigrationOptions::default())
            .await
            .unwrap();
        assert_eq!(
            super::super::api::inspect_lix_with_adapter(&adapter)
                .await
                .unwrap(),
            super::super::api::MigrationStatus::Current { version: 78 }
        );
    }

    #[tokio::test]
    async fn arity_preflight_limit_does_not_publish_partial_records() {
        let (adapter, _, _) = fixture(false).await;
        downgrade(&adapter).await;
        let error = migrate(
            &adapter,
            MigrationOptions {
                max_changes: 1,
                max_preflight_bytes: usize::MAX,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
        assert_eq!(
            super::super::api::load_repository_protocol_marker(&adapter)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            crate::init::REPOSITORY_PROTOCOL_V77
        );
        migrate(&adapter, MigrationOptions::default())
            .await
            .unwrap();
    }
}
