//! Checkpoint recovery roots and repository garbage collection.
//!
//! Recovery refs are local, mutable roots. They deliberately live outside the
//! changelog: rotating a ref must not create history that itself keeps the
//! recovered commit alive. The checkpoint transaction stages the rotation in
//! the same storage write set that publishes the compacted checkpoint.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::binary_cas::{BinaryCasContext, BinaryCasGcSweep, BlobHash};
use crate::changelog::{
    ChangeRecordProjection, ChangelogContext, ChangelogReader, ChangelogWriter, CommitId, GcPlan,
    GcRoot, materialize_change_payloads,
};
use crate::live_state::LiveStateIndexContext;
use crate::storage_adapter::{
    ScanPlan, StorageAdapterRead, StorageKey, StoragePrefix, StorageProjectedValue,
    StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, storage_codec};

pub(crate) const CHECKPOINT_RECOVERY_REF_NAMESPACE: &str = "checkpoint.recovery_ref.v1";
pub(crate) const CHECKPOINT_RECOVERY_REF_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0008_0001),
    CHECKPOINT_RECOVERY_REF_NAMESPACE,
);

const CHECKPOINT_RECOVERY_REF_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointRecoveryRef {
    pub(crate) branch_id: String,
    pub(crate) recovered_head_commit_id: CommitId,
    pub(crate) checkpoint_commit_id: CommitId,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct CheckpointRecoveryRefKey<'a> {
    branch_id: &'a str,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredCheckpointRecoveryRef {
    format_version: u32,
    branch_id: String,
    recovered_head_commit_id: CommitId,
    checkpoint_commit_id: CommitId,
}

/// Stages one branch's recovery-root rotation.
///
/// The caller owns the surrounding transaction. Replacing the key drops the
/// prior interval from the next GC root set without ever exposing an
/// intermediate root-less checkpoint.
pub(crate) fn stage_recovery_ref_rotation(
    writes: &mut StorageWriteSet,
    recovery: &CheckpointRecoveryRef,
) -> Result<(), LixError> {
    if recovery.branch_id.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "checkpoint recovery branch id must not be empty",
        ));
    }
    let key = recovery_ref_key(&recovery.branch_id)?;
    let value = storage_codec::encode(
        "checkpoint recovery ref",
        &StoredCheckpointRecoveryRef {
            format_version: CHECKPOINT_RECOVERY_REF_FORMAT_VERSION,
            branch_id: recovery.branch_id.clone(),
            recovered_head_commit_id: recovery.recovered_head_commit_id,
            checkpoint_commit_id: recovery.checkpoint_commit_id,
        },
    )?;
    writes.put(
        CHECKPOINT_RECOVERY_REF_SPACE,
        StorageKey(Bytes::from(key)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

pub(crate) async fn load_recovery_refs(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<CheckpointRecoveryRef>, LixError> {
    let plan = ScanPlan::prefix(
        CHECKPOINT_RECOVERY_REF_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut refs = BTreeMap::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint recovery scan unexpectedly omitted its value",
                ));
            };
            let stored: StoredCheckpointRecoveryRef =
                storage_codec::decode("checkpoint recovery ref", &bytes)?;
            if stored.format_version != CHECKPOINT_RECOVERY_REF_FORMAT_VERSION {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint recovery ref for branch '{}' has unsupported format version {}",
                        stored.branch_id, stored.format_version
                    ),
                ));
            }
            let expected_key = recovery_ref_key(&stored.branch_id)?;
            if entry.key.0.as_ref() != expected_key.as_slice() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint recovery ref key does not match branch '{}'",
                        stored.branch_id
                    ),
                ));
            }
            refs.insert(
                stored.branch_id.clone(),
                CheckpointRecoveryRef {
                    branch_id: stored.branch_id,
                    recovered_head_commit_id: stored.recovered_head_commit_id,
                    checkpoint_commit_id: stored.checkpoint_commit_id,
                },
            );
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(refs.into_values().collect())
}

fn recovery_ref_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "checkpoint recovery ref key",
        &CheckpointRecoveryRefKey { branch_id },
    )
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RepositoryGcSweep {
    pub(crate) tracked_commit_roots: Vec<CommitId>,
    pub(crate) tracked_tree_chunks: Vec<[u8; 32]>,
    pub(crate) binary_cas: BinaryCasGcSweep,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RepositoryGcPlan {
    pub(crate) changelog: GcPlan,
    pub(crate) sweep: RepositoryGcSweep,
}

/// Plans and stages a complete repository sweep against one pinned read.
///
/// The caller must serialize this operation with repository writes and commit
/// `writes` atomically. Planning and mutation are deliberately separated from
/// storage commit so checkpoint/session code can retain lifecycle control.
pub(crate) async fn stage_repository_gc<S>(
    store: S,
    writes: &mut StorageWriteSet,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    let flat_rows = LiveStateIndexContext::new()
        .reader(store.clone())
        .scan_all_index_rows()
        .await?;
    let standalone_change_ids = flat_rows
        .iter()
        .map(|row| row.change_id)
        .collect::<BTreeSet<_>>();
    let flat_payloads = materialize_change_payloads(
        &store,
        standalone_change_ids.iter().copied(),
        ChangeRecordProjection::from_columns(&["snapshot_content".to_string()]),
        "garbage-collection flat live-state root",
    )
    .await?;

    let mut roots = flat_rows
        .iter()
        .map(|row| GcRoot::StandaloneChange(row.change_id))
        .collect::<Vec<_>>();
    for row in &flat_rows {
        if row.schema_key != "lix_branch_ref" {
            continue;
        }
        let payload = flat_payloads.get(&row.change_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "garbage collection could not materialize branch-ref change '{}'",
                    row.change_id
                ),
            )
        })?;
        let snapshot = parse_snapshot(
            payload.snapshot_content.as_deref(),
            "branch ref",
            row.change_id,
        )?;
        let commit_id = required_snapshot_text(&snapshot, "commit_id", "branch ref")?;
        let commit_id = CommitId::parse_lix(commit_id, "garbage-collection branch head")?;
        roots.push(GcRoot::BranchHead(commit_id));
    }
    for recovery in load_recovery_refs(&store).await? {
        roots.push(GcRoot::RecoveryHead {
            branch_id: recovery.branch_id,
            commit_id: recovery.recovered_head_commit_id,
        });
    }

    let mut changelog_store = store.clone();
    let changelog_plan = ChangelogContext::new()
        .reader(changelog_store.clone())
        .plan_gc(&roots)
        .await?;
    let live_commits = changelog_plan
        .live
        .commits
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    let all_tracked_roots = crate::tracked_state::scan_commit_roots(&store).await?;
    let roots_by_commit = all_tracked_roots
        .iter()
        .map(|root| (root.commit_id, root))
        .collect::<BTreeMap<_, _>>();
    let mut live_tracked_root_ids = Vec::with_capacity(live_commits.len());
    for commit_id in &live_commits {
        let root = roots_by_commit.get(commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("live commit '{commit_id}' has no tracked-state root"),
            )
        })?;
        live_tracked_root_ids.push(root.root_id.clone());
    }
    let sweep_tracked_commit_roots = all_tracked_roots
        .iter()
        .filter(|root| !live_commits.contains(&root.commit_id))
        .map(|root| root.commit_id)
        .collect::<Vec<_>>();
    let live_tree_chunks = crate::tracked_state::TrackedStateContext::new()
        .reachable_tree_chunk_hashes(&store, live_tracked_root_ids)
        .await?;
    let sweep_tracked_tree_chunks = crate::tracked_state::scan_tree_chunk_hashes(&store)
        .await?
        .into_iter()
        .filter(|hash| !live_tree_chunks.contains(hash))
        .collect::<Vec<_>>();

    let live_change_payloads = materialize_change_payloads(
        &store,
        changelog_plan.live.changes.iter().copied(),
        ChangeRecordProjection::from_columns(&["snapshot_content".to_string()]),
        "garbage-collection live change",
    )
    .await?;
    let live_blob_hashes = collect_live_blob_hashes(&live_change_payloads)?;
    let binary_cas = BinaryCasContext::new();
    let binary_cas_sweep = binary_cas.plan_gc(&store, &live_blob_hashes).await?;

    {
        let mut writer = ChangelogContext::new().writer(&mut changelog_store, writes);
        let staged_plan = writer.collect_garbage(&roots).await?;
        if staged_plan != changelog_plan {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "garbage-collection plan changed while staging its sweep",
            ));
        }
    }
    for commit_id in &sweep_tracked_commit_roots {
        crate::tracked_state::stage_delete_commit_root(writes, *commit_id);
    }
    for hash in &sweep_tracked_tree_chunks {
        crate::tracked_state::stage_delete_tree_chunk(writes, *hash);
    }
    binary_cas.stage_gc_sweep(writes, &binary_cas_sweep);

    Ok(RepositoryGcPlan {
        changelog: changelog_plan,
        sweep: RepositoryGcSweep {
            tracked_commit_roots: sweep_tracked_commit_roots,
            tracked_tree_chunks: sweep_tracked_tree_chunks,
            binary_cas: binary_cas_sweep,
        },
    })
}

fn collect_live_blob_hashes(
    payloads: &std::collections::HashMap<
        crate::changelog::ChangeId,
        crate::changelog::MaterializedChangePayload,
    >,
) -> Result<BTreeSet<BlobHash>, LixError> {
    let mut hashes = BTreeSet::new();
    for payload in payloads.values() {
        let Some(identity) = payload.identity.as_ref() else {
            continue;
        };
        let Some(snapshot_content) = payload.snapshot_content.as_deref() else {
            continue;
        };
        if identity.schema_key == "lix_binary_blob_ref" {
            let snapshot =
                serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("invalid live lix_binary_blob_ref snapshot: {error}"),
                    )
                })?;
            hashes.insert(BlobHash::from_hex(required_snapshot_text(
                &snapshot,
                "blob_hash",
                "lix_binary_blob_ref",
            )?)?);
            continue;
        }
        if identity.schema_key == "lix_key_value"
            && identity.entity_pk.as_single_string()? == crate::plugin::PLUGIN_REGISTRY_KEY
        {
            let snapshot =
                serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("invalid live plugin registry snapshot: {error}"),
                    )
                })?;
            if let Some(plugins) = snapshot
                .get("value")
                .and_then(|value| value.get("plugins"))
                .and_then(serde_json::Value::as_array)
            {
                for plugin in plugins {
                    for field in ["archive_blob_hash", "wasm_blob_hash"] {
                        hashes.insert(BlobHash::from_hex(required_snapshot_text(
                            plugin,
                            field,
                            "plugin registry entry",
                        )?)?);
                    }
                }
            }
        }
    }
    Ok(hashes)
}

fn parse_snapshot(
    snapshot: Option<&str>,
    kind: &str,
    change_id: crate::changelog::ChangeId,
) -> Result<serde_json::Value, LixError> {
    let snapshot = snapshot.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("live {kind} change '{change_id}' has no snapshot"),
        )
    })?;
    serde_json::from_str(snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("live {kind} change '{change_id}' has invalid JSON: {error}"),
        )
    })
}

fn required_snapshot_text<'a>(
    snapshot: &'a serde_json::Value,
    field: &str,
    kind: &str,
) -> Result<&'a str, LixError> {
    snapshot
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("{kind} snapshot is missing non-empty text field '{field}'"),
            )
        })
}

#[cfg(test)]
mod tests {
    use crate::changelog::CommitId;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::{CheckpointRecoveryRef, load_recovery_refs, stage_recovery_ref_rotation};

    #[tokio::test]
    async fn recovery_ref_rotation_replaces_only_the_target_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let first_main = recovery("main", "main-old-1", "main-checkpoint-1");
        let first_other = recovery("other", "other-old-1", "other-checkpoint-1");
        let mut writes = storage.new_write_set();
        stage_recovery_ref_rotation(&mut writes, &first_main)
            .expect("first main recovery ref should stage");
        stage_recovery_ref_rotation(&mut writes, &first_other)
            .expect("other recovery ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("initial recovery refs should commit");

        let second_main = recovery("main", "main-old-2", "main-checkpoint-2");
        let mut writes = storage.new_write_set();
        stage_recovery_ref_rotation(&mut writes, &second_main)
            .expect("second main recovery ref should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rotated recovery ref should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("recovery read should open");
        assert_eq!(
            load_recovery_refs(&read)
                .await
                .expect("recovery refs should load"),
            vec![second_main, first_other]
        );
    }

    fn recovery(branch_id: &str, recovered_head: &str, checkpoint: &str) -> CheckpointRecoveryRef {
        CheckpointRecoveryRef {
            branch_id: branch_id.to_string(),
            recovered_head_commit_id: CommitId::for_test_label(recovered_head),
            checkpoint_commit_id: CommitId::for_test_label(checkpoint),
        }
    }
}
