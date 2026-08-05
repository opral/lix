use std::collections::BTreeSet;

use bytes::Bytes;

use crate::binary_cas::BlobId;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions,
    StorageKey, StoragePrefix, StorageProjectedValue, StorageScanOptions, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::storage_codec;
use crate::{Blob, LixError};

pub(crate) const PLUGIN_CHECKPOINT_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0004_0026), "plugin.current_checkpoint.v1");
pub(crate) const PLUGIN_CHECKPOINT_RECLAMATION_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0034),
    "plugin.current_checkpoint_reclamation.v1",
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct PluginCheckpointReclamation {
    generation: [u8; 16],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct PluginCheckpointReclamationKey {
    branch_id: [u8; 16],
    generation: [u8; 16],
}

const MAGIC: &[u8; 4] = b"LPC2";
const HEADER_BYTES: usize = 4 + 32 + 32 + 16 + 4 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentPluginCheckpoint {
    pub(crate) runtime: Blob,
    pub(crate) authority: Blob,
}

pub(crate) fn stage_current_plugin_checkpoint(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    file_id: &str,
    generation: &str,
    semantic_root: &str,
    blob_hash: BlobId,
    runtime: &[u8],
    authority: &[u8],
) -> Result<(), LixError> {
    let generation = BlobId::from_hex(generation)?;
    let semantic_root = parse_semantic_root(semantic_root)?;
    let runtime_len = u32::try_from(runtime.len()).map_err(|_| checkpoint_too_large())?;
    let authority_len = u32::try_from(authority.len()).map_err(|_| checkpoint_too_large())?;
    let capacity = HEADER_BYTES
        .checked_add(runtime.len())
        .and_then(|length| length.checked_add(authority.len()))
        .ok_or_else(checkpoint_too_large)?;
    let mut value = Vec::with_capacity(capacity);
    value.extend_from_slice(MAGIC);
    value.extend_from_slice(generation.as_bytes());
    value.extend_from_slice(blob_hash.as_bytes());
    value.extend_from_slice(semantic_root.as_bytes());
    value.extend_from_slice(&runtime_len.to_le_bytes());
    value.extend_from_slice(&authority_len.to_le_bytes());
    value.extend_from_slice(runtime);
    value.extend_from_slice(authority);
    writes.put(
        PLUGIN_CHECKPOINT_SPACE,
        checkpoint_key(branch_id, file_id)?,
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

pub(crate) async fn stage_delete_current_plugin_checkpoints(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    owners: &[(String, String)],
) -> Result<(), LixError> {
    let keys = owners
        .iter()
        .map(|(branch_id, file_id)| checkpoint_key(branch_id, file_id))
        .collect::<Result<Vec<_>, _>>()?;
    if keys.is_empty() {
        return Ok(());
    }
    let existing = PointReadPlan::new(PLUGIN_CHECKPOINT_SPACE, &keys)
        .materialize(
            read,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?
        .value;
    writes.delete_batch(
        PLUGIN_CHECKPOINT_SPACE,
        keys.into_iter()
            .zip(existing)
            .filter_map(|(key, value)| value.is_some().then_some(key)),
    );
    Ok(())
}

/// Enqueues branch-local plugin checkpoint reclamation. The foreground branch
/// lifecycle remains O(1); GC consumes the authenticated branch prefix later.
pub(crate) fn stage_enqueue_branch_plugin_checkpoint_reclamation(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: [u8; 16],
) -> Result<(), LixError> {
    let branch_id = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("plugin checkpoint branch id is not a UUID: {error}"),
        )
    })?;
    writes.put(
        PLUGIN_CHECKPOINT_RECLAMATION_SPACE,
        StorageKey(Bytes::from(storage_codec::encode(
            "plugin checkpoint reclamation key",
            &PluginCheckpointReclamationKey {
                branch_id: *branch_id.as_bytes(),
                generation,
            },
        )?)),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode(
                "plugin checkpoint reclamation",
                &PluginCheckpointReclamation { generation },
            )?),
        },
    );
    Ok(())
}

pub(crate) async fn stage_collect_branch_plugin_checkpoint_reclamations<S>(
    read: &S,
    writes: &mut StorageWriteSet,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let plan = ScanPlan::prefix(
        PLUGIN_CHECKPOINT_RECLAMATION_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    let mut reclaimed_branches = BTreeSet::<[u8; 16]>::new();
    loop {
        let page = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?
            .value;
        resume_after = page.entries.last().map(|entry| entry.key.clone());
        for entry in page.entries {
            let key: PluginCheckpointReclamationKey =
                storage_codec::decode("plugin checkpoint reclamation key", entry.key.0.as_ref())?;
            if key.generation == [0; 16] {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin checkpoint reclamation key has an invalid generation token",
                ));
            }
            let branch_id = uuid::Uuid::from_bytes(key.branch_id);
            let branch_bytes = branch_id.as_bytes();
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin checkpoint reclamation omitted its value",
                ));
            };
            let stored: PluginCheckpointReclamation =
                storage_codec::decode("plugin checkpoint reclamation", &bytes)?;
            if stored.generation != key.generation {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin checkpoint reclamation key/value generation mismatch",
                ));
            }
            let branch_id_string = branch_id.to_string();
            let control = crate::branch::BranchHeadControlContext::new()
                .reader(read)
                .load(&branch_id_string)
                .await?;
            if control.is_some() {
                // A branch may have been recreated before GC. Its new
                // checkpoints share the branch prefix, so fail closed rather
                // than deleting the recreated branch's active files.
                continue;
            }
            if reclaimed_branches.insert(key.branch_id) {
                let checkpoint_plan = ScanPlan::prefix(
                    PLUGIN_CHECKPOINT_SPACE,
                    StoragePrefix {
                        bytes: Bytes::copy_from_slice(branch_bytes),
                    },
                );
                let mut checkpoint_resume = None;
                loop {
                    let chunk = checkpoint_plan
                        .collect(
                            read,
                            StorageScanOptions {
                                projection: StorageCoreProjection::KeyOnly,
                                resume_after: checkpoint_resume.clone(),
                                ..StorageScanOptions::default()
                            },
                        )
                        .await?
                        .value;
                    checkpoint_resume = chunk.entries.last().map(|item| item.key.clone());
                    writes.delete_batch(
                        PLUGIN_CHECKPOINT_SPACE,
                        chunk.entries.into_iter().map(|item| item.key),
                    );
                    if !chunk.has_more || checkpoint_resume.is_none() {
                        break;
                    }
                }
            }
            writes.delete(PLUGIN_CHECKPOINT_RECLAMATION_SPACE, entry.key);
        }
        if !page.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

pub(crate) async fn load_current_plugin_checkpoint(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    file_id: &str,
    generation: &str,
    semantic_root: &str,
    blob_hash: BlobId,
) -> Result<Option<CurrentPluginCheckpoint>, LixError> {
    let expected_generation = BlobId::from_hex(generation)?;
    let expected_semantic_root = parse_semantic_root(semantic_root)?;
    let values = PointReadPlan::new(
        PLUGIN_CHECKPOINT_SPACE,
        &[checkpoint_key(branch_id, file_id)?],
    )
    .materialize(
        read,
        StorageGetOptions {
            projection: StorageCoreProjection::FullValue,
        },
    )
    .await?
    .value;
    let Some(StorageProjectedValue::FullValue(value)) = values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let Some(header) = value.get(..HEADER_BYTES) else {
        return Ok(None);
    };
    if &header[..4] != MAGIC
        || header[4..36] != expected_generation.as_bytes()[..]
        || header[36..68] != blob_hash.as_bytes()[..]
        || header[68..84] != expected_semantic_root.as_bytes()[..]
    {
        return Ok(None);
    }
    let runtime_len = u32::from_le_bytes(header[84..88].try_into().expect("runtime length"));
    let authority_len = u32::from_le_bytes(header[88..92].try_into().expect("authority length"));
    let runtime_len = runtime_len as usize;
    let authority_len = authority_len as usize;
    let runtime_end = HEADER_BYTES
        .checked_add(runtime_len)
        .filter(|end| *end <= value.len());
    let value_end = runtime_end
        .and_then(|end| end.checked_add(authority_len))
        .filter(|end| *end == value.len());
    let (Some(runtime_end), Some(value_end)) = (runtime_end, value_end) else {
        return Ok(None);
    };
    Ok(Some(CurrentPluginCheckpoint {
        runtime: value.slice(HEADER_BYTES..runtime_end).into(),
        authority: value.slice(runtime_end..value_end).into(),
    }))
}

fn parse_semantic_root(semantic_root: &str) -> Result<uuid::Uuid, LixError> {
    uuid::Uuid::parse_str(semantic_root).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("plugin checkpoint semantic root is not a UUID: {error}"),
        )
    })
}

fn checkpoint_key(branch_id: &str, file_id: &str) -> Result<StorageKey, LixError> {
    let branch_id = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("plugin checkpoint branch id is not a UUID: {error}"),
        )
    })?;
    let file_id = uuid::Uuid::parse_str(file_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("plugin checkpoint file id is not a UUID: {error}"),
        )
    })?;
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(branch_id.as_bytes());
    key.extend_from_slice(file_id.as_bytes());
    Ok(StorageKey(Bytes::from(key)))
}

fn checkpoint_too_large() -> LixError {
    LixError::new(
        LixError::CODE_PLUGIN_RESOURCE_LIMIT,
        "plugin checkpoint exceeds the current-checkpoint storage limit",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    const BRANCH_ID: &str = "01920000-0000-7000-8000-000000000001";
    const OTHER_BRANCH_ID: &str = "01920000-0000-7000-8000-000000000003";
    const FILE_ID: &str = "01920000-0000-7000-8000-000000000002";
    const SEMANTIC_ROOT: &str = "01920000-0000-7000-8000-000000000004";
    const OTHER_SEMANTIC_ROOT: &str = "01920000-0000-7000-8000-000000000005";

    #[tokio::test]
    async fn current_checkpoint_overwrites_and_is_bound_to_generation_blob_and_semantic_root() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = BlobId::from_content(b"generation");
        let first_blob = BlobId::from_content(b"first");
        let second_blob = BlobId::from_content(b"second");

        for (blob_hash, runtime, authority) in [
            (
                first_blob,
                b"runtime-one".as_slice(),
                b"authority-one".as_slice(),
            ),
            (
                second_blob,
                b"runtime-two".as_slice(),
                b"authority-two".as_slice(),
            ),
        ] {
            let mut writes = storage.new_write_set();
            stage_current_plugin_checkpoint(
                &mut writes,
                BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                blob_hash,
                runtime,
                authority,
            )
            .unwrap();
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .unwrap();
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert!(
            load_current_plugin_checkpoint(
                &read,
                BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                first_blob,
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            load_current_plugin_checkpoint(
                &read,
                BRANCH_ID,
                FILE_ID,
                &BlobId::from_content(b"other-generation").to_hex(),
                SEMANTIC_ROOT,
                second_blob,
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            load_current_plugin_checkpoint(
                &read,
                BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                OTHER_SEMANTIC_ROOT,
                second_blob,
            )
            .await
            .unwrap()
            .is_none()
        );
        let checkpoint = load_current_plugin_checkpoint(
            &read,
            BRANCH_ID,
            FILE_ID,
            &generation.to_hex(),
            SEMANTIC_ROOT,
            second_blob,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(checkpoint.runtime.as_ref(), b"runtime-two");
        assert_eq!(checkpoint.authority.as_ref(), b"authority-two");
    }

    #[tokio::test]
    async fn checkpoint_cleanup_follows_file_and_branch_lifetimes() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = BlobId::from_content(b"generation");
        let blob_hash = BlobId::from_content(b"file");
        let mut writes = storage.new_write_set();
        for branch_id in [BRANCH_ID, OTHER_BRANCH_ID] {
            stage_current_plugin_checkpoint(
                &mut writes,
                branch_id,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                blob_hash,
                b"runtime",
                b"authority",
            )
            .unwrap();
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let mut writes = storage.new_write_set();
        stage_enqueue_branch_plugin_checkpoint_reclamation(
            &mut writes,
            BRANCH_ID,
            generation.as_bytes()[..16].try_into().unwrap(),
        )
        .unwrap();
        stage_enqueue_branch_plugin_checkpoint_reclamation(&mut writes, BRANCH_ID, [7; 16])
            .unwrap();
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut writes = storage.new_write_set();
        stage_collect_branch_plugin_checkpoint_reclamations(&read, &mut writes)
            .await
            .unwrap();
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert!(
            load_current_plugin_checkpoint(
                &read,
                BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                blob_hash,
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            load_current_plugin_checkpoint(
                &read,
                OTHER_BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                blob_hash,
            )
            .await
            .unwrap()
            .is_some()
        );

        let mut writes = storage.new_write_set();
        stage_delete_current_plugin_checkpoints(
            &read,
            &mut writes,
            &[(OTHER_BRANCH_ID.to_owned(), FILE_ID.to_owned())],
        )
        .await
        .unwrap();
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert!(
            load_current_plugin_checkpoint(
                &read,
                OTHER_BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                blob_hash,
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn branch_recreation_defers_plugin_checkpoint_reclamation() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = BlobId::from_content(b"generation");
        let blob_hash = BlobId::from_content(b"file");
        let mut writes = storage.new_write_set();
        stage_current_plugin_checkpoint(
            &mut writes,
            BRANCH_ID,
            FILE_ID,
            &generation.to_hex(),
            SEMANTIC_ROOT,
            blob_hash,
            b"runtime",
            b"authority",
        )
        .unwrap();
        stage_enqueue_branch_plugin_checkpoint_reclamation(
            &mut writes,
            BRANCH_ID,
            generation.as_bytes()[..16].try_into().unwrap(),
        )
        .unwrap();
        stage_branch_head_control(
            &mut writes,
            BRANCH_ID,
            BranchHeadControl {
                head_commit_id: CommitId::for_test_label("recreated-head"),
                generation: CommitId::for_test_label("recreated-generation"),
                current_state_revision: 0,
                working_diff_checkpoint_commit_id: None,
                created_at: LixTimestamp::expect_parse(
                    "plugin recreation timestamp",
                    "2026-01-01T00:00:00Z",
                ),
                updated_at: LixTimestamp::expect_parse(
                    "plugin recreation timestamp",
                    "2026-01-01T00:00:00Z",
                ),
                ref_change_id: ChangeId::for_test_label("recreated-ref"),
                schema_presence_bloom: [0; 4],
                untracked_row_count: 0,
                untracked_identity_xor: [0; 32],
            },
        )
        .unwrap();
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut gc_writes = storage.new_write_set();
        stage_collect_branch_plugin_checkpoint_reclamations(&read, &mut gc_writes)
            .await
            .unwrap();
        assert!(gc_writes.is_empty());
        assert!(
            load_current_plugin_checkpoint(
                &read,
                BRANCH_ID,
                FILE_ID,
                &generation.to_hex(),
                SEMANTIC_ROOT,
                blob_hash,
            )
            .await
            .unwrap()
            .is_some()
        );
    }
}
