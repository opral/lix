use bytes::Bytes;

use crate::binary_cas::BlobId;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
    StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::{Blob, LixError};

pub(crate) const PLUGIN_CHECKPOINT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0026),
    "plugin.current_checkpoint.v2",
    ValueSemantics::Mutable,
);

const MAGIC: &[u8; 4] = b"LPC3";
const HEADER_BYTES: usize = 4 + 32 + 32 + 16 + 4 + 4;
const DIGEST_BYTES: usize = 32;
const DIGEST_CONTEXT: &str = "lix plugin current checkpoint v3";

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
        .and_then(|length| length.checked_add(DIGEST_BYTES))
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
    value.extend_from_slice(&checkpoint_digest(branch_id, file_id, &value));
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

/// Deletes every derived checkpoint owned by a branch lifecycle record.
///
/// Checkpoints are a current-state accelerator, not repository history. Like
/// an index maintained beside an MVCC log, their lifetime follows the current
/// owner and a branch deletion removes the entire UUID-prefixed key range.
pub(crate) async fn stage_delete_branch_plugin_checkpoints(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
) -> Result<(), LixError> {
    let branch_id = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("plugin checkpoint branch id is not a UUID: {error}"),
        )
    })?;
    let range = StoragePrefix {
        bytes: Bytes::copy_from_slice(branch_id.as_bytes()),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            PLUGIN_CHECKPOINT_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (chunk, chunk_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?.into_parts();
        if chunk.is_empty() {
            break;
        }
        writes.delete_batch(
            PLUGIN_CHECKPOINT_SPACE,
            chunk.into_iter().map(|entry| entry.key),
        );
        if !chunk_has_more {
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
    let authenticated_end = value
        .len()
        .checked_sub(DIGEST_BYTES)
        .filter(|end| *end >= HEADER_BYTES)
        .ok_or_else(checkpoint_corruption)?;
    let (authenticated, stored_digest) = value.split_at(authenticated_end);
    if stored_digest != checkpoint_digest(branch_id, file_id, authenticated) {
        return Err(checkpoint_corruption());
    }
    let header = &authenticated[..HEADER_BYTES];
    if &header[..4] != MAGIC {
        return Err(checkpoint_corruption());
    }
    if header[4..36] != expected_generation.as_bytes()[..]
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
        .filter(|end| *end <= authenticated.len());
    let value_end = runtime_end
        .and_then(|end| end.checked_add(authority_len))
        .filter(|end| *end == authenticated.len());
    let (Some(runtime_end), Some(value_end)) = (runtime_end, value_end) else {
        return Err(checkpoint_corruption());
    };
    Ok(Some(CurrentPluginCheckpoint {
        runtime: value.slice(HEADER_BYTES..runtime_end).into(),
        authority: value.slice(runtime_end..value_end).into(),
    }))
}

fn checkpoint_digest(branch_id: &str, file_id: &str, authenticated: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(DIGEST_CONTEXT);
    for owner in [branch_id, file_id] {
        hasher.update(&(owner.len() as u64).to_be_bytes());
        hasher.update(owner.as_bytes());
    }
    hasher.update(authenticated);
    *hasher.finalize().as_bytes()
}

fn checkpoint_corruption() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "plugin current checkpoint authentication digest mismatch",
    )
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

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut writes = storage.new_write_set();
        stage_delete_branch_plugin_checkpoints(&read, &mut writes, BRANCH_ID)
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
    async fn present_corrupt_checkpoint_fails_closed_instead_of_becoming_a_cache_miss() {
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
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let key = checkpoint_key(BRANCH_ID, FILE_ID).unwrap();
        let mut value = PointReadPlan::new(PLUGIN_CHECKPOINT_SPACE, std::slice::from_ref(&key))
            .materialize(&read, StorageGetOptions::default())
            .await
            .unwrap()
            .value
            .pop()
            .flatten()
            .and_then(|value| match value {
                StorageProjectedValue::FullValue(value) => Some(value.to_vec()),
                StorageProjectedValue::KeyOnly => None,
            })
            .expect("checkpoint value should exist");
        drop(read);
        value[HEADER_BYTES] ^= 1;
        let mut writes = storage.new_write_set();
        writes.put(
            PLUGIN_CHECKPOINT_SPACE,
            key,
            StorageValue {
                bytes: Bytes::from(value),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let error = load_current_plugin_checkpoint(
            &read,
            BRANCH_ID,
            FILE_ID,
            &generation.to_hex(),
            SEMANTIC_ROOT,
            blob_hash,
        )
        .await
        .expect_err("present corrupt checkpoint must fail closed");
        assert!(error.to_string().contains("authentication digest mismatch"));
    }
}
