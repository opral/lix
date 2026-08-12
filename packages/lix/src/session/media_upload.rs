use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use crate::binary_cas::{BlobChunkReceipt, BlobId, ChunkHash};
use crate::storage_adapter::{
    MAX_SCAN_PAGE_ROWS, Storage, StorageBeginScanOptions, StorageCoreProjection,
    StorageGetManyRequest, StorageGetOptions, StorageKey, StorageKeyRange, StoragePrecondition,
    StorageProjectedValue, StorageReadOptions, StorageSpace, StorageSpaceId, StorageValue,
    StorageWriteOptions, ValueSemantics, exact_get_many,
};
use crate::transaction::{begin_commit_boundary, commit_at_boundary};
use crate::{Blob, LixError};

use super::SessionContext;

pub(crate) const UPLOAD_STATE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0006),
    "session.file_upload.v2",
    ValueSemantics::Mutable,
);
pub(crate) const UPLOAD_MANIFEST_LEAF_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0007),
    "session.file_upload_manifest_leaf.v2",
    ValueSemantics::Mutable,
);
pub const FILE_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;
const MAX_FILE_UPLOAD_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const UPLOAD_PART_WINDOW: u32 = 4;
const UPLOAD_MANIFEST_LEAF_MAGIC: &[u8; 8] = b"LIXUML2\0";

/// Collects active resumable-upload receipt chunks and stages receipt cleanup.
/// Upload leaves contain hashes and sizes only; they are never a second payload
/// authority. Open uploads retain their receipt chunks. A completed state is
/// only an idempotency receipt: the published file reference is the sole blob
/// root, so a completed state with no live file root can be retired.
pub(crate) async fn stage_reclaimable_upload_receipts(
    store: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    writes: &mut crate::storage_adapter::StorageWriteSet,
    live_blob_roots: &BTreeSet<BlobId>,
) -> Result<BTreeMap<ChunkHash, u64>, LixError> {
    let mut states = Vec::<(String, UploadState)>::new();
    let mut state_cursor = store
        .begin_scan(
            UPLOAD_STATE_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = state_cursor.next_page(MAX_SCAN_PAGE_ROWS).await?.into_parts();
        for entry in page {
            let upload_id = std::str::from_utf8(&entry.key.0)
                .map_err(|_| invalid_upload_storage("upload state key is not UTF-8"))?
                .to_owned();
            validate_upload_id_for_storage(&upload_id)?;
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(invalid_upload_storage(
                    "upload state scan omitted its value",
                ));
            };
            let state = serde_json::from_slice(&value)
                .map_err(|_| invalid_upload_storage("upload state value is invalid JSON"))?;
            states.push((upload_id, state));
        }
        if !page_has_more {
            break;
        }
    }

    let mut open_ids = BTreeSet::new();
    for (upload_id, state) in states {
        match state {
            UploadState::Open(_) => {
                open_ids.insert(upload_id);
            }
            UploadState::Complete(complete) => {
                if !live_blob_roots.contains(&BlobId::from_bytes(complete.blob_id)) {
                    writes.delete(UPLOAD_STATE_SPACE, upload_state_key(&upload_id)?);
                }
            }
        }
    }

    let mut upload_chunks = BTreeMap::new();
    let mut leaf_cursor = store
        .begin_scan(
            UPLOAD_MANIFEST_LEAF_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = leaf_cursor.next_page(MAX_SCAN_PAGE_ROWS).await?.into_parts();
        for entry in page {
            let upload_id = decode_upload_manifest_leaf_upload_id(&entry.key)?;
            if !open_ids.contains(&upload_id) {
                // Finalized or state-less receipts are not active roots; the
                // published file snapshot, if any, owns the blob instead.
                writes.delete(UPLOAD_MANIFEST_LEAF_SPACE, entry.key);
                continue;
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(invalid_upload_storage(
                    "active upload manifest leaf scan omitted its value",
                ));
            };
            let leaf = decode_upload_manifest_leaf(&value)?;
            for chunk in leaf.chunks {
                match upload_chunks.entry(chunk.hash) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(chunk.size_bytes);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if *entry.get() != chunk.size_bytes =>
                    {
                        return Err(invalid_upload_storage(format!(
                            "active upload chunk '{}' has conflicting declared sizes {} and {}",
                            chunk.hash.to_hex(),
                            entry.get(),
                            chunk.size_bytes
                        )));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
            }
        }
        if !page_has_more {
            break;
        }
    }

    Ok(upload_chunks)
}

fn decode_upload_manifest_leaf_upload_id(key: &StorageKey) -> Result<String, LixError> {
    if key.0.len() < 2 + 4 {
        return Err(invalid_upload_storage(
            "upload manifest leaf key is too short",
        ));
    }
    let id_len = usize::from(u16::from_be_bytes([key.0[0], key.0[1]]));
    if key.0.len() != 2 + id_len + 4 {
        return Err(invalid_upload_storage(
            "upload manifest leaf key has an invalid upload id length",
        ));
    }
    let upload_id = std::str::from_utf8(&key.0[2..2 + id_len])
        .map_err(|_| invalid_upload_storage("upload manifest leaf id is not UTF-8"))?
        .to_owned();
    validate_upload_id_for_storage(&upload_id)?;
    Ok(upload_id)
}

fn validate_upload_id_for_storage(upload_id: &str) -> Result<(), LixError> {
    if upload_id.is_empty() || upload_id.len() > 200 || !upload_id.is_ascii() {
        return Err(invalid_upload_storage("upload id is not a valid ASCII key"));
    }
    Ok(())
}

fn invalid_upload_storage(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_STORAGE_ERROR, message)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileUploadProgress {
    pub next_offset: u64,
    pub total_size: u64,
    pub finalized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum UploadState {
    Open(UploadOpen),
    Complete(UploadComplete),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadOpen {
    path: String,
    total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UploadManifestLeaf {
    part_size: u64,
    chunks: Vec<BlobChunkReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadComplete {
    path: String,
    total_size: u64,
    blob_id: [u8; 32],
    part_identities: Vec<[u8; 32]>,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Stages one aligned part through the ordinary file-upsert abstraction.
    /// Up to four 16 MiB parts may complete out of order. Each request persists
    /// one manifest leaf plus its missing immutable payloads; publication folds
    /// the leaves into the root manifest atomically with ordinary file history.
    pub async fn upsert_file_content_part(
        &self,
        upload_id: String,
        path: String,
        start: u64,
        total_size: u64,
        content: Blob,
    ) -> Result<FileUploadProgress, LixError> {
        self.ensure_open()?;
        crate::common::LixPath::try_from_file_path(&path)?;
        validate_upload_request(&upload_id, start, total_size, content.len())?;
        let operation_guard = self.begin_waitable_session_operation().await?;
        let state_key = upload_state_key(&upload_id)?;
        let part_number = u32::try_from(start / FILE_UPLOAD_PART_BYTES as u64)
            .map_err(|_| invalid_upload("upload part number exceeds u32"))?;
        let leaf_key = upload_manifest_leaf_key(&upload_id, part_number)?;
        let state = UploadOpen {
            path: path.clone(),
            total_size,
        };

        let mut last_error = None;
        for _attempt in 0..UPLOAD_PART_WINDOW {
            let read = self
                .storage
                .begin_read(StorageReadOptions::default())
                .await?;
            let loaded_state = load_upload_state(&read, &state_key).await?;
            match &loaded_state {
                Some(UploadState::Complete(complete)) => {
                    validate_upload_binding(&complete.path, complete.total_size, &path, total_size)?
                }
                Some(UploadState::Open(existing)) => {
                    validate_upload_binding(
                        &existing.path,
                        existing.total_size,
                        &path,
                        total_size,
                    )?;
                }
                None => {}
            }

            let mut writes = self.storage.new_write_set();
            let mut writer = self
                .binary_cas
                .writer_skipping_existing_chunks(&read, &mut writes);
            let chunks = if content.is_empty() {
                Vec::new()
            } else {
                writer.stage_upload_part(&content).await?
            };
            drop(writer);
            let leaf = UploadManifestLeaf {
                part_size: content.len() as u64,
                chunks,
            };
            if let Some(UploadState::Complete(complete)) = &loaded_state {
                if complete.part_identities.get(part_number as usize)
                    != Some(&upload_manifest_leaf_identity(&leaf))
                {
                    return Err(invalid_upload(
                        "completed upload part was replayed with different bytes",
                    ));
                }
                return Ok(FileUploadProgress {
                    next_offset: complete.total_size,
                    total_size: complete.total_size,
                    finalized: true,
                });
            }

            if let Some(existing_leaf) = load_upload_manifest_leaf(&read, &leaf_key).await? {
                if existing_leaf != leaf {
                    return Err(invalid_upload(
                        "upload part was replayed with different bytes",
                    ));
                }
                let progress = load_upload_progress(&read, &upload_id, total_size).await?;
                drop(read);
                drop(operation_guard);
                return self
                    .publish_completed_upload(upload_id, state_key, state, progress)
                    .await;
            }
            let progress = load_upload_progress(&read, &upload_id, total_size).await?;
            let next_part = u32::try_from(progress.next_offset / FILE_UPLOAD_PART_BYTES as u64)
                .map_err(|_| invalid_upload("upload progress exceeds u32"))?;
            if part_number >= next_part.saturating_add(UPLOAD_PART_WINDOW) {
                return Err(invalid_upload(
                    "upload part is outside the four-part completion window",
                ));
            }

            stage_upload_manifest_leaf(&mut writes, leaf_key.clone(), &leaf)?;
            let mut preconditions = vec![StoragePrecondition::KeyAbsent {
                space: UPLOAD_MANIFEST_LEAF_SPACE,
                key: leaf_key.clone(),
            }];
            match loaded_state {
                Some(UploadState::Open(existing)) => {
                    preconditions.push(StoragePrecondition::KeyValueEquals {
                        space: UPLOAD_STATE_SPACE,
                        key: state_key.clone(),
                        expected: Bytes::from(encode_upload_state(&UploadState::Open(existing))?),
                    });
                }
                None => {
                    stage_upload_state(
                        &mut writes,
                        state_key.clone(),
                        &UploadState::Open(state.clone()),
                    )?;
                    preconditions.push(StoragePrecondition::KeyAbsent {
                        space: UPLOAD_STATE_SPACE,
                        key: state_key.clone(),
                    });
                }
                Some(UploadState::Complete(_)) => unreachable!("complete state returned above"),
            }
            crate::binary_cas::stage_cas_publication_fence(
                &read,
                &mut writes,
                &mut preconditions,
            )
            .await?;
            drop(read);

            let commit_boundary = self.transaction_commit_boundary();
            let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
            let result = async {
                let prepared = self
                    .storage
                    .prepare_write_set(
                        writes,
                        StorageWriteOptions {
                            preconditions,
                            await_durable: true,
                            ..StorageWriteOptions::default()
                        },
                    )
                    .await?;
                commit_at_boundary(Some(&commit_boundary), || async move {
                    let (_, stats) = prepared.commit().await?;
                    Ok(stats)
                })
                .await
            }
            .await;
            match result {
                Ok(stats) => {
                    #[cfg(feature = "storage-benches")]
                    crate::storage_bench::record_media_upload_manifest_leaf(leaf.chunks.len());
                    self.observe_invalidation.bump_if_storage_changed(&stats);
                    let read = self
                        .storage
                        .begin_read(StorageReadOptions::default())
                        .await?;
                    let progress = load_upload_progress(&read, &upload_id, total_size).await?;
                    drop(read);
                    drop(operation_guard);
                    return self
                        .publish_completed_upload(upload_id, state_key, state, progress)
                        .await;
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("bounded upload retry loop records an error"))
    }

    async fn publish_completed_upload(
        &self,
        upload_id: String,
        state_key: StorageKey,
        state: UploadOpen,
        progress: FileUploadProgress,
    ) -> Result<FileUploadProgress, LixError> {
        if progress.next_offset != state.total_size {
            return Ok(progress);
        }
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let (receipts, part_identities) =
            load_upload_manifest_leaves(&read, &upload_id, state.total_size).await?;
        let mut finalization_writes = self.storage.new_write_set();
        finalization_writes
            .delete_range_exclusive(
                UPLOAD_MANIFEST_LEAF_SPACE,
                upload_manifest_leaf_range(&upload_id)?,
            )
            .map_err(LixError::from)?;
        let receipt = self
            .binary_cas
            .writer_skipping_existing_chunks(&read, &mut finalization_writes)
            .stage_upload_manifest(&receipts)?;
        let complete = UploadState::Complete(UploadComplete {
            path: state.path.clone(),
            total_size: state.total_size,
            blob_id: receipt.hash.into_bytes(),
            part_identities,
        });
        let publication_blob_id = receipt.hash;
        let expected_blob_id = receipt.hash.into_bytes();
        stage_upload_state(&mut finalization_writes, state_key.clone(), &complete)?;
        let expected_open = encode_upload_state(&UploadState::Open(state.clone()))?;
        let finalization_preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: UPLOAD_STATE_SPACE,
            key: state_key,
            expected: Bytes::from(expected_open),
        }];
        drop(read);
        let path = state.path.clone();
        let write_access = self.begin_session_write_access().await?;
        let publish_result = self
            .with_write_transaction_reserved_lending(
                write_access,
                async move |transaction| {
                    transaction.stage_atomic_cas_publication(
                        finalization_writes,
                        finalization_preconditions,
                        publication_blob_id,
                    )?;
                    crate::sql2::execute_fast_lix_file_prepared_path_write(
                        transaction,
                        path,
                        receipt,
                    )
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_CONSTRAINT_VIOLATION,
                            "resumable file publication requires an unambiguous filesystem layout",
                        )
                    })
                },
                |_| Ok(()),
            )
            .await;
        if let Err(error) = publish_result {
            let read = self
                .storage
                .begin_read(StorageReadOptions::default())
                .await?;
            if matches!(
                load_upload_state(&read, &upload_state_key(&upload_id)?).await?,
                Some(UploadState::Complete(complete))
                    if complete.path == state.path
                        && complete.total_size == state.total_size
                        && complete.blob_id == expected_blob_id
            ) {
                return Ok(FileUploadProgress {
                    next_offset: state.total_size,
                    total_size: state.total_size,
                    finalized: true,
                });
            }
            return Err(error);
        }
        Ok(FileUploadProgress {
            next_offset: state.total_size,
            total_size: state.total_size,
            finalized: true,
        })
    }
}

fn validate_upload_request(
    upload_id: &str,
    start: u64,
    total_size: u64,
    part_len: usize,
) -> Result<(), LixError> {
    if upload_id.is_empty() || upload_id.len() > 200 || !upload_id.is_ascii() {
        return Err(invalid_upload("upload id must be 1-200 ASCII bytes"));
    }
    if total_size > MAX_FILE_UPLOAD_BYTES {
        return Err(invalid_upload("file exceeds the 20 GiB media target"));
    }
    let end = start
        .checked_add(part_len as u64)
        .ok_or_else(|| invalid_upload("upload range exceeds u64"))?;
    if end > total_size || part_len > FILE_UPLOAD_PART_BYTES {
        return Err(invalid_upload(
            "upload part is outside the declared file size",
        ));
    }
    if end < total_size && part_len != FILE_UPLOAD_PART_BYTES {
        return Err(invalid_upload(
            "non-final upload parts must be exactly 16 MiB",
        ));
    }
    if start % FILE_UPLOAD_PART_BYTES as u64 != 0 {
        return Err(invalid_upload("upload part offset must be 16 MiB aligned"));
    }
    Ok(())
}

fn upload_state_key(upload_id: &str) -> Result<StorageKey, LixError> {
    if upload_id.is_empty() || upload_id.len() > 200 || !upload_id.is_ascii() {
        return Err(invalid_upload("upload id must be 1-200 ASCII bytes"));
    }
    Ok(StorageKey(Bytes::copy_from_slice(upload_id.as_bytes())))
}

async fn load_upload_state(
    store: &impl crate::storage_adapter::StorageAdapterRead,
    key: &StorageKey,
) -> Result<Option<UploadState>, LixError> {
    let values = exact_get_many(
        store,
        &[StorageGetManyRequest {
            space: UPLOAD_STATE_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let Some(value) = values.values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(value) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "upload state read returned no value bytes",
        ));
    };
    serde_json::from_slice(&value).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("decode file upload state: {error}"),
        )
    })
}

fn stage_upload_state(
    writes: &mut crate::storage_adapter::StorageWriteSet,
    key: StorageKey,
    state: &UploadState,
) -> Result<(), LixError> {
    let value = encode_upload_state(state)?;
    writes.put(
        UPLOAD_STATE_SPACE,
        key,
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

fn encode_upload_state(state: &UploadState) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(state).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode file upload state: {error}"),
        )
    })
}

fn validate_upload_binding(
    existing_path: &str,
    existing_total_size: u64,
    path: &str,
    total_size: u64,
) -> Result<(), LixError> {
    if existing_path != path || existing_total_size != total_size {
        return Err(invalid_upload(
            "upload id is already bound to a different path or size",
        ));
    }
    Ok(())
}

fn upload_manifest_leaf_prefix(upload_id: &str) -> Result<Vec<u8>, LixError> {
    let id_len = u16::try_from(upload_id.len())
        .map_err(|_| invalid_upload("upload id exceeds receipt key limit"))?;
    let mut key = Vec::with_capacity(2 + upload_id.len());
    key.extend_from_slice(&id_len.to_be_bytes());
    key.extend_from_slice(upload_id.as_bytes());
    Ok(key)
}

fn upload_manifest_leaf_key(upload_id: &str, part_number: u32) -> Result<StorageKey, LixError> {
    let mut key = upload_manifest_leaf_prefix(upload_id)?;
    key.extend_from_slice(&part_number.to_be_bytes());
    Ok(StorageKey(Bytes::from(key)))
}

fn stage_upload_manifest_leaf(
    writes: &mut crate::storage_adapter::StorageWriteSet,
    key: StorageKey,
    leaf: &UploadManifestLeaf,
) -> Result<(), LixError> {
    writes.put(
        UPLOAD_MANIFEST_LEAF_SPACE,
        key,
        StorageValue {
            bytes: Bytes::from(encode_upload_manifest_leaf(leaf)?),
        },
    );
    Ok(())
}

fn encode_upload_manifest_leaf(leaf: &UploadManifestLeaf) -> Result<Vec<u8>, LixError> {
    let chunk_count = u32::try_from(leaf.chunks.len())
        .map_err(|_| invalid_upload("upload manifest leaf has too many chunks"))?;
    let mut value = Vec::with_capacity(
        UPLOAD_MANIFEST_LEAF_MAGIC.len() + 8 + 4 + leaf.chunks.len().saturating_mul(40),
    );
    value.extend_from_slice(UPLOAD_MANIFEST_LEAF_MAGIC);
    value.extend_from_slice(&leaf.part_size.to_be_bytes());
    value.extend_from_slice(&chunk_count.to_be_bytes());
    for chunk in &leaf.chunks {
        value.extend_from_slice(chunk.hash.as_bytes());
        value.extend_from_slice(&chunk.size_bytes.to_be_bytes());
    }
    Ok(value)
}

fn upload_manifest_leaf_identity(leaf: &UploadManifestLeaf) -> [u8; 32] {
    let mut identity = blake3::Hasher::new_derive_key("lix upload manifest leaf identity v1");
    identity.update(&leaf.part_size.to_le_bytes());
    for chunk in &leaf.chunks {
        identity.update(chunk.hash.as_bytes());
        identity.update(&chunk.size_bytes.to_le_bytes());
    }
    *identity.finalize().as_bytes()
}

fn decode_upload_manifest_leaf(value: &[u8]) -> Result<UploadManifestLeaf, LixError> {
    const HEADER_BYTES: usize = 8 + 8 + 4;
    if value.len() < HEADER_BYTES || !value.starts_with(UPLOAD_MANIFEST_LEAF_MAGIC) {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload manifest leaf header is invalid",
        ));
    }
    let part_size = u64::from_be_bytes(
        value[8..16]
            .try_into()
            .expect("upload manifest leaf part size"),
    );
    let chunk_count = u32::from_be_bytes(
        value[16..20]
            .try_into()
            .expect("upload manifest leaf chunk count"),
    ) as usize;
    let expected_len = HEADER_BYTES
        .checked_add(chunk_count.saturating_mul(40))
        .ok_or_else(|| invalid_upload("upload manifest leaf size overflows usize"))?;
    if value.len() != expected_len {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload manifest leaf body is invalid",
        ));
    }
    let mut chunks = Vec::with_capacity(chunk_count);
    for encoded in value[HEADER_BYTES..].chunks_exact(40) {
        let mut hash = [0; 32];
        hash.copy_from_slice(&encoded[..32]);
        let size_bytes = u64::from_be_bytes(
            encoded[32..]
                .try_into()
                .expect("upload manifest leaf chunk size"),
        );
        chunks.push(BlobChunkReceipt {
            hash: ChunkHash::from_bytes(hash),
            size_bytes,
        });
    }
    let encoded_part_size = chunks
        .iter()
        .try_fold(0_u64, |total, chunk| total.checked_add(chunk.size_bytes));
    if encoded_part_size != Some(part_size) {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload manifest leaf chunk sizes do not match its part size",
        ));
    }
    Ok(UploadManifestLeaf { part_size, chunks })
}

async fn load_upload_manifest_leaf(
    store: &impl crate::storage_adapter::StorageAdapterRead,
    key: &StorageKey,
) -> Result<Option<UploadManifestLeaf>, LixError> {
    let values = exact_get_many(
        store,
        &[StorageGetManyRequest {
            space: UPLOAD_MANIFEST_LEAF_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let Some(StorageProjectedValue::FullValue(value)) = values.values.into_iter().next().flatten()
    else {
        return Ok(None);
    };
    decode_upload_manifest_leaf(&value).map(Some)
}

async fn load_upload_progress(
    store: &impl crate::storage_adapter::StorageAdapterRead,
    upload_id: &str,
    total_size: u64,
) -> Result<FileUploadProgress, LixError> {
    let range = upload_manifest_leaf_range(upload_id)?;
    let mut expected_part = 0_u32;
    let mut cursor = store
        .begin_scan(
            UPLOAD_MANIFEST_LEAF_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    'pages: loop {
        let (page, page_has_more) = cursor.next_page(MAX_SCAN_PAGE_ROWS).await?.into_parts();
        for entry in &page {
            let part_number = decode_upload_manifest_leaf_part_number(upload_id, &entry.key)?;
            if part_number != expected_part {
                break 'pages;
            }
            expected_part = expected_part
                .checked_add(1)
                .ok_or_else(|| invalid_upload("upload part count exceeds u32"))?;
        }
        if !page_has_more {
            break;
        }
    }
    let next_offset = u64::from(expected_part)
        .saturating_mul(FILE_UPLOAD_PART_BYTES as u64)
        .min(total_size);
    Ok(FileUploadProgress {
        next_offset,
        total_size,
        finalized: false,
    })
}

async fn load_upload_manifest_leaves(
    store: &impl crate::storage_adapter::StorageAdapterRead,
    upload_id: &str,
    total_size: u64,
) -> Result<(Vec<BlobChunkReceipt>, Vec<[u8; 32]>), LixError> {
    let range = upload_manifest_leaf_range(upload_id)?;
    let expected_leaf_count = upload_part_count(total_size)?;
    let mut receipts = Vec::new();
    let mut part_identities = Vec::new();
    let mut next_part = 0_u32;
    let mut cursor = store
        .begin_scan(
            UPLOAD_MANIFEST_LEAF_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor.next_page(MAX_SCAN_PAGE_ROWS).await?.into_parts();
        for entry in &page {
            let part_number = decode_upload_manifest_leaf_part_number(upload_id, &entry.key)?;
            if part_number != next_part {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "upload manifest leaf sequence is incomplete",
                ));
            }
            let StorageProjectedValue::FullValue(value) = &entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "upload manifest leaf read returned no value bytes",
                ));
            };
            let leaf = decode_upload_manifest_leaf(value)?;
            let expected_part_size = upload_part_size(total_size, part_number)?;
            if leaf.part_size != expected_part_size {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "upload manifest leaf has the wrong part size",
                ));
            }
            part_identities.push(upload_manifest_leaf_identity(&leaf));
            receipts.extend(leaf.chunks);
            next_part = next_part
                .checked_add(1)
                .ok_or_else(|| invalid_upload("upload part count exceeds u32"))?;
        }
        if !page_has_more {
            break;
        }
    }
    if next_part != expected_leaf_count {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload manifest leaf sequence is incomplete",
        ));
    }
    Ok((receipts, part_identities))
}

fn decode_upload_manifest_leaf_part_number(
    upload_id: &str,
    key: &StorageKey,
) -> Result<u32, LixError> {
    let prefix = upload_manifest_leaf_prefix(upload_id)?;
    let suffix = key.0.strip_prefix(prefix.as_slice()).ok_or_else(|| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload manifest leaf key has the wrong prefix",
        )
    })?;
    let encoded: [u8; 4] = suffix.try_into().map_err(|_| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload manifest leaf key has an invalid part number",
        )
    })?;
    Ok(u32::from_be_bytes(encoded))
}

fn upload_part_count(total_size: u64) -> Result<u32, LixError> {
    if total_size == 0 {
        return Ok(1);
    }
    let parts = total_size.div_ceil(FILE_UPLOAD_PART_BYTES as u64);
    u32::try_from(parts).map_err(|_| invalid_upload("upload part count exceeds u32"))
}

fn upload_part_size(total_size: u64, part_number: u32) -> Result<u64, LixError> {
    if total_size == 0 && part_number == 0 {
        return Ok(0);
    }
    let start = u64::from(part_number)
        .checked_mul(FILE_UPLOAD_PART_BYTES as u64)
        .ok_or_else(|| invalid_upload("upload part offset exceeds u64"))?;
    if start >= total_size {
        return Err(invalid_upload("upload part number exceeds declared size"));
    }
    Ok((total_size - start).min(FILE_UPLOAD_PART_BYTES as u64))
}

fn upload_manifest_leaf_range(upload_id: &str) -> Result<StorageKeyRange, LixError> {
    let prefix = upload_manifest_leaf_prefix(upload_id)?;
    let mut upper = prefix.clone();
    upper.push(0xff);
    Ok(StorageKeyRange {
        lower: Bound::Included(StorageKey(Bytes::from(prefix))),
        upper: Bound::Excluded(StorageKey(Bytes::from(upper))),
    })
}

fn invalid_upload(message: &'static str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_cas::BINARY_CAS_CHUNK_SPACE;
    use crate::storage_adapter::{
        StorageAdapter, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
        StorageKeyRange, StorageWriteOptions, StorageWriteSet,
    };
    use crate::{Memory, engine::Engine};
    use std::ops::Bound;

    async fn seed_orphan_upload_chunk(
        storage: &StorageAdapter<Memory>,
        payload: &[u8],
    ) -> ChunkHash {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan chunk staging read should open");
        let mut writes = storage.new_write_set();
        let receipts = crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut writes)
            .stage_upload_part(payload)
            .await
            .expect("orphan upload chunk should stage");
        assert_eq!(receipts.len(), 1);
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("orphan upload chunk should commit");
        receipts[0].hash
    }

    /// A resumable upload keeps four parts in flight by design, and the engine —
    /// not the caller — owns that window. Parts staged from one snapshot write
    /// disjoint manifest leaves over content-addressed payloads, so they are
    /// independent publications: every one of them must commit.
    ///
    /// Making publishers share a compare-and-set row broke exactly this. It was
    /// invisible at the public surface because `upsert_file_content_part` retries
    /// a bounded number of times — a full window plus any other concurrent writer
    /// exhausts that budget, which is how the movie-workspace qualification fails
    /// its upload acknowledgement.
    #[tokio::test]
    async fn concurrent_upload_part_publications_from_one_snapshot_all_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let payload = b"windowed-upload-part-payload";
        seed_orphan_upload_chunk(&storage, payload).await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("upload window read should open");
        let mut window = Vec::new();
        for part in 0..4 {
            window.push(
                stage_deduplicated_receipt_publication(
                    &storage,
                    &read,
                    &format!("windowed-part-{part}"),
                    payload,
                    true,
                )
                .await,
            );
        }
        drop(read);

        for (part, (writes, preconditions)) in window.into_iter().enumerate() {
            storage
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "every part of one upload window must commit; part {part} was rejected: {error:?}"
                    )
                });
        }
    }

    async fn stage_deduplicated_receipt_publication(
        storage: &StorageAdapter<Memory>,
        read: &impl StorageAdapterRead,
        upload_id: &str,
        payload: &[u8],
        expect_deduplicated: bool,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let mut writes = storage.new_write_set();
        let chunks = crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(read, &mut writes)
            .stage_upload_part(payload)
            .await
            .expect("deduplicated receipt chunk should stage");
        assert_eq!(
            writes.is_empty(),
            expect_deduplicated,
            "receipt payload staging did not match the expected deduplication state"
        );
        let leaf_key = upload_manifest_leaf_key(upload_id, 0).unwrap();
        stage_upload_manifest_leaf(
            &mut writes,
            leaf_key.clone(),
            &UploadManifestLeaf {
                part_size: payload.len() as u64,
                chunks,
            },
        )
        .expect("deduplicated receipt leaf should stage");
        let state_key = upload_state_key(upload_id).unwrap();
        stage_upload_state(
            &mut writes,
            state_key.clone(),
            &UploadState::Open(UploadOpen {
                path: format!("/{upload_id}.bin"),
                total_size: payload.len() as u64 + 1,
            }),
        )
        .expect("deduplicated receipt state should stage");
        let mut preconditions = vec![
            StoragePrecondition::KeyAbsent {
                space: UPLOAD_MANIFEST_LEAF_SPACE,
                key: leaf_key,
            },
            StoragePrecondition::KeyAbsent {
                space: UPLOAD_STATE_SPACE,
                key: state_key,
            },
        ];
        crate::binary_cas::stage_cas_publication_fence(read, &mut writes, &mut preconditions)
            .await
            .expect("deduplicated receipt publication fence should stage");
        (writes, preconditions)
    }

    async fn stage_cas_sweep(
        storage: &StorageAdapter<Memory>,
        read: &impl StorageAdapterRead,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let upload_chunks = stage_reclaimable_upload_receipts(read, &mut writes, &BTreeSet::new())
            .await
            .expect("stale sweep upload mark should collect");
        let swept = crate::binary_cas::stage_gc_reclamation(
            read,
            &mut writes,
            &BTreeSet::new(),
            &upload_chunks,
        )
        .await
        .expect("stale CAS sweep should stage");
        assert_eq!(swept.reclaimed_chunk_rows, 1);
        crate::binary_cas::stage_cas_reclamation_fence(read, &mut writes, &mut preconditions)
            .await
            .expect("stale sweep reclamation fence should stage");
        (writes, preconditions)
    }

    async fn chunk_exists(storage: &StorageAdapter<Memory>, hash: ChunkHash) -> bool {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("chunk verification read should open");
        let mut cursor = read
            .begin_scan(
                BINARY_CAS_CHUNK_SPACE,
                StorageKeyRange {
                    lower: Bound::Included(StorageKey(Bytes::copy_from_slice(hash.as_bytes()))),
                    upper: Bound::Included(StorageKey(Bytes::copy_from_slice(hash.as_bytes()))),
                },
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("chunk verification scan should succeed");
        let (page, _page_has_more) = cursor
            .next_page(1)
            .await
            .expect("chunk verification page should succeed").into_parts();
        !page.is_empty()
    }

    #[tokio::test]
    async fn receipt_first_rejects_stale_gc_deleting_a_deduplicated_chunk() {
        let storage = StorageAdapter::new(Memory::new());
        let payload = b"deduplicated-upload-race";
        let hash = seed_orphan_upload_chunk(&storage, payload).await;
        let sweep_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("stale sweep read should open");
        let receipt_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("receipt publication read should open");
        let (sweep, sweep_preconditions) = stage_cas_sweep(&storage, &sweep_read).await;
        let (receipt, receipt_preconditions) = stage_deduplicated_receipt_publication(
            &storage,
            &receipt_read,
            "receipt-first",
            payload,
            true,
        )
        .await;
        drop(sweep_read);
        drop(receipt_read);

        storage
            .commit_write_set(
                receipt,
                StorageWriteOptions {
                    preconditions: receipt_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("receipt should win the publication fence");
        assert!(
            storage
                .commit_write_set(
                    sweep,
                    StorageWriteOptions {
                        preconditions: sweep_preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .is_err(),
            "stale GC must lose after receipt publication",
        );
        assert!(chunk_exists(&storage, hash).await);
    }

    #[tokio::test]
    async fn gc_first_rejects_stale_receipt_publication_after_payload_deletion() {
        let storage = StorageAdapter::new(Memory::new());
        let payload = b"deduplicated-upload-race";
        let hash = seed_orphan_upload_chunk(&storage, payload).await;
        let sweep_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("sweep read should open");
        let receipt_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("stale receipt read should open");
        let (sweep, sweep_preconditions) = stage_cas_sweep(&storage, &sweep_read).await;
        let (receipt, receipt_preconditions) = stage_deduplicated_receipt_publication(
            &storage,
            &receipt_read,
            "gc-first",
            payload,
            true,
        )
        .await;
        drop(sweep_read);
        drop(receipt_read);

        storage
            .commit_write_set(
                sweep,
                StorageWriteOptions {
                    preconditions: sweep_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("GC should win the publication fence");
        assert!(!chunk_exists(&storage, hash).await);
        assert!(
            storage
                .commit_write_set(
                    receipt,
                    StorageWriteOptions {
                        preconditions: receipt_preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .is_err(),
            "stale receipt must not publish after GC deletes its payload",
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("receipt absence verification read should open");
        assert!(
            load_upload_state(&read, &upload_state_key("gc-first").unwrap())
                .await
                .expect("stale receipt state lookup should succeed")
                .is_none()
        );
        assert!(
            load_upload_manifest_leaf(&read, &upload_manifest_leaf_key("gc-first", 0).unwrap())
                .await
                .expect("stale receipt leaf lookup should succeed")
                .is_none()
        );
        drop(read);

        let retry_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("receipt retry read should open after GC");
        let (retry, retry_preconditions) = stage_deduplicated_receipt_publication(
            &storage,
            &retry_read,
            "gc-first",
            payload,
            false,
        )
        .await;
        drop(retry_read);
        storage
            .commit_write_set(
                retry,
                StorageWriteOptions {
                    preconditions: retry_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("fresh receipt retry should restage the deleted payload");
        assert!(chunk_exists(&storage, hash).await);
        let cold_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cold receipt verification read should open");
        assert!(
            load_upload_manifest_leaf(
                &cold_read,
                &upload_manifest_leaf_key("gc-first", 0).unwrap()
            )
            .await
            .expect("retried receipt leaf lookup should succeed")
            .is_some()
        );
    }

    #[tokio::test]
    async fn completed_receipt_without_a_live_file_root_is_reclaimed() {
        let storage = StorageAdapter::new(Memory::new());
        let upload_id = "completed-receipt";
        let mut initial = storage.new_write_set();
        stage_upload_state(
            &mut initial,
            upload_state_key(upload_id).expect("upload state key should encode"),
            &UploadState::Complete(UploadComplete {
                path: "/orphaned.bin".to_owned(),
                total_size: 7,
                blob_id: BlobId::from_content(b"orphaned").into_bytes(),
                part_identities: Vec::new(),
            }),
        )
        .expect("completed state should encode");
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("completed receipt should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("receipt read should open");
        let mut writes = storage.new_write_set();
        let chunks = stage_reclaimable_upload_receipts(&read, &mut writes, &BTreeSet::new())
            .await
            .expect("receipt sweep should succeed");
        assert!(chunks.is_empty());
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("completed receipt cleanup should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reopened receipt read should open");
        assert!(
            load_upload_state(&read, &upload_state_key(upload_id).unwrap())
                .await
                .expect("completed receipt lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn active_receipt_declared_size_must_match_authenticated_chunk_bytes() {
        let storage = StorageAdapter::new(Memory::new());
        let payload = b"active-upload-chunk";
        let chunk_hash = ChunkHash::from_content(payload);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("active receipt staging read should open");
        let mut initial = storage.new_write_set();
        crate::binary_cas::BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut initial)
            .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(
                payload.to_vec(),
            ))
            .await
            .expect("active receipt chunk should stage");
        stage_upload_state(
            &mut initial,
            upload_state_key("wrong-size-receipt").unwrap(),
            &UploadState::Open(UploadOpen {
                path: "/wrong-size.bin".to_owned(),
                total_size: payload.len() as u64 + 1,
            }),
        )
        .expect("active upload state should stage");
        stage_upload_manifest_leaf(
            &mut initial,
            upload_manifest_leaf_key("wrong-size-receipt", 0).unwrap(),
            &UploadManifestLeaf {
                part_size: payload.len() as u64 + 1,
                chunks: vec![BlobChunkReceipt {
                    hash: chunk_hash,
                    size_bytes: payload.len() as u64 + 1,
                }],
            },
        )
        .expect("wrong-size receipt should stage");
        drop(read);
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("wrong-size receipt fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("wrong-size receipt GC read should open");
        let mut sweep = storage.new_write_set();
        let upload_chunks = stage_reclaimable_upload_receipts(&read, &mut sweep, &BTreeSet::new())
            .await
            .expect("active receipt should collect");
        let error = crate::binary_cas::stage_gc_reclamation(
            &read,
            &mut sweep,
            &BTreeSet::new(),
            &upload_chunks,
        )
        .await
        .expect_err("wrong declared upload size must fail GC closed");
        assert!(
            error.message.contains("expected 20 uncompressed bytes"),
            "{error:?}"
        );
        assert!(sweep.is_empty(), "wrong size must stage no reclamation");
    }

    #[tokio::test]
    async fn active_receipts_reject_conflicting_sizes_for_one_chunk() {
        let storage = StorageAdapter::new(Memory::new());
        let hash = ChunkHash::from_content(b"shared-active-upload-chunk");
        let mut initial = storage.new_write_set();
        for (upload_id, size_bytes) in [("receipt-a", 7), ("receipt-b", 9)] {
            stage_upload_state(
                &mut initial,
                upload_state_key(upload_id).unwrap(),
                &UploadState::Open(UploadOpen {
                    path: format!("/{upload_id}.bin"),
                    total_size: size_bytes,
                }),
            )
            .expect("active upload state should stage");
            stage_upload_manifest_leaf(
                &mut initial,
                upload_manifest_leaf_key(upload_id, 0).unwrap(),
                &UploadManifestLeaf {
                    part_size: size_bytes,
                    chunks: vec![BlobChunkReceipt { hash, size_bytes }],
                },
            )
            .expect("active upload receipt should stage");
        }
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("conflicting active receipt fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("conflicting receipt read should open");
        let mut sweep = storage.new_write_set();
        let error = stage_reclaimable_upload_receipts(&read, &mut sweep, &BTreeSet::new())
            .await
            .expect_err("conflicting active receipt sizes must fail closed");
        assert!(error.message.contains("conflicting declared sizes"));
        assert!(sweep.is_empty(), "conflict must stage no receipt cleanup");
    }

    #[tokio::test]
    async fn sequential_parts_survive_a_new_session_and_publish_one_file() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage.clone()).await.expect("open engine");
        let first_session = engine
            .open_workspace_session()
            .await
            .expect("open first session");
        let first = vec![0x31; FILE_UPLOAD_PART_BYTES];
        let tail = vec![0x72; 123];
        let total = (first.len() + tail.len()) as u64;

        let progress = first_session
            .upsert_file_content_part(
                "movie-proxy-1".into(),
                "/media/proxy.mov".into(),
                0,
                total,
                first.clone().into(),
            )
            .await
            .expect("stage first part");
        assert_eq!(progress.next_offset, FILE_UPLOAD_PART_BYTES as u64);
        assert!(!progress.finalized);
        assert!(
            first_session
                .read_file_content("/media/proxy.mov".into(), None)
                .await
                .expect("read before publish")
                .is_none()
        );

        let resumed_session = engine
            .open_workspace_session()
            .await
            .expect("open resumed session");
        let progress = resumed_session
            .upsert_file_content_part(
                "movie-proxy-1".into(),
                "/media/proxy.mov".into(),
                FILE_UPLOAD_PART_BYTES as u64,
                total,
                tail.clone().into(),
            )
            .await
            .expect("finalize resumed upload");
        assert!(progress.finalized);

        let boundary = resumed_session
            .read_file_content(
                "/media/proxy.mov".into(),
                Some((FILE_UPLOAD_PART_BYTES as u64 - 4)..(FILE_UPLOAD_PART_BYTES as u64 + 4)),
            )
            .await
            .expect("range read")
            .expect("published file");
        assert_eq!(
            boundary.content().as_ref(),
            [vec![0x31; 4], vec![0x72; 4]].concat().as_slice()
        );
        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open upload cleanup read");
        let mut cursor = read
            .begin_scan(
                UPLOAD_MANIFEST_LEAF_SPACE,
                upload_manifest_leaf_range("movie-proxy-1").expect("upload leaf range"),
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("begin temporary upload receipt scan");
        let (temporary_receipts, _temporary_receipts_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan temporary upload receipts").into_parts();
        assert!(
            temporary_receipts.is_empty(),
            "publication must atomically remove temporary chunk receipts",
        );
        drop(cursor);
        drop(read);
        let published_commit_id = resumed_session
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("published branch head")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("published commit id");

        let replay = resumed_session
            .upsert_file_content_part(
                "movie-proxy-1".into(),
                "/media/proxy.mov".into(),
                FILE_UPLOAD_PART_BYTES as u64,
                total,
                tail.into(),
            )
            .await
            .expect("replay final part");
        assert!(replay.finalized);
        assert_eq!(replay.next_offset, total);
        let replayed_commit_id = resumed_session
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("replayed branch head")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("replayed commit id");
        assert_eq!(
            replayed_commit_id, published_commit_id,
            "a completed upload replay must not publish duplicate history",
        );
        let mismatched_replay = resumed_session
            .upsert_file_content_part(
                "movie-proxy-1".into(),
                "/media/proxy.mov".into(),
                FILE_UPLOAD_PART_BYTES as u64,
                total,
                vec![0x73; 123].into(),
            )
            .await
            .expect_err("completed part replay must preserve content identity");
        assert_eq!(mismatched_replay.code, LixError::CODE_INVALID_PARAM);

        resumed_session
            .upsert_file_content_part(
                "movie-proxy-copy".into(),
                "/media/proxy-copy.mov".into(),
                0,
                total,
                first.into(),
            )
            .await
            .expect("stage identical first part");
        resumed_session
            .upsert_file_content_part(
                "movie-proxy-copy".into(),
                "/media/proxy-copy.mov".into(),
                FILE_UPLOAD_PART_BYTES as u64,
                total,
                vec![0x72; 123].into(),
            )
            .await
            .expect("publish identical copy");

        let adapter = StorageAdapter::new(storage);
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open CAS accounting read");
        let mut cursor = read
            .begin_scan(
                BINARY_CAS_CHUNK_SPACE,
                StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("begin CAS chunk scan");
        let (chunks, chunks_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan CAS chunks").into_parts();
        assert!(!chunks_has_more);
        assert_eq!(
            chunks.len(),
            2,
            "identical media must reuse payloads"
        );
    }

    #[tokio::test]
    async fn four_part_window_persists_one_leaf_per_completed_part() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage.clone()).await.expect("open engine");
        let session = engine.open_workspace_session().await.expect("open session");
        let total_size = 4 * FILE_UPLOAD_PART_BYTES as u64;

        let second = session.upsert_file_content_part(
            "windowed-proxy".into(),
            "/media/windowed.mov".into(),
            FILE_UPLOAD_PART_BYTES as u64,
            total_size,
            vec![0x22; FILE_UPLOAD_PART_BYTES].into(),
        );
        let third = session.upsert_file_content_part(
            "windowed-proxy".into(),
            "/media/windowed.mov".into(),
            2 * FILE_UPLOAD_PART_BYTES as u64,
            total_size,
            vec![0x33; FILE_UPLOAD_PART_BYTES].into(),
        );
        let fourth = session.upsert_file_content_part(
            "windowed-proxy".into(),
            "/media/windowed.mov".into(),
            3 * FILE_UPLOAD_PART_BYTES as u64,
            total_size,
            vec![0x44; FILE_UPLOAD_PART_BYTES].into(),
        );
        let (second, third, fourth) = tokio::join!(second, third, fourth);
        for progress in [second, third, fourth] {
            let progress = progress.expect("windowed part completes");
            assert_eq!(progress.next_offset, 0);
            assert!(!progress.finalized);
        }

        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read upload leaves");
        let mut cursor = read
            .begin_scan(
                UPLOAD_MANIFEST_LEAF_SPACE,
                upload_manifest_leaf_range("windowed-proxy").expect("leaf range"),
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("begin upload leaf scan");
        let (leaves, _leaves_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan upload leaves").into_parts();
        assert_eq!(leaves.len(), 3);
        for entry in leaves {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                panic!("manifest leaf scan must return values");
            };
            let leaf = decode_upload_manifest_leaf(&value).expect("decode manifest leaf");
            assert!(!leaf.chunks.is_empty());
            assert_eq!(
                leaf.chunks.iter().map(|chunk| chunk.size_bytes).sum::<u64>(),
                FILE_UPLOAD_PART_BYTES as u64,
                "a part's content-defined chunks must tile the part exactly"
            );
        }
        drop(cursor);
        drop(read);

        let outside_window = session
            .upsert_file_content_part(
                "window-gap".into(),
                "/media/window-gap.mov".into(),
                4 * FILE_UPLOAD_PART_BYTES as u64,
                5 * FILE_UPLOAD_PART_BYTES as u64,
                vec![0x55; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect_err("fifth part cannot pass a missing first part");
        assert_eq!(outside_window.code, LixError::CODE_INVALID_PARAM);

        session
            .upsert_file_content_part(
                "sparse-chain".into(),
                "/media/sparse-chain.mov".into(),
                0,
                9 * FILE_UPLOAD_PART_BYTES as u64,
                vec![0x10; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect("stage sparse-chain first part");
        session
            .upsert_file_content_part(
                "sparse-chain".into(),
                "/media/sparse-chain.mov".into(),
                4 * FILE_UPLOAD_PART_BYTES as u64,
                9 * FILE_UPLOAD_PART_BYTES as u64,
                vec![0x14; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect("stage last part inside the first moving window");
        let sparse_escape = session
            .upsert_file_content_part(
                "sparse-chain".into(),
                "/media/sparse-chain.mov".into(),
                8 * FILE_UPLOAD_PART_BYTES as u64,
                9 * FILE_UPLOAD_PART_BYTES as u64,
                vec![0x18; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect_err("sparse receipts cannot advance the bounded window");
        assert_eq!(sparse_escape.code, LixError::CODE_INVALID_PARAM);

        let mismatched_active_replay = session
            .upsert_file_content_part(
                "windowed-proxy".into(),
                "/media/windowed.mov".into(),
                FILE_UPLOAD_PART_BYTES as u64,
                total_size,
                vec![0x23; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect_err("staged part replay must preserve content identity");
        assert_eq!(mismatched_active_replay.code, LixError::CODE_INVALID_PARAM);

        let completed = session
            .upsert_file_content_part(
                "windowed-proxy".into(),
                "/media/windowed.mov".into(),
                0,
                total_size,
                vec![0x11; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect("first part closes the completion gap");
        assert!(completed.finalized);
        assert_eq!(completed.next_offset, total_size);

        for (part, expected) in [0x11, 0x22, 0x33, 0x44].into_iter().enumerate() {
            let offset = part as u64 * FILE_UPLOAD_PART_BYTES as u64;
            let byte = session
                .read_file_content(
                    "/media/windowed.mov".into(),
                    Some(offset..offset.saturating_add(1)),
                )
                .await
                .expect("read completed part")
                .expect("published file");
            assert_eq!(byte.content().as_ref(), &[expected]);
        }
    }
}
