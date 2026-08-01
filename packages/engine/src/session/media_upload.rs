use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::Bound;

use crate::binary_cas::{BlobChunkReceipt, ChunkHash};
use crate::storage_adapter::{
    MAX_SCAN_PAGE_ROWS, Storage, StorageCoreProjection, StorageGetManyRequest, StorageGetOptions,
    StorageKey, StorageKeyRange, StoragePrecondition, StorageProjectedValue, StorageReadOptions,
    StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteOptions,
};
use crate::transaction::{begin_commit_boundary, commit_at_boundary};
use crate::{Blob, LixError};

use super::SessionContext;

const UPLOAD_STATE_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0007_0006), "session.file_upload.v1");
const UPLOAD_CHUNK_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0007_0007), "session.file_upload_chunk.v1");
pub const FILE_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;
const MAX_FILE_UPLOAD_BYTES: u64 = 20 * 1024 * 1024 * 1024;

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
    next_offset: u64,
    chunk_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadComplete {
    path: String,
    total_size: u64,
    blob_id: [u8; 32],
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Stages one sequential part through the ordinary file-upsert abstraction.
    /// Full non-final parts are 16 MiB; CAS ownership remains bounded to one
    /// request part and publication of the final manifest is atomic with file
    /// history in the normal transaction path.
    pub async fn upsert_file_data_part(
        &self,
        upload_id: String,
        path: String,
        start: u64,
        total_size: u64,
        data: Blob,
    ) -> Result<FileUploadProgress, LixError> {
        self.ensure_open()?;
        crate::common::LixPath::try_from_file_path(&path)?;
        validate_upload_request(&upload_id, start, total_size, data.len())?;

        let state_key = upload_state_key(&upload_id)?;
        let write_access = self.begin_session_write_access().await?;
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let loaded_state = load_upload_state(&read, &state_key).await?;
        let (mut state, state_precondition) = match loaded_state {
            Some(UploadState::Complete(complete)) => {
                if complete.path != path || complete.total_size != total_size {
                    return Err(invalid_upload(
                        "upload id is already bound to a different path or size",
                    ));
                }
                return Ok(FileUploadProgress {
                    next_offset: complete.total_size,
                    total_size: complete.total_size,
                    finalized: true,
                });
            }
            Some(UploadState::Open(state)) => {
                let expected = encode_upload_state(&UploadState::Open(state.clone()))?;
                (
                    state,
                    StoragePrecondition::KeyValueEquals {
                        space: UPLOAD_STATE_SPACE.id,
                        key: state_key.clone(),
                        expected: Bytes::from(expected),
                    },
                )
            }
            None => (
                UploadOpen {
                    path: path.clone(),
                    total_size,
                    next_offset: 0,
                    chunk_count: 0,
                },
                StoragePrecondition::KeyAbsent {
                    space: UPLOAD_STATE_SPACE.id,
                    key: state_key.clone(),
                },
            ),
        };
        if state.path != path || state.total_size != total_size {
            return Err(invalid_upload(
                "upload id is already bound to a different path or size",
            ));
        }
        if start < state.next_offset {
            drop(write_access);
            return self
                .publish_completed_upload(upload_id, state_key, state)
                .await;
        }
        if start != state.next_offset {
            return Err(invalid_upload(
                "upload part does not begin at the acknowledged offset",
            ));
        }

        let mut writes = self.storage.new_write_set();
        let mut writer = self
            .binary_cas
            .writer_skipping_existing_chunks(&read, &mut writes);
        let part_receipts = if data.is_empty() {
            Vec::new()
        } else {
            writer.stage_fixed_part(&data).await?
        };
        drop(writer);
        let part_chunk_start = state.chunk_count;
        for (part_index, receipt) in part_receipts.iter().enumerate() {
            let index = part_chunk_start
                .checked_add(
                    u32::try_from(part_index)
                        .map_err(|_| invalid_upload("upload part has too many chunks"))?,
                )
                .ok_or_else(|| invalid_upload("upload has too many chunks"))?;
            stage_upload_chunk(&mut writes, &upload_id, index, *receipt)?;
        }
        state.chunk_count = state
            .chunk_count
            .checked_add(
                u32::try_from(part_receipts.len())
                    .map_err(|_| invalid_upload("upload part has too many chunks"))?,
            )
            .ok_or_else(|| invalid_upload("upload has too many chunks"))?;
        state.next_offset = state
            .next_offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| invalid_upload("upload offset exceeds u64"))?;
        stage_upload_state(
            &mut writes,
            state_key.clone(),
            &UploadState::Open(state.clone()),
        )?;
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let prepared = self
            .storage
            .prepare_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![state_precondition],
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        let stats = commit_at_boundary(Some(&commit_boundary), || async move {
            let (_, stats) = prepared.commit().await?;
            Ok(stats)
        })
        .await?;
        self.observe_invalidation.bump_if_storage_changed(&stats);
        drop(write_access);
        self.publish_completed_upload(upload_id, state_key, state)
            .await
    }

    async fn publish_completed_upload(
        &self,
        upload_id: String,
        state_key: StorageKey,
        state: UploadOpen,
    ) -> Result<FileUploadProgress, LixError> {
        if state.next_offset != state.total_size {
            return Ok(FileUploadProgress {
                next_offset: state.next_offset,
                total_size: state.total_size,
                finalized: false,
            });
        }
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let receipts = load_upload_chunks(&read, &upload_id, state.chunk_count).await?;
        let mut finalization_writes = self.storage.new_write_set();
        finalization_writes
            .delete_range_exclusive(UPLOAD_CHUNK_SPACE, upload_chunk_range(&upload_id)?)
            .map_err(LixError::from)?;
        let receipt = self
            .binary_cas
            .writer_skipping_existing_chunks(&read, &mut finalization_writes)
            .stage_fixed_manifest(&receipts)?;
        let complete = UploadState::Complete(UploadComplete {
            path: state.path.clone(),
            total_size: state.total_size,
            blob_id: receipt.hash.into_bytes(),
        });
        let publication_blob_id = receipt.hash;
        let expected_blob_id = receipt.hash.into_bytes();
        stage_upload_state(&mut finalization_writes, state_key.clone(), &complete)?;
        let expected_open = encode_upload_state(&UploadState::Open(state.clone()))?;
        let finalization_preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: UPLOAD_STATE_SPACE.id,
            key: state_key,
            expected: Bytes::from(expected_open),
        }];
        drop(read);
        let path = state.path.clone();
        let write_access = self.begin_session_write_access().await?;
        let publish_result = self
            .with_write_transaction_reserved(write_access, |transaction| {
                Box::pin(async move {
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
                })
            })
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
            next_offset: state.next_offset,
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
    let values = store
        .get_many(&[StorageGetManyRequest {
            space: UPLOAD_STATE_SPACE.id,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }])
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

fn upload_chunk_prefix(upload_id: &str) -> Result<Vec<u8>, LixError> {
    let id_len = u16::try_from(upload_id.len())
        .map_err(|_| invalid_upload("upload id exceeds receipt key limit"))?;
    let mut key = Vec::with_capacity(2 + upload_id.len());
    key.extend_from_slice(&id_len.to_be_bytes());
    key.extend_from_slice(upload_id.as_bytes());
    Ok(key)
}

fn stage_upload_chunk(
    writes: &mut crate::storage_adapter::StorageWriteSet,
    upload_id: &str,
    index: u32,
    receipt: BlobChunkReceipt,
) -> Result<(), LixError> {
    let mut key = upload_chunk_prefix(upload_id)?;
    key.extend_from_slice(&index.to_be_bytes());
    let mut value = Vec::with_capacity(40);
    value.extend_from_slice(receipt.hash.as_bytes());
    value.extend_from_slice(&receipt.size_bytes.to_be_bytes());
    writes.put(
        UPLOAD_CHUNK_SPACE,
        StorageKey(Bytes::from(key)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    Ok(())
}

async fn load_upload_chunks(
    store: &impl crate::storage_adapter::StorageAdapterRead,
    upload_id: &str,
    expected_count: u32,
) -> Result<Vec<BlobChunkReceipt>, LixError> {
    let range = upload_chunk_range(upload_id)?;
    let mut receipts = Vec::with_capacity(expected_count as usize);
    let mut resume_after = None;
    loop {
        let page = store
            .scan(
                UPLOAD_CHUNK_SPACE.id,
                range.clone(),
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await?;
        for entry in &page.entries {
            let StorageProjectedValue::FullValue(value) = &entry.value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "upload chunk receipt read returned no value bytes",
                ));
            };
            if value.len() != 40 {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "upload chunk receipt has invalid length",
                ));
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&value[..32]);
            let mut size = [0u8; 8];
            size.copy_from_slice(&value[32..]);
            receipts.push(BlobChunkReceipt {
                hash: ChunkHash::from_bytes(hash),
                size_bytes: u64::from_be_bytes(size),
            });
        }
        if !page.has_more {
            break;
        }
        resume_after = Some(
            page.entries
                .last()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "upload chunk receipt scan returned an empty partial page",
                    )
                })?
                .key
                .clone(),
        );
    }
    if receipts.len() != expected_count as usize {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "upload chunk receipt sequence is incomplete",
        ));
    }
    Ok(receipts)
}

fn upload_chunk_range(upload_id: &str) -> Result<StorageKeyRange, LixError> {
    let prefix = upload_chunk_prefix(upload_id)?;
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
    use crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE;
    use crate::storage_adapter::{
        StorageAdapter, StorageAdapterRead, StorageCoreProjection, StorageKeyRange,
        StorageScanOptions,
    };
    use crate::{Engine, Memory};
    use std::ops::Bound;

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
            .upsert_file_data_part(
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
                .read_file_data("/media/proxy.mov".into(), None)
                .await
                .expect("read before publish")
                .is_none()
        );

        let resumed_session = engine
            .open_workspace_session()
            .await
            .expect("open resumed session");
        let progress = resumed_session
            .upsert_file_data_part(
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
            .read_file_data(
                "/media/proxy.mov".into(),
                Some((FILE_UPLOAD_PART_BYTES as u64 - 4)..(FILE_UPLOAD_PART_BYTES as u64 + 4)),
            )
            .await
            .expect("range read")
            .expect("published file");
        assert_eq!(
            boundary.data().as_ref(),
            [vec![0x31; 4], vec![0x72; 4]].concat().as_slice()
        );
        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open upload cleanup read");
        let temporary_receipts = read
            .scan(
                UPLOAD_CHUNK_SPACE.id,
                upload_chunk_range("movie-proxy-1").expect("upload receipt range"),
                StorageScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after: None,
                },
            )
            .await
            .expect("scan temporary upload receipts");
        assert!(
            temporary_receipts.entries.is_empty(),
            "publication must atomically remove temporary chunk receipts",
        );
        drop(read);
        let published_commit_id = resumed_session
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("published branch head")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("published commit id");

        let replay = resumed_session
            .upsert_file_data_part(
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

        resumed_session
            .upsert_file_data_part(
                "movie-proxy-copy".into(),
                "/media/proxy-copy.mov".into(),
                0,
                total,
                first.into(),
            )
            .await
            .expect("stage identical first part");
        resumed_session
            .upsert_file_data_part(
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
        let chunks = read
            .scan(
                BINARY_CAS_CHUNK_SPACE.id,
                StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                StorageScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after: None,
                },
            )
            .await
            .expect("scan CAS chunks");
        assert!(!chunks.has_more);
        assert_eq!(
            chunks.entries.len(),
            2,
            "identical media must reuse payloads"
        );
    }
}
