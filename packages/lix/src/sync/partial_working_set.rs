//! Opportunistic bounded delivery of inputs read by the ordinary candidate
//! evaluator. This transfers immutable inputs, never authority HOT/local state
//! or coverage claims. The client remains responsible for candidate validation.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use super::native_metadata::NativeMetadata;
use super::native_object::NativeObject;
use super::partial_state::PartialReplicaState;
use crate::LixError;
use crate::hot_state::ReadInterestSnapshot;
use crate::storage_adapter::{
    StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection, StorageError,
    StorageGetManyRequest, StorageGetManyResult, StorageKeyRange, StorageProjectedValue,
    StorageScanCursor, StorageSpace,
};
use crate::tracked_state::{NativeMetadataRef, NativeObjectRef};

pub(crate) const MAX_WORKING_SET_BYTES: usize = 1024 * 1024;
const MAX_WORKING_SET_ENTRIES: usize = 1024;
fn budget_exhausted() -> StorageError {
    StorageError::Io("working-set delivery collection budget exhausted".into())
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkingSetBundle {
    pub(crate) objects: Vec<NativeObject>,
    pub(crate) metadata: Vec<NativeMetadata>,
    pub(crate) blobs: Vec<super::SyncBlobManifest>,
    pub(crate) complete: bool,
}

#[derive(Default)]
struct Recording {
    entries: BTreeMap<(&'static str, Vec<u8>), (StorageSpace, bytes::Bytes)>,
    bytes: usize,
    incomplete: bool,
}
impl Recording {
    fn insert(&mut self, space: StorageSpace, key: &[u8], bytes: &bytes::Bytes) {
        if self.incomplete || self.entries.contains_key(&(space.name, key.to_vec())) {
            return;
        }
        if self.entries.len() == MAX_WORKING_SET_ENTRIES
            || self.bytes.saturating_add(bytes.len()) > MAX_WORKING_SET_BYTES
        {
            self.incomplete = true;
            self.entries.clear();
            return;
        }
        self.bytes += bytes.len();
        self.entries
            .insert((space.name, key.to_vec()), (space, bytes.clone()));
    }
}

fn native_family(space: StorageSpace) -> bool {
    [
        NativeObjectRef::TrackedStateTreeChunk([0; 32]),
        NativeObjectRef::ScopedRangeNode([0; 32]),
        NativeObjectRef::MutationDirectoryNode([0; 32]),
        NativeObjectRef::MutationCatalog {
            commit_id: [0; 16],
            expected_digest: [0; 32],
        },
        NativeObjectRef::CommitDeltaPart {
            commit_id: [0; 16],
            part_index: 0,
            expected_digest: [0; 32],
            replacement: false,
        },
    ]
    .iter()
    .any(|address| address.space() == space)
        || space == crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
        || space == crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE
        || space == crate::changelog::COMMIT_SPACE
}
fn record_family(space: StorageSpace) -> bool {
    native_family(space) || space == crate::binary_cas::BINARY_CAS_MANIFEST_SPACE
}

#[derive(Clone)]
struct RecordingRead<R> {
    inner: R,
    recording: Arc<Mutex<Recording>>,
}
impl<R: StorageAdapterRead> StorageAdapterRead for RecordingRead<R> {
    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<StorageGetManyResult, StorageError> {
        if self
            .recording
            .lock()
            .map_err(|_| StorageError::Io("working-set recorder poisoned".into()))?
            .incomplete
        {
            return Err(budget_exhausted());
        }
        // Presence-only dependency checks must also contribute their immutable
        // payloads; preserve the caller's requested projection on return.
        let expanded = requests
            .iter()
            .map(|request| StorageGetManyRequest {
                space: request.space,
                keys: request.keys,
                opts: crate::storage_adapter::StorageGetOptions {
                    projection: if record_family(request.space) {
                        StorageCoreProjection::FullValue
                    } else {
                        request.opts.projection
                    },
                    ..request.opts.clone()
                },
            })
            .collect::<Vec<_>>();
        let mut result = self.inner.get_many(&expanded).await?;
        if result.values.len()
            != requests
                .iter()
                .map(|request| request.keys.len())
                .sum::<usize>()
        {
            return Err(StorageError::Corruption(
                "working-set read cardinality mismatch".into(),
            ));
        }
        let mut index = 0;
        let mut recording = self
            .recording
            .lock()
            .map_err(|_| StorageError::Io("working-set recorder poisoned".into()))?;
        for request in requests {
            for key in request.keys {
                if record_family(request.space) {
                    if let Some(StorageProjectedValue::FullValue(bytes)) = &result.values[index] {
                        if request.space == crate::binary_cas::BINARY_CAS_MANIFEST_SPACE {
                            let manifest = crate::binary_cas::decode_binary_cas_manifest(bytes)
                                .map_err(|error| StorageError::Corruption(error.to_string()))?;
                            if manifest.size_bytes()
                                > super::blob::MAX_INLINE_SYNC_BLOB_BYTES as u64
                            {
                                recording.incomplete = true;
                                recording.entries.clear();
                                return Err(budget_exhausted());
                            }
                        }
                        recording.insert(request.space, &key.0, bytes);
                        if recording.incomplete {
                            return Err(budget_exhausted());
                        }
                        if request.opts.projection == StorageCoreProjection::KeyOnly {
                            result.values[index] = Some(StorageProjectedValue::KeyOnly);
                        }
                    }
                }
                index += 1;
            }
        }
        Ok(result)
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> Result<StorageScanCursor<'_>, StorageError> {
        if record_family(space) {
            // Native recipes normally resolve immutable payloads by point read.
            // An inventory scan is not a license to export an entire inventory.
            self.recording
                .lock()
                .map_err(|_| StorageError::Io("working-set recorder poisoned".into()))?
                .incomplete = true;
            return Err(budget_exhausted());
        }
        self.inner.begin_scan(space, range, opts).await
    }
}

fn invalid() -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        "invalid native working-set address",
    )
}
fn object_address(
    space: StorageSpace,
    key: &[u8],
    bytes: &[u8],
) -> Result<Option<NativeObjectRef>, LixError> {
    for probe in [
        NativeObjectRef::TrackedStateTreeChunk([0; 32]),
        NativeObjectRef::ScopedRangeNode([0; 32]),
        NativeObjectRef::MutationDirectoryNode([0; 32]),
    ] {
        if space == probe.space() {
            let digest = key.try_into().map_err(|_| invalid())?;
            return Ok(Some(match probe {
                NativeObjectRef::TrackedStateTreeChunk(_) => {
                    NativeObjectRef::TrackedStateTreeChunk(digest)
                }
                NativeObjectRef::ScopedRangeNode(_) => NativeObjectRef::ScopedRangeNode(digest),
                _ => NativeObjectRef::MutationDirectoryNode(digest),
            }));
        }
    }
    if space
        == (NativeObjectRef::MutationCatalog {
            commit_id: [0; 16],
            expected_digest: [0; 32],
        })
        .space()
    {
        return Ok(Some(NativeObjectRef::MutationCatalog {
            commit_id: key.try_into().map_err(|_| invalid())?,
            expected_digest: *blake3::hash(bytes).as_bytes(),
        }));
    }
    if space
        == (NativeObjectRef::CommitDeltaPart {
            commit_id: [0; 16],
            part_index: 0,
            expected_digest: [0; 32],
            replacement: false,
        })
        .space()
    {
        if key.len() != 20 && key.len() != 52 {
            return Err(invalid());
        }
        return Ok(Some(NativeObjectRef::CommitDeltaPart {
            commit_id: key[..16].try_into().map_err(|_| invalid())?,
            part_index: u32::from_be_bytes(key[16..20].try_into().map_err(|_| invalid())?),
            expected_digest: if key.len() == 52 {
                key[20..].try_into().map_err(|_| invalid())?
            } else {
                *blake3::hash(bytes).as_bytes()
            },
            replacement: key.len() == 52,
        }));
    }
    Ok(None)
}

pub(crate) async fn collect_working_set<R>(
    read: R,
    state: &PartialReplicaState,
    interests: &ReadInterestSnapshot,
    plugin_host: crate::plugin::runtime::PluginRuntimeHost,
) -> Result<WorkingSetBundle, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let recording = Arc::new(Mutex::new(Recording::default()));
    let evaluation = super::partial_candidate_prepare::prepare_authority_working_set(
        RecordingRead {
            inner: read.clone(),
            recording: recording.clone(),
        },
        state,
        interests,
        plugin_host,
    )
    .await;
    if let Err(error) = evaluation {
        if error.code == LixError::CODE_STORAGE_ERROR
            && error.message == budget_exhausted().to_string()
        {
            return Ok(WorkingSetBundle::default());
        }
        return Err(error);
    }
    let entries = {
        let mut captured = recording
            .lock()
            .map_err(|_| LixError::unknown("working-set recorder poisoned"))?;
        if captured.incomplete {
            return Ok(WorkingSetBundle::default());
        }
        std::mem::take(&mut captured.entries)
    };
    let mut bundle = WorkingSetBundle {
        complete: true,
        ..Default::default()
    };
    let mut blobs = BTreeSet::new();
    let mut metadata_bytes = 0usize;
    for ((_, key), (space, bytes)) in entries {
        if let Some(address) = object_address(space, &key, &bytes)? {
            address.validate(&bytes)?;
            bundle.objects.push(NativeObject {
                address,
                bytes: bytes.to_vec(),
            });
        } else if space == crate::binary_cas::BINARY_CAS_MANIFEST_SPACE {
            blobs.insert(crate::binary_cas::BlobId::from_bytes(
                key.as_slice().try_into().map_err(|_| invalid())?,
            ));
        } else {
            let id = uuid::Uuid::from_slice(&key)
                .map_err(|_| invalid())?
                .to_string();
            let address = if space == crate::changelog::COMMIT_SPACE {
                NativeMetadataRef::CommitGraphRecord(id)
            } else if space == crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE {
                NativeMetadataRef::ChangeLocator(id)
            } else if space == crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE {
                NativeMetadataRef::CommitStateHeader(id)
            } else {
                return Err(invalid());
            };
            metadata_bytes += bytes.len();
            if metadata_bytes > super::native_metadata::MAX_NATIVE_METADATA_PAYLOAD_BYTES {
                return Ok(WorkingSetBundle::default());
            }
            bundle.metadata.push(NativeMetadata {
                address,
                bytes: bytes.to_vec(),
            });
        }
    }
    for id in blobs {
        let metadata = crate::binary_cas::load_metadata_many(&read, &[id])
            .await?
            .into_vec()
            .pop()
            .flatten()
            .ok_or_else(invalid)?;
        if metadata.size_bytes > super::blob::MAX_INLINE_SYNC_BLOB_BYTES as u64 {
            return Ok(WorkingSetBundle::default());
        }
        let chunks = crate::binary_cas::load_canonical_blob_chunks(&read, id)
            .await?
            .ok_or_else(invalid)?;
        let manifest = super::blob::encode_manifest(id, &chunks)?;
        if manifest.inline_bytes_base64.is_none() {
            return Ok(WorkingSetBundle::default());
        }
        bundle.blobs.push(manifest);
        if serde_json::to_vec(&bundle).map_err(|_| invalid())?.len() > MAX_WORKING_SET_BYTES {
            return Ok(WorkingSetBundle::default());
        }
    }
    if serde_json::to_vec(&bundle).map_err(|_| invalid())?.len() > MAX_WORKING_SET_BYTES {
        return Ok(WorkingSetBundle::default());
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oversized_file_collector_falls_back_before_reading_content_chunks() {
        #[derive(Clone)]
        struct RejectChunks<R> {
            inner: R,
            oversized_chunk_keys: BTreeSet<Vec<u8>>,
            chunks: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl<R: StorageAdapterRead> StorageAdapterRead for RejectChunks<R> {
            async fn get_many(
                &self,
                requests: &[StorageGetManyRequest<'_>],
            ) -> Result<StorageGetManyResult, StorageError> {
                if requests.iter().any(|r| {
                    r.space == crate::binary_cas::BINARY_CAS_CHUNK_SPACE
                        && r.keys
                            .iter()
                            .any(|key| self.oversized_chunk_keys.contains(key.0.as_ref()))
                }) {
                    self.chunks
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(StorageError::Corruption(
                        "oversized collector read payload chunks".into(),
                    ));
                }
                self.inner.get_many(requests).await
            }
            async fn begin_scan(
                &self,
                space: StorageSpace,
                range: StorageKeyRange,
                opts: StorageBeginScanOptions,
            ) -> Result<StorageScanCursor<'_>, StorageError> {
                if space == crate::binary_cas::BINARY_CAS_CHUNK_SPACE {
                    self.chunks
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(StorageError::Corruption(
                        "oversized collector scanned payload chunks".into(),
                    ));
                }
                self.inner.begin_scan(space, range, opts).await
            }
        }
        let authority = crate::open_lix().await.unwrap();
        let id = "00000000-0000-7000-8000-000000000124";
        let payload = vec![71; super::super::blob::MAX_INLINE_SYNC_BLOB_BYTES + 1];
        let oversized_chunk_keys = crate::binary_cas::CanonicalBlobManifest::from_bytes(&payload)
            .chunks
            .into_iter()
            .map(|chunk| chunk.hash.as_bytes().to_vec())
            .collect();
        authority
            .execute(
                "INSERT INTO lix_file(id,path,content) VALUES($1,'/oversized.bin',$2)",
                &[
                    crate::Value::Text(id.into()),
                    crate::Value::Blob(payload.into()),
                ],
            )
            .await
            .unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            descriptor.clone(),
        )
        .unwrap()
        .with_descriptor_and_fresh_generations(descriptor)
        .unwrap();
        let mut request = crate::hot_state::HotStateScanRequest::default();
        request.filter.branch_ids = vec![authority.active_branch_id().await.unwrap()];
        request.filter.schema_keys = vec!["lix_file".into()];
        request.projection.columns = vec!["content".into()];
        let interests = ReadInterestSnapshot {
            revision: 1,
            serialized_bytes: 0,
            interests: vec![Arc::new(
                crate::hot_state::LogicalReadInterest::FileContent {
                    request,
                    file_ids: Some(vec![id.into()]),
                    directory_ids: None,
                    root_directory: false,
                    indexed: true,
                    path_predicate: crate::hot_state::FilePathInterest::All,
                    byte_range: None,
                },
            )],
        };
        let storage = authority.storage_adapter();
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage.begin_read(Default::default()).await.unwrap(),
        );
        let chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let bundle = collect_working_set(
            RejectChunks {
                inner: read,
                oversized_chunk_keys,
                chunks: chunks.clone(),
            },
            &state,
            &interests,
            crate::plugin::runtime::PluginRuntimeHost::new(Arc::new(
                crate::plugin::runtime::UnsupportedWasmRuntime,
            )),
        )
        .await
        .unwrap();
        assert!(!bundle.complete);
        assert!(bundle.objects.is_empty() && bundle.metadata.is_empty() && bundle.blobs.is_empty());
        assert_eq!(chunks.load(std::sync::atomic::Ordering::SeqCst), 0);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn exhausted_recorder_aborts_before_another_storage_read() {
        struct CountRead<R> {
            inner: R,
            calls: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl<R: StorageAdapterRead> StorageAdapterRead for CountRead<R> {
            async fn get_many(
                &self,
                requests: &[StorageGetManyRequest<'_>],
            ) -> Result<StorageGetManyResult, StorageError> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.inner.get_many(requests).await
            }
            async fn begin_scan(
                &self,
                space: StorageSpace,
                range: StorageKeyRange,
                opts: StorageBeginScanOptions,
            ) -> Result<StorageScanCursor<'_>, StorageError> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.inner.begin_scan(space, range, opts).await
            }
        }
        let storage = crate::storage_adapter::StorageAdapter::new(crate::Memory::new());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recording = Arc::new(Mutex::new(Recording::default()));
        recording.lock().unwrap().insert(
            NativeObjectRef::TrackedStateTreeChunk([0; 32]).space(),
            &[0; 32],
            &bytes::Bytes::from(vec![0; MAX_WORKING_SET_BYTES + 1]),
        );
        let reader = RecordingRead {
            inner: CountRead {
                inner: read,
                calls: calls.clone(),
            },
            recording,
        };
        assert!(
            matches!(reader.get_many(&[]).await, Err(StorageError::Io(message)) if message == "working-set delivery collection budget exhausted")
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn typed_addresses_round_trip_existing_storage_keys() {
        let payload = b"native payload";
        let digest = *blake3::hash(payload).as_bytes();
        for address in [
            NativeObjectRef::TrackedStateTreeChunk(digest),
            NativeObjectRef::ScopedRangeNode(digest),
            NativeObjectRef::MutationDirectoryNode(digest),
            NativeObjectRef::MutationCatalog {
                commit_id: [1; 16],
                expected_digest: digest,
            },
            NativeObjectRef::CommitDeltaPart {
                commit_id: [2; 16],
                part_index: 7,
                expected_digest: digest,
                replacement: false,
            },
            NativeObjectRef::CommitDeltaPart {
                commit_id: [3; 16],
                part_index: 8,
                expected_digest: digest,
                replacement: true,
            },
        ] {
            assert_eq!(
                object_address(address.space(), &address.storage_key(), payload).unwrap(),
                Some(address)
            );
        }
        assert!(!record_family(crate::branch::BRANCH_HEAD_CONTROL_SPACE));
        assert!(!record_family(crate::hot_state::ROOT_CURRENT_BASE_SPACE));
    }

    #[test]
    fn recording_deduplicates_and_discards_oversized_bundle() {
        let mut recorder = Recording::default();
        let space = NativeObjectRef::TrackedStateTreeChunk([0; 32]).space();
        let payload = bytes::Bytes::from_static(b"payload");
        recorder.insert(space, &[1; 32], &payload);
        recorder.insert(space, &[1; 32], &payload);
        assert_eq!(recorder.entries.len(), 1);
        assert_eq!(recorder.bytes, payload.len());
        recorder.insert(
            space,
            &[2; 32],
            &bytes::Bytes::from(vec![0; MAX_WORKING_SET_BYTES]),
        );
        assert!(recorder.incomplete);
        assert!(recorder.entries.is_empty());
        recorder.insert(space, &[3; 32], &payload);
        assert!(recorder.entries.is_empty());
    }
}
