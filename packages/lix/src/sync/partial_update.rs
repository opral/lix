//! A bounded delivery optimization for retained native read recipes. The
//! ordinary candidate evaluator remains the only owner of coverage/publication.
use crate::{
    LixError,
    hot_state::{LogicalReadInterest, ReadInterestSnapshot},
};
use serde::{Deserialize, Serialize};

// The durable registry budgets each recipe; reserve room for array delimiters
// and the transport envelope without rejecting a valid full registry.
pub(crate) const MAX_PARTIAL_UPDATE_REQUEST_BYTES: usize = 4 * 1024 * 1024 + 65536;
pub(crate) const MAX_PARTIAL_UPDATE_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialUpdateRequest {
    pub(crate) branch_id: String,
    pub(crate) after: Option<u64>,
    pub(crate) known_cursor: u64,
    pub(crate) interests: Vec<LogicalReadInterest>,
}

impl PartialUpdateRequest {
    pub(crate) fn snapshot(&self) -> Result<ReadInterestSnapshot, LixError> {
        super::validate_sync_branch_id(&self.branch_id)?;
        if self.interests.len() > 4096 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "working set exceeds interest limit",
            ));
        }
        let bytes = serde_json::to_vec(&self.interests)
            .map_err(|_| LixError::unknown("encode working-set interests"))?
            .len();
        if bytes > MAX_PARTIAL_UPDATE_REQUEST_BYTES {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "working set exceeds byte limit",
            ));
        }
        Ok(ReadInterestSnapshot {
            revision: 0,
            interests: self
                .interests
                .iter()
                .cloned()
                .map(std::sync::Arc::new)
                .collect(),
            serialized_bytes: bytes,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialUpdateResponse {
    pub(crate) descriptor: super::LeasedPartialReplicaDescriptor,
    pub(crate) bundle: super::partial_working_set::WorkingSetBundle,
}

/// Install only immutable inputs. Even a complete server bundle is not a
/// coverage certificate; the ordinary candidate preparation still runs next.
pub(super) async fn install_bundle<S>(
    storage: &crate::storage_adapter::StorageAdapter<S>,
    state: &super::PartialReplicaState,
    bundle: &super::partial_working_set::WorkingSetBundle,
) -> Result<(), LixError>
where
    S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static,
{
    use super::native_metadata::{
        NativeMetadataRequest, NativeMetadataResponse, stage_native_metadata,
    };
    use super::native_object::{NativeObjectResponse, stage_native_objects};
    use crate::storage_adapter::{StoragePrecondition, StorageWriteOptions};
    let invalid = || LixError::new(LixError::CODE_INVALID_PARAM, "invalid working-set bundle");
    if bundle
        .metadata
        .len()
        .saturating_add(bundle.objects.len())
        .saturating_add(bundle.blobs.len())
        > 1024
        || serde_json::to_vec(bundle).map_err(|_| invalid())?.len()
            > super::partial_working_set::MAX_WORKING_SET_BYTES
        || bundle.metadata.iter().map(|v| v.bytes.len()).sum::<usize>()
            > super::native_metadata::MAX_NATIVE_METADATA_PAYLOAD_BYTES
        || bundle
            .metadata
            .iter()
            .map(|v| &v.address)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != bundle.metadata.len()
        || bundle
            .objects
            .iter()
            .map(|v| v.address)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != bundle.objects.len()
        || bundle
            .blobs
            .iter()
            .map(|v| &v.blob_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != bundle.blobs.len()
    {
        return Err(invalid());
    }
    // One durable installation for the native frontier, independent of how
    // many metadata and content objects were collected on the authority.
    if !bundle.metadata.is_empty() || !bundle.objects.is_empty() {
        for attempt in 0..4 {
            let read = storage.begin_read(Default::default()).await?;
            let (actual, raw) = super::partial_state::load_partial_replica_state(&read)
                .await?
                .ok_or_else(|| LixError::unknown("working-set admission disappeared"))?;
            if &actual != state {
                return Err(LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "working-set admission changed",
                ));
            }
            let mut writes = storage.new_write_set();
            let mut preconditions = vec![StoragePrecondition::KeyValueEquals {
                space: super::PARTIAL_REPLICA_STATE_SPACE,
                key: super::partial_state::partial_replica_state_key(),
                expected: raw,
            }];
            for objects in bundle.metadata.chunks(32) {
                let request = NativeMetadataRequest {
                    epoch_id: state.epoch_id().into(),
                    objects: objects.iter().map(|v| v.address.clone()).collect(),
                };
                let response = NativeMetadataResponse {
                    lix_id: state.repository_id().into(),
                    epoch_id: state.epoch_id().into(),
                    objects: objects.to_vec(),
                };
                preconditions.extend(
                    stage_native_metadata(&read, &mut writes, state, &request, &response).await?,
                );
            }
            for objects in bundle.objects.chunks(32) {
                stage_native_objects(
                    state.repository_id(),
                    &objects.iter().map(|v| v.address).collect::<Vec<_>>(),
                    &NativeObjectResponse {
                        lix_id: state.repository_id().into(),
                        objects: objects.to_vec(),
                    },
                    &mut writes,
                )?;
            }
            drop(read);
            match storage
                .commit_partial_replica_write_set(
                    super::partial_replica_write_capability(),
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Err(crate::storage_adapter::StorageWriteSetError::Storage(
                    crate::storage_adapter::StorageError::PreconditionFailed(_),
                )) if attempt < 3 => continue,
                result => {
                    result?;
                    break;
                }
            }
        }
    }
    for blob in &bundle.blobs {
        super::partial_blob::install_manifest(
            storage,
            state,
            crate::binary_cas::BlobId::from_hex(&blob.blob_id)?,
            blob,
        )
        .await?;
    }
    Ok(())
}
