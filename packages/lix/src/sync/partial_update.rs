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
        NativeMetadataRequest, NativeMetadataResponse, native_metadata_residency,
        stage_native_metadata, validate_native_metadata_response,
    };
    use super::native_object::{NativeObjectResponse, stage_native_objects};
    use crate::storage_adapter::{
        StorageAdapterRead as _, StoragePrecondition, StorageWriteOptions,
    };
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
            let headers = bundle
                .metadata
                .iter()
                .filter(|object| {
                    matches!(
                        object.address,
                        crate::tracked_state::NativeMetadataRef::CommitStateHeader(_)
                    )
                })
                .collect::<Vec<_>>();
            let header_keys = headers
                .iter()
                .map(|object| {
                    uuid::Uuid::parse_str(object.address.id())
                        .map(|id| {
                            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
                                id.as_bytes(),
                            ))
                        })
                        .map_err(|_| invalid())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let header_values = read
                .get_many(&[crate::storage_adapter::StorageGetManyRequest {
                    space: crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                    keys: &header_keys,
                    opts: Default::default(),
                }])
                .await?
                .values;
            if header_values.len() != headers.len() {
                return Err(invalid());
            }
            let mut different_headers = std::collections::BTreeSet::new();
            for ((object, key), value) in headers.iter().zip(&header_keys).zip(header_values) {
                if let Some(crate::storage_adapter::StorageProjectedValue::FullValue(existing)) =
                    value
                {
                    if existing.as_ref() != object.bytes.as_slice() {
                        different_headers
                            .insert(<[u8; 16]>::try_from(key.0.as_ref()).map_err(|_| invalid())?);
                    }
                }
            }
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
                // The UUID identifies a logical record, not its physical
                // encoding. Own published commits may already have a valid
                // locally authored header/locator representation. Match the
                // ordinary hydration owner: validate both inputs, preserve
                // resident bytes, and install only missing native metadata.
                validate_native_metadata_response(state.repository_id(), &request, &response)?;
                let residency = native_metadata_residency(&read, state, &request.objects).await?;
                let missing = response
                    .objects
                    .into_iter()
                    .zip(residency)
                    .filter_map(|(object, resident)| (!resident).then_some(object))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    let request = NativeMetadataRequest {
                        epoch_id: state.epoch_id().into(),
                        objects: missing
                            .iter()
                            .map(|object| object.address.clone())
                            .collect(),
                    };
                    let response = NativeMetadataResponse {
                        lix_id: state.repository_id().into(),
                        epoch_id: state.epoch_id().into(),
                        objects: missing,
                    };
                    preconditions.extend(
                        stage_native_metadata(&read, &mut writes, state, &request, &response)
                            .await?,
                    );
                }
            }
            for objects in bundle.objects.chunks(32) {
                super::native_object::validate_response(
                    state.repository_id(),
                    &objects
                        .iter()
                        .map(|object| object.address)
                        .collect::<Vec<_>>(),
                    &NativeObjectResponse {
                        lix_id: state.repository_id().into(),
                        objects: objects.to_vec(),
                    },
                )?;
            }
            let keys = bundle
                .objects
                .iter()
                .map(|object| {
                    crate::storage_adapter::StorageKey(bytes::Bytes::from(
                        object.address.storage_key(),
                    ))
                })
                .collect::<Vec<_>>();
            let requests = bundle
                .objects
                .iter()
                .zip(&keys)
                .map(
                    |(object, key)| crate::storage_adapter::StorageGetManyRequest {
                        space: object.address.space(),
                        keys: std::slice::from_ref(key),
                        opts: Default::default(),
                    },
                )
                .collect::<Vec<_>>();
            let values = read.get_many(&requests).await?.values;
            if values.len() != bundle.objects.len() {
                return Err(invalid());
            }
            let mut missing = Vec::new();
            for ((object, key), value) in bundle.objects.iter().zip(&keys).zip(values) {
                use crate::tracked_state::NativeObjectRef;
                let owner = match object.address {
                    NativeObjectRef::MutationCatalog { commit_id, .. }
                    | NativeObjectRef::CommitDeltaPart { commit_id, .. } => Some(commit_id),
                    _ => None,
                };
                if owner.is_some_and(|owner| different_headers.contains(&owner)) {
                    // Keep the resident physical representation. These server
                    // objects belong to a different header and are not installed;
                    // ordinary candidate preparation validates its local inputs
                    // before publishing any serving state or coverage.
                    continue;
                }
                match value {
                    Some(crate::storage_adapter::StorageProjectedValue::FullValue(bytes)) => {
                        object.address.validate(&bytes)?
                    }
                    Some(_) => return Err(invalid()),
                    None => {
                        preconditions.push(StoragePrecondition::KeyAbsent {
                            space: object.address.space(),
                            key: key.clone(),
                        });
                        missing.push(object.clone());
                    }
                }
            }
            for objects in missing.chunks(32) {
                stage_native_objects(
                    state.repository_id(),
                    &objects
                        .iter()
                        .map(|object| object.address)
                        .collect::<Vec<_>>(),
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
