//! Explicit authority metadata transfer for a partial replica.
//!
//! UUID-addressed metadata is not content-addressed object data. Receipt epoch
//! binding, native decoding, and byte-for-byte existing-value guards precede
//! publication. Exact locator responses may include bounded owner dependencies;
//! they do not traverse history or certify coverage.

use super::native_object::base64_bytes;
use super::partial_state::{
    PartialReplicaState, load_partial_replica_state, partial_replica_state_key,
};
use crate::changelog::{ChangeId, CommitId};
use crate::storage_adapter::{
    Storage, StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StoragePrecondition, StorageProjectedValue, StorageReadOptions, StorageSpace, StorageValue,
    StorageWriteSet,
};
use crate::tracked_state::NativeMetadataRef;
use crate::{Lix, LixError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const MAX_NATIVE_METADATA_BATCH: usize = 32;
/// A bounded history walk may return a graph record and one optional state
/// header for each of its 64 selected commits. Ordinary exact metadata
/// requests and dependency selection remain capped at `MAX_NATIVE_METADATA_BATCH`.
pub(crate) const MAX_NATIVE_METADATA_WALK_RECORDS: usize = 128;
pub(crate) const MAX_NATIVE_METADATA_PAYLOAD_BYTES: usize = 256 * 1024;
pub(crate) const MAX_NATIVE_METADATA_RESPONSE_BYTES: usize =
    MAX_NATIVE_METADATA_PAYLOAD_BYTES.div_ceil(3) * 4 + 65536;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeMetadataRequest {
    pub(crate) epoch_id: String,
    pub(crate) objects: Vec<NativeMetadataRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeMetadataResponse {
    pub(crate) lix_id: String,
    pub(crate) epoch_id: String,
    pub(crate) objects: Vec<NativeMetadata>,
    #[serde(
        default,
        skip_serializing_if = "super::native_dependencies::NativeDependencyBundle::is_empty"
    )]
    pub(crate) dependencies: super::native_dependencies::NativeDependencyBundle,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeMetadata {
    pub(crate) address: NativeMetadataRef,
    #[serde(with = "base64_bytes")]
    pub(crate) bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) checkpoint_conversation: Option<CheckpointConversationEnvelope>,
}

/// The non-optional wrapper makes a missing nullable wire property a serde
/// error while preserving an explicit JSON null as authenticated information.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct RequiredNullable<T>(pub(crate) Option<T>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CheckpointConversationEnvelope {
    pub(crate) commit_id: String,
    #[serde(deserialize_with = "deserialize_required_nullable_envelope_value")]
    pub(crate) conversation_id: RequiredNullable<String>,
}

fn deserialize_required_nullable_envelope_value<'de, D, T>(
    deserializer: D,
) -> Result<RequiredNullable<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(RequiredNullable)
}
fn invalid(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}
fn canonical_id(value: &str) -> Result<CommitId, LixError> {
    if crate::storage_codec::id_string::uuid_bytes_from_canonical(value).is_none() {
        return Err(invalid("native metadata ID must be a canonical UUID"));
    }
    CommitId::parse(value).map_err(|_| invalid("native metadata ID must be a canonical UUID"))
}
pub(crate) fn space(address: &NativeMetadataRef) -> StorageSpace {
    match address {
        NativeMetadataRef::CommitStateHeader(_) => {
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
        }
        NativeMetadataRef::CommitGraphRecord(_) => crate::changelog::COMMIT_SPACE,
        NativeMetadataRef::CheckpointConversation(_) => crate::changelog::COMMIT_SPACE,
        NativeMetadataRef::ChangeLocator(_) => {
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE
        }
    }
}
pub(crate) fn key(address: &NativeMetadataRef) -> Result<StorageKey, LixError> {
    let id = canonical_id(address.id())?;
    Ok(match address {
        NativeMetadataRef::CommitStateHeader(_) => {
            crate::tracked_state::commit_state_authority_key(id)
        }
        NativeMetadataRef::CommitGraphRecord(_) => {
            StorageKey(Bytes::from(crate::changelog::commit_key(id)))
        }
        NativeMetadataRef::CheckpointConversation(_) => {
            StorageKey(Bytes::from(crate::changelog::commit_key(id)))
        }
        NativeMetadataRef::ChangeLocator(_) => {
            StorageKey(Bytes::copy_from_slice(id.as_uuid().as_bytes()))
        }
    })
}
pub(crate) fn validate_bytes(address: &NativeMetadataRef, bytes: &[u8]) -> Result<(), LixError> {
    let id = canonical_id(address.id())?;
    match address {
        NativeMetadataRef::CommitStateHeader(_) => {
            crate::tracked_state::decode_commit_state_authority_id(
                id,
                Some(StorageProjectedValue::FullValue(Bytes::copy_from_slice(
                    bytes,
                ))),
            )?;
            Ok(())
        }
        NativeMetadataRef::CommitGraphRecord(_) => {
            crate::commit_graph::validate_native_commit_graph_record(id, bytes)
        }
        NativeMetadataRef::CheckpointConversation(_) => {
            crate::commit_graph::validate_native_commit_graph_record(id, bytes)?;
            let record: crate::changelog::CommitRecord =
                crate::storage_codec::decode("commit record", bytes)?;
            if !record.is_checkpoint {
                return Err(invalid(
                    "checkpoint conversation metadata refers to a non-checkpoint commit",
                ));
            }
            Ok(())
        }
        NativeMetadataRef::ChangeLocator(_) => {
            crate::tracked_state::decode_change_locator(ChangeId::new(*id.as_uuid()), bytes)?;
            Ok(())
        }
    }
}
/// Check one native record only after matching its exact local admission.
/// An invalid resident record is corruption, never an invitation to refetch.
#[cfg(test)]
pub(super) async fn native_metadata_is_resident(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    address: &NativeMetadataRef,
) -> Result<bool, LixError> {
    Ok(native_metadata_residency(read, state, std::slice::from_ref(address)).await?[0])
}

/// Validate one admitted, bounded metadata frontier with one grouped point read.
/// Every resident payload is checked even when another entry is absent.
pub(super) async fn native_metadata_residency(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    addresses: &[NativeMetadataRef],
) -> Result<Vec<bool>, LixError> {
    validate_native_metadata_request(&NativeMetadataRequest {
        epoch_id: state.epoch_id().to_owned(),
        objects: addresses.to_vec(),
    })?;
    let Some((actual, _)) = load_partial_replica_state(read).await? else {
        return Err(invalid(
            "native metadata requires an installed partial epoch",
        ));
    };
    if &actual != state {
        return Err(invalid("native metadata partial admission changed"));
    }
    let keys = addresses.iter().map(key).collect::<Result<Vec<_>, _>>()?;
    let requests = addresses
        .iter()
        .zip(&keys)
        .map(|(address, key)| StorageGetManyRequest {
            space: space(address),
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let (values, _, _) = crate::storage_adapter::collect_bounded_point_pages(
        read,
        &requests,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
            max_single_value_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        },
        MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        32,
    )
    .await?;
    let values = values.values;
    if values.len() != addresses.len() {
        return Err(invalid("native metadata storage cardinality mismatch"));
    }
    let mut resident = addresses
        .iter()
        .zip(values)
        .map(|(address, value)| match value {
            None => Ok(false),
            Some(StorageProjectedValue::FullValue(bytes)) => {
                if bytes.len() > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
                    return Err(invalid("native metadata payload exceeds bound"));
                }
                validate_bytes(address, &bytes)?;
                Ok(true)
            }
            Some(StorageProjectedValue::KeyOnly) => {
                Err(invalid("native metadata read omitted payload"))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let conversation_requests = addresses
        .iter()
        .enumerate()
        .filter_map(|(index, address)| match address {
            NativeMetadataRef::CheckpointConversation(id) => Some((index, id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !conversation_requests.is_empty() {
        let ids = conversation_requests
            .iter()
            .map(|(_, id)| canonical_id(id))
            .collect::<Result<Vec<_>, _>>()?;
        let coverage = crate::checkpoint_conversation::partial_checkpoint_conversation_residency(
            read,
            state.epoch_id(),
            &ids,
        )
        .await?;
        for ((index, _), covered) in conversation_requests.into_iter().zip(coverage) {
            resident[index] &= covered;
        }
    }
    Ok(resident)
}

pub(crate) fn validate_native_metadata_request(
    request: &NativeMetadataRequest,
) -> Result<(), LixError> {
    validate_native_metadata_addresses(
        &request.epoch_id,
        &request.objects,
        MAX_NATIVE_METADATA_BATCH,
    )
}

fn validate_native_metadata_addresses(
    epoch_id: &str,
    addresses: &[NativeMetadataRef],
    max_records: usize,
) -> Result<(), LixError> {
    canonical_id(epoch_id)?;
    if max_records == 0
        || max_records > MAX_NATIVE_METADATA_WALK_RECORDS
        || addresses.is_empty()
        || addresses.len() > max_records
    {
        return Err(invalid("native metadata request exceeds its record bound"));
    }
    let mut unique = BTreeSet::new();
    for address in addresses {
        key(address)?;
        if !unique.insert(address) {
            return Err(invalid("native metadata request repeats an address"));
        }
    }
    Ok(())
}
pub(crate) fn validate_native_metadata_response(
    repository_id: &str,
    request: &NativeMetadataRequest,
    response: &NativeMetadataResponse,
) -> Result<(), LixError> {
    validate_native_metadata_request(request)?;
    validate_native_metadata_response_for_addresses(
        repository_id,
        &request.epoch_id,
        &request.objects,
        MAX_NATIVE_METADATA_BATCH,
        response,
    )
}

/// Validates an already-authorized exact response address list. Walk responses
/// use this shared payload/owner validator with up to 128 records while the
/// standalone exact metadata endpoint remains capped at 32 request objects.
pub(crate) fn validate_native_metadata_response_for_addresses(
    repository_id: &str,
    epoch_id: &str,
    expected_addresses: &[NativeMetadataRef],
    max_records: usize,
    response: &NativeMetadataResponse,
) -> Result<(), LixError> {
    validate_native_metadata_addresses(epoch_id, expected_addresses, max_records)?;
    if response.lix_id != repository_id
        || response.epoch_id != epoch_id
        || response.objects.len() != expected_addresses.len()
    {
        return Err(invalid(
            "native metadata response repository, epoch or cardinality mismatch",
        ));
    }
    let mut total = 0usize;
    let mut seen = BTreeSet::new();
    for (expected, object) in expected_addresses.iter().zip(&response.objects) {
        key(expected)?;
        if !seen.insert(expected) {
            return Err(invalid("native metadata response repeats an address"));
        }
        if expected != &object.address {
            return Err(invalid("native metadata response address mismatch"));
        }
        total = total
            .checked_add(object.bytes.len())
            .ok_or_else(|| invalid("native metadata payload exceeds bound"))?;
        if total > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
            return Err(invalid("native metadata payload exceeds bound"));
        }
        validate_bytes(expected, &object.bytes)?;
        validate_checkpoint_conversation_envelope(
            expected,
            &object.bytes,
            object.checkpoint_conversation.as_ref(),
        )?;
    }
    super::native_dependencies::validate(response, max_records)?;
    Ok(())
}

pub(super) fn validate_checkpoint_conversation_envelope(
    address: &NativeMetadataRef,
    bytes: &[u8],
    envelope: Option<&CheckpointConversationEnvelope>,
) -> Result<(), LixError> {
    let (commit_id, is_checkpoint, requires_conversation_fact) = match address {
        NativeMetadataRef::CommitGraphRecord(id) => {
            let record: crate::changelog::CommitRecord =
                crate::storage_codec::decode("commit record", bytes)?;
            (id.as_str(), record.is_checkpoint, false)
        }
        NativeMetadataRef::CheckpointConversation(id) => {
            let record: crate::changelog::CommitRecord =
                crate::storage_codec::decode("commit record", bytes)?;
            (id.as_str(), record.is_checkpoint, true)
        }
        _ => {
            return if envelope.is_none() {
                Ok(())
            } else {
                Err(invalid(
                    "checkpoint conversation envelope is unrelated to native metadata",
                ))
            };
        }
    };
    if !is_checkpoint {
        return if envelope.is_none() && !requires_conversation_fact {
            Ok(())
        } else {
            Err(invalid(
                "checkpoint conversation envelope refers to a non-checkpoint",
            ))
        };
    }
    let envelope = envelope.ok_or_else(|| {
        invalid("checkpoint graph metadata omitted its nullable conversation envelope")
    })?;
    if envelope.commit_id != commit_id
        || crate::storage_codec::id_string::uuid_bytes_from_canonical(&envelope.commit_id)
            .is_none()
    {
        return Err(invalid(
            "checkpoint conversation envelope commit identity mismatch",
        ));
    }
    if let Some(conversation_id) = &envelope.conversation_id.0
        && crate::storage_codec::id_string::uuid_bytes_from_canonical(conversation_id).is_none()
    {
        return Err(invalid(
            "checkpoint conversation envelope ID must be a canonical UUID",
        ));
    }
    Ok(())
}
impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    pub(crate) async fn read_sync_native_metadata(
        &self,
        request: &NativeMetadataRequest,
    ) -> Result<NativeMetadataResponse, LixError> {
        self.read_sync_native_metadata_with_lease(request, None)
            .await
    }
    pub(crate) async fn read_sync_native_metadata_leased(
        &self,
        request: &NativeMetadataRequest,
        lease_id: &str,
    ) -> Result<NativeMetadataResponse, LixError> {
        self.read_sync_native_metadata_with_lease(request, Some(lease_id))
            .await
    }
    async fn read_sync_native_metadata_with_lease(
        &self,
        request: &NativeMetadataRequest,
        lease_id: Option<&str>,
    ) -> Result<NativeMetadataResponse, LixError> {
        validate_native_metadata_request(request)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if let Some(id) = lease_id {
            crate::gc::require_native_baseline_lease(
                &read,
                id,
                self.active_account_id(),
                crate::telemetry::unix_time_ms(),
            )
            .await?;
        }
        let keys = request
            .objects
            .iter()
            .map(key)
            .collect::<Result<Vec<_>, _>>()?;
        let mut values = std::iter::repeat_with(|| None)
            .take(request.objects.len())
            .collect::<Vec<_>>();
        let mut physical_indices = Vec::new();
        let mut requests = Vec::new();
        let mut locator_indices = Vec::new();
        let mut locator_ids = Vec::new();
        for (index, (address, key)) in request.objects.iter().zip(&keys).enumerate() {
            if let NativeMetadataRef::ChangeLocator(id) = address {
                let id = canonical_id(id)?;
                locator_indices.push(index);
                locator_ids.push(ChangeId::new(*id.as_uuid()));
            } else {
                physical_indices.push(index);
                requests.push(StorageGetManyRequest {
                    space: space(address),
                    keys: std::slice::from_ref(key),
                    opts: StorageGetOptions::default(),
                });
            }
        }
        if !requests.is_empty() {
            let (loaded, _, _) = crate::storage_adapter::collect_bounded_point_pages(
                &read,
                &requests,
                crate::storage_adapter::ReadBudget {
                    max_result_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
                    max_single_value_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
                },
                MAX_NATIVE_METADATA_PAYLOAD_BYTES,
                MAX_NATIVE_METADATA_BATCH,
            )
            .await?;
            let loaded = loaded.values;
            if loaded.len() != physical_indices.len() {
                return Err(invalid("native metadata storage cardinality mismatch"));
            }
            for (index, value) in physical_indices.into_iter().zip(loaded) {
                values[index] = value;
            }
        }
        if !locator_ids.is_empty() {
            let locators = crate::tracked_state::load_canonical_change_locators(
                &read,
                &locator_ids,
            )
            .await?;
            if locators.len() != locator_indices.len() {
                return Err(invalid("native metadata locator cardinality mismatch"));
            }
            for (index, locator) in locator_indices.into_iter().zip(locators) {
                values[index] = locator.map(|locator| {
                    StorageProjectedValue::FullValue(Bytes::from(
                        crate::tracked_state::encode_change_locator(locator),
                    ))
                });
            }
        }
        if values.len() != request.objects.len() {
            return Err(invalid("native metadata storage cardinality mismatch"));
        }
        let mut checkpoint_pointer_indices = Vec::new();
        for (index, (address, value)) in request.objects.iter().zip(&values).enumerate() {
            if !matches!(
                address,
                NativeMetadataRef::CommitGraphRecord(_)
                    | NativeMetadataRef::CheckpointConversation(_)
            ) {
                continue;
            }
            let Some(StorageProjectedValue::FullValue(bytes)) = value else {
                continue;
            };
            let record: crate::changelog::CommitRecord =
                crate::storage_codec::decode("commit record", bytes)?;
            if record.is_checkpoint {
                canonical_id(address.id())?;
                checkpoint_pointer_indices.push(index);
            }
        }
        let checkpoint_ids = checkpoint_pointer_indices
            .iter()
            .map(|index| canonical_id(request.objects[*index].id()))
            .collect::<Result<Vec<_>, _>>()?;
        let conversation_ids = if checkpoint_ids.is_empty() {
            Vec::new()
        } else {
            crate::checkpoint_conversation::load_checkpoint_conversations(&read, &checkpoint_ids)
                .await?
        };
        if conversation_ids.len() != checkpoint_pointer_indices.len() {
            return Err(invalid("checkpoint conversation metadata cardinality mismatch"));
        }
        let mut checkpoint_conversations = BTreeMap::new();
        for (index, conversation_id) in checkpoint_pointer_indices.into_iter().zip(conversation_ids) {
            checkpoint_conversations.insert(index, conversation_id);
        }
        let mut objects = Vec::with_capacity(values.len());
        let mut total = 0usize;
        for (index, (address, value)) in request.objects.iter().zip(values).enumerate() {
            let bytes = match value {
                Some(StorageProjectedValue::FullValue(bytes)) => bytes,
                Some(StorageProjectedValue::KeyOnly) => {
                    return Err(invalid("native metadata read omitted payload"));
                }
                None => {
                    return Err(address.clone().annotate_missing(LixError::new(
                        "LIX_NATIVE_METADATA_UNAVAILABLE",
                        "requested native metadata is unavailable",
                    )));
                }
            };
            total = total
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("native metadata payload exceeds bound"))?;
            if total > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
                return Err(invalid("native metadata payload exceeds bound"));
            }
            validate_bytes(address, &bytes)?;
            let checkpoint_conversation = checkpoint_conversations.get(&index).map(|id| {
                CheckpointConversationEnvelope {
                    commit_id: address.id().to_owned(),
                    conversation_id: RequiredNullable(id.clone()),
                }
            });
            objects.push(NativeMetadata {
                address: address.clone(),
                bytes: bytes.to_vec(),
                checkpoint_conversation,
            });
        }
        let mut response = NativeMetadataResponse {
            lix_id: self.lix_id().to_owned(),
            epoch_id: request.epoch_id.clone(),
            objects,
            dependencies: Default::default(),
        };
        response.dependencies =
            super::native_dependencies::select(&read, &response.objects).await?;
        validate_native_metadata_response(self.lix_id(), request, &response)?;
        Ok(response)
    }
}

/// Stage only against this exact durable partial-replica epoch. Existing or
/// already staged differing bytes are conflicts, including mutable graph KV.
/// Return all CAS guards for the caller's atomic commit; do not publish refs.
#[must_use = "native metadata writes must commit with every returned precondition"]
pub(crate) async fn stage_native_metadata(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &NativeMetadataRequest,
    response: &NativeMetadataResponse,
) -> Result<Vec<StoragePrecondition>, LixError> {
    validate_native_metadata_request(request)?;
    stage_validated_native_metadata(
        read,
        writes,
        state,
        request,
        response,
        MAX_NATIVE_METADATA_BATCH,
    )
    .await
}

/// Stage the missing suffix of an already authenticated bounded-walk response.
/// The full response is validated first; only selected primary records and
/// dependencies whose owners are among those selected records are installed.
pub(crate) async fn stage_selected_native_metadata(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    full_request: &NativeMetadataRequest,
    full_response: &NativeMetadataResponse,
    selected_addresses: &[NativeMetadataRef],
) -> Result<Vec<StoragePrecondition>, LixError> {
    validate_native_metadata_addresses(
        &full_request.epoch_id,
        &full_request.objects,
        MAX_NATIVE_METADATA_WALK_RECORDS,
    )?;
    validate_native_metadata_response_for_addresses(
        state.repository_id(),
        &full_request.epoch_id,
        &full_request.objects,
        MAX_NATIVE_METADATA_WALK_RECORDS,
        full_response,
    )?;
    if full_request.epoch_id != state.epoch_id() {
        return Err(invalid(
            "native metadata request belongs to another local epoch",
        ));
    }
    if selected_addresses.is_empty() || selected_addresses.len() > full_request.objects.len() {
        return Err(invalid("native metadata selection exceeds its request"));
    }
    let requested = full_request
        .objects
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();
    for address in selected_addresses {
        key(address)?;
        if !requested.contains(address) || !selected.insert(address.clone()) {
            return Err(invalid("native metadata selection is not an exact subset"));
        }
    }
    let selected_objects = full_response
        .objects
        .iter()
        .filter(|object| selected.contains(&object.address))
        .cloned()
        .collect::<Vec<_>>();
    if selected_objects.len() != selected_addresses.len() {
        return Err(invalid("native metadata selection cardinality mismatch"));
    }
    let dependencies = if full_response.dependencies.metadata.is_empty()
        && full_response.dependencies.objects.is_empty()
    {
        // Native metadata walks return graph records and optional headers, not
        // change locators. Their validated response therefore has no derived
        // dependencies, so avoid decoding every selected header a second time.
        super::native_dependencies::NativeDependencyBundle::default()
    } else {
        let selected_graph_owners = selected_objects
            .iter()
            .filter_map(|object| match &object.address {
                NativeMetadataRef::CommitGraphRecord(id) => Some(id.as_str()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        // Dependency selection is optional cache warming. Keep only dependencies
        // validated by the full response that belong to a selected graph record.
        let dependency_metadata = full_response
            .dependencies
            .metadata
            .iter()
            .filter(|item| match &item.address {
                NativeMetadataRef::CommitStateHeader(id) => {
                    selected_graph_owners.contains(id.as_str())
                }
                _ => false,
            })
            .cloned()
            .collect::<Vec<_>>();
        let available_headers = selected_objects
            .iter()
            .chain(&dependency_metadata)
            .filter_map(|item| match &item.address {
                NativeMetadataRef::CommitStateHeader(id) => {
                    Some((id.as_str(), item.bytes.as_slice()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut selected_catalogs = BTreeSet::new();
        for (id, bytes) in available_headers {
            let owner = canonical_id(id)?;
            selected_catalogs.insert(crate::tracked_state::commit_state_catalog_address(
                owner, bytes,
            )?);
        }
        let dependency_objects = full_response
            .dependencies
            .objects
            .iter()
            .filter(|item| selected_catalogs.contains(&item.address))
            .cloned()
            .collect::<Vec<_>>();
        super::native_dependencies::NativeDependencyBundle {
            metadata: dependency_metadata,
            objects: dependency_objects,
        }
    };
    let selected_request = NativeMetadataRequest {
        epoch_id: full_request.epoch_id.clone(),
        objects: selected_objects
            .iter()
            .map(|object| object.address.clone())
            .collect(),
    };
    let selected_response = NativeMetadataResponse {
        lix_id: full_response.lix_id.clone(),
        epoch_id: full_response.epoch_id.clone(),
        objects: selected_objects,
        dependencies,
    };
    stage_validated_native_metadata(
        read,
        writes,
        state,
        &selected_request,
        &selected_response,
        MAX_NATIVE_METADATA_WALK_RECORDS,
    )
    .await
}

async fn stage_validated_native_metadata(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &NativeMetadataRequest,
    response: &NativeMetadataResponse,
    max_records: usize,
) -> Result<Vec<StoragePrecondition>, LixError> {
    validate_native_metadata_response_for_addresses(
        state.repository_id(),
        &request.epoch_id,
        &request.objects,
        max_records,
        response,
    )?;
    if request.epoch_id != state.epoch_id() {
        return Err(invalid(
            "native metadata request belongs to another local epoch",
        ));
    }
    if response.dependencies.metadata.is_empty() && response.dependencies.objects.is_empty() {
        return stage_exact_metadata(read, writes, state, request, response).await;
    }
    // Validate staged immutable conflicts before adding any metadata writes.
    for object in &response.dependencies.objects {
        if writes
            .staged_value(object.address.space(), &object.address.storage_key())
            .is_some_and(|bytes| bytes.as_ref() != object.bytes.as_slice())
        {
            return Err(invalid(
                "native dependency conflicts with staged immutable bytes",
            ));
        }
    }
    let exact_request = NativeMetadataRequest {
        epoch_id: request.epoch_id.clone(),
        objects: response
            .objects
            .iter()
            .chain(&response.dependencies.metadata)
            .map(|v| v.address.clone())
            .collect(),
    };
    let exact_response = NativeMetadataResponse {
        lix_id: response.lix_id.clone(),
        epoch_id: response.epoch_id.clone(),
        objects: response
            .objects
            .iter()
            .chain(&response.dependencies.metadata)
            .cloned()
            .collect(),
        dependencies: Default::default(),
    };
    let guards = stage_exact_metadata(read, writes, state, &exact_request, &exact_response).await?;
    if !response.dependencies.objects.is_empty() {
        super::native_object::stage_native_objects(
            state.repository_id(),
            &response
                .dependencies
                .objects
                .iter()
                .map(|v| v.address)
                .collect::<Vec<_>>(),
            &super::native_object::NativeObjectResponse {
                lix_id: response.lix_id.clone(),
                objects: response.dependencies.objects.clone(),
            },
            writes,
        )?;
    }
    Ok(guards)
}

async fn stage_exact_metadata(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &NativeMetadataRequest,
    response: &NativeMetadataResponse,
) -> Result<Vec<StoragePrecondition>, LixError> {
    if request.epoch_id != state.epoch_id() {
        return Err(invalid(
            "native metadata request belongs to another local epoch",
        ));
    }
    let Some((stored, receipt)) = load_partial_replica_state(read).await? else {
        return Err(invalid(
            "native metadata requires an installed partial epoch",
        ));
    };
    if &stored != state {
        return Err(invalid("native metadata partial admission changed"));
    }
    let keys = request
        .objects
        .iter()
        .map(key)
        .collect::<Result<Vec<_>, _>>()?;
    let requests = request
        .objects
        .iter()
        .zip(&keys)
        .map(|(address, key)| StorageGetManyRequest {
            space: space(address),
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let (values, _, _) = crate::storage_adapter::collect_bounded_point_pages(
        read,
        &requests,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
            max_single_value_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        },
        MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        32,
    )
    .await?;
    let values = values.values;
    if values.len() != keys.len() {
        return Err(invalid("native metadata storage cardinality mismatch"));
    }
    let mut guards = vec![StoragePrecondition::KeyValueEquals {
        space: super::PARTIAL_REPLICA_STATE_SPACE,
        key: partial_replica_state_key(),
        expected: receipt,
    }];
    for ((object, key), value) in response.objects.iter().zip(&keys).zip(values) {
        let object_space = space(&object.address);
        if writes
            .staged_value(object_space, &key.0)
            .is_some_and(|bytes| bytes.as_ref() != object.bytes.as_slice())
        {
            return Err(invalid("native metadata conflicts with staged bytes"));
        }
        match value {
            None => guards.push(StoragePrecondition::KeyAbsent {
                space: object_space,
                key: key.clone(),
            }),
            Some(StorageProjectedValue::FullValue(bytes))
                if bytes.as_ref() == object.bytes.as_slice() =>
            {
                guards.push(StoragePrecondition::KeyValueEquals {
                    space: object_space,
                    key: key.clone(),
                    expected: bytes,
                })
            }
            Some(_) => return Err(invalid("native metadata conflicts with existing bytes")),
        }
    }
    stage_checkpoint_conversation_proofs(read, writes, state, response, &mut guards).await?;
    for (object, key) in response.objects.iter().zip(keys) {
        if writes
            .staged_value(space(&object.address), &key.0)
            .is_some()
        {
            continue;
        }
        writes.put(
            space(&object.address),
            key,
            StorageValue {
                bytes: Bytes::copy_from_slice(&object.bytes),
            },
        );
    }
    Ok(guards)
}

async fn stage_checkpoint_conversation_proofs(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    response: &NativeMetadataResponse,
    guards: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    let mut facts = BTreeMap::<CommitId, Option<String>>::new();
    for object in &response.objects {
        let Some(envelope) = &object.checkpoint_conversation else {
            continue;
        };
        let id = canonical_id(&envelope.commit_id)?;
        let value = envelope.conversation_id.0.clone();
        if facts.insert(id, value.clone()).is_some_and(|prior| prior != value) {
            return Err(invalid("conflicting checkpoint conversation envelopes"));
        }
    }
    if facts.is_empty() {
        return Ok(());
    }

    let ids = facts.keys().copied().collect::<Vec<_>>();
    let pointer_keys = ids
        .iter()
        .map(|id| StorageKey(Bytes::copy_from_slice(id.as_uuid().as_bytes())))
        .collect::<Vec<_>>();
    let coverage_keys = ids
        .iter()
        .map(|id| crate::checkpoint_conversation::partial_null_coverage_key(*id))
        .collect::<Vec<_>>();
    let pointer_requests = pointer_keys
        .iter()
        .map(|key| StorageGetManyRequest {
            space: crate::checkpoint_conversation::CHECKPOINT_CONVERSATION_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let coverage_requests = coverage_keys
        .iter()
        .map(|key| StorageGetManyRequest {
            space: crate::checkpoint_conversation::PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let (pointer_values, _, _) = crate::storage_adapter::collect_bounded_point_pages(
        read,
        &pointer_requests,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
            max_single_value_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        },
        MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        32,
    )
    .await?;
    let (coverage_values, _, _) = crate::storage_adapter::collect_bounded_point_pages(
        read,
        &coverage_requests,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
            max_single_value_bytes: MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        },
        MAX_NATIVE_METADATA_PAYLOAD_BYTES,
        32,
    )
    .await?;
    let pointer_values = pointer_values.values;
    let coverage_values = coverage_values.values;
    if pointer_values.len() != ids.len() || coverage_values.len() != ids.len() {
        return Err(invalid(
            "checkpoint conversation proof cardinality mismatch",
        ));
    }
    let coverage_bytes =
        crate::checkpoint_conversation::partial_null_coverage_bytes(state.epoch_id())?;
    for (index, (((_id, expected), pointer), coverage)) in facts
        .iter()
        .zip(pointer_values)
        .zip(coverage_values)
        .enumerate()
    {
        let pointer_key = pointer_keys[index].clone();
        let coverage_key = coverage_keys[index].clone();
        let pointer_space = crate::checkpoint_conversation::CHECKPOINT_CONVERSATION_SPACE;
        let coverage_space =
            crate::checkpoint_conversation::PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE;
        let staged_pointer = writes.staged_value(pointer_space, &pointer_key.0);
        let staged_coverage = writes.staged_value(coverage_space, &coverage_key.0);

        let pointer_bytes = match pointer {
            Some(StorageProjectedValue::FullValue(bytes)) => {
                if bytes.len() != 16 {
                    return Err(invalid("checkpoint conversation pointer has invalid length"));
                }
                uuid::Uuid::from_slice(&bytes).map_err(|_| {
                    invalid("checkpoint conversation pointer is not a UUID")
                })?;
                Some(bytes)
            }
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(invalid("checkpoint conversation pointer omitted its value"));
            }
            None => None,
        };
        let coverage_bytes_existing = match coverage {
            Some(StorageProjectedValue::FullValue(bytes)) => {
                crate::checkpoint_conversation::validate_partial_null_coverage(&bytes, state.epoch_id())?;
                Some(bytes)
            }
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(invalid("checkpoint conversation coverage omitted its value"));
            }
            None => None,
        };

        let existing_coverage_current = coverage_bytes_existing.as_ref().is_some_and(|bytes| bytes == &coverage_bytes);
        if let Some(staged) = staged_pointer.as_ref() {
            let staged_matches = expected.as_ref().is_some_and(|expected_id| {
                uuid::Uuid::parse_str(expected_id)
                    .is_ok_and(|expected| staged.as_ref() == expected.as_bytes())
            });
            if !staged_matches {
                return Err(invalid("staged checkpoint conversation pointer conflicts with authority"));
            }
        }
        if let Some(staged) = staged_coverage.as_ref() {
            let staged_is_current = crate::checkpoint_conversation::validate_partial_null_coverage(
                staged,
                state.epoch_id(),
            )?;
            if expected.is_some() && staged_is_current {
                return Err(invalid("staged checkpoint NULL proof conflicts with authority pointer"));
            }
            if expected.is_none() && staged_is_current && pointer_bytes.is_some() {
                return Err(invalid("staged checkpoint NULL proof conflicts with resident pointer"));
            }
        }
        if let Some(expected_id) = expected {
            let expected_uuid = uuid::Uuid::parse_str(expected_id)
                .map_err(|_| invalid("invalid checkpoint conversation UUID"))?;
            if pointer_bytes.as_ref().is_some_and(|actual| actual.as_ref() != expected_uuid.as_bytes())
                || existing_coverage_current
            {
                return Err(invalid("checkpoint conversation pointer conflicts with authority"));
            }
            match pointer_bytes {
                Some(bytes) => guards.push(StoragePrecondition::KeyValueEquals {
                    space: pointer_space,
                    key: pointer_key.clone(),
                    expected: bytes,
                }),
                None => {
                    guards.push(StoragePrecondition::KeyAbsent {
                    space: pointer_space,
                    key: pointer_key.clone(),
                    });
                    writes.put(pointer_space, pointer_key, StorageValue { bytes: Bytes::copy_from_slice(expected_uuid.as_bytes()) });
                }
            }
            match coverage_bytes_existing {
                Some(actual) if !existing_coverage_current => {
                    guards.push(StoragePrecondition::KeyValueEquals {
                        space: coverage_space,
                        key: coverage_key.clone(),
                        expected: actual,
                    });
                    writes.delete(coverage_space, coverage_key);
                }
                Some(_) => {}
                None => guards.push(StoragePrecondition::KeyAbsent {
                    space: coverage_space,
                    key: coverage_key,
                }),
            }
        } else {
            if pointer_bytes.is_some() {
                return Err(invalid("authority NULL conflicts with resident checkpoint pointer"));
            }
            guards.push(StoragePrecondition::KeyAbsent {
                space: pointer_space,
                key: pointer_key,
            });
            match coverage_bytes_existing {
                Some(actual) if existing_coverage_current => {
                    guards.push(StoragePrecondition::KeyValueEquals {
                        space: coverage_space,
                        key: coverage_key,
                        expected: actual,
                    });
                }
                Some(actual) => {
                    guards.push(StoragePrecondition::KeyValueEquals {
                        space: coverage_space,
                        key: coverage_key.clone(),
                        expected: actual,
                    });
                    writes.put(
                        coverage_space,
                        coverage_key,
                        StorageValue {
                            bytes: coverage_bytes.clone(),
                        },
                    );
                }
                None => {
                    guards.push(StoragePrecondition::KeyAbsent {
                        space: coverage_space,
                        key: coverage_key.clone(),
                    });
                    writes.put(
                        coverage_space,
                        coverage_key,
                        StorageValue {
                            bytes: coverage_bytes.clone(),
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::partial_state::stage_partial_replica_state;
    use super::*;
    use crate::storage::StorageWrite;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    use crate::{Memory, open_lix};

    #[test]
    fn checkpoint_conversation_envelope_requires_explicit_nullable_value() {
        let explicit_null = serde_json::json!({
            "commitId": "00000000-0000-7000-8000-000000000001",
            "conversationId": null,
        });
        let envelope: CheckpointConversationEnvelope =
            serde_json::from_value(explicit_null.clone()).expect("explicit null is present");
        assert_eq!(envelope.conversation_id.0, None);
        let mut omitted = explicit_null;
        omitted
            .as_object_mut()
            .unwrap()
            .remove("conversationId");
        assert!(serde_json::from_value::<CheckpointConversationEnvelope>(omitted).is_err());
    }

    #[tokio::test]
    async fn exact_checkpoint_conversation_reference_rejects_noncheckpoint_graph() {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_key_value(key, value) VALUES ('ordinary-commit', 'seed')",
            &[],
        )
        .await
        .unwrap();
        let rows = lix
            .execute(
                "SELECT commit_id FROM lix_log() WHERE NOT is_checkpoint ORDER BY created_at DESC LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        let id = rows.rows()[0].get::<String>("commit_id").unwrap();
        let request = NativeMetadataRequest {
            epoch_id: "00000000-0000-7000-8000-000000000293".into(),
            objects: vec![NativeMetadataRef::CommitGraphRecord(id.clone())],
        };
        let response = lix.read_sync_native_metadata(&request).await.unwrap();
        assert!(validate_checkpoint_conversation_envelope(
            &NativeMetadataRef::CheckpointConversation(id),
            &response.objects[0].bytes,
            None,
        )
        .is_err());
    }

    async fn commit_raw_fixture(
        storage: &StorageAdapter<Memory>,
        writes: StorageWriteSet,
        options: StorageWriteOptions,
    ) {
        let mut raw = storage.begin_migration_write(options).await.unwrap();
        writes.lower_into(&mut raw).await.unwrap();
        raw.commit().await.unwrap();
    }

    #[tokio::test]
    async fn direct_change_locator_is_resolved_without_a_physical_locator_row() {
        let authority = open_lix().await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('locator-native', 'value')",
                &[],
            )
            .await
            .unwrap();
        let rows = authority
            .execute(
                "SELECT lixcol_change_id AS id FROM lix_key_value WHERE key = 'locator-native'",
                &[],
            )
            .await
            .unwrap();
        let change = rows.rows()[0].get::<String>("id").unwrap();
        let address = NativeMetadataRef::ChangeLocator(change.clone());
        let adapter = authority.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let keys = [key(&address).unwrap()];
        let values = read
            .get_many(&[StorageGetManyRequest {
                space: space(&address),
                keys: &keys,
                opts: Default::default(),
            }])
            .await
            .unwrap()
            .values;
        assert!(
            values[0].is_none(),
            "direct native changes need no persisted locator"
        );
        drop(read);
        let response = authority
            .read_sync_native_metadata(&NativeMetadataRequest {
                epoch_id: "00000000-0000-7000-8000-000000000291".into(),
                objects: vec![address.clone()],
            })
            .await
            .unwrap();
        let locator = crate::tracked_state::decode_change_locator(
            ChangeId::parse_lix(&change, "test change").unwrap(),
            &response.objects[0].bytes,
        )
        .unwrap();
        assert_eq!(locator.change_id.to_string(), change);
        assert_eq!(response.objects[0].address, address);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_random_locator_round_trip_is_epoch_bound_and_conflict_checked() {
        let (state, mut request, mut response) = fixture().await;
        let change =
            ChangeId::new(uuid::Uuid::parse_str("91e23c10-c68a-41b6-a0d1-e9b359727441").unwrap());
        let locator = crate::tracked_state::CommitDeltaChangeLocator {
            change_id: change,
            commit_id: canonical_id(&state.descriptor().selected_branch.head.commit_id).unwrap(),
            segment_index: 7,
            ordinal: 3,
        };
        let address = NativeMetadataRef::ChangeLocator(change.to_string());
        request.objects = vec![address.clone()];
        response.objects = vec![NativeMetadata {
            address: address.clone(),
            bytes: crate::tracked_state::encode_change_locator(locator),
            checkpoint_conversation: None,
        }];
        validate_bytes(&address, &response.objects[0].bytes).unwrap();
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        let guard = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        commit_raw_fixture(
            &adapter,
            writes,
            StorageWriteOptions {
                preconditions: vec![guard],
                ..Default::default()
            },
        )
        .await;
        adapter.admit_partial_replica_writer(super::super::partial_replica_write_capability());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        let guards = stage_native_metadata(&read, &mut writes, &state, &request, &response)
            .await
            .unwrap();
        drop(read);
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(
            native_metadata_is_resident(&read, &state, &address)
                .await
                .unwrap()
        );
        let mut changed = locator;
        changed.ordinal += 1;
        response.objects[0].bytes = crate::tracked_state::encode_change_locator(changed);
        assert!(
            stage_native_metadata(
                &read,
                &mut adapter.new_write_set(),
                &state,
                &request,
                &response
            )
            .await
            .is_err()
        );
        request.epoch_id = "00000000-0000-7000-8000-000000000292".into();
        assert!(
            stage_native_metadata(
                &read,
                &mut adapter.new_write_set(),
                &state,
                &request,
                &response
            )
            .await
            .is_err()
        );
    }

    async fn fixture() -> (
        PartialReplicaState,
        NativeMetadataRequest,
        NativeMetadataResponse,
    ) {
        let authority = open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let commit = descriptor.selected_branch.head.commit_id.clone();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000299".into(),
            descriptor,
        )
        .unwrap();
        let request = NativeMetadataRequest {
            epoch_id: state.epoch_id().to_owned(),
            objects: vec![
                NativeMetadataRef::CommitStateHeader(commit.clone()),
                NativeMetadataRef::CommitGraphRecord(commit),
            ],
        };
        let response = authority.read_sync_native_metadata(&request).await.unwrap();
        (state, request, response)
    }

    #[tokio::test]
    async fn native_metadata_stages_verified_bytes_with_epoch_and_existing_value_guards() {
        let (state, request, response) = fixture().await;
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        let guard = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        commit_raw_fixture(
            &adapter,
            writes,
            StorageWriteOptions {
                preconditions: vec![guard],
                ..Default::default()
            },
        )
        .await;
        // Test-only admission of the fixture's replica write lane. Production
        // admission remains the dedicated opener's responsibility.
        adapter.admit_partial_replica_writer(super::super::partial_replica_write_capability());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut first = adapter.new_write_set();
        let first_guards = stage_native_metadata(&read, &mut first, &state, &request, &response)
            .await
            .unwrap();
        let mut racing = adapter.new_write_set();
        let racing_guards = stage_native_metadata(&read, &mut racing, &state, &request, &response)
            .await
            .unwrap();
        assert_eq!(first_guards.len(), 3);
        drop(read);
        adapter
            .commit_write_set(
                first,
                StorageWriteOptions {
                    preconditions: first_guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            adapter
                .commit_write_set(
                    racing,
                    StorageWriteOptions {
                        preconditions: racing_guards,
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut same = adapter.new_write_set();
        let guards = stage_native_metadata(&read, &mut same, &state, &request, &response)
            .await
            .unwrap();
        assert!(
            guards
                .iter()
                .all(|guard| matches!(guard, StoragePrecondition::KeyValueEquals { .. }))
        );
        drop(read);
        adapter
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                same,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .expect("reinstalling identical native metadata must be idempotent");

        // The append-only fence must still reject a write that carries a
        // value-equality guard for the old bytes but stages different bytes.
        let graph = response
            .objects
            .iter()
            .find(|object| matches!(&object.address, NativeMetadataRef::CommitGraphRecord(_)))
            .unwrap();
        let mut changed_bytes = graph.bytes.clone();
        changed_bytes[0] ^= 1;
        let mut overwrite = adapter.new_write_set();
        overwrite.put(
            space(&graph.address),
            key(&graph.address).unwrap(),
            StorageValue {
                bytes: Bytes::from(changed_bytes),
            },
        );
        let overwrite_error = adapter
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                overwrite,
                StorageWriteOptions {
                    preconditions: vec![StoragePrecondition::KeyValueEquals {
                        space: space(&graph.address),
                        key: key(&graph.address).unwrap(),
                        expected: Bytes::copy_from_slice(&graph.bytes),
                    }],
                    ..Default::default()
                },
            )
            .await
            .expect_err("partial commit graph writes must remain append-only");
        assert!(matches!(
            overwrite_error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(ref failures)
            ) if failures.iter().any(|failure| failure.index == 2)
        ));

        let read = adapter.begin_read(Default::default()).await.unwrap();
        let graph_key = [key(&graph.address).unwrap()];
        let stored = read
            .get_many(&[StorageGetManyRequest {
                space: space(&graph.address),
                keys: &graph_key,
                opts: StorageGetOptions::default(),
            }])
            .await
            .unwrap();
        assert_eq!(
            stored.values,
            [Some(StorageProjectedValue::FullValue(Bytes::copy_from_slice(
                &graph.bytes
            )))]
        );
        let mut changed = response.clone();
        let graph = changed
            .objects
            .iter_mut()
            .find(|object| matches!(&object.address, NativeMetadataRef::CommitGraphRecord(_)))
            .unwrap();
        let mut record: crate::changelog::CommitRecord =
            crate::storage_codec::decode("commit record", &graph.bytes).unwrap();
        record.account_id = crate::SYSTEM_ACCOUNT_ID.to_owned();
        if graph.bytes == crate::storage_codec::encode("commit record", &record).unwrap() {
            record.account_id = crate::ANONYMOUS_ACCOUNT_ID.to_owned();
        }
        graph.bytes = crate::storage_codec::encode("commit record", &record).unwrap();
        validate_native_metadata_response(state.repository_id(), &request, &changed).unwrap();
        let mut conflict = adapter.new_write_set();
        assert!(
            stage_native_metadata(&read, &mut conflict, &state, &request, &changed)
                .await
                .is_err()
        );
        for object in &changed.objects {
            assert!(
                conflict
                    .staged_value(space(&object.address), &key(&object.address).unwrap().0)
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn native_metadata_residency_fails_closed_for_wrong_epoch_and_corruption() {
        let (state, request, _) = fixture().await;
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let guard = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        commit_raw_fixture(
            &storage,
            writes,
            StorageWriteOptions {
                preconditions: vec![guard],
                ..Default::default()
            },
        )
        .await;
        let address = &request.objects[0];
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(
            !native_metadata_is_resident(&read, &state, address)
                .await
                .unwrap()
        );
        let wrong_epoch = PartialReplicaState::new(
            state.remote_id().into(),
            state.active_account_id().into(),
            "00000000-0000-7000-8000-000000000999".into(),
            state.descriptor().clone(),
        )
        .unwrap();
        assert!(
            native_metadata_is_resident(&read, &wrong_epoch, address)
                .await
                .is_err()
        );
        drop(read);
        // Deliberate fixture corruption bypasses the native admission helper.
        let mut writes = storage.new_write_set();
        writes.put(
            space(address),
            key(address).unwrap(),
            StorageValue {
                bytes: Bytes::from_static(b"corrupt"),
            },
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions::default(),
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(
            native_metadata_is_resident(&read, &state, address)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn native_metadata_rejects_invalid_identity_epoch_and_native_bytes() {
        let (state, request, response) = fixture().await;
        for mutation in 0..4 {
            let mut wrong = response.clone();
            match mutation {
                0 => wrong.epoch_id = "00000000-0000-7000-8000-000000000399".into(),
                1 => wrong.lix_id = "different".into(),
                2 => wrong.objects.swap(0, 1),
                _ => wrong.objects[0].bytes.push(0),
            }
            assert!(
                validate_native_metadata_response(state.repository_id(), &request, &wrong).is_err()
            );
        }
        let mut wrong = request.clone();
        wrong.objects = vec![NativeMetadataRef::CommitGraphRecord("bad-id".into())];
        assert!(validate_native_metadata_request(&wrong).is_err());
        let mut wrong = request.clone();
        wrong.objects.push(wrong.objects[0].clone());
        assert!(validate_native_metadata_request(&wrong).is_err());
    }
}
