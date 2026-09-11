//! Explicit authority metadata transfer for a partial replica.
//!
//! UUID-addressed metadata is not content-addressed object data. Receipt epoch
//! binding, native decoding, and byte-for-byte existing-value guards precede
//! publication. Fetching these records never walks parents or inventories.

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
use std::collections::BTreeSet;

pub(crate) const MAX_NATIVE_METADATA_BATCH: usize = 32;
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
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeMetadata {
    pub(crate) address: NativeMetadataRef,
    #[serde(with = "base64_bytes")]
    pub(crate) bytes: Vec<u8>,
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
fn space(address: &NativeMetadataRef) -> StorageSpace {
    match address {
        NativeMetadataRef::CommitStateHeader(_) => {
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
        }
        NativeMetadataRef::CommitGraphRecord(_) => crate::changelog::COMMIT_SPACE,
        NativeMetadataRef::ChangeLocator(_) => {
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE
        }
    }
}
fn key(address: &NativeMetadataRef) -> Result<StorageKey, LixError> {
    let id = canonical_id(address.id())?;
    Ok(match address {
        NativeMetadataRef::CommitStateHeader(_) => {
            crate::tracked_state::commit_state_authority_key(id)
        }
        NativeMetadataRef::CommitGraphRecord(_) => {
            StorageKey(Bytes::from(crate::changelog::commit_key(id)))
        }
        NativeMetadataRef::ChangeLocator(_) => {
            StorageKey(Bytes::copy_from_slice(id.as_uuid().as_bytes()))
        }
    })
}
fn validate_bytes(address: &NativeMetadataRef, bytes: &[u8]) -> Result<(), LixError> {
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
        NativeMetadataRef::ChangeLocator(_) => {
            crate::tracked_state::decode_change_locator(ChangeId::new(*id.as_uuid()), bytes)?;
            Ok(())
        }
    }
}
/// Check one native record only after matching its exact local admission.
/// An invalid resident record is corruption, never an invitation to refetch.
pub(super) async fn native_metadata_is_resident(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    address: &NativeMetadataRef,
) -> Result<bool, LixError> {
    address.validate_address()?;
    let Some((actual, _)) = load_partial_replica_state(read).await? else {
        return Err(invalid(
            "native metadata requires an installed partial epoch",
        ));
    };
    if &actual != state {
        return Err(invalid("native metadata partial admission changed"));
    }
    let key = key(address)?;
    let values = read
        .get_many(&[StorageGetManyRequest {
            space: space(address),
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }])
        .await?
        .values;
    if values.len() != 1 {
        return Err(invalid("native metadata storage cardinality mismatch"));
    }
    match values.into_iter().next().flatten() {
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
    }
}

pub(crate) fn validate_native_metadata_request(
    request: &NativeMetadataRequest,
) -> Result<(), LixError> {
    canonical_id(&request.epoch_id)?;
    if request.objects.is_empty() || request.objects.len() > MAX_NATIVE_METADATA_BATCH {
        return Err(invalid(
            "native metadata request requires between 1 and 32 objects",
        ));
    }
    let mut unique = BTreeSet::new();
    for address in &request.objects {
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
    if response.lix_id != repository_id
        || response.epoch_id != request.epoch_id
        || response.objects.len() != request.objects.len()
    {
        return Err(invalid(
            "native metadata response repository, epoch or cardinality mismatch",
        ));
    }
    let mut total = 0usize;
    for (expected, object) in request.objects.iter().zip(&response.objects) {
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
        let values = read.get_many(&requests).await?.values;
        if values.len() != request.objects.len() {
            return Err(invalid("native metadata storage cardinality mismatch"));
        }
        let mut objects = Vec::with_capacity(values.len());
        let mut total = 0usize;
        for (address, value) in request.objects.iter().zip(values) {
            // Direct IDs normally have no physical locator row. Resolve their
            // authenticated native owner rather than treating that absence as
            // unavailable metadata or trusting an address-shaped guess.
            let value = if matches!(address, NativeMetadataRef::ChangeLocator(_)) {
                let id = canonical_id(address.id())?;
                crate::tracked_state::load_canonical_change_locator(
                    &read,
                    ChangeId::new(*id.as_uuid()),
                )
                .await?
                .map(|locator| {
                    StorageProjectedValue::FullValue(Bytes::from(
                        crate::tracked_state::encode_change_locator(locator),
                    ))
                })
            } else {
                value
            };
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
            objects.push(NativeMetadata {
                address: address.clone(),
                bytes: bytes.to_vec(),
            });
        }
        Ok(NativeMetadataResponse {
            lix_id: self.lix_id().to_owned(),
            epoch_id: request.epoch_id.clone(),
            objects,
        })
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
    validate_native_metadata_response(state.repository_id(), request, response)?;
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
    let values = read.get_many(&requests).await?.values;
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

#[cfg(test)]
mod tests {
    use super::super::partial_state::stage_partial_replica_state;
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    use crate::{Memory, open_lix};

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
        }];
        validate_bytes(&address, &response.objects[0].bytes).unwrap();
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        let guard = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
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
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
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
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
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
