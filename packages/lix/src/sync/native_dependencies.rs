//! Bounded immutable dependencies derived from exact native metadata.
//! These records accelerate hydration; they certify no coverage or absence.
use super::native_metadata::{
    MAX_NATIVE_METADATA_BATCH, MAX_NATIVE_METADATA_PAYLOAD_BYTES, NativeMetadata,
    NativeMetadataResponse, key, space, validate_bytes,
};
use super::native_object::NativeObject;
use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::storage_adapter::{
    StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageSpace,
};
use crate::tracked_state::NativeMetadataRef;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeDependencyBundle {
    pub(crate) metadata: Vec<NativeMetadata>,
    pub(crate) objects: Vec<NativeObject>,
}
impl NativeDependencyBundle {
    pub(crate) fn is_empty(&self) -> bool {
        self.metadata.is_empty() && self.objects.is_empty()
    }
}
fn invalid(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}
fn owners(objects: &[NativeMetadata]) -> Result<BTreeSet<CommitId>, LixError> {
    objects
        .iter()
        .filter_map(|v| match &v.address {
            NativeMetadataRef::ChangeLocator(id) => Some((id, &v.bytes)),
            _ => None,
        })
        .map(|(id, bytes)| {
            let id = CommitId::parse_lix(id, "change locator")?;
            Ok(
                crate::tracked_state::decode_change_locator(ChangeId::new(*id.as_uuid()), bytes)?
                    .commit_id,
            )
        })
        .collect()
}

pub(super) fn validate(response: &NativeMetadataResponse) -> Result<(), LixError> {
    let deps = &response.dependencies;
    if response.objects.len() + deps.metadata.len() + deps.objects.len() > MAX_NATIVE_METADATA_BATCH
    {
        return Err(invalid("native dependency record limit exceeded"));
    }
    let total = response
        .objects
        .iter()
        .chain(&deps.metadata)
        .map(|v| v.bytes.len())
        .chain(deps.objects.iter().map(|v| v.bytes.len()))
        .try_fold(0usize, |n, len| n.checked_add(len))
        .ok_or_else(|| invalid("native dependency byte limit exceeded"))?;
    if total > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
        return Err(invalid("native dependency byte limit exceeded"));
    }
    if deps.metadata.is_empty() && deps.objects.is_empty() {
        return Ok(());
    }
    let owners = owners(&response.objects)?;
    let mut seen = response
        .objects
        .iter()
        .map(|v| v.address.clone())
        .collect::<BTreeSet<_>>();
    for item in &deps.metadata {
        let NativeMetadataRef::CommitStateHeader(id) = &item.address else {
            return Err(invalid("unexpected native metadata dependency"));
        };
        if !owners.contains(&CommitId::parse_lix(id, "dependency owner")?)
            || !seen.insert(item.address.clone())
        {
            return Err(invalid("unrelated or duplicate native metadata dependency"));
        }
        validate_bytes(&item.address, &item.bytes)?;
    }
    let mut catalogs = BTreeSet::new();
    for item in response.objects.iter().chain(&deps.metadata) {
        if let NativeMetadataRef::CommitStateHeader(id) = &item.address {
            let owner = CommitId::parse_lix(id, "dependency owner")?;
            if owners.contains(&owner) {
                catalogs.insert(crate::tracked_state::commit_state_catalog_address(
                    owner,
                    &item.bytes,
                )?);
            }
        }
    }
    let mut seen = BTreeSet::new();
    for item in &deps.objects {
        if !catalogs.contains(&item.address) || !seen.insert(item.address) {
            return Err(invalid("unrelated or duplicate native object dependency"));
        }
        item.address.validate(&item.bytes)?;
    }
    Ok(())
}

async fn read_optional(
    read: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    key: StorageKey,
) -> Result<Option<Bytes>, LixError> {
    let values = read
        .get_many(&[StorageGetManyRequest {
            space,
            keys: &[key],
            opts: StorageGetOptions::default(),
        }])
        .await?
        .values;
    if values.len() != 1 {
        return Err(invalid("native dependency storage cardinality mismatch"));
    }
    Ok(match values.into_iter().next().flatten() {
        Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes),
        _ => None,
    })
}

pub(super) async fn select(
    read: &(impl StorageAdapterRead + ?Sized),
    required: &[NativeMetadata],
) -> Result<NativeDependencyBundle, LixError> {
    let mut result = NativeDependencyBundle::default();
    let mut count = required.len();
    let mut bytes = required.iter().map(|v| v.bytes.len()).sum::<usize>();
    for owner in owners(required)? {
        if count >= MAX_NATIVE_METADATA_BATCH {
            break;
        }
        let address = NativeMetadataRef::CommitStateHeader(owner.to_string());
        let resident = required.iter().find(|v| v.address == address);
        let header = if let Some(item) = resident {
            Bytes::copy_from_slice(&item.bytes)
        } else {
            let Some(header) = read_optional(read, space(&address), key(&address)?).await? else {
                continue;
            };
            if validate_bytes(&address, &header).is_err()
                || bytes.saturating_add(header.len()) > MAX_NATIVE_METADATA_PAYLOAD_BYTES
            {
                continue;
            }
            bytes += header.len();
            count += 1;
            result.metadata.push(NativeMetadata {
                address,
                bytes: header.to_vec(),
            });
            header
        };
        if count >= MAX_NATIVE_METADATA_BATCH {
            continue;
        }
        let catalog = crate::tracked_state::commit_state_catalog_address(owner, &header)?;
        let Some(payload) = read_optional(
            read,
            catalog.space(),
            StorageKey(Bytes::from(catalog.storage_key())),
        )
        .await?
        else {
            continue;
        };
        if bytes.saturating_add(payload.len()) > MAX_NATIVE_METADATA_PAYLOAD_BYTES
            || catalog.validate(&payload).is_err()
        {
            continue;
        }
        bytes += payload.len();
        count += 1;
        result.objects.push(NativeObject {
            address: catalog,
            bytes: payload.to_vec(),
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::super::native_metadata::{NativeMetadataRequest, validate_native_metadata_response};
    use super::*;
    use crate::open_lix;

    async fn fixture() -> (
        crate::Lix<crate::Memory>,
        NativeMetadataRequest,
        NativeMetadataResponse,
    ) {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES('bundle','value')",
            &[],
        )
        .await
        .unwrap();
        let rows = lix
            .execute(
                "SELECT lixcol_change_id AS id FROM lix_key_value WHERE key='bundle'",
                &[],
            )
            .await
            .unwrap();
        let request = NativeMetadataRequest {
            epoch_id: "00000000-0000-7000-8000-000000000291".into(),
            objects: vec![NativeMetadataRef::ChangeLocator(
                rows.rows()[0].get::<String>("id").unwrap(),
            )],
        };
        let response = lix.read_sync_native_metadata(&request).await.unwrap();
        assert_eq!(response.dependencies.metadata.len(), 1);
        assert_eq!(response.dependencies.objects.len(), 1);
        (lix, request, response)
    }

    #[tokio::test]
    async fn companions_are_derived_and_reject_wrong_owner_digest_duplicates_and_limits() {
        let (lix, request, response) = fixture().await;
        validate_native_metadata_response(lix.lix_id(), &request, &response).unwrap();
        let mut bad = response.clone();
        bad.dependencies.metadata[0].address =
            NativeMetadataRef::CommitStateHeader("00000000-0000-7000-8000-000000000292".into());
        assert!(validate(&bad).is_err());
        let mut bad = response.clone();
        bad.dependencies.objects[0].bytes[0] ^= 1;
        assert!(validate(&bad).is_err());
        let mut bad = response.clone();
        bad.dependencies
            .metadata
            .push(bad.dependencies.metadata[0].clone());
        assert!(validate(&bad).is_err());
        let mut bad = response.clone();
        bad.dependencies
            .objects
            .push(bad.dependencies.objects[0].clone());
        assert!(validate(&bad).is_err());
        let mut bad = response.clone();
        bad.dependencies.metadata.clear();
        assert!(
            validate(&bad).is_err(),
            "catalog requires the authenticated owner header"
        );
        let mut bad = response.clone();
        bad.dependencies.objects[0]
            .bytes
            .resize(MAX_NATIVE_METADATA_PAYLOAD_BYTES + 1, 0);
        assert!(validate(&bad).is_err());
        let mut bad = response.clone();
        bad.dependencies.metadata =
            vec![bad.dependencies.metadata[0].clone(); MAX_NATIVE_METADATA_BATCH];
        assert!(validate(&bad).is_err());
        let mut exact = response;
        exact.dependencies = Default::default();
        validate_native_metadata_response(lix.lix_id(), &request, &exact).unwrap();
    }

    #[tokio::test]
    async fn selector_deduplicates_and_preserves_exact_fallback_for_missing_or_corrupt_optional_records()
     {
        let (lix, _, response) = fixture().await;
        let adapter = lix.storage_adapter();
        let header = response.dependencies.metadata[0].clone();
        let catalog = response.dependencies.objects[0].address;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut required = response.objects.clone();
        required.push(header.clone());
        let deps = select(&read, &required).await.unwrap();
        assert!(deps.metadata.is_empty());
        assert_eq!(deps.objects.len(), 1);
        drop(read);
        for corrupt in [false, true] {
            let mut writes = adapter.new_write_set();
            writes.delete(
                catalog.space(),
                StorageKey(Bytes::from(catalog.storage_key())),
            );
            adapter
                .commit_write_set(writes, Default::default())
                .await
                .unwrap();
            if corrupt {
                let mut writes = adapter.new_write_set();
                writes.put(
                    catalog.space(),
                    StorageKey(Bytes::from(catalog.storage_key())),
                    crate::storage_adapter::StorageValue {
                        bytes: Bytes::from_static(b"corrupt"),
                    },
                );
                adapter
                    .commit_write_set(writes, Default::default())
                    .await
                    .unwrap();
            }
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let deps = select(&read, &response.objects).await.unwrap();
            assert_eq!(deps.metadata.len(), 1);
            assert!(deps.objects.is_empty());
        }
        let mut writes = adapter.new_write_set();
        writes.delete(space(&header.address), key(&header.address).unwrap());
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let deps = select(&read, &response.objects).await.unwrap();
        assert!(deps.metadata.is_empty());
        assert!(deps.objects.is_empty());
    }
    #[tokio::test]
    async fn bundle_installation_is_atomic_and_epoch_fenced() {
        use super::super::native_metadata::stage_native_metadata;
        use super::super::partial_state::{PartialReplicaState, stage_partial_replica_state};
        use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
        let (lix, request, response) = fixture().await;
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            request.epoch_id.clone(),
            descriptor,
        )
        .unwrap();
        let adapter = StorageAdapter::new(crate::Memory::new());
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
        let mut bad = response.clone();
        bad.dependencies.objects[0].bytes[0] ^= 1;
        assert!(
            stage_native_metadata(&read, &mut writes, &state, &request, &bad)
                .await
                .is_err()
        );
        for item in response
            .objects
            .iter()
            .chain(&response.dependencies.metadata)
        {
            assert!(
                writes
                    .staged_value(space(&item.address), &key(&item.address).unwrap().0)
                    .is_none()
            );
        }
        let mut wrong_request = request.clone();
        wrong_request.epoch_id = "00000000-0000-7000-8000-000000000292".into();
        let mut wrong_response = response.clone();
        wrong_response.epoch_id = wrong_request.epoch_id.clone();
        assert!(
            stage_native_metadata(&read, &mut writes, &state, &wrong_request, &wrong_response)
                .await
                .is_err()
        );
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
        for item in response
            .objects
            .iter()
            .chain(&response.dependencies.metadata)
        {
            assert_eq!(
                read_optional(&read, space(&item.address), key(&item.address).unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                item.bytes
            );
        }
        for item in &response.dependencies.objects {
            assert_eq!(
                read_optional(
                    &read,
                    item.address.space(),
                    StorageKey(Bytes::from(item.address.storage_key()))
                )
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
                item.bytes
            );
        }
    }
}
