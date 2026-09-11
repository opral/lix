//! Bounded native-object transport primitives for on-demand sync.
//! These do not install refs, certify coverage, or activate partial replication.

use crate::storage_adapter::StorageProjectedValue;
use crate::storage_adapter::{
    Storage, StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StorageReadOptions, StorageValue, StorageWriteSet,
};
use crate::tracked_state::NativeObjectRef;
use crate::{Lix, LixError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) const MAX_NATIVE_OBJECT_BATCH: usize = 32;
pub(crate) const MAX_NATIVE_OBJECT_PAYLOAD_BYTES: usize = 1024 * 1024;
// Base64 payload plus per-object padding and a bounded JSON envelope. The
// transport enforces this while receiving, before JSON allocation/decoding.
pub(crate) const MAX_NATIVE_OBJECT_RESPONSE_BYTES: usize =
    MAX_NATIVE_OBJECT_PAYLOAD_BYTES.div_ceil(3) * 4 + MAX_NATIVE_OBJECT_BATCH * 4 + 65536;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeObjectResponse {
    pub(crate) lix_id: String,
    pub(crate) objects: Vec<NativeObject>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NativeObject {
    pub(crate) address: NativeObjectRef,
    #[serde(with = "base64_bytes")]
    pub(crate) bytes: Vec<u8>,
}

pub(super) mod base64_bytes {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(in crate::sync) fn serialize<S: Serializer>(
        bytes: &[u8],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub(in crate::sync) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        if encoded.len() > super::MAX_NATIVE_OBJECT_PAYLOAD_BYTES.div_ceil(3) * 4 {
            return Err(serde::de::Error::custom(
                "native object payload limit exceeded",
            ));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(serde::de::Error::custom)?;
        if bytes.len() > super::MAX_NATIVE_OBJECT_PAYLOAD_BYTES {
            return Err(serde::de::Error::custom(
                "native object payload limit exceeded",
            ));
        }
        Ok(bytes)
    }
}

fn invalid(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

pub(crate) fn validate_request(requested: &[NativeObjectRef]) -> Result<(), LixError> {
    if requested.is_empty() || requested.len() > MAX_NATIVE_OBJECT_BATCH {
        return Err(invalid(
            "native object request must contain between 1 and 32 addresses",
        ));
    }
    if requested
        .iter()
        .map(|address| (address.space().name, address.storage_key()))
        .collect::<BTreeSet<_>>()
        .len()
        != requested.len()
    {
        return Err(invalid("native object request repeats an address"));
    }
    Ok(())
}

impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    pub(crate) async fn read_sync_native_objects(
        &self,
        requested: &[NativeObjectRef],
    ) -> Result<NativeObjectResponse, LixError> {
        self.read_sync_native_objects_with_lease(requested, None)
            .await
    }
    pub(crate) async fn read_sync_native_objects_leased(
        &self,
        requested: &[NativeObjectRef],
        lease_id: &str,
    ) -> Result<NativeObjectResponse, LixError> {
        self.read_sync_native_objects_with_lease(requested, Some(lease_id))
            .await
    }
    async fn read_sync_native_objects_with_lease(
        &self,
        requested: &[NativeObjectRef],
        lease_id: Option<&str>,
    ) -> Result<NativeObjectResponse, LixError> {
        validate_request(requested)?;
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
        let keys = requested
            .iter()
            .map(|address| [StorageKey(Bytes::copy_from_slice(&address.storage_key()))])
            .collect::<Vec<_>>();
        let mut objects = Vec::with_capacity(requested.len());
        let mut total = 0usize;
        // Enforce the aggregate byte cap as each payload is loaded. A batch
        // of range-sized objects must not allocate all 32 before checking it.
        for (address, keys) in requested.iter().copied().zip(&keys) {
            let mut values = read
                .get_many(&[StorageGetManyRequest {
                    space: address.space(),
                    keys,
                    opts: StorageGetOptions::default(),
                }])
                .await?
                .values;
            if values.len() != 1 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "native object storage returned incorrect cardinality",
                ));
            }
            let value = values.pop().flatten();
            let value = value.ok_or_else(|| {
                address.annotate_missing(LixError::new(
                    "LIX_NATIVE_OBJECT_UNAVAILABLE",
                    "requested native object is unavailable",
                ))
            })?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(invalid("native object read omitted payload"));
            };
            total = total
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("native object payload limit exceeded"))?;
            if total > MAX_NATIVE_OBJECT_PAYLOAD_BYTES {
                return Err(LixError::new(
                    "LIX_NATIVE_OBJECT_BATCH_TOO_LARGE",
                    "native object payload limit exceeded; split the request or use object ranges",
                ));
            }
            address.validate(&bytes)?;
            objects.push(NativeObject {
                address,
                bytes: bytes.to_vec(),
            });
        }
        let response = NativeObjectResponse {
            lix_id: self.lix_id().to_owned(),
            objects,
        };
        Ok(response)
    }
}

pub(crate) fn validate_response(
    expected_lix_id: &str,
    requested: &[NativeObjectRef],
    response: &NativeObjectResponse,
) -> Result<(), LixError> {
    validate_request(requested)?;
    if response.lix_id != expected_lix_id || response.lix_id.len() > 128 {
        return Err(invalid(
            "native object response belongs to another repository",
        ));
    }
    if response.objects.len() != requested.len() {
        return Err(invalid("native object response has incorrect cardinality"));
    }
    let mut total = 0usize;
    for (expected, object) in requested.iter().zip(&response.objects) {
        if *expected != object.address {
            return Err(invalid("native object response address mismatch"));
        }
        total = total
            .checked_add(object.bytes.len())
            .ok_or_else(|| invalid("native object payload limit exceeded"))?;
        if total > MAX_NATIVE_OBJECT_PAYLOAD_BYTES {
            return Err(invalid("native object payload limit exceeded"));
        }
        object.address.validate(&object.bytes)?;
    }
    Ok(())
}

/// Validate the entire response before modifying the caller's write set.
/// The caller owns atomic publication and must establish baseline/coverage
/// separately. These bytes alone prove no logical row absence or completeness.
pub(crate) fn stage_native_objects(
    expected_lix_id: &str,
    requested: &[NativeObjectRef],
    response: &NativeObjectResponse,
    writes: &mut StorageWriteSet,
) -> Result<(), LixError> {
    validate_response(expected_lix_id, requested, response)?;
    for object in &response.objects {
        if let Some(existing) =
            writes.staged_value(object.address.space(), &object.address.storage_key())
        {
            if existing.as_ref() != object.bytes.as_slice() {
                return Err(invalid(
                    "native object conflicts with staged immutable bytes",
                ));
            }
        }
    }
    for object in &response.objects {
        writes.put_content_addressed_batch(
            object.address.space(),
            [(
                StorageKey(Bytes::copy_from_slice(&object.address.storage_key())),
                StorageValue {
                    bytes: Bytes::copy_from_slice(&object.bytes),
                },
            )],
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    use crate::{Memory, open_lix};

    fn object(bytes: &[u8]) -> NativeObject {
        NativeObject {
            address: NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(bytes).as_bytes()),
            bytes: bytes.to_vec(),
        }
    }

    #[tokio::test]
    async fn uuid_keyed_catalog_cannot_overwrite_another_immutable_value() {
        let adapter = StorageAdapter::new(Memory::new());
        let make = |bytes: &[u8]| NativeObject {
            address: NativeObjectRef::MutationCatalog {
                commit_id: [9; 16],
                expected_digest: *blake3::hash(bytes).as_bytes(),
            },
            bytes: bytes.to_vec(),
        };
        let first = make(b"first catalog");
        let conflicting = make(b"different catalog");
        assert!(
            validate_request(&[first.address, conflicting.address]).is_err(),
            "different digests cannot alias one physical address in a batch"
        );
        let mut writes = adapter.new_write_set();
        let response = NativeObjectResponse {
            lix_id: "repo".into(),
            objects: vec![first.clone()],
        };
        stage_native_objects("repo", &[first.address], &response, &mut writes).unwrap();
        let other = NativeObjectResponse {
            lix_id: "repo".into(),
            objects: vec![conflicting.clone()],
        };
        assert!(stage_native_objects("repo", &[conflicting.address], &other, &mut writes).is_err());
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let mut writes = adapter.new_write_set();
        stage_native_objects("repo", &[conflicting.address], &other, &mut writes).unwrap();
        assert!(
            adapter
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .is_err(),
            "canonical immutable storage rejects committed same-key conflicts"
        );
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let values = read
            .get_many(&[StorageGetManyRequest {
                space: first.address.space(),
                keys: &[StorageKey(Bytes::from(first.address.storage_key()))],
                opts: StorageGetOptions::default(),
            }])
            .await
            .unwrap();
        let Some(StorageProjectedValue::FullValue(bytes)) = &values.values[0] else {
            panic!("catalog missing");
        };
        assert_eq!(bytes.as_ref(), first.bytes.as_slice());
    }

    #[tokio::test]
    async fn native_object_roundtrip_and_missing_are_explicit() {
        let storage = Memory::new();
        let authority = open_lix().with_storage(storage.clone()).await.unwrap();
        let adapter = authority.storage_adapter();
        let payload = object(b"immutable transport test");
        let requested = [payload.address];
        assert_eq!(
            authority
                .read_sync_native_objects(&requested)
                .await
                .unwrap_err()
                .code,
            "LIX_NATIVE_OBJECT_UNAVAILABLE"
        );
        let response = NativeObjectResponse {
            lix_id: authority.lix_id().to_owned(),
            objects: vec![payload],
        };
        let mut writes = adapter.new_write_set();
        stage_native_objects(authority.lix_id(), &requested, &response, &mut writes).unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let fetched = authority
            .read_sync_native_objects(&requested)
            .await
            .unwrap();
        assert_eq!(fetched.objects[0].bytes, response.objects[0].bytes);
    }

    #[test]
    fn native_object_validation_rejects_corruption_and_mixed_repositories_before_staging() {
        let adapter = StorageAdapter::new(Memory::new());
        let first = object(b"first");
        let second = object(b"second");
        let requested = [first.address, second.address];
        let mut response = NativeObjectResponse {
            lix_id: "repository".to_owned(),
            objects: vec![first, second],
        };
        let mut writes = adapter.new_write_set();
        assert!(stage_native_objects("other", &requested, &response, &mut writes).is_err());
        response.objects[1].bytes[0] ^= 1;
        assert!(stage_native_objects("repository", &requested, &response, &mut writes).is_err());
        assert!(
            writes
                .staged_value(requested[0].space(), &requested[0].digest())
                .is_none(),
            "valid prefix must not be staged before a later digest fails"
        );
        response.objects[1] = object(b"replacement");
        assert!(stage_native_objects("repository", &requested, &response, &mut writes).is_err());
        let tree_digest = requested[0].digest();
        assert!(
            NativeObjectRef::ScopedRangeNode(tree_digest)
                .validate(b"first")
                .is_err(),
            "object families must retain domain-specific hashing"
        );
    }

    #[test]
    fn maximum_native_payload_base64_roundtrip_fits_transport_cap() {
        // Thirty-one one-byte objects maximize per-object base64 padding;
        // the final object fills the remaining allowed aggregate payload.
        let mut objects = (0..MAX_NATIVE_OBJECT_BATCH - 1)
            .map(|index| object(&[index as u8]))
            .collect::<Vec<_>>();
        objects.push(object(&vec![
            255;
            MAX_NATIVE_OBJECT_PAYLOAD_BYTES - objects.len()
        ]));
        let requested = objects
            .iter()
            .map(|object| object.address)
            .collect::<Vec<_>>();
        // Escape-heavy repository ID exercises the envelope margin too.
        let response = NativeObjectResponse {
            lix_id: "\"".repeat(128),
            objects,
        };
        validate_response(&response.lix_id, &requested, &response).unwrap();
        let encoded = serde_json::to_vec(&response).unwrap();
        assert!(encoded.len() <= MAX_NATIVE_OBJECT_RESPONSE_BYTES);
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert!(json["objects"][0]["bytes"].is_string());
        let decoded: NativeObjectResponse = serde_json::from_slice(&encoded).unwrap();
        validate_response(&response.lix_id, &requested, &decoded).unwrap();
        assert_eq!(
            decoded
                .objects
                .iter()
                .map(|object| object.bytes.len())
                .sum::<usize>(),
            MAX_NATIVE_OBJECT_PAYLOAD_BYTES
        );
        for (expected, actual) in response.objects.iter().zip(decoded.objects) {
            assert_eq!(expected.address, actual.address);
            assert_eq!(expected.bytes, actual.bytes);
        }
    }

    #[tokio::test]
    async fn native_mixed_family_batch_preserves_order_and_rejects_incomplete_responses() {
        let authority = open_lix().await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('native-fixture', 'value')",
                &[],
            )
            .await
            .unwrap();
        authority
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .unwrap();
        let adapter = authority.storage_adapter();
        // Generate each actual native family explicitly: repository initialization
        // need not choose both physical serving formats for this small fixture.
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut writes = adapter.new_write_set();
        crate::tracked_state::TrackedStateTree::new()
            .apply_mutations(&read, &mut writes, None, Default::default(), None)
            .await
            .unwrap();
        use crate::tracked_state::scoped_range::{
            ScopedRangeCoverageMarker, ScopedRangePrefix, stage_scoped_range_tree,
        };
        stage_scoped_range_tree(
            &mut writes,
            [(
                ScopedRangeCoverageMarker {
                    scope: ScopedRangePrefix::try_from_components([b"native-fixture".as_slice()])
                        .unwrap(),
                    row_count: 0,
                    part_count: 0,
                },
                Vec::new(),
            )],
        )
        .unwrap();
        drop(read);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut fixtures = Vec::new();
        // Setup scans discover actual native objects. The serving operation
        // below uses only their two explicit addresses, in mixed-family order.
        for family in [
            NativeObjectRef::ScopedRangeNode([0; 32]),
            NativeObjectRef::TrackedStateTreeChunk([0; 32]),
        ] {
            let entries = read
                .begin_scan(
                    family.space(),
                    crate::storage::KeyRange {
                        lower: std::ops::Bound::Unbounded,
                        upper: std::ops::Bound::Unbounded,
                    },
                    crate::storage::BeginScanOptions::default(),
                )
                .await
                .unwrap()
                .collect_all()
                .await
                .unwrap();
            let entry = entries
                .first()
                .expect("initialized authority has native root objects");
            let digest: [u8; 32] = entry.key.0.as_ref().try_into().unwrap();
            let address = match family {
                NativeObjectRef::ScopedRangeNode(_) => NativeObjectRef::ScopedRangeNode(digest),
                NativeObjectRef::TrackedStateTreeChunk(_) => {
                    NativeObjectRef::TrackedStateTreeChunk(digest)
                }
                _ => unreachable!("fixture uses two tree families"),
            };
            let StorageProjectedValue::FullValue(bytes) = &entry.value else {
                panic!("native fixture payload omitted")
            };
            address.validate(bytes).unwrap();
            fixtures.push(NativeObject {
                address,
                bytes: bytes.to_vec(),
            });
        }
        drop(read);
        let requested = fixtures
            .iter()
            .map(|object| object.address)
            .collect::<Vec<_>>();
        let response = authority
            .read_sync_native_objects(&requested)
            .await
            .unwrap();
        validate_response(authority.lix_id(), &requested, &response).unwrap();
        for (expected, actual) in fixtures.iter().zip(&response.objects) {
            assert_eq!(expected.address, actual.address);
            assert_eq!(expected.bytes, actual.bytes);
        }
        let mut reordered = response.clone();
        reordered.objects.reverse();
        assert!(validate_response(authority.lix_id(), &requested, &reordered).is_err());
        let mut omitted = response;
        omitted.objects.pop();
        assert!(validate_response(authority.lix_id(), &requested, &omitted).is_err());
    }

    #[test]
    fn native_object_limits_and_duplicate_addresses_are_rejected() {
        assert!(validate_request(&[]).is_err());
        let payload = object(b"test");
        assert!(validate_request(&[payload.address; MAX_NATIVE_OBJECT_BATCH + 1]).is_err());
        assert!(validate_request(&[payload.address; 2]).is_err());
        let oversized = object(&vec![1; MAX_NATIVE_OBJECT_PAYLOAD_BYTES + 1]);
        let requested = [oversized.address];
        let response = NativeObjectResponse {
            lix_id: "repository".to_owned(),
            objects: vec![oversized],
        };
        assert!(validate_response("repository", &requested, &response).is_err());
    }
}
