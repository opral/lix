//! Bounded range transfer for immutable native objects larger than one batch.
//!
//! The storage API currently reads whole values: each authority range request
//! loads (and verifies) the complete object before slicing. This bounds wire
//! transfer, not backend memory or repeated read/hash work. Partial bytes are
//! never installed in native storage or interpreted as logical coverage.

use super::native_object::{MAX_NATIVE_OBJECT_PAYLOAD_BYTES, base64_bytes};
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageReadOptions, StorageValue, StorageWriteSet,
};
use crate::tracked_state::NativeObjectRef;
use crate::{Lix, LixError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeObjectRangeRequest {
    pub(crate) address: NativeObjectRef,
    pub(crate) offset: u64,
    pub(crate) max_bytes: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeObjectRangeResponse {
    pub(crate) lix_id: String,
    pub(crate) address: NativeObjectRef,
    pub(crate) offset: u64,
    pub(crate) total_bytes: u64,
    #[serde(with = "base64_bytes")]
    pub(crate) bytes: Vec<u8>,
}

fn invalid(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

pub(crate) fn validate_range_request(request: &NativeObjectRangeRequest) -> Result<(), LixError> {
    if request.max_bytes == 0 || request.max_bytes as usize > MAX_NATIVE_OBJECT_PAYLOAD_BYTES {
        return Err(invalid(
            "native object range length must be between 1 and 1048576 bytes",
        ));
    }
    request
        .offset
        .checked_add(u64::from(request.max_bytes))
        .ok_or_else(|| invalid("native object range overflows"))?;
    Ok(())
}

impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    pub(crate) async fn read_sync_native_object_range(
        &self,
        request: &NativeObjectRangeRequest,
    ) -> Result<NativeObjectRangeResponse, LixError> {
        self.read_sync_native_object_range_with_lease(request, None)
            .await
    }
    pub(crate) async fn read_sync_native_object_range_leased(
        &self,
        request: &NativeObjectRangeRequest,
        lease_id: &str,
    ) -> Result<NativeObjectRangeResponse, LixError> {
        self.read_sync_native_object_range_with_lease(request, Some(lease_id))
            .await
    }
    async fn read_sync_native_object_range_with_lease(
        &self,
        request: &NativeObjectRangeRequest,
        lease_id: Option<&str>,
    ) -> Result<NativeObjectRangeResponse, LixError> {
        validate_range_request(request)?;
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
        let key = [StorageKey(Bytes::from(request.address.storage_key()))];
        let value = PointReadPlan::new(request.address.space(), &key)
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                request.address.annotate_missing(LixError::new(
                    "LIX_NATIVE_OBJECT_UNAVAILABLE",
                    "requested native object is unavailable",
                ))
            })?;
        let StorageProjectedValue::FullValue(bytes) = value else {
            return Err(invalid("native object read omitted payload"));
        };
        request.address.validate(&bytes)?;
        let total_bytes =
            u64::try_from(bytes.len()).map_err(|_| invalid("native object length exceeds u64"))?;
        if request.offset > total_bytes {
            return Err(invalid("native object range starts beyond object end"));
        }
        let start = usize::try_from(request.offset)
            .map_err(|_| invalid("native object offset exceeds address space"))?;
        let count = (bytes.len() - start).min(request.max_bytes as usize);
        Ok(NativeObjectRangeResponse {
            lix_id: self.lix_id().to_owned(),
            address: request.address,
            offset: request.offset,
            total_bytes,
            bytes: bytes[start..start + count].to_vec(),
        })
    }
}

/// Complete, domain-hash-verified bytes. Only the assembler constructs this.
/// Owning one does not certify a baseline or row coverage.
pub(crate) struct CompletedNativeObject {
    address: NativeObjectRef,
    bytes: Vec<u8>,
}
impl CompletedNativeObject {
    pub(crate) fn stage_into(self, writes: &mut StorageWriteSet) {
        writes.put_content_addressed_batch(
            self.address.space(),
            [(
                StorageKey(Bytes::from(self.address.storage_key())),
                StorageValue {
                    bytes: Bytes::from(self.bytes),
                },
            )],
        );
    }
}

pub(crate) struct NativeObjectAssembler {
    lix_id: String,
    address: NativeObjectRef,
    max_object_bytes: usize,
    total_bytes: Option<u64>,
    bytes: Vec<u8>,
    complete: bool,
}

impl NativeObjectAssembler {
    pub(crate) fn new(
        lix_id: String,
        address: NativeObjectRef,
        max_object_bytes: usize,
    ) -> Result<Self, LixError> {
        if lix_id.is_empty() || lix_id.len() > 128 {
            return Err(invalid("invalid native object repository identity"));
        }
        Ok(Self {
            lix_id,
            address,
            max_object_bytes,
            total_bytes: None,
            bytes: Vec::new(),
            complete: false,
        })
    }

    /// Accept exactly the requested contiguous range. The caller's explicit
    /// object budget is checked before retaining bytes; advertised total size
    /// never drives allocation. Invalid responses leave assembly unchanged.
    pub(crate) fn accept(
        &mut self,
        request: &NativeObjectRangeRequest,
        response: &NativeObjectRangeResponse,
    ) -> Result<Option<CompletedNativeObject>, LixError> {
        validate_range_request(request)?;
        if self.complete {
            return Err(invalid("native object assembly is already complete"));
        }
        if request.address != self.address
            || response.address != self.address
            || response.lix_id != self.lix_id
        {
            return Err(invalid("native object range identity mismatch"));
        }
        let offset = u64::try_from(self.bytes.len())
            .map_err(|_| invalid("native object offset exceeds u64"))?;
        if request.offset != offset || response.offset != offset {
            return Err(invalid("native object ranges must be contiguous"));
        }
        let total = usize::try_from(response.total_bytes)
            .map_err(|_| invalid("native object total exceeds address space"))?;
        if total > self.max_object_bytes {
            return Err(invalid("native object exceeds caller assembly budget"));
        }
        if self
            .total_bytes
            .is_some_and(|expected| expected != response.total_bytes)
        {
            return Err(invalid("native object total changed during assembly"));
        }
        if offset > response.total_bytes {
            return Err(invalid("native object range starts beyond object end"));
        }
        let expected_len = (total - self.bytes.len()).min(request.max_bytes as usize);
        if response.bytes.len() != expected_len {
            return Err(invalid("native object range is truncated or oversized"));
        }
        let end = self
            .bytes
            .len()
            .checked_add(response.bytes.len())
            .ok_or_else(|| invalid("native object range overflows"))?;
        let old_len = self.bytes.len();
        self.bytes.extend_from_slice(&response.bytes);
        if end == total {
            if let Err(error) = self.address.validate(&self.bytes) {
                self.bytes.truncate(old_len);
                return Err(error);
            }
            self.complete = true;
            self.total_bytes = Some(response.total_bytes);
            return Ok(Some(CompletedNativeObject {
                address: self.address,
                bytes: std::mem::take(&mut self.bytes),
            }));
        }
        self.total_bytes = Some(response.total_bytes);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_lix;
    use crate::storage_adapter::StorageWriteOptions;

    fn address(bytes: &[u8]) -> NativeObjectRef {
        NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(bytes).as_bytes())
    }
    fn request(address: NativeObjectRef, offset: u64, max_bytes: u32) -> NativeObjectRangeRequest {
        NativeObjectRangeRequest {
            address,
            offset,
            max_bytes,
        }
    }
    fn response(
        address: NativeObjectRef,
        offset: u64,
        total_bytes: u64,
        bytes: &[u8],
    ) -> NativeObjectRangeResponse {
        NativeObjectRangeResponse {
            lix_id: "repository".to_owned(),
            address,
            offset,
            total_bytes,
            bytes: bytes.to_vec(),
        }
    }

    #[tokio::test]
    async fn large_native_object_ranges_stage_only_after_complete_hash_verification() {
        let authority = open_lix().await.unwrap();
        let payload = (0..MAX_NATIVE_OBJECT_PAYLOAD_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let address = address(&payload);
        let adapter = authority.storage_adapter();
        let mut seed = adapter.new_write_set();
        seed.put_content_addressed_batch(
            address.space(),
            [(
                StorageKey(Bytes::copy_from_slice(&address.digest())),
                StorageValue {
                    bytes: Bytes::copy_from_slice(&payload),
                },
            )],
        );
        adapter
            .commit_write_set(seed, StorageWriteOptions::default())
            .await
            .unwrap();
        let mut assembler =
            NativeObjectAssembler::new(authority.lix_id().to_owned(), address, payload.len())
                .unwrap();
        let mut destination = adapter.new_write_set();
        let mut offset = 0u64;
        loop {
            let request = request(
                address,
                offset,
                (MAX_NATIVE_OBJECT_PAYLOAD_BYTES / 2) as u32,
            );
            let response = authority
                .read_sync_native_object_range(&request)
                .await
                .unwrap();
            let wire = serde_json::to_vec(&response).unwrap();
            assert!(wire.len() <= super::super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES);
            let response: NativeObjectRangeResponse = serde_json::from_slice(&wire).unwrap();
            offset += response.bytes.len() as u64;
            match assembler.accept(&request, &response).unwrap() {
                None => assert!(
                    destination
                        .staged_value(address.space(), &address.digest())
                        .is_none()
                ),
                Some(complete) => {
                    assert_eq!(offset, payload.len() as u64);
                    complete.stage_into(&mut destination);
                    break;
                }
            }
        }
        assert_eq!(
            destination
                .staged_value(address.space(), &address.digest())
                .unwrap()
                .as_ref(),
            payload.as_slice()
        );
        let at_end = request(address, payload.len() as u64, 1);
        let end_response = authority
            .read_sync_native_object_range(&at_end)
            .await
            .unwrap();
        assert!(end_response.bytes.is_empty());
        assert!(assembler.accept(&at_end, &end_response).is_err());
        assert!(
            authority
                .read_sync_native_object_range(&request(address, payload.len() as u64 + 1, 1))
                .await
                .is_err()
        );
    }

    #[test]
    fn ranges_reject_gaps_overlaps_identity_changes_totals_and_corruption() {
        let payload = b"abcdefgh";
        let address = address(payload);
        let mut assembler =
            NativeObjectAssembler::new("repository".to_owned(), address, 8).unwrap();
        let first = request(address, 0, 4);
        assert!(
            assembler
                .accept(&first, &response(address, 0, 8, b"abcd"))
                .unwrap()
                .is_none()
        );
        for offset in [0, 3, 5] {
            assert!(
                assembler
                    .accept(
                        &request(address, offset, 4),
                        &response(address, offset, 8, b"efgh")
                    )
                    .is_err()
            );
        }
        let next = request(address, 4, 4);
        for total in [7, 9] {
            assert!(
                assembler
                    .accept(&next, &response(address, 4, total, b"efgh"))
                    .is_err()
            );
        }
        for bytes in [b"".as_slice(), b"efg", b"efghi", b"xxxx"] {
            assert!(
                assembler
                    .accept(&next, &response(address, 4, 8, bytes))
                    .is_err()
            );
        }
        let mut wrong = response(address, 4, 8, b"efgh");
        wrong.lix_id = "other".to_owned();
        assert!(assembler.accept(&next, &wrong).is_err());
        wrong.lix_id = "repository".to_owned();
        wrong.address = NativeObjectRef::ScopedRangeNode(address.digest());
        assert!(assembler.accept(&next, &wrong).is_err());
        assert!(
            assembler
                .accept(&next, &response(address, 4, 8, b"efgh"))
                .unwrap()
                .is_some(),
            "invalid responses must leave retained prefix unchanged"
        );
    }

    #[test]
    fn range_limits_and_empty_objects_are_explicit() {
        let empty = address(b"");
        assert!(validate_range_request(&request(empty, 0, 0)).is_err());
        assert!(
            validate_range_request(&request(
                empty,
                0,
                MAX_NATIVE_OBJECT_PAYLOAD_BYTES as u32 + 1
            ))
            .is_err()
        );
        assert!(validate_range_request(&request(empty, u64::MAX, 1)).is_err());
        let mut assembler = NativeObjectAssembler::new("repository".to_owned(), empty, 0).unwrap();
        let initial = request(empty, 0, 1);
        assert!(
            assembler
                .accept(&initial, &response(empty, 0, u64::MAX, b""))
                .is_err()
        );
        assert!(assembler.bytes.is_empty());
        assert_eq!(
            assembler.bytes.capacity(),
            0,
            "untrusted total must not allocate a buffer"
        );
        assert!(
            assembler
                .accept(&initial, &response(empty, 0, 0, b""))
                .unwrap()
                .is_some()
        );
        let nonempty = address(b"a");
        let mut bounded = NativeObjectAssembler::new("repository".to_owned(), nonempty, 0).unwrap();
        assert!(
            bounded
                .accept(&request(nonempty, 0, 1), &response(nonempty, 0, 1, b"a"))
                .is_err()
        );
        assert_eq!(bounded.bytes.capacity(), 0);
    }
}
