pub(crate) mod compression;
pub(crate) mod context;
mod encoded;
pub(crate) mod store;
pub(crate) mod types;

use crate::storage::{KvGetGroup, KvGetRequest, KvScanRange, KvScanRequest, StorageWriteSet};
use crate::LixError;

#[allow(unused_imports)]
pub(crate) use context::{JsonStoreContext, JsonStoreReader, JsonStoreWriter};
pub(crate) use types::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonWritePlacementRef, NormalizedJson,
    NormalizedJsonRef,
};

pub(crate) fn direct_json_payload_scan_request(
    after: Option<Vec<u8>>,
    limit: usize,
) -> KvScanRequest {
    KvScanRequest {
        namespace: store::JSON_NAMESPACE.to_string(),
        range: KvScanRange::prefix(Vec::new()),
        after,
        limit,
    }
}

pub(crate) fn direct_json_payload_get_request<'a>(
    json_refs: impl IntoIterator<Item = &'a JsonRef>,
) -> KvGetRequest {
    KvGetRequest {
        groups: vec![KvGetGroup {
            namespace: store::JSON_NAMESPACE.to_string(),
            keys: json_refs
                .into_iter()
                .map(|json_ref| json_ref.as_hash_bytes().to_vec())
                .collect(),
        }],
    }
}

pub(crate) fn direct_json_payload_ref_from_key(key: &[u8]) -> Result<JsonRef, LixError> {
    let hash: [u8; 32] = key.try_into().map_err(|_| {
        LixError::unknown(format!(
            "json_store direct payload key has {} bytes, expected 32",
            key.len()
        ))
    })?;
    Ok(JsonRef::from_hash_bytes(hash))
}

pub(crate) fn stage_direct_json_payload_delete(writes: &mut StorageWriteSet, json_ref: &JsonRef) {
    writes.delete(store::JSON_NAMESPACE, json_ref.as_hash_bytes().to_vec());
}

#[cfg(test)]
pub(crate) fn stage_direct_json_payload_put(
    writes: &mut StorageWriteSet,
    json_ref: &JsonRef,
    bytes: Vec<u8>,
) {
    writes.put(
        store::JSON_NAMESPACE,
        json_ref.as_hash_bytes().to_vec(),
        bytes,
    );
}
