pub(crate) mod compression;
pub(crate) mod context;
mod encoded;
pub(crate) mod store;
pub(crate) mod types;

use crate::storage::StorageWriteSet;
use crate::LixError;

#[allow(unused_imports)]
pub(crate) use context::{JsonStoreContext, JsonStoreReader, JsonStoreWriter};
pub(crate) use types::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonWritePlacementRef, NormalizedJson,
    NormalizedJsonRef,
};

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
    writes.delete(store::JSON_SPACE, json_ref.as_hash_bytes().to_vec());
}

#[cfg(test)]
pub(crate) fn stage_direct_json_payload_put(
    writes: &mut StorageWriteSet,
    json_ref: &JsonRef,
    bytes: Vec<u8>,
) {
    writes.put(store::JSON_SPACE, json_ref.as_hash_bytes().to_vec(), bytes);
}
