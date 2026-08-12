pub(crate) mod compression;
pub(crate) mod context;
mod encoded;
pub(crate) mod store;
pub(crate) mod types;

#[cfg(test)]
pub(crate) use context::UntrackedJsonReclaimCandidate;
#[allow(unused_imports)]
pub(crate) use context::{
    JsonStoreContext, JsonStoreReader, JsonStoreWriter, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
};
// Owner facade for the storage-space registry (`crate::storage_spaces`),
// which is compiled in every configuration.
pub(crate) use store::JSON_SPACE;
pub(crate) use types::{
    JSON_INLINE_MAX_BYTES, JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonSlotRef,
    JsonWritePlacementRef, NormalizedJson, NormalizedJsonRef, json_slot_storage,
    json_slot_storage_ref,
};
