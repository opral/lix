pub(crate) mod compression;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) mod context;
mod encoded;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) mod store;
pub(crate) mod types;

#[cfg(test)]
pub(crate) use context::UntrackedJsonReclaimCandidate;
#[cfg(any(test, feature = "storage-benches"))]
#[allow(unused_imports)]
pub(crate) use context::{
    JsonStoreContext, JsonStoreReader, JsonStoreWriter, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
};
pub(crate) use types::{
    JSON_INLINE_MAX_BYTES, JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonSlotRef,
    JsonWritePlacementRef, NormalizedJson, NormalizedJsonRef, json_slot_storage,
    json_slot_storage_ref,
};
