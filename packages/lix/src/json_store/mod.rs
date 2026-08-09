#[cfg(test)]
pub(crate) mod compression;
#[cfg(test)]
pub(crate) mod context;
#[cfg(test)]
mod encoded;
#[cfg(test)]
pub(crate) mod store;
pub(crate) mod types;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use context::{
    JsonStoreContext, JsonStoreReader, JsonStoreWriter, UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
};
pub(crate) use types::{
    JSON_INLINE_MAX_BYTES, JsonRef, JsonSlot, JsonSlotRef, json_slot_storage, json_slot_storage_ref,
};
#[cfg(test)]
pub(crate) use types::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonWritePlacementRef, NormalizedJsonRef,
};
