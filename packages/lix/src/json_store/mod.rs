pub(crate) mod compression;
pub(crate) mod context;
mod encoded;
mod fence;
pub(crate) mod store;
pub(crate) mod types;

#[allow(unused_imports)]
pub(crate) use context::{JsonStoreContext, JsonStoreReader, JsonStoreWriter};
pub(crate) use fence::{stage_json_publication_fence, stage_json_reclamation_fence};
// Owner facade for the storage-space registry (`crate::storage_spaces`),
// which is compiled in every configuration.
pub(crate) use store::JSON_SPACE;
pub(crate) use types::{
    JSON_INLINE_MAX_BYTES, JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlot, JsonSlotRef,
    JsonWritePlacementRef, NormalizedJson, NormalizedJsonRef, json_slot_storage,
    json_slot_storage_ref,
};
