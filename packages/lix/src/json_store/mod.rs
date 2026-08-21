pub(crate) mod compression;
pub(crate) mod context;
mod encoded;
mod fence;
pub(crate) mod store;
pub(crate) mod types;

#[allow(unused_imports)]
pub(crate) use context::{JsonStoreContext, JsonStoreWriter};
pub(crate) use fence::stage_json_publication_fence;
// Owner facade for the storage-space registry (`crate::storage_spaces`),
// which is compiled in every configuration.
pub(crate) use store::JSON_SPACE;
pub(crate) use types::{
    JSON_INLINE_MAX_BYTES, JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonWritePlacementRef,
    LegacyJsonValue, LegacyJsonValueRef, NormalizedJson, NormalizedJsonRef, json_slot_storage,
    json_slot_storage_ref,
};
