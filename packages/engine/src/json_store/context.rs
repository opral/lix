use crate::LixError;
use crate::json_store::store;
use crate::json_store::types::{
    JsonLoadBatch, JsonLoadRequestRef, JsonRef, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::storage::{StorageKey, StorageRead, StorageValue, StorageWriteSet};
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonStoreContext;

impl JsonStoreContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> JsonStoreReader<S>
    where
        S: StorageRead,
    {
        JsonStoreReader { store }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer(&self) -> JsonStoreWriter {
        JsonStoreWriter::new()
    }

    pub(crate) async fn load_bytes_many(
        &self,
        store: &(impl StorageRead + ?Sized),
        request: JsonLoadRequestRef<'_>,
    ) -> Result<JsonLoadBatch, LixError> {
        store::load_json_bytes_many_in_scope(store, request.refs, request.scope)
            .await
            .map(JsonLoadBatch::new)
    }
}

pub(crate) struct JsonStoreReader<S> {
    store: S,
}

impl<S> Clone for JsonStoreReader<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<S> JsonStoreReader<S>
where
    S: StorageRead,
{
    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) async fn load_bytes_many(
        &mut self,
        request: JsonLoadRequestRef<'_>,
    ) -> Result<JsonLoadBatch, LixError> {
        store::load_json_bytes_many_in_scope(&self.store, request.refs, request.scope)
            .await
            .map(JsonLoadBatch::new)
    }
}

pub(crate) struct JsonStoreWriter;

impl JsonStoreWriter {
    fn new() -> Self {
        Self
    }

    #[expect(clippy::needless_pass_by_ref_mut, clippy::unused_self)]
    pub(crate) fn stage_batch<'a>(
        &mut self,
        writes: &mut StorageWriteSet,
        placement: JsonWritePlacementRef,
        payloads: impl IntoIterator<Item = NormalizedJsonRef<'a>>,
    ) -> Result<Vec<JsonRef>, LixError> {
        let JsonWritePlacementRef::OutOfBand = placement;
        let mut unique_encoded = Vec::new();
        let mut order = Vec::new();
        let mut seen = HashSet::new();
        for payload in payloads {
            let encoded = match payload.trusted_json_ref() {
                Some(json_ref) => store::encode_json_str_with_ref(payload.normalized(), json_ref)?,
                None => store::encode_json_str(payload.normalized())?,
            };
            let hash: [u8; 32] = encoded
                .json_ref
                .as_hash_bytes()
                .try_into()
                .expect("json ref hash is fixed size");
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_json_store_stage_bytes(hash);
            order.push(encoded.json_ref);
            if seen.insert(hash) {
                unique_encoded.push(encoded);
            }
        }

        for encoded in &unique_encoded {
            writes.put(
                store::JSON_SPACE,
                StorageKey(Bytes::copy_from_slice(encoded.json_ref.as_hash_bytes())),
                StorageValue {
                    bytes: Bytes::from(store::encode_direct_json_payload(encoded)),
                },
            );
        }

        Ok(order)
    }
}
