use crate::json_store::store;
use crate::json_store::types::{JsonProjection, JsonProjectionPath, JsonRef, NormalizedJson};
use crate::storage::{StorageReader, StorageWriteSet};
use crate::LixError;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonStoreContext;

impl JsonStoreContext {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn reader<S>(&self, store: S) -> JsonStoreReader<S>
    where
        S: StorageReader,
    {
        JsonStoreReader { store }
    }

    pub(crate) fn writer(&self) -> JsonStoreWriter {
        JsonStoreWriter::new()
    }

    pub(crate) async fn load_bytes(
        &self,
        store: &mut impl StorageReader,
        json_ref: &JsonRef,
    ) -> Result<Option<Vec<u8>>, LixError> {
        store::load_json_bytes(store, json_ref).await
    }

    pub(crate) async fn load_bytes_many(
        &self,
        store: &mut impl StorageReader,
        json_refs: &[JsonRef],
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        store::load_json_bytes_many(store, json_refs).await
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
    S: StorageReader,
{
    pub(crate) async fn load_bytes(
        &mut self,
        json_ref: &JsonRef,
    ) -> Result<Option<Vec<u8>>, LixError> {
        store::load_json_bytes(&mut self.store, json_ref).await
    }

    pub(crate) async fn load_bytes_many(
        &mut self,
        json_refs: &[JsonRef],
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        store::load_json_bytes_many(&mut self.store, json_refs).await
    }

    pub(crate) async fn load_json_value(
        &mut self,
        json_ref: &JsonRef,
    ) -> Result<Option<serde_json::Value>, LixError> {
        let Some(bytes) = self.load_bytes(json_ref).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("json ref '{}' is invalid JSON: {error}", json_ref.to_hex()),
            )
        })
    }

    pub(crate) async fn load_json_projection(
        &mut self,
        json_ref: &JsonRef,
        paths: &[JsonProjectionPath],
    ) -> Result<Option<JsonProjection>, LixError> {
        let Some(value) = self.load_json_value(json_ref).await? else {
            return Ok(None);
        };
        let values = paths
            .iter()
            .map(|path| value.pointer(path.as_str()).cloned())
            .collect();
        Ok(Some(JsonProjection::new(values)))
    }
}

pub(crate) struct JsonStoreWriter {
    pending: HashMap<[u8; 32], PendingJsonWrite>,
    flushed: HashSet<[u8; 32]>,
}

struct PendingJsonWrite {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl JsonStoreWriter {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            flushed: HashSet::new(),
        }
    }

    pub(crate) fn prepare_json(&mut self, normalized: NormalizedJson) -> Result<JsonRef, LixError> {
        let hash = blake3::hash(normalized.as_bytes());
        let hash_bytes = *hash.as_bytes();
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_json_store_stage_bytes(hash_bytes);
        let json_ref = JsonRef::from_hash(hash);
        if self.flushed.contains(&hash_bytes) {
            return Ok(json_ref);
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = self.pending.entry(hash_bytes) {
            let (json_ref, stored_payload) =
                store::encode_json_str_for_storage_with_ref(normalized.as_str(), json_ref.clone())?;
            entry.insert(PendingJsonWrite {
                key: json_ref.as_hash_bytes().to_vec(),
                value: stored_payload,
            });
        }
        Ok(json_ref)
    }

    pub(crate) fn flush_into(&mut self, writes: &mut StorageWriteSet) {
        for (hash, pending) in std::mem::take(&mut self.pending) {
            writes.put(store::JSON_NAMESPACE, pending.key, pending.value);
            self.flushed.insert(hash);
        }
    }
}
