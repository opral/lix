use crate::json_store::store;
use crate::json_store::types::{JsonProjection, JsonProjectionPath, JsonRef};
use crate::storage::{StorageReader, StorageWriteSet};
use crate::LixError;
use std::collections::HashSet;

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

    pub(crate) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> JsonStoreWriter<'a> {
        JsonStoreWriter::new(writes)
    }

    pub(crate) async fn load_bytes(
        &self,
        store: &mut impl StorageReader,
        json_ref: &JsonRef,
    ) -> Result<Option<Vec<u8>>, LixError> {
        store::load_json_bytes(store, json_ref).await
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

pub(crate) struct JsonStoreWriter<'a> {
    writes: &'a mut StorageWriteSet,
    seen: HashSet<[u8; 32]>,
}

impl<'a> JsonStoreWriter<'a> {
    fn new(writes: &'a mut StorageWriteSet) -> Self {
        Self {
            writes,
            seen: HashSet::new(),
        }
    }

    pub(crate) fn stage_bytes(&mut self, bytes: &[u8]) -> Result<JsonRef, LixError> {
        let json = std::str::from_utf8(bytes).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("json bytes are invalid UTF-8: {error}"),
            )
        })?;
        let hash = blake3::hash(bytes);
        let hash_bytes = *hash.as_bytes();
        let json_ref = JsonRef::from_hash(hash);
        if !self.seen.insert(hash_bytes) {
            return Ok(json_ref);
        }
        let (json_ref, stored_payload) =
            store::encode_json_str_for_storage_with_ref(json, json_ref)?;
        self.writes.put(
            store::JSON_NAMESPACE,
            json_ref.as_hash_bytes().to_vec(),
            stored_payload,
        );
        Ok(json_ref)
    }
}
