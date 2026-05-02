use crate::backend::{KvStore, KvWriter};
use crate::json_store::store;
use crate::json_store::types::{JsonProjection, JsonProjectionPath, JsonRef, StoreJsonOptions};
use crate::LixError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonStoreContext;

impl JsonStoreContext {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn reader<S>(&self, store: S) -> JsonStoreReader<S>
    where
        S: KvStore,
    {
        JsonStoreReader { store }
    }

    pub(crate) fn writer<S>(&self, store: S) -> JsonStoreWriter<S>
    where
        S: KvWriter,
    {
        JsonStoreWriter { store }
    }
}

pub(crate) struct JsonStoreReader<S> {
    store: S,
}

impl<S> JsonStoreReader<S>
where
    S: KvStore,
{
    pub(crate) async fn load_json_bytes(
        &mut self,
        json_ref: &JsonRef,
    ) -> Result<Option<Vec<u8>>, LixError> {
        store::load_json_bytes(&mut self.store, json_ref).await
    }

    pub(crate) async fn load_json_value(
        &mut self,
        json_ref: &JsonRef,
    ) -> Result<Option<serde_json::Value>, LixError> {
        let Some(bytes) = self.load_json_bytes(json_ref).await? else {
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

pub(crate) struct JsonStoreWriter<S> {
    store: S,
}

impl<S> JsonStoreWriter<S>
where
    S: KvWriter,
{
    pub(crate) async fn store_json_bytes(
        &mut self,
        bytes: &[u8],
        options: StoreJsonOptions<'_>,
    ) -> Result<JsonRef, LixError> {
        store::store_json_bytes(&mut self.store, bytes, options).await
    }
}
