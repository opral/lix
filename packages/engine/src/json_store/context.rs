use crate::json_store::store;
use crate::json_store::types::{
    JsonLoadBatch, JsonLoadRequestRef, JsonProjection, JsonProjectionBatch,
    JsonProjectionLoadRequestRef, JsonRef, JsonValueBatch, JsonWritePlacementRef,
    NormalizedJsonRef,
};
use crate::storage::{StorageReader, StorageWriteSet};
use crate::LixError;
use std::collections::HashSet;

const PACK_LOCAL_MAX_JSON_BYTES: usize = 64 * 1024;

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

    pub(crate) async fn load_bytes_many(
        &self,
        store: &mut impl StorageReader,
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
    S: StorageReader,
{
    pub(crate) async fn load_bytes_many(
        &mut self,
        request: JsonLoadRequestRef<'_>,
    ) -> Result<JsonLoadBatch, LixError> {
        store::load_json_bytes_many_in_scope(&mut self.store, request.refs, request.scope)
            .await
            .map(JsonLoadBatch::new)
    }

    pub(crate) async fn load_values_many(
        &mut self,
        request: JsonLoadRequestRef<'_>,
    ) -> Result<JsonValueBatch, LixError> {
        let refs = request.refs;
        let values = self
            .load_bytes_many(request)
            .await?
            .into_values()
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| match bytes {
                Some(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "json ref '{}' is invalid JSON: {error}",
                            refs[index].to_hex()
                        ),
                    )
                }),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(JsonValueBatch::new(values))
    }

    pub(crate) async fn load_projections_many(
        &mut self,
        request: JsonProjectionLoadRequestRef<'_>,
    ) -> Result<JsonProjectionBatch, LixError> {
        let values = self
            .load_values_many(JsonLoadRequestRef {
                refs: request.refs,
                scope: request.scope,
            })
            .await?
            .into_values()
            .into_iter()
            .map(|value| {
                value.map(|value| {
                    JsonProjection::new(
                        request
                            .paths
                            .iter()
                            .map(|path| value.pointer(path.as_str()).cloned())
                            .collect(),
                    )
                })
            })
            .collect();
        Ok(JsonProjectionBatch::new(values))
    }
}

pub(crate) struct JsonStoreWriter;

impl JsonStoreWriter {
    fn new() -> Self {
        Self
    }

    pub(crate) fn stage_batch<'a>(
        &mut self,
        writes: &mut StorageWriteSet,
        placement: JsonWritePlacementRef<'a>,
        payloads: impl IntoIterator<Item = NormalizedJsonRef<'a>>,
    ) -> Result<Vec<JsonRef>, LixError> {
        let mut unique_encoded = Vec::new();
        let mut order = Vec::new();
        let mut seen = HashSet::new();
        for payload in payloads {
            let encoded = store::encode_json_str(payload.normalized)?;
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

        let pack_local = matches!(placement, JsonWritePlacementRef::CommitPack { .. });
        if let JsonWritePlacementRef::CommitPack { commit_id, pack_id } = placement {
            let pack_entries = unique_encoded
                .iter()
                .filter(|encoded| encoded.uncompressed_len <= PACK_LOCAL_MAX_JSON_BYTES)
                .collect::<Vec<_>>();
            if !pack_entries.is_empty() {
                let encoded_pack = store::encode_json_pack(&pack_entries)?;
                writes.put(
                    store::JSON_PACK_NAMESPACE,
                    store::pack_key(commit_id, pack_id),
                    encoded_pack,
                );
            }
        }

        for encoded in &unique_encoded {
            if pack_local && encoded.uncompressed_len <= PACK_LOCAL_MAX_JSON_BYTES {
                continue;
            }
            writes.put(
                store::JSON_NAMESPACE,
                encoded.json_ref.as_hash_bytes().to_vec(),
                store::encode_direct_json_payload(encoded),
            );
        }

        Ok(order)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::json_store::types::JsonReadScopeRef;
    use crate::storage::StorageContext;

    #[tokio::test]
    async fn commit_local_batch_writes_pack_without_direct_rows() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let context = JsonStoreContext::new();
        let first = "{\"value\":\"first\"}";
        let second = "{\"value\":\"second\"}";

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        context
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::CommitPack {
                    commit_id: "commit-a",
                    pack_id: 0,
                },
                [
                    NormalizedJsonRef { normalized: first },
                    NormalizedJsonRef { normalized: second },
                ],
            )
            .expect("json pack should stage");
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("json pack should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let refs = [
            JsonRef::for_content(first.as_bytes()),
            JsonRef::for_content(second.as_bytes()),
        ];
        let unknown = context
            .reader(storage.clone())
            .load_bytes_many(JsonLoadRequestRef {
                refs: &refs,
                scope: JsonReadScopeRef::OutOfBand,
            })
            .await
            .expect("unknown load should check direct rows");
        assert_eq!(unknown.into_values(), vec![None, None]);

        let pack_ids = [0];
        let packed = context
            .reader(storage.clone())
            .load_bytes_many(JsonLoadRequestRef {
                refs: &refs,
                scope: JsonReadScopeRef::CommitPacks {
                    commit_id: "commit-a",
                    pack_ids: &pack_ids,
                },
            })
            .await
            .expect("packed load should hydrate");
        assert_eq!(
            packed.into_values(),
            vec![
                Some(first.as_bytes().to_vec()),
                Some(second.as_bytes().to_vec())
            ]
        );
    }

    #[tokio::test]
    async fn commit_local_batch_dedupes_pack_payloads_but_returns_request_order() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let context = JsonStoreContext::new();
        let first = "{\"value\":\"first\"}";
        let second = "{\"value\":\"second\"}";

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let staged_refs = context
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::CommitPack {
                    commit_id: "commit-a",
                    pack_id: 0,
                },
                [
                    NormalizedJsonRef { normalized: first },
                    NormalizedJsonRef { normalized: first },
                    NormalizedJsonRef { normalized: second },
                ],
            )
            .expect("json pack should stage");
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("json pack should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let first_ref = JsonRef::for_content(first.as_bytes());
        let second_ref = JsonRef::for_content(second.as_bytes());
        assert_eq!(staged_refs, vec![first_ref, first_ref, second_ref]);

        let refs = [first_ref, second_ref];
        let unknown = context
            .reader(storage.clone())
            .load_bytes_many(JsonLoadRequestRef {
                refs: &refs,
                scope: JsonReadScopeRef::OutOfBand,
            })
            .await
            .expect("unknown load should check direct rows");
        assert_eq!(unknown.into_values(), vec![None, None]);

        let pack_ids = [0];
        let packed = context
            .reader(storage.clone())
            .load_bytes_many(JsonLoadRequestRef {
                refs: &refs,
                scope: JsonReadScopeRef::CommitPacks {
                    commit_id: "commit-a",
                    pack_ids: &pack_ids,
                },
            })
            .await
            .expect("packed load should hydrate");
        assert_eq!(
            packed.into_values(),
            vec![
                Some(first.as_bytes().to_vec()),
                Some(second.as_bytes().to_vec())
            ]
        );
    }
}
