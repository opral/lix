use crate::LixError;
use crate::json_store::store;
use crate::json_store::types::{
    JsonLoadBatch, JsonLoadRequestRef, JsonRef, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, StorageAdapterRead, StorageWriteSet,
};
use bytes::Bytes;
use std::collections::HashSet;

const JSON_REF_BYTES: usize = 32;

#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonStoreContext;

impl JsonStoreContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Protocol-v68 migrations still need to resolve retired out-of-band JSON
    /// references even though protocol-v69 live rows never create them.
    pub(crate) async fn load_bytes_many<S>(
        &self,
        store: &S,
        request: JsonLoadRequestRef<'_>,
    ) -> Result<JsonLoadBatch, LixError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        store::load_json_bytes_many_in_scope(store, request.refs, request.scope)
            .await
            .map(JsonLoadBatch::new)
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer(&self) -> JsonStoreWriter {
        JsonStoreWriter::new()
    }
}

pub(crate) struct JsonStoreWriter;

#[derive(Clone, Copy)]
struct UniqueJsonPayloadRef<'a> {
    json_ref: JsonRef,
    normalized: &'a str,
}

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
        let payloads = payloads.into_iter();
        let (lower_bound, upper_bound) = payloads.size_hint();
        let row_capacity = upper_bound.unwrap_or(lower_bound);
        let mut unique_payloads = Vec::with_capacity(row_capacity);
        let mut value_plan = store::StoredJsonBatchPlan::default();
        let mut order = Vec::with_capacity(row_capacity);
        let mut seen = HashSet::with_capacity(row_capacity);
        for payload in payloads {
            let normalized = payload.normalized();
            let json_ref = payload
                .trusted_json_ref()
                .unwrap_or_else(|| JsonRef::for_content(normalized.as_bytes()));
            let hash: [u8; 32] = json_ref
                .as_hash_bytes()
                .try_into()
                .expect("json ref hash is fixed size");
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_json_store_stage_bytes(hash);
            order.push(json_ref);
            if seen.insert(hash) {
                value_plan.push_json(normalized)?;
                unique_payloads.push(UniqueJsonPayloadRef {
                    json_ref,
                    normalized,
                });
            }
        }

        if unique_payloads.is_empty() {
            return Ok(order);
        }

        let key_bytes_len = unique_payloads
            .len()
            .checked_mul(JSON_REF_BYTES)
            .ok_or_else(|| LixError::unknown("JSON-store key batch exceeds addressable memory"))?;
        let mut key_bytes = Vec::with_capacity(key_bytes_len);
        let mut value_encoder = value_plan.encoder()?;
        let mut puts = Vec::with_capacity(unique_payloads.len());
        for payload in unique_payloads {
            let key_offset = key_bytes.len();
            key_bytes.extend_from_slice(payload.json_ref.as_hash_bytes());
            let value_range =
                value_encoder.append_json_with_ref(payload.normalized, payload.json_ref)?;
            puts.push(EncodedPut {
                key: BufferRange::new(key_offset, JSON_REF_BYTES),
                value: BufferRange::new(value_range.start, value_range.len()),
            });
        }
        debug_assert_eq!(key_bytes.len(), key_bytes_len);
        let value_bytes = value_encoder.finish();
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(key_bytes),
            Bytes::from(value_bytes),
            puts,
            Vec::new(),
        )
        .expect("JSON-store batch descriptors are built from arena offsets");
        writes.stage_encoded_batch(store::JSON_SPACE, batch);

        Ok(order)
    }

    /// Deletes JSON payload rows.
    ///
    /// The caller must have proved that no owner outliving its write set names
    /// these refs — see `gc::collect_live_json_payload_hashes` — and must stage
    /// [`crate::json_store::stage_json_reclamation_fence`] in the same write
    /// set, because payload rows are content addressed and a concurrent
    /// publisher can resolve onto one of them.
    #[expect(clippy::unused_self)]
    pub(crate) fn stage_delete_refs<I>(&self, writes: &mut StorageWriteSet, refs: I)
    where
        I: IntoIterator<Item = JsonRef>,
        I::IntoIter: ExactSizeIterator,
    {
        let refs = refs.into_iter();
        let row_capacity = refs.len();
        let mut key_bytes =
            Vec::with_capacity(row_capacity.checked_mul(JSON_REF_BYTES).unwrap_or_default());
        let mut deletes = Vec::with_capacity(row_capacity);
        for json_ref in refs {
            let offset = key_bytes.len();
            key_bytes.extend_from_slice(json_ref.as_hash_bytes());
            deletes.push(BufferRange::new(offset, JSON_REF_BYTES));
        }
        if deletes.is_empty() {
            return;
        }
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(key_bytes),
            Bytes::new(),
            Vec::new(),
            deletes,
        )
        .expect("JSON-store delete descriptors are built from arena offsets");
        writes.stage_encoded_batch(store::JSON_SPACE, batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_store::types::JsonReadScopeRef;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    #[tokio::test]
    async fn stage_batch_deduplicates_into_one_key_and_value_arena() {
        let first = format!(r#"{{"kind":"first","data":"{}"}}"#, "a".repeat(1_024));
        let second = format!(r#"{{"kind":"second","data":"{}"}}"#, "b".repeat(1_024));
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let refs = JsonStoreContext::new()
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [
                    NormalizedJsonRef::new(&first),
                    NormalizedJsonRef::new(&first),
                    NormalizedJsonRef::new(&second),
                ],
            )
            .expect("JSON batch should stage");

        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0], refs[1], "duplicate content keeps request order");
        assert_ne!(refs[0], refs[2]);
        let arenas = writes.arena_stats();
        assert_eq!(arenas.spaces, 1);
        assert_eq!(arenas.put_descriptors, 2);
        assert_eq!(arenas.key_shared_buffers, 1);
        assert_eq!(arenas.key_shared_bytes, 2 * 32);
        assert_eq!(arenas.value_shared_buffers, 1);
        assert_eq!(arenas.key_inline_bytes, 0);
        assert_eq!(arenas.value_inline_bytes, 0);

        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("shared JSON batch should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let loaded = JsonStoreContext::new()
            .load_bytes_many(&read, JsonLoadRequestRef {
                refs: &refs,
                scope: JsonReadScopeRef::OutOfBand,
            })
            .await
            .expect("shared JSON batch should decode")
            .into_values();
        assert_eq!(
            loaded,
            vec![
                Some(Bytes::copy_from_slice(first.as_bytes())),
                Some(Bytes::copy_from_slice(first.as_bytes())),
                Some(Bytes::copy_from_slice(second.as_bytes())),
            ]
        );
        assert_eq!(
            loaded[0].as_ref().expect("first payload").as_ptr(),
            loaded[1].as_ref().expect("duplicate payload").as_ptr(),
            "duplicate request rows must share one loaded payload buffer"
        );
    }

    #[test]
    fn delete_batches_use_one_key_arena_for_ten_thousand_rows() {
        const ROW_COUNT: usize = 10_000;

        let refs = (0_u32..ROW_COUNT as u32)
            .map(|index| JsonRef::for_content(&index.to_be_bytes()))
            .collect::<Vec<_>>();

        let mut payload_deletes = StorageWriteSet::new();
        JsonStoreContext::new()
            .writer()
            .stage_delete_refs(&mut payload_deletes, refs.iter().copied());
        let arenas = payload_deletes.arena_stats();
        assert_eq!(arenas.put_descriptors, 0);
        assert_eq!(arenas.delete_descriptors, ROW_COUNT);
        assert_eq!(arenas.key_shared_buffers, 1);
        assert_eq!(arenas.key_shared_bytes, ROW_COUNT * JSON_REF_BYTES);
        assert_eq!(arenas.value_shared_buffers, 0);
    }
}
