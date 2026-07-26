use crate::LixError;
use crate::json_store::store;
use crate::json_store::types::{
    JsonLoadBatch, JsonLoadRequestRef, JsonRef, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::storage_adapter::{
    ScanPlan, StorageAdapterRead, StorageCoreProjection, StorageKey, StoragePrefix,
    StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use bytes::Bytes;
use std::collections::HashSet;

/// A durable deletion hint for a large JSON payload retired from the
/// untracked current-state plane.
///
/// The content-addressed JSON store intentionally has no refcount: the same
/// payload can be owned by tracked history and by multiple untracked rows.
/// This key is therefore not an ownership edge. Repository GC compares it
/// with the complete live-payload set before it deletes the JSON value.
pub(crate) const UNTRACKED_JSON_RECLAIM_CANDIDATE_NAMESPACE: &str =
    "json_store.untracked_reclaim_candidate.v1";
pub(crate) const UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0002_0002),
    UNTRACKED_JSON_RECLAIM_CANDIDATE_NAMESPACE,
);

const UNTRACKED_JSON_RECLAIM_CANDIDATE_VALUE: &[u8] = b"\x01";

/// One candidate record discovered from the pinned repository-GC view.
///
/// Malformed keys are retained as `None` so GC can reclaim the derived record
/// without treating a corrupt hint as a reason to touch an arbitrary JSON
/// payload.
#[derive(Clone, Debug)]
pub(crate) struct UntrackedJsonReclaimCandidate {
    pub(crate) key: StorageKey,
    pub(crate) json_ref: Option<JsonRef>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonStoreContext;

impl JsonStoreContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> JsonStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        JsonStoreReader { store }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer(&self) -> JsonStoreWriter {
        JsonStoreWriter::new()
    }

    pub(crate) async fn load_bytes_many(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        request: JsonLoadRequestRef<'_>,
    ) -> Result<JsonLoadBatch, LixError> {
        store::load_json_bytes_many_in_scope(store, request.refs, request.scope)
            .await
            .map(JsonLoadBatch::new)
    }

    /// Lists the durable hints for out-of-band untracked payloads that may no
    /// longer have an owner. This is deliberately a key-only, paged scan: the
    /// hint value carries no data and the caller already holds the complete
    /// logical root set from the same pinned read.
    pub(crate) async fn scan_untracked_reclaim_candidates(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
    ) -> Result<Vec<UntrackedJsonReclaimCandidate>, LixError> {
        let plan = ScanPlan::prefix(
            UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        );
        let mut candidates = Vec::new();
        let mut resume_after = None;
        loop {
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        projection: StorageCoreProjection::KeyOnly,
                        resume_after: resume_after.clone(),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            candidates.extend(page.value.entries.into_iter().map(|entry| {
                let json_ref = <[u8; 32]>::try_from(entry.key.0.as_ref())
                    .ok()
                    .map(JsonRef::from_hash_bytes);
                UntrackedJsonReclaimCandidate {
                    key: entry.key,
                    json_ref,
                }
            }));
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
        Ok(candidates)
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
    S: StorageAdapterRead,
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

    /// Records exactly the out-of-band untracked JSON payloads that lost one
    /// current-state owner in this atomic write. Repeated content hashes are
    /// idempotent: they remain one durable GC hint until a sweep proves them
    /// dead.
    #[expect(clippy::needless_pass_by_ref_mut, clippy::unused_self)]
    pub(crate) fn stage_untracked_reclaim_candidates(
        &self,
        writes: &mut StorageWriteSet,
        refs: impl IntoIterator<Item = JsonRef>,
    ) {
        // The caller can retire thousands of rows in one current-state
        // mutation. Batch the idempotent puts so the write set indexes the
        // candidate lane once rather than linearly rescanning it per hash.
        // The write-set helper also coalesces a candidate emitted by an
        // earlier current-state staging call in the same atomic commit.
        writes.put_content_addressed_batch(
            UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
            refs.into_iter().map(|json_ref| {
                (
                    StorageKey(Bytes::copy_from_slice(json_ref.as_hash_bytes())),
                    StorageValue {
                        bytes: Bytes::from_static(UNTRACKED_JSON_RECLAIM_CANDIDATE_VALUE),
                    },
                )
            }),
        );
    }

    /// Removes only candidate hints whose payload was proved dead (or whose
    /// key was malformed) from the same pinned GC view.
    #[expect(clippy::needless_pass_by_ref_mut, clippy::unused_self)]
    pub(crate) fn stage_delete_untracked_reclaim_candidates(
        &self,
        writes: &mut StorageWriteSet,
        keys: impl IntoIterator<Item = StorageKey>,
    ) {
        for key in keys {
            writes.delete(UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE, key);
        }
    }

    #[allow(dead_code)] // Activated by the checkpoint GC integration.
    #[expect(clippy::unused_self)]
    pub(crate) fn stage_delete_refs(
        &self,
        writes: &mut StorageWriteSet,
        refs: impl IntoIterator<Item = JsonRef>,
    ) {
        for json_ref in refs {
            writes.delete(
                store::JSON_SPACE,
                StorageKey(Bytes::copy_from_slice(json_ref.as_hash_bytes())),
            );
        }
    }
}
