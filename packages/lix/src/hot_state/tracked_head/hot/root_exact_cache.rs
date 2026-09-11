//! Bounded cache of immutable native exact candidates. Current HOT collection
//! controls, tombstones and winner selection are deliberately outside it.
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::tracked_state::{
    MaterializedTrackedStateExactBatch, TrackedStateKey, TrackedStateKeyRef,
};
use std::sync::{Arc, Mutex};

const MAX_ENTRIES: usize = 32;
const MAX_BYTES: usize = 8 * 1024 * 1024;
#[derive(Default)]
pub(super) struct Cache {
    entries: Mutex<Entries>,
}
#[derive(Default)]
struct Entries {
    resident: Vec<Entry>,
    bytes: usize,
}
struct Entry {
    base: CommitId,
    keys: Vec<TrackedStateKey>,
    projection: ChangeRecordProjection,
    batch: Arc<MaterializedTrackedStateExactBatch>,
    bytes: usize,
}
fn same_keys(owned: &[TrackedStateKey], requested: &[TrackedStateKeyRef<'_>]) -> bool {
    owned.len() == requested.len()
        && owned.iter().zip(requested).all(|(a, b)| {
            a.schema_key == b.schema_key
                && a.file_id.as_deref() == b.file_id
                && &a.row_pk == b.row_pk
        })
}
impl Cache {
    pub(super) fn get(
        &self,
        base: CommitId,
        keys: &[TrackedStateKeyRef<'_>],
        projection: ChangeRecordProjection,
    ) -> Option<Arc<MaterializedTrackedStateExactBatch>> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = entries.resident.iter().position(|entry| {
            entry.base == base && entry.projection == projection && same_keys(&entry.keys, keys)
        })?;
        let entry = entries.resident.remove(index);
        let result = entry.batch.clone();
        entries.resident.insert(0, entry);
        Some(result)
    }
    pub(super) fn insert(
        &self,
        base: CommitId,
        keys: &[TrackedStateKeyRef<'_>],
        projection: ChangeRecordProjection,
        batch: Arc<MaterializedTrackedStateExactBatch>,
    ) {
        let Some(bytes) = admission_bytes(keys, &batch).filter(|bytes| *bytes <= MAX_BYTES) else {
            return;
        };
        let keys = keys
            .iter()
            .map(|key| TrackedStateKey {
                schema_key: key.schema_key.to_owned(),
                file_id: key.file_id.map(str::to_owned),
                row_pk: key.row_pk.clone(),
            })
            .collect();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.resident.insert(
            0,
            Entry {
                base,
                keys,
                projection,
                batch,
                bytes,
            },
        );
        entries.bytes += bytes;
        while entries.resident.len() > MAX_ENTRIES || entries.bytes > MAX_BYTES {
            if let Some(entry) = entries.resident.pop() {
                entries.bytes -= entry.bytes;
            }
        }
    }
}
/// Conservative retained-payload admission estimate, not allocator/RSS
/// metering. Duplicate/shared rows are charged per requested slot. Populate a
/// typed row's durable encoding now so a later read cannot grow that cache
/// owner outside this charge. Encoding failure merely bypasses this cache.
fn admission_bytes(
    keys: &[TrackedStateKeyRef<'_>],
    batch: &MaterializedTrackedStateExactBatch,
) -> Option<usize> {
    let mut bytes = size_of::<Entry>();
    for key in keys {
        bytes = bytes
            .checked_add(size_of::<TrackedStateKey>())?
            .checked_add(key.schema_key.len())?
            .checked_add(key.file_id.map_or(0, str::len))?
            .checked_add(key.row_pk.estimated_heap_bytes())?;
    }
    for index in 0..batch.len() {
        bytes = bytes.checked_add(size_of::<Option<u32>>())?;
        if let Some(row) = batch.row(index) {
            bytes = bytes
                .checked_add(256)?
                .checked_add(row.schema_key().len())?
                .checked_add(row.file_id().map_or(0, str::len))?
                .checked_add(row.row_pk().estimated_heap_bytes())?
                .checked_add(row.snapshot_content().map_or(0, |value| value.len()))?
                .checked_add(row.metadata().map_or(0, |value| value.len()))?;
            if let Some(typed) = row.decoded_snapshot() {
                bytes = bytes
                    .checked_add(usize::try_from(typed.estimated_size()).ok()?)?
                    .checked_add(typed.durable_payload().ok()?.len())?;
            }
        }
        if bytes > MAX_BYTES {
            return None;
        }
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_pk::RowPk;
    fn key(name: &str) -> TrackedStateKey {
        TrackedStateKey {
            schema_key: "lix_key_value".into(),
            file_id: None,
            row_pk: RowPk::single(name),
        }
    }
    fn refs(keys: &[TrackedStateKey]) -> Vec<TrackedStateKeyRef<'_>> {
        keys.iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            })
            .collect()
    }
    fn missing(slots: usize) -> Arc<MaterializedTrackedStateExactBatch> {
        Arc::new(
            MaterializedTrackedStateExactBatch::new(Default::default(), vec![None; slots]).unwrap(),
        )
    }
    #[test]
    fn exact_cache_separates_root_projection_order_and_negative_alignment() {
        let cache = Cache::default();
        let base = CommitId::for_test_label("exact-base");
        let keys = [key("a"), key("b"), key("a")];
        cache.insert(
            base,
            &refs(&keys),
            ChangeRecordProjection::full(),
            missing(3),
        );
        let cached = cache
            .get(base, &refs(&keys), ChangeRecordProjection::full())
            .unwrap();
        assert_eq!(cached.len(), 3);
        assert!((0..3).all(|index| cached.row(index).is_none()));
        assert!(
            cache
                .get(
                    CommitId::for_test_label("other"),
                    &refs(&keys),
                    ChangeRecordProjection::full()
                )
                .is_none()
        );
        assert!(
            cache
                .get(base, &refs(&keys), ChangeRecordProjection::identity_only())
                .is_none()
        );
        assert!(
            cache
                .get(
                    base,
                    &refs(&[key("b"), key("a"), key("a")]),
                    ChangeRecordProjection::full()
                )
                .is_none()
        );
    }
    #[test]
    fn exact_cache_caps_entries_and_negative_key_bytes() {
        let cache = Cache::default();
        let base = CommitId::for_test_label("exact-base");
        for index in 0..MAX_ENTRIES + 1 {
            cache.insert(
                base,
                &refs(&[key(&index.to_string())]),
                ChangeRecordProjection::identity_only(),
                missing(1),
            );
        }
        assert!(
            cache
                .get(
                    base,
                    &refs(&[key("0")]),
                    ChangeRecordProjection::identity_only()
                )
                .is_none()
        );
        let huge = [key(&"x".repeat(MAX_BYTES))];
        cache.insert(
            base,
            &refs(&huge),
            ChangeRecordProjection::identity_only(),
            missing(1),
        );
        assert!(
            cache
                .get(base, &refs(&huge), ChangeRecordProjection::identity_only())
                .is_none()
        );
        assert!(cache.entries.lock().unwrap().bytes <= MAX_BYTES);
    }
    #[tokio::test]
    async fn cached_native_candidate_still_obeys_new_current_collection_control() {
        use super::super::*;
        let authority = crate::open_lix().await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('cached-candidate', 'before')",
                &[],
            )
            .await
            .unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let branch = descriptor.selected_branch.branch_id;
        let base = CommitId::parse(&descriptor.selected_branch.head.commit_id).unwrap();
        let generation = CommitId::for_test_label("exact-cache-generation");
        let storage = authority.storage_adapter();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        HotStateWriter {
            store: &read,
            writes: &mut writes,
            transaction_global_schema_keys: None,
        }
        .stage_root_current_base(&branch, generation, base);
        drop(read);
        storage
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let cache = RootBaseBatchCache::default();
        let keys = [key("cached-candidate")];
        let read = storage.begin_read(Default::default()).await.unwrap();
        let first = load_root_current_base_exact(
            &read,
            &branch,
            generation,
            None,
            &refs(&keys),
            ChangeRecordProjection::full(),
            Some(&cache),
        )
        .await
        .unwrap();
        assert!(first.row(0).is_some());
        let raw = cache
            .exact
            .get(base, &refs(&keys), ChangeRecordProjection::full())
            .unwrap();
        assert!(raw.row(0).is_some());
        drop(read);
        let mut writes = storage.new_write_set();
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: "lix_key_value",
            file_id: None,
        };
        writes.put(
            COLLECTION_CONTROL_SPACE,
            hot_collection_control_key(&branch, generation, scope),
            storage_codec::encode(
                "hot collection control",
                &HotCollectionControl {
                    active_generation: CommitId::for_test_label("replacement-generation"),
                    live_count: 0,
                    ordered_identity_digest: None,
                },
            )
            .unwrap(),
        );
        storage
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let current = load_root_current_base_exact(
            &read,
            &branch,
            generation,
            None,
            &refs(&keys),
            ChangeRecordProjection::full(),
            Some(&cache),
        )
        .await
        .unwrap();
        assert!(
            current.row(0).is_none(),
            "current collection replacement must filter a cached old native candidate"
        );
        assert!(
            cache
                .exact
                .get(base, &refs(&keys), ChangeRecordProjection::full())
                .unwrap()
                .row(0)
                .is_some(),
            "cache stores raw immutable candidate, never filtered current visibility"
        );
    }
}
