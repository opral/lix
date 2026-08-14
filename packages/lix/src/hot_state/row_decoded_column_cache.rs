//! Repository-owned decoded columns for immutable columnar row groups.
//!
//! Entries are addressed only by persisted content. Visibility artifacts
//! (providers, overlays, coordinate masks, and transaction state) deliberately
//! live above this cache and are never retained here.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, ArrayRef};
use tokio::sync::watch;

use crate::LixError;
use crate::columnar_row_group::{RowGroupManifest, RowGroupSetId};

const DECODED_COLUMN_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;
const DECODED_COLUMN_CACHE_MAX_ENTRIES: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct DecodedColumnKey {
    row_groups: RowGroupSetId,
    manifest_digest: [u8; 32],
    group_index: usize,
    column_index: usize,
}

struct CacheState {
    entries: HashMap<DecodedColumnKey, (ArrayRef, usize)>,
    insertion_order: VecDeque<DecodedColumnKey>,
    bytes: usize,
    in_flight: HashSet<DecodedColumnKey>,
    generation: u64,
}

struct CacheInner {
    state: Mutex<CacheState>,
    changes: watch::Sender<u64>,
    max_bytes: usize,
    max_entries: usize,
    array_budget: Arc<crate::hot_state::RowColumnarArrayBudget>,
    #[cfg(test)]
    waiter_observed: tokio::sync::Notify,
}

impl Drop for CacheInner {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (array, _) in state.entries.values() {
            self.array_budget.release(std::slice::from_ref(array));
        }
        state.bytes = 0;
    }
}

#[derive(Clone)]
pub(crate) struct RowDecodedColumnCache {
    inner: Arc<CacheInner>,
}

impl Default for RowDecodedColumnCache {
    fn default() -> Self {
        Self::with_limits(
            DECODED_COLUMN_CACHE_MAX_BYTES,
            DECODED_COLUMN_CACHE_MAX_ENTRIES,
        )
    }
}

impl RowDecodedColumnCache {
    fn with_limits(max_bytes: usize, max_entries: usize) -> Self {
        Self::with_limits_and_budget(
            max_bytes,
            max_entries,
            Arc::new(crate::hot_state::RowColumnarArrayBudget::new(max_bytes)),
        )
    }

    pub(crate) fn with_array_budget(
        array_budget: Arc<crate::hot_state::RowColumnarArrayBudget>,
    ) -> Self {
        Self::with_limits_and_budget(
            DECODED_COLUMN_CACHE_MAX_BYTES,
            DECODED_COLUMN_CACHE_MAX_ENTRIES,
            array_budget,
        )
    }

    fn with_limits_and_budget(
        max_bytes: usize,
        max_entries: usize,
        array_budget: Arc<crate::hot_state::RowColumnarArrayBudget>,
    ) -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(CacheInner {
                state: Mutex::new(CacheState {
                    entries: HashMap::new(),
                    insertion_order: VecDeque::new(),
                    bytes: 0,
                    in_flight: HashSet::new(),
                    generation: 0,
                }),
                changes,
                max_bytes,
                max_entries,
                array_budget,
                #[cfg(test)]
                waiter_observed: tokio::sync::Notify::new(),
            }),
        }
    }

    pub(crate) async fn load_projection<S>(
        &self,
        store: &S,
        id: RowGroupSetId,
        manifest_digest: [u8; 32],
        manifest: &RowGroupManifest,
        group_index: usize,
        projection: &[usize],
    ) -> Result<Vec<ArrayRef>, LixError>
    where
        S: crate::storage_adapter::StorageAdapterRead + ?Sized,
    {
        self.load_projection_with(
            id,
            manifest_digest,
            group_index,
            projection,
            |claimed| async move {
                let arrays = crate::columnar_row_group::load_row_group_columns(
                    store,
                    id,
                    manifest,
                    group_index,
                    &claimed,
                )
                .await?;
                Ok(claimed.into_iter().zip(arrays).collect())
            },
        )
        .await
    }

    async fn load_projection_with<F, Fut>(
        &self,
        id: RowGroupSetId,
        manifest_digest: [u8; 32],
        group_index: usize,
        projection: &[usize],
        mut load: F,
    ) -> Result<Vec<ArrayRef>, LixError>
    where
        F: FnMut(Vec<usize>) -> Fut,
        Fut: Future<Output = Result<Vec<(usize, ArrayRef)>, LixError>>,
    {
        let mut resolved = HashMap::<usize, ArrayRef>::new();
        loop {
            let (claimed, mut changed) = {
                let mut state = self.lock()?;
                let mut missing = Vec::new();
                for &column_index in projection {
                    if resolved.contains_key(&column_index) {
                        continue;
                    }
                    let key = DecodedColumnKey {
                        row_groups: id,
                        manifest_digest,
                        group_index,
                        column_index,
                    };
                    if let Some((array, _)) = state.entries.get(&key) {
                        resolved.insert(column_index, Arc::clone(array));
                    } else {
                        missing.push(key);
                    }
                }
                if missing.is_empty() {
                    return projection
                        .iter()
                        .map(|index| {
                            resolved.get(index).cloned().ok_or_else(|| {
                                cache_error("decoded-column projection assembly lost a column")
                            })
                        })
                        .collect();
                }
                let claimed = missing
                    .into_iter()
                    .filter(|key| state.in_flight.insert(*key))
                    .collect::<Vec<_>>();
                (claimed, self.inner.changes.subscribe())
            };

            if claimed.is_empty() {
                #[cfg(test)]
                self.inner.waiter_observed.notify_waiters();
                changed
                    .changed()
                    .await
                    .map_err(|_| cache_error("decoded-column cache coordination channel closed"))?;
                continue;
            }

            let mut claim = ColumnLoadClaim::new(Arc::clone(&self.inner), claimed);
            let claimed_indices = claim
                .keys
                .iter()
                .map(|key| key.column_index)
                .collect::<Vec<_>>();
            let loaded = load(claimed_indices).await?;
            let expected = claim
                .keys
                .iter()
                .map(|key| key.column_index)
                .collect::<HashSet<_>>();
            if loaded.len() != expected.len()
                || loaded.iter().any(|(index, _)| !expected.contains(index))
                || loaded
                    .iter()
                    .map(|(index, _)| *index)
                    .collect::<HashSet<_>>()
                    .len()
                    != loaded.len()
            {
                return Err(cache_error(
                    "decoded-column loader returned a mismatched projection",
                ));
            }
            for (index, array) in &loaded {
                resolved.insert(*index, Arc::clone(array));
            }
            claim.publish(loaded)?;
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, CacheState>, LixError> {
        self.inner
            .state
            .lock()
            .map_err(|_| cache_error("decoded-column cache lock poisoned"))
    }
}

struct ColumnLoadClaim {
    inner: Arc<CacheInner>,
    keys: Vec<DecodedColumnKey>,
    finished: bool,
}

impl ColumnLoadClaim {
    fn new(inner: Arc<CacheInner>, keys: Vec<DecodedColumnKey>) -> Self {
        Self {
            inner,
            keys,
            finished: false,
        }
    }

    fn publish(&mut self, loaded: Vec<(usize, ArrayRef)>) -> Result<(), LixError> {
        let arrays = loaded.into_iter().collect::<HashMap<_, _>>();
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| cache_error("decoded-column cache lock poisoned"))?;
        for key in &self.keys {
            let array = arrays
                .get(&key.column_index)
                .expect("loader projection was validated");
            let bytes = array.get_array_memory_size();
            if bytes <= self.inner.max_bytes && self.inner.max_entries != 0 {
                while state.entries.len() >= self.inner.max_entries
                    || state.bytes.saturating_add(bytes) > self.inner.max_bytes
                {
                    let Some(evicted) = state.insertion_order.pop_front() else {
                        break;
                    };
                    if let Some((evicted_array, evicted_bytes)) = state.entries.remove(&evicted) {
                        state.bytes = state.bytes.saturating_sub(evicted_bytes);
                        self.inner
                            .array_budget
                            .release(std::slice::from_ref(&evicted_array));
                    }
                }
                let mut reserved = self
                    .inner
                    .array_budget
                    .try_reserve(std::slice::from_ref(array));
                while !reserved {
                    let Some(evicted) = state.insertion_order.pop_front() else {
                        break;
                    };
                    if let Some((evicted_array, evicted_bytes)) = state.entries.remove(&evicted) {
                        state.bytes = state.bytes.saturating_sub(evicted_bytes);
                        self.inner
                            .array_budget
                            .release(std::slice::from_ref(&evicted_array));
                    }
                    reserved = self
                        .inner
                        .array_budget
                        .try_reserve(std::slice::from_ref(array));
                }
                if reserved {
                    state.bytes = state.bytes.saturating_add(bytes);
                    state.insertion_order.push_back(*key);
                    state.entries.insert(*key, (Arc::clone(array), bytes));
                }
            }
            state.in_flight.remove(key);
        }
        state.generation = state.generation.wrapping_add(1);
        self.inner.changes.send_replace(state.generation);
        self.finished = true;
        Ok(())
    }
}

impl Drop for ColumnLoadClaim {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in &self.keys {
            state.in_flight.remove(key);
        }
        state.generation = state.generation.wrapping_add(1);
        self.inner.changes.send_replace(state.generation);
    }
}

fn cache_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use tokio::sync::Notify;

    use super::*;

    struct CountingRead<R> {
        inner: R,
        column_reads: Arc<Mutex<Vec<usize>>>,
    }

    impl<R> crate::storage_adapter::StorageAdapterRead for CountingRead<R>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> impl Future<
            Output = Result<crate::storage::GetManyResult, crate::storage::StorageError>,
        > + Send {
            for request in requests {
                if request.space == crate::columnar_row_group::ROW_GROUP_COLUMN_SPACE {
                    self.column_reads
                        .lock()
                        .expect("column reads lock")
                        .push(request.keys.len());
                }
            }
            self.inner.get_many(requests)
        }

        fn begin_scan(
            &self,
            space: crate::storage::StorageSpace,
            range: crate::storage::KeyRange,
            opts: crate::storage::BeginScanOptions,
        ) -> impl Future<
            Output = Result<crate::storage::ScanCursor<'_>, crate::storage::StorageError>,
        > + Send {
            self.inner.begin_scan(space, range, opts)
        }
    }

    fn array(column: usize) -> ArrayRef {
        Arc::new(Int64Array::from(vec![column as i64; 8]))
    }

    fn loaded(columns: Vec<usize>) -> Vec<(usize, ArrayRef)> {
        columns
            .into_iter()
            .map(|column| (column, array(column)))
            .collect()
    }

    #[tokio::test]
    async fn storage_reads_only_new_columns_and_reuses_decoded_arcs() {
        use crate::storage_adapter::{
            Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![array(10), array(20), array(30)],
        )
        .expect("row-group batch");
        let encoded = crate::columnar_row_group::encode_row_group_set(
            "decoded-cache-storage-test",
            schema,
            &[batch],
        )
        .expect("encode row group");
        let id = RowGroupSetId::new(*b"decoded-cache-01");
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        crate::columnar_row_group::stage_row_group_set(&mut writes, id, &encoded)
            .expect("stage row group");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit row group");
        let column_reads = Arc::new(Mutex::new(Vec::new()));
        let read = CountingRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin read"),
            column_reads: Arc::clone(&column_reads),
        };
        let cache = RowDecodedColumnCache::default();
        let digest = encoded.manifest.content_digest().expect("manifest digest");

        let first = cache
            .load_projection(&read, id, digest, &encoded.manifest, 0, &[1, 0])
            .await
            .expect("first projection");
        let second = cache
            .load_projection(&read, id, digest, &encoded.manifest, 0, &[0, 2, 1])
            .await
            .expect("overlapping projection");

        assert_eq!(*column_reads.lock().expect("column reads lock"), vec![2, 1]);
        assert!(Arc::ptr_eq(&first[1], &second[0]));
        assert!(Arc::ptr_eq(&first[0], &second[2]));
        assert_eq!(
            second
                .iter()
                .map(|value| {
                    value
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("int array")
                        .value(0)
                })
                .collect::<Vec<_>>(),
            vec![10, 30, 20]
        );
    }

    #[tokio::test]
    async fn empty_projection_never_invokes_loader() {
        let cache = RowDecodedColumnCache::default();
        let loads = AtomicUsize::new(0);
        let arrays = cache
            .load_projection_with(RowGroupSetId::new([16; 16]), [17; 32], 0, &[], |_columns| {
                loads.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(Vec::new()))
            })
            .await
            .expect("empty projection");
        assert!(arrays.is_empty());
        assert_eq!(loads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn overlapping_ordered_projections_reuse_array_identity() {
        let cache = RowDecodedColumnCache::default();
        let loads = Arc::new(Mutex::new(Vec::<Vec<usize>>::new()));
        let first = cache
            .load_projection_with(RowGroupSetId::new([1; 16]), [2; 32], 3, &[2, 0], {
                let loads = Arc::clone(&loads);
                move |columns| {
                    loads.lock().expect("loads lock").push(columns.clone());
                    std::future::ready(Ok(loaded(columns)))
                }
            })
            .await
            .expect("first projection");
        let second = cache
            .load_projection_with(RowGroupSetId::new([1; 16]), [2; 32], 3, &[0, 1, 2], {
                let loads = Arc::clone(&loads);
                move |columns| {
                    loads.lock().expect("loads lock").push(columns.clone());
                    std::future::ready(Ok(loaded(columns)))
                }
            })
            .await
            .expect("overlapping projection");

        assert_eq!(
            *loads.lock().expect("loads lock"),
            vec![vec![2, 0], vec![1]]
        );
        assert!(Arc::ptr_eq(&first[1], &second[0]));
        assert!(Arc::ptr_eq(&first[0], &second[2]));
        assert_eq!(
            second
                .iter()
                .map(|value| {
                    value
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("int array")
                        .value(0)
                })
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[tokio::test]
    async fn concurrent_overlaps_claim_each_missing_column_once() {
        let cache = RowDecodedColumnCache::default();
        let first_started = Arc::new(tokio::sync::Barrier::new(2));
        let release_first = Arc::new(Notify::new());
        let first_loads = Arc::new(Mutex::new(Vec::new()));
        let second_loads = Arc::new(Mutex::new(Vec::new()));

        let first_task = tokio::spawn({
            let cache = cache.clone();
            let started = Arc::clone(&first_started);
            let release = Arc::clone(&release_first);
            let loads = Arc::clone(&first_loads);
            async move {
                cache
                    .load_projection_with(
                        RowGroupSetId::new([3; 16]),
                        [4; 32],
                        0,
                        &[0, 1],
                        move |columns| {
                            let started = Arc::clone(&started);
                            let release = Arc::clone(&release);
                            let loads = Arc::clone(&loads);
                            async move {
                                loads.lock().expect("loads lock").push(columns.clone());
                                started.wait().await;
                                release.notified().await;
                                Ok(loaded(columns))
                            }
                        },
                    )
                    .await
            }
        });
        first_started.wait().await;
        let waiter_observed = cache.inner.waiter_observed.notified();
        let second_task = tokio::spawn({
            let cache = cache.clone();
            let loads = Arc::clone(&second_loads);
            async move {
                cache
                    .load_projection_with(
                        RowGroupSetId::new([3; 16]),
                        [4; 32],
                        0,
                        &[1, 2],
                        move |columns| {
                            loads.lock().expect("loads lock").push(columns.clone());
                            std::future::ready(Ok(loaded(columns)))
                        },
                    )
                    .await
            }
        });
        waiter_observed.await;
        release_first.notify_one();
        first_task.await.expect("first join").expect("first load");
        second_task
            .await
            .expect("second join")
            .expect("second load");

        assert_eq!(*first_loads.lock().expect("loads lock"), vec![vec![0, 1]]);
        assert_eq!(*second_loads.lock().expect("loads lock"), vec![vec![2]]);
    }

    #[tokio::test]
    async fn waiters_retry_after_owner_error_and_cancellation() {
        let cache = RowDecodedColumnCache::default();
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(Notify::new());
        let owner = tokio::spawn({
            let cache = cache.clone();
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                cache
                    .load_projection_with(
                        RowGroupSetId::new([5; 16]),
                        [6; 32],
                        0,
                        &[0],
                        move |_columns| {
                            let started = Arc::clone(&started);
                            let release = Arc::clone(&release);
                            async move {
                                started.wait().await;
                                release.notified().await;
                                Err(cache_error("corrupt or missing test column"))
                            }
                        },
                    )
                    .await
            }
        });
        started.wait().await;
        let waiter_observed = cache.inner.waiter_observed.notified();
        let error_waiter = tokio::spawn({
            let cache = cache.clone();
            async move {
                cache
                    .load_projection_with(
                        RowGroupSetId::new([5; 16]),
                        [6; 32],
                        0,
                        &[0],
                        |columns| std::future::ready(Ok(loaded(columns))),
                    )
                    .await
            }
        });
        waiter_observed.await;
        release.notify_one();
        let error = owner
            .await
            .expect("owner join")
            .expect_err("failure must surface");
        assert!(error.to_string().contains("corrupt or missing"));
        tokio::time::timeout(std::time::Duration::from_secs(1), error_waiter)
            .await
            .expect("error waiter must wake")
            .expect("error waiter join")
            .expect("error waiter retry");

        let cancel_started = Arc::new(tokio::sync::Barrier::new(2));
        let never = Arc::new(Notify::new());
        let cancelled_owner = tokio::spawn({
            let cache = cache.clone();
            let started = Arc::clone(&cancel_started);
            let never = Arc::clone(&never);
            async move {
                cache
                    .load_projection_with(
                        RowGroupSetId::new([7; 16]),
                        [8; 32],
                        0,
                        &[0],
                        move |_columns| {
                            let started = Arc::clone(&started);
                            let never = Arc::clone(&never);
                            async move {
                                started.wait().await;
                                never.notified().await;
                                Ok(Vec::new())
                            }
                        },
                    )
                    .await
            }
        });
        cancel_started.wait().await;
        let waiter_observed = cache.inner.waiter_observed.notified();
        let cancel_waiter = tokio::spawn({
            let cache = cache.clone();
            async move {
                cache
                    .load_projection_with(
                        RowGroupSetId::new([7; 16]),
                        [8; 32],
                        0,
                        &[0],
                        |columns| std::future::ready(Ok(loaded(columns))),
                    )
                    .await
            }
        });
        waiter_observed.await;
        cancelled_owner.abort();
        let _ = cancelled_owner.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), cancel_waiter)
            .await
            .expect("cancellation waiter must wake")
            .expect("cancellation waiter join")
            .expect("cancellation waiter retry");
    }

    #[tokio::test]
    async fn digest_entry_byte_and_oversize_limits_are_enforced() {
        let bytes = array(0).get_array_memory_size();
        let entry_cache = RowDecodedColumnCache::with_limits(usize::MAX, 2);
        let entry_loads = Arc::new(AtomicUsize::new(0));
        for projection in [&[0, 1][..], &[2][..], &[0][..]] {
            entry_cache
                .load_projection_with(RowGroupSetId::new([7; 16]), [8; 32], 0, projection, {
                    let loads = Arc::clone(&entry_loads);
                    move |columns| {
                        loads.fetch_add(columns.len(), Ordering::SeqCst);
                        std::future::ready(Ok(loaded(columns)))
                    }
                })
                .await
                .expect("entry-bounded load");
        }
        assert_eq!(entry_loads.load(Ordering::SeqCst), 4);

        let byte_cache = RowDecodedColumnCache::with_limits(bytes, 8);
        byte_cache
            .load_projection_with(
                RowGroupSetId::new([9; 16]),
                [10; 32],
                0,
                &[0, 1],
                |columns| std::future::ready(Ok(loaded(columns))),
            )
            .await
            .expect("byte-bounded load");
        assert_eq!(byte_cache.lock().expect("cache lock").entries.len(), 1);

        let oversize = RowDecodedColumnCache::with_limits(bytes - 1, 8);
        let oversize_loads = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            oversize
                .load_projection_with(RowGroupSetId::new([11; 16]), [12; 32], 0, &[0], {
                    let loads = Arc::clone(&oversize_loads);
                    move |columns| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(loaded(columns)))
                    }
                })
                .await
                .expect("oversize result remains usable");
        }
        assert_eq!(oversize_loads.load(Ordering::SeqCst), 2);
        assert!(oversize.lock().expect("cache lock").entries.is_empty());

        let digest_cache = RowDecodedColumnCache::default();
        let digest_loads = Arc::new(AtomicUsize::new(0));
        for digest in [[13; 32], [14; 32]] {
            digest_cache
                .load_projection_with(RowGroupSetId::new([15; 16]), digest, 0, &[0], {
                    let loads = Arc::clone(&digest_loads);
                    move |columns| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(loaded(columns)))
                    }
                })
                .await
                .expect("digest-specific load");
        }
        assert_eq!(digest_loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shared_budget_bounds_exact_batch_and_decoded_layers() {
        let resident_array = array(0);
        let bytes = resident_array.get_array_memory_size();
        let budget = Arc::new(crate::hot_state::RowColumnarArrayBudget::new(bytes));
        let mut exact =
            crate::hot_state::RowColumnarShadowMaskCache::with_array_budget(Arc::clone(&budget));
        let shadow = crate::hot_state::RowColumnarShadowMaskKey {
            row_groups: RowGroupSetId::new([18; 16]),
            branch_id: Arc::from("main"),
            head_commit_id: crate::changelog::CommitId::for_test_label(
                "shared-columnar-budget-head",
            ),
            current_state_revision: 1,
            shadow_identity_digest: [19; 32],
            group_index: 0,
        };
        let exact_batch = Arc::new(
            datafusion::arrow::record_batch::RecordBatch::try_from_iter([(
                "v",
                Arc::clone(&resident_array),
            )])
            .expect("exact batch"),
        );
        exact.insert_batch(shadow.clone(), vec![0], exact_batch);

        let decoded =
            RowDecodedColumnCache::with_limits_and_budget(usize::MAX, 8, Arc::clone(&budget));
        let decoded_loads = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            decoded
                .load_projection_with(RowGroupSetId::new([18; 16]), [20; 32], 0, &[1], {
                    let loads = Arc::clone(&decoded_loads);
                    let resident_array = Arc::clone(&resident_array);
                    move |columns| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(columns, vec![1]);
                        std::future::ready(Ok(vec![(1, Arc::clone(&resident_array))]))
                    }
                })
                .await
                .expect("shared immutable array remains reusable");
        }

        assert_eq!(budget.used(), bytes);
        assert_eq!(decoded_loads.load(Ordering::SeqCst), 1);
        assert_eq!(decoded.lock().expect("decoded cache lock").entries.len(), 1);
        assert!(exact.batch(&shadow, &[0]).is_some());
        drop(exact);
        assert_eq!(budget.used(), bytes);
        drop(decoded);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn dropping_one_cache_releases_capacity_to_its_sibling() {
        let bytes = array(0).get_array_memory_size();
        let budget = Arc::new(crate::hot_state::RowColumnarArrayBudget::new(bytes));
        {
            let mut exact = crate::hot_state::RowColumnarShadowMaskCache::with_array_budget(
                Arc::clone(&budget),
            );
            exact.insert_batch(
                crate::hot_state::RowColumnarShadowMaskKey {
                    row_groups: RowGroupSetId::new([21; 16]),
                    branch_id: Arc::from("main"),
                    head_commit_id: crate::changelog::CommitId::for_test_label(
                        "released-columnar-budget-head",
                    ),
                    current_state_revision: 1,
                    shadow_identity_digest: [22; 32],
                    group_index: 0,
                },
                vec![0],
                Arc::new(
                    datafusion::arrow::record_batch::RecordBatch::try_from_iter([("v", array(0))])
                        .expect("exact batch"),
                ),
            );
            assert_eq!(budget.used(), bytes);
        }
        assert_eq!(budget.used(), 0);

        let decoded =
            RowDecodedColumnCache::with_limits_and_budget(usize::MAX, 8, Arc::clone(&budget));
        let loads = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            decoded
                .load_projection_with(RowGroupSetId::new([21; 16]), [23; 32], 0, &[0], {
                    let loads = Arc::clone(&loads);
                    move |columns| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(loaded(columns)))
                    }
                })
                .await
                .expect("released capacity admits sibling entry");
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(budget.used(), bytes);
        drop(decoded);
        assert_eq!(budget.used(), 0);
    }
}
