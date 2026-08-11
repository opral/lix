//! Repository-scoped reconciliation artifacts for columnar row scans.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Statistics;

const SHADOW_MASK_CACHE_ENTRIES: usize = 256;
const STATISTICS_PROJECTIONS_PER_MASK: usize = 8;
const RECONCILED_BATCH_CACHE_ENTRIES: usize = 256;
const RECONCILED_BATCH_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// One repository-wide budget for immutable and reconciled Arrow arrays.
/// Exact-batch and decoded-column entries often clone the same immutable
/// `ArrayRef`. Pointer identity plus reference counts charges that allocation
/// once; independently constructed (including sliced or filtered) arrays are
/// conservatively separate allocations.
pub(crate) struct RowColumnarArrayBudget {
    state: Mutex<RowColumnarArrayBudgetState>,
    max: usize,
}

#[derive(Default)]
struct RowColumnarArrayBudgetState {
    used: usize,
    arrays: HashMap<usize, (usize, usize)>,
}

impl Default for RowColumnarArrayBudget {
    fn default() -> Self {
        Self::new(RECONCILED_BATCH_CACHE_MAX_BYTES)
    }
}

impl RowColumnarArrayBudget {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            state: Mutex::new(RowColumnarArrayBudgetState::default()),
            max,
        }
    }

    pub(crate) fn try_reserve(&self, arrays: &[datafusion::arrow::array::ArrayRef]) -> bool {
        let mut requested = HashMap::<usize, (usize, usize)>::new();
        for array in arrays {
            let identity = Arc::as_ptr(array) as *const () as usize;
            let entry = requested
                .entry(identity)
                .or_insert((array.get_array_memory_size(), 0));
            let Some(references) = entry.1.checked_add(1) else {
                return false;
            };
            entry.1 = references;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(additional) = requested
            .iter()
            .filter(|(identity, _)| !state.arrays.contains_key(identity))
            .map(|(_, (bytes, _))| *bytes)
            .try_fold(0_usize, usize::checked_add)
        else {
            return false;
        };
        let Some(next_used) = state.used.checked_add(additional) else {
            return false;
        };
        if next_used > self.max
            || requested.iter().any(|(identity, (bytes, references))| {
                state.arrays.get(identity).is_some_and(|resident| {
                    resident.0 != *bytes || resident.1.checked_add(*references).is_none()
                })
            })
        {
            return false;
        }
        state.used = next_used;
        for (identity, (bytes, references)) in requested {
            let resident = state.arrays.entry(identity).or_insert((bytes, 0));
            resident.1 = resident
                .1
                .checked_add(references)
                .expect("reference-count overflow was preflighted");
        }
        true
    }

    pub(crate) fn release(&self, arrays: &[datafusion::arrow::array::ArrayRef]) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for array in arrays {
            let identity = Arc::as_ptr(array) as *const () as usize;
            let Some((bytes, references)) = state.arrays.get_mut(&identity) else {
                debug_assert!(false, "released an unreserved columnar array");
                continue;
            };
            debug_assert!(*references > 0, "columnar array reference underflow");
            *references = references.saturating_sub(1);
            if *references == 0 {
                let bytes = *bytes;
                state.arrays.remove(&identity);
                state.used = state.used.saturating_sub(bytes);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .used
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RowColumnarBatchKey {
    shadow: RowColumnarShadowMaskKey,
    projection: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowColumnarShadowMaskKey {
    pub(crate) row_groups: crate::columnar_row_group::RowGroupSetId,
    pub(crate) branch_id: Arc<str>,
    pub(crate) head_commit_id: crate::changelog::CommitId,
    pub(crate) current_state_revision: u64,
    pub(crate) shadow_identity_digest: [u8; 32],
    pub(crate) group_index: usize,
}

pub(crate) struct RowColumnarShadowMaskCache {
    entries: HashMap<RowColumnarShadowMaskKey, RowColumnarShadowCacheEntry>,
    insertion_order: VecDeque<RowColumnarShadowMaskKey>,
    batches: HashMap<RowColumnarBatchKey, (Arc<RecordBatch>, usize)>,
    batch_insertion_order: VecDeque<RowColumnarBatchKey>,
    batch_bytes: usize,
    array_budget: Arc<RowColumnarArrayBudget>,
}

impl Default for RowColumnarShadowMaskCache {
    fn default() -> Self {
        Self::with_array_budget(Arc::new(RowColumnarArrayBudget::default()))
    }
}

impl Drop for RowColumnarShadowMaskCache {
    fn drop(&mut self) {
        for (batch, _) in self.batches.values() {
            self.array_budget.release(batch.columns());
        }
        self.batch_bytes = 0;
    }
}

struct RowColumnarShadowCacheEntry {
    mask: Arc<BooleanArray>,
    statistics_by_projection: HashMap<Vec<usize>, Statistics>,
    statistics_insertion_order: VecDeque<Vec<usize>>,
}

impl RowColumnarShadowMaskCache {
    pub(crate) fn with_array_budget(array_budget: Arc<RowColumnarArrayBudget>) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            batches: HashMap::new(),
            batch_insertion_order: VecDeque::new(),
            batch_bytes: 0,
            array_budget,
        }
    }

    pub(crate) fn get(&self, key: &RowColumnarShadowMaskKey) -> Option<Arc<BooleanArray>> {
        self.entries.get(key).map(|entry| Arc::clone(&entry.mask))
    }

    pub(crate) fn insert(
        &mut self,
        key: RowColumnarShadowMaskKey,
        mask: Arc<BooleanArray>,
    ) -> Arc<BooleanArray> {
        if let Some(existing) = self.entries.get(&key) {
            return Arc::clone(&existing.mask);
        }
        while self.entries.len() >= SHADOW_MASK_CACHE_ENTRIES {
            if let Some(evicted) = self.insertion_order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
        self.insertion_order.push_back(key.clone());
        self.entries.insert(
            key,
            RowColumnarShadowCacheEntry {
                mask: Arc::clone(&mask),
                statistics_by_projection: HashMap::new(),
                statistics_insertion_order: VecDeque::new(),
            },
        );
        mask
    }

    pub(crate) fn statistics(
        &self,
        key: &RowColumnarShadowMaskKey,
        projection: &[usize],
    ) -> Option<Statistics> {
        self.entries
            .get(key)?
            .statistics_by_projection
            .get(projection)
            .cloned()
    }

    pub(crate) fn insert_statistics(
        &mut self,
        key: &RowColumnarShadowMaskKey,
        projection: Vec<usize>,
        statistics: Statistics,
    ) {
        if let Some(entry) = self.entries.get_mut(key) {
            if !entry.statistics_by_projection.contains_key(&projection) {
                while entry.statistics_by_projection.len() >= STATISTICS_PROJECTIONS_PER_MASK {
                    if let Some(evicted) = entry.statistics_insertion_order.pop_front() {
                        entry.statistics_by_projection.remove(&evicted);
                    }
                }
                entry
                    .statistics_insertion_order
                    .push_back(projection.clone());
            }
            entry
                .statistics_by_projection
                .insert(projection, statistics);
        }
    }

    pub(crate) fn batch(
        &self,
        key: &RowColumnarShadowMaskKey,
        projection: &[usize],
    ) -> Option<Arc<RecordBatch>> {
        self.batches
            .get(&RowColumnarBatchKey {
                shadow: key.clone(),
                projection: projection.to_vec(),
            })
            .map(|(batch, _)| Arc::clone(batch))
    }

    pub(crate) fn insert_batch(
        &mut self,
        key: RowColumnarShadowMaskKey,
        projection: Vec<usize>,
        batch: Arc<RecordBatch>,
    ) -> Arc<RecordBatch> {
        self.insert_batch_with_limits(
            key,
            projection,
            batch,
            RECONCILED_BATCH_CACHE_ENTRIES,
            RECONCILED_BATCH_CACHE_MAX_BYTES,
        )
    }

    fn insert_batch_with_limits(
        &mut self,
        key: RowColumnarShadowMaskKey,
        projection: Vec<usize>,
        batch: Arc<RecordBatch>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Arc<RecordBatch> {
        let key = RowColumnarBatchKey {
            shadow: key,
            projection,
        };
        if let Some((resident, _)) = self.batches.get(&key) {
            return Arc::clone(resident);
        }
        let bytes = batch
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        if bytes > max_bytes {
            return batch;
        }
        while self.batches.len() >= max_entries
            || self.batch_bytes.saturating_add(bytes) > max_bytes
        {
            let Some(evicted) = self.batch_insertion_order.pop_front() else {
                return batch;
            };
            if let Some((evicted_batch, evicted_bytes)) = self.batches.remove(&evicted) {
                self.batch_bytes = self.batch_bytes.saturating_sub(evicted_bytes);
                self.array_budget.release(evicted_batch.columns());
            }
        }
        while !self.array_budget.try_reserve(batch.columns()) {
            let Some(evicted) = self.batch_insertion_order.pop_front() else {
                return batch;
            };
            if let Some((evicted_batch, evicted_bytes)) = self.batches.remove(&evicted) {
                self.batch_bytes = self.batch_bytes.saturating_sub(evicted_bytes);
                self.array_budget.release(evicted_batch.columns());
            }
        }
        self.batch_bytes = self.batch_bytes.saturating_add(bytes);
        self.batch_insertion_order.push_back(key.clone());
        self.batches.insert(key, (Arc::clone(&batch), bytes));
        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn budget_array(values: usize) -> datafusion::arrow::array::ArrayRef {
        Arc::new(Int64Array::from(vec![1; values]))
    }

    #[test]
    fn shared_budget_charges_identical_arc_once_until_last_release() {
        let array = budget_array(8);
        let bytes = array.get_array_memory_size();
        let budget = RowColumnarArrayBudget::new(bytes);
        assert!(budget.try_reserve(&[Arc::clone(&array), Arc::clone(&array)]));
        assert_eq!(budget.used(), bytes);
        budget.release(std::slice::from_ref(&array));
        assert_eq!(budget.used(), bytes);
        budget.release(std::slice::from_ref(&array));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn shared_budget_treats_distinct_and_sliced_array_objects_separately() {
        let base = budget_array(8);
        let slice = base.slice(0, 4);
        let distinct = budget_array(8);
        let expected = base
            .get_array_memory_size()
            .saturating_add(slice.get_array_memory_size())
            .saturating_add(distinct.get_array_memory_size());
        let budget = RowColumnarArrayBudget::new(expected);
        assert!(budget.try_reserve(&[
            Arc::clone(&base),
            Arc::clone(&slice),
            Arc::clone(&distinct),
        ]));
        assert_eq!(budget.used(), expected);
        budget.release(&[base, slice, distinct]);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn failed_aggregate_reservation_is_atomic_and_concurrent_shares_dedupe() {
        let first = budget_array(8);
        let second = budget_array(8);
        let bytes = first.get_array_memory_size();
        let budget = Arc::new(RowColumnarArrayBudget::new(bytes));
        assert!(!budget.try_reserve(&[Arc::clone(&first), second]));
        assert_eq!(budget.used(), 0);

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let budget = Arc::clone(&budget);
                let array = Arc::clone(&first);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    assert!(budget.try_reserve(std::slice::from_ref(&array)));
                    array
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let arrays = handles
            .into_iter()
            .map(|handle| handle.join().expect("budget thread"))
            .collect::<Vec<_>>();
        assert_eq!(budget.used(), bytes);
        for array in arrays {
            budget.release(std::slice::from_ref(&array));
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn abstract_budget_and_reference_overflow_fail_without_mutation() {
        let array = budget_array(8);
        let bytes = array.get_array_memory_size();
        let identity = Arc::as_ptr(&array) as *const () as usize;
        let budget = RowColumnarArrayBudget::new(usize::MAX);
        assert!(budget.try_reserve(std::slice::from_ref(&array)));
        {
            let mut state = budget.state.lock().expect("budget lock");
            state.arrays.get_mut(&identity).expect("resident array").1 = usize::MAX;
        }
        assert!(!budget.try_reserve(std::slice::from_ref(&array)));
        {
            let mut state = budget.state.lock().expect("budget lock");
            assert_eq!(state.used, bytes);
            assert_eq!(state.arrays[&identity].1, usize::MAX);
            state.arrays.get_mut(&identity).expect("resident array").1 = 1;
        }
        budget.release(std::slice::from_ref(&array));

        let overflow = RowColumnarArrayBudget::new(usize::MAX);
        overflow.state.lock().expect("budget lock").used = usize::MAX;
        assert!(!overflow.try_reserve(std::slice::from_ref(&array)));
        assert_eq!(overflow.used(), usize::MAX);
    }

    #[test]
    fn statistics_projections_are_bounded_per_mask() {
        let key = RowColumnarShadowMaskKey {
            row_groups: crate::columnar_row_group::RowGroupSetId::new([7; 16]),
            branch_id: Arc::from("branch"),
            head_commit_id: crate::changelog::CommitId::for_test_label("scan-cache-head"),
            current_state_revision: 3,
            shadow_identity_digest: [9; 32],
            group_index: 1,
        };
        let mut cache = RowColumnarShadowMaskCache::default();
        cache.insert(key.clone(), Arc::new(BooleanArray::from(vec![true])));
        for projection in 0..(STATISTICS_PROJECTIONS_PER_MASK + 2) {
            let schema = Schema::new(vec![Field::new(
                format!("c{projection}"),
                DataType::Int64,
                true,
            )]);
            cache.insert_statistics(&key, vec![projection], Statistics::new_unknown(&schema));
        }

        let entry = cache.entries.get(&key).expect("mask remains cached");
        assert_eq!(
            entry.statistics_by_projection.len(),
            STATISTICS_PROJECTIONS_PER_MASK
        );
        assert!(!entry.statistics_by_projection.contains_key(&vec![0]));
        assert!(!entry.statistics_by_projection.contains_key(&vec![1]));
        assert!(entry.statistics_by_projection.contains_key(&vec![2]));
    }

    #[test]
    fn reconciled_batches_are_entry_bounded() {
        let shadow = RowColumnarShadowMaskKey {
            row_groups: crate::columnar_row_group::RowGroupSetId::new([3; 16]),
            branch_id: Arc::from("branch"),
            head_commit_id: crate::changelog::CommitId::for_test_label("batch-cache-head"),
            current_state_revision: 1,
            shadow_identity_digest: [4; 32],
            group_index: 0,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = Arc::new(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch"),
        );
        let mut cache = RowColumnarShadowMaskCache::default();
        for projection in 0..(RECONCILED_BATCH_CACHE_ENTRIES + 2) {
            cache.insert_batch(shadow.clone(), vec![projection], Arc::clone(&batch));
        }
        assert_eq!(cache.batches.len(), RECONCILED_BATCH_CACHE_ENTRIES);
        assert!(cache.batch_bytes <= RECONCILED_BATCH_CACHE_MAX_BYTES);
        assert!(cache.batch(&shadow, &[0]).is_none());
        assert!(cache.batch(&shadow, &[2]).is_some());
    }

    #[test]
    fn reconciled_batches_are_byte_bounded() {
        let shadow = batch_cache_key();
        let batch = test_batch(&[1, 2, 3, 4]);
        let bytes = batch
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let mut cache = RowColumnarShadowMaskCache::default();

        cache.insert_batch_with_limits(shadow.clone(), vec![0], Arc::clone(&batch), 8, bytes);
        cache.insert_batch_with_limits(shadow.clone(), vec![1], Arc::clone(&batch), 8, bytes);

        assert!(cache.batch(&shadow, &[0]).is_none());
        assert!(cache.batch(&shadow, &[1]).is_some());
        assert_eq!(cache.batch_bytes, bytes);

        let oversized = test_batch(&[5, 6, 7, 8, 9]);
        cache.insert_batch_with_limits(shadow.clone(), vec![2], oversized, 8, bytes);
        assert!(cache.batch(&shadow, &[2]).is_none());
        assert_eq!(cache.batch_bytes, bytes);
    }

    #[test]
    fn batch_cache_key_covers_every_visibility_and_projection_dimension() {
        let base = batch_cache_key();
        let batch = test_batch(&[7]);
        let mut cache = RowColumnarShadowMaskCache::default();
        let resident = cache.insert_batch(base.clone(), vec![1, 3], Arc::clone(&batch));
        assert!(cache.batch(&base, &[1, 3]).is_some());
        let duplicate = cache.insert_batch(base.clone(), vec![1, 3], test_batch(&[8]));
        assert!(Arc::ptr_eq(&resident, &duplicate));
        assert!(Arc::ptr_eq(&batch, &duplicate));

        let variants = [
            RowColumnarShadowMaskKey {
                row_groups: crate::columnar_row_group::RowGroupSetId::new([8; 16]),
                ..base.clone()
            },
            RowColumnarShadowMaskKey {
                branch_id: Arc::from("other"),
                ..base.clone()
            },
            RowColumnarShadowMaskKey {
                head_commit_id: crate::changelog::CommitId::for_test_label("other-head"),
                ..base.clone()
            },
            RowColumnarShadowMaskKey {
                current_state_revision: 2,
                ..base.clone()
            },
            RowColumnarShadowMaskKey {
                shadow_identity_digest: [9; 32],
                ..base.clone()
            },
            RowColumnarShadowMaskKey {
                group_index: 1,
                ..base.clone()
            },
        ];
        for variant in variants {
            assert!(cache.batch(&variant, &[1, 3]).is_none());
        }
        assert!(cache.batch(&base, &[3, 1]).is_none());
        assert!(cache.batch(&base, &[1]).is_none());
    }

    fn batch_cache_key() -> RowColumnarShadowMaskKey {
        RowColumnarShadowMaskKey {
            row_groups: crate::columnar_row_group::RowGroupSetId::new([3; 16]),
            branch_id: Arc::from("branch"),
            head_commit_id: crate::changelog::CommitId::for_test_label("batch-cache-head"),
            current_state_revision: 1,
            shadow_identity_digest: [4; 32],
            group_index: 0,
        }
    }

    fn test_batch(values: &[i64]) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        Arc::new(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
                .expect("batch"),
        )
    }
}
