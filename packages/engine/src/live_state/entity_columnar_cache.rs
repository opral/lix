//! Repository-scoped reconciliation artifacts for analytical entity scans.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Statistics;

const SHADOW_MASK_CACHE_ENTRIES: usize = 256;
const STATISTICS_PROJECTIONS_PER_MASK: usize = 8;
const RECONCILED_BATCH_CACHE_ENTRIES: usize = 256;
const RECONCILED_BATCH_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EntityColumnarBatchKey {
    shadow: EntityColumnarShadowMaskKey,
    projection: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EntityColumnarShadowMaskKey {
    pub(crate) row_groups: crate::columnar_row_group::RowGroupSetId,
    pub(crate) branch_id: Arc<str>,
    pub(crate) head_commit_id: crate::changelog::CommitId,
    pub(crate) current_state_revision: u64,
    pub(crate) shadow_identity_digest: [u8; 32],
    pub(crate) group_index: usize,
}

#[derive(Default)]
pub(crate) struct EntityColumnarShadowMaskCache {
    entries: HashMap<EntityColumnarShadowMaskKey, EntityColumnarShadowCacheEntry>,
    insertion_order: VecDeque<EntityColumnarShadowMaskKey>,
    batches: HashMap<EntityColumnarBatchKey, (Arc<RecordBatch>, usize)>,
    batch_insertion_order: VecDeque<EntityColumnarBatchKey>,
    batch_bytes: usize,
}

struct EntityColumnarShadowCacheEntry {
    mask: Arc<BooleanArray>,
    statistics_by_projection: HashMap<Vec<usize>, Statistics>,
    statistics_insertion_order: VecDeque<Vec<usize>>,
}

impl EntityColumnarShadowMaskCache {
    pub(crate) fn get(&self, key: &EntityColumnarShadowMaskKey) -> Option<Arc<BooleanArray>> {
        self.entries.get(key).map(|entry| Arc::clone(&entry.mask))
    }

    pub(crate) fn insert(
        &mut self,
        key: EntityColumnarShadowMaskKey,
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
            EntityColumnarShadowCacheEntry {
                mask: Arc::clone(&mask),
                statistics_by_projection: HashMap::new(),
                statistics_insertion_order: VecDeque::new(),
            },
        );
        mask
    }

    pub(crate) fn statistics(
        &self,
        key: &EntityColumnarShadowMaskKey,
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
        key: &EntityColumnarShadowMaskKey,
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
        key: &EntityColumnarShadowMaskKey,
        projection: &[usize],
    ) -> Option<Arc<RecordBatch>> {
        self.batches
            .get(&EntityColumnarBatchKey {
                shadow: key.clone(),
                projection: projection.to_vec(),
            })
            .map(|(batch, _)| Arc::clone(batch))
    }

    pub(crate) fn insert_batch(
        &mut self,
        key: EntityColumnarShadowMaskKey,
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
        key: EntityColumnarShadowMaskKey,
        projection: Vec<usize>,
        batch: Arc<RecordBatch>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Arc<RecordBatch> {
        let key = EntityColumnarBatchKey {
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
                break;
            };
            if let Some((_, evicted_bytes)) = self.batches.remove(&evicted) {
                self.batch_bytes = self.batch_bytes.saturating_sub(evicted_bytes);
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

    #[test]
    fn statistics_projections_are_bounded_per_mask() {
        let key = EntityColumnarShadowMaskKey {
            row_groups: crate::columnar_row_group::RowGroupSetId::new([7; 16]),
            branch_id: Arc::from("branch"),
            head_commit_id: crate::changelog::CommitId::for_test_label("scan-cache-head"),
            current_state_revision: 3,
            shadow_identity_digest: [9; 32],
            group_index: 1,
        };
        let mut cache = EntityColumnarShadowMaskCache::default();
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
        let shadow = EntityColumnarShadowMaskKey {
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
        let mut cache = EntityColumnarShadowMaskCache::default();
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
        let mut cache = EntityColumnarShadowMaskCache::default();

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
        let mut cache = EntityColumnarShadowMaskCache::default();
        let resident = cache.insert_batch(base.clone(), vec![1, 3], Arc::clone(&batch));
        assert!(cache.batch(&base, &[1, 3]).is_some());
        let duplicate = cache.insert_batch(base.clone(), vec![1, 3], test_batch(&[8]));
        assert!(Arc::ptr_eq(&resident, &duplicate));
        assert!(Arc::ptr_eq(&batch, &duplicate));

        let variants = [
            EntityColumnarShadowMaskKey {
                row_groups: crate::columnar_row_group::RowGroupSetId::new([8; 16]),
                ..base.clone()
            },
            EntityColumnarShadowMaskKey {
                branch_id: Arc::from("other"),
                ..base.clone()
            },
            EntityColumnarShadowMaskKey {
                head_commit_id: crate::changelog::CommitId::for_test_label("other-head"),
                ..base.clone()
            },
            EntityColumnarShadowMaskKey {
                current_state_revision: 2,
                ..base.clone()
            },
            EntityColumnarShadowMaskKey {
                shadow_identity_digest: [9; 32],
                ..base.clone()
            },
            EntityColumnarShadowMaskKey {
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

    fn batch_cache_key() -> EntityColumnarShadowMaskKey {
        EntityColumnarShadowMaskKey {
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
