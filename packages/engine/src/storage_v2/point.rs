use std::collections::HashMap;

use crate::backend_v2::{BackendError, BackendRead, GetOptions, Key, ProjectedValue, SpaceId};
use crate::storage_v2::{StorageReadResult, StorageReadStats};
use ahash::RandomState;

type FastHashBuilder = RandomState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointSlot {
    pub key: Key,
    pub value: Option<ProjectedValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedPointValues {
    pub unique_values: Vec<Option<ProjectedValue>>,
    pub requested_to_unique: Vec<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BorrowedIndexedPointValues<'a> {
    pub unique_values: Vec<Option<ProjectedValue>>,
    pub requested_to_unique: &'a [usize],
}

#[derive(Clone, Debug)]
pub struct PointRequestPlan {
    pub unique_keys: Vec<Key>,
    pub requested_to_unique: Vec<usize>,
}

struct BorrowedPointRequestPlan {
    unique_keys: Vec<Key>,
    requested_to_unique: Vec<usize>,
}

impl PointRequestPlan {
    pub fn new(keys: &[Key]) -> Self {
        let mut unique_index_by_key =
            HashMap::<Key, usize, FastHashBuilder>::with_capacity_and_hasher(
                keys.len(),
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            );
        let mut unique_keys = Vec::with_capacity(keys.len());
        let mut requested_to_unique = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(&unique_index) = unique_index_by_key.get(key) {
                requested_to_unique.push(unique_index);
                continue;
            }

            let unique_index = unique_keys.len();
            unique_index_by_key.insert(key.clone(), unique_index);
            unique_keys.push(key.clone());
            requested_to_unique.push(unique_index);
        }

        Self {
            unique_keys,
            requested_to_unique,
        }
    }

    pub fn from_unique_keys(unique_keys: Vec<Key>) -> Self {
        debug_assert!(
            keys_are_unique(&unique_keys),
            "PointRequestPlan::from_unique_keys requires unique keys"
        );
        let requested_to_unique = (0..unique_keys.len()).collect();
        Self {
            unique_keys,
            requested_to_unique,
        }
    }

    pub fn len(&self) -> usize {
        self.requested_to_unique.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requested_to_unique.is_empty()
    }
}

fn keys_are_unique(keys: &[Key]) -> bool {
    let mut seen = HashMap::<&Key, (), FastHashBuilder>::with_capacity_and_hasher(
        keys.len(),
        FastHashBuilder::with_seeds(0, 0, 0, 0),
    );
    keys.iter().all(|key| seen.insert(key, ()).is_none())
}

impl BorrowedPointRequestPlan {
    fn new<'a>(keys: &'a [Key]) -> Self {
        let mut unique_index_by_key =
            HashMap::<&'a Key, usize, FastHashBuilder>::with_capacity_and_hasher(
                keys.len(),
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            );
        let mut unique_keys = Vec::with_capacity(keys.len());
        let mut requested_to_unique = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(&unique_index) = unique_index_by_key.get(key) {
                requested_to_unique.push(unique_index);
                continue;
            }

            let unique_index = unique_keys.len();
            unique_index_by_key.insert(key, unique_index);
            unique_keys.push(key.clone());
            requested_to_unique.push(unique_index);
        }

        Self {
            unique_keys,
            requested_to_unique,
        }
    }
}

impl IndexedPointValues {
    pub fn len(&self) -> usize {
        self.requested_to_unique.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requested_to_unique.is_empty()
    }

    pub fn value_at(&self, requested_index: usize) -> Option<&ProjectedValue> {
        let unique_index = *self.requested_to_unique.get(requested_index)?;
        self.unique_values.get(unique_index)?.as_ref()
    }

    pub fn materialize_caller_order(self) -> Vec<Option<ProjectedValue>> {
        let mut values = Vec::with_capacity(self.requested_to_unique.len());
        for unique_index in self.requested_to_unique {
            values.push(self.unique_values[unique_index].clone());
        }
        values
    }
}

impl<'a> BorrowedIndexedPointValues<'a> {
    pub fn len(&self) -> usize {
        self.requested_to_unique.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requested_to_unique.is_empty()
    }

    pub fn value_at(&self, requested_index: usize) -> Option<&ProjectedValue> {
        let unique_index = *self.requested_to_unique.get(requested_index)?;
        self.unique_values.get(unique_index)?.as_ref()
    }

    pub fn into_owned(self) -> IndexedPointValues {
        IndexedPointValues {
            unique_values: self.unique_values,
            requested_to_unique: self.requested_to_unique.to_vec(),
        }
    }
}

pub(crate) fn get_many_caller_order<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<Vec<PointSlot>, BackendError>
where
    R: BackendRead,
{
    Ok(get_many_caller_order_with_stats(read, space, keys, opts)?.value)
}

pub(crate) fn get_many_caller_order_with_stats<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<StorageReadResult<Vec<PointSlot>>, BackendError>
where
    R: BackendRead,
{
    let values = get_many_values_caller_order_with_stats(read, space, keys, opts)?;
    let mut slots = Vec::with_capacity(keys.len());
    for (key, value) in keys.iter().cloned().zip(values.value) {
        slots.push(PointSlot { key, value });
    }

    Ok(StorageReadResult::new(slots, values.stats))
}

pub(crate) fn get_many_values_caller_order<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<Vec<Option<ProjectedValue>>, BackendError>
where
    R: BackendRead,
{
    Ok(
        get_many_indexed_values_caller_order_with_stats(read, space, keys, opts)?
            .value
            .materialize_caller_order(),
    )
}

pub(crate) fn get_many_values_caller_order_with_stats<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<StorageReadResult<Vec<Option<ProjectedValue>>>, BackendError>
where
    R: BackendRead,
{
    let indexed = get_many_indexed_values_caller_order_with_stats(read, space, keys, opts)?;
    Ok(StorageReadResult::new(
        indexed.value.materialize_caller_order(),
        indexed.stats,
    ))
}

pub(crate) fn get_many_indexed_values_caller_order<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<IndexedPointValues, BackendError>
where
    R: BackendRead,
{
    Ok(get_many_indexed_values_caller_order_with_stats(read, space, keys, opts)?.value)
}

pub(crate) fn get_many_indexed_values_caller_order_with_stats<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<StorageReadResult<IndexedPointValues>, BackendError>
where
    R: BackendRead,
{
    let plan = BorrowedPointRequestPlan::new(keys);
    get_many_indexed_values_for_borrowed_plan_with_stats(read, space, &plan, opts)
}

pub(crate) fn get_many_indexed_values_for_plan<R>(
    read: &R,
    space: SpaceId,
    plan: &PointRequestPlan,
    opts: GetOptions<'_>,
) -> Result<IndexedPointValues, BackendError>
where
    R: BackendRead,
{
    Ok(
        get_many_borrowed_indexed_values_for_plan_with_stats(read, space, plan, opts)?
            .value
            .into_owned(),
    )
}

fn get_many_indexed_values_for_borrowed_plan_with_stats<R>(
    read: &R,
    space: SpaceId,
    plan: &BorrowedPointRequestPlan,
    opts: GetOptions<'_>,
) -> Result<StorageReadResult<IndexedPointValues>, BackendError>
where
    R: BackendRead,
{
    let result = read.get_many(space, &plan.unique_keys, opts)?;

    Ok(StorageReadResult::new(
        IndexedPointValues {
            unique_values: result.values,
            requested_to_unique: plan.requested_to_unique.clone(),
        },
        StorageReadStats {
            requested_keys: plan.requested_to_unique.len() as u64,
            unique_backend_keys: plan.unique_keys.len() as u64,
            backend_calls: 1,
            prefix_lowered: 0,
        },
    ))
}

pub(crate) fn get_many_indexed_values_for_plan_with_stats<R>(
    read: &R,
    space: SpaceId,
    plan: &PointRequestPlan,
    opts: GetOptions<'_>,
) -> Result<StorageReadResult<IndexedPointValues>, BackendError>
where
    R: BackendRead,
{
    let result = get_many_borrowed_indexed_values_for_plan_with_stats(read, space, plan, opts)?;
    Ok(StorageReadResult::new(
        result.value.into_owned(),
        result.stats,
    ))
}

pub(crate) fn get_many_borrowed_indexed_values_for_plan<'a, R>(
    read: &R,
    space: SpaceId,
    plan: &'a PointRequestPlan,
    opts: GetOptions<'_>,
) -> Result<BorrowedIndexedPointValues<'a>, BackendError>
where
    R: BackendRead,
{
    Ok(get_many_borrowed_indexed_values_for_plan_with_stats(read, space, plan, opts)?.value)
}

pub(crate) fn get_many_borrowed_indexed_values_for_plan_with_stats<'a, R>(
    read: &R,
    space: SpaceId,
    plan: &'a PointRequestPlan,
    opts: GetOptions<'_>,
) -> Result<StorageReadResult<BorrowedIndexedPointValues<'a>>, BackendError>
where
    R: BackendRead,
{
    let result = read.get_many(space, &plan.unique_keys, opts)?;

    Ok(StorageReadResult::new(
        BorrowedIndexedPointValues {
            unique_values: result.values,
            requested_to_unique: &plan.requested_to_unique,
        },
        StorageReadStats {
            requested_keys: plan.requested_to_unique.len() as u64,
            unique_backend_keys: plan.unique_keys.len() as u64,
            backend_calls: 1,
            prefix_lowered: 0,
        },
    ))
}
