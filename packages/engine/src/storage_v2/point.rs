use std::collections::{hash_map::Entry, HashMap};

use crate::backend_v2::{BackendError, BackendRead, GetOptions, Key, ProjectedValue, SpaceId};
use crate::storage_v2::{StorageReadResult, StorageReadStats};
use ahash::RandomState;

type FastHashBuilder = RandomState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointSlot {
    pub key: Key,
    pub value: Option<ProjectedValue>,
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
    Ok(get_many_values_caller_order_with_stats(read, space, keys, opts)?.value)
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
    let mut unique_index_by_key = HashMap::<&Key, usize, FastHashBuilder>::with_capacity_and_hasher(
        keys.len(),
        FastHashBuilder::with_seeds(0, 0, 0, 0),
    );
    let mut backend_keys = Vec::with_capacity(keys.len());
    let mut requested_to_unique = Vec::with_capacity(keys.len());
    for key in keys {
        let unique_index = backend_keys.len();
        match unique_index_by_key.entry(key) {
            Entry::Occupied(entry) => requested_to_unique.push(*entry.get()),
            Entry::Vacant(entry) => {
                entry.insert(unique_index);
                backend_keys.push(key.clone());
                requested_to_unique.push(unique_index);
            }
        }
    }

    let result = read.get_many(space, &backend_keys, opts)?;

    let mut unique_values = Vec::with_capacity(backend_keys.len());
    unique_values.resize_with(backend_keys.len(), || None);
    for entry in result.entries.entries {
        if let Some(&unique_index) = unique_index_by_key.get(&entry.key) {
            unique_values[unique_index] = Some(entry.value);
        }
    }

    let mut values = Vec::with_capacity(keys.len());
    for unique_index in requested_to_unique {
        values.push(unique_values[unique_index].clone());
    }

    Ok(StorageReadResult::new(
        values,
        StorageReadStats {
            requested_keys: keys.len() as u64,
            unique_backend_keys: backend_keys.len() as u64,
            backend_calls: 1,
            prefix_lowered: 0,
        },
    ))
}
