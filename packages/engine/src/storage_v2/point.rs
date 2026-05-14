use std::collections::{HashMap, HashSet};

use crate::backend_v2::{BackendError, BackendRead, GetOptions, Key, ProjectedValue, SpaceId};
use crate::storage_v2::{StorageReadResult, StorageReadStats};

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
    let mut seen = HashSet::with_capacity(keys.len());
    let mut backend_keys = Vec::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key.clone()) {
            backend_keys.push(key.clone());
        }
    }

    let result = read.get_many(space, &backend_keys, opts)?;

    let mut found = HashMap::with_capacity(result.entries.entries.len());
    for entry in result.entries.entries {
        found.insert(entry.key, entry.value);
    }

    let mut slots = Vec::with_capacity(keys.len());
    for key in keys {
        slots.push(PointSlot {
            key: key.clone(),
            value: found.get(key).cloned(),
        });
    }

    Ok(StorageReadResult::new(
        slots,
        StorageReadStats {
            requested_keys: keys.len() as u64,
            unique_backend_keys: backend_keys.len() as u64,
            backend_calls: 1,
            prefix_lowered: 0,
        },
    ))
}
