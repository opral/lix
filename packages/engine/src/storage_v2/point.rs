use std::collections::{BTreeMap, BTreeSet};

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
    let unique_keys = keys.iter().cloned().collect::<BTreeSet<_>>();
    let backend_keys = unique_keys.iter().cloned().collect::<Vec<_>>();
    let result = read.get_many(space, &backend_keys, opts)?;
    let found = result
        .entries
        .entries
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect::<BTreeMap<_, _>>();

    let slots = keys
        .iter()
        .map(|key| PointSlot {
            key: key.clone(),
            value: found.get(key).cloned(),
        })
        .collect();

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
