use std::collections::{BTreeMap, BTreeSet};

use crate::backend_v2::{BackendError, BackendRead, GetOptions, Key, ProjectedValue, SpaceId};

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
    let unique_keys = keys.iter().cloned().collect::<BTreeSet<_>>();
    let backend_keys = unique_keys.iter().cloned().collect::<Vec<_>>();
    let result = read.get_many(space, &backend_keys, opts)?;
    let found = result
        .entries
        .entries
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect::<BTreeMap<_, _>>();

    Ok(keys
        .iter()
        .map(|key| PointSlot {
            key: key.clone(),
            value: found.get(key).cloned(),
        })
        .collect())
}
