use std::collections::{HashMap, HashSet};

use ahash::RandomState;

use crate::backend::{BackendError, GetOptions, Key, ProjectedValue, SpaceId};
use crate::storage::{StorageRead, StorageReadResult, StorageReadStats, StorageSpace};

type FastHashBuilder = RandomState;

#[derive(Clone, Debug)]
pub struct PointReadPlan {
    pub space: SpaceId,
    pub logical_unique_keys: Vec<Key>,
    pub requested_to_unique: RequestedToUnique,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PointValues<'plan> {
    pub unique_values: Vec<Option<ProjectedValue>>,
    pub requested_to_unique: RequestedToUniqueRef<'plan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestedToUnique {
    Identity { len: usize },
    Indexes(Vec<usize>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestedToUniqueRef<'a> {
    Identity { len: usize },
    Indexes(&'a [usize]),
}

impl PointReadPlan {
    pub fn new(space: StorageSpace, keys: &[Key]) -> Self {
        let mut unique_index_by_key =
            HashMap::<Key, usize, FastHashBuilder>::with_capacity_and_hasher(
                keys.len(),
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            );
        let mut logical_unique_keys = Vec::with_capacity(keys.len());
        let mut requested_to_unique = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(&unique_index) = unique_index_by_key.get(key) {
                requested_to_unique.push(unique_index);
                continue;
            }

            let unique_index = logical_unique_keys.len();
            unique_index_by_key.insert(key.clone(), unique_index);
            logical_unique_keys.push(key.clone());
            requested_to_unique.push(unique_index);
        }

        Self::from_parts(
            space,
            logical_unique_keys,
            requested_to_unique_mapping(requested_to_unique, keys.len()),
        )
    }

    pub fn from_unique_keys(space: StorageSpace, unique_keys: Vec<Key>) -> Self {
        debug_assert!(
            keys_are_unique(&unique_keys),
            "PointReadPlan::from_unique_keys requires unique keys"
        );
        let len = unique_keys.len();
        Self::from_parts(space, unique_keys, RequestedToUnique::Identity { len })
    }

    pub fn len(&self) -> usize {
        self.requested_to_unique.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requested_to_unique.is_empty()
    }

    pub fn requested_to_unique(&self) -> RequestedToUniqueRef<'_> {
        self.requested_to_unique.as_ref()
    }

    pub async fn collect<R>(
        &self,
        read: &R,
        opts: GetOptions,
    ) -> Result<StorageReadResult<PointValues<'_>>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        let unique_values = read
            .get_many(self.space, &self.logical_unique_keys, opts)
            .await?
            .values;
        Ok(StorageReadResult::new(
            PointValues {
                unique_values,
                requested_to_unique: self.requested_to_unique.as_ref(),
            },
            self.stats(),
        ))
    }

    pub async fn materialize<R>(
        &self,
        read: &R,
        opts: GetOptions,
    ) -> Result<StorageReadResult<Vec<Option<ProjectedValue>>>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        let result = self.collect(read, opts).await?;
        Ok(StorageReadResult::new(
            result.value.materialize_caller_order(),
            result.stats,
        ))
    }

    fn from_parts(
        space: StorageSpace,
        logical_unique_keys: Vec<Key>,
        requested_to_unique: RequestedToUnique,
    ) -> Self {
        Self {
            space: space.id,
            logical_unique_keys,
            requested_to_unique,
        }
    }

    fn stats(&self) -> StorageReadStats {
        StorageReadStats {
            requested_keys: self.requested_to_unique.len() as u64,
            unique_backend_keys: self.logical_unique_keys.len() as u64,
            backend_calls: 1,
            prefix_lowered: 0,
            ..StorageReadStats::default()
        }
    }
}

impl RequestedToUnique {
    pub fn len(&self) -> usize {
        match self {
            Self::Identity { len } => *len,
            Self::Indexes(indexes) => indexes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn unique_index(&self, requested_index: usize) -> Option<usize> {
        match self {
            Self::Identity { len } => (requested_index < *len).then_some(requested_index),
            Self::Indexes(indexes) => indexes.get(requested_index).copied(),
        }
    }

    pub fn as_ref(&self) -> RequestedToUniqueRef<'_> {
        match self {
            Self::Identity { len } => RequestedToUniqueRef::Identity { len: *len },
            Self::Indexes(indexes) => RequestedToUniqueRef::Indexes(indexes),
        }
    }

    pub fn to_vec(&self) -> Vec<usize> {
        match self {
            Self::Identity { len } => (0..*len).collect(),
            Self::Indexes(indexes) => indexes.clone(),
        }
    }
}

impl RequestedToUniqueRef<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::Identity { len } => *len,
            Self::Indexes(indexes) => indexes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn unique_index(&self, requested_index: usize) -> Option<usize> {
        match self {
            Self::Identity { len } => (requested_index < *len).then_some(requested_index),
            Self::Indexes(indexes) => indexes.get(requested_index).copied(),
        }
    }

    pub fn to_vec(self) -> Vec<usize> {
        match self {
            Self::Identity { len } => (0..len).collect(),
            Self::Indexes(indexes) => indexes.to_vec(),
        }
    }
}

impl PointValues<'_> {
    pub fn len(&self) -> usize {
        self.requested_to_unique.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requested_to_unique.is_empty()
    }

    pub fn value_at(&self, requested_index: usize) -> Option<&ProjectedValue> {
        let unique_index = self.requested_to_unique.unique_index(requested_index)?;
        self.unique_values.get(unique_index)?.as_ref()
    }

    pub fn materialize_caller_order(self) -> Vec<Option<ProjectedValue>> {
        materialize_caller_order(self.unique_values, self.requested_to_unique)
    }
}

fn keys_are_unique(keys: &[Key]) -> bool {
    let mut seen = HashSet::<&Key, FastHashBuilder>::with_capacity_and_hasher(
        keys.len(),
        FastHashBuilder::with_seeds(0, 0, 0, 0),
    );
    keys.iter().all(|key| seen.insert(key))
}

fn requested_to_unique_mapping(indexes: Vec<usize>, requested_len: usize) -> RequestedToUnique {
    if indexes.len() == requested_len
        && indexes
            .iter()
            .copied()
            .enumerate()
            .all(|(requested_index, unique_index)| requested_index == unique_index)
    {
        RequestedToUnique::Identity { len: requested_len }
    } else {
        RequestedToUnique::Indexes(indexes)
    }
}

fn materialize_caller_order(
    unique_values: Vec<Option<ProjectedValue>>,
    requested_to_unique: RequestedToUniqueRef<'_>,
) -> Vec<Option<ProjectedValue>> {
    let mut values = Vec::with_capacity(requested_to_unique.len());
    for requested_index in 0..requested_to_unique.len() {
        let unique_index = requested_to_unique
            .unique_index(requested_index)
            .expect("requested index is inside requested_to_unique");
        values.push(unique_values[unique_index].clone());
    }
    values
}
