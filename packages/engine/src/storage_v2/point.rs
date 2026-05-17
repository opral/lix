use std::collections::HashMap;

use ahash::RandomState;

use crate::backend_v2::{
    BackendError, BackendRead, GetOptions, Key, PointVisitor, ProjectedValue, ProjectedValueRef,
};
use crate::storage_v2::{StorageRead, StorageReadResult, StorageReadStats, StorageSpace};

type FastHashBuilder = RandomState;

#[derive(Clone, Debug)]
pub struct PointReadPlan {
    pub logical_unique_keys: Vec<Key>,
    pub physical_unique_keys: Vec<Key>,
    pub requested_to_unique: RequestedToUnique,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PointValues<'plan> {
    pub unique_values: Vec<Option<ProjectedValue>>,
    pub requested_to_unique: RequestedToUniqueRef<'plan>,
}

#[derive(Debug, Default)]
pub struct PointReadBuffer {
    unique_values: Vec<Option<ProjectedValue>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PointValuesRef<'plan, 'buf> {
    pub unique_values: &'buf [Option<ProjectedValue>],
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

    pub fn collect<R>(
        &self,
        read: &R,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<PointValues<'_>>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        let unique_values =
            collect_physical_unique_values(read.backend_read(), &self.physical_unique_keys, opts)?;
        Ok(StorageReadResult::new(
            PointValues {
                unique_values,
                requested_to_unique: self.requested_to_unique.as_ref(),
            },
            self.stats(),
        ))
    }

    pub fn materialize<R>(
        &self,
        read: &R,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<Vec<Option<ProjectedValue>>>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        let result = self.collect(read, opts)?;
        Ok(StorageReadResult::new(
            result.value.materialize_caller_order(),
            result.stats,
        ))
    }

    pub fn collect_into<'plan, 'buf, R>(
        &'plan self,
        read: &R,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointReadBuffer,
    ) -> Result<StorageReadResult<PointValuesRef<'plan, 'buf>>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        collect_physical_unique_values_into(
            read.backend_read(),
            &self.physical_unique_keys,
            opts,
            buffer,
        )?;

        Ok(StorageReadResult::new(
            PointValuesRef {
                unique_values: buffer.unique_values.as_slice(),
                requested_to_unique: self.requested_to_unique.as_ref(),
            },
            self.stats(),
        ))
    }

    pub fn visit<R, V>(
        &self,
        read: &R,
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadStats, BackendError>
    where
        R: StorageRead + ?Sized,
        V: PointVisitor + ?Sized,
    {
        struct LogicalPointVisitor<'a, V: ?Sized> {
            logical_keys: &'a [Key],
            inner: &'a mut V,
        }

        impl<V> PointVisitor for LogicalPointVisitor<'_, V>
        where
            V: PointVisitor + ?Sized,
        {
            fn visit(
                &mut self,
                index: usize,
                _key: &Key,
                value: Option<ProjectedValueRef<'_>>,
            ) -> Result<(), BackendError> {
                let Some(logical_key) = self.logical_keys.get(index) else {
                    return Ok(());
                };
                self.inner.visit(index, logical_key, value)
            }
        }

        read.backend_read().visit_keys(
            &self.physical_unique_keys,
            opts,
            &mut LogicalPointVisitor {
                logical_keys: &self.logical_unique_keys,
                inner: visitor,
            },
        )?;
        Ok(self.stats())
    }

    fn from_parts(
        space: StorageSpace,
        logical_unique_keys: Vec<Key>,
        requested_to_unique: RequestedToUnique,
    ) -> Self {
        let physical_unique_keys = logical_unique_keys
            .iter()
            .map(|key| space.encode_key(key))
            .collect();
        Self {
            logical_unique_keys,
            physical_unique_keys,
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

impl<'a> RequestedToUniqueRef<'a> {
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

impl<'plan> PointValues<'plan> {
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

impl PointReadBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn capacity(&self) -> usize {
        self.unique_values.capacity()
    }

    pub fn clear(&mut self) {
        self.unique_values.clear();
    }

    fn reset_for_len(&mut self, len: usize) {
        self.unique_values.clear();
        self.unique_values.resize_with(len, || None);
    }
}

impl<'plan, 'buf> PointValuesRef<'plan, 'buf> {
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
}

fn collect_physical_unique_values<R>(
    read: &R,
    physical_unique_keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<Vec<Option<ProjectedValue>>, BackendError>
where
    R: BackendRead,
{
    let mut values = vec![None; physical_unique_keys.len()];
    collect_physical_unique_values_into_slice(
        read,
        physical_unique_keys,
        opts,
        values.as_mut_slice(),
    )?;
    Ok(values)
}

fn collect_physical_unique_values_into<R>(
    read: &R,
    physical_unique_keys: &[Key],
    opts: GetOptions<'_>,
    buffer: &mut PointReadBuffer,
) -> Result<(), BackendError>
where
    R: BackendRead,
{
    buffer.reset_for_len(physical_unique_keys.len());
    collect_physical_unique_values_into_slice(
        read,
        physical_unique_keys,
        opts,
        buffer.unique_values.as_mut_slice(),
    )
}

fn collect_physical_unique_values_into_slice<R>(
    read: &R,
    physical_unique_keys: &[Key],
    opts: GetOptions<'_>,
    values: &mut [Option<ProjectedValue>],
) -> Result<(), BackendError>
where
    R: BackendRead,
{
    struct Collector<'a> {
        values: &'a mut [Option<ProjectedValue>],
    }

    impl PointVisitor for Collector<'_> {
        fn visit(
            &mut self,
            index: usize,
            _key: &Key,
            value: Option<ProjectedValueRef<'_>>,
        ) -> Result<(), BackendError> {
            if let Some(slot) = self.values.get_mut(index) {
                *slot = value.map(|value| value.to_owned());
            }
            Ok(())
        }
    }

    read.visit_keys(physical_unique_keys, opts, &mut Collector { values })
}

fn keys_are_unique(keys: &[Key]) -> bool {
    let mut seen = HashMap::<&Key, (), FastHashBuilder>::with_capacity_and_hasher(
        keys.len(),
        FastHashBuilder::with_seeds(0, 0, 0, 0),
    );
    keys.iter().all(|key| seen.insert(key, ()).is_none())
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
