use std::collections::HashMap;

use ahash::RandomState;

use crate::backend::{
    BackendError, BackendRead, GetOptions, Key, PointVisitor, ProjectedValue, ProjectedValueRef,
};
use crate::storage::{StorageRead, StorageReadResult, StorageReadStats, StorageSpace};

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
    visited: Vec<bool>,
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
            physical_keys: &'a [Key],
            logical_keys: &'a [Key],
            visited: PointVisitTracker<'a>,
            inner: &'a mut V,
        }

        impl<V> PointVisitor for LogicalPointVisitor<'_, V>
        where
            V: PointVisitor + ?Sized,
        {
            fn visit(
                &mut self,
                index: usize,
                key: &Key,
                value: Option<ProjectedValueRef<'_>>,
            ) -> Result<(), BackendError> {
                let Some(expected_physical_key) = self.physical_keys.get(index) else {
                    return Err(BackendError::Corruption(format!(
                        "point read backend visited out-of-range key index {index} for {} requested keys",
                        self.physical_keys.len()
                    )));
                };
                if expected_physical_key != key {
                    return Err(BackendError::Corruption(
                        "point read backend visited key that does not match requested index"
                            .to_string(),
                    ));
                }
                self.visited.mark(index, "point read visitor")?;
                let Some(logical_key) = self.logical_keys.get(index) else {
                    return Err(BackendError::Corruption(format!(
                        "point read visitor has no logical key for key index {index}"
                    )));
                };
                self.inner.visit(index, logical_key, value)
            }
        }

        let mut visited = Vec::new();
        let tracker = PointVisitTracker::new(
            self.physical_unique_keys.len(),
            &mut visited,
            PointVisitTrackerAllocation::InlineForSmallReads,
        );
        let mut logical_visitor = LogicalPointVisitor {
            physical_keys: &self.physical_unique_keys,
            logical_keys: &self.logical_unique_keys,
            visited: tracker,
            inner: visitor,
        };
        read.backend_read()
            .visit_keys(&self.physical_unique_keys, opts, &mut logical_visitor)?;
        if logical_visitor.visited.count() != self.physical_unique_keys.len() {
            let index = logical_visitor.visited.missing_index();
            return Err(BackendError::Corruption(format!(
                "point read backend did not visit requested key index {index}"
            )));
        }
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
        self.visited.clear();
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
    let mut visited = Vec::new();
    collect_physical_unique_values_into_slice(
        read,
        physical_unique_keys,
        opts,
        values.as_mut_slice(),
        &mut visited,
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
        &mut buffer.visited,
    )
}

fn collect_physical_unique_values_into_slice<R>(
    read: &R,
    physical_unique_keys: &[Key],
    opts: GetOptions<'_>,
    values: &mut [Option<ProjectedValue>],
    visited: &mut Vec<bool>,
) -> Result<(), BackendError>
where
    R: BackendRead,
{
    struct Collector<'a> {
        keys: &'a [Key],
        values: &'a mut [Option<ProjectedValue>],
        visited: PointVisitTracker<'a>,
    }

    impl PointVisitor for Collector<'_> {
        fn visit(
            &mut self,
            index: usize,
            key: &Key,
            value: Option<ProjectedValueRef<'_>>,
        ) -> Result<(), BackendError> {
            let Some(expected_key) = self.keys.get(index) else {
                return Err(BackendError::Corruption(format!(
                    "point read backend visited out-of-range key index {index} for {} requested keys",
                    self.keys.len()
                )));
            };
            if expected_key != key {
                return Err(BackendError::Corruption(
                    "point read backend visited key that does not match requested index"
                        .to_string(),
                ));
            }
            let Some(slot) = self.values.get_mut(index) else {
                return Err(BackendError::Corruption(format!(
                    "point read collector has no value slot for key index {index}"
                )));
            };
            self.visited.mark(index, "point read collector")?;
            *slot = value.map(|value| value.to_owned());
            Ok(())
        }
    }

    let tracker = PointVisitTracker::new(
        physical_unique_keys.len(),
        visited,
        PointVisitTrackerAllocation::UseProvidedSlice,
    );
    let mut collector = Collector {
        keys: physical_unique_keys,
        values,
        visited: tracker,
    };
    read.visit_keys(physical_unique_keys, opts, &mut collector)?;
    if collector.visited.count() != physical_unique_keys.len() {
        let index = collector.visited.missing_index();
        return Err(BackendError::Corruption(format!(
            "point read backend did not visit requested key index {index}"
        )));
    }
    Ok(())
}

enum PointVisitTrackerAllocation {
    InlineForSmallReads,
    UseProvidedSlice,
}

enum PointVisitTracker<'a> {
    Inline {
        bits: u64,
        count: usize,
        len: usize,
    },
    Slice {
        visited: &'a mut [bool],
        count: usize,
    },
}

impl<'a> PointVisitTracker<'a> {
    fn new(
        len: usize,
        visited: &'a mut Vec<bool>,
        allocation: PointVisitTrackerAllocation,
    ) -> Self {
        if matches!(allocation, PointVisitTrackerAllocation::InlineForSmallReads)
            && len <= u64::BITS as usize
        {
            return Self::Inline {
                bits: 0,
                count: 0,
                len,
            };
        }

        visited.clear();
        visited.resize(len, false);
        Self::Slice {
            visited: visited.as_mut_slice(),
            count: 0,
        }
    }

    fn mark(&mut self, index: usize, context: &str) -> Result<(), BackendError> {
        match self {
            Self::Inline { bits, count, len } => {
                if index >= *len {
                    return Err(BackendError::Corruption(format!(
                        "{context} has no visit slot for key index {index}"
                    )));
                }
                let mask = 1_u64 << index;
                if *bits & mask != 0 {
                    return Err(BackendError::Corruption(format!(
                        "point read backend visited key index {index} more than once"
                    )));
                }
                *bits |= mask;
                *count += 1;
                Ok(())
            }
            Self::Slice { visited, count } => {
                let Some(slot) = visited.get_mut(index) else {
                    return Err(BackendError::Corruption(format!(
                        "{context} has no visit slot for key index {index}"
                    )));
                };
                if *slot {
                    return Err(BackendError::Corruption(format!(
                        "point read backend visited key index {index} more than once"
                    )));
                }
                *slot = true;
                *count += 1;
                Ok(())
            }
        }
    }

    fn count(&self) -> usize {
        match self {
            Self::Inline { count, .. } | Self::Slice { count, .. } => *count,
        }
    }

    fn missing_index(&self) -> usize {
        match self {
            Self::Inline { bits, len, .. } => {
                for index in 0..*len {
                    if bits & (1_u64 << index) == 0 {
                        return index;
                    }
                }
                *len
            }
            Self::Slice { visited, .. } => visited
                .iter()
                .position(|visited| !visited)
                .unwrap_or(visited.len()),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BufferedRangeScan, KeyRange, ScanOptions};
    use crate::storage::{StorageReadScope, StorageSpaceId};

    enum BrokenPointReadMode {
        Skip,
        Duplicate,
        OutOfRange,
        WrongKey,
    }

    struct BrokenPointRead {
        mode: BrokenPointReadMode,
    }

    impl BackendRead for BrokenPointRead {
        type RangeScan<'cursor> = BufferedRangeScan;

        fn visit_keys<V>(
            &self,
            keys: &[Key],
            _opts: GetOptions<'_>,
            visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            match self.mode {
                BrokenPointReadMode::Skip => Ok(()),
                BrokenPointReadMode::Duplicate => {
                    visitor.visit(0, &keys[0], None)?;
                    visitor.visit(0, &keys[0], None)
                }
                BrokenPointReadMode::OutOfRange => visitor.visit(keys.len(), &keys[0], None),
                BrokenPointReadMode::WrongKey => {
                    let wrong_key = Key(bytes::Bytes::from_static(b"wrong-key"));
                    visitor.visit(0, &wrong_key, None)
                }
            }
        }

        fn with_range_scan<T, F>(
            &self,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
            f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            f(&mut BufferedRangeScan::default())
        }
    }

    fn collect_error(mode: BrokenPointReadMode) -> BackendError {
        let read = BrokenPointRead { mode };
        let keys = vec![Key(bytes::Bytes::from_static(b"key-1"))];
        collect_physical_unique_values(&read, &keys, GetOptions::default())
            .expect_err("broken point-read visitor contract should be rejected")
    }

    fn visit_error(mode: BrokenPointReadMode) -> BackendError {
        let read = StorageReadScope::new(BrokenPointRead { mode });
        let space = StorageSpace::new(StorageSpaceId(0x0000_0001), "test.point");
        let keys = vec![Key(bytes::Bytes::from_static(b"key-1"))];
        let plan = PointReadPlan::new(space, &keys);
        let mut visitor = |_index: usize, _key: &Key, _value: Option<ProjectedValueRef<'_>>| Ok(());
        plan.visit(&read, GetOptions::default(), &mut visitor)
            .expect_err("broken point-read visitor contract should be rejected")
    }

    #[test]
    fn point_read_rejects_missing_backend_visit() {
        let error = collect_error(BrokenPointReadMode::Skip);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("did not visit requested key index 0")
        ));
    }

    #[test]
    fn point_read_rejects_duplicate_backend_visit() {
        let error = collect_error(BrokenPointReadMode::Duplicate);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited key index 0 more than once")
        ));
    }

    #[test]
    fn point_read_rejects_out_of_range_backend_visit() {
        let error = collect_error(BrokenPointReadMode::OutOfRange);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited out-of-range key index 1")
        ));
    }

    #[test]
    fn point_read_rejects_wrong_key_for_backend_visit_index() {
        let error = collect_error(BrokenPointReadMode::WrongKey);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited key that does not match requested index")
        ));
    }

    #[test]
    fn point_read_visit_rejects_missing_backend_visit() {
        let error = visit_error(BrokenPointReadMode::Skip);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("did not visit requested key index 0")
        ));
    }

    #[test]
    fn point_read_visit_rejects_duplicate_backend_visit() {
        let error = visit_error(BrokenPointReadMode::Duplicate);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited key index 0 more than once")
        ));
    }

    #[test]
    fn point_read_visit_rejects_out_of_range_backend_visit() {
        let error = visit_error(BrokenPointReadMode::OutOfRange);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited out-of-range key index 1")
        ));
    }

    #[test]
    fn point_read_visit_rejects_wrong_key_for_backend_visit_index() {
        let error = visit_error(BrokenPointReadMode::WrongKey);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited key that does not match requested index")
        ));
    }
}
