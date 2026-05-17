use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend_v2::conformance::{BackendFactory, BackendFixture, BackendTestConfig};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, BufferedRangeScan,
    CommitResult, CoreProjection, GetOptions, Key, KeyRange, PointVisitor, ProjectedValueRef,
    PutBatch, ReadEntry, ReadOptions, ScanOptions, StoredValue, WriteConcurrency, WriteOptions,
    WriteStats,
};

type ConformanceMap = BTreeMap<Key, Bytes>;

#[derive(Clone, Debug, Default)]
pub struct ConformanceBackend {
    entries: Arc<Mutex<ConformanceMap>>,
}

#[derive(Clone, Debug, Default)]
pub struct ConformanceBackendFactory;

#[derive(Clone, Debug, Default)]
pub struct ConformanceBackendFixture {
    entries: Arc<Mutex<ConformanceMap>>,
}

pub struct ConformanceRead {
    entries: ConformanceMap,
}

pub struct ConformanceWrite {
    parent: Arc<Mutex<ConformanceMap>>,
    entries: ConformanceMap,
}

impl ConformanceBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BackendFactory for ConformanceBackendFactory {
    type Backend = ConformanceBackend;
    type Fixture = ConformanceBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        ConformanceBackendFixture::default()
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: true,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for ConformanceBackendFixture {
    type Backend = ConformanceBackend;

    fn open(&self) -> Self::Backend {
        ConformanceBackend {
            entries: Arc::clone(&self.entries),
        }
    }
}

impl Backend for ConformanceBackend {
    type Read<'a>
        = ConformanceRead
    where
        Self: 'a;

    type Write<'a>
        = ConformanceWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(ConformanceRead {
            entries: self.snapshot()?,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(ConformanceWrite {
            parent: Arc::clone(&self.entries),
            entries: self.snapshot()?,
        })
    }
}

impl BackendRead for ConformanceRead {
    type RangeScan<'a> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        visit_keys_from_map(&self.entries, keys, opts, visitor)
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        let mut cursor = BufferedRangeScan::new(scan_rows_from_map(&self.entries, range, opts));
        f(&mut cursor)
    }
}

impl BackendWrite for ConformanceWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            self.entries
                .insert(entry.key, stored_value_bytes(entry.value));
        }
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.entries.remove(key);
        }
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        self.entries
            .retain(|key, _value| !range_contains(&range, key));
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        *self
            .parent
            .lock()
            .map_err(|_| BackendError::Io("conformance backend lock poisoned".to_string()))? =
            self.entries;
        Ok(CommitResult {
            commit_id: None,
            stats: WriteStats::default(),
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

impl ConformanceBackend {
    fn snapshot(&self) -> Result<ConformanceMap, BackendError> {
        self.entries
            .lock()
            .map_err(|_| BackendError::Io("conformance backend lock poisoned".to_string()))
            .map(|entries| entries.clone())
    }
}

fn visit_keys_from_map<V>(
    entries: &ConformanceMap,
    keys: &[Key],
    opts: GetOptions<'_>,
    visitor: &mut V,
) -> Result<(), BackendError>
where
    V: PointVisitor + ?Sized,
{
    for (index, key) in keys.iter().enumerate() {
        let value = entries
            .get(key)
            .map(|value| project_value_ref(value, opts.projection));
        visitor.visit(index, key, value)?;
    }
    Ok(())
}

fn scan_rows_from_map(
    entries: &ConformanceMap,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Vec<ReadEntry> {
    if opts.limit_rows == 0 {
        return Vec::new();
    }

    let mut rows = Vec::new();
    for (key, value) in entries {
        if !range_contains(&range, key) {
            continue;
        }
        if opts
            .resume_after
            .is_some_and(|resume_after| key <= resume_after)
        {
            continue;
        }
        rows.push(ReadEntry {
            key: key.clone(),
            value: project_value_ref(value, opts.projection).to_owned(),
        });
    }
    rows
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        std::ops::Bound::Included(lower) => key >= lower,
        std::ops::Bound::Excluded(lower) => key > lower,
        std::ops::Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        std::ops::Bound::Included(upper) => key <= upper,
        std::ops::Bound::Excluded(upper) => key < upper,
        std::ops::Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn project_value_ref(value: &Bytes, projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value.as_ref()),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}
