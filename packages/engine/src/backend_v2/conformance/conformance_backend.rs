use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend_v2::conformance::{BackendFactory, BackendFixture, BackendTestConfig};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue, ProjectedValueRef,
    PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId, StoredValue,
    WriteConcurrency, WriteOptions, WriteStats,
};

type ConformanceMap = BTreeMap<(SpaceId, Key), Bytes>;

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
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        get_many_from_map(&self.entries, space, keys, opts)
    }

    fn visit_range<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_range_from_map(&self.entries, space, range, opts, visitor)
    }
}

impl BackendWrite for ConformanceWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            self.entries
                .insert((space, entry.key), stored_value_bytes(entry.value));
        }
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.entries.remove(&(space, key.clone()));
        }
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

fn get_many_from_map(
    entries: &ConformanceMap,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<GetManyResult, BackendError> {
    Ok(GetManyResult::new(
        keys.iter()
            .map(|key| {
                entries
                    .get(&(space, key.clone()))
                    .map(|value| project_value(value, opts.projection))
            })
            .collect(),
    ))
}

fn visit_range_from_map<V>(
    entries: &ConformanceMap,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
{
    let mut emitted = 0;
    let mut has_more = false;

    if opts.limit_rows == 0 {
        return Ok(ScanResult::default());
    }

    for ((entry_space, key), value) in entries {
        if *entry_space != space || !range_contains(&range, key) {
            continue;
        }
        if opts
            .resume_after
            .is_some_and(|resume_after| key <= resume_after)
        {
            continue;
        }
        if emitted == opts.limit_rows {
            has_more = true;
            break;
        }
        visitor.visit(key.as_ref(), project_value_ref(value, opts.projection))?;
        emitted += 1;
    }

    Ok(ScanResult { emitted, has_more })
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

fn project_value(value: &Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value.clone()),
    }
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
