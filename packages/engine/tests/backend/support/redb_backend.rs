use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetOptions,
    Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions,
    ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
    WriteTransaction as RedbWriteTxn,
};
use tempfile::TempDir;

const ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entries");

#[derive(Debug)]
pub struct RedbBackendFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct RedbBackendFixture {
    path: PathBuf,
}

#[derive(Clone)]
pub struct RedbBackend {
    path: PathBuf,
    db: Arc<Database>,
}

pub struct RedbRead {
    read: ReadTransaction,
}

pub struct RedbWrite {
    write: RedbWriteTxn,
    stats: WriteStats,
}

impl RedbBackendFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create redb backend temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl BackendFactory for RedbBackendFactory {
    type Backend = RedbBackend;
    type Fixture = RedbBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("backend-{database_id}.redb"));
        RedbBackendFixture { path }
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for RedbBackendFixture {
    type Backend = RedbBackend;

    fn open(&self) -> Self::Backend {
        RedbBackend::open(&self.path).expect("open redb backend")
    }
}

impl RedbBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        let db = Arc::new(Database::create(&path).map_err(redb_error)?);
        initialize_database(&db)?;
        Ok(Self { path, db })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Backend for RedbBackend {
    type Read<'a>
        = RedbRead
    where
        Self: 'a;

    type Write<'a>
        = RedbWrite
    where
        Self: 'a;
    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(RedbRead {
            read: self.db.begin_read().map_err(redb_error)?,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(RedbWrite {
            write: self.db.begin_write().map_err(redb_error)?,
            stats: WriteStats::default(),
        })
    }
}

/// redb keeps its single-table layout; spaces are scoped by prefixing the
/// 4-byte big-endian space id internally. Visitors observe logical keys.
fn physical_key(space: SpaceId, key: &Key) -> Key {
    let mut bytes = Vec::with_capacity(4 + key.0.len());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Key(Bytes::from(bytes))
}

fn physical_range(space: SpaceId, range: KeyRange) -> KeyRange {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| match bound {
        Bound::Included(key) => Bound::Included(physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(physical_key(space, &key)),
        Bound::Unbounded => unbounded,
    };
    KeyRange {
        lower: map(
            range.lower,
            Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))),
        ),
        upper: map(
            range.upper,
            space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
                Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
            }),
        ),
    }
}

impl BackendRead for RedbRead {
    fn visit_keys<V>(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
        for (index, key) in keys.iter().enumerate() {
            let value = table
                .get(physical_key(space, key).0.as_ref())
                .map_err(redb_error)?;
            visitor.visit(
                index,
                key,
                value
                    .as_ref()
                    .map(|value| project_value_ref(value.value(), opts.projection)),
            )?;
        }
        Ok(())
    }

    fn scan<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if opts.limit_rows == 0 {
            return Ok(ScanResult {
                emitted: 0,
                has_more: false,
            });
        }
        let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
        let resume_after = opts.resume_after.map(|key| physical_key(space, key));
        let (lower, upper) = encoded_bounds(physical_range(space, range), resume_after.as_ref());
        let lower = bound_as_slice(&lower);
        let upper = bound_as_slice(&upper);
        let mut rows = table.range::<&[u8]>((lower, upper)).map_err(redb_error)?;
        let mut emitted = 0usize;
        while emitted < opts.limit_rows {
            let Some(row) = rows.next() else {
                return Ok(ScanResult {
                    emitted,
                    has_more: false,
                });
            };
            let (key, value) = row.map_err(redb_error)?;
            visitor.visit(
                KeyRef(&key.value()[4..]),
                project_value_ref(value.value(), opts.projection),
            )?;
            emitted += 1;
        }
        Ok(ScanResult {
            emitted,
            has_more: rows.next().is_some(),
        })
    }
}

impl BackendWrite for RedbWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            table
                .insert(physical_key(space, &entry.key).0.as_ref(), value.as_ref())
                .map_err(redb_error)?;
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        for key in keys {
            table
                .remove(physical_key(space, key).0.as_ref())
                .map_err(redb_error)?;
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let (lower, upper) = encoded_bounds(physical_range(space, range), None);
        let lower = bound_as_slice(&lower);
        let upper = bound_as_slice(&upper);
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        let keys = table
            .range::<&[u8]>((lower, upper))
            .map_err(redb_error)?
            .map(|row| {
                let (key, _value) = row.map_err(redb_error)?;
                Ok::<_, BackendError>(key.value().to_vec())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in &keys {
            table.remove(key.as_slice()).map_err(redb_error)?;
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.deleted_ranges += 1;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.write.commit().map_err(redb_error)?;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        self.write.abort().map_err(redb_error)
    }
}

fn initialize_database(db: &Database) -> Result<(), BackendError> {
    let write = db.begin_write().map_err(redb_error)?;
    {
        let _table = write.open_table(ENTRIES).map_err(redb_error)?;
    }
    write.commit().map_err(redb_error)
}

fn encoded_bounds(range: KeyRange, resume_after: Option<&Key>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let range_lower = match range.lower {
        Bound::Included(key) => Bound::Included(key.0.to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let lower = match resume_after {
        Some(resume_after) => {
            max_lower_bound(range_lower, Bound::Excluded(resume_after.0.to_vec()))
        }
        None => range_lower,
    };

    let upper = match range.upper {
        Bound::Included(key) => Bound::Included(key.0.to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };

    (lower, upper)
}

fn max_lower_bound(left: Bound<Vec<u8>>, right: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (Bound::Included(left), Bound::Included(right)) => {
            Bound::Included(if left >= right { left } else { right })
        }
        (Bound::Included(left), Bound::Excluded(right)) => {
            if left > right {
                Bound::Included(left)
            } else {
                Bound::Excluded(right)
            }
        }
        (Bound::Excluded(left), Bound::Included(right)) => {
            if left >= right {
                Bound::Excluded(left)
            } else {
                Bound::Included(right)
            }
        }
        (Bound::Excluded(left), Bound::Excluded(right)) => {
            Bound::Excluded(if left >= right { left } else { right })
        }
    }
}

fn bound_as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.as_slice()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn redb_error(error: impl std::fmt::Display) -> BackendError {
    BackendError::Io(format!("redb backend: {error}"))
}
