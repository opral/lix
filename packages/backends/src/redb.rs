#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Backend traits and keep Send guarantees visible"
)]

use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetManyResult,
    GetOptions, Key, KeyRange, ProjectedValue, PutBatch, ReadEntry, ReadOptions, ScanChunk,
    ScanOptions, SpaceId, StoredValue, WriteOptions, WriteStats,
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
#[allow(missing_debug_implementations)]
pub struct RedbBackend {
    path: PathBuf,
    db: Arc<Database>,
}

#[allow(missing_debug_implementations)]
pub struct RedbRead {
    read: ReadTransaction,
}

#[allow(missing_debug_implementations)]
pub struct RedbWrite {
    write: RedbWriteTxn,
    stats: WriteStats,
}

impl Default for RedbBackendFactory {
    fn default() -> Self {
        Self::new()
    }
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

    fn open(&self) -> impl Future<Output = Self::Backend> + Send {
        async move { RedbBackend::open(&self.path).expect("open redb backend") }
    }
}

impl RedbBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        let db = Arc::new(Database::create(&path).map_err(redb_error)?);
        initialize_database(&db)?;
        Ok(Self { path, db })
    }

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
    fn begin_read(
        &self,
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, BackendError>> + Send {
        async move {
            Ok(RedbRead {
                read: self.db.begin_read().map_err(redb_error)?,
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, BackendError>> + Send {
        async move {
            Ok(RedbWrite {
                write: self.db.begin_write().map_err(redb_error)?,
                stats: WriteStats::default(),
            })
        }
    }
}

/// redb keeps its single-table layout; spaces are scoped by prefixing the
/// 4-byte big-endian space id internally. Reads return logical keys.
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
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        async move {
            let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                let value = table
                    .get(physical_key(space, key).0.as_ref())
                    .map_err(redb_error)?;
                values.push(
                    value
                        .as_ref()
                        .map(|value| project_value(value.value(), opts.projection)),
                );
            }
            Ok(GetManyResult::new(values))
        }
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        async move {
            if opts.page_size() == 0 {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }
            let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
            let resume_after = opts
                .resume_after
                .as_ref()
                .map(|key| physical_key(space, key));
            let (lower, upper) =
                encoded_bounds(physical_range(space, range), resume_after.as_ref());
            let lower = bound_as_slice(&lower);
            let upper = bound_as_slice(&upper);
            let mut rows = table.range::<&[u8]>((lower, upper)).map_err(redb_error)?;
            let mut entries = Vec::with_capacity(opts.page_size());
            while entries.len() < opts.page_size() {
                let Some(row) = rows.next() else {
                    return Ok(ScanChunk {
                        entries,
                        has_more: false,
                    });
                };
                let (key, value) = row.map_err(redb_error)?;
                entries.push(ReadEntry {
                    key: Key(Bytes::copy_from_slice(&key.value()[4..])),
                    value: project_value(value.value(), opts.projection),
                });
            }
            let has_more = match rows.next() {
                Some(row) => {
                    let _ = row.map_err(redb_error)?;
                    true
                }
                None => false,
            };
            Ok(ScanChunk { entries, has_more })
        }
    }
}

impl BackendWrite for RedbWrite {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
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
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
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
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
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
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, BackendError>> + Send {
        async move {
            self.write.commit().map_err(redb_error)?;
            Ok(CommitResult {
                commit_id: None,
                stats: self.stats,
            })
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move { self.write.abort().map_err(redb_error) }
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

fn project_value(value: &[u8], projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::copy_from_slice(value)),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn redb_error(error: impl std::fmt::Display) -> BackendError {
    BackendError::Io(format!("redb backend: {error}"))
}
