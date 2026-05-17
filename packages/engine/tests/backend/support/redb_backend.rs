use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
    CommitResult, CoreProjection, GetOptions, Key, KeyRange, KeyRef, PointVisitor,
    ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, StoredValue,
    WriteConcurrency, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use redb::{
    Database, Range as RedbRange, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction as RedbWriteTxn,
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

pub struct RedbRangeScan<'a> {
    rows: RedbRange<'a, &'static [u8], &'static [u8]>,
    projection: CoreProjection,
    pending: Option<RedbPendingRow>,
    done: bool,
}

struct RedbPendingRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
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

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

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

impl BackendRead for RedbRead {
    type RangeScan<'a> = RedbRangeScan<'a>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
        for (index, key) in keys.iter().enumerate() {
            let value = table.get(key.0.as_ref()).map_err(redb_error)?;
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

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
        let (lower, upper) = encoded_bounds(range, opts.resume_after);
        let lower = bound_as_slice(&lower);
        let upper = bound_as_slice(&upper);
        let rows = table.range::<&[u8]>((lower, upper)).map_err(redb_error)?;
        let mut cursor = RedbRangeScan {
            rows,
            projection: opts.projection,
            pending: None,
            done: opts.limit_rows == 0,
        };
        f(&mut cursor)
    }
}

impl BackendRangeScan for RedbRangeScan<'_> {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if limit_rows == 0 || self.done {
            return Ok(ScanResult {
                emitted: 0,
                has_more: !self.done,
            });
        }

        let mut emitted = 0;
        while emitted < limit_rows {
            if let Some(pending) = self.pending.take() {
                visit_redb_pending_row(pending, self.projection, visitor)?;
                emitted += 1;
                continue;
            }

            let Some(row) = self.rows.next() else {
                self.done = true;
                return Ok(ScanResult {
                    emitted,
                    has_more: false,
                });
            };
            let (key, value) = row.map_err(redb_error)?;
            visitor.visit(
                KeyRef(key.value()),
                project_value_ref(value.value(), self.projection),
            )?;
            emitted += 1;
        }

        let has_more = self.ensure_pending()?;
        Ok(ScanResult { emitted, has_more })
    }
}

impl RedbRangeScan<'_> {
    fn ensure_pending(&mut self) -> Result<bool, BackendError> {
        if self.pending.is_some() {
            return Ok(true);
        }
        let Some(row) = self.rows.next() else {
            self.done = true;
            return Ok(false);
        };
        let (key, value) = row.map_err(redb_error)?;
        let value = if matches!(self.projection, CoreProjection::FullValue) {
            Some(value.value().to_vec())
        } else {
            None
        };
        self.pending = Some(RedbPendingRow {
            key: key.value().to_vec(),
            value,
        });
        Ok(true)
    }
}

fn visit_redb_pending_row<V>(
    row: RedbPendingRow,
    projection: CoreProjection,
    visitor: &mut V,
) -> Result<(), BackendError>
where
    V: ScanVisitor + ?Sized,
{
    match projection {
        CoreProjection::KeyOnly => {
            visitor.visit(KeyRef(row.key.as_slice()), ProjectedValueRef::KeyOnly)
        }
        CoreProjection::FullValue => {
            let value = row
                .value
                .as_deref()
                .ok_or_else(|| BackendError::Io("redb pending row missing value".to_string()))?;
            visitor.visit(
                KeyRef(row.key.as_slice()),
                ProjectedValueRef::FullValue(value),
            )
        }
    }
}

impl BackendWrite for RedbWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            table
                .insert(entry.key.0.as_ref(), value.as_ref())
                .map_err(redb_error)?;
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        for key in keys {
            table.remove(key.0.as_ref()).map_err(redb_error)?;
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        let (lower, upper) = encoded_bounds(range, None);
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
    let lower = match (range.lower, resume_after) {
        (_, Some(resume_after)) => Bound::Excluded(resume_after.0.to_vec()),
        (Bound::Included(key), None) => Bound::Included(key.0.to_vec()),
        (Bound::Excluded(key), None) => Bound::Excluded(key.0.to_vec()),
        (Bound::Unbounded, None) => Bound::Unbounded,
    };

    let upper = match range.upper {
        Bound::Included(key) => Bound::Included(key.0.to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };

    (lower, upper)
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
