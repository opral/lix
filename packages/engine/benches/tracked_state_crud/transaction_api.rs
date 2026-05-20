use bytes::Bytes;
use lix_engine::backend::WriteConcurrency;
use lix_engine::storage::StorageContext;
use lix_engine::transaction::bench::{
    BenchLayoutAccounting, BenchTransactionFixture, BenchTransactionRow, BenchWriteAccounting,
};
use lix_engine::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, BufferedRangeScan,
    CommitResult, CoreProjection, DurableWriteLock, Key, KeyRange, PointVisitor, ProjectedValue,
    PutBatch, ReadEntry, ReadOptions, ScanOptions, StoredValue, WriteOptions, WriteStats,
};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use std::sync::{Arc, Mutex};

use crate::backends::{BackendProfile, ProfileBackend, RedbBackend, SqliteBackend};
use crate::workload::{snapshot_value, WorkloadRow};

pub(crate) enum TransactionFixture {
    Sqlite(BenchTransactionFixture<BenchBackend<SqliteBackend>>),
    RocksDb(BenchTransactionFixture<OwnedRocksDbBackend>),
    Redb(BenchTransactionFixture<BenchBackend<RedbBackend>>),
}

pub(crate) type TransactionWriteAccounting = BenchWriteAccounting;
pub(crate) type TransactionLayoutAccounting = BenchLayoutAccounting;

pub(crate) async fn empty_fixture(
    profile: BackendProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let rows = bench_rows(rows);
    match profile.backend() {
        ProfileBackend::Sqlite(backend) => TransactionFixture::Sqlite(
            BenchTransactionFixture::new(StorageContext::new(BenchBackend::new(backend)), rows)
                .await,
        ),
        ProfileBackend::RocksDb(_backend) => {
            let dir = tempfile::TempDir::new().expect("create owned rocksdb transaction tempdir");
            let backend = OwnedRocksDbBackend::open(dir.keep().join("bench.rocksdb"));
            TransactionFixture::RocksDb(
                BenchTransactionFixture::new(StorageContext::new(backend), rows).await,
            )
        }
        ProfileBackend::Redb(backend) => TransactionFixture::Redb(
            BenchTransactionFixture::new(StorageContext::new(BenchBackend::new(backend)), rows)
                .await,
        ),
    }
}

#[derive(Clone)]
pub(crate) struct BenchBackend<B> {
    inner: B,
}

impl<B> BenchBackend<B> {
    fn new(inner: B) -> Self {
        Self { inner }
    }
}

impl<B> Backend for BenchBackend<B>
where
    B: Backend + Clone,
    for<'a> B::Read<'a>: Send + 'a,
{
    type Read<'a>
        = BenchRead<B::Read<'a>>
    where
        Self: 'a;

    type Write<'a>
        = B::Write<'a>
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(BenchRead::new(self.inner.begin_read(opts)?))
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.inner.begin_write(opts)
    }

    fn durable_write_lock(&self) -> DurableWriteLock {
        self.inner.durable_write_lock()
    }
}

pub(crate) struct BenchRead<R> {
    inner: Arc<Mutex<R>>,
}

impl<R> Clone for BenchRead<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R> BenchRead<R> {
    fn new(inner: R) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

unsafe impl<R: Send> Send for BenchRead<R> {}
unsafe impl<R: Send> Sync for BenchRead<R> {}

impl<R> BackendRead for BenchRead<R>
where
    R: BackendRead + Send,
{
    type RangeScan<'cursor> = R::RangeScan<'cursor>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: lix_engine::GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        self.inner
            .lock()
            .expect("bench read mutex should not poison")
            .visit_keys(keys, opts, visitor)
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
        self.inner
            .lock()
            .expect("bench read mutex should not poison")
            .with_range_scan(range, opts, f)
    }
}

pub(crate) async fn seeded_fixture(
    profile: BackendProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let mut fixture = empty_fixture(profile, rows).await;
    fixture.seed().await;
    fixture
}

impl TransactionFixture {
    pub(crate) async fn seed(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.seed().await,
            Self::RocksDb(fixture) => fixture.seed().await,
            Self::Redb(fixture) => fixture.seed().await,
        }
    }

    pub(crate) async fn insert_all(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.insert_all().await,
            Self::RocksDb(fixture) => fixture.insert_all().await,
            Self::Redb(fixture) => fixture.insert_all().await,
        }
    }

    pub(crate) async fn insert_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.insert_all_accounting().await,
            Self::RocksDb(fixture) => fixture.insert_all_accounting().await,
            Self::Redb(fixture) => fixture.insert_all_accounting().await,
        }
    }

    pub(crate) async fn read_all(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_all().await,
            Self::RocksDb(fixture) => fixture.read_all().await,
            Self::Redb(fixture) => fixture.read_all().await,
        }
    }

    pub(crate) async fn read_all_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_all_by_pk().await,
            Self::RocksDb(fixture) => fixture.read_all_by_pk().await,
            Self::Redb(fixture) => fixture.read_all_by_pk().await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.read_one_by_pk().await,
            Self::Redb(fixture) => fixture.read_one_by_pk().await,
        }
    }

    pub(crate) async fn update_all(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.update_all().await,
            Self::RocksDb(fixture) => fixture.update_all().await,
            Self::Redb(fixture) => fixture.update_all().await,
        }
    }

    pub(crate) async fn update_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.update_all_accounting().await,
            Self::RocksDb(fixture) => fixture.update_all_accounting().await,
            Self::Redb(fixture) => fixture.update_all_accounting().await,
        }
    }

    pub(crate) async fn update_one_by_pk(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.update_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.update_one_by_pk().await,
            Self::Redb(fixture) => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn update_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.update_one_by_pk_accounting().await,
            Self::RocksDb(fixture) => fixture.update_one_by_pk_accounting().await,
            Self::Redb(fixture) => fixture.update_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn delete_all(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.delete_all().await,
            Self::RocksDb(fixture) => fixture.delete_all().await,
            Self::Redb(fixture) => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.delete_all_accounting().await,
            Self::RocksDb(fixture) => fixture.delete_all_accounting().await,
            Self::Redb(fixture) => fixture.delete_all_accounting().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.delete_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.delete_one_by_pk().await,
            Self::Redb(fixture) => fixture.delete_one_by_pk().await,
        }
    }

    pub(crate) async fn delete_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::RocksDb(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::Redb(fixture) => fixture.delete_one_by_pk_accounting().await,
        }
    }

    pub(crate) fn layout_accounting(&self) -> Vec<TransactionLayoutAccounting> {
        match self {
            Self::Sqlite(fixture) => fixture.layout_accounting(),
            Self::RocksDb(fixture) => fixture.layout_accounting(),
            Self::Redb(fixture) => fixture.layout_accounting(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct OwnedRocksDbBackend {
    db: Arc<DB>,
    durable_write_lock: DurableWriteLock,
}

impl OwnedRocksDbBackend {
    fn open(path: impl AsRef<std::path::Path>) -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_use_fsync(false);
        options.set_write_buffer_size(64 * 1024 * 1024);
        let db = DB::open(&options, path).expect("open owned rocksdb transaction bench");
        Self {
            db: Arc::new(db),
            durable_write_lock: DurableWriteLock::new(),
        }
    }
}

impl Backend for OwnedRocksDbBackend {
    type Read<'a>
        = OwnedRocksDbRead
    where
        Self: 'a;

    type Write<'a>
        = OwnedRocksDbWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(OwnedRocksDbRead {
            db: Arc::clone(&self.db),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(OwnedRocksDbWrite {
            db: Arc::clone(&self.db),
            batch: WriteBatch::default(),
            staged_put_keys: Vec::new(),
            stats: WriteStats::default(),
        })
    }

    fn durable_write_lock(&self) -> DurableWriteLock {
        self.durable_write_lock.clone()
    }
}

#[derive(Clone)]
pub(crate) struct OwnedRocksDbRead {
    db: Arc<DB>,
}

impl BackendRead for OwnedRocksDbRead {
    type RangeScan<'cursor> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: lix_engine::GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        for (index, (key, value)) in keys
            .iter()
            .zip(
                self.db
                    .multi_get(keys.iter().map(|key| key.0.as_ref()))
                    .into_iter(),
            )
            .enumerate()
        {
            let value = value.map_err(rocksdb_error)?;
            visitor.visit(
                index,
                key,
                value.as_ref().map(|value| match opts.projection {
                    CoreProjection::KeyOnly => lix_engine::ProjectedValueRef::KeyOnly,
                    CoreProjection::FullValue => {
                        lix_engine::ProjectedValueRef::FullValue(value.as_ref())
                    }
                }),
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
        let bounds = EncodedBounds::new(range, opts.resume_after);
        let mut rows = Vec::new();
        if opts.limit_rows != 0 {
            for item in self
                .db
                .iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward))
            {
                let (key, value) = item.map_err(rocksdb_error)?;
                let key_ref = key.as_ref();
                if !bounds.after_lower(key_ref) {
                    continue;
                }
                if !bounds.before_upper(key_ref) {
                    break;
                }
                rows.push(ReadEntry {
                    key: Key(Bytes::copy_from_slice(key_ref)),
                    value: match opts.projection {
                        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                        CoreProjection::FullValue => {
                            ProjectedValue::FullValue(Bytes::copy_from_slice(value.as_ref()))
                        }
                    },
                });
            }
        }
        let mut scan = BufferedRangeScan::new(rows);
        f(&mut scan)
    }
}

pub(crate) struct OwnedRocksDbWrite {
    db: Arc<DB>,
    batch: WriteBatch,
    staged_put_keys: Vec<Key>,
    stats: WriteStats,
}

impl BackendWrite for OwnedRocksDbWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.staged_put_keys.push(entry.key.clone());
            self.batch.put(entry.key.0.as_ref(), value.as_ref());
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.batch.delete(key.0.as_ref());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        if let Some((lower, upper)) = rocksdb_delete_range_bounds(&range) {
            self.batch.delete_range(lower.as_slice(), upper.as_slice());
        } else {
            let bounds = EncodedBounds::new(range, None);
            for item in self
                .db
                .iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward))
            {
                let (encoded_key, _value) = item.map_err(rocksdb_error)?;
                let encoded_key = encoded_key.as_ref();
                if !bounds.after_lower(encoded_key) {
                    continue;
                }
                if !bounds.before_upper(encoded_key) {
                    break;
                }
                self.batch.delete(encoded_key);
            }

            for key in &self.staged_put_keys {
                if bounds.contains(key.0.as_ref()) {
                    self.batch.delete(key.0.as_ref());
                }
            }
        }

        self.stats.deleted_ranges += 1;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.db.write(self.batch).map_err(rocksdb_error)?;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

struct EncodedBounds {
    lower_seek: Vec<u8>,
    lower: std::ops::Bound<Vec<u8>>,
    upper: std::ops::Bound<Vec<u8>>,
}

impl EncodedBounds {
    fn new(range: KeyRange, resume_after: Option<&Key>) -> Self {
        use std::ops::Bound;

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

        let lower_seek = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => key.clone(),
            Bound::Unbounded => Vec::new(),
        };

        Self {
            lower_seek,
            lower,
            upper,
        }
    }

    fn after_lower(&self, encoded_key: &[u8]) -> bool {
        use std::ops::Bound;

        match &self.lower {
            Bound::Included(lower) if encoded_key < lower.as_slice() => false,
            Bound::Excluded(lower) if encoded_key <= lower.as_slice() => false,
            _ => true,
        }
    }

    fn before_upper(&self, encoded_key: &[u8]) -> bool {
        use std::ops::Bound;

        match &self.upper {
            Bound::Included(upper) => encoded_key <= upper.as_slice(),
            Bound::Excluded(upper) => encoded_key < upper.as_slice(),
            Bound::Unbounded => true,
        }
    }

    fn contains(&self, encoded_key: &[u8]) -> bool {
        self.after_lower(encoded_key) && self.before_upper(encoded_key)
    }
}

fn max_lower_bound(
    left: std::ops::Bound<Vec<u8>>,
    right: std::ops::Bound<Vec<u8>>,
) -> std::ops::Bound<Vec<u8>> {
    use std::ops::Bound;

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

fn rocksdb_delete_range_bounds(range: &KeyRange) -> Option<(Vec<u8>, Vec<u8>)> {
    use std::ops::Bound;

    let lower = match &range.lower {
        Bound::Included(key) => key.0.to_vec(),
        Bound::Excluded(key) => next_lexicographic_key(key),
        Bound::Unbounded => Vec::new(),
    };
    let upper = match &range.upper {
        Bound::Included(key) => next_lexicographic_key(key),
        Bound::Excluded(key) => key.0.to_vec(),
        Bound::Unbounded => return None,
    };

    if lower >= upper {
        None
    } else {
        Some((lower, upper))
    }
}

fn next_lexicographic_key(key: &Key) -> Vec<u8> {
    let mut bytes = key.0.to_vec();
    bytes.push(0);
    bytes
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn rocksdb_error(error: rocksdb::Error) -> BackendError {
    BackendError::Io(format!("owned rocksdb transaction bench: {error}"))
}

fn bench_rows(rows: &[WorkloadRow]) -> Vec<BenchTransactionRow> {
    rows.iter()
        .map(|row| BenchTransactionRow {
            schema_key: "json_pointer".to_string(),
            file_id: None,
            entity_id: row.path.clone(),
            value: serde_json::from_str(&snapshot_value(&row.path, &row.value_json))
                .expect("transaction bench value should parse"),
            updated_value: serde_json::from_str(&snapshot_value(
                &row.path,
                &row.updated_value_json,
            ))
            .expect("transaction bench updated value should parse"),
        })
        .collect()
}
