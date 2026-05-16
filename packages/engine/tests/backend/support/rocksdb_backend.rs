use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use lix_engine::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetOptions, Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId, StoredValue, WriteConcurrency,
    WriteOptions, WriteStats,
};
use lix_engine::{BackendV2Factory, BackendV2Fixture, BackendV2TestConfig};
use rocksdb::Snapshot;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use tempfile::TempDir;

#[derive(Debug)]
pub struct RocksDbBackendFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct RocksDbBackendFixture {
    path: PathBuf,
}

#[derive(Clone)]
pub struct RocksDbBackend {
    path: PathBuf,
    db: Arc<DB>,
}

pub struct RocksDbRead<'a> {
    snapshot: Snapshot<'a>,
}

pub struct RocksDbWrite {
    db: Arc<DB>,
    batch: WriteBatch,
    stats: WriteStats,
}

impl RocksDbBackendFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create rocksdb backend temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl BackendV2Factory for RocksDbBackendFactory {
    type Backend = RocksDbBackend;
    type Fixture = RocksDbBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("backend-{database_id}.rocksdb"));
        RocksDbBackendFixture { path }
    }

    fn config(&self) -> BackendV2TestConfig {
        BackendV2TestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendV2TestConfig::default()
        }
    }
}

impl BackendV2Fixture for RocksDbBackendFixture {
    type Backend = RocksDbBackend;

    fn open(&self) -> Self::Backend {
        RocksDbBackend::open(&self.path).expect("open rocksdb backend")
    }
}

impl RocksDbBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        let db = Arc::new(open_rocksdb(&path)?);
        Ok(Self { path, db })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub fn flush(&self) -> Result<(), BackendError> {
        self.db.flush().map_err(rocksdb_error)
    }
}

impl Backend for RocksDbBackend {
    type Read<'a>
        = RocksDbRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDbWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(RocksDbRead {
            snapshot: self.db.snapshot(),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(RocksDbWrite {
            db: Arc::clone(&self.db),
            batch: WriteBatch::default(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for RocksDbRead<'_> {
    fn visit_many<V>(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let encoded_keys = keys
            .iter()
            .map(|key| encode_entry_key(space, key))
            .collect::<Vec<_>>();
        for (index, (key, value)) in keys
            .iter()
            .zip(self.snapshot.multi_get(encoded_keys.iter()).into_iter())
            .enumerate()
        {
            let value = value.map_err(rocksdb_error)?;
            visitor.visit(
                index,
                key,
                value
                    .as_ref()
                    .map(|value| project_value_ref(value.as_ref(), opts.projection)),
            )?;
        }
        Ok(())
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
        if opts.limit_rows == 0 {
            return Ok(ScanResult::default());
        }

        let bounds = EncodedBounds::new(space, range, opts.resume_after);
        let mut emitted = 0;
        for item in self
            .snapshot
            .iterator(IteratorMode::From(&bounds.lower_seek, Direction::Forward))
        {
            let (encoded_key, value) = item.map_err(rocksdb_error)?;
            let encoded_key = encoded_key.as_ref();
            if !bounds.after_lower(encoded_key) {
                continue;
            }
            if !bounds.before_upper(encoded_key) {
                break;
            }
            if emitted == opts.limit_rows {
                return Ok(ScanResult {
                    emitted,
                    has_more: true,
                });
            }

            let key = decode_entry_key_ref(encoded_key)?;
            match opts.projection {
                CoreProjection::KeyOnly => visitor.visit(key, ProjectedValueRef::KeyOnly)?,
                CoreProjection::FullValue => {
                    visitor.visit(key, ProjectedValueRef::FullValue(value.as_ref()))?;
                }
            }
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: false,
        })
    }
}

impl BackendWrite for RocksDbWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let key = encode_entry_key(space, &entry.key);
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.batch.put(key, value.as_ref());
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.batch.delete(encode_entry_key(space, key));
        }
        self.stats.deleted_entries += keys.len() as u64;
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
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl EncodedBounds {
    fn new(space: SpaceId, range: KeyRange, resume_after: Option<&Key>) -> Self {
        let lower = match (range.lower, resume_after) {
            (_, Some(resume_after)) => Bound::Excluded(encode_entry_key(space, resume_after)),
            (Bound::Included(key), None) => Bound::Included(encode_entry_key(space, &key)),
            (Bound::Excluded(key), None) => Bound::Excluded(encode_entry_key(space, &key)),
            (Bound::Unbounded, None) => Bound::Included(space.0.to_be_bytes().to_vec()),
        };

        let upper = match range.upper {
            Bound::Included(key) => Bound::Included(encode_entry_key(space, &key)),
            Bound::Excluded(key) => Bound::Excluded(encode_entry_key(space, &key)),
            Bound::Unbounded => Bound::Excluded(space_upper_bound(space)),
        };

        let lower_seek = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => key.clone(),
            Bound::Unbounded => space.0.to_be_bytes().to_vec(),
        };

        Self {
            lower_seek,
            lower,
            upper,
        }
    }

    fn after_lower(&self, encoded_key: &[u8]) -> bool {
        match &self.lower {
            Bound::Included(lower) if encoded_key < lower.as_slice() => false,
            Bound::Excluded(lower) if encoded_key <= lower.as_slice() => false,
            _ => true,
        }
    }

    fn before_upper(&self, encoded_key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(upper) => encoded_key <= upper.as_slice(),
            Bound::Excluded(upper) => encoded_key < upper.as_slice(),
            Bound::Unbounded => true,
        }
    }

    #[allow(dead_code)]
    fn contains(&self, encoded_key: &[u8]) -> bool {
        if !self.after_lower(encoded_key) {
            return false;
        }
        match &self.upper {
            Bound::Included(upper) => encoded_key <= upper.as_slice(),
            Bound::Excluded(upper) => encoded_key < upper.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

fn open_rocksdb(path: &Path) -> Result<DB, BackendError> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_write_buffer_size(64 * 1024 * 1024);
    DB::open(&options, path).map_err(rocksdb_error)
}

fn encode_entry_key(space: SpaceId, key: &Key) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + key.0.len());
    encoded.extend_from_slice(&space.0.to_be_bytes());
    encoded.extend_from_slice(key.0.as_ref());
    encoded
}

fn decode_entry_key_ref(encoded: &[u8]) -> Result<KeyRef<'_>, BackendError> {
    if encoded.len() < 4 {
        return Err(BackendError::Corruption(
            "rocksdb entry key shorter than space prefix".into(),
        ));
    }
    Ok(KeyRef(&encoded[4..]))
}

fn space_upper_bound(space: SpaceId) -> Vec<u8> {
    if space.0 == u32::MAX {
        vec![0xff, 0xff, 0xff, 0xff, 0xff]
    } else {
        (space.0 + 1).to_be_bytes().to_vec()
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn rocksdb_error(error: rocksdb::Error) -> BackendError {
    BackendError::Io(format!("rocksdb backend: {error}"))
}
