use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetOptions,
    Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions,
    ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions, WriteStats,
};
use rocksdb::Snapshot;
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};

#[derive(Clone, Debug, PartialEq)]
pub struct RocksDbFilesystemBackendOptions {
    pub path: PathBuf,
    pub blob: RocksDbBlobOptions,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RocksDbBlobOptions {
    Disabled,
    Enabled {
        min_blob_size: u64,
        blob_file_size: u64,
        enable_gc: bool,
        gc_age_cutoff: f64,
    },
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct RocksDbFilesystemBackend {
    path: PathBuf,
    db: Arc<DB>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDbFilesystemRead<'a> {
    snapshot: Snapshot<'a>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDbFilesystemWrite {
    db: Arc<DB>,
    batch: WriteBatch,
    staged_put_keys: Vec<Key>,
    stats: WriteStats,
}

impl RocksDbFilesystemBackendOptions {
    pub fn plain(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            blob: RocksDbBlobOptions::Disabled,
        }
    }

    pub fn blob(path: impl Into<PathBuf>, min_blob_size: u64) -> Self {
        Self {
            path: path.into(),
            blob: RocksDbBlobOptions::Enabled {
                min_blob_size,
                blob_file_size: 256 * 1024 * 1024,
                enable_gc: true,
                gc_age_cutoff: 0.25,
            },
        }
    }
}

impl RocksDbFilesystemBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        Self::open_with_options(RocksDbFilesystemBackendOptions::plain(path))
    }

    pub fn open_with_options(
        options: RocksDbFilesystemBackendOptions,
    ) -> Result<Self, BackendError> {
        let db = Arc::new(open_rocksdb(&options)?);
        Ok(Self {
            path: options.path,
            db,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn flush(&self) -> Result<(), BackendError> {
        self.db.flush().map_err(rocksdb_error)
    }

    pub fn compact_all(&self) -> Result<(), BackendError> {
        self.flush()?;
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        self.flush()
    }
}

impl Backend for RocksDbFilesystemBackend {
    type Read<'a>
        = RocksDbFilesystemRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDbFilesystemWrite
    where
        Self: 'a;

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(RocksDbFilesystemRead {
            snapshot: self.db.snapshot(),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(RocksDbFilesystemWrite {
            db: Arc::clone(&self.db),
            batch: WriteBatch::default(),
            staged_put_keys: Vec::new(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for RocksDbFilesystemRead<'_> {
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
        let physical_keys = keys
            .iter()
            .map(|key| physical_key(space, key))
            .collect::<Vec<_>>();
        for (index, (key, value)) in keys
            .iter()
            .zip(
                self.snapshot
                    .multi_get(physical_keys.iter().map(|key| key.0.as_ref())),
            )
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

        let resume_after = opts.resume_after.map(|key| physical_key(space, key));
        let bounds = EncodedBounds::new(physical_range(space, range), resume_after.as_ref());
        let mut emitted = 0usize;

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
            match opts.projection {
                CoreProjection::KeyOnly => {
                    visitor.visit(KeyRef(&encoded_key[4..]), ProjectedValueRef::KeyOnly)?;
                }
                CoreProjection::FullValue => visitor.visit(
                    KeyRef(&encoded_key[4..]),
                    ProjectedValueRef::FullValue(value.as_ref()),
                )?,
            }
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: false,
        })
    }
}

impl BackendWrite for RocksDbFilesystemWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let key = physical_key(space, &entry.key);
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.staged_put_keys.push(key.clone());
            self.batch.put(key.0.as_ref(), value.as_ref());
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.batch.delete(physical_key(space, key).0.as_ref());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let range = physical_range(space, range);
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

fn open_rocksdb(options: &RocksDbFilesystemBackendOptions) -> Result<DB, BackendError> {
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.set_use_fsync(false);
    db_options.set_write_buffer_size(64 * 1024 * 1024);
    match options.blob {
        RocksDbBlobOptions::Disabled => {}
        RocksDbBlobOptions::Enabled {
            min_blob_size,
            blob_file_size,
            enable_gc,
            gc_age_cutoff,
        } => {
            db_options.set_enable_blob_files(true);
            db_options.set_min_blob_size(min_blob_size);
            db_options.set_blob_file_size(blob_file_size);
            db_options.set_enable_blob_gc(enable_gc);
            db_options.set_blob_gc_age_cutoff(gc_age_cutoff);
        }
    }
    DB::open(&db_options, &options.path).map_err(rocksdb_error)
}

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

struct EncodedBounds {
    lower_seek: Vec<u8>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl EncodedBounds {
    fn new(range: KeyRange, resume_after: Option<&Key>) -> Self {
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

fn rocksdb_delete_range_bounds(range: &KeyRange) -> Option<(Vec<u8>, Vec<u8>)> {
    let lower = match &range.lower {
        Bound::Included(key) => key.0.to_vec(),
        Bound::Excluded(key) => next_lexicographic_key(key)?,
        Bound::Unbounded => Vec::new(),
    };
    let upper = match &range.upper {
        Bound::Included(key) => next_lexicographic_key(key)?,
        Bound::Excluded(key) => key.0.to_vec(),
        Bound::Unbounded => return None,
    };

    if lower >= upper {
        None
    } else {
        Some((lower, upper))
    }
}

#[expect(clippy::unnecessary_wraps)]
fn next_lexicographic_key(key: &Key) -> Option<Vec<u8>> {
    let mut bytes = key.0.to_vec();
    bytes.push(0);
    Some(bytes)
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
    BackendError::Io(format!("rocksdb filesystem backend: {error}"))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use lix_engine::backend::{
        Backend, BackendWrite, Key, PutBatch, PutEntry, ReadOptions, SpaceId, StoredValue,
        WriteOptions, get_many,
    };

    use super::{RocksDbFilesystemBackend, RocksDbFilesystemBackendOptions};

    #[test]
    fn plain_backend_roundtrips_point_read_after_reopen() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("fs.rocksdb");
        let key = Key(Bytes::from_static(b"file-chunk"));
        let value = Bytes::from_static(b"chunk bytes");

        {
            let backend = RocksDbFilesystemBackend::open(&path).expect("open backend");
            let mut write = backend
                .begin_write(WriteOptions::default())
                .expect("begin write");
            write
                .put_many(
                    SpaceId(0x0005_0003),
                    PutBatch {
                        entries: vec![PutEntry {
                            key: key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        }],
                    },
                )
                .expect("put chunk");
            write.commit().expect("commit write");
            backend.flush().expect("flush backend");
        }

        let backend = RocksDbFilesystemBackend::open(&path).expect("reopen backend");
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let result =
            get_many(&read, SpaceId(0x0005_0003), &[key], Default::default()).expect("read chunk");
        assert_eq!(result.values.len(), 1);
        assert_eq!(
            result.values[0].as_ref().map(|value| value.as_ref()),
            Some(lix_engine::backend::ProjectedValueRef::FullValue(
                value.as_ref()
            ))
        );
    }

    #[test]
    fn blob_options_open() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let options = RocksDbFilesystemBackendOptions::blob(temp_dir.path().join("fs.rocksdb"), 16);
        let backend =
            RocksDbFilesystemBackend::open_with_options(options).expect("open blob backend");
        backend.flush().expect("flush blob backend");
    }
}
