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
use redb::{
    Database, ReadTransaction, ReadableDatabase, TableDefinition, WriteTransaction as RedbWriteTxn,
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

impl BackendV2Factory for RedbBackendFactory {
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

    fn config(&self) -> BackendV2TestConfig {
        BackendV2TestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendV2TestConfig::default()
        }
    }
}

impl BackendV2Fixture for RedbBackendFixture {
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
        let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
        for (index, key) in keys.iter().enumerate() {
            let encoded = encode_entry_key(space, key);
            let value = table.get(encoded.as_slice()).map_err(redb_error)?;
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

        let table = self.read.open_table(ENTRIES).map_err(redb_error)?;
        let (lower, upper) = encoded_bounds(space, range, opts.resume_after);
        let lower = bound_as_slice(&lower);
        let upper = bound_as_slice(&upper);
        let mut emitted = 0;
        let mut rows = table.range::<&[u8]>((lower, upper)).map_err(redb_error)?;

        while let Some(row) = rows.next() {
            let (key, value) = row.map_err(redb_error)?;
            if emitted == opts.limit_rows {
                return Ok(ScanResult {
                    emitted,
                    has_more: true,
                });
            }

            let key = decode_entry_key_ref(key.value())?;
            visitor.visit(key, project_value_ref(value.value(), opts.projection))?;
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: false,
        })
    }
}

impl BackendWrite for RedbWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        for entry in entries.entries {
            let key = encode_entry_key(space, &entry.key);
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            table
                .insert(key.as_slice(), value.as_ref())
                .map_err(redb_error)?;
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        let mut table = self.write.open_table(ENTRIES).map_err(redb_error)?;
        for key in keys {
            let encoded = encode_entry_key(space, key);
            table.remove(encoded.as_slice()).map_err(redb_error)?;
        }
        self.stats.deleted_entries += keys.len() as u64;
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

fn encode_entry_key(space: SpaceId, key: &Key) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + key.0.len());
    encoded.extend_from_slice(&space.0.to_be_bytes());
    encoded.extend_from_slice(key.0.as_ref());
    encoded
}

fn decode_entry_key_ref(encoded: &[u8]) -> Result<KeyRef<'_>, BackendError> {
    if encoded.len() < 4 {
        return Err(BackendError::Corruption(
            "redb entry key shorter than space prefix".into(),
        ));
    }
    Ok(KeyRef(&encoded[4..]))
}

fn encoded_bounds(
    space: SpaceId,
    range: KeyRange,
    resume_after: Option<&Key>,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let space_prefix = space.0.to_be_bytes();
    let lower = match (range.lower, resume_after) {
        (_, Some(resume_after)) => Bound::Excluded(encode_entry_key(space, resume_after)),
        (Bound::Included(key), None) => Bound::Included(encode_entry_key(space, &key)),
        (Bound::Excluded(key), None) => Bound::Excluded(encode_entry_key(space, &key)),
        (Bound::Unbounded, None) => Bound::Included(space_prefix.to_vec()),
    };

    let mut next_space = (space.0 + 1).to_be_bytes().to_vec();
    if space.0 == u32::MAX {
        next_space = vec![0xff, 0xff, 0xff, 0xff, 0xff];
    }
    let upper = match range.upper {
        Bound::Included(key) => Bound::Included(encode_entry_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(encode_entry_key(space, &key)),
        Bound::Unbounded => Bound::Excluded(next_space),
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
