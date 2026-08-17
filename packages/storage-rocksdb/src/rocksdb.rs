#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::HashMap;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[cfg(test)]
use bytes::Buf as _;
use bytes::Bytes;
use bytes::BytesMut;
use lix::storage::conformance::{StorageFactory, StorageFixture, StorageTestConfig};
use lix::storage::immutable::validate_immutable_batch;
use lix::storage::{
    BeginScanOptions, Capability, CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key,
    KeyRange, Precondition, PreconditionFailure, ProjectedValue, PutBatch, ReadDurability,
    ReadEntry, ReadOptions, ScanChunk, ScanCursor, ScanOrder, SpaceId, Storage, StorageError,
    StorageRead, StorageScanSource, StorageSpace, StorageWrite, StoredValue, ValueIntegrity,
    ValueSemantics, WriteOptions, WriteStats,
};
use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options,
    WriteBatch, WriteOptions as RocksDBWriteOptions,
};
use rocksdb::{DBRawIteratorWithThreadMode, Snapshot};
use tempfile::TempDir;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

const WRITE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
/// Spare memtables per column family. Four buffers cap resident memtable memory
/// at `WRITE_BUFFER_BYTES * WRITE_BUFFER_COUNT` per family (512 MiB across the
/// two families) and are what keeps a flush off the next writer's latency.
const WRITE_BUFFER_COUNT: i32 = 4;
const DEFAULT_BLOB_MIN_SIZE: u64 = 32 * 1024;
const DEFAULT_BLOB_FILE_SIZE: u64 = 256 * 1024 * 1024;
const BLOB_GC_FORCE_THRESHOLD: f64 = 0.5;
const MUTABLE_COLUMN_FAMILY: &str = "default";
const IMMUTABLE_COLUMN_FAMILY: &str = "lix-immutable-v1";

#[derive(Debug)]
pub struct RocksDBFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct RocksDBFixture {
    path: PathBuf,
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct RocksDB {
    path: PathBuf,
    inner: Arc<RocksDBInner>,
}

#[allow(missing_debug_implementations)]
struct RocksDBInner {
    db: DB,
    write_gate: WriteGate,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBRead<'a> {
    db: &'a DB,
    snapshot: Snapshot<'a>,
}

#[allow(missing_debug_implementations)]
pub struct RocksDBWrite {
    inner: Arc<RocksDBInner>,
    _writer_permit: OwnedMutexGuard<()>,
    batch: WriteBatch,
    immutable_values: HashMap<Vec<u8>, Bytes>,
    stats: WriteStats,
    /// Mirrors [`WriteOptions::await_durable`]. `commit()` does not otherwise
    /// see the options it was opened with, which is how this adapter came to
    /// accept the flag and silently drop it.
    await_durable: bool,
}

static OPEN_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<RocksDBInner>>>> = OnceLock::new();

impl Default for RocksDBFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RocksDBFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create rocksdb storage temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl StorageFactory for RocksDBFactory {
    type Storage = RocksDB;
    type Fixture = RocksDBFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("storage-{database_id}.rocksdb"));
        RocksDBFixture { path }
    }

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..StorageTestConfig::default()
        }
    }
}

impl StorageFixture for RocksDBFixture {
    type Storage = RocksDB;

    fn open(&self) -> impl Future<Output = Self::Storage> + Send {
        async move { RocksDB::open(&self.path).expect("open rocksdb storage") }
    }
}

impl RocksDB {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        Ok(Self {
            inner: open_shared_rocksdb(path.clone())?,
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        for name in [MUTABLE_COLUMN_FAMILY, IMMUTABLE_COLUMN_FAMILY] {
            let cf = self
                .inner
                .db
                .cf_handle(name)
                .expect("configured column family is open");
            self.inner.db.flush_cf(cf).map_err(rocksdb_error)?;
        }
        Ok(())
    }

    /// Flushes the WAL without forcing the active memtable into an SST.
    ///
    /// Storage lifecycle benchmarks use this to distinguish durable,
    /// memory-resident reads from memtable-flushed reads.
    #[doc(hidden)]
    pub fn flush_wal_for_diagnostics(&self) -> Result<(), StorageError> {
        self.inner.db.flush_wal(true).map_err(rocksdb_error)
    }
}

impl Storage for RocksDB {
    type Read<'a>
        = RocksDBRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = RocksDBWrite
    where
        Self: 'a;
    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            // Capture the sequence first, then fence the WAL. A flush-before-
            // snapshot ordering has a race where a concurrent write can land
            // between the fence and snapshot and be returned by a supposedly
            // durable read before its WAL is synced. The snapshot-before-
            // fence order makes the read view immutable while the fence
            // guarantees every write visible in that view has crossed the
            // backend's durable boundary.
            let snapshot = self.inner.db.snapshot();
            if opts.durability == ReadDurability::Durable {
                // RocksDB's durable write boundary is its synced WAL. A
                // durable read must fence the WAL before returning its
                // snapshot; otherwise an idempotency receipt that was
                // published with `await_durable` would still be treated as
                // unknowable by retry recovery on the FilesystemStorage
                // adapter.
                self.inner.db.flush_wal(true).map_err(rocksdb_error)?;
            }
            Ok(RocksDBRead {
                db: &self.inner.db,
                snapshot,
            })
        }
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            let writer_permit = self.inner.write_gate.acquire().await;
            check_preconditions(&self.inner.db, &opts.preconditions)?;
            Ok(RocksDBWrite {
                inner: Arc::clone(&self.inner),
                _writer_permit: writer_permit,
                batch: if opts.batch_capacity_hint_bytes == 0 {
                    WriteBatch::default()
                } else {
                    WriteBatch::with_capacity_bytes(opts.batch_capacity_hint_bytes)
                },
                immutable_values: HashMap::new(),
                stats: WriteStats::default(),
                await_durable: opts.await_durable,
            })
        }
    }
}

fn check_preconditions(db: &DB, preconditions: &[Precondition]) -> Result<(), StorageError> {
    let mut failures = Vec::new();
    for (index, precondition) in preconditions.iter().enumerate() {
        let matches = match precondition {
            Precondition::KeyAbsent { space, key } => !key_exists(
                db,
                column_family(db, *space),
                &physical_key(space.id, key).0,
            )?,
            Precondition::KeyPresent { space, key } => key_exists(
                db,
                column_family(db, *space),
                &physical_key(space.id, key).0,
            )?,
            Precondition::KeyValueHashEquals { space, key, hash } => db
                .get_cf(column_family(db, *space), physical_key(space.id, key).0)
                .map_err(rocksdb_error)?
                .is_some_and(|value| blake3::hash(&value).as_bytes() == hash),
            Precondition::KeyValueEquals {
                space,
                key,
                expected,
            } => db
                .get_cf(column_family(db, *space), physical_key(space.id, key).0)
                .map_err(rocksdb_error)?
                .is_some_and(|value| value.as_slice() == expected.as_ref()),
            Precondition::RangeEmpty { space, range } => {
                let bounds = EncodedBounds::new(physical_range(space.id, range.clone()));
                range_is_empty(db, column_family(db, *space), &bounds)?
            }
            Precondition::BranchEquals { ref_key, expected } => db
                .get_cf(mutable_column_family(db), ref_key.0.as_ref())
                .map_err(rocksdb_error)?
                .is_some_and(|value| value.as_slice() == expected.as_ref()),
        };
        if !matches {
            failures.push(PreconditionFailure { index });
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed(failures))
    }
}

/// Read options for a full-value read of `space`.
///
/// RocksDB verifies a CRC32C over every block and every blob-file record it
/// reads. For a [`ValueIntegrity::ContentAddressed`] space that is a strictly
/// weaker duplicate of a check the engine has already made unconditional: the
/// key *is* the BLAKE3-256 digest of the value, and the engine recomputes and
/// compares it before the bytes escape the read. Corruption that RocksDB's
/// CRC32C would have caught is caught by the digest instead — including the
/// cases CRC32C cannot distinguish — so the second pass buys nothing and costs
/// a full sweep over every payload byte.
///
/// It is worth stating what is *not* claimed. Skipping verification means a
/// corrupt block reaches the engine before it is rejected, so the failure mode
/// moves from RocksDB's error to the engine's content-address error. The bytes
/// never escape either way. `verify_checksums` also covers this column
/// family's index and filter blocks; a corrupt index can only send the read to
/// the wrong key, and the wrong payload fails the digest check for the key that
/// was actually asked for.
///
/// Every other space gets RocksDB's default, which verifies.
fn value_read_options(space: StorageSpace) -> rocksdb::ReadOptions {
    let mut options = rocksdb::ReadOptions::default();
    if space.value_integrity == ValueIntegrity::ContentAddressed {
        options.set_verify_checksums(false);
    }
    options
}

fn column_family(db: &DB, space: StorageSpace) -> &ColumnFamily {
    match space.value_semantics {
        ValueSemantics::Mutable => mutable_column_family(db),
        ValueSemantics::Immutable => db
            .cf_handle(IMMUTABLE_COLUMN_FAMILY)
            .expect("immutable column family is opened with the database"),
    }
}

fn mutable_column_family(db: &DB) -> &ColumnFamily {
    db.cf_handle(MUTABLE_COLUMN_FAMILY)
        .expect("default column family is opened with the database")
}

fn key_exists(db: &DB, cf: &ColumnFamily, key: &[u8]) -> Result<bool, StorageError> {
    let mut iterator = db.raw_iterator_cf(cf);
    iterator.seek(key);
    iterator.status().map_err(rocksdb_error)?;
    Ok(iterator.key().is_some_and(|candidate| candidate == key))
}

fn range_is_empty(
    db: &DB,
    cf: &ColumnFamily,
    bounds: &EncodedBounds,
) -> Result<bool, StorageError> {
    let mut iterator = db.raw_iterator_cf(cf);
    iterator.seek(&bounds.lower_seek);
    while let Some(key) = iterator.key() {
        if bounds.after_lower(key) {
            return Ok(!bounds.before_upper(key));
        }
        iterator.next();
    }
    iterator.status().map_err(rocksdb_error)?;
    Ok(true)
}

/// Spaces share the column family for their value semantics and are scoped by
/// a four-byte big-endian space id prefix within that physical domain.
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

impl StorageRead for RocksDBRead<'_> {
    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            let key_count = requests.iter().map(|request| request.keys.len()).sum();
            let mut results = Vec::with_capacity(key_count);
            for request in requests {
                let cf = column_family(self.db, request.space);
                let physical_keys = request
                    .keys
                    .iter()
                    .map(|key| physical_key(request.space.id, key))
                    .collect::<Vec<_>>();
                match request.opts.projection {
                    CoreProjection::KeyOnly => {
                        for key in &physical_keys {
                            let mut iterator = self.snapshot.raw_iterator_cf(cf);
                            iterator.seek(key.0.as_ref());
                            iterator.status().map_err(rocksdb_error)?;
                            results.push(
                                iterator
                                    .key()
                                    .is_some_and(|candidate| candidate == key.0.as_ref())
                                    .then_some(ProjectedValue::KeyOnly),
                            );
                        }
                    }
                    CoreProjection::FullValue => {
                        let values = self.snapshot.multi_get_cf_opt(
                            physical_keys.iter().map(|key| (cf, key.0.as_ref())),
                            value_read_options(request.space),
                        );
                        results.extend(
                            values
                                .into_iter()
                                .map(|value| {
                                    value.map_err(rocksdb_error).map(|value| {
                                        value.map(|value| ProjectedValue::FullValue(value.into()))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        );
                    }
                }
            }
            Ok(GetManyResult::new(results))
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        async move {
            ScanCursor::validate_range(&range)?;
            if opts.order == ScanOrder::Descending {
                return Err(StorageError::Unsupported(Capability::ReverseScan));
            }
            let bounds = EncodedBounds::new(physical_range(space.id, range.clone()));
            let mut iterator = match opts.projection {
                // A key-only scan never materializes a value, so there is no
                // value checksum to skip and the default options are right.
                CoreProjection::KeyOnly => {
                    self.snapshot.raw_iterator_cf(column_family(self.db, space))
                }
                CoreProjection::FullValue => self
                    .snapshot
                    .raw_iterator_cf_opt(column_family(self.db, space), value_read_options(space)),
            };
            iterator.seek(&bounds.lower_seek);
            iterator.status().map_err(rocksdb_error)?;
            ScanCursor::from_source(
                range,
                opts.order,
                RocksDBScanSource {
                    iterator,
                    bounds,
                    projection: opts.projection,
                    space,
                    keys: ScanKeyArena::new(),
                },
            )
        }
    }
}

struct RocksDBScanSource<'a> {
    iterator: DBRawIteratorWithThreadMode<'a, DB>,
    bounds: EncodedBounds,
    projection: CoreProjection,
    space: StorageSpace,
    keys: ScanKeyArena,
}

/// Chunk size for [`ScanKeyArena`].
///
/// This constant is the whole trade. Larger chunks mean fewer allocations per
/// page; they also mean a single retained row pins a larger buffer, because a
/// key handed out of a chunk keeps the entire chunk alive. At 16 KiB a chunk
/// holds roughly 270 typical HOT keys, so a full page of 4 096 rows costs
/// about 16 allocations instead of 4 096, while the worst case — one surviving
/// row per chunk — pins 16 KiB rather than a whole page.
const SCAN_KEY_ARENA_CHUNK_BYTES: usize = 16 * 1024;

/// Hands out scanned keys as refcounted slices of a shared chunk instead of
/// one heap allocation per row.
///
/// `Bytes::copy_from_slice` produces a `Vec`-backed buffer, and a `Vec`-backed
/// `Bytes` cannot be cloned without first being **promoted**: the clone
/// allocates a refcount control block and installs it with a compare-and-swap.
/// The HOT key decoder clones every row's key onto its primary-key components,
/// so the per-row cost was two allocations and a promotion to hand out bytes
/// the page already had in memory.
///
/// Splitting one `BytesMut` gives every key in a chunk a handle on a single
/// shared allocation, so cloning a key is a plain uncontended increment and
/// nothing is promoted.
///
/// **Keys only, deliberately.** Values are unbounded — a value can be a blob —
/// so pooling them would trade a bounded amount of time for an unbounded
/// amount of retained memory, and would do it worst on exactly the selective
/// scans that already materialize more than they return.
struct ScanKeyArena {
    chunk: BytesMut,
}

impl ScanKeyArena {
    fn new() -> Self {
        Self {
            chunk: BytesMut::new(),
        }
    }

    /// Copies `bytes` into the current chunk and returns a shared handle on it.
    ///
    /// Keys already frozen out of a full chunk keep that chunk alive on their
    /// own, so starting a fresh one never invalidates them.
    fn take(&mut self, bytes: &[u8]) -> Bytes {
        if self.chunk.capacity() < bytes.len() {
            let capacity = SCAN_KEY_ARENA_CHUNK_BYTES.max(bytes.len());
            self.chunk = BytesMut::with_capacity(capacity);
            #[cfg(feature = "storage-benches")]
            {
                lix::storage_bench::record_scan_key_buffer_allocation(capacity);
            }
        }
        self.chunk.extend_from_slice(bytes);
        let key = self.chunk.split_to(bytes.len()).freeze();
        // Guards the one thing that can go wrong when keys are carved out of a
        // shared buffer rather than copied into their own: handing back a
        // window of the wrong width would silently mint a different logical
        // key. Cheap — a compare against a length already in a register.
        //
        // The message doubles as this build's arm marker. `ScanKeyArena`
        // inlines away in release and leaves no symbol, and a `#[used]` static
        // is dropped by the linker, but a panic string cannot be collected, so
        // `strings | grep lix.rocksdb.scan_key_arena.v1` answers "is the arena
        // in this artifact?" without trusting which directory it came from.
        assert_eq!(
            key.len(),
            bytes.len(),
            "lix.rocksdb.scan_key_arena.v1 handed out a key of the wrong width"
        );
        key
    }
}

impl StorageScanSource for RocksDBScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let mut entries = Vec::with_capacity(limit_rows);
            while entries.len() < limit_rows {
                let Some(encoded_key) = self.iterator.key() else {
                    break;
                };
                if !self.bounds.after_lower(encoded_key) {
                    self.iterator.next();
                    continue;
                }
                if !self.bounds.before_upper(encoded_key) {
                    break;
                }
                let key = Key(self.keys.take(scan_key_payload(self.space, encoded_key)?));
                let value = match self.projection {
                    CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                    CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::copy_from_slice(
                        self.iterator.value().ok_or_else(|| {
                            StorageError::Corruption(
                                "RocksDB scan key had no corresponding value".to_string(),
                            )
                        })?,
                    )),
                };
                entries.push(ReadEntry { key, value });
                self.iterator.next();
            }
            self.iterator.status().map_err(rocksdb_error)?;
            let has_more = self
                .iterator
                .key()
                .is_some_and(|key| self.bounds.before_upper(key));
            Ok(ScanChunk::new(entries, has_more))
        })
    }
}

fn scan_key_payload<'a>(
    space: StorageSpace,
    encoded_key: &'a [u8],
) -> Result<&'a [u8], StorageError> {
    if encoded_key.len() < 4 || encoded_key[..4] != space.id.0.to_be_bytes() {
        return Err(StorageError::Corruption(
            "RocksDB scan key escaped its storage space".to_string(),
        ));
    }
    Ok(&encoded_key[4..])
}

impl StorageWrite for RocksDBWrite {
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            if space.value_semantics == ValueSemantics::Immutable {
                validate_immutable_batch(&entries)?;
            }
            let max_key_bytes = entries
                .entries
                .iter()
                .map(|entry| 4_usize.saturating_add(entry.key.0.len()))
                .max()
                .unwrap_or(0);
            let mut physical_key = Vec::with_capacity(max_key_bytes);
            let cf = column_family(&self.inner.db, space);
            let space_prefix = space.id.0.to_be_bytes();
            for entry in entries.entries {
                physical_key.clear();
                physical_key.extend_from_slice(&space_prefix);
                physical_key.extend_from_slice(&entry.key.0);
                let value = stored_value_bytes(entry.value);
                if space.value_semantics == ValueSemantics::Immutable {
                    if let Some(staged) = self.immutable_values.get(physical_key.as_slice()) {
                        if staged != &value {
                            return Err(StorageError::Corruption(
                                "immutable identity was assigned different bytes".to_string(),
                            ));
                        }
                        continue;
                    }
                    if let Some(existing) = self
                        .inner
                        .db
                        .get_cf(cf, physical_key.as_slice())
                        .map_err(rocksdb_error)?
                    {
                        if existing.as_slice() != value.as_ref() {
                            return Err(StorageError::Corruption(
                                "immutable identity was assigned different bytes".to_string(),
                            ));
                        }
                        continue;
                    }
                    self.immutable_values
                        .insert(physical_key.clone(), value.clone());
                }
                self.stats.put_entries += 1;
                self.stats.written_bytes += value.len() as u64;
                self.batch
                    .put_cf(cf, physical_key.as_slice(), value.as_ref());
            }
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            let physical_key_bytes = keys.iter().fold(0_usize, |bytes, key| {
                bytes.saturating_add(4).saturating_add(key.0.len())
            });
            let mut key_bytes = Vec::with_capacity(physical_key_bytes);
            let cf = column_family(&self.inner.db, space);
            let space_prefix = space.id.0.to_be_bytes();
            for key in keys {
                let key_start = key_bytes.len();
                key_bytes.extend_from_slice(&space_prefix);
                key_bytes.extend_from_slice(&key.0);
                let key_end = key_bytes.len();
                self.batch.delete_cf(cf, &key_bytes[key_start..key_end]);
            }
            self.stats.deleted_entries += keys.len() as u64;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            let cf = column_family(&self.inner.db, space);
            let range = physical_range(space.id, range);
            if let Some((lower, upper)) = rocksdb_delete_range_bounds(&range) {
                self.batch
                    .delete_range_cf(cf, lower.as_slice(), upper.as_slice());
            } else {
                let bounds = EncodedBounds::new(range);
                for item in self.inner.db.iterator_cf(
                    cf,
                    IteratorMode::From(&bounds.lower_seek, Direction::Forward),
                ) {
                    let (encoded_key, _value) = item.map_err(rocksdb_error)?;
                    let encoded_key = encoded_key.as_ref();
                    if !bounds.after_lower(encoded_key) {
                        continue;
                    }
                    if !bounds.before_upper(encoded_key) {
                        break;
                    }
                    self.batch.delete_cf(cf, encoded_key);
                }
            }
            self.stats.deleted_ranges += 1;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            // `await_durable` means "do not acknowledge until the backend has
            // crossed its durable persistence boundary". For RocksDB that is
            // `sync = true`: the WAL append is fsynced before `write` returns,
            // so an acknowledged publication survives power loss, not merely
            // process death.
            //
            // Deliberately conditional. Ordinary row commits do not request
            // durability and must not pay an fsync for it; the engine sets the
            // flag only for atomic content-addressed publications and media
            // uploads, which is precisely where losing an acknowledged write
            // would be visible as a missing file.
            let mut write_options = RocksDBWriteOptions::default();
            write_options.set_sync(self.await_durable);
            self.inner
                .db
                .write_opt(self.batch, &write_options)
                .map_err(rocksdb_error)?;
            Ok(CommitResult {
                commit_id: None,
                stats: self.stats,
            })
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }
}

struct EncodedBounds {
    lower_seek: Vec<u8>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl EncodedBounds {
    fn new(range: KeyRange) -> Self {
        let lower = match range.lower {
            Bound::Included(key) => Bound::Included(key.0.to_vec()),
            Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
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

fn next_lexicographic_key(key: &Key) -> Option<Vec<u8>> {
    let mut bytes = key.0.to_vec();
    bytes.push(0);
    Some(bytes)
}

fn open_shared_rocksdb(path: PathBuf) -> Result<Arc<RocksDBInner>, StorageError> {
    let path = registry_key(&path)?;
    let registry = OPEN_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut open_databases = registry
        .lock()
        .map_err(|error| StorageError::Io(format!("rocksdb registry lock poisoned: {error}")))?;

    if let Some(existing) = open_databases.get(&path) {
        if let Some(inner) = existing.upgrade() {
            return Ok(inner);
        }
    }

    let db = open_rocksdb(&path)?;
    let inner = Arc::new(RocksDBInner {
        db,
        write_gate: WriteGate::new(),
    });
    open_databases.insert(path, Arc::downgrade(&inner));
    Ok(inner)
}

fn registry_key(path: &Path) -> Result<PathBuf, StorageError> {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| StorageError::Io(format!("read current directory: {error}")))?
            .join(path)
    };

    if absolute_path.exists() {
        return std::fs::canonicalize(&absolute_path).map_err(|error| {
            StorageError::Io(format!(
                "canonicalize rocksdb storage path {}: {error}",
                absolute_path.display()
            ))
        });
    }

    let parent = absolute_path.parent().ok_or_else(|| {
        StorageError::Io(format!(
            "rocksdb storage path has no parent: {}",
            absolute_path.display()
        ))
    })?;
    let file_name = absolute_path.file_name().ok_or_else(|| {
        StorageError::Io(format!(
            "rocksdb storage path has no final component: {}",
            absolute_path.display()
        ))
    })?;
    let canonical_parent = std::fs::canonicalize(parent).map_err(|error| {
        StorageError::Io(format!(
            "canonicalize rocksdb storage parent {}: {error}",
            parent.display()
        ))
    })?;
    Ok(canonical_parent.join(file_name))
}

fn open_rocksdb(path: &Path) -> Result<DB, StorageError> {
    let mut database_options = Options::default();
    database_options.create_if_missing(true);
    database_options.create_missing_column_families(true);
    database_options.set_use_fsync(false);

    let mut mutable_options = column_family_options();
    mutable_options.set_compression_type(rocksdb::DBCompressionType::Zstd);
    mutable_options.set_compression_options(-14, 1, 0, 0);

    let mut immutable_options = column_family_options();
    // Media payloads are generally compressed already. Blob separation keeps
    // their bytes out of LSM compaction while the CF retains keys and indexes.
    immutable_options.set_compression_type(rocksdb::DBCompressionType::None);
    immutable_options.set_enable_blob_files(true);
    immutable_options.set_min_blob_size(DEFAULT_BLOB_MIN_SIZE);
    immutable_options.set_blob_file_size(DEFAULT_BLOB_FILE_SIZE);
    immutable_options.set_blob_compression_type(rocksdb::DBCompressionType::None);
    immutable_options.set_enable_blob_gc(true);
    immutable_options.set_blob_gc_age_cutoff(0.0);
    immutable_options.set_blob_gc_force_threshold(BLOB_GC_FORCE_THRESHOLD);

    let column_families = [
        ColumnFamilyDescriptor::new(MUTABLE_COLUMN_FAMILY, mutable_options),
        ColumnFamilyDescriptor::new(IMMUTABLE_COLUMN_FAMILY, immutable_options),
    ];
    DB::open_cf_descriptors(&database_options, path, column_families)
        .map_err(|error| rocksdb_open_error(error, path))
}

fn column_family_options() -> Options {
    let mut options = Options::default();
    options.set_write_buffer_size(WRITE_BUFFER_BYTES);
    // RocksDB's default of two write buffers gives a writer exactly one spare
    // memtable: the moment the active one fills, the next `db.write` blocks
    // until the previous flush has finished. A media commit stages megabytes at
    // a time, so a bulk import fills both buffers and the *next* ordinary agent
    // commit pays the whole flush inside its own latency. Measured on a 64 file
    // / 10 MiB corpus, that one commit cost 337-369 ms against a 21 ms median;
    // with four buffers it costs 22 ms and the median does not move. Raising
    // `max_background_jobs` instead changes nothing (measured 339/369 ms), so
    // the spare-memtable count is the whole effect.
    options.set_max_write_buffer_number(WRITE_BUFFER_COUNT);
    let mut table_options = BlockBasedOptions::default();
    // Full whole-key filters let missing point reads skip unrelated SST data.
    table_options.set_bloom_filter(8.0, false);
    table_options.set_optimize_filters_for_memory(true);
    options.set_block_based_table_factory(&table_options);
    options
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

/// Reclaims the iterator-owned physical key and removes its four-byte space
/// prefix without copying its logical-key bytes.
#[cfg(test)]
fn logical_key_from_physical(encoded_key: Box<[u8]>) -> Key {
    let mut key = Bytes::from(encoded_key);
    key.advance(4);
    Key(key)
}

#[cfg(test)]
fn project_owned_value<T>(value: T, projection: CoreProjection) -> ProjectedValue
where
    Bytes: From<T>,
{
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        // `Snapshot::get` and `Snapshot::multi_get` yield Rust-owned
        // `Vec<u8>` values, while the standard iterator yields Rust-owned
        // `Box<[u8]>` values. Retaining those allocations in `Bytes` avoids a
        // second full value copy.
        CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::from(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreProjection, ProjectedValue, logical_key_from_physical, project_owned_value};

    #[test]
    fn logical_key_reuses_the_physical_key_allocation() {
        let physical_key: Box<[u8]> = Box::from(&b"\0\0\0\x01logical-key"[..]);
        let logical_ptr = physical_key.as_ptr().wrapping_add(4);

        let key = logical_key_from_physical(physical_key);

        assert_eq!(key.0.as_ptr(), logical_ptr);
        assert_eq!(&key.0[..], b"logical-key");
    }

    #[test]
    fn full_value_reuses_vec_allocation() {
        let value = b"small value that used to take the copy path".to_vec();
        let ptr = value.as_ptr();

        let ProjectedValue::FullValue(value) =
            project_owned_value(value, CoreProjection::FullValue)
        else {
            panic!("full-value projection should return bytes");
        };

        assert_eq!(value.as_ptr(), ptr);
        assert_eq!(&value[..], b"small value that used to take the copy path");
    }

    #[test]
    fn full_value_reuses_boxed_slice_allocation() {
        let value: Box<[u8]> = Box::from(&b"iterator value"[..]);
        let ptr = value.as_ptr();

        let ProjectedValue::FullValue(value) =
            project_owned_value(value, CoreProjection::FullValue)
        else {
            panic!("full-value projection should return bytes");
        };

        assert_eq!(value.as_ptr(), ptr);
        assert_eq!(&value[..], b"iterator value");
    }
}

fn rocksdb_error(error: rocksdb::Error) -> StorageError {
    StorageError::Io(format!("rocksdb storage: {error}"))
}

fn rocksdb_open_error(error: rocksdb::Error, path: &Path) -> StorageError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("lock") {
        StorageError::Io(format!(
            "rocksdb storage at {} is already open by another process: {message}",
            path.display()
        ))
    } else {
        StorageError::Io(format!(
            "rocksdb storage failed to open {}: {message}",
            path.display()
        ))
    }
}

#[derive(Default)]
#[allow(missing_debug_implementations)]
struct WriteGate {
    state: Arc<AsyncMutex<()>>,
}

impl WriteGate {
    fn new() -> Self {
        Self::default()
    }

    async fn acquire(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.state).lock_owned().await
    }
}
