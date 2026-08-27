use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::filesystem::{PersistentMap, PersistentMapRangeCursor};
use crate::storage::conformance::{StorageFactory, StorageFixture, StorageTestConfig};
use crate::storage::{
    BeginScanOptions, Capability, CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key,
    KeyRange, Precondition, PreconditionFailure, ProjectedValue, PutBatch, ReadDurability,
    ReadEntry, ReadOptions, ScanChunk, ScanCursor, ScanOrder, SpaceId, Storage, StorageError,
    StorageRead, StorageScanSource, StorageSessionGate, StorageSessionToken, StorageSpace,
    StorageWrite, StoredValue, ValueSemantics, WriteOptions, WriteStats,
};

type InMemoryMap = PersistentMap<Key, Bytes>;

/// The in-memory storage has no native namespaces; it scopes keys to spaces
/// by prefixing the 4-byte big-endian space id internally. The prefix never
/// crosses the trait boundary: reads return logical keys.
fn physical_key(space: SpaceId, key: &Key) -> Key {
    let mut bytes = bytes::BytesMut::with_capacity(4 + key.0.len());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Key(bytes.freeze())
}

fn physical_bound(space: SpaceId, bound: Bound<Key>, unbounded: Bound<Key>) -> Bound<Key> {
    match bound {
        Bound::Included(key) => Bound::Included(physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(physical_key(space, &key)),
        Bound::Unbounded => unbounded,
    }
}

fn physical_range(space: SpaceId, range: KeyRange) -> KeyRange {
    let lower_unbounded = Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes())));
    let upper_unbounded = space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
        Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
    });
    KeyRange {
        lower: physical_bound(space, range.lower, lower_unbounded),
        upper: physical_bound(space, range.upper, upper_unbounded),
    }
}

#[derive(Clone, Debug, Default)]
pub struct Memory {
    entries: Arc<Mutex<InMemoryMap>>,
    sessions: Arc<StorageSessionGate>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryFactory;

#[derive(Clone, Debug, Default)]
pub struct MemoryFixture {
    entries: Arc<Mutex<InMemoryMap>>,
    sessions: Arc<StorageSessionGate>,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct MemoryRead {
    entries: InMemoryMap,
}

#[expect(missing_debug_implementations)]
pub struct MemoryWrite {
    parent: Arc<Mutex<InMemoryMap>>,
    sessions: Arc<StorageSessionGate>,
    session_token: Option<StorageSessionToken>,
    base: InMemoryMap,
    preconditions: Vec<Precondition>,
    overlay: EntriesOverlay,
    immutable_values: BTreeMap<Key, Bytes>,
    stats: WriteStats,
}

#[derive(Debug, Default)]
struct EntriesOverlay {
    puts: BTreeMap<Key, Bytes>,
    deletes: BTreeSet<Key>,
}

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn shared_handle_count(&self) -> usize {
        Arc::strong_count(&self.entries)
    }

    /// Forks the current in-memory state without serializing it.
    ///
    /// This is primarily useful for tests and benchmarks that need an isolated
    /// `Memory` instance. Portable Lix snapshots are exported from [`crate::Lix`].
    pub fn fork(&self) -> Result<Self, StorageError> {
        Ok(Self {
            entries: Arc::new(Mutex::new(self.snapshot()?)),
            sessions: Arc::new(StorageSessionGate::default()),
        })
    }

    fn snapshot(&self) -> Result<InMemoryMap, StorageError> {
        self.entries
            .lock()
            .map_err(|_| StorageError::Io("in-memory storage lock poisoned".to_string()))
            .map(|entries| entries.clone())
    }
}

impl StorageFactory for MemoryFactory {
    type Storage = Memory;
    type Fixture = MemoryFixture;

    fn create_fixture(&self) -> Self::Fixture {
        MemoryFixture::default()
    }

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig {
            ephemeral: true,
            supports_concurrent_writers: false,
            ..StorageTestConfig::default()
        }
    }
}

impl StorageFixture for MemoryFixture {
    type Storage = Memory;

    async fn open(&self) -> Self::Storage {
        Memory {
            entries: Arc::clone(&self.entries),
            sessions: Arc::clone(&self.sessions),
        }
    }
}

impl Storage for Memory {
    type Read<'a>
        = MemoryRead
    where
        Self: 'a;

    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;
    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.sessions.acquire()
    }

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        if opts.durability == ReadDurability::Durable {
            return Err(StorageError::Durability);
        }
        let _permit = self.sessions.validate(opts.session_token)?;
        Ok(MemoryRead {
            entries: self.snapshot()?,
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        let _permit = self.sessions.validate(opts.session_token)?;
        Ok(MemoryWrite {
            parent: Arc::clone(&self.entries),
            sessions: Arc::clone(&self.sessions),
            session_token: opts.session_token,
            base: self.snapshot()?,
            preconditions: opts.preconditions,
            overlay: EntriesOverlay::default(),
            immutable_values: BTreeMap::new(),
            stats: WriteStats::default(),
        })
    }
}

impl StorageRead for MemoryRead {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        let values = requests
            .iter()
            .flat_map(|request| {
                request.keys.iter().map(|key| {
                    self.entries
                        .get(&physical_key(request.space.id, key))
                        .map(|value| project_value(value, request.opts.projection))
                })
            })
            .collect();
        Ok(GetManyResult::new(values))
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        ScanCursor::validate_range(&range)?;
        if opts.order == ScanOrder::Descending {
            return Err(StorageError::Unsupported(Capability::ReverseScan));
        }
        let physical = physical_range(space.id, range.clone());
        ScanCursor::from_source(
            range,
            opts.order,
            MemoryScanSource {
                cursor: self.entries.range_cursor(physical.lower, physical.upper),
                projection: opts.projection,
                space,
                pending: None,
            },
        )
    }
}

struct MemoryScanSource<'a> {
    cursor: PersistentMapRangeCursor<'a, Key, Bytes>,
    projection: CoreProjection,
    space: StorageSpace,
    pending: Option<(Key, Bytes)>,
}

impl StorageScanSource for MemoryScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let mut entries = Vec::with_capacity(limit_rows);
            while entries.len() < limit_rows {
                let Some((physical_key, value)) =
                    self.pending.take().or_else(|| self.cursor.next())
                else {
                    break;
                };
                entries.push(ReadEntry {
                    key: decode_memory_scan_key(self.space, physical_key)?,
                    value: project_value(&value, self.projection),
                });
            }
            self.pending = self.cursor.next();
            Ok(ScanChunk::new(entries, self.pending.is_some()))
        })
    }
}

fn decode_memory_scan_key(space: StorageSpace, key: Key) -> Result<Key, StorageError> {
    let bytes = key.0;
    if bytes.len() < 4 || bytes[..4] != space.id.0.to_be_bytes() {
        return Err(StorageError::Corruption(
            "in-memory scan key escaped its storage space".to_string(),
        ));
    }
    Ok(Key(bytes.slice(4..)))
}

impl StorageWrite for MemoryWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        for entry in entries.entries {
            let key = physical_key(space.id, &entry.key);
            let value = stored_value_bytes(entry.value);
            if space.value_semantics == ValueSemantics::Immutable {
                if let Some(existing) = self.immutable_values.get(&key) {
                    if existing != &value {
                        return Err(StorageError::Corruption(
                            "immutable identity was assigned different bytes".to_string(),
                        ));
                    }
                    continue;
                }
                if let Some(existing) = self.base.get(&key) {
                    if existing != &value {
                        return Err(StorageError::Corruption(
                            "immutable identity was assigned different bytes".to_string(),
                        ));
                    }
                    continue;
                }
                self.immutable_values.insert(key.clone(), value.clone());
            }
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            if !self.overlay.deletes.is_empty() {
                self.overlay.deletes.remove(&key);
            }
            self.overlay.puts.insert(key, value);
        }
        self.stats.storage_calls += 1;
        Ok(())
    }

    async fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        if space.value_semantics != ValueSemantics::Immutable
            || space.value_integrity == crate::storage::ValueIntegrity::ContentAddressed
        {
            return Err(StorageError::Corruption(
                "replace_many requires an immutable non-content-addressed storage space"
                    .to_string(),
            ));
        }
        for entry in entries.entries {
            let key = physical_key(space.id, &entry.key);
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.overlay.deletes.remove(&key);
            self.overlay.puts.insert(key, value);
        }
        self.stats.storage_calls += 1;
        Ok(())
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        for key in keys {
            let key = physical_key(space.id, key);
            if !self.overlay.puts.is_empty() {
                self.overlay.puts.remove(&key);
            }
            self.overlay.deletes.insert(key);
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.storage_calls += 1;
        Ok(())
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        let range = physical_range(space.id, range);
        let base_keys = self
            .base
            .entries_range(lower_bound(&range), upper_bound(&range), usize::MAX)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        let overlay_puts_before = self.overlay.puts.len();
        self.overlay
            .puts
            .retain(|key, _value| !range_contains(&range, key));
        let removed_overlay_puts = overlay_puts_before - self.overlay.puts.len();

        for key in &base_keys {
            self.overlay.deletes.insert(key.clone());
        }

        self.stats.deleted_entries += (base_keys.len() + removed_overlay_puts) as u64;
        self.stats.deleted_ranges += 1;
        self.stats.storage_calls += 1;
        Ok(())
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        let _permit = self.sessions.validate(self.session_token)?;
        let mut parent = self
            .parent
            .lock()
            .map_err(|_| StorageError::Io("in-memory storage lock poisoned".to_string()))?;
        check_preconditions(&parent, &self.preconditions)?;
        for (key, value) in &self.immutable_values {
            if let Some(existing) = parent.get(key)
                && existing != value
            {
                return Err(StorageError::Corruption(
                    "immutable identity was assigned different bytes".to_string(),
                ));
            }
        }
        let mut entries = parent.clone();
        for key in self.overlay.deletes {
            entries = entries.remove(&key);
        }
        for (key, value) in self.overlay.puts {
            entries = entries.insert(key, value);
        }
        *parent = entries;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    async fn rollback(self) -> Result<(), StorageError> {
        Ok(())
    }
}

fn check_preconditions(
    entries: &InMemoryMap,
    preconditions: &[Precondition],
) -> Result<(), StorageError> {
    let failures = preconditions
        .iter()
        .enumerate()
        .filter_map(|(index, precondition)| {
            let matches = match precondition {
                Precondition::KeyAbsent { space, key } => {
                    entries.get(&physical_key(space.id, key)).is_none()
                }
                Precondition::KeyPresent { space, key } => {
                    entries.get(&physical_key(space.id, key)).is_some()
                }
                Precondition::KeyValueHashEquals { space, key, hash } => entries
                    .get(&physical_key(space.id, key))
                    .is_some_and(|value| blake3::hash(value).as_bytes() == hash),
                Precondition::KeyValueEquals {
                    space,
                    key,
                    expected,
                } => entries
                    .get(&physical_key(space.id, key))
                    .is_some_and(|value| value == expected),
                Precondition::RangeEmpty { space, range } => {
                    let range = physical_range(space.id, range.clone());
                    entries
                        .entries_range(lower_bound(&range), upper_bound(&range), 1)
                        .is_empty()
                }
                Precondition::BranchEquals { .. } => false,
            };
            (!matches).then_some(PreconditionFailure { index })
        })
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed(failures))
    }
}

fn lower_bound(range: &KeyRange) -> Bound<&Key> {
    let range_lower = match &range.lower {
        Bound::Included(key) => Some((key, true)),
        Bound::Excluded(key) => Some((key, false)),
        Bound::Unbounded => None,
    };
    match range_lower {
        Some((lower, true)) => Bound::Included(lower),
        Some((lower, false)) => Bound::Excluded(lower),
        None => Bound::Unbounded,
    }
}

fn upper_bound(range: &KeyRange) -> Bound<&Key> {
    match &range.upper {
        Bound::Included(key) => Bound::Included(key),
        Bound::Excluded(key) => Bound::Excluded(key),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn project_value(value: &Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value.clone()),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::storage::conformance::{ConformanceStatus, run_storage_conformance};
    use crate::storage::{
        BeginScanOptions, GetManyRequest, GetOptions, Key, KeyRange, MAX_SCAN_PAGE_ROWS, Memory,
        PutBatch, PutEntry, ReadOptions, SpaceId, Storage, StorageError, StorageRead,
        StorageSession, StorageSpace, StorageWrite, StoredValue, WriteOptions,
    };

    #[tokio::test]
    async fn memory_passes_storage_conformance() {
        let report = run_storage_conformance(&crate::storage::MemoryFactory).await;

        report.assert_no_failures();

        assert!(
            report
                .tests
                .iter()
                .any(|test| matches!(test.status, ConformanceStatus::Passed)),
            "expected at least one conformance test to run"
        );
    }

    #[tokio::test]
    async fn prepared_tokenless_write_is_fenced_by_first_session_acquisition() {
        let storage = Memory::new();
        let space = StorageSpace::mutable(SpaceId(91), "test.mutable");
        let mut prepared = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("prepare tokenless write");
        prepared
            .put_many(
                space,
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(b"prepared")),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"must-not-commit"),
                        },
                    }],
                },
            )
            .await
            .expect("stage tokenless write");

        let session = StorageSession::acquire(storage.clone())
            .await
            .expect("acquire first session");

        assert_eq!(prepared.commit().await, Err(StorageError::Fenced));
        let read = session
            .begin_read(ReadOptions::default())
            .await
            .expect("session read");
        let keys = [Key(Bytes::from_static(b"prepared"))];
        let values = read
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .expect("verify fenced write");
        assert_eq!(values.values, vec![None]);
    }

    #[tokio::test]
    async fn tokenless_access_is_permanently_fenced_after_acquisition() {
        let storage = Memory::new();
        let first = StorageSession::acquire(storage.clone())
            .await
            .expect("acquire first session");
        let second = StorageSession::acquire(storage.clone())
            .await
            .expect("acquire shared session");

        assert_eq!(first.token(), second.token());
        assert!(matches!(
            storage.begin_read(ReadOptions::default()).await,
            Err(StorageError::Fenced)
        ));
        assert!(matches!(
            storage.begin_write(WriteOptions::default()).await,
            Err(StorageError::Fenced)
        ));

        let write = second
            .begin_write(WriteOptions::default())
            .await
            .expect("token-bearing write");
        write.rollback().await.expect("rollback session write");
    }

    #[tokio::test]
    async fn delete_range_covers_more_than_one_scan_page() {
        let storage = Memory::new();
        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin seed write");
        write
            .put_many(
                space,
                PutBatch {
                    entries: (0..=MAX_SCAN_PAGE_ROWS)
                        .map(|index| {
                            let index = u32::try_from(index).expect("test index fits u32");
                            PutEntry {
                                key: Key(Bytes::copy_from_slice(&index.to_be_bytes())),
                                value: StoredValue {
                                    bytes: Bytes::from_static(b"value"),
                                },
                            }
                        })
                        .collect(),
                },
            )
            .await
            .expect("seed rows");
        write.commit().await.expect("commit seed rows");

        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin range delete");
        write
            .delete_range(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
            )
            .await
            .expect("delete all rows");
        write.commit().await.expect("commit range delete");

        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin verification read");
        let mut cursor = read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .expect("begin scan after range delete");
        let (chunk, chunk_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan after range delete")
            .into_parts();
        assert!(chunk.is_empty());
        assert!(!chunk_has_more);
    }

}
