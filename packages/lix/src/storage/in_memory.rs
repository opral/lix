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
    StorageRead, StorageScanSource, StorageSpace, StorageWrite, StoredValue, ValueSemantics,
    WriteOptions, WriteStats,
};

type InMemoryMap = PersistentMap<Key, Bytes>;

const SNAPSHOT_MAGIC: &[u8; 8] = b"LIXMEM\0\x01";
const SNAPSHOT_HEADER_BYTES: usize = SNAPSHOT_MAGIC.len() + size_of::<u32>();
const SNAPSHOT_ENTRY_HEADER_BYTES: usize = size_of::<u32>() * 2;

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
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryFactory;

#[derive(Clone, Debug, Default)]
pub struct MemoryFixture {
    entries: Arc<Mutex<InMemoryMap>>,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct MemoryRead {
    entries: InMemoryMap,
}

#[expect(missing_debug_implementations)]
pub struct MemoryWrite {
    parent: Arc<Mutex<InMemoryMap>>,
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

    /// Builds an in-memory storage from raw physical entries — 4-byte
    /// big-endian space prefix followed by the logical key. Debug/export
    /// tooling only: bypasses write semantics entirely.
    pub fn from_physical_entries(
        entries: impl IntoIterator<Item = (Key, Bytes)>,
    ) -> Self {
        Self {
            entries: Arc::new(Mutex::new(PersistentMap::from_sorted(
                entries.into_iter().collect(),
            ))),
        }
    }

    /// Opens an in-memory storage from a deterministic snapshot previously
    /// returned by [`Self::export_snapshot`].
    pub fn from_snapshot(snapshot: &[u8]) -> Result<Self, StorageError> {
        let entries = decode_snapshot(snapshot)?;
        Ok(Self {
            entries: Arc::new(Mutex::new(PersistentMap::from_sorted(
                entries.into_iter().collect(),
            ))),
        })
    }

    /// Exports one coherent, deterministic snapshot of the complete storage.
    pub fn export_snapshot(&self) -> Result<Vec<u8>, StorageError> {
        let state = self.snapshot()?;
        encode_snapshot(&state)
    }

    #[cfg(feature = "storage-benches")]
    pub fn fork_snapshot(&self) -> Result<Self, StorageError> {
        Ok(Self {
            entries: Arc::new(Mutex::new(self.snapshot()?)),
        })
    }

    fn snapshot(&self) -> Result<InMemoryMap, StorageError> {
        self.entries
            .lock()
            .map_err(|_| StorageError::Io("in-memory storage lock poisoned".to_string()))
            .map(|entries| entries.clone())
    }
}

fn encode_snapshot(entries: &InMemoryMap) -> Result<Vec<u8>, StorageError> {
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| snapshot_corruption("too many entries to encode"))?;
    let entries = entries.entries_range(Bound::Unbounded, Bound::Unbounded, usize::MAX);
    let mut encoded_len = SNAPSHOT_HEADER_BYTES;
    for (key, value) in &entries {
        let _ = u32::try_from(key.0.len())
            .map_err(|_| snapshot_corruption("key is too large to encode"))?;
        let _ = u32::try_from(value.len())
            .map_err(|_| snapshot_corruption("value is too large to encode"))?;
        encoded_len = encoded_len
            .checked_add(SNAPSHOT_ENTRY_HEADER_BYTES)
            .and_then(|len| len.checked_add(key.0.len()))
            .and_then(|len| len.checked_add(value.len()))
            .ok_or_else(|| snapshot_corruption("encoded snapshot length overflowed"))?;
    }

    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&entry_count.to_be_bytes());
    for (key, value) in &entries {
        let key_len = u32::try_from(key.0.len())
            .map_err(|_| snapshot_corruption("key is too large to encode"))?;
        let value_len = u32::try_from(value.len())
            .map_err(|_| snapshot_corruption("value is too large to encode"))?;
        encoded.extend_from_slice(&key_len.to_be_bytes());
        encoded.extend_from_slice(&value_len.to_be_bytes());
        encoded.extend_from_slice(&key.0);
        encoded.extend_from_slice(value);
    }
    Ok(encoded)
}

fn decode_snapshot(snapshot: &[u8]) -> Result<BTreeMap<Key, Bytes>, StorageError> {
    let mut decoder = SnapshotDecoder::new(snapshot);
    let magic = decoder.take(SNAPSHOT_MAGIC.len(), "snapshot magic")?;
    if magic != SNAPSHOT_MAGIC {
        return Err(snapshot_corruption("unsupported snapshot magic or version"));
    }
    let entry_count = decoder.read_u32("entry count")? as usize;
    if entry_count > decoder.remaining() / SNAPSHOT_ENTRY_HEADER_BYTES {
        return Err(snapshot_corruption("entry count exceeds snapshot length"));
    }

    let mut entries = BTreeMap::new();
    let mut previous_key: Option<Key> = None;
    for index in 0..entry_count {
        let key_len = decoder.read_u32("key length")? as usize;
        let value_len = decoder.read_u32("value length")? as usize;
        if key_len < size_of::<u32>() {
            return Err(snapshot_corruption(format!(
                "entry {index} key is missing its space prefix"
            )));
        }
        let key = Key(Bytes::copy_from_slice(decoder.take(key_len, "entry key")?));
        let value = Bytes::copy_from_slice(decoder.take(value_len, "entry value")?);
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(snapshot_corruption(format!(
                "entry {index} keys are duplicated or out of order"
            )));
        }
        previous_key = Some(key.clone());
        entries.insert(key, value);
    }
    if decoder.remaining() != 0 {
        return Err(snapshot_corruption("snapshot contains trailing data"));
    }
    Ok(entries)
}

struct SnapshotDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_u32(&mut self, label: &str) -> Result<u32, StorageError> {
        let bytes: [u8; 4] = self
            .take(size_of::<u32>(), label)?
            .try_into()
            .map_err(|_| snapshot_corruption(format!("invalid {label}")))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn take(&mut self, len: usize, label: &str) -> Result<&'a [u8], StorageError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| snapshot_corruption(format!("{label} length overflowed")))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| snapshot_corruption(format!("snapshot truncated in {label}")))?;
        self.offset = end;
        Ok(bytes)
    }
}

fn snapshot_corruption(message: impl Into<String>) -> StorageError {
    StorageError::Corruption(format!("invalid in-memory snapshot: {}", message.into()))
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
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        if opts.durability == ReadDurability::Durable {
            return Err(StorageError::Durability);
        }
        Ok(MemoryRead {
            entries: self.snapshot()?,
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(MemoryWrite {
            parent: Arc::clone(&self.entries),
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
    use super::SNAPSHOT_MAGIC;
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::storage::conformance::{ConformanceStatus, run_storage_conformance};
    use crate::storage::{
        BeginScanOptions, GetManyRequest, GetOptions, Key, KeyRange, MAX_SCAN_PAGE_ROWS, Memory,
        ProjectedValue, PutBatch, PutEntry, ReadOptions, SpaceId, Storage, StorageError,
        StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
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

    #[tokio::test]
    async fn snapshot_roundtrip_is_deterministic_and_point_in_time() {
        let storage = Memory::new();
        let space = StorageSpace::mutable(SpaceId(17), "test.mutable");
        let key_a = Key(Bytes::from_static(b"a"));
        let key_b = Key(Bytes::from_static(b"b"));
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin seed write");
        write
            .put_many(
                space,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: key_b.clone(),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"B"),
                            },
                        },
                        PutEntry {
                            key: key_a.clone(),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"A"),
                            },
                        },
                    ],
                },
            )
            .await
            .expect("seed rows");
        write.commit().await.expect("commit seed rows");

        let snapshot = storage.export_snapshot().expect("export snapshot");
        assert_eq!(
            snapshot,
            storage
                .export_snapshot()
                .expect("repeat deterministic export")
        );

        let mut later = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin later write");
        later
            .delete_many(space, std::slice::from_ref(&key_a))
            .await
            .expect("delete a");
        later.commit().await.expect("commit later write");

        let restored = Memory::from_snapshot(&snapshot).expect("restore snapshot");
        let read = restored
            .begin_read(ReadOptions::default())
            .await
            .expect("begin restored read");
        let values = read
            .get_many(&[GetManyRequest {
                space,
                keys: &[key_a, key_b],
                opts: GetOptions::default(),
            }])
            .await
            .expect("read restored rows");
        assert_eq!(
            values.values,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
            ]
        );
    }

    #[test]
    fn snapshot_rejects_malformed_encodings() {
        let empty = Memory::new()
            .export_snapshot()
            .expect("export empty snapshot");
        let entry = |key: &[u8], value: &[u8]| {
            [
                &u32::try_from(key.len())
                    .expect("test key length fits")
                    .to_be_bytes(),
                &u32::try_from(value.len())
                    .expect("test value length fits")
                    .to_be_bytes(),
                key,
                value,
            ]
            .concat()
        };
        let physical_key = [0_u8, 0, 0, 1, b'k'];
        let duplicate_entries = [
            SNAPSHOT_MAGIC.as_slice(),
            &2_u32.to_be_bytes(),
            &entry(&physical_key, b"one"),
            &entry(&physical_key, b"two"),
        ]
        .concat();
        let impossible_lengths = [
            SNAPSHOT_MAGIC.as_slice(),
            &1_u32.to_be_bytes(),
            &u32::MAX.to_be_bytes(),
            &u32::MAX.to_be_bytes(),
        ]
        .concat();
        let cases = [
            Vec::new(),
            b"not-a-lix-snapshot".to_vec(),
            empty[..empty.len() - 1].to_vec(),
            [empty.as_slice(), b"trailing"].concat(),
            [b"LIXMEM\0\x01".as_slice(), &1_u32.to_be_bytes()].concat(),
            duplicate_entries,
            impossible_lengths,
        ];
        for snapshot in cases {
            assert!(matches!(
                Memory::from_snapshot(&snapshot),
                Err(StorageError::Corruption(message))
                    if message.contains("invalid in-memory snapshot")
            ));
        }
    }
}
