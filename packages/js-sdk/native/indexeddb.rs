use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures_util::lock::Mutex;
use js_sys::Promise;
use lix::storage::{
    BeginScanOptions, Capability, CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key,
    KeyRange, Memory, MemoryRead, MemoryWrite, Precondition, PreconditionFailure, ProjectedValue,
    PutBatch, ReadDurability, ReadEntry, ReadOptions, ScanChunk, ScanCursor, ScanOrder, SpaceId,
    Storage, StorageError, StorageRead, StorageScanSource, StorageSpace, StorageWrite, StoredValue,
    ValueIntegrity, ValueSemantics, WriteOptions, WriteStats,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

type EntryMap = BTreeMap<Key, Bytes>;

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
    pub type IndexedDbBackend;

    #[wasm_bindgen(method, js_name = loadEntries)]
    fn load_entries(this: &IndexedDbBackend) -> Promise;

    #[wasm_bindgen(method, js_name = applyChanges)]
    fn apply_changes(this: &IndexedDbBackend, changes: JsValue) -> Promise;

    #[wasm_bindgen(method)]
    fn close(this: &IndexedDbBackend) -> Promise;
}

#[derive(Clone)]
struct SendBackend(IndexedDbBackend);

// Browser WASM is single-threaded. The storage traits retain Send bounds so
// the same engine can use native asynchronous adapters.
unsafe impl Send for SendBackend {}
unsafe impl Sync for SendBackend {}

struct SendJsFuture(JsFuture);

// See SendBackend: the browser target has no cross-thread WASM execution.
unsafe impl Send for SendJsFuture {}

impl Future for SendJsFuture {
    type Output = Result<JsValue, JsValue>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(context)
    }
}

#[derive(Clone)]
pub struct IndexedDb {
    entries: Arc<Mutex<EntryMap>>,
    backend: SendBackend,
}

#[derive(Clone, Debug)]
pub enum BrowserStorage {
    Memory(Memory),
    IndexedDb(IndexedDb),
}

pub enum BrowserRead {
    Memory(MemoryRead),
    IndexedDb(IndexedDbRead),
}

pub enum BrowserWrite {
    Memory(MemoryWrite),
    IndexedDb(IndexedDbWrite),
}

impl std::fmt::Debug for IndexedDb {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IndexedDb").finish_non_exhaustive()
    }
}

pub struct IndexedDbRead {
    entries: EntryMap,
}

pub struct IndexedDbWrite {
    entries: Arc<Mutex<EntryMap>>,
    backend: SendBackend,
    base: EntryMap,
    preconditions: Vec<Precondition>,
    overlay: EntriesOverlay,
    immutable_values: BTreeMap<Key, Bytes>,
    await_durable: bool,
    stats: WriteStats,
}

#[derive(Default)]
struct EntriesOverlay {
    puts: BTreeMap<Key, Bytes>,
    deletes: BTreeSet<Key>,
}

#[derive(Deserialize)]
struct EntryDto {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ChangesDto {
    deletes: Vec<ByteDto>,
    puts: Vec<EntryDto>,
    strict_durability: bool,
}

#[derive(Serialize)]
#[serde(transparent)]
struct ByteDto(#[serde(with = "serde_bytes")] Vec<u8>);

impl Serialize for EntryDto {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct SerializableEntry<'a> {
            #[serde(with = "serde_bytes")]
            key: &'a [u8],
            #[serde(with = "serde_bytes")]
            value: &'a [u8],
        }
        SerializableEntry {
            key: &self.key,
            value: &self.value,
        }
        .serialize(serializer)
    }
}

impl IndexedDb {
    pub async fn open(backend: IndexedDbBackend) -> Result<Self, StorageError> {
        let loaded = JsFuture::from(backend.load_entries())
            .await
            .map_err(indexed_db_error)?;
        let loaded: Vec<EntryDto> = serde_wasm_bindgen::from_value(loaded).map_err(|error| {
            StorageError::Corruption(format!("decode IndexedDB entries: {error}"))
        })?;
        let mut entries = BTreeMap::new();
        for entry in loaded {
            if entry.key.len() < size_of::<u32>() {
                return Err(StorageError::Corruption(
                    "IndexedDB entry key is missing its space prefix".to_string(),
                ));
            }
            let key = Key(Bytes::from(entry.key));
            if entries.insert(key, Bytes::from(entry.value)).is_some() {
                return Err(StorageError::Corruption(
                    "IndexedDB contains a duplicate entry key".to_string(),
                ));
            }
        }
        Ok(Self {
            entries: Arc::new(Mutex::new(entries)),
            backend: SendBackend(backend),
        })
    }

    pub async fn close(&self) -> Result<(), StorageError> {
        JsFuture::from(self.backend.0.close())
            .await
            .map_err(indexed_db_error)?;
        Ok(())
    }
}

impl BrowserStorage {
    pub async fn close(&self) -> Result<(), StorageError> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::IndexedDb(storage) => storage.close().await,
        }
    }
}

impl Storage for BrowserStorage {
    type Read<'a>
        = BrowserRead
    where
        Self: 'a;
    type Write<'a>
        = BrowserWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        match self {
            Self::Memory(storage) => storage.begin_read(opts).await.map(BrowserRead::Memory),
            Self::IndexedDb(storage) => storage.begin_read(opts).await.map(BrowserRead::IndexedDb),
        }
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        match self {
            Self::Memory(storage) => storage.begin_write(opts).await.map(BrowserWrite::Memory),
            Self::IndexedDb(storage) => {
                storage.begin_write(opts).await.map(BrowserWrite::IndexedDb)
            }
        }
    }
}

impl StorageRead for BrowserRead {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        match self {
            Self::Memory(read) => read.get_many(requests).await,
            Self::IndexedDb(read) => read.get_many(requests).await,
        }
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        match self {
            Self::Memory(read) => read.begin_scan(space, range, opts).await,
            Self::IndexedDb(read) => read.begin_scan(space, range, opts).await,
        }
    }
}

impl StorageWrite for BrowserWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.put_many(space, entries).await,
            Self::IndexedDb(write) => write.put_many(space, entries).await,
        }
    }

    async fn replace_many_for_migration(
        &mut self,
        token: &lix::storage::MigrationReplaceToken,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => {
                write
                    .replace_many_for_migration(token, space, entries)
                    .await
            }
            Self::IndexedDb(write) => {
                write
                    .replace_many_for_migration(token, space, entries)
                    .await
            }
        }
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.delete_many(space, keys).await,
            Self::IndexedDb(write) => write.delete_many(space, keys).await,
        }
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.delete_range(space, range).await,
            Self::IndexedDb(write) => write.delete_range(space, range).await,
        }
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        match self {
            Self::Memory(write) => write.commit().await,
            Self::IndexedDb(write) => write.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StorageError> {
        match self {
            Self::Memory(write) => write.rollback().await,
            Self::IndexedDb(write) => write.rollback().await,
        }
    }
}

impl Storage for IndexedDb {
    type Read<'a>
        = IndexedDbRead
    where
        Self: 'a;
    type Write<'a>
        = IndexedDbWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        if opts.durability == ReadDurability::Durable {
            return Err(StorageError::Durability);
        }
        Ok(IndexedDbRead {
            entries: self.entries.lock().await.clone(),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(IndexedDbWrite {
            entries: Arc::clone(&self.entries),
            backend: self.backend.clone(),
            base: self.entries.lock().await.clone(),
            preconditions: opts.preconditions,
            overlay: EntriesOverlay::default(),
            immutable_values: BTreeMap::new(),
            await_durable: opts.await_durable,
            stats: WriteStats::default(),
        })
    }
}

impl StorageRead for IndexedDbRead {
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
        let entries = self
            .entries
            .range((physical.lower, physical.upper))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        ScanCursor::from_source(
            range,
            opts.order,
            IndexedDbScanSource {
                entries,
                offset: 0,
                projection: opts.projection,
                space,
            },
        )
    }
}

struct IndexedDbScanSource {
    entries: Vec<(Key, Bytes)>,
    offset: usize,
    projection: CoreProjection,
    space: StorageSpace,
}

impl StorageScanSource for IndexedDbScanSource {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let end = self
                .offset
                .saturating_add(limit_rows)
                .min(self.entries.len());
            let entries = self.entries[self.offset..end]
                .iter()
                .map(|(key, value)| {
                    Ok(ReadEntry {
                        key: decode_scan_key(self.space, key)?,
                        value: project_value(value, self.projection),
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
            self.offset = end;
            Ok(ScanChunk::new(entries, self.offset < self.entries.len()))
        })
    }
}

impl StorageWrite for IndexedDbWrite {
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
                        return Err(immutable_value_error());
                    }
                    continue;
                }
                if let Some(existing) = self.base.get(&key) {
                    if existing != &value {
                        return Err(immutable_value_error());
                    }
                    continue;
                }
                self.immutable_values.insert(key.clone(), value.clone());
            }
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            self.overlay.deletes.remove(&key);
            self.overlay.puts.insert(key, value);
        }
        self.stats.storage_calls += 1;
        Ok(())
    }

    async fn replace_many_for_migration(
        &mut self,
        _token: &lix::storage::MigrationReplaceToken,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        if space.value_semantics != ValueSemantics::Immutable
            || space.value_integrity == ValueIntegrity::ContentAddressed
        {
            return Err(StorageError::Corruption(
                "migration replacement requires an immutable storage space".to_string(),
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
            self.overlay.puts.remove(&key);
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
            .range((range.lower.clone(), range.upper.clone()))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let puts_before = self.overlay.puts.len();
        self.overlay
            .puts
            .retain(|key, _| !range_contains(&range, key));
        for key in &base_keys {
            self.overlay.deletes.insert(key.clone());
        }
        self.stats.deleted_entries +=
            (base_keys.len() + puts_before - self.overlay.puts.len()) as u64;
        self.stats.deleted_ranges += 1;
        self.stats.storage_calls += 1;
        Ok(())
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        let mut current = self.entries.lock().await;
        check_preconditions(&current, &self.preconditions)?;
        for (key, value) in &self.immutable_values {
            if current.get(key).is_some_and(|existing| existing != value) {
                return Err(immutable_value_error());
            }
        }

        let changes = ChangesDto {
            deletes: self
                .overlay
                .deletes
                .iter()
                .map(|key| ByteDto(key.0.to_vec()))
                .collect(),
            puts: self
                .overlay
                .puts
                .iter()
                .map(|(key, value)| EntryDto {
                    key: key.0.to_vec(),
                    value: value.to_vec(),
                })
                .collect(),
            strict_durability: self.await_durable,
        };
        let changes = serde_wasm_bindgen::to_value(&changes)
            .map_err(|error| StorageError::Io(format!("encode IndexedDB commit: {error}")))?;
        SendJsFuture(JsFuture::from(self.backend.0.apply_changes(changes)))
            .await
            .map_err(indexed_db_error)?;

        for key in self.overlay.deletes {
            current.remove(&key);
        }
        for (key, value) in self.overlay.puts {
            current.insert(key, value);
        }
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    async fn rollback(self) -> Result<(), StorageError> {
        Ok(())
    }
}

fn physical_key(space: SpaceId, key: &Key) -> Key {
    let mut bytes = BytesMut::with_capacity(4 + key.0.len());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Key(bytes.freeze())
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

fn physical_bound(space: SpaceId, bound: Bound<Key>, unbounded: Bound<Key>) -> Bound<Key> {
    match bound {
        Bound::Included(key) => Bound::Included(physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(physical_key(space, &key)),
        Bound::Unbounded => unbounded,
    }
}

fn decode_scan_key(space: StorageSpace, key: &Key) -> Result<Key, StorageError> {
    if key.0.len() < 4 || key.0[..4] != space.id.0.to_be_bytes() {
        return Err(StorageError::Corruption(
            "IndexedDB scan escaped its storage space".to_string(),
        ));
    }
    Ok(Key(key.0.slice(4..)))
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let lower = match &range.lower {
        Bound::Included(bound) => key >= bound,
        Bound::Excluded(bound) => key > bound,
        Bound::Unbounded => true,
    };
    let upper = match &range.upper {
        Bound::Included(bound) => key <= bound,
        Bound::Excluded(bound) => key < bound,
        Bound::Unbounded => true,
    };
    lower && upper
}

fn check_preconditions(
    entries: &EntryMap,
    preconditions: &[Precondition],
) -> Result<(), StorageError> {
    let failures = preconditions
        .iter()
        .enumerate()
        .filter_map(|(index, precondition)| {
            let matches = match precondition {
                Precondition::KeyAbsent { space, key } => {
                    !entries.contains_key(&physical_key(space.id, key))
                }
                Precondition::KeyPresent { space, key } => {
                    entries.contains_key(&physical_key(space.id, key))
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
                    entries.range((range.lower, range.upper)).next().is_none()
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

fn project_value(value: &Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value.clone()),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn immutable_value_error() -> StorageError {
    StorageError::Corruption("immutable identity was assigned different bytes".to_string())
}

fn indexed_db_error(error: JsValue) -> StorageError {
    let message = error
        .as_string()
        .or_else(|| {
            js_sys::Reflect::get(&error, &JsValue::from_str("message"))
                .ok()?
                .as_string()
        })
        .unwrap_or_else(|| "IndexedDB operation failed".to_string());
    StorageError::Io(message)
}
