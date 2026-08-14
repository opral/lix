//! Host-owned immutable arenas for the Lix plugin API v1.
//!
//! A [`Root`] names three independently persistent values:
//!
//! - exact accepted file bytes as a rope of immutable page slices;
//! - durable semantic rows as a copy-on-write keyed map;
//! - opaque plugin state as a separate copy-on-write keyed map.
//!
//! Transactions stage all three successors and publish one content-addressed
//! root only after every edit validates. Cloning a root is a constant-time
//! branch. Unchanged pages and map entries are shared across successors.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub const DEFAULT_PAGE_BYTES: usize = 64 * 1024;
const SUCCESSOR_CHECKPOINT_MAGIC: &[u8; 8] = b"LIXPSC01";
const SUCCESSOR_CHECKPOINT_HASH_DOMAIN: &[u8] = b"lix-plugin-v3/successor-checkpoint\0";
pub const REQUIRED_V1_SPEEDUP: u64 = 2;
pub const REQUIRED_V1_MEMORY_REDUCTION: u64 = 3;

/// One end-to-end benchmark lane. `peak_total_bytes` must include host live
/// ownership, guest linear-memory high water, and transient materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerformanceMeasurement {
    pub p95_nanoseconds: u64,
    pub peak_total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Acceptance {
    pub latency_passes: bool,
    pub memory_passes: bool,
}

impl Acceptance {
    pub const fn passes(self) -> bool {
        self.latency_passes && self.memory_passes
    }
}

/// Applies the non-negotiable API v1 performance gate without floating-point
/// rounding: the candidate must be at least 2× faster and consume at least 3×
/// less total peak memory than its matching baseline.
pub const fn compare_to_baseline(
    baseline: PerformanceMeasurement,
    candidate: PerformanceMeasurement,
) -> Acceptance {
    Acceptance {
        latency_passes: match candidate.p95_nanoseconds.checked_mul(REQUIRED_V1_SPEEDUP) {
            Some(scaled) => scaled <= baseline.p95_nanoseconds,
            None => false,
        },
        memory_passes: match candidate
            .peak_total_bytes
            .checked_mul(REQUIRED_V1_MEMORY_REDUCTION)
        {
            Some(scaled) => scaled <= baseline.peak_total_bytes,
            None => false,
        },
    }
}

/// Format-owned paging policy. These declarations live with each plugin so a
/// schema or semantic boundary cannot silently drift into host infrastructure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatLayout {
    pub plugin_key: &'static str,
    pub schema_keys: &'static [&'static str],
    pub state_pages: &'static [StatePageLayout],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatePageLayout {
    pub kind: &'static str,
    pub target_items: u32,
}

impl FormatLayout {
    pub const fn is_valid(self) -> bool {
        !self.plugin_key.is_empty()
            && !self.schema_keys.is_empty()
            && !self.state_pages.is_empty()
            && state_pages_are_valid(self.state_pages)
    }
}

const fn state_pages_are_valid(pages: &[StatePageLayout]) -> bool {
    let mut index = 0;
    while index < pages.len() {
        if pages[index].kind.is_empty() || pages[index].target_items == 0 {
            return false;
        }
        index += 1;
    }
    true
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", &self.to_string()[..16])
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

fn digest(domain: &[u8], parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for part in parts {
        let part = part.as_ref();
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    Digest(*hasher.finalize().as_bytes())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Metrics {
    pub page_reads: u64,
    pub page_bytes_read: u64,
    pub pages_interned: u64,
    pub page_bytes_interned: u64,
}

#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

#[derive(Debug)]
struct StoreInner {
    page_bytes: usize,
    pages: Mutex<HashMap<Digest, Weak<[u8]>>>,
    interns_since_sweep: AtomicUsize,
    metrics: Mutex<Metrics>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new(DEFAULT_PAGE_BYTES)
    }
}

impl Store {
    pub fn new(page_bytes: usize) -> Self {
        assert!(page_bytes > 0, "arena pages must be non-empty");
        Self {
            inner: Arc::new(StoreInner {
                page_bytes,
                pages: Mutex::new(HashMap::new()),
                interns_since_sweep: AtomicUsize::new(0),
                metrics: Mutex::new(Metrics::default()),
            }),
        }
    }

    pub fn page_bytes(&self) -> usize {
        self.inner.page_bytes
    }

    pub fn metrics(&self) -> Metrics {
        *self.inner.metrics.lock()
    }

    pub fn reset_metrics(&self) {
        *self.inner.metrics.lock() = Metrics::default();
    }

    pub fn unique_page_count(&self) -> usize {
        let mut pages = self.inner.pages.lock();
        pages.retain(|_, page| page.strong_count() > 0);
        pages.len()
    }

    pub fn unique_page_bytes(&self) -> usize {
        let mut pages = self.inner.pages.lock();
        pages.retain(|_, page| page.strong_count() > 0);
        pages
            .values()
            .filter_map(Weak::upgrade)
            .map(|page| page.len())
            .sum()
    }

    fn intern(&self, bytes: &[u8]) -> Page {
        let id = digest(b"lix-plugin-v3/page\0", [bytes]);
        let mut pages = self.inner.pages.lock();
        if self
            .inner
            .interns_since_sweep
            .fetch_add(1, Ordering::Relaxed)
            >= 1_023
        {
            pages.retain(|_, page| page.strong_count() > 0);
            self.inner.interns_since_sweep.store(0, Ordering::Relaxed);
        }
        if let Some(bytes) = pages.get(&id).and_then(Weak::upgrade) {
            return Page { id, bytes };
        }
        let bytes: Arc<[u8]> = Arc::from(bytes);
        pages.insert(id, Arc::downgrade(&bytes));
        {
            let mut metrics = self.inner.metrics.lock();
            metrics.pages_interned = metrics.pages_interned.saturating_add(1);
            metrics.page_bytes_interned = metrics
                .page_bytes_interned
                .saturating_add(bytes.len() as u64);
        }
        Page { id, bytes }
    }

    fn read_page(&self, page: &Page, range: Range<usize>) -> Result<Vec<u8>, Error> {
        let selected = page.bytes.get(range).ok_or(Error::InvalidPageRange)?;
        let output = selected.to_vec();
        let mut metrics = self.inner.metrics.lock();
        metrics.page_reads = metrics.page_reads.saturating_add(1);
        metrics.page_bytes_read = metrics.page_bytes_read.saturating_add(output.len() as u64);
        Ok(output)
    }

    fn insert_archived_page(&self, expected: Digest, bytes: &[u8]) -> Result<Page, Error> {
        let actual = digest(b"lix-plugin-v3/page\0", [bytes]);
        if actual != expected {
            return Err(Error::CorruptArchive);
        }
        Ok(self.intern(bytes))
    }

    fn page(&self, id: Digest) -> Result<Page, Error> {
        self.inner
            .pages
            .lock()
            .get(&id)
            .and_then(Weak::upgrade)
            .map(|bytes| Page { id, bytes })
            .ok_or(Error::MissingPage(id))
    }
}

#[derive(Debug, Clone)]
struct Page {
    id: Digest,
    bytes: Arc<[u8]>,
}

impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Page {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Segment {
    page: Page,
    offset: u32,
    len: u32,
}

#[derive(Debug, Clone)]
pub struct ByteArena {
    store: Store,
    len: u64,
    segments: Arc<[Segment]>,
    id: Digest,
    retained_heap_bytes: usize,
}

impl ByteArena {
    pub fn empty(store: Store) -> Self {
        Self::from_segments(store, 0, Vec::new())
    }

    pub fn from_bytes(store: Store, bytes: &[u8]) -> Self {
        let segments = bytes
            .chunks(store.page_bytes())
            .map(|chunk| Segment {
                page: store.intern(chunk),
                offset: 0,
                len: u32::try_from(chunk.len()).expect("page size must fit u32"),
            })
            .collect();
        Self::from_segments(store, bytes.len() as u64, segments)
    }

    fn from_segments(store: Store, len: u64, segments: Vec<Segment>) -> Self {
        let mut parts = Vec::with_capacity(1 + segments.len() * 3);
        parts.push(len.to_le_bytes().to_vec());
        for segment in &segments {
            parts.push(segment.page.id.0.to_vec());
            parts.push(segment.offset.to_le_bytes().to_vec());
            parts.push(segment.len.to_le_bytes().to_vec());
        }
        let id = digest(b"lix-plugin-v3/bytes\0", parts);
        let retained_heap_bytes = size_of::<ByteArena>()
            .saturating_add(segments.len().saturating_mul(size_of::<Segment>()))
            .saturating_add(2 * size_of::<usize>())
            .saturating_add(segments.iter().fold(0_usize, |total, segment| {
                total
                    .saturating_add(segment.page.bytes.len())
                    .saturating_add(2 * size_of::<usize>())
            }));
        Self {
            store,
            len,
            segments: segments.into(),
            id,
            retained_heap_bytes,
        }
    }

    pub fn id(&self) -> Digest {
        self.id
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>, Error> {
        let end = offset.checked_add(length).ok_or(Error::RangeOverflow)?;
        if end > self.len {
            return Err(Error::RangeOutOfBounds);
        }
        let capacity = usize::try_from(length).map_err(|_| Error::RangeOverflow)?;
        let mut output = Vec::with_capacity(capacity);
        let mut logical = 0_u64;
        for segment in self.segments.iter() {
            let segment_end = logical + u64::from(segment.len);
            let selected_start = offset.max(logical);
            let selected_end = end.min(segment_end);
            if selected_start < selected_end {
                let start = usize::try_from(
                    u64::from(segment.offset) + selected_start.saturating_sub(logical),
                )
                .map_err(|_| Error::RangeOverflow)?;
                let selected_len = usize::try_from(selected_end - selected_start)
                    .map_err(|_| Error::RangeOverflow)?;
                output.extend_from_slice(
                    &self.store.read_page(
                        &segment.page,
                        start
                            ..start
                                .checked_add(selected_len)
                                .ok_or(Error::RangeOverflow)?,
                    )?,
                );
            }
            if segment_end >= end {
                break;
            }
            logical = segment_end;
        }
        Ok(output)
    }

    pub fn apply(&self, edits: &[ByteEdit]) -> Result<Self, Error> {
        validate_edits(edits, self.len)?;
        let mut successor = Vec::new();
        let mut cursor = 0_u64;
        let mut successor_len = self.len;
        for edit in edits {
            self.append_segments(cursor, edit.offset, &mut successor)?;
            for chunk in edit.insert.chunks(self.store.page_bytes()) {
                if !chunk.is_empty() {
                    successor.push(Segment {
                        page: self.store.intern(chunk),
                        offset: 0,
                        len: u32::try_from(chunk.len()).expect("page size must fit u32"),
                    });
                }
            }
            cursor = edit
                .offset
                .checked_add(edit.delete_len)
                .ok_or(Error::RangeOverflow)?;
            successor_len = successor_len
                .checked_sub(edit.delete_len)
                .and_then(|len| len.checked_add(edit.insert.len() as u64))
                .ok_or(Error::RangeOverflow)?;
        }
        self.append_segments(cursor, self.len, &mut successor)?;
        coalesce_segments(&mut successor);
        repack_fragmented_segments(&self.store, &mut successor);
        Ok(Self::from_segments(
            self.store.clone(),
            successor_len,
            successor,
        ))
    }

    fn append_segments(
        &self,
        start: u64,
        end: u64,
        output: &mut Vec<Segment>,
    ) -> Result<(), Error> {
        if start > end || end > self.len {
            return Err(Error::RangeOutOfBounds);
        }
        let mut logical = 0_u64;
        for segment in self.segments.iter() {
            let segment_end = logical + u64::from(segment.len);
            let selected_start = start.max(logical);
            let selected_end = end.min(segment_end);
            if selected_start < selected_end {
                output.push(Segment {
                    page: segment.page.clone(),
                    offset: u32::try_from(
                        u64::from(segment.offset) + selected_start.saturating_sub(logical),
                    )
                    .map_err(|_| Error::RangeOverflow)?,
                    len: u32::try_from(selected_end - selected_start)
                        .map_err(|_| Error::RangeOverflow)?,
                });
            }
            if segment_end >= end {
                break;
            }
            logical = segment_end;
        }
        Ok(())
    }
}

fn repack_fragmented_segments(store: &Store, segments: &mut Vec<Segment>) {
    let page_bytes = store.page_bytes();
    let mut output = Vec::with_capacity(segments.len());
    let mut fragments = Vec::new();
    for segment in segments.drain(..) {
        let is_full_page = segment.offset == 0
            && segment.len as usize == page_bytes
            && segment.page.bytes.len() == page_bytes;
        if is_full_page {
            flush_fragmented_segments(store, &mut fragments, &mut output);
            output.push(segment);
        } else {
            fragments.push(segment);
        }
    }
    flush_fragmented_segments(store, &mut fragments, &mut output);
    *segments = output;
}

fn flush_fragmented_segments(
    store: &Store,
    fragments: &mut Vec<Segment>,
    output: &mut Vec<Segment>,
) {
    if fragments.len() <= 1 {
        output.append(fragments);
        return;
    }
    let total = fragments
        .iter()
        .map(|segment| segment.len as usize)
        .sum::<usize>();
    let mut bytes = Vec::with_capacity(total);
    for segment in fragments.drain(..) {
        let start = segment.offset as usize;
        let end = start + segment.len as usize;
        bytes.extend_from_slice(&segment.page.bytes[start..end]);
    }
    for chunk in bytes.chunks(store.page_bytes()) {
        output.push(Segment {
            page: store.intern(chunk),
            offset: 0,
            len: u32::try_from(chunk.len()).expect("page size must fit u32"),
        });
    }
}

fn coalesce_segments(segments: &mut Vec<Segment>) {
    let mut output: Vec<Segment> = Vec::with_capacity(segments.len());
    for segment in segments.drain(..) {
        if let Some(last) = output.last_mut()
            && last.page.id == segment.page.id
            && last.offset.checked_add(last.len) == Some(segment.offset)
            && last.len.checked_add(segment.len).is_some()
        {
            last.len += segment.len;
        } else {
            output.push(segment);
        }
    }
    *segments = output;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Vec<u8>,
}

fn validate_edits(edits: &[ByteEdit], base_len: u64) -> Result<(), Error> {
    let mut previous_end = 0_u64;
    for (index, edit) in edits.iter().enumerate() {
        let end = edit
            .offset
            .checked_add(edit.delete_len)
            .ok_or(Error::RangeOverflow)?;
        if end > base_len || (index > 0 && edit.offset < previous_end) {
            return Err(Error::InvalidEdits);
        }
        if index > 0 && edit.offset == edits[index - 1].offset {
            return Err(Error::InvalidEdits);
        }
        previous_end = end;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct MapArena {
    store: Store,
    entries: Arc<BTreeMap<Vec<u8>, ByteArena>>,
    id: Digest,
    retained_heap_bytes: usize,
}

impl MapArena {
    pub fn empty(store: Store) -> Self {
        Self::from_entries(store, BTreeMap::new())
    }

    fn from_entries(store: Store, entries: BTreeMap<Vec<u8>, ByteArena>) -> Self {
        let mut parts = Vec::with_capacity(entries.len() * 2);
        let mut retained_heap_bytes = size_of::<MapArena>();
        for (key, value) in &entries {
            parts.push(key.clone());
            parts.push(value.id.0.to_vec());
            retained_heap_bytes = retained_heap_bytes
                .saturating_add(key.capacity())
                .saturating_add(size_of::<(Vec<u8>, ByteArena)>())
                .saturating_add(64)
                .saturating_add(value.retained_heap_bytes);
        }
        let id = digest(b"lix-plugin-v3/map\0", parts);
        Self {
            store,
            entries: Arc::new(entries),
            id,
            retained_heap_bytes,
        }
    }

    pub fn id(&self) -> Digest {
        self.id
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.entries
            .get(key)
            .map(|value| value.read(0, value.len()))
            .transpose()
    }

    pub fn value_len(&self, key: &[u8]) -> Option<u64> {
        self.entries.get(key).map(ByteArena::len)
    }

    pub fn read(&self, key: &[u8], offset: u64, length: u64) -> Result<Option<Vec<u8>>, Error> {
        self.entries
            .get(key)
            .map(|value| value.read(offset, length))
            .transpose()
    }

    pub fn value_id(&self, key: &[u8]) -> Option<Digest> {
        self.entries.get(key).map(ByteArena::id)
    }

    fn apply(&self, changes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Self {
        if changes.is_empty() {
            return self.clone();
        }
        let mut entries = self.entries.as_ref().clone();
        for (key, value) in changes {
            if let Some(value) = value {
                entries.insert(
                    key.clone(),
                    ByteArena::from_bytes(self.store.clone(), value),
                );
            } else {
                entries.remove(key.as_slice());
            }
        }
        Self::from_entries(self.store.clone(), entries)
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.keys().map(Vec::as_slice)
    }

    fn archived(&self) -> ArchivedMap {
        ArchivedMap {
            id: self.id,
            entries: self
                .entries
                .iter()
                .map(|(key, value)| (key.clone(), value.archived()))
                .collect(),
        }
    }

    fn reopen(store: Store, archived: &ArchivedMap) -> Result<Self, Error> {
        let arena = Self::from_entries(
            store.clone(),
            archived
                .entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), ByteArena::reopen(store.clone(), value)?)))
                .collect::<Result<_, Error>>()?,
        );
        if arena.id != archived.id {
            return Err(Error::CorruptArchive);
        }
        Ok(arena)
    }
}

#[derive(Debug, Clone)]
pub struct Root {
    pub generation: Arc<str>,
    pub bytes: ByteArena,
    pub rows: MapArena,
    pub state: MapArena,
    id: Digest,
    retained_heap_bytes: usize,
}

impl Root {
    pub fn empty(store: Store, generation: impl Into<Arc<str>>) -> Self {
        Self::new(
            generation.into(),
            ByteArena::empty(store.clone()),
            MapArena::empty(store.clone()),
            MapArena::empty(store),
        )
    }

    pub fn import(
        store: Store,
        generation: impl Into<Arc<str>>,
        bytes: &[u8],
        rows: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        state: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Self {
        let rows = rows
            .into_iter()
            .map(|(key, value)| (key, ByteArena::from_bytes(store.clone(), &value)))
            .collect();
        let state = state
            .into_iter()
            .map(|(key, value)| (key, ByteArena::from_bytes(store.clone(), &value)))
            .collect();
        Self::new(
            generation.into(),
            ByteArena::from_bytes(store.clone(), bytes),
            MapArena::from_entries(store.clone(), rows),
            MapArena::from_entries(store, state),
        )
    }

    fn new(generation: Arc<str>, bytes: ByteArena, rows: MapArena, state: MapArena) -> Self {
        let id = digest(
            b"lix-plugin-v3/root\0",
            [
                generation.as_bytes(),
                bytes.id.as_bytes(),
                rows.id.as_bytes(),
                state.id.as_bytes(),
            ],
        );
        let retained_heap_bytes = size_of::<Root>()
            .saturating_add(bytes.retained_heap_bytes)
            .saturating_add(rows.retained_heap_bytes)
            .saturating_add(state.retained_heap_bytes);
        Self {
            generation,
            bytes,
            rows,
            state,
            id,
            retained_heap_bytes,
        }
    }

    pub fn id(&self) -> Digest {
        self.id
    }

    /// Approximate heap bytes retained by this immutable root.
    ///
    /// This includes page payloads plus the per-row keys, arenas,
    /// segment arrays, and a conservative allowance for `BTreeMap` nodes and
    /// allocation headers. It deliberately overcounts shared pages and
    /// metadata so a decoded-root cache remains a hard memory bound. The value
    /// is maintained while immutable arenas are built, so reading it is O(1).
    pub fn retained_heap_bytes(&self) -> usize {
        self.retained_heap_bytes
    }

    /// Returns the byte-and-state view needed to resume an accepted file
    /// transition after its Wasm Store is evicted.
    ///
    /// Durable rows remain the engine's semantic authority; production
    /// plugins keep their incremental indexes in `state` and do not consult
    /// the row arena while applying a byte successor. Retaining row
    /// snapshots here would therefore keep a second complete semantic copy
    /// solely as a cache optimization.
    pub fn successor_checkpoint(&self) -> Self {
        Self::new(
            self.generation.clone(),
            self.bytes.clone(),
            MapArena::empty(self.bytes.store.clone()),
            self.state.clone(),
        )
    }

    /// Encodes only opaque plugin state. Accepted file bytes are already
    /// durable in the engine's binary CAS and semantic rows remain in
    /// tracked state, so neither is duplicated in this checkpoint payload.
    pub fn successor_checkpoint_encoded_len(&self) -> Result<usize, Error> {
        let mut len = SUCCESSOR_CHECKPOINT_MAGIC
            .len()
            .checked_add(12)
            .and_then(|len| len.checked_add(self.generation.len()))
            .ok_or(Error::RangeOverflow)?;
        for (key, value) in self.state.entries.iter() {
            let value_len = usize::try_from(value.len()).map_err(|_| Error::RangeOverflow)?;
            len = len
                .checked_add(12)
                .and_then(|len| len.checked_add(key.len()))
                .and_then(|len| len.checked_add(value_len))
                .ok_or(Error::RangeOverflow)?;
        }
        len.checked_add(32).ok_or(Error::RangeOverflow)
    }

    pub fn encode_successor_checkpoint(&self) -> Result<Vec<u8>, Error> {
        let generation = self.generation.as_bytes();
        let generation_len = u32::try_from(generation.len()).map_err(|_| Error::RangeOverflow)?;
        let state_count =
            u32::try_from(self.state.entries.len()).map_err(|_| Error::RangeOverflow)?;
        let page_bytes =
            u32::try_from(self.bytes.store.page_bytes()).map_err(|_| Error::RangeOverflow)?;
        let mut encoded = Vec::with_capacity(self.successor_checkpoint_encoded_len()?);
        encoded.extend_from_slice(SUCCESSOR_CHECKPOINT_MAGIC);
        encoded.extend_from_slice(&page_bytes.to_le_bytes());
        encoded.extend_from_slice(&generation_len.to_le_bytes());
        encoded.extend_from_slice(&state_count.to_le_bytes());
        encoded.extend_from_slice(generation);
        for (key, value) in self.state.entries.iter() {
            let key_len = u32::try_from(key.len()).map_err(|_| Error::RangeOverflow)?;
            encoded.extend_from_slice(&key_len.to_le_bytes());
            encoded.extend_from_slice(&value.len().to_le_bytes());
            encoded.extend_from_slice(key);
            encoded.extend_from_slice(&value.read(0, value.len())?);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(SUCCESSOR_CHECKPOINT_HASH_DOMAIN);
        hasher.update(&encoded);
        encoded.extend_from_slice(hasher.finalize().as_bytes());
        Ok(encoded)
    }

    /// Reopens a successor checkpoint against independently loaded accepted
    /// bytes. The row arena intentionally starts empty; tracked-state
    /// authority is validated by the engine before publication.
    pub fn decode_successor_checkpoint(accepted: &[u8], encoded: &[u8]) -> Result<Self, Error> {
        let payload_len = encoded
            .len()
            .checked_sub(32)
            .ok_or(Error::CorruptCheckpoint)?;
        let (payload, checksum) = encoded.split_at(payload_len);
        let mut hasher = blake3::Hasher::new();
        hasher.update(SUCCESSOR_CHECKPOINT_HASH_DOMAIN);
        hasher.update(payload);
        if hasher.finalize().as_bytes() != checksum {
            return Err(Error::CorruptCheckpoint);
        }
        let mut cursor = CheckpointCursor::new(payload);
        if cursor.take(SUCCESSOR_CHECKPOINT_MAGIC.len())? != SUCCESSOR_CHECKPOINT_MAGIC {
            return Err(Error::CorruptCheckpoint);
        }
        let page_bytes = usize::try_from(cursor.u32()?).map_err(|_| Error::CorruptCheckpoint)?;
        if page_bytes == 0 {
            return Err(Error::CorruptCheckpoint);
        }
        let generation_len =
            usize::try_from(cursor.u32()?).map_err(|_| Error::CorruptCheckpoint)?;
        let state_count = usize::try_from(cursor.u32()?).map_err(|_| Error::CorruptCheckpoint)?;
        let generation = std::str::from_utf8(cursor.take(generation_len)?)
            .map_err(|_| Error::CorruptCheckpoint)?;
        let mut state = Vec::new();
        for _ in 0..state_count {
            let key_len = usize::try_from(cursor.u32()?).map_err(|_| Error::CorruptCheckpoint)?;
            let value_len = usize::try_from(cursor.u64()?).map_err(|_| Error::CorruptCheckpoint)?;
            state.push((
                cursor.take(key_len)?.to_vec(),
                cursor.take(value_len)?.to_vec(),
            ));
        }
        if !cursor.is_empty() {
            return Err(Error::CorruptCheckpoint);
        }
        Ok(Self::import(
            Store::new(page_bytes),
            Arc::<str>::from(generation),
            accepted,
            std::iter::empty(),
            state,
        ))
    }

    pub fn transaction(&self) -> Transaction {
        Transaction {
            base: self.clone(),
            byte_edits: Vec::new(),
            row_changes: BTreeMap::new(),
            state_changes: BTreeMap::new(),
            generation: None,
        }
    }

    /// Serializes the immutable manifests and reachable content-addressed
    /// pages. Reopening does not parse plugin state or change the root digest.
    pub fn archive(&self) -> Result<Archive, Error> {
        let mut reachable = BTreeMap::new();
        collect_byte_pages(&self.bytes, &mut reachable)?;
        for value in self.rows.entries.values() {
            collect_byte_pages(value, &mut reachable)?;
        }
        for value in self.state.entries.values() {
            collect_byte_pages(value, &mut reachable)?;
        }
        Ok(Archive {
            page_bytes: self.bytes.store.page_bytes(),
            pages: reachable.into_iter().collect(),
            generation: self.generation.clone(),
            bytes: self.bytes.archived(),
            rows: self.rows.archived(),
            state: self.state.archived(),
            id: self.id,
        })
    }

    /// Deterministically merges durable row values. Opaque state is not
    /// merge authority and is retained from `a`; callers may rebuild it.
    /// Concurrent unequal values use their content digest as a canonical,
    /// merge-direction-independent tie break.
    pub fn merge_rows(base: &Self, a: &Self, b: &Self) -> Result<Self, Error> {
        if !Arc::ptr_eq(&base.bytes.store.inner, &a.bytes.store.inner)
            || !Arc::ptr_eq(&base.bytes.store.inner, &b.bytes.store.inner)
        {
            return Err(Error::DifferentStores);
        }
        let mut keys = BTreeMap::<Vec<u8>, ()>::new();
        for key in base
            .rows
            .entries
            .keys()
            .chain(a.rows.entries.keys())
            .chain(b.rows.entries.keys())
        {
            keys.insert(key.clone(), ());
        }
        let mut merged = BTreeMap::new();
        for key in keys.keys() {
            let base_value = base.rows.entries.get(key);
            let a_value = a.rows.entries.get(key);
            let b_value = b.rows.entries.get(key);
            let selected = if same_value(a_value, b_value) {
                a_value
            } else if same_value(a_value, base_value) {
                b_value
            } else if same_value(b_value, base_value) {
                a_value
            } else {
                [a_value, b_value]
                    .into_iter()
                    .flatten()
                    .min_by_key(|value| value.id())
            };
            if let Some(selected) = selected {
                merged.insert(key.clone(), selected.clone());
            }
        }
        Ok(Self::new(
            a.generation.clone(),
            a.bytes.clone(),
            MapArena::from_entries(a.bytes.store.clone(), merged),
            a.state.clone(),
        ))
    }
}

fn same_value(a: Option<&ByteArena>, b: Option<&ByteArena>) -> bool {
    a.map(ByteArena::id) == b.map(ByteArena::id)
}

fn collect_byte_pages(
    arena: &ByteArena,
    output: &mut BTreeMap<Digest, Vec<u8>>,
) -> Result<(), Error> {
    for segment in arena.segments.iter() {
        output
            .entry(segment.page.id)
            .or_insert_with(|| segment.page.bytes.as_ref().to_vec());
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ArchivedSegment {
    page: Digest,
    offset: u32,
    len: u32,
}

#[derive(Debug, Clone)]
struct ArchivedByteArena {
    id: Digest,
    len: u64,
    segments: Vec<ArchivedSegment>,
}

impl ByteArena {
    fn archived(&self) -> ArchivedByteArena {
        ArchivedByteArena {
            id: self.id,
            len: self.len,
            segments: self
                .segments
                .iter()
                .map(|segment| ArchivedSegment {
                    page: segment.page.id,
                    offset: segment.offset,
                    len: segment.len,
                })
                .collect(),
        }
    }

    fn reopen(store: Store, archived: &ArchivedByteArena) -> Result<Self, Error> {
        let segments = archived
            .segments
            .iter()
            .map(|segment| {
                Ok(Segment {
                    page: store.page(segment.page)?,
                    offset: segment.offset,
                    len: segment.len,
                })
            })
            .collect::<Result<_, Error>>()?;
        let arena = Self::from_segments(store, archived.len, segments);
        if arena.id != archived.id {
            return Err(Error::CorruptArchive);
        }
        Ok(arena)
    }
}

#[derive(Debug, Clone)]
struct ArchivedMap {
    id: Digest,
    entries: Vec<(Vec<u8>, ArchivedByteArena)>,
}

/// Durable representation used to reopen an arena root after all decoded
/// pages and Wasm instances have been evicted.
#[derive(Debug, Clone)]
pub struct Archive {
    page_bytes: usize,
    pages: Vec<(Digest, Vec<u8>)>,
    generation: Arc<str>,
    bytes: ArchivedByteArena,
    rows: ArchivedMap,
    state: ArchivedMap,
    id: Digest,
}

impl Archive {
    pub fn reopen(&self) -> Result<(Store, Root), Error> {
        let store = Store::new(self.page_bytes);
        let _pages = self
            .pages
            .iter()
            .map(|(id, bytes)| store.insert_archived_page(*id, bytes))
            .collect::<Result<Vec<_>, Error>>()?;
        let root = Root::new(
            self.generation.clone(),
            ByteArena::reopen(store.clone(), &self.bytes)?,
            MapArena::reopen(store.clone(), &self.rows)?,
            MapArena::reopen(store.clone(), &self.state)?,
        );
        if root.id != self.id {
            return Err(Error::CorruptArchive);
        }
        Ok((store, root))
    }
}

#[derive(Debug)]
pub struct Transaction {
    base: Root,
    byte_edits: Vec<ByteEdit>,
    row_changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    state_changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    generation: Option<Arc<str>>,
}

impl Transaction {
    pub fn edit_bytes(&mut self, edit: ByteEdit) {
        self.byte_edits.push(edit);
    }

    pub fn upsert_row(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.row_changes.insert(key, Some(value));
    }

    pub fn put_state(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.state_changes.insert(key, Some(value));
    }

    pub fn delete_state(&mut self, key: Vec<u8>) {
        self.state_changes.insert(key, None);
    }

    pub fn upgrade_to(&mut self, generation: impl Into<Arc<str>>) {
        self.generation = Some(generation.into());
    }

    pub fn commit(mut self) -> Result<Root, Error> {
        self.byte_edits.sort_by_key(|edit| edit.offset);
        let bytes = self.base.bytes.apply(&self.byte_edits)?;
        let rows = self.base.rows.apply(&self.row_changes);
        let state = self.base.state.apply(&self.state_changes);
        Ok(Root::new(
            self.generation.unwrap_or(self.base.generation),
            bytes,
            rows,
            state,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    CorruptArchive,
    CorruptCheckpoint,
    DifferentStores,
    InvalidEdits,
    InvalidPageRange,
    MissingPage(Digest),
    RangeOutOfBounds,
    RangeOverflow,
}

struct CheckpointCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CheckpointCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(Error::CorruptCheckpoint)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(Error::CorruptCheckpoint)?;
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| Error::CorruptCheckpoint)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| Error::CorruptCheckpoint)?,
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Store, Root) {
        let store = Store::new(4);
        let root = Root::import(
            store.clone(),
            "generation-a",
            b"abcdefghijkl",
            [(b"row/1".to_vec(), b"{\"id\":1}".to_vec())],
            [(b"index/0".to_vec(), b"row/1".to_vec())],
        );
        (store, root)
    }

    #[test]
    fn exact_bytes_and_sparse_successor_share_unchanged_pages() {
        let (store, root) = fixture();
        let base_pages = store.unique_page_count();
        let mut transaction = root.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: 5,
            delete_len: 1,
            insert: b"X".to_vec(),
        });
        transaction.upsert_row(b"row/1".to_vec(), b"{\"id\":1,\"v\":\"X\"}".to_vec());
        transaction.put_state(b"index/0".to_vec(), b"row/1@5".to_vec());
        let successor = transaction.commit().unwrap();

        assert_eq!(
            successor.bytes.read(0, successor.bytes.len()).unwrap(),
            b"abcdeXghijkl"
        );
        assert_eq!(
            root.bytes.read(0, root.bytes.len()).unwrap(),
            b"abcdefghijkl"
        );
        assert_ne!(root.id(), successor.id());
        assert!(store.unique_page_count() < base_pages * 2 + 8);
    }

    #[test]
    fn dropped_roots_reclaim_unreferenced_page_contents() {
        let store = Store::new(4);
        let mut root = Root::import(
            store.clone(),
            "generation-a",
            b"abcdefgh",
            std::iter::empty(),
            std::iter::empty(),
        );
        for value in 0_u8..64 {
            let mut transaction = root.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: 0,
                delete_len: 1,
                insert: vec![value],
            });
            root = transaction.commit().unwrap();
        }

        assert_eq!(root.bytes.read(0, root.bytes.len()).unwrap().len(), 8);
        assert_eq!(store.unique_page_count(), root.bytes.segment_count());
        assert!(store.unique_page_bytes() <= 8 + store.page_bytes());
    }

    #[test]
    fn repeated_small_insertions_keep_byte_segments_page_bounded() {
        let store = Store::new(64);
        let mut root = Root::empty(store.clone(), "generation-a");
        for _ in 0..10_000 {
            let mut transaction = root.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: root.bytes.len(),
                delete_len: 0,
                insert: vec![b'x'],
            });
            root = transaction.commit().unwrap();
        }

        assert_eq!(
            root.bytes.read(0, root.bytes.len()).unwrap(),
            vec![b'x'; 10_000]
        );
        assert!(
            root.bytes.segment_count() <= 10_000_usize.div_ceil(store.page_bytes()),
            "incremental inserts must not retain one segment per edit"
        );
    }

    #[test]
    fn rollback_does_not_publish_a_partial_root() {
        let (_store, root) = fixture();
        let original = root.id();
        let mut transaction = root.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: 99,
            delete_len: 1,
            insert: Vec::new(),
        });
        transaction.upsert_row(b"row/2".to_vec(), b"partial".to_vec());
        assert!(matches!(transaction.commit(), Err(Error::InvalidEdits)));
        assert_eq!(root.id(), original);
        assert!(root.rows.get(b"row/2").unwrap().is_none());
    }

    #[test]
    fn branches_are_stable_and_deterministic() {
        let (_store, root) = fixture();
        let branch = root.clone();
        let make_successor = |base: &Root| {
            let mut transaction = base.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: 4,
                delete_len: 0,
                insert: b"!".to_vec(),
            });
            transaction.upsert_row(b"row/2".to_vec(), b"{\"id\":2}".to_vec());
            transaction.commit().unwrap()
        };
        let a = make_successor(&root);
        let b = make_successor(&branch);
        assert_eq!(a.id(), b.id());
        assert_eq!(
            root.bytes.read(0, root.bytes.len()).unwrap(),
            b"abcdefghijkl"
        );
    }

    #[test]
    fn upgrade_changes_generation_without_rewriting_arenas() {
        let (_store, root) = fixture();
        let byte_id = root.bytes.id();
        let row_id = root.rows.id();
        let mut transaction = root.transaction();
        transaction.upgrade_to("generation-b");
        let upgraded = transaction.commit().unwrap();
        assert_eq!(upgraded.bytes.id(), byte_id);
        assert_eq!(upgraded.rows.id(), row_id);
        assert_ne!(upgraded.id(), root.id());
    }

    #[test]
    fn reads_materialize_only_requested_pages() {
        let (store, root) = fixture();
        store.reset_metrics();
        assert_eq!(root.bytes.read(5, 2).unwrap(), b"fg");
        let metrics = store.metrics();
        assert_eq!(metrics.page_reads, 1);
        assert_eq!(metrics.page_bytes_read, 2);
    }

    #[test]
    fn retained_heap_accounting_includes_row_index_overhead() {
        let store = Store::new(64);
        let empty = Root::empty(store.clone(), "generation-a");
        let populated = Root::import(
            store,
            "generation-a",
            b"x",
            (0..1_000).map(|index| {
                (
                    format!("row/{index:04}").into_bytes(),
                    format!("{{\"id\":{index}}}").into_bytes(),
                )
            }),
            std::iter::empty(),
        );

        assert!(populated.retained_heap_bytes() > empty.retained_heap_bytes());
        assert!(
            populated.retained_heap_bytes() > 100_000,
            "small row snapshots still retain map, key, arena, and allocation metadata"
        );
    }

    #[test]
    fn successor_checkpoint_retains_bytes_and_state_without_rows() {
        let (_store, root) = fixture();
        let checkpoint = root.successor_checkpoint();

        assert_eq!(checkpoint.generation, root.generation);
        assert_eq!(checkpoint.bytes.id(), root.bytes.id());
        assert_eq!(checkpoint.state.id(), root.state.id());
        assert!(checkpoint.rows.is_empty());
        assert_eq!(
            checkpoint.state.get(b"index/0").unwrap(),
            Some(b"row/1".to_vec())
        );
        assert!(checkpoint.retained_heap_bytes() < root.retained_heap_bytes());
    }

    #[test]
    fn encoded_successor_checkpoint_reopens_against_independent_bytes() {
        let (_store, root) = fixture();
        let encoded = root.encode_successor_checkpoint().unwrap();
        assert_eq!(
            encoded.len(),
            root.successor_checkpoint_encoded_len().unwrap()
        );
        let reopened = Root::decode_successor_checkpoint(b"abcdefghijkl", &encoded).unwrap();

        assert_eq!(reopened.generation, root.generation);
        assert_eq!(
            reopened.bytes.read(0, reopened.bytes.len()).unwrap(),
            b"abcdefghijkl"
        );
        assert_eq!(
            reopened.state.get(b"index/0").unwrap(),
            Some(b"row/1".to_vec())
        );
        assert!(reopened.rows.is_empty());

        let mut corrupt = encoded;
        corrupt[8] ^= 1;
        assert!(matches!(
            Root::decode_successor_checkpoint(b"abcdefghijkl", &corrupt),
            Err(Error::CorruptCheckpoint)
        ));
    }

    #[test]
    fn unchanged_map_values_keep_stable_identity() {
        let (_store, root) = fixture();
        let before = root.rows.value_id(b"row/1").unwrap();
        let mut transaction = root.transaction();
        transaction.upsert_row(b"row/2".to_vec(), b"{\"id\":2}".to_vec());
        let successor = transaction.commit().unwrap();
        assert_eq!(successor.rows.value_id(b"row/1"), Some(before));
    }

    #[test]
    fn eviction_and_reopen_preserve_exact_root_and_identity() {
        let (_store, root) = fixture();
        let mut transaction = root.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: 6,
            delete_len: 2,
            insert: b"XY".to_vec(),
        });
        let successor = transaction.commit().unwrap();
        let expected_id = successor.id();
        let expected_row = successor.rows.value_id(b"row/1");
        let archive = successor.archive().unwrap();
        drop(successor);
        drop(root);

        let (_reopened_store, reopened) = archive.reopen().unwrap();
        assert_eq!(reopened.id(), expected_id);
        assert_eq!(reopened.rows.value_id(b"row/1"), expected_row);
        assert_eq!(
            reopened.bytes.read(0, reopened.bytes.len()).unwrap(),
            b"abcdefXYijkl"
        );
    }

    #[test]
    fn merge_is_direction_independent_and_preserves_disjoint_changes() {
        let (_store, base) = fixture();
        let mut a = base.transaction();
        a.upsert_row(b"row/a".to_vec(), b"A".to_vec());
        a.upsert_row(b"row/conflict".to_vec(), b"left".to_vec());
        let a = a.commit().unwrap();
        let mut b = base.transaction();
        b.upsert_row(b"row/b".to_vec(), b"B".to_vec());
        b.upsert_row(b"row/conflict".to_vec(), b"right".to_vec());
        let b = b.commit().unwrap();

        let ab = Root::merge_rows(&base, &a, &b).unwrap();
        let ba = Root::merge_rows(&base, &b, &a).unwrap();
        assert_eq!(ab.rows.id(), ba.rows.id());
        assert_eq!(ab.rows.get(b"row/a").unwrap().unwrap(), b"A");
        assert_eq!(ab.rows.get(b"row/b").unwrap().unwrap(), b"B");
    }

    #[test]
    fn format_layout_requires_explicit_schema_and_state_granularity() {
        assert!(
            (FormatLayout {
                plugin_key: "plugin",
                schema_keys: &["schema"],
                state_pages: &[StatePageLayout {
                    kind: "span-index",
                    target_items: 512,
                }],
            })
            .is_valid()
        );
    }

    #[test]
    fn hard_performance_gate_requires_both_two_x_speed_and_three_x_memory() {
        let v2 = PerformanceMeasurement {
            p95_nanoseconds: 6_000_000,
            peak_total_bytes: 30_000_000,
        };
        assert!(
            compare_to_baseline(
                v2,
                PerformanceMeasurement {
                    p95_nanoseconds: 3_000_000,
                    peak_total_bytes: 10_000_000,
                }
            )
            .passes()
        );
        assert_eq!(
            compare_to_baseline(
                v2,
                PerformanceMeasurement {
                    p95_nanoseconds: 3_000_001,
                    peak_total_bytes: 9_999_999,
                }
            ),
            Acceptance {
                latency_passes: false,
                memory_passes: true,
            }
        );
        assert_eq!(
            compare_to_baseline(
                v2,
                PerformanceMeasurement {
                    p95_nanoseconds: 2_999_999,
                    peak_total_bytes: 10_000_001,
                }
            ),
            Acceptance {
                latency_passes: true,
                memory_passes: false,
            }
        );
    }
}
