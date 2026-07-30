//! Host-owned immutable arenas for the Lix plugin API v3 prototype.
//!
//! A [`Root`] names three independently persistent values:
//!
//! - exact accepted file bytes as a rope of immutable page slices;
//! - durable semantic entities as a copy-on-write keyed map;
//! - opaque plugin state as a separate copy-on-write keyed map.
//!
//! Transactions stage all three successors and publish one content-addressed
//! root only after every edit validates. Cloning a root is a constant-time
//! branch. Unchanged pages and map entries are shared across successors.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use rayon::prelude::*;

pub const DEFAULT_PAGE_BYTES: usize = 64 * 1024;
pub const REQUIRED_V3_SPEEDUP: u64 = 2;
pub const REQUIRED_V3_MEMORY_REDUCTION: u64 = 3;

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

/// Applies the non-negotiable API v3 performance gate without floating-point
/// rounding: v3 must be at least 2× faster and consume at least 3× less total
/// peak memory than the matching v2 lane.
pub const fn compare_to_v2(v2: PerformanceMeasurement, v3: PerformanceMeasurement) -> Acceptance {
    Acceptance {
        latency_passes: match v3.p95_nanoseconds.checked_mul(REQUIRED_V3_SPEEDUP) {
            Some(scaled) => scaled <= v2.p95_nanoseconds,
            None => false,
        },
        memory_passes: match v3
            .peak_total_bytes
            .checked_mul(REQUIRED_V3_MEMORY_REDUCTION)
        {
            Some(scaled) => scaled <= v2.peak_total_bytes,
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
    pages: Mutex<HashMap<Digest, StoredPage>>,
    metrics: Mutex<Metrics>,
}

#[derive(Debug)]
struct StoredPage {
    compressed: Arc<[u8]>,
    resident: Option<Arc<[u8]>>,
    uncompressed_len: usize,
}

#[cfg(not(target_family = "wasm"))]
fn compress_page(bytes: &[u8]) -> Vec<u8> {
    thread_local! {
        static COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> =
            std::cell::RefCell::new(
                zstd::bulk::Compressor::new(1)
                    .expect("creating an in-memory arena compressor cannot fail"),
            );
    }
    COMPRESSOR.with(|compressor| {
        compressor
            .borrow_mut()
            .compress(bytes)
            .expect("compressing an in-memory arena page cannot fail")
    })
}

#[cfg(target_family = "wasm")]
fn compress_page(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

#[cfg(not(target_family = "wasm"))]
fn decompress_page(bytes: &[u8], expected_len: usize) -> Result<Vec<u8>, Error> {
    zstd::bulk::decompress(bytes, expected_len).map_err(|_| Error::CorruptArchive)
}

#[cfg(target_family = "wasm")]
fn decompress_page(bytes: &[u8], expected_len: usize) -> Result<Vec<u8>, Error> {
    (bytes.len() == expected_len)
        .then(|| bytes.to_vec())
        .ok_or(Error::CorruptArchive)
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
        self.inner.pages.lock().len()
    }

    pub fn unique_page_bytes(&self) -> usize {
        self.inner
            .pages
            .lock()
            .values()
            .map(|page| page.uncompressed_len)
            .sum()
    }

    /// Actual bytes retained by this in-process store after decoded page
    /// eviction. Durable deployments can replace the compressed backing with
    /// SQLite/blob storage without changing root identities.
    pub fn resident_page_bytes(&self) -> usize {
        self.inner
            .pages
            .lock()
            .values()
            .map(|page| {
                page.compressed.len() + page.resident.as_ref().map_or(0, |resident| resident.len())
            })
            .sum()
    }

    pub fn evict_resident_pages(&self) {
        for page in self.inner.pages.lock().values_mut() {
            page.resident = None;
        }
    }

    /// Removes content pages unreachable from the retained immutable root.
    /// Production storage performs the same operation through durable
    /// reachability/GC rather than retaining superseded transition pages.
    pub fn retain_reachable(&self, root: &Root) {
        let mut reachable = std::collections::HashSet::new();
        collect_page_ids(&root.bytes, &mut reachable);
        for arena in [&root.entities, &root.state] {
            for page in arena.pages.iter() {
                reachable.insert(page.manifest);
                reachable.extend(page.referenced_pages.iter().copied());
            }
        }
        self.inner
            .pages
            .lock()
            .retain(|id, _| reachable.contains(id));
    }

    fn intern(&self, bytes: &[u8]) -> Digest {
        self.intern_with_residency(bytes, true)
    }

    fn intern_evicted(&self, bytes: &[u8]) -> Digest {
        self.intern_with_residency(bytes, false)
    }

    fn intern_with_residency(&self, bytes: &[u8], resident: bool) -> Digest {
        let id = digest(b"lix-plugin-v3/page\0", [bytes]);
        // Compression is pure and comparatively expensive. Never serialize
        // independent page compression behind the store metadata mutex.
        let compressed = compress_page(bytes);
        let mut pages = self.inner.pages.lock();
        if let std::collections::hash_map::Entry::Vacant(entry) = pages.entry(id) {
            entry.insert(StoredPage {
                compressed: Arc::from(compressed),
                resident: resident.then(|| Arc::from(bytes)),
                uncompressed_len: bytes.len(),
            });
            let mut metrics = self.inner.metrics.lock();
            metrics.pages_interned = metrics.pages_interned.saturating_add(1);
            metrics.page_bytes_interned = metrics
                .page_bytes_interned
                .saturating_add(bytes.len() as u64);
        }
        id
    }

    fn insert_precompressed_page(&self, id: Digest, compressed: Vec<u8>, uncompressed_len: usize) {
        let mut pages = self.inner.pages.lock();
        if let std::collections::hash_map::Entry::Vacant(entry) = pages.entry(id) {
            entry.insert(StoredPage {
                compressed: Arc::from(compressed),
                resident: None,
                uncompressed_len,
            });
            let mut metrics = self.inner.metrics.lock();
            metrics.pages_interned = metrics.pages_interned.saturating_add(1);
            metrics.page_bytes_interned = metrics
                .page_bytes_interned
                .saturating_add(uncompressed_len as u64);
        }
    }

    fn read_page(&self, id: Digest, range: Range<usize>) -> Result<Vec<u8>, Error> {
        let output = self.read_page_untracked(id, range)?;
        let mut metrics = self.inner.metrics.lock();
        metrics.page_reads = metrics.page_reads.saturating_add(1);
        metrics.page_bytes_read = metrics.page_bytes_read.saturating_add(output.len() as u64);
        Ok(output)
    }

    fn read_page_untracked(&self, id: Digest, range: Range<usize>) -> Result<Vec<u8>, Error> {
        let mut pages = self.inner.pages.lock();
        let page = pages.get_mut(&id).ok_or(Error::MissingPage(id))?;
        if page.resident.is_none() {
            let bytes = decompress_page(&page.compressed, page.uncompressed_len)?;
            page.resident = Some(Arc::from(bytes));
        }
        let selected = page
            .resident
            .as_ref()
            .expect("resident page was populated")
            .get(range)
            .ok_or(Error::InvalidPageRange)?;
        let output = selected.to_vec();
        Ok(output)
    }

    fn insert_archived_page(&self, expected: Digest, bytes: &[u8]) -> Result<(), Error> {
        let actual = digest(b"lix-plugin-v3/page\0", [bytes]);
        if actual != expected {
            return Err(Error::CorruptArchive);
        }
        self.inner
            .pages
            .lock()
            .entry(expected)
            .or_insert_with(|| StoredPage {
                compressed: Arc::from(compress_page(bytes)),
                resident: Some(Arc::from(bytes)),
                uncompressed_len: bytes.len(),
            });
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Segment {
    page: Digest,
    offset: u32,
    len: u32,
}

#[derive(Debug, Clone)]
pub struct ByteArena {
    store: Store,
    len: u64,
    segments: Arc<[Segment]>,
    id: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteRecordLocator {
    pub offset: u64,
    pub length: u64,
    pub ordinal: u64,
    pub content: Vec<u8>,
}

impl ByteArena {
    pub fn empty(store: Store) -> Self {
        Self::from_segments(store, 0, Vec::new())
    }

    pub fn from_bytes(store: Store, bytes: &[u8]) -> Self {
        Self::from_bytes_with_residency(store, bytes, true)
    }

    fn from_value_bytes(store: Store, bytes: &[u8]) -> Self {
        let mut arena = Self::from_bytes(store, bytes);
        arena.id = stable_content_arena_id(bytes);
        arena
    }

    #[cfg(not(target_family = "wasm"))]
    fn from_bytes_evicted(store: Store, bytes: &[u8]) -> Self {
        Self::from_bytes_with_residency(store, bytes, false)
    }

    #[cfg(target_family = "wasm")]
    fn from_bytes_evicted(store: Store, bytes: &[u8]) -> Self {
        Self::from_bytes_with_residency(store, bytes, false)
    }

    fn from_bytes_with_residency(store: Store, bytes: &[u8], resident: bool) -> Self {
        let segments = bytes
            .chunks(store.page_bytes())
            .map(|chunk| Segment {
                page: if resident {
                    store.intern(chunk)
                } else {
                    store.intern_evicted(chunk)
                },
                offset: 0,
                len: u32::try_from(chunk.len()).expect("page size must fit u32"),
            })
            .collect();
        Self::from_segments(store, bytes.len() as u64, segments)
    }

    fn range_with_content_id(&self, range: Range<usize>, content: &[u8]) -> Result<Self, Error> {
        if range.end.saturating_sub(range.start) != content.len() {
            return Err(Error::InvalidPageRange);
        }
        let mut segments = Vec::new();
        self.append_segments(range.start as u64, range.end as u64, &mut segments)?;
        Ok(Self {
            store: self.store.clone(),
            len: content.len() as u64,
            segments: segments.into(),
            id: stable_content_arena_id(content),
        })
    }

    fn from_segments(store: Store, len: u64, segments: Vec<Segment>) -> Self {
        let id = byte_arena_digest(len, &segments);
        Self {
            store,
            len,
            segments: segments.into(),
            id,
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
                        segment.page,
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

    pub fn find_byte_forward(
        &self,
        offset: u64,
        length: u64,
        needle: u8,
    ) -> Result<Option<u64>, Error> {
        let end = checked_range_end(offset, length, self.len)?;
        let mut cursor = offset;
        while cursor < end {
            let chunk_len = (end - cursor).min(self.store.page_bytes() as u64);
            let chunk = self.read(cursor, chunk_len)?;
            if let Some(relative) = chunk.iter().position(|byte| *byte == needle) {
                return Ok(Some(cursor + relative as u64));
            }
            cursor += chunk_len;
        }
        Ok(None)
    }

    pub fn find_byte_backward(
        &self,
        offset: u64,
        length: u64,
        needle: u8,
    ) -> Result<Option<u64>, Error> {
        let mut cursor = checked_range_end(offset, length, self.len)?;
        while cursor > offset {
            let chunk_len = (cursor - offset).min(self.store.page_bytes() as u64);
            let chunk_start = cursor - chunk_len;
            let chunk = self.read(chunk_start, chunk_len)?;
            if let Some(relative) = chunk.iter().rposition(|byte| *byte == needle) {
                return Ok(Some(chunk_start + relative as u64));
            }
            cursor = chunk_start;
        }
        Ok(None)
    }

    pub fn count_byte(&self, offset: u64, length: u64, needle: u8) -> Result<u64, Error> {
        let end = checked_range_end(offset, length, self.len)?;
        let mut cursor = offset;
        let mut count = 0_u64;
        while cursor < end {
            let chunk_len = (end - cursor).min(self.store.page_bytes() as u64);
            let chunk = self.read(cursor, chunk_len)?;
            count = count
                .checked_add(chunk.iter().filter(|byte| **byte == needle).count() as u64)
                .ok_or(Error::RangeOverflow)?;
            cursor += chunk_len;
        }
        Ok(count)
    }

    pub fn locate_record(
        &self,
        range_offset: u64,
        range_length: u64,
        position: u64,
        delimiter: u8,
        forbidden: &[u8],
    ) -> Result<Option<ByteRecordLocator>, Error> {
        let range_end = checked_range_end(range_offset, range_length, self.len)?;
        if position < range_offset || position >= range_end {
            return Err(Error::RangeOutOfBounds);
        }
        let bytes = self.read(range_offset, range_length)?;
        if bytes.iter().any(|byte| forbidden.contains(byte)) {
            return Ok(None);
        }
        let relative_position =
            usize::try_from(position - range_offset).map_err(|_| Error::RangeOverflow)?;
        let relative_start = bytes[..relative_position]
            .iter()
            .rposition(|byte| *byte == delimiter)
            .map_or(0, |offset| offset + 1);
        let relative_end = bytes[relative_position..]
            .iter()
            .position(|byte| *byte == delimiter)
            .map_or(bytes.len(), |offset| relative_position + offset + 1);
        let ordinal = bytes[..relative_start]
            .iter()
            .filter(|byte| **byte == delimiter)
            .count() as u64;
        Ok(Some(ByteRecordLocator {
            offset: range_offset + relative_start as u64,
            length: (relative_end - relative_start) as u64,
            ordinal,
            content: bytes[relative_start..relative_end].to_vec(),
        }))
    }

    fn materialize_untracked(&self) -> Result<Vec<u8>, Error> {
        let mut output =
            Vec::with_capacity(usize::try_from(self.len).map_err(|_| Error::RangeOverflow)?);
        for segment in self.segments.iter() {
            output.extend_from_slice(&self.store.read_page_untracked(
                segment.page,
                usize::try_from(segment.offset).expect("u32 fits usize")
                    ..usize::try_from(segment.offset + segment.len).expect("u32 fits usize"),
            )?);
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
                    page: segment.page,
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

fn checked_range_end(offset: u64, length: u64, arena_len: u64) -> Result<u64, Error> {
    let end = offset.checked_add(length).ok_or(Error::RangeOverflow)?;
    if end > arena_len {
        return Err(Error::RangeOutOfBounds);
    }
    Ok(end)
}

fn byte_arena_digest(len: u64, segments: &[Segment]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix-plugin-v3/bytes\0");
    update_digest_part(&mut hasher, &len.to_le_bytes());
    for segment in segments {
        update_digest_part(&mut hasher, segment.page.as_bytes());
        update_digest_part(&mut hasher, &segment.offset.to_le_bytes());
        update_digest_part(&mut hasher, &segment.len.to_le_bytes());
    }
    Digest(*hasher.finalize().as_bytes())
}

fn stable_content_arena_id(bytes: &[u8]) -> Digest {
    digest(b"lix-plugin-v3/value\0", [bytes])
}

fn update_digest_part(hasher: &mut blake3::Hasher, part: &[u8]) {
    hasher.update(&(part.len() as u64).to_le_bytes());
    hasher.update(part);
}

fn coalesce_segments(segments: &mut Vec<Segment>) {
    let mut output: Vec<Segment> = Vec::with_capacity(segments.len());
    for segment in segments.drain(..) {
        if let Some(last) = output.last_mut()
            && last.page == segment.page
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
    pages: Arc<Vec<MapPage>>,
    len: usize,
    id: Digest,
}

// Content-defined boundaries keep semantic ranges stable when an entity is
// inserted or deleted. Fixed record-count pages shift every later boundary
// after an early insertion, defeating page-fingerprint reconciliation.
const MAP_PAGE_MIN_ENTRIES: usize = 192;
const MAP_PAGE_MAX_ENTRIES: usize = 320;
const MAP_PAGE_BOUNDARY_MASK: u64 = 0x3f;

fn key_ends_map_page(key: &[u8], entries: usize) -> bool {
    if entries < MAP_PAGE_MIN_ENTRIES {
        return false;
    }
    // Stable FNV-1a is sufficient for chunk boundaries. The hard maximum
    // bounds adversarial keys; cryptographic identity remains the page digest.
    let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    hash & MAP_PAGE_BOUNDARY_MASK == 0
}

fn ends_map_page(key: &[u8], entries: usize) -> bool {
    entries >= MAP_PAGE_MAX_ENTRIES || key_ends_map_page(key, entries)
}

fn map_entries_end_at_boundary<'a>(keys: impl Iterator<Item = &'a Vec<u8>>) -> bool {
    let mut page_entries = 0usize;
    for key in keys {
        page_entries += 1;
        if ends_map_page(key, page_entries) {
            page_entries = 0;
        }
    }
    page_entries == 0
}

#[derive(Debug, Clone)]
struct MapPage {
    first_key: Arc<[u8]>,
    last_key: Arc<[u8]>,
    record_count: u32,
    manifest: Digest,
    referenced_pages: Arc<[Digest]>,
    id: Digest,
}

impl MapPage {
    fn new(store: &Store, entries: Vec<(Vec<u8>, ByteArena)>) -> Self {
        Self::new_with_residency(store, entries, true)
    }

    fn new_evicted(store: &Store, entries: Vec<(Vec<u8>, ByteArena)>) -> Self {
        Self::new_with_residency(store, entries, false)
    }

    fn new_with_residency(
        store: &Store,
        entries: Vec<(Vec<u8>, ByteArena)>,
        resident: bool,
    ) -> Self {
        assert!(!entries.is_empty(), "map pages are never empty");
        let id = digest(
            b"lix-plugin-v3/map-page\0",
            entries
                .iter()
                .flat_map(|(key, value)| [key.as_slice(), value.id.as_bytes().as_slice()]),
        );
        let manifest_bytes = encode_map_page_manifest(&entries);
        let manifest = if resident {
            store.intern(&manifest_bytes)
        } else {
            store.intern_evicted(&manifest_bytes)
        };
        let mut referenced_pages = entries
            .iter()
            .flat_map(|(_, value)| value.segments.iter().map(|segment| segment.page))
            .collect::<Vec<_>>();
        referenced_pages.sort_unstable();
        referenced_pages.dedup();
        Self {
            first_key: Arc::from(entries.first().expect("page is nonempty").0.as_slice()),
            last_key: Arc::from(entries.last().expect("page is nonempty").0.as_slice()),
            record_count: u32::try_from(entries.len()).expect("map page record count fits u32"),
            manifest,
            referenced_pages: referenced_pages.into(),
            id,
        }
    }

    fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    fn entries(&self, store: &Store) -> Result<Vec<(Vec<u8>, ByteArena)>, Error> {
        let bytes = store.read_page_untracked(self.manifest, 0..self.manifest_len(store)?)?;
        decode_map_page_manifest(store, &bytes, self.record_count)
    }

    fn manifest_len(&self, store: &Store) -> Result<usize, Error> {
        store
            .inner
            .pages
            .lock()
            .get(&self.manifest)
            .map(|page| page.uncompressed_len)
            .ok_or(Error::MissingPage(self.manifest))
    }
}

fn encode_map_page_manifest(entries: &[(Vec<u8>, ByteArena)]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(
        &u32::try_from(entries.len())
            .expect("map page record count fits u32")
            .to_le_bytes(),
    );
    for (key, value) in entries {
        output.extend_from_slice(
            &u32::try_from(key.len())
                .expect("map key length fits u32")
                .to_le_bytes(),
        );
        output.extend_from_slice(key);
        output.extend_from_slice(value.id.as_bytes());
        output.extend_from_slice(&value.len.to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(value.segments.len())
                .expect("arena segment count fits u32")
                .to_le_bytes(),
        );
        for segment in value.segments.iter() {
            output.extend_from_slice(segment.page.as_bytes());
            output.extend_from_slice(&segment.offset.to_le_bytes());
            output.extend_from_slice(&segment.len.to_le_bytes());
        }
    }
    output
}

fn decode_map_page_manifest(
    store: &Store,
    bytes: &[u8],
    expected_records: u32,
) -> Result<Vec<(Vec<u8>, ByteArena)>, Error> {
    let mut cursor = 0usize;
    let records = take_manifest_u32(bytes, &mut cursor)?;
    if records != expected_records {
        return Err(Error::CorruptArchive);
    }
    let mut entries = Vec::with_capacity(records as usize);
    for _ in 0..records {
        let key_len = take_manifest_u32(bytes, &mut cursor)? as usize;
        let key = take_manifest_bytes(bytes, &mut cursor, key_len)?.to_vec();
        let id = Digest(
            take_manifest_bytes(bytes, &mut cursor, 32)?
                .try_into()
                .expect("digest slice is exact"),
        );
        let len = u64::from_le_bytes(
            take_manifest_bytes(bytes, &mut cursor, 8)?
                .try_into()
                .expect("length slice is exact"),
        );
        let segment_count = take_manifest_u32(bytes, &mut cursor)? as usize;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let page = Digest(
                take_manifest_bytes(bytes, &mut cursor, 32)?
                    .try_into()
                    .expect("digest slice is exact"),
            );
            let offset = take_manifest_u32(bytes, &mut cursor)?;
            let segment_len = take_manifest_u32(bytes, &mut cursor)?;
            segments.push(Segment {
                page,
                offset,
                len: segment_len,
            });
        }
        entries.push((
            key,
            ByteArena {
                store: store.clone(),
                len,
                segments: segments.into(),
                id,
            },
        ));
    }
    if cursor != bytes.len() {
        return Err(Error::CorruptArchive);
    }
    Ok(entries)
}

fn take_manifest_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        take_manifest_bytes(bytes, cursor, 4)?
            .try_into()
            .expect("u32 slice is exact"),
    ))
}

fn take_manifest_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], Error> {
    let end = cursor.checked_add(len).ok_or(Error::CorruptArchive)?;
    let value = bytes.get(*cursor..end).ok_or(Error::CorruptArchive)?;
    *cursor = end;
    Ok(value)
}

impl MapArena {
    pub fn empty(store: Store) -> Self {
        Self::from_entries(store, BTreeMap::new())
    }

    fn from_entries(store: Store, entries: BTreeMap<Vec<u8>, ByteArena>) -> Self {
        let entries = pack_map_values(&store, entries);
        Self::from_existing_entries(store, entries)
    }

    fn from_existing_entries(store: Store, entries: BTreeMap<Vec<u8>, ByteArena>) -> Self {
        let len = entries.len();
        let pages = map_pages_from_entries(&store, entries);
        Self::from_pages(store, pages, len)
    }

    fn from_raw_entries(store: Store, entries: BTreeMap<Vec<u8>, Vec<u8>>) -> Self {
        let entries = pack_raw_map_values(&store, entries);
        let len = entries.len();
        let pages = map_pages_from_entries(&store, entries);
        Self::from_pages(store, pages, len)
    }

    fn from_raw_sequence(store: Store, mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        if !entries.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        }
        let len = entries.len();
        let entries = pack_raw_sequence_values(&store, entries);
        let pages = map_pages_from_sequence(&store, entries);
        Self::from_pages(store, pages, len)
    }

    fn from_pages(store: Store, pages: Vec<MapPage>, len: usize) -> Self {
        let id = digest(
            b"lix-plugin-v3/map\0",
            pages.iter().map(|page| page.id.as_bytes()),
        );
        Self {
            store,
            pages: Arc::new(pages),
            len,
            id,
        }
    }

    pub fn id(&self) -> Digest {
        self.id
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.value(key)?
            .map(|value| value.read(0, value.len()))
            .transpose()
    }

    pub fn value_id(&self, key: &[u8]) -> Option<Digest> {
        self.value(key).ok().flatten().map(|value| value.id())
    }

    pub fn scan(&self, after_key: Option<&[u8]>, max_bytes: usize) -> Result<KeyedPage, Error> {
        if max_bytes == 0 {
            return Err(Error::LimitExceeded);
        }
        let first_page = after_key.map_or(0, |key| {
            self.pages.partition_point(|page| page.last_key() <= key)
        });
        let mut output = Vec::<(Vec<u8>, Vec<u8>)>::new();
        let mut output_bytes = 0usize;
        for page in &self.pages[first_page..] {
            for (key, value) in page.entries(&self.store)? {
                if after_key.is_some_and(|after| key.as_slice() <= after) {
                    continue;
                }
                let value_len = usize::try_from(value.len).map_err(|_| Error::RangeOverflow)?;
                let entry_bytes = key
                    .len()
                    .checked_add(value_len)
                    .ok_or(Error::RangeOverflow)?;
                if entry_bytes > max_bytes && output.is_empty() {
                    return Err(Error::LimitExceeded);
                }
                if output_bytes
                    .checked_add(entry_bytes)
                    .is_none_or(|total| total > max_bytes)
                {
                    return Ok(KeyedPage {
                        next_key: output.last().map(|(key, _)| key.clone()),
                        entries: output,
                    });
                }
                output_bytes += entry_bytes;
                output.push((key, value.read(0, value.len)?));
            }
        }
        Ok(KeyedPage {
            next_key: None,
            entries: output,
        })
    }

    pub fn semantic_pages(
        &self,
        after_key: Option<&[u8]>,
        max_pages: u32,
    ) -> Result<SemanticPageBatch, Error> {
        self.semantic_pages_bounded(after_key, max_pages, usize::MAX)
    }

    pub fn semantic_pages_bounded(
        &self,
        after_key: Option<&[u8]>,
        max_pages: u32,
        max_bytes: usize,
    ) -> Result<SemanticPageBatch, Error> {
        if max_pages == 0 {
            return Err(Error::LimitExceeded);
        }
        let first = after_key.map_or(0, |key| {
            self.pages.partition_point(|page| page.last_key() <= key)
        });
        let mut selected = Vec::new();
        let mut selected_bytes = 0usize;
        for page in self.pages[first..]
            .iter()
            .take(usize::try_from(max_pages).unwrap_or(usize::MAX))
        {
            let page_bytes = page
                .first_key
                .len()
                .checked_add(page.last_key.len())
                .and_then(|bytes| bytes.checked_add(36))
                .ok_or(Error::RangeOverflow)?;
            if page_bytes > max_bytes && selected.is_empty() {
                return Err(Error::LimitExceeded);
            }
            if selected_bytes
                .checked_add(page_bytes)
                .is_none_or(|bytes| bytes > max_bytes)
            {
                break;
            }
            selected_bytes += page_bytes;
            selected.push(SemanticPage {
                first_key: page.first_key.to_vec(),
                last_key: page.last_key.to_vec(),
                fingerprint: page.id.0,
                record_count: page.record_count,
            });
        }
        let consumed = first.saturating_add(selected.len());
        Ok(SemanticPageBatch {
            next_key: (consumed < self.pages.len()).then(|| {
                selected
                    .last()
                    .expect("more pages implies one selected")
                    .last_key
                    .clone()
            }),
            pages: selected,
        })
    }

    fn apply(&self, changes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Self {
        if changes.is_empty() {
            return self.clone();
        }
        if self.is_empty() && changes.values().all(Option::is_some) {
            // Cold import previously interned and compressed every small value
            // individually, only to materialize and repack it into 64 KiB
            // pages immediately afterward. Build the packed immutable pages
            // directly while retaining the exact per-value arena identity.
            return Self::from_raw_entries(
                self.store.clone(),
                changes
                    .iter()
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            value
                                .as_ref()
                                .expect("cold import values were verified")
                                .clone(),
                        )
                    })
                    .collect(),
            );
        }
        if changes.iter().all(|(key, value)| {
            value.is_some() && self.value(key).is_ok_and(|value| value.is_some())
        }) {
            let mut pages = self.pages.as_ref().clone();
            for (key, value) in changes {
                let page_index = pages.partition_point(|page| page.last_key() < key.as_slice());
                let page = &mut pages[page_index];
                let mut entries = page
                    .entries(&self.store)
                    .expect("live map page manifest is readable");
                let entry_index = entries
                    .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key.as_slice()))
                    .expect("the sparse replacement key was verified above");
                entries[entry_index].1 = ByteArena::from_value_bytes(
                    self.store.clone(),
                    value.as_ref().expect("replacement was verified above"),
                );
                *page = MapPage::new(&self.store, entries);
            }
            return Self::from_pages(self.store.clone(), pages, self.len);
        }

        let first_key = changes
            .first_key_value()
            .expect("nonempty changes were checked above")
            .0;
        let last_key = changes
            .last_key_value()
            .expect("nonempty changes were checked above")
            .0;
        let mut first_page = self
            .pages
            .partition_point(|page| page.last_key() < first_key.as_slice());
        // An insertion after the final partial page must rebuild that page as
        // part of the affected range. Otherwise the same logical map can gain
        // a non-canonical extra page, changing its durable identity and making
        // archive reopen fail.
        if first_page > 0 {
            let previous = &self.pages[first_page - 1];
            if !ends_map_page(previous.last_key(), previous.record_count as usize) {
                first_page -= 1;
            }
        }
        let last_page = self
            .pages
            .partition_point(|page| page.last_key() < last_key.as_slice());
        let mut end_page = last_page.saturating_add(1).min(self.pages.len());
        let mut entries = self.pages[first_page..end_page]
            .iter()
            .flat_map(|page| {
                page.entries(&self.store)
                    .expect("live map page manifest is readable")
            })
            .collect::<BTreeMap<_, _>>();
        for (key, value) in changes {
            if let Some(value) = value {
                entries.insert(
                    key.clone(),
                    ByteArena::from_value_bytes(self.store.clone(), value),
                );
            } else {
                entries.remove(key.as_slice());
            }
        }
        // Continue through old pages until the rebuilt sequence reaches a
        // canonical cut. This handles both position-dependent hard cuts and a
        // large deletion that makes an old key-derived page shorter than the
        // minimum. The untouched suffix can then be shared byte-for-byte.
        while end_page < self.pages.len() && !map_entries_end_at_boundary(entries.keys()) {
            for (key, value) in self.pages[end_page]
                .entries(&self.store)
                .expect("live map page manifest is readable")
            {
                entries.insert(key, value);
            }
            end_page += 1;
        }
        let mut pages = Vec::with_capacity(
            first_page
                .saturating_add(entries.len().div_ceil(MAP_PAGE_MIN_ENTRIES))
                .saturating_add(self.pages.len().saturating_sub(end_page)),
        );
        pages.extend_from_slice(&self.pages[..first_page]);
        pages.extend(map_pages_from_entries(&self.store, entries));
        pages.extend_from_slice(&self.pages[end_page..]);
        let len = pages.iter().map(|page| page.record_count as usize).sum();
        Self::from_pages(self.store.clone(), pages, len)
    }

    fn apply_owned(&self, changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Self {
        if self.is_empty() && changes.values().all(Option::is_some) {
            return Self::from_raw_entries(
                self.store.clone(),
                changes
                    .into_iter()
                    .map(|(key, value)| (key, value.expect("cold import values were verified")))
                    .collect(),
            );
        }
        self.apply(&changes)
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.entries().into_iter().map(|(key, _)| key).collect()
    }

    fn entries(&self) -> Vec<(Vec<u8>, ByteArena)> {
        self.pages
            .iter()
            .flat_map(|page| {
                page.entries(&self.store)
                    .expect("live map page manifest is readable")
            })
            .collect()
    }

    fn value(&self, key: &[u8]) -> Result<Option<ByteArena>, Error> {
        let Some(page) = self
            .pages
            .get(self.pages.partition_point(|page| page.last_key() < key))
        else {
            return Ok(None);
        };
        let entries = page.entries(&self.store)?;
        Ok(entries
            .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            .ok()
            .map(|index| entries[index].1.clone()))
    }

    fn archived(&self) -> ArchivedMap {
        ArchivedMap {
            id: self.id,
            entries: self
                .entries()
                .into_iter()
                .map(|(key, value)| (key.clone(), value.archived()))
                .collect(),
        }
    }

    fn reopen(store: Store, archived: &ArchivedMap) -> Result<Self, Error> {
        let arena = Self::from_existing_entries(
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

fn map_pages_from_entries(store: &Store, entries: BTreeMap<Vec<u8>, ByteArena>) -> Vec<MapPage> {
    let mut pages = Vec::new();
    let mut page = Vec::new();
    for entry in entries {
        page.push(entry);
        if ends_map_page(&page.last().expect("entry was just pushed").0, page.len()) {
            pages.push(MapPage::new(store, std::mem::take(&mut page)));
        }
    }
    if !page.is_empty() {
        pages.push(MapPage::new(store, page));
    }
    pages
}

#[cfg(not(target_family = "wasm"))]
fn map_pages_from_sequence(store: &Store, entries: Vec<(Vec<u8>, ByteArena)>) -> Vec<MapPage> {
    let mut pages = Vec::new();
    let mut page = Vec::new();
    for entry in entries {
        page.push(entry);
        if ends_map_page(&page.last().expect("entry was just pushed").0, page.len()) {
            pages.push(MapPage::new_evicted(store, std::mem::take(&mut page)));
        }
    }
    if !page.is_empty() {
        pages.push(MapPage::new_evicted(store, page));
    }
    pages
}

#[cfg(target_family = "wasm")]
fn map_pages_from_sequence(store: &Store, entries: Vec<(Vec<u8>, ByteArena)>) -> Vec<MapPage> {
    let mut pages = Vec::new();
    let mut page = Vec::new();
    for entry in entries {
        page.push(entry);
        if ends_map_page(&page.last().expect("entry was just pushed").0, page.len()) {
            pages.push(MapPage::new_evicted(store, std::mem::take(&mut page)));
        }
    }
    if !page.is_empty() {
        pages.push(MapPage::new_evicted(store, page));
    }
    pages
}

fn raw_sequence_value(
    store: &Store,
    key: Vec<u8>,
    bytes: Vec<u8>,
) -> (Vec<u8>, Vec<u8>, Option<Digest>) {
    let id = (bytes.len() <= store.page_bytes()).then(|| stable_content_arena_id(&bytes));
    (key, bytes, id)
}

#[cfg(not(target_family = "wasm"))]
fn prepare_raw_sequence_values(
    store: &Store,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>, Option<Digest>)> {
    entries
        .into_par_iter()
        .map(|(key, bytes)| raw_sequence_value(store, key, bytes))
        .collect()
}

#[cfg(target_family = "wasm")]
fn prepare_raw_sequence_values(
    store: &Store,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>, Option<Digest>)> {
    entries
        .into_iter()
        .map(|(key, bytes)| raw_sequence_value(store, key, bytes))
        .collect()
}

struct PreparedPackedValuePage {
    id: Digest,
    compressed: Vec<u8>,
    uncompressed_len: usize,
    entries: Vec<(Vec<u8>, Vec<u8>, Digest)>,
}

fn prepare_packed_value_page(entries: Vec<(Vec<u8>, Vec<u8>, Digest)>) -> PreparedPackedValuePage {
    let uncompressed_len = entries.iter().map(|(_, bytes, _)| bytes.len()).sum();
    let mut packed = Vec::with_capacity(uncompressed_len);
    for (_, bytes, _) in &entries {
        packed.extend_from_slice(bytes);
    }
    PreparedPackedValuePage {
        id: digest(b"lix-plugin-v3/page\0", [&packed]),
        compressed: compress_page(&packed),
        uncompressed_len,
        entries,
    }
}

#[cfg(not(target_family = "wasm"))]
fn prepare_packed_value_pages(
    pages: Vec<Vec<(Vec<u8>, Vec<u8>, Digest)>>,
) -> Vec<PreparedPackedValuePage> {
    pages
        .into_par_iter()
        .map(prepare_packed_value_page)
        .collect()
}

#[cfg(target_family = "wasm")]
fn prepare_packed_value_pages(
    pages: Vec<Vec<(Vec<u8>, Vec<u8>, Digest)>>,
) -> Vec<PreparedPackedValuePage> {
    pages.into_iter().map(prepare_packed_value_page).collect()
}

#[derive(Debug)]
struct OrderedMapBuilder {
    store: Store,
    value_pending: Vec<(Vec<u8>, Vec<u8>, Digest)>,
    value_pending_bytes: usize,
    semantic_pending: Vec<(Vec<u8>, ByteArena)>,
    #[cfg(not(target_family = "wasm"))]
    packet_worker: Option<OrderedPacketWorker>,
    pages: Vec<MapPage>,
    last_key: Option<Vec<u8>>,
    len: usize,
}

impl OrderedMapBuilder {
    fn new(store: Store) -> Self {
        Self {
            store,
            value_pending: Vec::new(),
            value_pending_bytes: 0,
            semantic_pending: Vec::new(),
            #[cfg(not(target_family = "wasm"))]
            packet_worker: None,
            pages: Vec::new(),
            last_key: None,
            len: 0,
        }
    }

    fn push(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        let mut complete_value_pages = Vec::new();
        for (key, bytes, id) in prepare_raw_sequence_values(&self.store, entries) {
            if self
                .last_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
            {
                return Err(Error::InvalidOrderedOutput);
            }
            self.last_key = Some(key.clone());
            if bytes.len() > self.store.page_bytes() {
                if let Some(page) = self.take_value_page() {
                    complete_value_pages.push(page);
                }
                self.flush_prepared_value_pages(std::mem::take(&mut complete_value_pages));
                self.push_semantic_entry((
                    key,
                    ByteArena::from_value_bytes(self.store.clone(), &bytes),
                ));
                continue;
            }
            if self.value_pending_bytes + bytes.len() > self.store.page_bytes() {
                if let Some(page) = self.take_value_page() {
                    complete_value_pages.push(page);
                }
            }
            self.value_pending_bytes += bytes.len();
            self.value_pending.push((
                key,
                bytes,
                id.expect("small raw values have a precomputed identity"),
            ));
        }
        self.flush_prepared_value_pages(complete_value_pages);
        self.flush_complete_semantic_pages();
        Ok(())
    }

    fn push_packet(
        &mut self,
        packet: Vec<u8>,
        entries: Vec<(Vec<u8>, Range<usize>)>,
    ) -> Result<(), Error> {
        if !self.value_pending.is_empty() {
            return Err(Error::InvalidOrderedOutput);
        }
        for (key, _) in &entries {
            if self
                .last_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
            {
                return Err(Error::InvalidOrderedOutput);
            }
            self.last_key = Some(key.clone());
        }
        self.push_packet_prepared(packet, entries)
    }

    #[cfg(not(target_family = "wasm"))]
    fn push_packet_prepared(
        &mut self,
        packet: Vec<u8>,
        entries: Vec<(Vec<u8>, Range<usize>)>,
    ) -> Result<(), Error> {
        self.flush_pending_packet()?;
        let worker = self
            .packet_worker
            .get_or_insert_with(|| OrderedPacketWorker::new(self.store.clone()));
        worker
            .jobs
            .send((packet, entries))
            .map_err(|_| Error::InvalidOrderedOutput)?;
        worker.pending = true;
        Ok(())
    }

    #[cfg(target_family = "wasm")]
    fn push_packet_prepared(
        &mut self,
        packet: Vec<u8>,
        entries: Vec<(Vec<u8>, Range<usize>)>,
    ) -> Result<(), Error> {
        for entry in prepare_ordered_packet(self.store.clone(), packet, entries)? {
            self.push_semantic_entry(entry);
        }
        self.flush_complete_semantic_pages();
        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    fn flush_pending_packet(&mut self) -> Result<(), Error> {
        let Some(worker) = self.packet_worker.as_mut() else {
            return Ok(());
        };
        if !worker.pending {
            return Ok(());
        }
        let entries = worker
            .results
            .recv()
            .map_err(|_| Error::InvalidOrderedOutput)??;
        worker.pending = false;
        for entry in entries {
            self.push_semantic_entry(entry);
        }
        self.flush_complete_semantic_pages();
        Ok(())
    }

    fn take_value_page(&mut self) -> Option<Vec<(Vec<u8>, Vec<u8>, Digest)>> {
        if self.value_pending.is_empty() {
            return None;
        }
        let pending = std::mem::take(&mut self.value_pending);
        self.value_pending_bytes = 0;
        Some(pending)
    }

    fn flush_prepared_value_pages(&mut self, pages: Vec<Vec<(Vec<u8>, Vec<u8>, Digest)>>) {
        for prepared in prepare_packed_value_pages(pages) {
            self.store.insert_precompressed_page(
                prepared.id,
                prepared.compressed,
                prepared.uncompressed_len,
            );
            let mut offset = 0u32;
            for (key, bytes, id) in prepared.entries {
                let len = u32::try_from(bytes.len()).expect("packed map value page fits u32");
                self.push_semantic_entry((
                    key,
                    ByteArena {
                        store: self.store.clone(),
                        len: u64::from(len),
                        segments: Arc::from([Segment {
                            page: prepared.id,
                            offset,
                            len,
                        }]),
                        id,
                    },
                ));
                offset += len;
            }
        }
    }

    fn push_semantic_entry(&mut self, entry: (Vec<u8>, ByteArena)) {
        self.semantic_pending.push(entry);
        self.len += 1;
    }

    fn flush_complete_semantic_pages(&mut self) {
        let mut page_entries = 0usize;
        let mut complete_len = 0usize;
        for (index, (key, _)) in self.semantic_pending.iter().enumerate() {
            page_entries += 1;
            if ends_map_page(key, page_entries) {
                complete_len = index + 1;
                page_entries = 0;
            }
        }
        if complete_len == 0 {
            return;
        }
        let remainder = self.semantic_pending.split_off(complete_len);
        let complete = std::mem::replace(&mut self.semantic_pending, remainder);
        self.pages
            .extend(map_pages_from_sequence(&self.store, complete));
    }

    fn finish(mut self) -> Result<MapArena, Error> {
        #[cfg(not(target_family = "wasm"))]
        self.flush_pending_packet()?;
        if let Some(page) = self.take_value_page() {
            self.flush_prepared_value_pages(vec![page]);
        }
        self.flush_complete_semantic_pages();
        if !self.semantic_pending.is_empty() {
            let entries = std::mem::take(&mut self.semantic_pending);
            self.pages.push(MapPage::new_evicted(&self.store, entries));
        }
        Ok(MapArena::from_pages(self.store, self.pages, self.len))
    }
}

#[cfg(not(target_family = "wasm"))]
struct OrderedPacketWorker {
    jobs: std::sync::mpsc::SyncSender<(Vec<u8>, Vec<(Vec<u8>, Range<usize>)>)>,
    results: std::sync::mpsc::Receiver<Result<Vec<(Vec<u8>, ByteArena)>, Error>>,
    pending: bool,
}

#[cfg(not(target_family = "wasm"))]
impl fmt::Debug for OrderedPacketWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrderedPacketWorker")
            .field("pending", &self.pending)
            .finish_non_exhaustive()
    }
}

#[cfg(not(target_family = "wasm"))]
impl OrderedPacketWorker {
    fn new(store: Store) -> Self {
        let (job_sender, job_receiver) = std::sync::mpsc::sync_channel(1);
        let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(1);
        rayon::spawn_fifo(move || {
            while let Ok((packet, entries)) = job_receiver.recv() {
                if result_sender
                    .send(prepare_ordered_packet(store.clone(), packet, entries))
                    .is_err()
                {
                    break;
                }
            }
        });
        Self {
            jobs: job_sender,
            results: result_receiver,
            pending: false,
        }
    }
}

fn prepare_ordered_packet(
    store: Store,
    packet: Vec<u8>,
    entries: Vec<(Vec<u8>, Range<usize>)>,
) -> Result<Vec<(Vec<u8>, ByteArena)>, Error> {
    let packet_arena = ByteArena::from_bytes_evicted(store, &packet);
    entries
        .into_iter()
        .map(|(key, range)| {
            let content = packet.get(range.clone()).ok_or(Error::InvalidPageRange)?;
            let value = packet_arena.range_with_content_id(range, content)?;
            Ok((key, value))
        })
        .collect()
}

fn pack_raw_sequence_values(
    store: &Store,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, ByteArena)> {
    let mut output = Vec::with_capacity(entries.len());
    let mut pending = Vec::<(Vec<u8>, Vec<u8>, Digest)>::new();
    let mut pending_bytes = 0usize;

    let flush = |pending: &mut Vec<(Vec<u8>, Vec<u8>, Digest)>,
                 output: &mut Vec<(Vec<u8>, ByteArena)>| {
        if pending.is_empty() {
            return;
        }
        let mut packed = Vec::with_capacity(pending.iter().map(|(_, bytes, _)| bytes.len()).sum());
        for (_, bytes, _) in pending.iter() {
            packed.extend_from_slice(bytes);
        }
        let page = store.intern_evicted(&packed);
        let mut offset = 0u32;
        for (key, bytes, id) in pending.drain(..) {
            let len = u32::try_from(bytes.len()).expect("packed map value page fits u32");
            output.push((
                key,
                ByteArena {
                    store: store.clone(),
                    len: u64::from(len),
                    segments: Arc::from([Segment { page, offset, len }]),
                    id,
                },
            ));
            offset += len;
        }
    };

    for (key, bytes, id) in prepare_raw_sequence_values(store, entries) {
        if bytes.len() > store.page_bytes() {
            flush(&mut pending, &mut output);
            pending_bytes = 0;
            output.push((key, ByteArena::from_value_bytes(store.clone(), &bytes)));
            continue;
        }
        if pending_bytes + bytes.len() > store.page_bytes() {
            flush(&mut pending, &mut output);
            pending_bytes = 0;
        }
        pending_bytes += bytes.len();
        pending.push((
            key,
            bytes,
            id.expect("small raw values have a precomputed identity"),
        ));
    }
    flush(&mut pending, &mut output);
    output
}

fn pack_raw_map_values(
    store: &Store,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
) -> BTreeMap<Vec<u8>, ByteArena> {
    let mut output = BTreeMap::new();
    let mut pending = Vec::<(Vec<u8>, Vec<u8>, Digest)>::new();
    let mut pending_bytes = 0usize;

    let flush = |pending: &mut Vec<(Vec<u8>, Vec<u8>, Digest)>,
                 output: &mut BTreeMap<Vec<u8>, ByteArena>| {
        if pending.is_empty() {
            return;
        }
        let mut packed = Vec::with_capacity(pending.iter().map(|(_, bytes, _)| bytes.len()).sum());
        for (_, bytes, _) in pending.iter() {
            packed.extend_from_slice(bytes);
        }
        let page = store.intern(&packed);
        let mut offset = 0u32;
        for (key, bytes, id) in pending.drain(..) {
            let len = u32::try_from(bytes.len()).expect("packed map value page fits u32");
            output.insert(
                key,
                ByteArena {
                    store: store.clone(),
                    len: u64::from(len),
                    segments: Arc::from([Segment { page, offset, len }]),
                    id,
                },
            );
            offset += len;
        }
    };

    for (key, bytes) in entries {
        if bytes.len() > store.page_bytes() {
            flush(&mut pending, &mut output);
            pending_bytes = 0;
            output.insert(key, ByteArena::from_value_bytes(store.clone(), &bytes));
            continue;
        }
        if pending_bytes + bytes.len() > store.page_bytes() {
            flush(&mut pending, &mut output);
            pending_bytes = 0;
        }
        let page = digest(b"lix-plugin-v3/page\0", [&bytes]);
        let len = u32::try_from(bytes.len()).expect("one packed value fits u32");
        let id = byte_arena_digest(
            u64::from(len),
            &[Segment {
                page,
                offset: 0,
                len,
            }],
        );
        pending_bytes += bytes.len();
        pending.push((key, bytes, id));
    }
    flush(&mut pending, &mut output);
    output
}

fn pack_map_values(
    store: &Store,
    entries: BTreeMap<Vec<u8>, ByteArena>,
) -> BTreeMap<Vec<u8>, ByteArena> {
    let mut output = BTreeMap::new();
    let mut pending = Vec::<(Vec<u8>, ByteArena, Vec<u8>)>::new();
    let mut pending_bytes = 0usize;

    let flush = |pending: &mut Vec<(Vec<u8>, ByteArena, Vec<u8>)>,
                 output: &mut BTreeMap<Vec<u8>, ByteArena>| {
        if pending.is_empty() {
            return;
        }
        let mut packed = Vec::with_capacity(pending.iter().map(|(_, _, bytes)| bytes.len()).sum());
        for (_, _, bytes) in pending.iter() {
            packed.extend_from_slice(bytes);
        }
        let page = store.intern(&packed);
        let mut offset = 0u32;
        for (key, original, bytes) in pending.drain(..) {
            let len = u32::try_from(bytes.len()).expect("packed map value page fits u32");
            output.insert(
                key,
                ByteArena {
                    store: store.clone(),
                    len: u64::from(len),
                    segments: Arc::from([Segment { page, offset, len }]),
                    id: original.id,
                },
            );
            offset += len;
        }
    };

    for (key, value) in entries {
        let bytes = value
            .materialize_untracked()
            .expect("newly constructed map values are readable");
        if bytes.len() > store.page_bytes() {
            flush(&mut pending, &mut output);
            pending_bytes = 0;
            output.insert(key, value);
            continue;
        }
        if pending_bytes + bytes.len() > store.page_bytes() {
            flush(&mut pending, &mut output);
            pending_bytes = 0;
        }
        pending_bytes += bytes.len();
        pending.push((key, value, bytes));
    }
    flush(&mut pending, &mut output);
    output
}

fn collect_page_ids(arena: &ByteArena, output: &mut std::collections::HashSet<Digest>) {
    output.extend(arena.segments.iter().map(|segment| segment.page));
}

#[derive(Debug, Clone)]
pub struct Root {
    pub generation: Arc<str>,
    pub bytes: ByteArena,
    pub entities: MapArena,
    pub state: MapArena,
    id: Digest,
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
        entities: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        state: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Self {
        let entities = entities
            .into_iter()
            .map(|(key, value)| (key, ByteArena::from_value_bytes(store.clone(), &value)))
            .collect();
        let state = state
            .into_iter()
            .map(|(key, value)| (key, ByteArena::from_value_bytes(store.clone(), &value)))
            .collect();
        Self::new(
            generation.into(),
            ByteArena::from_bytes(store.clone(), bytes),
            MapArena::from_entries(store.clone(), entities),
            MapArena::from_entries(store, state),
        )
    }

    fn new(generation: Arc<str>, bytes: ByteArena, entities: MapArena, state: MapArena) -> Self {
        let id = digest(
            b"lix-plugin-v3/root\0",
            [
                generation.as_bytes(),
                bytes.id.as_bytes(),
                entities.id.as_bytes(),
                state.id.as_bytes(),
            ],
        );
        Self {
            generation,
            bytes,
            entities,
            state,
            id,
        }
    }

    pub fn id(&self) -> Digest {
        self.id
    }

    pub fn transaction(&self) -> Transaction {
        Transaction {
            base: self.clone(),
            byte_edits: Vec::new(),
            entity_changes: BTreeMap::new(),
            streamed_entity_changes: Vec::new(),
            ordered_entity_builder: None,
            state_changes: BTreeMap::new(),
            generation: None,
        }
    }

    /// Serializes the immutable manifests and reachable content-addressed
    /// pages. Reopening does not parse plugin state or change the root digest.
    pub fn archive(&self) -> Result<Archive, Error> {
        let mut reachable = BTreeMap::new();
        collect_byte_pages(&self.bytes, &mut reachable)?;
        for (_, value) in self.entities.entries() {
            collect_byte_pages(&value, &mut reachable)?;
        }
        for (_, value) in self.state.entries() {
            collect_byte_pages(&value, &mut reachable)?;
        }
        Ok(Archive {
            page_bytes: self.bytes.store.page_bytes(),
            pages: reachable.into_iter().collect(),
            generation: self.generation.clone(),
            bytes: self.bytes.archived(),
            entities: self.entities.archived(),
            state: self.state.archived(),
            id: self.id,
        })
    }

    /// Deterministically merges durable entity values. Opaque state is not
    /// merge authority and is retained from `a`; callers may rebuild it.
    /// Concurrent unequal values use their content digest as a canonical,
    /// merge-direction-independent tie break.
    pub fn merge_entities(base: &Self, a: &Self, b: &Self) -> Result<Self, Error> {
        if !Arc::ptr_eq(&base.bytes.store.inner, &a.bytes.store.inner)
            || !Arc::ptr_eq(&base.bytes.store.inner, &b.bytes.store.inner)
        {
            return Err(Error::DifferentStores);
        }
        let mut keys = BTreeMap::<Vec<u8>, ()>::new();
        for key in base
            .entities
            .keys()
            .into_iter()
            .chain(a.entities.keys())
            .chain(b.entities.keys())
        {
            keys.insert(key.to_vec(), ());
        }
        let mut merged = BTreeMap::new();
        for key in keys.keys() {
            let base_value = base.entities.value(key)?;
            let a_value = a.entities.value(key)?;
            let b_value = b.entities.value(key)?;
            let selected = if same_value(a_value.as_ref(), b_value.as_ref()) {
                a_value.as_ref()
            } else if same_value(a_value.as_ref(), base_value.as_ref()) {
                b_value.as_ref()
            } else if same_value(b_value.as_ref(), base_value.as_ref()) {
                a_value.as_ref()
            } else {
                [a_value.as_ref(), b_value.as_ref()]
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
            MapArena::from_existing_entries(a.bytes.store.clone(), merged),
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
    let pages = arena.store.inner.pages.lock();
    for segment in arena.segments.iter() {
        let page = pages
            .get(&segment.page)
            .ok_or(Error::MissingPage(segment.page))?;
        if let std::collections::btree_map::Entry::Vacant(entry) = output.entry(segment.page) {
            let bytes = match &page.resident {
                Some(bytes) => bytes.as_ref().to_vec(),
                None => decompress_page(&page.compressed, page.uncompressed_len)?,
            };
            entry.insert(bytes);
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ArchivedByteArena {
    id: Digest,
    len: u64,
    segments: Vec<Segment>,
}

impl ByteArena {
    fn archived(&self) -> ArchivedByteArena {
        ArchivedByteArena {
            id: self.id,
            len: self.len,
            segments: self.segments.to_vec(),
        }
    }

    fn reopen(store: Store, archived: &ArchivedByteArena) -> Result<Self, Error> {
        Ok(Self {
            store,
            len: archived.len,
            segments: archived.segments.clone().into(),
            id: archived.id,
        })
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
    entities: ArchivedMap,
    state: ArchivedMap,
    id: Digest,
}

impl Archive {
    /// Encodes this archive into the stable v3.2 durable wire format.
    ///
    /// The body is canonical (pages are content-digest ordered) and followed
    /// by a BLAKE3 checksum over all preceding bytes. The checksum protects
    /// root metadata as well as content pages, allowing corruption to be
    /// rejected before any arena is reopened or exposed to a plugin.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        const MAGIC: &[u8] = b"LIX-PLUGIN-ARENA\x03\x02";

        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        put_archive_u64(
            &mut output,
            u64::try_from(self.page_bytes).map_err(|_| Error::RangeOverflow)?,
        );
        output.extend_from_slice(self.id.as_bytes());
        put_archive_bytes(&mut output, self.generation.as_bytes())?;
        encode_archived_byte_arena(&mut output, &self.bytes)?;
        encode_archived_map(&mut output, &self.entities)?;
        encode_archived_map(&mut output, &self.state)?;
        put_archive_u64(
            &mut output,
            u64::try_from(self.pages.len()).map_err(|_| Error::RangeOverflow)?,
        );
        let mut previous = None;
        for (id, bytes) in &self.pages {
            if previous.is_some_and(|previous| previous >= *id) {
                return Err(Error::CorruptArchive);
            }
            previous = Some(*id);
            output.extend_from_slice(id.as_bytes());
            put_archive_bytes(&mut output, bytes)?;
        }
        let checksum = digest(b"lix-plugin-v3/archive\0", [&output]);
        output.extend_from_slice(checksum.as_bytes());
        Ok(output)
    }

    /// Decodes and authenticates the stable v3.2 durable wire format.
    ///
    /// Lengths and collection counts are bounded by the input itself before
    /// allocating, and page order/content digests are checked eagerly.
    pub fn decode(encoded: &[u8]) -> Result<Self, Error> {
        const MAGIC: &[u8] = b"LIX-PLUGIN-ARENA\x03\x02";
        const CHECKSUM_BYTES: usize = 32;

        let body_len = encoded
            .len()
            .checked_sub(CHECKSUM_BYTES)
            .ok_or(Error::CorruptArchive)?;
        let (body, encoded_checksum) = encoded.split_at(body_len);
        let expected_checksum = digest(b"lix-plugin-v3/archive\0", [body]);
        if encoded_checksum != expected_checksum.as_bytes() {
            return Err(Error::CorruptArchive);
        }

        let mut decoder = ArchiveDecoder::new(body);
        if decoder.take(MAGIC.len())? != MAGIC {
            return Err(Error::CorruptArchive);
        }
        let page_bytes = usize::try_from(decoder.u64()?).map_err(|_| Error::CorruptArchive)?;
        if page_bytes == 0 {
            return Err(Error::CorruptArchive);
        }
        let id = decoder.digest()?;
        let generation = std::str::from_utf8(decoder.bytes()?)
            .map_err(|_| Error::CorruptArchive)?
            .to_owned();
        let bytes = decoder.byte_arena()?;
        let entities = decoder.map()?;
        let state = decoder.map()?;
        let page_count = decoder.take_count(40)?;
        let mut pages = Vec::with_capacity(page_count);
        let mut previous = None;
        for _ in 0..page_count {
            let page_id = decoder.digest()?;
            if previous.is_some_and(|previous| previous >= page_id) {
                return Err(Error::CorruptArchive);
            }
            previous = Some(page_id);
            let page = decoder.bytes()?.to_vec();
            if digest(b"lix-plugin-v3/page\0", [&page]) != page_id {
                return Err(Error::CorruptArchive);
            }
            pages.push((page_id, page));
        }
        if !decoder.is_finished() {
            return Err(Error::CorruptArchive);
        }
        Ok(Self {
            page_bytes,
            pages,
            generation: Arc::from(generation),
            bytes,
            entities,
            state,
            id,
        })
    }

    pub fn reopen(&self) -> Result<(Store, Root), Error> {
        let store = Store::new(self.page_bytes);
        for (id, bytes) in &self.pages {
            store.insert_archived_page(*id, bytes)?;
        }
        let root = Root::new(
            self.generation.clone(),
            ByteArena::reopen(store.clone(), &self.bytes)?,
            MapArena::reopen(store.clone(), &self.entities)?,
            MapArena::reopen(store.clone(), &self.state)?,
        );
        if root.id != self.id {
            return Err(Error::CorruptArchive);
        }
        Ok((store, root))
    }
}

fn put_archive_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_archive_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), Error> {
    put_archive_u64(
        output,
        u64::try_from(bytes.len()).map_err(|_| Error::RangeOverflow)?,
    );
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_archived_byte_arena(
    output: &mut Vec<u8>,
    arena: &ArchivedByteArena,
) -> Result<(), Error> {
    output.extend_from_slice(arena.id.as_bytes());
    put_archive_u64(output, arena.len);
    put_archive_u64(
        output,
        u64::try_from(arena.segments.len()).map_err(|_| Error::RangeOverflow)?,
    );
    for segment in &arena.segments {
        output.extend_from_slice(segment.page.as_bytes());
        output.extend_from_slice(&segment.offset.to_le_bytes());
        output.extend_from_slice(&segment.len.to_le_bytes());
    }
    Ok(())
}

fn encode_archived_map(output: &mut Vec<u8>, map: &ArchivedMap) -> Result<(), Error> {
    output.extend_from_slice(map.id.as_bytes());
    put_archive_u64(
        output,
        u64::try_from(map.entries.len()).map_err(|_| Error::RangeOverflow)?,
    );
    let mut previous: Option<&[u8]> = None;
    for (key, value) in &map.entries {
        if previous.is_some_and(|previous| previous >= key.as_slice()) {
            return Err(Error::CorruptArchive);
        }
        previous = Some(key);
        put_archive_bytes(output, key)?;
        encode_archived_byte_arena(output, value)?;
    }
    Ok(())
}

struct ArchiveDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> ArchiveDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self.cursor.checked_add(len).ok_or(Error::CorruptArchive)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(Error::CorruptArchive)?;
        self.cursor = end;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .expect("archive u64 slice has exact length"),
        ))
    }

    fn digest(&mut self) -> Result<Digest, Error> {
        Ok(Digest(
            self.take(32)?
                .try_into()
                .expect("archive digest slice has exact length"),
        ))
    }

    fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let len = usize::try_from(self.u64()?).map_err(|_| Error::CorruptArchive)?;
        self.take(len)
    }

    fn count(&self, minimum_item_bytes: usize) -> Result<usize, Error> {
        let mut cursor = self.cursor;
        let count = u64::from_le_bytes(
            self.bytes
                .get(cursor..cursor.saturating_add(8))
                .ok_or(Error::CorruptArchive)?
                .try_into()
                .expect("archive count slice has exact length"),
        );
        cursor += 8;
        let count = usize::try_from(count).map_err(|_| Error::CorruptArchive)?;
        if count
            .checked_mul(minimum_item_bytes)
            .is_none_or(|minimum| minimum > self.bytes.len().saturating_sub(cursor))
        {
            return Err(Error::CorruptArchive);
        }
        Ok(count)
    }

    fn take_count(&mut self, minimum_item_bytes: usize) -> Result<usize, Error> {
        let count = self.count(minimum_item_bytes)?;
        self.cursor += 8;
        Ok(count)
    }

    fn byte_arena(&mut self) -> Result<ArchivedByteArena, Error> {
        let id = self.digest()?;
        let len = self.u64()?;
        let segment_count = self.take_count(40)?;
        let mut segments = Vec::with_capacity(segment_count);
        let mut total_len = 0_u64;
        for _ in 0..segment_count {
            let page = self.digest()?;
            let offset = u32::from_le_bytes(
                self.take(4)?
                    .try_into()
                    .expect("archive segment offset has exact length"),
            );
            let segment_len = u32::from_le_bytes(
                self.take(4)?
                    .try_into()
                    .expect("archive segment length has exact length"),
            );
            if segment_len == 0 {
                return Err(Error::CorruptArchive);
            }
            total_len = total_len
                .checked_add(u64::from(segment_len))
                .ok_or(Error::CorruptArchive)?;
            segments.push(Segment {
                page,
                offset,
                len: segment_len,
            });
        }
        if total_len != len || (len == 0) != segments.is_empty() {
            return Err(Error::CorruptArchive);
        }
        Ok(ArchivedByteArena { id, len, segments })
    }

    fn map(&mut self) -> Result<ArchivedMap, Error> {
        let id = self.digest()?;
        let entry_count = self.take_count(80)?;
        let mut entries = Vec::with_capacity(entry_count);
        let mut previous: Option<Vec<u8>> = None;
        for _ in 0..entry_count {
            let key = self.bytes()?.to_vec();
            if previous
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
            {
                return Err(Error::CorruptArchive);
            }
            previous = Some(key.clone());
            entries.push((key, self.byte_arena()?));
        }
        Ok(ArchivedMap { id, entries })
    }

    fn is_finished(&self) -> bool {
        self.cursor == self.bytes.len()
    }
}

#[derive(Debug)]
pub struct Transaction {
    base: Root,
    byte_edits: Vec<ByteEdit>,
    entity_changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    streamed_entity_changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ordered_entity_builder: Option<OrderedMapBuilder>,
    state_changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    generation: Option<Arc<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedPage {
    /// The last key returned when more entries remain. Pass this value as the
    /// next call's exclusive `after_key`.
    pub next_key: Option<Vec<u8>>,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticPage {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub fingerprint: [u8; 32],
    pub record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticPageBatch {
    pub next_key: Option<Vec<u8>>,
    pub pages: Vec<SemanticPage>,
}

impl Transaction {
    pub fn edit_bytes(&mut self, edit: ByteEdit) {
        self.byte_edits.push(edit);
    }

    pub fn upsert_entity(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.entity_changes.insert(key, Some(value));
    }

    pub fn delete_entity(&mut self, key: Vec<u8>) {
        self.entity_changes.insert(key, None);
    }

    /// Stages already host-validated cursor output without building a second
    /// document-sized ordered map. The commit path sorts only when the plugin
    /// did not emit stable key order.
    pub fn stream_upsert_entity(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.streamed_entity_changes.push((key, Some(value)));
    }

    pub fn stream_delete_entity(&mut self, key: Vec<u8>) {
        self.streamed_entity_changes.push((key, None));
    }

    pub fn declare_ordered_entity_output(&mut self) -> Result<(), Error> {
        if !self.base.entities.is_empty()
            || !self.entity_changes.is_empty()
            || !self.streamed_entity_changes.is_empty()
            || self.ordered_entity_builder.is_some()
        {
            return Err(Error::InvalidOrderedOutput);
        }
        self.ordered_entity_builder =
            Some(OrderedMapBuilder::new(self.base.entities.store.clone()));
        Ok(())
    }

    pub fn stream_ordered_entity_page(
        &mut self,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), Error> {
        self.ordered_entity_builder
            .as_mut()
            .ok_or(Error::InvalidOrderedOutput)?
            .push(entries)
    }

    pub fn stream_ordered_entity_packet(
        &mut self,
        packet: Vec<u8>,
        entries: Vec<(Vec<u8>, Range<usize>)>,
    ) -> Result<(), Error> {
        self.ordered_entity_builder
            .as_mut()
            .ok_or(Error::InvalidOrderedOutput)?
            .push_packet(packet, entries)
    }

    pub fn has_ordered_entity_output(&self) -> bool {
        self.ordered_entity_builder.is_some()
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

    pub fn file_len(&self) -> Result<u64, Error> {
        Ok(self.candidate_bytes()?.len())
    }

    /// Reads the verified prospective file, not the accepted base. The host
    /// can stage sparse byte edits before the guest runs, and the guest only
    /// materializes the ranges needed by its format-specific incremental
    /// parser.
    pub fn read_file(&self, offset: u64, length: u64) -> Result<Vec<u8>, Error> {
        self.candidate_bytes()?.read(offset, length)
    }

    pub fn find_file_byte_forward(
        &self,
        offset: u64,
        length: u64,
        needle: u8,
    ) -> Result<Option<u64>, Error> {
        self.candidate_bytes()?
            .find_byte_forward(offset, length, needle)
    }

    pub fn find_file_byte_backward(
        &self,
        offset: u64,
        length: u64,
        needle: u8,
    ) -> Result<Option<u64>, Error> {
        self.candidate_bytes()?
            .find_byte_backward(offset, length, needle)
    }

    pub fn count_file_byte(&self, offset: u64, length: u64, needle: u8) -> Result<u64, Error> {
        self.candidate_bytes()?.count_byte(offset, length, needle)
    }

    pub fn locate_file_record(
        &self,
        range_offset: u64,
        range_length: u64,
        position: u64,
        delimiter: u8,
        forbidden: &[u8],
    ) -> Result<Option<ByteRecordLocator>, Error> {
        self.candidate_bytes()?.locate_record(
            range_offset,
            range_length,
            position,
            delimiter,
            forbidden,
        )
    }

    pub fn get_entity(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        get_prospective(&self.base.entities, &self.entity_changes, key)
    }

    pub fn scan_entities(
        &self,
        after_key: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<KeyedPage, Error> {
        if self.entity_changes.is_empty() {
            return self.base.entities.scan(after_key, max_bytes);
        }
        scan_prospective(
            &self.base.entities,
            &self.entity_changes,
            after_key,
            max_bytes,
        )
    }

    pub fn semantic_entity_pages(
        &self,
        after_key: Option<&[u8]>,
        max_pages: u32,
    ) -> Result<SemanticPageBatch, Error> {
        // Entity output has not been accepted while the guest is reconciling,
        // so the staged successor aliases the predecessor's semantic pages.
        self.base.entities.semantic_pages(after_key, max_pages)
    }

    pub fn semantic_entity_pages_bounded(
        &self,
        after_key: Option<&[u8]>,
        max_pages: u32,
        max_bytes: usize,
    ) -> Result<SemanticPageBatch, Error> {
        self.base
            .entities
            .semantic_pages_bounded(after_key, max_pages, max_bytes)
    }

    pub fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        get_prospective(&self.base.state, &self.state_changes, key)
    }

    pub fn scan_state(
        &self,
        after_key: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<KeyedPage, Error> {
        if self.state_changes.is_empty() {
            return self.base.state.scan(after_key, max_bytes);
        }
        scan_prospective(&self.base.state, &self.state_changes, after_key, max_bytes)
    }

    fn candidate_bytes(&self) -> Result<ByteArena, Error> {
        let mut edits = self.byte_edits.clone();
        edits.sort_by_key(|edit| edit.offset);
        self.base.bytes.apply(&edits)
    }

    pub fn commit(mut self) -> Result<Root, Error> {
        self.byte_edits.sort_by_key(|edit| edit.offset);
        let bytes = self.base.bytes.apply(&self.byte_edits)?;
        let entities = if let Some(builder) = self.ordered_entity_builder {
            if !self.entity_changes.is_empty() || !self.streamed_entity_changes.is_empty() {
                return Err(Error::InvalidOrderedOutput);
            }
            builder.finish()?
        } else if self.entity_changes.is_empty()
            && self.base.entities.is_empty()
            && self
                .streamed_entity_changes
                .iter()
                .all(|(_, value)| value.is_some())
        {
            MapArena::from_raw_sequence(
                self.base.entities.store.clone(),
                self.streamed_entity_changes
                    .into_iter()
                    .map(|(key, value)| (key, value.expect("cold streamed values were verified")))
                    .collect(),
            )
        } else {
            for (key, value) in self.streamed_entity_changes {
                self.entity_changes.insert(key, value);
            }
            self.base.entities.apply_owned(self.entity_changes)
        };
        let state = self.base.state.apply_owned(self.state_changes);
        Ok(Root::new(
            self.generation.unwrap_or(self.base.generation),
            bytes,
            entities,
            state,
        ))
    }
}

fn get_prospective(
    base: &MapArena,
    changes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    match changes.get(key) {
        Some(value) => Ok(value.clone()),
        None => base.get(key),
    }
}

fn scan_prospective(
    base: &MapArena,
    changes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    after_key: Option<&[u8]>,
    max_bytes: usize,
) -> Result<KeyedPage, Error> {
    if max_bytes == 0 {
        return Err(Error::LimitExceeded);
    }
    let mut keys = BTreeMap::<Vec<u8>, ()>::new();
    for key in base.keys().into_iter().chain(changes.keys().cloned()) {
        if after_key.is_none_or(|after| key.as_slice() > after) {
            keys.insert(key, ());
        }
    }

    let mut entries = Vec::new();
    let mut bytes = 0usize;
    let mut has_more = false;
    for key in keys.keys() {
        let Some(value) = get_prospective(base, changes, key)? else {
            continue;
        };
        let entry_bytes = key
            .len()
            .checked_add(value.len())
            .ok_or(Error::RangeOverflow)?;
        if entry_bytes > max_bytes && entries.is_empty() {
            return Err(Error::LimitExceeded);
        }
        if bytes
            .checked_add(entry_bytes)
            .is_none_or(|total| total > max_bytes)
        {
            has_more = true;
            break;
        }
        bytes += entry_bytes;
        entries.push((key.clone(), value));
    }
    Ok(KeyedPage {
        next_key: has_more.then(|| {
            entries
                .last()
                .expect("a remaining entry implies one entry fit")
                .0
                .clone()
        }),
        entries,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    CorruptArchive,
    DifferentStores,
    InvalidEdits,
    InvalidOrderedOutput,
    InvalidPageRange,
    MissingPage(Digest),
    RangeOutOfBounds,
    RangeOverflow,
    LimitExceeded,
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
        transaction.upsert_entity(b"row/1".to_vec(), b"{\"id\":1,\"v\":\"X\"}".to_vec());
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
    fn rollback_does_not_publish_a_partial_root() {
        let (_store, root) = fixture();
        let original = root.id();
        let mut transaction = root.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: 99,
            delete_len: 1,
            insert: Vec::new(),
        });
        transaction.upsert_entity(b"row/2".to_vec(), b"partial".to_vec());
        assert!(matches!(transaction.commit(), Err(Error::InvalidEdits)));
        assert_eq!(root.id(), original);
        assert!(root.entities.get(b"row/2").unwrap().is_none());
    }

    #[test]
    fn transaction_reads_verified_successor_without_publishing_it() {
        let (store, root) = fixture();
        let mut transaction = root.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: 4,
            delete_len: 4,
            insert: b"WXYZ".to_vec(),
        });
        transaction.upsert_entity(b"row/2".to_vec(), b"{\"id\":2}".to_vec());
        transaction.delete_entity(b"row/1".to_vec());
        transaction.put_state(b"index/1".to_vec(), b"row/2@4".to_vec());

        store.reset_metrics();
        assert_eq!(transaction.file_len().unwrap(), 12);
        assert_eq!(transaction.read_file(3, 6).unwrap(), b"dWXYZi");
        assert_eq!(store.metrics().page_bytes_read, 6);
        assert_eq!(transaction.get_entity(b"row/1").unwrap(), None);
        assert_eq!(
            transaction.get_entity(b"row/2").unwrap(),
            Some(b"{\"id\":2}".to_vec())
        );
        assert_eq!(
            transaction.get_state(b"index/1").unwrap(),
            Some(b"row/2@4".to_vec())
        );

        // Prospective reads do not mutate the accepted root.
        assert_eq!(
            root.bytes.read(0, root.bytes.len()).unwrap(),
            b"abcdefghijkl"
        );
        assert!(root.entities.get(b"row/2").unwrap().is_none());
    }

    #[test]
    fn transaction_scans_prospective_maps_in_bounded_stable_pages() {
        let (_store, root) = fixture();
        let mut transaction = root.transaction();
        transaction.delete_entity(b"row/1".to_vec());
        transaction.upsert_entity(b"row/2".to_vec(), b"two".to_vec());
        transaction.upsert_entity(b"row/3".to_vec(), b"three".to_vec());

        let first = transaction.scan_entities(None, 8).unwrap();
        assert_eq!(first.entries, vec![(b"row/2".to_vec(), b"two".to_vec())]);
        assert_eq!(first.next_key, Some(b"row/2".to_vec()));
        let second = transaction
            .scan_entities(first.next_key.as_deref(), 10)
            .unwrap();
        assert_eq!(second.entries, vec![(b"row/3".to_vec(), b"three".to_vec())]);
        assert_eq!(second.next_key, None);
    }

    #[test]
    fn ordered_scan_does_not_decode_manifests_beyond_its_byte_page() {
        let store = Store::new(64);
        let entities = (0..600)
            .map(|index| (format!("row/{index:04}").into_bytes(), b"v".to_vec()))
            .collect::<Vec<_>>();
        let root = Root::import(
            store.clone(),
            "generation-a",
            b"{}",
            entities,
            std::iter::empty(),
        );
        assert!(root.entities.pages.len() >= 2);
        let unavailable_manifest = root.entities.pages[1].manifest;
        store.inner.pages.lock().remove(&unavailable_manifest);

        let first = root.entities.scan(None, 20).unwrap();
        assert!(!first.entries.is_empty());
        assert!(first.next_key.is_some());
        assert!(
            matches!(
                root.entities.scan(None, usize::MAX),
                Err(Error::MissingPage(id)) if id == unavailable_manifest
            ),
            "an unbounded scan should eventually reach the unavailable later manifest"
        );
    }

    #[test]
    fn semantic_page_fingerprints_skip_unchanged_ordered_ranges() {
        let store = Store::new(64);
        let entities = (0..600)
            .map(|index| {
                (
                    format!("row/{index:04}").into_bytes(),
                    format!("{{\"value\":{index}}}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let root = Root::import(
            store.clone(),
            "generation-a",
            b"{}",
            entities.clone(),
            std::iter::empty(),
        );
        let first = root.entities.semantic_pages(None, 1).unwrap();
        assert!((192..=320).contains(&first.pages[0].record_count));
        assert!(first.next_key.is_some());
        let second = root
            .entities
            .semantic_pages(first.next_key.as_deref(), 8)
            .unwrap();
        assert_eq!(
            second
                .pages
                .iter()
                .map(|page| u64::from(page.record_count))
                .sum::<u64>()
                + u64::from(first.pages[0].record_count),
            600
        );

        let mut transaction = root.transaction();
        transaction.upsert_entity(b"row/0300".to_vec(), b"{\"value\":\"changed\"}".to_vec());
        let successor = transaction.commit().unwrap();
        let before = root.entities.semantic_pages(None, 8).unwrap();
        let after = successor.entities.semantic_pages(None, 8).unwrap();
        assert_eq!(before.pages.len(), after.pages.len());
        assert_eq!(before.pages[0].fingerprint, after.pages[0].fingerprint);
        assert_ne!(before.pages[1].fingerprint, after.pages[1].fingerprint);
        assert_eq!(before.pages[2].fingerprint, after.pages[2].fingerprint);

        let pages_before_insert = store.unique_page_count();
        let mut transaction = root.transaction();
        transaction.upsert_entity(b"row/0000a".to_vec(), b"{\"value\":\"inserted\"}".to_vec());
        let inserted = transaction.commit().unwrap();
        assert!(
            store
                .unique_page_count()
                .saturating_sub(pages_before_insert)
                <= 4,
            "one structural insertion must not repack the document-sized value arena"
        );
        let inserted_pages = inserted.entities.semantic_pages(None, 8).unwrap();
        assert_eq!(before.pages.len(), inserted_pages.pages.len());
        assert_eq!(before.pages[0].first_key, inserted_pages.pages[0].first_key);
        assert_eq!(before.pages[0].last_key, inserted_pages.pages[0].last_key);
        assert_ne!(
            before.pages[0].fingerprint,
            inserted_pages.pages[0].fingerprint
        );
        assert_eq!(
            before.pages[1..],
            inserted_pages.pages[1..],
            "an early insertion must not shift every later semantic page"
        );
        let mut canonical_entries = entities;
        canonical_entries.push((b"row/0000a".to_vec(), b"{\"value\":\"inserted\"}".to_vec()));
        let canonical = Root::import(
            Store::new(64),
            "generation-a",
            b"{}",
            canonical_entries,
            std::iter::empty(),
        );
        assert_eq!(
            inserted.entities.id(),
            canonical.entities.id(),
            "localized COW insertion must retain canonical map identity"
        );
        let inserted_id = inserted.id();
        let (_, reopened_inserted) = inserted.archive().unwrap().reopen().unwrap();
        assert_eq!(
            reopened_inserted.id(),
            inserted_id,
            "structurally edited pages must reopen without repacking"
        );

        let pages_before_delete = store.unique_page_count();
        let mut transaction = root.transaction();
        for index in 0..100 {
            transaction.delete_entity(format!("row/{index:04}").into_bytes());
        }
        let deleted = transaction.commit().unwrap();
        assert!(
            store
                .unique_page_count()
                .saturating_sub(pages_before_delete)
                <= 4,
            "a range deletion must reuse unchanged value pages"
        );
        let remaining = (100..600)
            .map(|index| {
                (
                    format!("row/{index:04}").into_bytes(),
                    format!("{{\"value\":{index}}}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let canonical = Root::import(
            Store::new(64),
            "generation-a",
            b"{}",
            remaining,
            std::iter::empty(),
        );
        assert_eq!(
            deleted.entities.id(),
            canonical.entities.id(),
            "localized COW deletion must retain canonical map identity"
        );
    }

    #[test]
    fn semantic_page_cursor_bounds_exact_summary_key_bytes() {
        let store = Store::new(64);
        let entities = (0..600)
            .map(|index| {
                (
                    format!("entity/{index:04}/{}", "x".repeat(96)).into_bytes(),
                    b"value".to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let root = Root::import(store, "generation-a", b"{}", entities, std::iter::empty());
        assert!(matches!(
            root.entities.semantic_pages_bounded(None, 8, 200),
            Err(Error::LimitExceeded)
        ));
        let batch = root.entities.semantic_pages_bounded(None, 8, 300).unwrap();
        assert_eq!(batch.pages.len(), 1);
        let lowered_bytes = batch.pages.iter().fold(0usize, |bytes, page| {
            bytes + page.first_key.len() + page.last_key.len() + 36
        });
        assert!(lowered_bytes <= 300);
        assert!(batch.next_key.is_some());
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
            transaction.upsert_entity(b"row/2".to_vec(), b"{\"id\":2}".to_vec());
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
    fn packet_backed_ordered_values_are_canonical_and_reopenable() {
        let store = Store::new(4);
        let base = Root::empty(store, "generation-a");
        let mut packed = base.transaction();
        packed.declare_ordered_entity_output().unwrap();
        packed
            .stream_ordered_entity_packet(
                b"xxoneYYtwo".to_vec(),
                vec![(b"row/1".to_vec(), 2..5), (b"row/2".to_vec(), 7..10)],
            )
            .unwrap();
        let packed = packed.commit().unwrap();

        let mut ordinary = base.transaction();
        ordinary.declare_ordered_entity_output().unwrap();
        ordinary
            .stream_ordered_entity_page(vec![
                (b"row/1".to_vec(), b"one".to_vec()),
                (b"row/2".to_vec(), b"two".to_vec()),
            ])
            .unwrap();
        let ordinary = ordinary.commit().unwrap();

        assert_eq!(packed.entities.id(), ordinary.entities.id());
        assert_eq!(packed.id(), ordinary.id());
        assert_eq!(packed.entities.get(b"row/1").unwrap().unwrap(), b"one");
        assert_eq!(packed.entities.get(b"row/2").unwrap().unwrap(), b"two");

        let expected = packed.id();
        let (_, reopened) = packed.archive().unwrap().reopen().unwrap();
        assert_eq!(reopened.id(), expected);
        assert_eq!(reopened.entities.get(b"row/2").unwrap().unwrap(), b"two");
    }

    #[test]
    fn content_defined_pages_are_canonical_across_stream_packet_boundaries() {
        let store = Store::new(64);
        let entries = (0..600)
            .map(|index| {
                (
                    format!("row/{index:04}").into_bytes(),
                    format!("value-{index:04}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let ordinary = Root::import(
            store.clone(),
            "generation-a",
            b"",
            entries.clone(),
            std::iter::empty(),
        );

        let base = Root::empty(store, "generation-a");
        let mut streamed = base.transaction();
        streamed.declare_ordered_entity_output().unwrap();
        for packet in entries.chunks(73) {
            streamed
                .stream_ordered_entity_page(packet.to_vec())
                .unwrap();
        }
        let streamed = streamed.commit().unwrap();

        assert_eq!(ordinary.entities.id(), streamed.entities.id());
        assert_eq!(ordinary.id(), streamed.id());
        assert_eq!(
            ordinary.entities.semantic_pages(None, 16).unwrap(),
            streamed.entities.semantic_pages(None, 16).unwrap()
        );
    }

    #[test]
    fn upgrade_changes_generation_without_rewriting_arenas() {
        let (_store, root) = fixture();
        let byte_id = root.bytes.id();
        let entity_id = root.entities.id();
        let mut transaction = root.transaction();
        transaction.upgrade_to("generation-b");
        let upgraded = transaction.commit().unwrap();
        assert_eq!(upgraded.bytes.id(), byte_id);
        assert_eq!(upgraded.entities.id(), entity_id);
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
    fn unchanged_map_values_keep_stable_identity() {
        let (_store, root) = fixture();
        let before = root.entities.value_id(b"row/1").unwrap();
        let mut transaction = root.transaction();
        transaction.upsert_entity(b"row/2".to_vec(), b"{\"id\":2}".to_vec());
        let successor = transaction.commit().unwrap();
        assert_eq!(successor.entities.value_id(b"row/1"), Some(before));
    }

    #[test]
    fn opaque_state_is_copy_on_write_rollback_safe_and_reopenable() {
        let (store, root) = fixture();
        let original_state = root.state.value_id(b"index/0").unwrap();
        let original_bytes = root.bytes.id();
        let original_entities = root.entities.id();

        let mut abandoned = root.transaction();
        abandoned.put_state(b"index/0".to_vec(), b"abandoned".to_vec());
        abandoned.put_state(b"index/1".to_vec(), b"partial".to_vec());
        drop(abandoned);
        assert_eq!(root.state.value_id(b"index/0"), Some(original_state));
        assert_eq!(root.state.get(b"index/1").unwrap(), None);

        let mut transaction = root.transaction();
        transaction.put_state(b"index/0".to_vec(), b"row/1@7".to_vec());
        transaction.put_state(b"index/1".to_vec(), b"opaque-plugin-page".to_vec());
        let successor = transaction.commit().unwrap();
        assert_eq!(successor.bytes.id(), original_bytes);
        assert_eq!(successor.entities.id(), original_entities);
        assert_ne!(successor.state.value_id(b"index/0"), Some(original_state));
        assert_eq!(
            successor.state.get(b"index/1").unwrap().unwrap(),
            b"opaque-plugin-page"
        );
        assert_eq!(
            root.state.get(b"index/0").unwrap().unwrap(),
            b"row/1",
            "publishing successor state must not mutate the predecessor"
        );

        let expected_root = successor.id();
        let expected_state = successor.state.id();
        let archive = successor.archive().unwrap();
        store.evict_resident_pages();
        let (_, reopened) = archive.reopen().unwrap();
        assert_eq!(reopened.id(), expected_root);
        assert_eq!(reopened.state.id(), expected_state);
        assert_eq!(
            reopened.state.get(b"index/1").unwrap().unwrap(),
            b"opaque-plugin-page"
        );
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
        let expected_entity = successor.entities.value_id(b"row/1");
        let archive = successor.archive().unwrap();
        drop(successor);
        drop(root);

        let (_reopened_store, reopened) = archive.reopen().unwrap();
        assert_eq!(reopened.id(), expected_id);
        assert_eq!(reopened.entities.value_id(b"row/1"), expected_entity);
        assert_eq!(
            reopened.bytes.read(0, reopened.bytes.len()).unwrap(),
            b"abcdefXYijkl"
        );
    }

    #[test]
    fn durable_archive_encoding_is_deterministic_and_rejects_corruption() {
        let (_store, root) = fixture();
        let mut transaction = root.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: 4,
            delete_len: 2,
            insert: b"WXYZ".to_vec(),
        });
        transaction.upsert_entity(b"row/2".to_vec(), b"{\"id\":2}".to_vec());
        transaction.put_state(b"index/1".to_vec(), b"opaque".to_vec());
        let successor = transaction.commit().unwrap();
        let expected_id = successor.id();
        let archive = successor.archive().unwrap();
        archive.reopen().unwrap();
        let encoded = archive.encode().unwrap();

        assert_eq!(
            encoded,
            successor.archive().unwrap().encode().unwrap(),
            "the same immutable root must have one canonical durable encoding"
        );
        let decoded = Archive::decode(&encoded).unwrap();
        assert_eq!(decoded.encode().unwrap(), encoded);
        let (_store, reopened) = decoded.reopen().unwrap();
        assert_eq!(reopened.id(), expected_id);
        assert_eq!(
            reopened.bytes.read(0, reopened.bytes.len()).unwrap(),
            b"abcdWXYZghijkl"
        );
        assert_eq!(
            reopened.entities.get(b"row/2").unwrap().unwrap(),
            b"{\"id\":2}"
        );
        assert_eq!(reopened.state.get(b"index/1").unwrap().unwrap(), b"opaque");

        for index in [0, encoded.len() / 2, encoded.len() - 1] {
            let mut corrupt = encoded.clone();
            corrupt[index] ^= 0x80;
            assert!(matches!(
                Archive::decode(&corrupt),
                Err(Error::CorruptArchive)
            ));
        }
        assert!(matches!(
            Archive::decode(&encoded[..encoded.len() - 1]),
            Err(Error::CorruptArchive)
        ));
    }

    #[test]
    fn merge_is_direction_independent_and_preserves_disjoint_changes() {
        let (_store, base) = fixture();
        let mut a = base.transaction();
        a.upsert_entity(b"row/a".to_vec(), b"A".to_vec());
        a.upsert_entity(b"row/conflict".to_vec(), b"left".to_vec());
        let a = a.commit().unwrap();
        let mut b = base.transaction();
        b.upsert_entity(b"row/b".to_vec(), b"B".to_vec());
        b.upsert_entity(b"row/conflict".to_vec(), b"right".to_vec());
        let b = b.commit().unwrap();

        let ab = Root::merge_entities(&base, &a, &b).unwrap();
        let ba = Root::merge_entities(&base, &b, &a).unwrap();
        assert_eq!(ab.entities.id(), ba.entities.id());
        assert_eq!(ab.entities.get(b"row/a").unwrap().unwrap(), b"A");
        assert_eq!(ab.entities.get(b"row/b").unwrap().unwrap(), b"B");
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
            compare_to_v2(
                v2,
                PerformanceMeasurement {
                    p95_nanoseconds: 3_000_000,
                    peak_total_bytes: 10_000_000,
                }
            )
            .passes()
        );
        assert_eq!(
            compare_to_v2(
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
            compare_to_v2(
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
