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
    zstd::bulk::compress(bytes, 3).expect("compressing an in-memory arena page cannot fail")
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
        for (_, value) in root.entities.iter().chain(root.state.iter()) {
            collect_page_ids(value, &mut reachable);
        }
        self.inner
            .pages
            .lock()
            .retain(|id, _| reachable.contains(id));
    }

    fn intern(&self, bytes: &[u8]) -> Digest {
        let id = digest(b"lix-plugin-v3/page\0", [bytes]);
        let mut pages = self.inner.pages.lock();
        if let std::collections::hash_map::Entry::Vacant(entry) = pages.entry(id) {
            let compressed = compress_page(bytes);
            entry.insert(StoredPage {
                compressed: Arc::from(compressed),
                resident: Some(Arc::from(bytes)),
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
            parts.push(segment.page.0.to_vec());
            parts.push(segment.offset.to_le_bytes().to_vec());
            parts.push(segment.len.to_le_bytes().to_vec());
        }
        let id = digest(b"lix-plugin-v3/bytes\0", parts);
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

const MAP_PAGE_ENTRIES: usize = 256;

#[derive(Debug, Clone)]
struct MapPage {
    entries: Arc<Vec<(Vec<u8>, ByteArena)>>,
    id: Digest,
}

impl MapPage {
    fn new(entries: Vec<(Vec<u8>, ByteArena)>) -> Self {
        let mut parts = Vec::with_capacity(entries.len() * 2);
        for (key, value) in &entries {
            parts.push(key.clone());
            parts.push(value.id.0.to_vec());
        }
        Self {
            entries: Arc::new(entries),
            id: digest(b"lix-plugin-v3/map-page\0", parts),
        }
    }

    fn last_key(&self) -> &[u8] {
        self.entries
            .last()
            .expect("map pages are never empty")
            .0
            .as_slice()
    }
}

impl MapArena {
    pub fn empty(store: Store) -> Self {
        Self::from_entries(store, BTreeMap::new())
    }

    fn from_entries(store: Store, entries: BTreeMap<Vec<u8>, ByteArena>) -> Self {
        let entries = pack_map_values(&store, entries);
        let len = entries.len();
        let pages = entries
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(MAP_PAGE_ENTRIES)
            .map(|entries| MapPage::new(entries.to_vec()))
            .collect();
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
        self.value(key)
            .map(|value| value.read(0, value.len()))
            .transpose()
    }

    pub fn value_id(&self, key: &[u8]) -> Option<Digest> {
        self.value(key).map(ByteArena::id)
    }

    pub fn scan(&self, after_key: Option<&[u8]>, max_bytes: usize) -> Result<KeyedPage, Error> {
        scan_prospective(self, &BTreeMap::new(), after_key, max_bytes)
    }

    pub fn semantic_pages(
        &self,
        after_key: Option<&[u8]>,
        max_pages: u32,
    ) -> Result<SemanticPageBatch, Error> {
        if max_pages == 0 {
            return Err(Error::LimitExceeded);
        }
        let first = after_key.map_or(0, |key| {
            self.pages.partition_point(|page| page.last_key() <= key)
        });
        let selected = self.pages[first..]
            .iter()
            .take(usize::try_from(max_pages).unwrap_or(usize::MAX))
            .map(|page| SemanticPage {
                first_key: page
                    .entries
                    .first()
                    .expect("map page is nonempty")
                    .0
                    .clone(),
                last_key: page.entries.last().expect("map page is nonempty").0.clone(),
                fingerprint: page.id.0,
                record_count: u32::try_from(page.entries.len())
                    .expect("map page entry bound fits u32"),
            })
            .collect::<Vec<_>>();
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
        if changes
            .iter()
            .all(|(key, value)| value.is_some() && self.value(key).is_some())
        {
            let mut pages = self.pages.as_ref().clone();
            for (key, value) in changes {
                let page_index = pages.partition_point(|page| page.last_key() < key.as_slice());
                let page = &mut pages[page_index];
                let mut entries = page.entries.as_ref().clone();
                let entry_index = entries
                    .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key.as_slice()))
                    .expect("the sparse replacement key was verified above");
                entries[entry_index].1 = ByteArena::from_bytes(
                    self.store.clone(),
                    value.as_ref().expect("replacement was verified above"),
                );
                *page = MapPage::new(entries);
            }
            return Self::from_pages(self.store.clone(), pages, self.len);
        }

        let mut entries = self
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
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
        self.iter().map(|(key, _)| key.as_slice())
    }

    fn iter(&self) -> impl Iterator<Item = &(Vec<u8>, ByteArena)> {
        self.pages.iter().flat_map(|page| page.entries.iter())
    }

    fn value(&self, key: &[u8]) -> Option<&ByteArena> {
        let page = self
            .pages
            .get(self.pages.partition_point(|page| page.last_key() < key))?;
        page.entries
            .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            .ok()
            .map(|index| &page.entries[index].1)
    }

    fn archived(&self) -> ArchivedMap {
        ArchivedMap {
            id: self.id,
            entries: self
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
            .map(|(key, value)| (key, ByteArena::from_bytes(store.clone(), &value)))
            .collect();
        let state = state
            .into_iter()
            .map(|(key, value)| (key, ByteArena::from_bytes(store.clone(), &value)))
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
            state_changes: BTreeMap::new(),
            generation: None,
        }
    }

    /// Serializes the immutable manifests and reachable content-addressed
    /// pages. Reopening does not parse plugin state or change the root digest.
    pub fn archive(&self) -> Result<Archive, Error> {
        let mut reachable = BTreeMap::new();
        collect_byte_pages(&self.bytes, &mut reachable)?;
        for (_, value) in self.entities.iter() {
            collect_byte_pages(value, &mut reachable)?;
        }
        for (_, value) in self.state.iter() {
            collect_byte_pages(value, &mut reachable)?;
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
            .chain(a.entities.keys())
            .chain(b.entities.keys())
        {
            keys.insert(key.to_vec(), ());
        }
        let mut merged = BTreeMap::new();
        for key in keys.keys() {
            let base_value = base.entities.value(key);
            let a_value = a.entities.value(key);
            let b_value = b.entities.value(key);
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

#[derive(Debug)]
pub struct Transaction {
    base: Root,
    byte_edits: Vec<ByteEdit>,
    entity_changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
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

    pub fn get_entity(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        get_prospective(&self.base.entities, &self.entity_changes, key)
    }

    pub fn scan_entities(
        &self,
        after_key: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<KeyedPage, Error> {
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

    pub fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        get_prospective(&self.base.state, &self.state_changes, key)
    }

    pub fn scan_state(
        &self,
        after_key: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<KeyedPage, Error> {
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
        let entities = self.base.entities.apply(&self.entity_changes);
        let state = self.base.state.apply(&self.state_changes);
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
    for key in base
        .keys()
        .map(ToOwned::to_owned)
        .chain(changes.keys().cloned())
    {
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
        let root = Root::import(store, "generation-a", b"{}", entities, std::iter::empty());
        let first = root.entities.semantic_pages(None, 1).unwrap();
        assert_eq!(first.pages[0].record_count, 256);
        let second = root
            .entities
            .semantic_pages(first.next_key.as_deref(), 8)
            .unwrap();
        assert_eq!(
            second
                .pages
                .iter()
                .map(|page| page.record_count)
                .collect::<Vec<_>>(),
            vec![256, 88]
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
