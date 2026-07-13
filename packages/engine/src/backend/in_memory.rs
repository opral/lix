use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend::conformance::{BackendFactory, BackendFixture, BackendTestConfig};
use crate::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetManyResult,
    GetOptions, Key, KeyRange, ProjectedValue, PutBatch, ReadEntry, ReadOptions, ScanChunk,
    ScanOptions, SpaceId, StoredValue, WriteOptions, WriteStats,
};

type InMemoryMap = BTreeMap<Key, Bytes>;

/// The in-memory backend has no native namespaces; it scopes keys to spaces
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
enum EntriesState {
    #[default]
    Empty,
    Flat(InMemoryMap),
    Layered {
        base: Arc<Self>,
        puts: InMemoryMap,
        deletes: BTreeSet<Key>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackend {
    entries: Arc<Mutex<Arc<EntriesState>>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct InMemoryBackendFactory;

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackendFixture {
    entries: Arc<Mutex<Arc<EntriesState>>>,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct InMemoryRead {
    entries: Arc<EntriesState>,
}

#[expect(missing_debug_implementations)]
pub struct InMemoryWrite {
    parent: Arc<Mutex<Arc<EntriesState>>>,
    base: Arc<EntriesState>,
    overlay: EntriesOverlay,
    stats: WriteStats,
}

#[derive(Debug, Default)]
struct EntriesOverlay {
    puts: InMemoryMap,
    deletes: BTreeSet<Key>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "storage-benches")]
    pub fn fork_snapshot(&self) -> Result<Self, BackendError> {
        Ok(Self {
            entries: Arc::new(Mutex::new(self.snapshot()?)),
        })
    }

    fn snapshot(&self) -> Result<Arc<EntriesState>, BackendError> {
        self.entries
            .lock()
            .map_err(|_| BackendError::Io("in-memory backend lock poisoned".to_string()))
            .map(|entries| Arc::clone(&entries))
    }
}

impl BackendFactory for InMemoryBackendFactory {
    type Backend = InMemoryBackend;
    type Fixture = InMemoryBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        InMemoryBackendFixture::default()
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: true,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for InMemoryBackendFixture {
    type Backend = InMemoryBackend;

    async fn open(&self) -> Self::Backend {
        InMemoryBackend {
            entries: Arc::clone(&self.entries),
        }
    }
}

impl Backend for InMemoryBackend {
    type Read<'a>
        = InMemoryRead
    where
        Self: 'a;

    type Write<'a>
        = InMemoryWrite
    where
        Self: 'a;
    async fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(InMemoryRead {
            entries: self.snapshot()?,
        })
    }

    async fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(InMemoryWrite {
            parent: Arc::clone(&self.entries),
            base: self.snapshot()?,
            overlay: EntriesOverlay::default(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for InMemoryRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, BackendError> {
        let values = keys
            .iter()
            .map(|key| {
                self.entries
                    .get(&physical_key(space, key))
                    .map(|value| project_value(value, opts.projection))
            })
            .collect();
        Ok(GetManyResult::new(values))
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, BackendError> {
        let physical = physical_range(space, range);
        let physical_opts = ScanOptions {
            resume_after: opts
                .resume_after
                .as_ref()
                .map(|key| physical_key(space, key)),
            ..opts
        };
        let mut chunk = collect_range_chunk(&self.entries, physical, &physical_opts);
        for entry in &mut chunk.entries {
            entry.key = Key(entry.key.0.slice(4..));
        }
        Ok(chunk)
    }
}

impl BackendWrite for InMemoryWrite {
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let key = physical_key(space, &entry.key);
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            if !self.overlay.deletes.is_empty() {
                self.overlay.deletes.remove(&key);
            }
            self.overlay.puts.insert(key, value);
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            let key = physical_key(space, key);
            if !self.overlay.puts.is_empty() {
                self.overlay.puts.remove(&key);
            }
            self.overlay.deletes.insert(key);
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let range = physical_range(space, range);
        let mut base_keys = Vec::new();
        let mut resume_after = None;
        loop {
            let chunk = collect_range_chunk(
                &self.base,
                range.clone(),
                &ScanOptions {
                    limit_rows: usize::MAX,
                    projection: CoreProjection::KeyOnly,
                    resume_after,
                },
            );
            let next_resume = chunk.entries.last().map(|entry| entry.key.clone());
            base_keys.extend(chunk.entries.into_iter().map(|entry| entry.key));
            if !chunk.has_more {
                break;
            }
            resume_after = next_resume;
        }

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
        self.stats.backend_calls += 1;
        Ok(())
    }

    async fn commit(self) -> Result<CommitResult, BackendError> {
        let mut parent = self
            .parent
            .lock()
            .map_err(|_| BackendError::Io("in-memory backend lock poisoned".to_string()))?;
        let base = if Arc::ptr_eq(&parent, &self.base) {
            self.base
        } else {
            Arc::clone(&parent)
        };
        let entries = if self.overlay.puts.is_empty() && self.overlay.deletes.is_empty() {
            base
        } else if matches!(base.as_ref(), EntriesState::Empty) && self.overlay.deletes.is_empty() {
            Arc::new(EntriesState::Flat(self.overlay.puts))
        } else {
            Arc::new(EntriesState::Layered {
                base,
                puts: self.overlay.puts,
                deletes: self.overlay.deletes,
            })
        };

        *parent = entries;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    async fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

impl EntriesState {
    fn get(&self, key: &Key) -> Option<&Bytes> {
        match self {
            Self::Empty => None,
            Self::Flat(entries) => entries.get(key),
            Self::Layered {
                base,
                puts,
                deletes,
            } => puts.get(key).or_else(|| {
                if deletes.contains(key) {
                    None
                } else {
                    base.get(key)
                }
            }),
        }
    }
}

fn collect_range_chunk(entries: &EntriesState, range: KeyRange, opts: &ScanOptions) -> ScanChunk {
    if opts.page_size() == 0 {
        return ScanChunk {
            entries: Vec::new(),
            has_more: false,
        };
    }

    let lower = lower_bound(&range, opts.resume_after.as_ref());
    let upper = upper_bound(&range);
    if bounds_are_empty(&lower, &upper) {
        return ScanChunk {
            entries: Vec::new(),
            has_more: false,
        };
    }

    collect_entries_range(entries, lower, upper, opts)
}

fn collect_entries_range(
    state: &EntriesState,
    lower: Bound<&Key>,
    upper: Bound<&Key>,
    opts: &ScanOptions,
) -> ScanChunk {
    match state {
        EntriesState::Empty => ScanChunk {
            entries: Vec::new(),
            has_more: false,
        },
        EntriesState::Flat(entries) => collect_flat_range(entries, lower, upper, opts),
        EntriesState::Layered {
            base,
            puts,
            deletes,
        } if !range_has_entries(puts, &lower, &upper)
            && !range_has_keys(deletes, &lower, &upper) =>
        {
            collect_entries_range(base, lower, upper, opts)
        }
        EntriesState::Layered { .. } => {
            let mut rows = BTreeMap::<&Key, Option<&Bytes>>::new();
            collect_layer_rows(state, &lower, &upper, &mut rows);
            materialize_rows(rows, opts)
        }
    }
}

fn range_has_entries(entries: &InMemoryMap, lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    entries.range((*lower, *upper)).next().is_some()
}

fn range_has_keys(keys: &BTreeSet<Key>, lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    keys.range((*lower, *upper)).next().is_some()
}

fn collect_flat_range(
    entries: &InMemoryMap,
    lower: Bound<&Key>,
    upper: Bound<&Key>,
    opts: &ScanOptions,
) -> ScanChunk {
    let mut rows = entries.range((lower, upper));
    let collected = rows
        .by_ref()
        .take(opts.page_size())
        .map(|(key, value)| ReadEntry {
            key: key.clone(),
            value: project_value(value, opts.projection),
        })
        .collect();
    ScanChunk {
        entries: collected,
        has_more: rows.next().is_some(),
    }
}

fn collect_layer_rows<'a>(
    state: &'a EntriesState,
    lower: &Bound<&'a Key>,
    upper: &Bound<&'a Key>,
    rows: &mut BTreeMap<&'a Key, Option<&'a Bytes>>,
) {
    match state {
        EntriesState::Empty => {}
        EntriesState::Flat(entries) => {
            for (key, value) in entries.range((*lower, *upper)) {
                rows.entry(key).or_insert(Some(value));
            }
        }
        EntriesState::Layered {
            base,
            puts,
            deletes,
        } => {
            collect_layer_rows(base, lower, upper, rows);
            for delete in deletes.range((*lower, *upper)) {
                rows.insert(delete, None);
            }
            for (key, value) in puts.range((*lower, *upper)) {
                rows.insert(key, Some(value));
            }
        }
    }
}

fn materialize_rows<'a>(
    rows: BTreeMap<&'a Key, Option<&'a Bytes>>,
    opts: &ScanOptions,
) -> ScanChunk {
    let mut present = rows
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)));
    let entries = present
        .by_ref()
        .take(opts.page_size())
        .map(|(key, value)| ReadEntry {
            key: key.clone(),
            value: project_value(value, opts.projection),
        })
        .collect();
    ScanChunk {
        entries,
        has_more: present.next().is_some(),
    }
}

fn lower_bound<'a>(range: &'a KeyRange, resume_after: Option<&'a Key>) -> Bound<&'a Key> {
    let range_lower = match &range.lower {
        Bound::Included(key) => Some((key, true)),
        Bound::Excluded(key) => Some((key, false)),
        Bound::Unbounded => None,
    };

    match (range_lower, resume_after) {
        (Some((lower, lower_inclusive)), Some(resume_after)) => {
            if resume_after >= lower {
                Bound::Excluded(resume_after)
            } else if lower_inclusive {
                Bound::Included(lower)
            } else {
                Bound::Excluded(lower)
            }
        }
        (Some((lower, true)), None) => Bound::Included(lower),
        (Some((lower, false)), None) => Bound::Excluded(lower),
        (None, Some(resume_after)) => Bound::Excluded(resume_after),
        (None, None) => Bound::Unbounded,
    }
}

fn upper_bound(range: &KeyRange) -> Bound<&Key> {
    match &range.upper {
        Bound::Included(key) => Bound::Included(key),
        Bound::Excluded(key) => Bound::Excluded(key),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn bounds_are_empty(lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    match (lower, upper) {
        (_, Bound::Unbounded) | (Bound::Unbounded, _) => false,
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower >= upper,
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

    use crate::backend::conformance::{ConformanceStatus, run_backend_conformance};
    use crate::backend::{
        Backend, BackendRead, BackendWrite, InMemoryBackend, Key, KeyRange, MAX_SCAN_PAGE_ROWS,
        PutBatch, PutEntry, ReadOptions, ScanOptions, SpaceId, StoredValue, WriteOptions,
    };

    #[tokio::test]
    async fn in_memory_backend_passes_backend_conformance() {
        let report = run_backend_conformance(&crate::backend::InMemoryBackendFactory).await;

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
        let backend = InMemoryBackend::new();
        let space = SpaceId(7);
        let mut write = backend
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

        let mut write = backend
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

        let read = backend
            .begin_read(ReadOptions::default())
            .await
            .expect("begin verification read");
        let chunk = read
            .scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                ScanOptions::default(),
            )
            .await
            .expect("scan after range delete");
        assert!(chunk.entries.is_empty());
        assert!(!chunk.has_more);
    }
}
