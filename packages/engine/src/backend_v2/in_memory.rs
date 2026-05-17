use std::collections::btree_map;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::Peekable;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend_v2::conformance::{BackendFactory, BackendFixture, BackendTestConfig};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendScanCursor, BackendWrite,
    BufferedScanCursor, CommitResult, CoreProjection, GetOptions, Key, KeyRange, KeyRef,
    PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor,
    StoredValue, WriteConcurrency, WriteOptions, WriteStats,
};

type InMemoryMap = BTreeMap<Key, Bytes>;

#[derive(Clone, Debug, Default)]
enum EntriesState {
    #[default]
    Empty,
    Flat(InMemoryMap),
    Layered {
        base: Arc<EntriesState>,
        puts: InMemoryMap,
        deletes: BTreeSet<Key>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackend {
    entries: Arc<Mutex<Arc<EntriesState>>>,
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackendFactory;

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackendFixture {
    entries: Arc<Mutex<Arc<EntriesState>>>,
}

pub struct InMemoryRead {
    entries: Arc<EntriesState>,
}

pub enum InMemoryScanCursor<'a> {
    Flat {
        iter: Peekable<btree_map::Range<'a, Key, Bytes>>,
        projection: CoreProjection,
    },
    Buffered(BufferedScanCursor),
}

pub type InMemoryScanVisitResult = ScanResult;

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

    fn open(&self) -> Self::Backend {
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

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(InMemoryRead {
            entries: self.snapshot()?,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(InMemoryWrite {
            parent: Arc::clone(&self.entries),
            base: self.snapshot()?,
            overlay: EntriesOverlay::default(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for InMemoryRead {
    fn visit_many<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        match self.entries.as_ref() {
            EntriesState::Flat(entries) => {
                for (index, key) in keys.iter().enumerate() {
                    let value = entries
                        .get(key)
                        .map(|value| project_value_ref(value, opts.projection));
                    visitor.visit(index, key, value)?;
                }
            }
            entries => {
                for (index, key) in keys.iter().enumerate() {
                    let value = entries
                        .get(key)
                        .map(|value| project_value_ref(value, opts.projection));
                    visitor.visit(index, key, value)?;
                }
            }
        }
        Ok(())
    }

    fn with_scan_cursor<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut dyn BackendScanCursor) -> Result<T, BackendError>,
    {
        if opts.limit_rows == 0 {
            let mut cursor = InMemoryScanCursor::Buffered(BufferedScanCursor::default());
            return f(&mut cursor);
        }

        let lower = lower_bound(&range, opts.resume_after);
        let upper = upper_bound(&range);
        if bounds_are_empty(&lower, &upper) {
            let mut cursor = InMemoryScanCursor::Buffered(BufferedScanCursor::default());
            return f(&mut cursor);
        }

        match self.entries.as_ref() {
            EntriesState::Flat(entries) => {
                let mut cursor = InMemoryScanCursor::Flat {
                    iter: entries.range((lower, upper)).peekable(),
                    projection: opts.projection,
                };
                f(&mut cursor)
            }
            entries => {
                let mut rows = Vec::new();
                visit_range(
                    entries,
                    range,
                    ScanOptions {
                        limit_rows: usize::MAX,
                        ..opts
                    },
                    &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                        rows.push(crate::backend_v2::ReadEntry {
                            key: key.to_owned_key(),
                            value: value.to_owned(),
                        });
                        Ok(())
                    },
                )?;
                let mut cursor = InMemoryScanCursor::Buffered(BufferedScanCursor::new(rows));
                f(&mut cursor)
            }
        }
    }
}

impl BackendScanCursor for InMemoryScanCursor<'_> {
    fn visit_next(
        &mut self,
        limit_rows: usize,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<ScanResult, BackendError> {
        match self {
            InMemoryScanCursor::Buffered(cursor) => cursor.visit_next(limit_rows, visitor),
            InMemoryScanCursor::Flat { iter, projection } => {
                if limit_rows == 0 {
                    return Ok(ScanResult {
                        emitted: 0,
                        has_more: iter.peek().is_some(),
                    });
                }

                let mut emitted = 0;
                while emitted < limit_rows {
                    let Some((key, value)) = iter.next() else {
                        return Ok(ScanResult {
                            emitted,
                            has_more: false,
                        });
                    };
                    visitor.visit(key.as_ref(), project_value_ref(value, *projection))?;
                    emitted += 1;
                }

                Ok(ScanResult {
                    emitted,
                    has_more: iter.peek().is_some(),
                })
            }
        }
    }
}

impl InMemoryRead {
    pub fn visit_scan_range<F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        mut visitor: F,
    ) -> Result<InMemoryScanVisitResult, BackendError>
    where
        F: FnMut(KeyRef<'_>, Option<&[u8]>),
    {
        let mut visitor = |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            let value = match value {
                ProjectedValueRef::KeyOnly => None,
                ProjectedValueRef::FullValue(value) => Some(value),
            };
            visitor(key, value);
            Ok(())
        };
        visit_range(&self.entries, range, opts, &mut visitor)
    }
}

impl BackendWrite for InMemoryWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            if !self.overlay.deletes.is_empty() {
                self.overlay.deletes.remove(&entry.key);
            }
            self.overlay.puts.insert(entry.key, value);
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            if !self.overlay.puts.is_empty() {
                self.overlay.puts.remove(key);
            }
            self.overlay.deletes.insert(key.clone());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        let mut base_keys = Vec::new();
        visit_range(
            &self.base,
            range.clone(),
            ScanOptions {
                limit_rows: usize::MAX,
                projection: CoreProjection::KeyOnly,
                resume_after: None,
            },
            &mut |key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                base_keys.push(key.to_owned_key());
                Ok(())
            },
        )?;

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

    fn commit(self) -> Result<CommitResult, BackendError> {
        let entries = if self.overlay.puts.is_empty() && self.overlay.deletes.is_empty() {
            self.base
        } else if matches!(self.base.as_ref(), EntriesState::Empty)
            && self.overlay.deletes.is_empty()
        {
            Arc::new(EntriesState::Flat(self.overlay.puts))
        } else {
            Arc::new(EntriesState::Layered {
                base: self.base,
                puts: self.overlay.puts,
                deletes: self.overlay.deletes,
            })
        };

        *self
            .parent
            .lock()
            .map_err(|_| BackendError::Io("in-memory backend lock poisoned".to_string()))? =
            entries;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

impl EntriesState {
    fn get(&self, key: &Key) -> Option<&Bytes> {
        match self {
            EntriesState::Empty => None,
            EntriesState::Flat(entries) => entries.get(key),
            EntriesState::Layered {
                base,
                puts,
                deletes,
            } => {
                if let Some(value) = puts.get(key) {
                    Some(value)
                } else if deletes.contains(key) {
                    None
                } else {
                    base.get(key)
                }
            }
        }
    }
}

fn visit_range<V>(
    entries: &EntriesState,
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

    let lower = lower_bound(&range, opts.resume_after);
    let upper = upper_bound(&range);
    if bounds_are_empty(&lower, &upper) {
        return Ok(ScanResult::default());
    }

    visit_entries_range(entries, lower, upper, opts, visitor)
}

fn visit_entries_range<V>(
    state: &EntriesState,
    lower: Bound<&Key>,
    upper: Bound<&Key>,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
{
    match state {
        EntriesState::Empty => Ok(ScanResult::default()),
        EntriesState::Flat(entries) => visit_flat_range(entries, lower, upper, opts, visitor),
        EntriesState::Layered {
            base,
            puts,
            deletes,
        } if !range_has_entries(puts, &lower, &upper)
            && !range_has_keys(deletes, &lower, &upper) =>
        {
            visit_entries_range(base, lower, upper, opts, visitor)
        }
        EntriesState::Layered { .. } => {
            let mut rows = BTreeMap::<&Key, Option<&Bytes>>::new();
            collect_range(state, &lower, &upper, &mut rows);

            match opts.projection {
                CoreProjection::KeyOnly => visit_rows(rows, opts.limit_rows, visitor, |_, _| {
                    ProjectedValueRef::KeyOnly
                }),
                CoreProjection::FullValue => {
                    visit_rows(rows, opts.limit_rows, visitor, |_, value| {
                        ProjectedValueRef::FullValue(value.as_ref())
                    })
                }
            }
        }
    }
}

fn range_has_entries(entries: &InMemoryMap, lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    entries.range((*lower, *upper)).next().is_some()
}

fn range_has_keys(keys: &BTreeSet<Key>, lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    keys.range((*lower, *upper)).next().is_some()
}

fn visit_flat_range<V>(
    entries: &InMemoryMap,
    lower: Bound<&Key>,
    upper: Bound<&Key>,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
{
    let mut emitted = 0;
    let mut has_more = false;

    match opts.projection {
        CoreProjection::KeyOnly => {
            for (key, _) in entries.range((lower, upper)) {
                if emitted == opts.limit_rows {
                    has_more = true;
                    break;
                }
                visitor.visit(key.as_ref(), ProjectedValueRef::KeyOnly)?;
                emitted += 1;
            }
        }
        CoreProjection::FullValue => {
            for (key, value) in entries.range((lower, upper)) {
                if emitted == opts.limit_rows {
                    has_more = true;
                    break;
                }
                visitor.visit(key.as_ref(), ProjectedValueRef::FullValue(value.as_ref()))?;
                emitted += 1;
            }
        }
    }

    Ok(ScanResult { emitted, has_more })
}

fn collect_range<'a>(
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
            for delete in deletes.range((*lower, *upper)) {
                rows.insert(delete, None);
            }
            for (key, value) in puts.range((*lower, *upper)) {
                rows.insert(key, Some(value));
            }
            collect_range(base, lower, upper, rows);
        }
    }
}

fn visit_rows<'a, V, F>(
    rows: BTreeMap<&'a Key, Option<&'a Bytes>>,
    limit_rows: usize,
    visitor: &mut V,
    project: F,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
    F: Fn(&'a Key, &'a Bytes) -> ProjectedValueRef<'a>,
{
    let mut emitted = 0;
    let mut has_more = false;
    for (key, value) in rows {
        let Some(value) = value else {
            continue;
        };
        if emitted == limit_rows {
            has_more = true;
            break;
        }
        visitor.visit(key.as_ref(), project(key, value))?;
        emitted += 1;
    }
    Ok(ScanResult { emitted, has_more })
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
        (Bound::Included(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper))
        | (Bound::Excluded(lower), Bound::Excluded(upper)) => lower >= upper,
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

fn project_value_ref(value: &Bytes, projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value.as_ref()),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

#[cfg(test)]
mod tests {
    use crate::backend_v2::conformance::{run_backend_conformance, ConformanceStatus};
    use crate::backend_v2::InMemoryBackendFactory;

    #[test]
    fn in_memory_backend_passes_backend_v2_conformance() {
        let report = run_backend_conformance(&InMemoryBackendFactory);

        report.assert_no_failures();

        assert!(
            report
                .tests
                .iter()
                .any(|test| matches!(test.status, ConformanceStatus::Passed)),
            "expected at least one conformance test to run"
        );
    }
}
