use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend::conformance::{BackendFactory, BackendFixture, BackendTestConfig};
use crate::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetOptions,
    Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions,
    ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions, WriteStats,
};

type InMemoryMap = BTreeMap<Key, Bytes>;

/// The in-memory backend has no native namespaces; it scopes keys to spaces
/// by prefixing the 4-byte big-endian space id internally. The prefix never
/// crosses the trait boundary: visitors observe logical keys.
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

pub type InMemoryScanVisitResult = ScanResult;

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
    fn visit_keys<V>(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let entries = self.entries.as_ref();
        for (index, key) in keys.iter().enumerate() {
            let value = entries
                .get(&physical_key(space, key))
                .map(|value| project_value_ref(value, opts.projection));
            visitor.visit(index, key, value)?;
        }
        Ok(())
    }

    fn scan<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        let physical = physical_range(space, range);
        let resume_after = opts.resume_after.map(|key| physical_key(space, key));
        let physical_opts = ScanOptions {
            resume_after: resume_after.as_ref(),
            ..opts
        };
        // Visitors observe logical keys; strip the internal prefix.
        let mut strip = |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            visitor.visit(KeyRef(&key.0[4..]), value)
        };
        visit_range(&self.entries, physical, physical_opts, &mut strip)
    }
}

impl BackendWrite for InMemoryWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
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

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
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

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let range = physical_range(space, range);
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

    fn rollback(self) -> Result<(), BackendError> {
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
            collect_range(base, lower, upper, rows);
            for delete in deletes.range((*lower, *upper)) {
                rows.insert(delete, None);
            }
            for (key, value) in puts.range((*lower, *upper)) {
                rows.insert(key, Some(value));
            }
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
    use crate::backend::conformance::{ConformanceStatus, run_backend_conformance};

    #[test]
    fn in_memory_backend_passes_backend_conformance() {
        let report = run_backend_conformance(&crate::backend::InMemoryBackendFactory);

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
