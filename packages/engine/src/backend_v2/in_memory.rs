use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend_v2::conformance::{BackendFactory, BackendFixture, BackendTestConfig};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetManyResult, GetOptions, Key, KeyRange, PointVisitor, ProjectedValue,
    ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId,
    StoredValue, WriteConcurrency, WriteOptions, WriteStats,
};

type SpaceEntries = BTreeMap<Key, Bytes>;
type InMemoryMap = BTreeMap<SpaceId, Arc<SpaceState>>;

#[derive(Clone, Debug, Default)]
enum SpaceState {
    #[default]
    Empty,
    Flat(SpaceEntries),
    Layered {
        base: Arc<SpaceState>,
        puts: SpaceEntries,
        deletes: BTreeSet<Key>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackend {
    entries: Arc<Mutex<Arc<InMemoryMap>>>,
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackendFactory;

#[derive(Clone, Debug, Default)]
pub struct InMemoryBackendFixture {
    entries: Arc<Mutex<Arc<InMemoryMap>>>,
}

pub struct InMemoryRead {
    entries: Arc<InMemoryMap>,
}

pub type InMemoryScanVisitResult = ScanResult;

pub struct InMemoryWrite {
    parent: Arc<Mutex<Arc<InMemoryMap>>>,
    base: Arc<InMemoryMap>,
    overlays: BTreeMap<SpaceId, SpaceOverlay>,
    stats: WriteStats,
}

#[derive(Debug, Default)]
struct SpaceOverlay {
    puts: SpaceEntries,
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

    fn snapshot(&self) -> Result<Arc<InMemoryMap>, BackendError> {
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
            overlays: BTreeMap::new(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for InMemoryRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        Ok(GetManyResult::new(
            self.entries
                .get(&space)
                .map(|space_entries| match space_entries.as_ref() {
                    SpaceState::Flat(entries) => keys
                        .iter()
                        .map(|key| {
                            entries
                                .get(key)
                                .map(|value| project_value(value, opts.projection))
                        })
                        .collect(),
                    _ => keys
                        .iter()
                        .map(|key| {
                            space_entries
                                .get(key)
                                .map(|value| project_value(value, opts.projection))
                        })
                        .collect(),
                })
                .unwrap_or_else(|| vec![None; keys.len()]),
        ))
    }

    fn visit_many<V>(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        if let Some(space_entries) = self.entries.get(&space) {
            match space_entries.as_ref() {
                SpaceState::Flat(entries) => {
                    for (index, key) in keys.iter().enumerate() {
                        let value = entries
                            .get(key)
                            .map(|value| project_value_ref(value, opts.projection));
                        visitor.visit(index, key, value)?;
                    }
                }
                _ => {
                    for (index, key) in keys.iter().enumerate() {
                        let value = space_entries
                            .get(key)
                            .map(|value| project_value_ref(value, opts.projection));
                        visitor.visit(index, key, value)?;
                    }
                }
            }
        } else {
            for (index, key) in keys.iter().enumerate() {
                visitor.visit(index, key, None)?;
            }
        }
        Ok(())
    }

    fn visit_range<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_range(&self.entries, space, range, opts, visitor)
    }
}

impl InMemoryRead {
    pub fn visit_scan_range<F>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        mut visitor: F,
    ) -> Result<InMemoryScanVisitResult, BackendError>
    where
        F: FnMut(&Key, Option<&Bytes>),
    {
        let mut visitor = |key: &Key, value: ProjectedValueRef<'_>| {
            let value = match value {
                ProjectedValueRef::KeyOnly => None,
                ProjectedValueRef::FullValue(value) => Some(value),
            };
            visitor(key, value);
            Ok(())
        };
        visit_range(&self.entries, space, range, opts, &mut visitor)
    }
}

impl BackendWrite for InMemoryWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        let overlay = self.overlays.entry(space).or_default();

        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            if !overlay.deletes.is_empty() {
                overlay.deletes.remove(&entry.key);
            }
            overlay.puts.insert(entry.key, value);
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        let overlay = self.overlays.entry(space).or_default();
        for key in keys {
            if !overlay.puts.is_empty() {
                overlay.puts.remove(key);
            }
            overlay.deletes.insert(key.clone());
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        let mut entries = self.base.as_ref().clone();
        for (space, overlay) in self.overlays {
            if overlay.puts.is_empty() && overlay.deletes.is_empty() {
                continue;
            }
            let base = entries
                .remove(&space)
                .unwrap_or_else(|| Arc::new(SpaceState::Empty));
            if matches!(base.as_ref(), SpaceState::Empty) {
                if !overlay.puts.is_empty() {
                    entries.insert(space, Arc::new(SpaceState::Flat(overlay.puts)));
                }
            } else {
                entries.insert(
                    space,
                    Arc::new(SpaceState::Layered {
                        base,
                        puts: overlay.puts,
                        deletes: overlay.deletes,
                    }),
                );
            }
        }

        *self
            .parent
            .lock()
            .map_err(|_| BackendError::Io("in-memory backend lock poisoned".to_string()))? =
            Arc::new(entries);
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

impl SpaceState {
    fn get(&self, key: &Key) -> Option<&Bytes> {
        match self {
            SpaceState::Empty => None,
            SpaceState::Flat(entries) => entries.get(key),
            SpaceState::Layered {
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
    entries: &InMemoryMap,
    space: SpaceId,
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

    let Some(space_entries) = entries.get(&space) else {
        return Ok(ScanResult::default());
    };

    let lower = lower_bound(&range, opts.resume_after);
    let upper = upper_bound(&range);
    if bounds_are_empty(&lower, &upper) {
        return Ok(ScanResult::default());
    }

    visit_space_range(space_entries, lower, upper, opts, visitor)
}

fn visit_space_range<V>(
    state: &SpaceState,
    lower: Bound<&Key>,
    upper: Bound<&Key>,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
{
    match state {
        SpaceState::Empty => Ok(ScanResult::default()),
        SpaceState::Flat(entries) => visit_flat_range(entries, lower, upper, opts, visitor),
        SpaceState::Layered {
            base,
            puts,
            deletes,
        } if !range_has_entries(puts, &lower, &upper)
            && !range_has_keys(deletes, &lower, &upper) =>
        {
            visit_space_range(base, lower, upper, opts, visitor)
        }
        SpaceState::Layered { .. } => {
            let mut rows = BTreeMap::<&Key, Option<&Bytes>>::new();
            collect_range(state, &lower, &upper, &mut rows);

            match opts.projection {
                CoreProjection::KeyOnly => visit_rows(rows, opts.limit_rows, visitor, |_, _| {
                    ProjectedValueRef::KeyOnly
                }),
                CoreProjection::FullValue => {
                    visit_rows(rows, opts.limit_rows, visitor, |_, value| {
                        ProjectedValueRef::FullValue(value)
                    })
                }
            }
        }
    }
}

fn range_has_entries(entries: &SpaceEntries, lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    entries.range((*lower, *upper)).next().is_some()
}

fn range_has_keys(keys: &BTreeSet<Key>, lower: &Bound<&Key>, upper: &Bound<&Key>) -> bool {
    keys.range((*lower, *upper)).next().is_some()
}

fn visit_flat_range<V>(
    entries: &SpaceEntries,
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
                visitor.visit(key, ProjectedValueRef::KeyOnly)?;
                emitted += 1;
            }
        }
        CoreProjection::FullValue => {
            for (key, value) in entries.range((lower, upper)) {
                if emitted == opts.limit_rows {
                    has_more = true;
                    break;
                }
                visitor.visit(key, ProjectedValueRef::FullValue(value))?;
                emitted += 1;
            }
        }
    }

    Ok(ScanResult { emitted, has_more })
}

fn collect_range<'a>(
    state: &'a SpaceState,
    lower: &Bound<&'a Key>,
    upper: &Bound<&'a Key>,
    rows: &mut BTreeMap<&'a Key, Option<&'a Bytes>>,
) {
    match state {
        SpaceState::Empty => {}
        SpaceState::Flat(entries) => {
            for (key, value) in entries.range((*lower, *upper)) {
                rows.entry(key).or_insert(Some(value));
            }
        }
        SpaceState::Layered {
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
        visitor.visit(key, project(key, value))?;
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

fn project_value(value: &Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value.clone()),
    }
}

fn project_value_ref(value: &Bytes, projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
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
