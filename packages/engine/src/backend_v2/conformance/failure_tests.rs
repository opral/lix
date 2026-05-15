use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{
    run_backend_conformance, BackendFactory, BackendFixture, BackendTestConfig, ConformanceStatus,
};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue, ProjectedValueRef,
    ProjectionCapabilities, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId,
    StoredValue, WriteConcurrency, WriteOptions, WriteStats,
};

type BrokenMap = BTreeMap<(SpaceId, Key), Bytes>;

#[derive(Clone, Copy, Debug)]
enum BrokenMode {
    GetManyMissesExistingKey,
    ReadSeesLaterCommits,
    ReadSeesSecondLaterCommit,
    ScanReadSeesLaterCommits,
    DeleteManyIgnoresExistingKeys,
    KeyOnlyScanReturnsFullValues,
    AdvertisesPendingCapability,
    RollbackCommits,
    BadByteOrdering,
    KeyResumeRepeatsLastKey,
    LoseCommittedDataOnReopen,
    CorruptOpaqueBytes,
}

#[derive(Clone, Debug)]
struct BrokenBackendFactory {
    mode: BrokenMode,
}

#[derive(Clone, Debug)]
struct BrokenBackendFixture {
    mode: BrokenMode,
    entries: Arc<Mutex<BrokenMap>>,
    commit_count: Arc<Mutex<u64>>,
    open_count: Arc<Mutex<u64>>,
}

#[derive(Clone, Debug)]
struct BrokenBackend {
    mode: BrokenMode,
    entries: Arc<Mutex<BrokenMap>>,
    commit_count: Arc<Mutex<u64>>,
}

struct BrokenRead {
    mode: BrokenMode,
    parent: Arc<Mutex<BrokenMap>>,
    commit_count: Arc<Mutex<u64>>,
    snapshot_commit_count: u64,
    snapshot: BrokenMap,
}

struct BrokenWrite {
    mode: BrokenMode,
    parent: Arc<Mutex<BrokenMap>>,
    commit_count: Arc<Mutex<u64>>,
    staged: BrokenMap,
}

#[test]
fn detects_get_many_missing_existing_key_violation() {
    assert_failed(
        BrokenMode::GetManyMissesExistingKey,
        "baseline::get_many_returns_requested_slots",
    );
}

#[test]
fn detects_read_snapshot_violation() {
    assert_failed(
        BrokenMode::ReadSeesLaterCommits,
        "baseline::begin_read_pins_coherent_view",
    );
}

#[test]
fn detects_read_snapshot_second_commit_violation() {
    assert_failed(
        BrokenMode::ReadSeesSecondLaterCommit,
        "baseline::begin_read_pins_coherent_view",
    );
}

#[test]
fn detects_scan_read_snapshot_violation() {
    assert_failed(
        BrokenMode::ScanReadSeesLaterCommits,
        "baseline::begin_read_pins_coherent_view",
    );
}

#[test]
fn detects_delete_many_ignores_existing_keys() {
    assert_failed(
        BrokenMode::DeleteManyIgnoresExistingKeys,
        "baseline::delete_many_removes_existing_keys",
    );
}

#[test]
fn detects_key_only_scan_projection_violation() {
    assert_failed(
        BrokenMode::KeyOnlyScanReturnsFullValues,
        "baseline::full_value_and_key_only_are_core",
    );
}

#[test]
fn detects_advertised_pending_capability() {
    let report = run_backend_conformance(&BrokenBackendFactory {
        mode: BrokenMode::AdvertisesPendingCapability,
    });
    let pending = report.tests.iter().any(|test| {
        test.name == "projection::header_returns_header_without_payload"
            && matches!(test.status, ConformanceStatus::Pending)
    });
    assert!(
        pending,
        "expected advertised projection capability to create pending test, got {report:#?}"
    );

    let panic = std::panic::catch_unwind(|| report.assert_no_failures());
    assert!(
        panic.is_err(),
        "assert_no_failures should fail when advertised capabilities are pending"
    );
}

#[test]
fn detects_rollback_commits_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "baseline::rollback_discards_staged_mutations",
    );
}

#[test]
fn detects_rollback_overwrite_delete_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "baseline::rollback_discards_overwrite_and_delete",
    );
}

#[test]
fn detects_bad_byte_ordering_violation() {
    assert_failed(
        BrokenMode::BadByteOrdering,
        "baseline::scan_range_orders_raw_byte_keys",
    );
}

#[test]
fn detects_multi_page_drain_repeat_violation() {
    assert_failed(
        BrokenMode::KeyResumeRepeatsLastKey,
        "baseline::scan_range_drains_multi_page_limits",
    );
}

#[test]
fn detects_opaque_byte_corruption_violation() {
    assert_failed(
        BrokenMode::CorruptOpaqueBytes,
        "baseline::full_value_preserves_opaque_bytes",
    );
}

#[test]
fn detects_persistent_commit_lost_on_reopen_violation() {
    assert_failed(
        BrokenMode::LoseCommittedDataOnReopen,
        "persistence::committed_data_survives_reopen",
    );
}

#[test]
fn detects_persistent_rollback_on_reopen_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "persistence::rolled_back_data_does_not_survive_reopen",
    );
}

fn assert_failed(mode: BrokenMode, test_name: &'static str) {
    let report = run_backend_conformance(&BrokenBackendFactory { mode });
    let failed = report
        .tests
        .iter()
        .any(|test| test.name == test_name && matches!(test.status, ConformanceStatus::Failed(_)));
    assert!(
        failed,
        "expected {test_name} to fail for {mode:?}, got {:#?}",
        report
    );
}

impl BackendFactory for BrokenBackendFactory {
    type Backend = BrokenBackend;
    type Fixture = BrokenBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        BrokenBackendFixture {
            mode: self.mode,
            entries: Arc::new(Mutex::new(BrokenMap::new())),
            commit_count: Arc::new(Mutex::new(0)),
            open_count: Arc::new(Mutex::new(0)),
        }
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig::default()
    }
}

impl BackendFixture for BrokenBackendFixture {
    type Backend = BrokenBackend;

    fn open(&self) -> Self::Backend {
        let mut open_count = self
            .open_count
            .lock()
            .expect("broken backend open count lock poisoned");
        if matches!(self.mode, BrokenMode::LoseCommittedDataOnReopen) && *open_count > 0 {
            self.entries
                .lock()
                .expect("broken backend entries lock poisoned")
                .clear();
        }
        *open_count += 1;
        BrokenBackend {
            mode: self.mode,
            entries: Arc::clone(&self.entries),
            commit_count: Arc::clone(&self.commit_count),
        }
    }
}

impl Backend for BrokenBackend {
    type Read<'a>
        = BrokenRead
    where
        Self: 'a;

    type Write<'a>
        = BrokenWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        let mut capabilities = BackendCapabilities::v0(WriteConcurrency::SingleWriter);
        if matches!(self.mode, BrokenMode::AdvertisesPendingCapability) {
            capabilities.projection = ProjectionCapabilities {
                header: true,
                ..ProjectionCapabilities::default()
            };
        }
        capabilities
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(BrokenRead {
            mode: self.mode,
            parent: Arc::clone(&self.entries),
            commit_count: Arc::clone(&self.commit_count),
            snapshot_commit_count: *self
                .commit_count
                .lock()
                .map_err(|_| BackendError::Io("broken backend commit lock poisoned".to_string()))?,
            snapshot: self.snapshot()?,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(BrokenWrite {
            mode: self.mode,
            parent: Arc::clone(&self.entries),
            commit_count: Arc::clone(&self.commit_count),
            staged: self.snapshot()?,
        })
    }
}

impl BackendRead for BrokenRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        let live_entries;
        let current_commit_count = *self
            .commit_count
            .lock()
            .map_err(|_| BackendError::Io("broken backend commit lock poisoned".to_string()))?;
        let entries = if matches!(self.mode, BrokenMode::ReadSeesLaterCommits)
            || (matches!(self.mode, BrokenMode::ReadSeesSecondLaterCommit)
                && current_commit_count >= self.snapshot_commit_count + 2)
        {
            live_entries = self
                .parent
                .lock()
                .map_err(|_| BackendError::Io("broken backend lock poisoned".to_string()))?
                .clone();
            &live_entries
        } else {
            &self.snapshot
        };
        get_many_from_map(entries, self.mode, space, keys, opts)
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
        let live_entries;
        let entries = if matches!(self.mode, BrokenMode::ScanReadSeesLaterCommits) {
            live_entries = self
                .parent
                .lock()
                .map_err(|_| BackendError::Io("broken backend lock poisoned".to_string()))?
                .clone();
            &live_entries
        } else {
            &self.snapshot
        };
        visit_range_from_map(entries, self.mode, space, range, opts, visitor)
    }
}

impl BackendWrite for BrokenWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            self.staged
                .insert((space, entry.key), stored_value_bytes(entry.value));
        }
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            if matches!(self.mode, BrokenMode::DeleteManyIgnoresExistingKeys)
                && self.staged.contains_key(&(space, key.clone()))
            {
                continue;
            }
            self.staged.remove(&(space, key.clone()));
        }
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        *self
            .parent
            .lock()
            .map_err(|_| BackendError::Io("broken backend lock poisoned".to_string()))? =
            self.staged;
        *self
            .commit_count
            .lock()
            .map_err(|_| BackendError::Io("broken backend commit lock poisoned".to_string()))? += 1;
        Ok(CommitResult {
            commit_id: None,
            stats: WriteStats::default(),
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        if matches!(self.mode, BrokenMode::RollbackCommits) {
            *self
                .parent
                .lock()
                .map_err(|_| BackendError::Io("broken backend lock poisoned".to_string()))? =
                self.staged;
            *self.commit_count.lock().map_err(|_| {
                BackendError::Io("broken backend commit lock poisoned".to_string())
            })? += 1;
        }
        Ok(())
    }
}

impl BrokenBackend {
    fn snapshot(&self) -> Result<BrokenMap, BackendError> {
        self.entries
            .lock()
            .map_err(|_| BackendError::Io("broken backend lock poisoned".to_string()))
            .map(|entries| entries.clone())
    }
}

fn get_many_from_map(
    entries: &BrokenMap,
    mode: BrokenMode,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<GetManyResult, BackendError> {
    let mut seen = BTreeSet::new();
    let mut values = Vec::new();
    for key in keys {
        if matches!(mode, BrokenMode::GetManyMissesExistingKey)
            && key == &Key(Bytes::from_static(b"a"))
        {
            values.push(None);
            continue;
        }
        if !seen.insert(key.clone()) {
            if let Some(value) = entries.get(&(space, key.clone())) {
                values.push(Some(project_value(value, mode, opts.projection, false)));
            } else {
                values.push(None);
            }
            continue;
        }
        values.push(
            entries
                .get(&(space, key.clone()))
                .map(|value| project_value(value, mode, opts.projection, false)),
        );
    }
    Ok(GetManyResult::new(values))
}

fn visit_range_from_map<V>(
    entries: &BrokenMap,
    mode: BrokenMode,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
{
    let mut emitted = 0;
    let mut has_more = false;
    let mut candidates = entries
        .iter()
        .filter(|((entry_space, key), _)| *entry_space == space && range_contains(&range, key))
        .collect::<Vec<_>>();
    if matches!(mode, BrokenMode::BadByteOrdering) {
        candidates.sort_by(|left, right| {
            left.0
                 .1
                 .0
                .len()
                .cmp(&right.0 .1 .0.len())
                .then(left.0 .1.cmp(&right.0 .1))
        });
    }

    for ((_, key), value) in candidates {
        if opts.resume_after.is_some_and(|resume_after| {
            if matches!(mode, BrokenMode::KeyResumeRepeatsLastKey) {
                key < resume_after
            } else {
                key <= resume_after
            }
        }) {
            continue;
        }
        if emitted == opts.limit_rows {
            has_more = true;
            break;
        }
        visitor.visit(key, project_value_ref(value, mode, opts.projection, true))?;
        emitted += 1;
    }

    Ok(ScanResult { emitted, has_more })
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

fn project_value(
    value: &Bytes,
    mode: BrokenMode,
    projection: CoreProjection,
    break_key_only: bool,
) -> ProjectedValue {
    let value = if matches!(mode, BrokenMode::CorruptOpaqueBytes) {
        Bytes::from(
            value
                .iter()
                .copied()
                .filter(|byte| byte.is_ascii_graphic() || *byte == b' ')
                .collect::<Vec<_>>(),
        )
    } else {
        value.clone()
    };
    match projection {
        CoreProjection::KeyOnly
            if break_key_only && matches!(mode, BrokenMode::KeyOnlyScanReturnsFullValues) =>
        {
            ProjectedValue::FullValue(value)
        }
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value),
    }
}

fn project_value_ref(
    value: &Bytes,
    mode: BrokenMode,
    projection: CoreProjection,
    break_key_only: bool,
) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly
            if break_key_only && matches!(mode, BrokenMode::KeyOnlyScanReturnsFullValues) =>
        {
            ProjectedValueRef::FullValue(value)
        }
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}
