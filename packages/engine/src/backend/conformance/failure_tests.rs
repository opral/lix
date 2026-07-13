#![allow(
    clippy::manual_async_fn,
    reason = "failure backends mirror explicit Send future signatures from backend traits"
)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{
    BackendFactory, BackendFixture, BackendTestConfig, ConformanceStatus, run_backend_conformance,
};
use crate::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetManyResult,
    GetOptions, Key, KeyRange, ProjectedValue, PutBatch, ReadEntry, ReadOptions, ScanChunk,
    ScanOptions, SpaceId, StoredValue, WriteOptions, WriteStats,
};

type BrokenMap = BTreeMap<Key, Bytes>;

#[derive(Clone, Copy, Debug)]
enum BrokenMode {
    GetManyMissesExistingKey,
    ReadSeesLaterCommits,
    ReadSeesSecondLaterCommit,
    ScanReadSeesLaterCommits,
    DeleteManyIgnoresExistingKeys,
    DeleteRangeIgnoresUpperBound,
    KeyOnlyScanReturnsFullValues,
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

#[derive(Clone)]
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

#[tokio::test]
async fn detects_get_many_missing_existing_key_violation() {
    assert_failed(
        BrokenMode::GetManyMissesExistingKey,
        "baseline::get_many_returns_requested_slots",
    )
    .await;
}

#[tokio::test]
async fn detects_read_snapshot_violation() {
    assert_failed(
        BrokenMode::ReadSeesLaterCommits,
        "baseline::begin_read_pins_coherent_view",
    )
    .await;
}

#[tokio::test]
async fn detects_read_snapshot_second_commit_violation() {
    assert_failed(
        BrokenMode::ReadSeesSecondLaterCommit,
        "baseline::begin_read_pins_coherent_view",
    )
    .await;
}

#[tokio::test]
async fn detects_scan_read_snapshot_violation() {
    assert_failed(
        BrokenMode::ScanReadSeesLaterCommits,
        "baseline::begin_read_pins_coherent_view",
    )
    .await;
}

#[tokio::test]
async fn detects_delete_many_ignores_existing_keys() {
    assert_failed(
        BrokenMode::DeleteManyIgnoresExistingKeys,
        "baseline::delete_many_removes_existing_keys",
    )
    .await;
}

#[tokio::test]
async fn detects_delete_range_ignores_upper_bound() {
    assert_failed(
        BrokenMode::DeleteRangeIgnoresUpperBound,
        "baseline::delete_range_removes_exact_range",
    )
    .await;
}

#[tokio::test]
async fn detects_key_only_scan_projection_violation() {
    assert_failed(
        BrokenMode::KeyOnlyScanReturnsFullValues,
        "baseline::full_value_and_key_only_are_core",
    )
    .await;
}

#[tokio::test]
async fn detects_rollback_commits_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "baseline::rollback_discards_staged_mutations",
    )
    .await;
}

#[tokio::test]
async fn detects_rollback_overwrite_delete_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "baseline::rollback_discards_overwrite_and_delete",
    )
    .await;
}

#[tokio::test]
async fn detects_bad_byte_ordering_violation() {
    assert_failed(
        BrokenMode::BadByteOrdering,
        "baseline::scan_range_orders_raw_byte_keys",
    )
    .await;
}

#[tokio::test]
async fn detects_multi_chunk_drain_repeat_violation() {
    assert_failed(
        BrokenMode::KeyResumeRepeatsLastKey,
        "baseline::scan_range_drains_multi_chunk_limits",
    )
    .await;
}

#[tokio::test]
async fn detects_opaque_byte_corruption_violation() {
    assert_failed(
        BrokenMode::CorruptOpaqueBytes,
        "baseline::full_value_preserves_opaque_bytes",
    )
    .await;
}

#[tokio::test]
async fn detects_persistent_commit_lost_on_reopen_violation() {
    assert_failed(
        BrokenMode::LoseCommittedDataOnReopen,
        "persistence::committed_data_survives_reopen",
    )
    .await;
}

#[tokio::test]
async fn detects_persistent_rollback_on_reopen_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "persistence::rolled_back_data_does_not_survive_reopen",
    )
    .await;
}

#[expect(clippy::uninlined_format_args)]
async fn assert_failed(mode: BrokenMode, test_name: &'static str) {
    let report = run_backend_conformance(&BrokenBackendFactory { mode }).await;
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

    fn open(&self) -> impl Future<Output = Self::Backend> + Send {
        async move {
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

    fn begin_read(
        &self,
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, BackendError>> + Send {
        async move {
            Ok(BrokenRead {
                mode: self.mode,
                parent: Arc::clone(&self.entries),
                commit_count: Arc::clone(&self.commit_count),
                snapshot_commit_count: *self.commit_count.lock().map_err(|_| {
                    BackendError::Io("broken backend commit lock poisoned".to_string())
                })?,
                snapshot: self.snapshot()?,
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, BackendError>> + Send {
        async move {
            Ok(BrokenWrite {
                mode: self.mode,
                parent: Arc::clone(&self.entries),
                commit_count: Arc::clone(&self.commit_count),
                staged: self.snapshot()?,
            })
        }
    }
}

fn broken_physical_key(space: SpaceId, key: &Key) -> Key {
    let mut bytes = Vec::with_capacity(4 + key.0.len());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Key(Bytes::from(bytes))
}

fn broken_physical_range(space: SpaceId, range: KeyRange) -> KeyRange {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| match bound {
        Bound::Included(key) => Bound::Included(broken_physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(broken_physical_key(space, &key)),
        Bound::Unbounded => unbounded,
    };
    KeyRange {
        lower: map(
            range.lower,
            Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))),
        ),
        upper: map(
            range.upper,
            space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
                Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
            }),
        ),
    }
}

impl BackendRead for BrokenRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        async move {
            let physical_keys = keys
                .iter()
                .map(|key| broken_physical_key(space, key))
                .collect::<Vec<_>>();
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
            Ok(get_many_from_map(entries, self.mode, &physical_keys, opts))
        }
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        async move {
            let range = broken_physical_range(space, range);
            let opts = ScanOptions {
                resume_after: opts
                    .resume_after
                    .as_ref()
                    .map(|key| broken_physical_key(space, key)),
                ..opts
            };
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
            let mut chunk = scan_from_map(entries, self.mode, range, &opts);
            for entry in &mut chunk.entries {
                entry.key = Key(entry.key.0.slice(4..));
            }
            Ok(chunk)
        }
    }
}

impl BackendWrite for BrokenWrite {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
            for mut entry in entries.entries {
                entry.key = broken_physical_key(space, &entry.key);
                let mut bytes = stored_value_bytes(entry.value);
                if matches!(self.mode, BrokenMode::CorruptOpaqueBytes) {
                    bytes = Bytes::from(
                        bytes
                            .iter()
                            .copied()
                            .filter(|byte| *byte != 0)
                            .collect::<Vec<_>>(),
                    );
                }
                self.staged.insert(entry.key, bytes);
            }
            Ok(())
        }
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
            for key in keys {
                let key = &broken_physical_key(space, key);
                if matches!(self.mode, BrokenMode::DeleteManyIgnoresExistingKeys)
                    && self.staged.contains_key(key)
                {
                    continue;
                }
                self.staged.remove(key);
            }
            Ok(())
        }
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
            let range = broken_physical_range(space, range);
            if matches!(self.mode, BrokenMode::DeleteRangeIgnoresUpperBound) {
                self.staged.retain(|key, _value| match &range.lower {
                    Bound::Included(lower) => key < lower,
                    Bound::Excluded(lower) => key <= lower,
                    Bound::Unbounded => false,
                });
            } else {
                self.staged
                    .retain(|key, _value| !range_contains(&range, key));
            }
            Ok(())
        }
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, BackendError>> + Send {
        async move {
            *self
                .parent
                .lock()
                .map_err(|_| BackendError::Io("broken backend lock poisoned".to_string()))? =
                self.staged;
            *self.commit_count.lock().map_err(|_| {
                BackendError::Io("broken backend commit lock poisoned".to_string())
            })? += 1;
            Ok(CommitResult {
                commit_id: None,
                stats: WriteStats::default(),
            })
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
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
    keys: &[Key],
    opts: GetOptions,
) -> GetManyResult {
    GetManyResult::new(
        keys.iter()
            .map(|key| {
                if matches!(mode, BrokenMode::GetManyMissesExistingKey) && key.0.ends_with(b"a") {
                    return None;
                }
                entries
                    .get(key)
                    .map(|value| project_value(value, mode, opts.projection, false))
            })
            .collect(),
    )
}

fn scan_from_map(
    entries: &BrokenMap,
    mode: BrokenMode,
    range: KeyRange,
    opts: &ScanOptions,
) -> ScanChunk {
    let mut candidates = entries
        .iter()
        .filter(|(key, _)| range_contains(&range, key))
        .collect::<Vec<_>>();
    if matches!(mode, BrokenMode::BadByteOrdering) {
        candidates.sort_by(|left, right| {
            left.0
                .0
                .len()
                .cmp(&right.0.0.len())
                .then(left.0.cmp(right.0))
        });
    }

    let mut candidates = candidates.into_iter().filter(|(key, _)| {
        !opts.resume_after.as_ref().is_some_and(|resume_after| {
            if matches!(mode, BrokenMode::KeyResumeRepeatsLastKey) {
                *key < resume_after
            } else {
                *key <= resume_after
            }
        })
    });
    let rows = candidates
        .by_ref()
        .take(opts.page_size())
        .map(|(key, value)| ReadEntry {
            key: key.clone(),
            value: project_value(value, mode, opts.projection, true),
        })
        .collect();
    ScanChunk {
        entries: rows,
        has_more: candidates.next().is_some(),
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

fn project_value(
    value: &Bytes,
    mode: BrokenMode,
    projection: CoreProjection,
    break_key_only: bool,
) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly
            if break_key_only && matches!(mode, BrokenMode::KeyOnlyScanReturnsFullValues) =>
        {
            ProjectedValue::FullValue(value.clone())
        }
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value.clone()),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}
