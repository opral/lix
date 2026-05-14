use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{run_backend_conformance, BackendFactory, BackendTestConfig, ConformanceStatus};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, Capability,
    CommitResult, Cursor, GetManyResult, GetOptions, GetSlot, Key, KeyRange, PointOrder,
    ProjectedValue, PutBatch, ReadBatch, ReadEntry, ReadOptions, ReadStats, ReadSupport,
    ScanDirection, ScanOptions, ScanPage, SpaceId, StoredValue, ValueProjection, WriteConcurrency,
    WriteOptions, WriteStats,
};

type BrokenMap = BTreeMap<(SpaceId, Key), Bytes>;

#[derive(Clone, Copy, Debug)]
enum BrokenMode {
    KeySortedGetMany,
    CoalesceDuplicateGetMany,
    WriteDoesNotReadOwnWrites,
    ReadSeesLaterCommits,
    RollbackCommits,
    ReverseUnsupportedReturnsForward,
}

#[derive(Clone, Debug)]
struct BrokenBackendFactory {
    mode: BrokenMode,
}

#[derive(Clone, Debug)]
struct BrokenBackend {
    mode: BrokenMode,
    entries: Arc<Mutex<BrokenMap>>,
}

struct BrokenRead {
    mode: BrokenMode,
    parent: Arc<Mutex<BrokenMap>>,
    snapshot: BrokenMap,
}

struct BrokenWrite {
    mode: BrokenMode,
    parent: Arc<Mutex<BrokenMap>>,
    base_snapshot: BrokenMap,
    staged: BrokenMap,
}

#[test]
fn detects_get_many_key_order_violation() {
    assert_failed(
        BrokenMode::KeySortedGetMany,
        "baseline::get_many_preserves_caller_order_duplicates_and_missing",
    );
}

#[test]
fn detects_get_many_duplicate_coalescing_violation() {
    assert_failed(
        BrokenMode::CoalesceDuplicateGetMany,
        "baseline::get_many_preserves_caller_order_duplicates_and_missing",
    );
}

#[test]
fn detects_write_read_your_writes_violation() {
    assert_failed(
        BrokenMode::WriteDoesNotReadOwnWrites,
        "baseline::write_reads_its_own_writes",
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
fn detects_rollback_commits_violation() {
    assert_failed(
        BrokenMode::RollbackCommits,
        "baseline::rollback_discards_staged_mutations",
    );
}

#[test]
fn detects_reverse_unsupported_forward_order_violation() {
    assert_failed(
        BrokenMode::ReverseUnsupportedReturnsForward,
        "scan::reverse_returns_unsupported_when_not_capable",
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

    fn fresh(&self) -> Self::Backend {
        BrokenBackend {
            mode: self.mode,
            entries: Arc::new(Mutex::new(BrokenMap::new())),
        }
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig::default()
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
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(BrokenRead {
            mode: self.mode,
            parent: Arc::clone(&self.entries),
            snapshot: self.snapshot()?,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let snapshot = self.snapshot()?;
        Ok(BrokenWrite {
            mode: self.mode,
            parent: Arc::clone(&self.entries),
            base_snapshot: snapshot.clone(),
            staged: snapshot,
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
        let entries = if matches!(self.mode, BrokenMode::ReadSeesLaterCommits) {
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

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        scan_range_from_map(&self.snapshot, self.mode, space, range, opts)
    }
}

impl BackendRead for BrokenWrite {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        let entries = if matches!(self.mode, BrokenMode::WriteDoesNotReadOwnWrites) {
            &self.base_snapshot
        } else {
            &self.staged
        };
        get_many_from_map(entries, self.mode, space, keys, opts)
    }

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        let entries = if matches!(self.mode, BrokenMode::WriteDoesNotReadOwnWrites) {
            &self.base_snapshot
        } else {
            &self.staged
        };
        scan_range_from_map(entries, self.mode, space, range, opts)
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
    if !opts.predicates.is_empty() {
        return Err(BackendError::Unsupported(Capability::PredicatePushdown));
    }
    match opts.order {
        PointOrder::Caller => {}
        PointOrder::KeyAsc => {
            return Err(BackendError::Unsupported(Capability::KeyOrderedPoints));
        }
        PointOrder::Unordered => {
            return Err(BackendError::Unsupported(Capability::UnorderedPoints));
        }
    }

    let requested = match mode {
        BrokenMode::KeySortedGetMany => {
            let mut sorted = keys.iter().enumerate().collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.1.cmp(right.1).then(left.0.cmp(&right.0)));
            sorted
        }
        BrokenMode::CoalesceDuplicateGetMany => {
            let mut seen = BTreeSet::new();
            keys.iter()
                .enumerate()
                .filter(|(_, key)| seen.insert((*key).clone()))
                .collect()
        }
        _ => keys.iter().enumerate().collect(),
    };

    let slots = requested
        .into_iter()
        .map(|(index, key)| {
            let value = entries
                .get(&(space, key.clone()))
                .map(|value| project_value(value, opts.projection))
                .transpose()?;
            Ok(GetSlot {
                requested_index: Some(index),
                key: key.clone(),
                value,
            })
        })
        .collect::<Result<Vec<_>, BackendError>>()?;

    Ok(GetManyResult {
        entries: slots,
        support: ReadSupport::exact(opts.projection),
        stats: ReadStats {
            backend_calls: 1,
            emitted_entries: keys.len() as u64,
            ..ReadStats::default()
        },
    })
}

fn scan_range_from_map(
    entries: &BrokenMap,
    mode: BrokenMode,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanPage, BackendError> {
    if !opts.predicates.is_empty() {
        return Err(BackendError::Unsupported(Capability::PredicatePushdown));
    }
    if opts.direction == ScanDirection::Reverse
        && !matches!(mode, BrokenMode::ReverseUnsupportedReturnsForward)
    {
        return Err(BackendError::Unsupported(Capability::ReverseScan));
    }

    let after = opts.cursor.map(|cursor| Key(cursor.0.clone()));
    let limit = opts.limit_rows.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(ScanPage {
            entries: ReadBatch {
                entries: Vec::new(),
            },
            next_cursor: None,
            support: ReadSupport::exact(opts.projection),
            stats: ReadStats {
                backend_calls: 1,
                ..ReadStats::default()
            },
        });
    }

    let mut scanned_entries = 0u64;
    let mut read_entries = Vec::new();
    let mut next_cursor = None;

    for ((entry_space, key), value) in entries {
        if *entry_space != space || !range_contains(&range, key) {
            continue;
        }
        if after.as_ref().is_some_and(|after| key <= after) {
            continue;
        }

        scanned_entries += 1;
        if read_entries.len() == limit {
            next_cursor = read_entries
                .last()
                .map(|entry: &ReadEntry| Cursor(entry.key.0.clone()));
            break;
        }
        read_entries.push(ReadEntry {
            key: key.clone(),
            value: project_value(value, opts.projection)?,
        });
    }

    Ok(ScanPage {
        entries: ReadBatch {
            entries: read_entries,
        },
        next_cursor,
        support: ReadSupport::exact(opts.projection),
        stats: ReadStats {
            scanned_entries,
            emitted_entries: scanned_entries.min(limit as u64),
            backend_calls: 1,
            ..ReadStats::default()
        },
    })
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
    projection: ValueProjection,
) -> Result<ProjectedValue, BackendError> {
    match projection {
        ValueProjection::KeyOnly => Ok(ProjectedValue::KeyOnly),
        ValueProjection::FullValue => Ok(ProjectedValue::FullValue(value.clone())),
        other => Err(BackendError::Unsupported(Capability::Projection(other))),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    match value {
        StoredValue::FullValue(bytes) => bytes,
        StoredValue::Envelope {
            header,
            refs,
            payload,
        } => {
            let mut bytes = Vec::with_capacity(header.len() + refs.len() + payload.len());
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&refs);
            bytes.extend_from_slice(&payload);
            Bytes::from(bytes)
        }
    }
}
