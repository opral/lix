//! Dependency diagnostic for a partial replica with on-demand sync.
//!
//! This uses the ordinary native engine, without forging a sync receipt or
//! activating a partial replica. Success only establishes that a prepared SQL
//! workload can retain the untouched native base by reference. Logical coverage,
//! missing-input admission, reconciliation and GC remain separate requirements.
#![allow(clippy::manual_async_fn)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    ops::Bound,
    pin::Pin,
    sync::{Arc, Mutex},
};

use crate::changelog::ChangelogReader;
use crate::storage::*;
use crate::storage_adapter::{StorageAdapter, StorageAdapterRead};
use crate::{CreateBranchOptions, Value, open_lix};

#[derive(Default)]
struct Trace {
    phase: &'static str,
    keys: BTreeMap<StorageSpace, BTreeSet<Key>>,
    returned: BTreeMap<(&'static str, StorageSpace), usize>,
    scans: Vec<(&'static str, StorageSpace, KeyRange)>,
    deleted: BTreeSet<(StorageSpace, Key)>,
    deleted_ranges: Vec<(StorageSpace, KeyRange)>,
    write_preconditions: Vec<(&'static str, Precondition)>,
}

#[derive(Clone)]
struct RecordingStorage {
    memory: Memory,
    trace: Arc<Mutex<Trace>>,
}

struct RecordingRead<R> {
    read: R,
    trace: Arc<Mutex<Trace>>,
}

struct RecordingWrite {
    write: MemoryWrite,
    read: MemoryRead,
    trace: Arc<Mutex<Trace>>,
}

impl Storage for RecordingStorage {
    type Read<'a>
        = RecordingRead<MemoryRead>
    where
        Self: 'a;
    type Write<'a>
        = RecordingWrite
    where
        Self: 'a;

    fn acquire_session(
        &self,
    ) -> impl Future<Output = Result<StorageSessionToken, StorageError>> + Send {
        self.memory.acquire_session()
    }

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            Ok(RecordingRead {
                read: self.memory.begin_read(opts).await?,
                trace: Arc::clone(&self.trace),
            })
        }
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            // This diagnostic has one writer and no concurrent authority
            // changes, so these two source snapshots are identical.
            let read = self
                .memory
                .begin_read(ReadOptions {
                    session_token: opts.session_token,
                    ..ReadOptions::default()
                })
                .await?;
            {
                let mut trace = self.trace.lock().expect("trace lock");
                let phase = trace.phase;
                for precondition in &opts.preconditions {
                    trace
                        .write_preconditions
                        .push((phase, precondition.clone()));
                    match precondition {
                        Precondition::KeyAbsent { space, key }
                        | Precondition::KeyPresent { space, key }
                        | Precondition::KeyValueHashEquals { space, key, .. }
                        | Precondition::KeyValueEquals { space, key, .. } => {
                            trace.keys.entry(*space).or_default().insert(key.clone());
                        }
                        Precondition::RangeEmpty { space, range } => {
                            trace.scans.push((phase, *space, range.clone()));
                        }
                    }
                }
            }
            Ok(RecordingWrite {
                write: self.memory.begin_write(opts).await?,
                read,
                trace: Arc::clone(&self.trace),
            })
        }
    }

    fn watch_for_changes(
        &self,
    ) -> impl Future<Output = Result<StorageChangeWatch, StorageError>> + Send {
        self.memory.watch_for_changes()
    }
}

impl RecordingWrite {
    fn record_puts(&self, space: StorageSpace, entries: &PutBatch) {
        let mut trace = self.trace.lock().expect("trace lock");
        for entry in &entries.entries {
            // Memory checks prior bytes when putting immutable identities.
            trace
                .keys
                .entry(space)
                .or_default()
                .insert(entry.key.clone());
            trace.deleted.remove(&(space, entry.key.clone()));
        }
    }
}

impl StorageWrite for RecordingWrite {
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.record_puts(space, &entries);
        self.write.put_many(space, entries)
    }

    fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.record_puts(space, &entries);
        self.write.replace_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        {
            let mut trace = self.trace.lock().expect("trace lock");
            trace
                .keys
                .entry(space)
                .or_default()
                .extend(keys.iter().cloned());
            trace
                .deleted
                .extend(keys.iter().cloned().map(|key| (space, key)));
        }
        self.write.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            // MemoryWrite enumerates the base keys in this range internally.
            // Record that real dependency rather than overlooking write I/O.
            let entries = self
                .read
                .begin_scan(space, range.clone(), BeginScanOptions::default())
                .await?
                .collect_all()
                .await?;
            {
                let mut trace = self.trace.lock().expect("trace lock");
                let phase = trace.phase;
                trace.scans.push((phase, space, range.clone()));
                trace
                    .keys
                    .entry(space)
                    .or_default()
                    .extend(entries.into_iter().map(|entry| entry.key));
                trace.deleted_ranges.push((space, range.clone()));
            }
            self.write.delete_range(space, range).await
        }
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        self.write.commit()
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.write.rollback()
    }
}

impl<R: StorageRead> StorageRead for RecordingRead<R> {
    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            let result = self.read.get_many(requests).await?;
            let mut trace = self.trace.lock().expect("trace lock");
            let phase = trace.phase;
            let mut values = result.values.iter();
            for request in requests {
                for key in request.keys {
                    // Include proven absences in the requested closure log;
                    // copying below only installs source-present values.
                    trace
                        .keys
                        .entry(request.space)
                        .or_default()
                        .insert(key.clone());
                    if values.next().expect("point result cardinality").is_some() {
                        *trace.returned.entry((phase, request.space)).or_default() += 1;
                    }
                }
            }
            Ok(result)
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        async move {
            let cursor = self.read.begin_scan(space, range.clone(), opts).await?;
            let phase = {
                let mut trace = self.trace.lock().expect("trace lock");
                let phase = trace.phase;
                trace.scans.push((phase, space, range.clone()));
                phase
            };
            ScanCursor::from_source(
                range,
                opts.order,
                RecordingScan {
                    cursor,
                    space,
                    phase,
                    trace: Arc::clone(&self.trace),
                },
            )
        }
    }
}

struct RecordingScan<'a> {
    cursor: ScanCursor<'a>,
    space: StorageSpace,
    phase: &'static str,
    trace: Arc<Mutex<Trace>>,
}

impl StorageScanSource for RecordingScan<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let (entries, more) = self.cursor.next_page(limit_rows).await?.into_parts();
            let mut trace = self.trace.lock().expect("trace lock");
            for entry in &entries {
                trace
                    .keys
                    .entry(self.space)
                    .or_default()
                    .insert(entry.key.clone());
            }
            *trace.returned.entry((self.phase, self.space)).or_default() += entries.len();
            Ok(ScanChunk::new(entries, more))
        })
    }
}

fn all_keys() -> KeyRange {
    KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    }
}

fn json_string(value: Value) -> String {
    match value {
        Value::Jsonb(value) => value.as_json_string().expect("JSON string value"),
        Value::Text(value) => value,
        value => panic!("expected string-valued key/value row, got {value:?}"),
    }
}

fn key_in_range(key: &Key, range: &KeyRange) -> bool {
    let lower = match &range.lower {
        Bound::Included(bound) => key >= bound,
        Bound::Excluded(bound) => key > bound,
        Bound::Unbounded => true,
    };
    let upper = match &range.upper {
        Bound::Included(bound) => key <= bound,
        Bound::Excluded(bound) => key < bound,
        Bound::Unbounded => true,
    };
    lower && upper
}

async fn branch_control<S: Storage + Clone + Send + Sync + 'static>(
    lix: &crate::Lix<S>,
    branch_id: &str,
) -> crate::branch::BranchHeadControl {
    let adapter = lix.storage_adapter();
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(branch_id)
        .await
        .unwrap()
        .unwrap()
}

/// Probe checkpoint publication and GC independently after the SQL/upload
/// gate. A blocked lifecycle probe is reported explicitly and cannot silently
/// turn into a full-baseline fetch or invalidate the successful edit result.
async fn probe_partial_checkpoint(
    partial: &Memory,
    baseline: &crate::Lix<Memory>,
    branch_id: &str,
    target: &str,
    root_backed: bool,
    row_count: usize,
) {
    let memory = partial.fork().unwrap();
    let root = open_lix().with_storage(memory.clone()).await.unwrap();
    let selected = root
        .open_another_session()
        .with_branch(branch_id)
        .await
        .unwrap();
    let before = branch_control(&selected, branch_id).await;
    // Checkpoint preparation differs from ordinary edit preparation: its
    // semantic parent is the declared checkpoint cursor. Load only that
    // immutable topology header (replay-debt/root references), not its mutation
    // inventory, tree, rows, contents, or first-parent ancestry.
    if let Some(checkpoint) = before.working_diff_checkpoint_commit_id {
        let source = baseline.storage_adapter();
        let source_read = source.begin_read(ReadOptions::default()).await.unwrap();
        let header =
            crate::tracked_state::load_published_commit_state_topology(&source_read, checkpoint)
                .await
                .unwrap()
                .expect("pinned baseline owns checkpoint parent authority");
        assert_eq!(header.commit_id(), checkpoint);
        let space = crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE;
        let keys = [Key(bytes::Bytes::copy_from_slice(
            checkpoint.as_uuid().as_bytes(),
        ))];
        let value = source_read
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .unwrap()
            .values
            .into_iter()
            .next()
            .flatten()
            .unwrap();
        let ProjectedValue::FullValue(bytes) = value else {
            panic!("checkpoint topology omitted bytes")
        };
        let destination = selected.storage_adapter();
        let read = destination
            .begin_read(ReadOptions::default())
            .await
            .unwrap();
        let existing = read
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .unwrap()
            .values
            .into_iter()
            .next()
            .flatten();
        drop(read);
        if let Some(ProjectedValue::FullValue(existing)) = existing {
            assert_eq!(
                existing, bytes,
                "immutable checkpoint header must agree with pinned baseline"
            );
        } else {
            let copied_bytes = bytes.len();
            let mut writes = destination.new_write_set();
            writes.put_content_addressed_batch(
                space,
                [(
                    keys[0].clone(),
                    crate::storage_adapter::StorageValue { bytes },
                )],
            );
            destination
                .commit_write_set(writes, WriteOptions::default())
                .await
                .unwrap();
            eprintln!(
                "partial checkpoint prepared parent topology root_backed={root_backed} rows={row_count} objects=1 bytes={copied_bytes}"
            );
        }
    }
    let started = std::time::Instant::now();
    let checkpoint = selected
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await;
    let checkpoint = match checkpoint {
        Ok(result) => {
            let commit_id = result.rows()[0].get::<String>("commit_id").unwrap();
            eprintln!(
                "partial checkpoint SUCCESS root_backed={root_backed} rows={row_count} elapsed_us={} commit_id={commit_id}",
                started.elapsed().as_micros()
            );
            commit_id
        }
        Err(error) => {
            eprintln!(
                "partial checkpoint BLOCKED root_backed={root_backed} rows={row_count} code={} message={} details={:?}",
                error.code, error.message, error.details
            );
            assert_eq!(
                branch_control(&selected, branch_id).await,
                before,
                "failed precommit checkpoint must not move the branch"
            );
            selected.close().await.unwrap();
            root.close().await.unwrap();
            return;
        }
    };
    let after_checkpoint = branch_control(&selected, branch_id).await;
    assert_eq!(after_checkpoint.head_commit_id.to_string(), checkpoint);
    let adapter = selected.storage_adapter();
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        adapter.begin_read(ReadOptions::default()).await.unwrap(),
    );
    let mut writes = adapter.new_write_set();
    let mut preconditions = Vec::new();
    match crate::gc::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
        .await
    {
        Ok(plan) => {
            adapter
                .commit_write_set(
                    writes,
                    WriteOptions {
                        preconditions,
                        ..WriteOptions::default()
                    },
                )
                .await
                .unwrap();
            eprintln!(
                "partial GC SUCCESS root_backed={root_backed} rows={row_count} reclaimed_commits={} has_more={}",
                plan.sweep.tracked_commit_roots.len(),
                plan.sweep.has_more
            );
        }
        Err(error) => {
            eprintln!(
                "partial GC BLOCKED root_backed={root_backed} rows={row_count} code={} message={} details={:?}; uncommitted_staged_mutations={}",
                error.code,
                error.message,
                error.details,
                writes.stats().staged_puts + writes.stats().staged_deletes
            );
            // Never commit a partially built sweep after a missing dependency.
            drop(writes);
        }
    }
    assert_eq!(branch_control(&selected, branch_id).await, after_checkpoint);
    selected.close().await.unwrap();
    root.close().await.unwrap();
    let reopened = open_lix().with_storage(memory).await.unwrap();
    let selected = reopened
        .open_another_session()
        .with_branch(branch_id)
        .await
        .unwrap();
    let row = selected
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(target.to_owned())],
        )
        .await
        .unwrap();
    assert_eq!(
        json_string(row.rows()[0].get::<Value>("value").unwrap()),
        "after-2"
    );
    selected.close().await.unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
#[ignore = "manual native SQL partial-storage dependency feasibility diagnostic"]
async fn prepared_sql_update_with_only_native_dependency_closure() {
    run_prepared_sql_dependency_case(false).await;
}

#[tokio::test]
#[ignore = "manual root-backed SQL partial-storage dependency feasibility diagnostic"]
async fn prepared_sql_update_from_root_backed_branch_with_native_dependency_closure() {
    run_prepared_sql_dependency_case(true).await;
}

async fn run_prepared_sql_dependency_case(root_backed: bool) {
    for row_count in [128, 4096] {
        eprintln!("native SQL dependency case root_backed={root_backed} rows={row_count}");
        let source_memory = Memory::new();
        let source = open_lix()
            .with_storage(source_memory.clone())
            .await
            .unwrap();
        let values = (0..row_count)
            .map(|index| format!("('partial-sql-{index:06}', 'before')"))
            .collect::<Vec<_>>()
            .join(",");
        source
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let branch_id = if root_backed {
            Some(
                source
                    .create_branch(CreateBranchOptions {
                        id: None,
                        name: "partial-root-backed".to_owned(),
                        from_commit_id: None,
                    })
                    .await
                    .unwrap()
                    .id,
            )
        } else {
            None
        };
        source.close().await.unwrap();
        // Logical reads must use the admitted epoch. The recorder sees raw
        // physical spaces, so copying those addresses uses a separate raw
        // adapter; never mix the two coordinate systems.
        let baseline_lix = open_lix()
            .with_storage(source_memory.fork().unwrap())
            .await
            .unwrap();
        let baseline = baseline_lix.storage_adapter();
        let baseline_raw = StorageAdapter::new(source_memory.fork().unwrap());
        if let Some(branch_id) = &branch_id {
            let read = baseline.begin_read(ReadOptions::default()).await.unwrap();
            let control = crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load(branch_id)
                .await
                .unwrap()
                .unwrap();
            let keys = [Key(crate::hot_state::hot_generation_scope_prefix(
                branch_id,
                control.tracked_generation,
            )
            .into())];
            let root_pointer = read
                .get_many(&[GetManyRequest {
                    space: crate::hot_state::ROOT_CURRENT_BASE_SPACE,
                    keys: &keys,
                    opts: GetOptions::default(),
                }])
                .await
                .unwrap();
            assert!(
                root_pointer.values[0].is_some(),
                "fixture must actually use the native root-backed current-state route"
            );
        }
        let trace = Arc::new(Mutex::new(Trace {
            phase: "open",
            ..Trace::default()
        }));
        let preparing_root = open_lix()
            .with_storage(RecordingStorage {
                memory: source_memory.fork().unwrap(),
                trace: Arc::clone(&trace),
            })
            .await
            .unwrap();
        trace.lock().unwrap().phase = "selected-session";
        let preparing = match &branch_id {
            Some(branch_id) => preparing_root
                .open_another_session()
                .with_branch(branch_id)
                .await
                .unwrap(),
            None => preparing_root.open_another_session().await.unwrap(),
        };
        let target = format!("partial-sql-{:06}", row_count / 2);
        let parameters = [Value::Text(target.clone())];
        trace.lock().unwrap().phase = "point-read";
        preparing
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1",
                &parameters,
            )
            .await
            .unwrap();
        trace.lock().unwrap().phase = "write-spine";
        let preparing_branch = preparing.active_branch_id().await.unwrap();
        let mut cursor = branch_control(&preparing, &preparing_branch)
            .await
            .head_commit_id;
        let adapter = preparing.storage_adapter();
        let spine_read = adapter.begin_read(ReadOptions::default()).await.unwrap();
        let mut spine = BTreeSet::new();
        loop {
            assert!(
                spine.insert(cursor),
                "baseline jump spine must be acyclic except its terminal self link"
            );
            let ids = [cursor];
            let record = crate::changelog::ChangelogContext::new()
                .reader(&spine_read)
                .load_commits(crate::changelog::CommitLoadRequest { commit_ids: &ids })
                .await
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .1
                .unwrap();
            if record.first_parent_jump_commit_id == cursor {
                break;
            }
            cursor = record.first_parent_jump_commit_id;
        }
        eprintln!(
            "native SQL prepared Myers spine root_backed={root_backed} rows={row_count} headers={}",
            spine.len()
        );
        drop(spine_read);
        trace.lock().unwrap().phase = "update";
        preparing
            .execute(
                "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                &[
                    Value::Text(target.clone()),
                    Value::Text("after-0".to_owned()),
                ],
            )
            .await
            .unwrap();
        trace.lock().unwrap().phase = "close";
        preparing.close().await.unwrap();
        preparing_root.close().await.unwrap();

        let keys = trace.lock().unwrap().keys.clone();
        let local_memory = Memory::new();
        let local_adapter = StorageAdapter::new(local_memory.clone());
        let baseline_read = baseline.begin_read(ReadOptions::default()).await.unwrap();
        let baseline_raw_read = baseline_raw
            .begin_read(ReadOptions::default())
            .await
            .unwrap();
        let mut writes = local_adapter.new_write_set();
        let mut copied = BTreeMap::<StorageSpace, (usize, usize)>::new();
        for (space, keys) in keys {
            let keys = keys.into_iter().collect::<Vec<_>>();
            let values = baseline_raw_read
                .get_many(&[GetManyRequest {
                    space,
                    keys: &keys,
                    opts: GetOptions::default(),
                }])
                .await
                .unwrap();
            for (key, value) in keys.into_iter().zip(values.values) {
                if let Some(ProjectedValue::FullValue(bytes)) = value {
                    let counts = copied.entry(space).or_default();
                    counts.0 += 1;
                    counts.1 += bytes.len();
                    writes.put(space, key, StoredValue { bytes });
                }
            }
        }
        local_adapter
            .commit_write_set(writes, WriteOptions::default())
            .await
            .unwrap();
        let mut total = BTreeMap::new();
        for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
            let entries = baseline_read
                .begin_scan(space, all_keys(), BeginScanOptions::default())
                .await
                .unwrap()
                .collect_all()
                .await
                .unwrap();
            if !entries.is_empty() {
                total.insert(
                    space,
                    (
                        entries.len(),
                        entries
                            .iter()
                            .map(|entry| match &entry.value {
                                ProjectedValue::FullValue(bytes) => bytes.len(),
                                ProjectedValue::KeyOnly => 0,
                            })
                            .sum::<usize>(),
                    ),
                );
            }
        }
        for (space, (total_keys, total_bytes)) in &total {
            let (copied_keys, copied_bytes) = copied
                .iter()
                .filter(|(physical, _)| physical.name == space.name)
                .fold((0, 0), |(keys, bytes), (_, (more_keys, more_bytes))| {
                    (keys + more_keys, bytes + more_bytes)
                });
            eprintln!(
                "native SQL closure rows={row_count} space={} copied_keys={copied_keys}/{total_keys} copied_bytes={copied_bytes}/{total_bytes}",
                space.name
            );
        }
        {
            let trace = trace.lock().unwrap();
            for ((phase, space), count) in &trace.returned {
                eprintln!(
                    "native SQL returned rows={row_count} phase={phase} space={} count={count}",
                    space.name
                );
            }
            for (phase, space, range) in &trace.scans {
                eprintln!(
                    "native SQL range rows={row_count} phase={phase} space={} range={range:?}",
                    space.name
                );
            }
            for (phase, precondition) in &trace.write_preconditions {
                eprintln!(
                    "native SQL write precondition rows={row_count} phase={phase} {precondition:?}"
                );
            }
        }

        // This is the feasibility gate. Any missing native publication input
        // must fail visibly here; never install a fake full-state certificate.
        let local_trace = Arc::new(Mutex::new(Trace {
            phase: "local",
            ..Trace::default()
        }));
        let local_root = open_lix()
            .with_storage(RecordingStorage {
                memory: local_memory.clone(),
                trace: Arc::clone(&local_trace),
            })
            .await
            .unwrap_or_else(|error| {
                panic!("partial native open needs additional dependencies: {error}")
            });
        let local = match &branch_id {
            Some(branch_id) => local_root
                .open_another_session()
                .with_branch(branch_id)
                .await
                .unwrap(),
            None => local_root.open_another_session().await.unwrap(),
        };
        let publication_branch_id = local.active_branch_id().await.unwrap();
        let before_control = branch_control(&local, &publication_branch_id).await;
        let authority_before = crate::branch::BranchHeadControlContext::new()
            .reader(&baseline_read)
            .load(&publication_branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before_control.head_commit_id, authority_before.head_commit_id,
            "selecting the editing session must not author unexported setup commits"
        );
        assert_eq!(
            before_control.working_diff_checkpoint_commit_id,
            authority_before.working_diff_checkpoint_commit_id
        );
        let account_id = local.active_account_id().to_owned();
        let mut commit_ids = Vec::new();
        for step in 0..3 {
            let updated_value = format!("after-{step}");
            local
                .execute(
                    "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                    &[
                        Value::Text(target.clone()),
                        Value::Text(updated_value.clone()),
                    ],
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("prepared partial native SQL update {step} failed: {error}")
                });
            let result = local
                .execute(
                    "SELECT value FROM lix_key_value WHERE key = $1",
                    &parameters,
                )
                .await
                .unwrap();
            assert_eq!(result.rows().len(), 1);
            assert_eq!(
                json_string(result.rows()[0].get::<Value>("value").unwrap()),
                updated_value
            );
            let head = branch_control(&local, &publication_branch_id)
                .await
                .head_commit_id;
            assert_ne!(head, before_control.head_commit_id);
            assert!(
                !commit_ids.contains(&head),
                "every update must author a distinct native commit"
            );
            commit_ids.push(head);
        }
        let after_control = branch_control(&local, &publication_branch_id).await;
        local.close().await.unwrap();
        local_root.close().await.unwrap();

        // Reopen the same still-partial storage before exporting. No baseline
        // bytes or authority reads enter the local path between editing and
        // export: only the three newly authored commit bodies leave it.
        let reopened = open_lix()
            .with_storage(RecordingStorage {
                memory: local_memory.clone(),
                trace: Arc::clone(&local_trace),
            })
            .await
            .unwrap_or_else(|error| {
                panic!("partial native reopen before publication failed: {error}")
            });
        let mut commits = Vec::new();
        let mut expected_parent = before_control.head_commit_id.to_string();
        for commit_id in &commit_ids {
            let commit = crate::sync::export_sync_commit(&reopened, &commit_id.to_string())
                .await
                .unwrap_or_else(|error| panic!("partial native commit export failed: {error}"))
                .expect("locally authored commit must export");
            assert_eq!(commit.parent_commit_ids, vec![expected_parent]);
            assert_eq!(commit.account_id, account_id);
            assert_eq!(
                commit.members.len(),
                1,
                "one-row update must not export the untouched base"
            );
            expected_parent = commit.commit_id.clone();
            commits.push(commit);
        }
        reopened.close().await.unwrap();
        let request = crate::sync::SyncPushRequest {
            commits,
            ref_updates: serde_json::from_value(serde_json::json!([{
                "branchId": publication_branch_id,
                "expectedHeadCommitId": before_control.head_commit_id.to_string(),
                "expectedCheckpointCommitId": before_control.working_diff_checkpoint_commit_id.map(|id| id.to_string()),
                "headCommitId": after_control.head_commit_id.to_string(),
                "checkpointCommitId": after_control.working_diff_checkpoint_commit_id.map(|id| id.to_string()),
            }])).unwrap(),
            inline_blobs: Vec::new(),
        };
        eprintln!(
            "native SQL publication root_backed={root_backed} rows={row_count} commits={} request_bytes={}",
            request.commits.len(),
            serde_json::to_vec(&request).unwrap().len()
        );

        // The independent authority still owns the original complete base.
        // Use normal account-bound import and exact head/checkpoint CAS, never
        // the test-only trusted-authorship importer or a replica receipt.
        let authority = open_lix()
            .with_storage(source_memory.fork().unwrap())
            .await
            .unwrap();
        authority
            .push_sync_repository_for_account(&request, &account_id)
            .await
            .unwrap_or_else(|error| {
                panic!("authority rejected partial native publication: {error}")
            });
        assert_eq!(
            branch_control(&authority, &publication_branch_id)
                .await
                .head_commit_id,
            after_control.head_commit_id
        );
        for commit in &request.commits {
            assert_eq!(
                crate::sync::export_sync_commit(&authority, &commit.commit_id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(commit),
                "authority must preserve exact native commit identity and membership"
            );
        }
        let authority_selected = authority
            .open_another_session()
            .with_branch(&publication_branch_id)
            .await
            .unwrap();
        let authority_rows = authority_selected
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key LIKE 'partial-sql-%' ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(authority_rows.rows().len(), row_count);
        for (index, row) in authority_rows.rows().iter().enumerate() {
            let key = format!("partial-sql-{index:06}");
            assert_eq!(row.get::<String>("key").unwrap(), key);
            assert_eq!(
                json_string(row.get::<Value>("value").unwrap()),
                if key == target { "after-2" } else { "before" }
            );
        }
        authority_selected.close().await.unwrap();
        authority.close().await.unwrap();
        probe_partial_checkpoint(
            &local_memory,
            &baseline_lix,
            &publication_branch_id,
            &target,
            root_backed,
            row_count,
        )
        .await;

        // Reconstitute only in the verifier; don't overwrite local mutations.
        let verifier_memory = local_memory.fork().unwrap();
        let verifier_session = open_lix()
            .with_storage(verifier_memory.clone())
            .await
            .unwrap();
        let verifier_adapter = verifier_session.storage_adapter();
        let local_read = verifier_adapter
            .begin_read(ReadOptions::default())
            .await
            .unwrap();
        let mut writes = verifier_adapter.new_write_set();
        for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
            let entries = baseline_read
                .begin_scan(space, all_keys(), BeginScanOptions::default())
                .await
                .unwrap()
                .collect_all()
                .await
                .unwrap();
            for entry in entries {
                // Internal index maintenance can delete keys even for a SQL
                // UPDATE. Restoring those would resurrect deliberately removed
                // state and would invalidate the untouched-row verifier.
                let deleted = {
                    let trace = local_trace.lock().unwrap();
                    trace.deleted.iter().any(|(deleted_space, key)| {
                        deleted_space.name == space.name && key == &entry.key
                    }) || trace.deleted_ranges.iter().any(|(deleted_space, range)| {
                        deleted_space.name == space.name && key_in_range(&entry.key, range)
                    })
                };
                if deleted {
                    continue;
                }
                let keys = [entry.key.clone()];
                let existing = local_read
                    .get_many(&[GetManyRequest {
                        space,
                        keys: &keys,
                        opts: GetOptions::default(),
                    }])
                    .await
                    .unwrap();
                if existing.values[0].is_none() {
                    if let ProjectedValue::FullValue(bytes) = entry.value {
                        writes.put(space, entry.key, StoredValue { bytes });
                    }
                }
            }
        }
        drop(local_read);
        verifier_adapter
            .commit_write_set(writes, WriteOptions::default())
            .await
            .unwrap();
        verifier_session.close().await.unwrap();
        let verifier_root = open_lix().with_storage(verifier_memory).await.unwrap();
        let verifier = verifier_root
            .open_another_session()
            .with_branch(&publication_branch_id)
            .await
            .unwrap();
        let rows = verifier
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key LIKE 'partial-sql-%' ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows().len(), row_count);
        for (index, row) in rows.rows().iter().enumerate() {
            let key = format!("partial-sql-{index:06}");
            assert_eq!(row.get::<String>("key").unwrap(), key);
            assert_eq!(
                json_string(row.get::<Value>("value").unwrap()),
                if key == target { "after-2" } else { "before" }
            );
        }
        verifier.close().await.unwrap();
        verifier_root.close().await.unwrap();
    }
}
