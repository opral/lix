//! Structural opening diagnostic, not partial-replica admission.
//!
//! Preserve canonical controls/commit IDs, install native root-backed serving
//! markers, and discover only core opening point dependencies. Every range
//! access is rejected rather than silently hydrating a repository inventory.
#![allow(clippy::manual_async_fn)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{Arc, Mutex},
};

use crate::storage::*;
use crate::storage_adapter::{StorageAdapter, StorageAdapterRead};
use bytes::Bytes;

#[derive(Default)]
struct OpeningTrace {
    missing: BTreeMap<StorageSpace, BTreeSet<Key>>,
    scans: Vec<(StorageSpace, KeyRange)>,
    point_keys: usize,
}

#[derive(Clone)]
struct PointOnlyStorage {
    memory: Memory,
    trace: Arc<Mutex<OpeningTrace>>,
}

struct PointOnlyRead {
    read: MemoryRead,
    trace: Arc<Mutex<OpeningTrace>>,
}

impl Storage for PointOnlyStorage {
    type Read<'a>
        = PointOnlyRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
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
            Ok(PointOnlyRead {
                read: self.memory.begin_read(opts).await?,
                trace: Arc::clone(&self.trace),
            })
        }
    }
    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        self.memory.begin_write(opts)
    }
    fn watch_for_changes(
        &self,
    ) -> impl Future<Output = Result<StorageChangeWatch, StorageError>> + Send {
        self.memory.watch_for_changes()
    }
}

impl StorageRead for PointOnlyRead {
    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            let result = self.read.get_many(requests).await?;
            let mut trace = self.trace.lock().unwrap();
            let mut values = result.values.iter();
            for request in requests {
                trace.point_keys += request.keys.len();
                for key in request.keys {
                    if values.next().expect("point result cardinality").is_none() {
                        trace
                            .missing
                            .entry(request.space)
                            .or_default()
                            .insert(key.clone());
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
        _opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        async move {
            self.trace
                .lock()
                .unwrap()
                .scans
                .push((space, range.clone()));
            Err(StorageError::Io(format!(
                "partial opening diagnostic rejected range access: space={} range={range:?}",
                space.name
            )))
        }
    }
}

async fn copy_point_inputs(
    source: &impl StorageAdapterRead,
    destination: &StorageAdapter,
    requested: BTreeMap<StorageSpace, BTreeSet<Key>>,
    remaining_bytes: usize,
) -> (usize, usize) {
    let mut writes = destination.new_write_set();
    let mut count = 0;
    let mut total_bytes = 0;
    for (space, keys) in requested {
        let keys = keys.into_iter().collect::<Vec<_>>();
        let result = source
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .unwrap();
        for (key, value) in keys.into_iter().zip(result.values) {
            if let Some(ProjectedValue::FullValue(bytes)) = value {
                total_bytes += bytes.len();
                assert!(
                    total_bytes <= remaining_bytes,
                    "core opening exceeded point hydration budget at {}",
                    space.name
                );
                eprintln!(
                    "partial opening input space={} bytes={} key_bytes={}",
                    space.name,
                    bytes.len(),
                    key.0.len()
                );
                writes.put(space, key, StoredValue { bytes });
                count += 1;
            }
        }
    }
    destination
        .commit_write_set(writes, WriteOptions::default())
        .await
        .unwrap();
    (count, total_bytes)
}

#[tokio::test]
#[ignore = "manual root-backed partial opening structural diagnostic"]
async fn bounded_root_installer_discovers_core_opening_dependencies_without_scans() {
    for row_count in [128, 12_800] {
        let authority_memory = Memory::new();
        let authority = crate::open_lix()
            .with_storage(authority_memory.clone())
            .await
            .unwrap();
        let values = (0..row_count)
            .map(|index| format!("('unopened-{index:06}', 'payload')"))
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let selected_branch = authority.active_branch_id().await.unwrap();
        let expected_lix_id = authority.lix_id().to_owned();
        let active_account_id = authority.active_account_id().to_owned();
        authority.close().await.unwrap();
        let source_lix = crate::open_lix()
            .with_storage(authority_memory.fork().unwrap())
            .await
            .unwrap();
        let source = source_lix.storage_adapter();
        let source_read = source.begin_read(ReadOptions::default()).await.unwrap();
        let local_memory = Memory::new();
        let local = StorageAdapter::new(local_memory.clone());
        let mut initial_keys = BTreeMap::<StorageSpace, BTreeSet<Key>>::new();
        initial_keys
            .entry(crate::init::REPOSITORY_PROTOCOL_SPACE)
            .or_default()
            .insert(Key(Bytes::from_static(
                crate::init::REPOSITORY_PROTOCOL_KEY,
            )));
        let mut writes = local.new_write_set();
        let mut canonical_controls = Vec::new();
        for branch in [selected_branch.as_str(), crate::GLOBAL_BRANCH_ID] {
            let control = crate::branch::BranchHeadControlContext::new()
                .reader(&source_read)
                .load(branch)
                .await
                .unwrap()
                .unwrap();
            canonical_controls.push((branch.to_owned(), control));
            crate::branch::stage_branch_head_control(&mut writes, branch, control).unwrap();
            // The canonical root remains the authority's current head. The
            // marker changes only how this local serving generation resolves
            // unknown rows; no replacement commit or full receipt is created.
            writes.put(
                crate::hot_state::ROOT_CURRENT_BASE_SPACE,
                Key(crate::hot_state::hot_generation_scope_prefix(
                    branch,
                    control.tracked_generation,
                )
                .into()),
                StoredValue {
                    bytes: Bytes::copy_from_slice(control.head_commit_id.as_uuid().as_bytes()),
                },
            );
            for commit_id in [
                Some(control.head_commit_id),
                control.working_diff_checkpoint_commit_id,
            ]
            .into_iter()
            .flatten()
            {
                let key = Key(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes()));
                initial_keys
                    .entry(crate::changelog::COMMIT_SPACE)
                    .or_default()
                    .insert(key.clone());
                initial_keys
                    .entry(crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE)
                    .or_default()
                    .insert(key);
            }
        }
        let initial_controls_bytes = writes.stats().written_bytes as usize;
        local
            .commit_write_set(writes, WriteOptions::default())
            .await
            .unwrap();
        let (initial_keys_count, initial_metadata_bytes) =
            copy_point_inputs(&source_read, &local, initial_keys, 64 * 1024).await;
        let initial_bytes = initial_controls_bytes + initial_metadata_bytes;
        let trace = Arc::new(Mutex::new(OpeningTrace::default()));
        let point_only = PointOnlyStorage {
            memory: local_memory.clone(),
            trace: Arc::clone(&trace),
        };
        let mut hydrated_bytes = 0;
        let mut hydrated_keys = 0;
        let mut opened = false;
        for attempt in 0..16 {
            *trace.lock().unwrap() = OpeningTrace::default();
            let result = async {
                let engine = crate::engine::Engine::new(point_only.clone()).await?;
                assert_eq!(engine.lix_id(), expected_lix_id);
                let _session = engine
                    .open_session_at_with_account(&selected_branch, &active_account_id)
                    .await?;
                Ok::<(), crate::LixError>(())
            }
            .await;
            let (missing, scans, point_keys) = {
                let trace = trace.lock().unwrap();
                (trace.missing.clone(), trace.scans.clone(), trace.point_keys)
            };
            match result {
                Ok(()) => {
                    opened = true;
                    eprintln!(
                        "partial structural opening SUCCESS rows={row_count} attempt={attempt} initial_keys={initial_keys_count} initial_bytes={initial_bytes} hydrated_keys={hydrated_keys} hydrated_bytes={hydrated_bytes} final_point_requests={point_keys}"
                    );
                    break;
                }
                Err(error) => {
                    eprintln!(
                        "partial structural opening dependency rows={row_count} attempt={attempt} code={} message={} details={:?}",
                        error.code, error.message, error.details
                    );
                }
            }
            let (keys, bytes) = copy_point_inputs(
                &source_read,
                &local,
                missing,
                (256 * 1024usize).saturating_sub(hydrated_bytes),
            )
            .await;
            if keys == 0 {
                eprintln!(
                    "partial structural opening BLOCKED rows={row_count}: no authority-present point dependency made progress; rejected_scans={scans:?}"
                );
                break;
            }
            hydrated_keys += keys;
            hydrated_bytes += bytes;
        }
        // Even a blocked diagnostic must preserve the source's identities.
        let read = local.begin_read(ReadOptions::default()).await.unwrap();
        for (branch, expected) in canonical_controls {
            assert_eq!(
                crate::branch::BranchHeadControlContext::new()
                    .reader(&read)
                    .load(&branch)
                    .await
                    .unwrap(),
                Some(expected)
            );
        }
        eprintln!(
            "partial structural opening result rows={row_count} opened={opened} initial_bytes={initial_bytes} hydrated_bytes={hydrated_bytes}; excludes public Lix admission and sync activation"
        );
    }
}
