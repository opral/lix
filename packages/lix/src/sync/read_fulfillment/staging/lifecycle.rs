//! Crash-safe ownership and admission for private operation scratch.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Arc;
use futures_util::FutureExt;
static RESERVED: AtomicUsize = AtomicUsize::new(0);
const GLOBAL_BYTES: usize = 1024 * 1024 * 1024;
const REPOSITORY_OPERATIONS: usize = 2;
const TTL_MS: u64 = 300_000;

pub(super) struct Permit {
    #[cfg(test)]
    observer: Option<Arc<AtomicUsize>>,
}
impl Permit {
    pub(super) fn acquire() -> Result<Self, LixError> {
        #[cfg(test)]
        {
            Self::acquire_inner(None)
        }
        #[cfg(not(test))]
        {
            Self::acquire_inner()
        }
    }

    #[cfg(test)]
    pub(super) fn acquire_observed(
        observer: Arc<AtomicUsize>,
    ) -> Result<Self, LixError> {
        Self::acquire_inner(Some(observer))
    }

    fn acquire_inner(
        #[cfg(test)] observer: Option<Arc<AtomicUsize>>,
    ) -> Result<Self, LixError> {
        RESERVED
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                bytes
                    .checked_add(MAX_PAYLOAD_BYTES)
                    .filter(|next| *next <= GLOBAL_BYTES)
            })
            .map_err(|_| {
                LixError::new(
                    "LIX_NATIVE_RECIPE_WORK_BOUND",
                    "client scratch admission quota exceeded",
                )
            })?;
        #[cfg(test)]
        if let Some(observer) = &observer {
            observer.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Self {
            #[cfg(test)]
            observer,
        })
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        RESERVED.fetch_sub(MAX_PAYLOAD_BYTES, Ordering::AcqRel);
        #[cfg(test)]
        if let Some(observer) = &self.observer {
            observer.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Owner {
    expires_at_ms: u64,
    epoch: String,
    reaping: bool,
}
type Ledger = BTreeMap<String, Owner>;
fn ledger_key() -> StorageKey {
    StorageKey(Bytes::from_static(b"operations"))
}

async fn load<R: StorageAdapterRead>(read: &R) -> Result<(Ledger, Option<Bytes>), LixError> {
    let value = read
        .get_many_bounded(
            &[StorageGetManyRequest {
                space: STAGING_SPACE,
                keys: &[ledger_key()],
                opts: Default::default(),
            }],
            ReadBudget {
                max_result_bytes: 4096,
                max_single_value_bytes: 4096,
            },
        )
        .await?
        .values
        .pop()
        .flatten();
    match value {
        Some(StorageProjectedValue::FullValue(bytes)) if bytes.len() <= 4096 => {
            let ledger: Ledger = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("invalid scratch ownership ledger"))?;
            if ledger.len() > REPOSITORY_OPERATIONS {
                return Err(invalid("scratch ledger exceeds operation quota"));
            }
            Ok((ledger, Some(bytes)))
        }
        None => Ok((Ledger::new(), None)),
        _ => Err(invalid("scratch ledger exceeds byte bound")),
    }
}

/// Add the active owner -> reaping transition to a caller's existing write
/// set. The caller supplies its own admission and domain preconditions, so the
/// canonical installation and scratch fence commit atomically.
pub(super) async fn stage_reaping_fence<R: StorageAdapterRead>(
    read: &R,
    state: &PartialReplicaState,
    id: uuid::Uuid,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    let (mut ledger, expected) = load(read).await?;
    let owner = ledger.get_mut(&id.to_string()).ok_or_else(|| {
        LixError::new(
            "LIX_READ_FULFILLMENT_RESTART",
            "scratch owner disappeared before finalization",
        )
    })?;
    if owner.reaping || owner.epoch != state.epoch_id() {
        return Err(LixError::new(
            "LIX_READ_FULFILLMENT_RESTART",
            "scratch owner was fenced before finalization",
        ));
    }
    owner.reaping = true;
    preconditions.push(condition(expected));
    writes.put(
        STAGING_SPACE,
        ledger_key(),
        serde_json::to_vec(&ledger).map_err(|_| invalid("invalid scratch ownership ledger"))?,
    );
    Ok(())
}

/// Empty closures still need an atomic admission check and owner reaping
/// claim. This is the terminal fence which a non-empty closure folds into its
/// final canonical install transaction.
pub(super) async fn finalize_empty<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    id: uuid::Uuid,
) -> Result<(), LixError> {
    for _ in 0..16 {
        let read = storage.begin_read(Default::default()).await?;
        let (actual, admission) =
            super::super::super::partial_state::load_partial_replica_state(&read)
                .await?
                .ok_or_else(|| invalid("partial admission is absent during finalization"))?;
        if actual != *state {
            return Err(LixError::new(
                super::super::super::runtime::PARTIAL_ADMISSION_CHANGED_CODE,
                "read promotion admission changed",
            ));
        }
        let mut writes = storage.new_write_set();
        let mut preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: super::super::super::PARTIAL_REPLICA_STATE_SPACE,
            key: super::super::super::partial_state::partial_replica_state_key(),
            expected: admission,
        }];
        stage_reaping_fence(&read, state, id, &mut writes, &mut preconditions).await?;
        drop(read);
        match storage
            .commit_partial_replica_write_set(
                super::super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Err(StorageWriteSetError::Storage(StorageError::PreconditionFailed(_))) => continue,
            result => return result.map(|_| ()).map_err(Into::into),
        }
    }
    Err(invalid("scratch finalization contention exceeded retry budget"))
}
fn condition(expected: Option<Bytes>) -> StoragePrecondition {
    match expected {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: STAGING_SPACE,
            key: ledger_key(),
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: STAGING_SPACE,
            key: ledger_key(),
        },
    }
}
async fn write<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    ledger: &Ledger,
    expected: Option<Bytes>,
    admission: Option<Bytes>,
) -> Result<bool, LixError> {
    let mut writes = storage.new_write_set();
    if ledger.is_empty() {
        writes.delete(STAGING_SPACE, ledger_key());
    } else {
        writes.put(
            STAGING_SPACE,
            ledger_key(),
            serde_json::to_vec(ledger).map_err(|_| invalid("invalid scratch ownership ledger"))?,
        );
    }
    let mut preconditions = vec![condition(expected)];
    if let Some(expected) = admission {
        preconditions.push(StoragePrecondition::KeyValueEquals {
            space: super::super::super::PARTIAL_REPLICA_STATE_SPACE,
            key: super::super::super::partial_state::partial_replica_state_key(),
            expected,
        });
    }
    match storage
        .commit_partial_replica_write_set(
            super::super::super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
    {
        Err(StorageWriteSetError::Storage(StorageError::PreconditionFailed(_))) => Ok(false),
        result => result.map(|_| true).map_err(Into::into),
    }
}

pub(crate) async fn reap_expired<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
) -> Result<(), LixError> {
    reap(storage, false).await
}

/// Requires a newly acquired exclusive physical replica-owner lease. Its prior
/// owner cannot still publish scratch mutations, even within the same epoch.
pub(crate) async fn reap_abandoned<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
) -> Result<(), LixError> {
    reap(storage, true).await
}

async fn reap<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    abandoned: bool,
) -> Result<(), LixError> {
    for _ in 0..16 {
        let read = storage.begin_read(Default::default()).await?;
        let (mut ledger, expected) = load(&read).await?;
        if ledger.is_empty() {
            let Some(expected) = expected else {
                return Ok(());
            };
            drop(read);
            if write(storage, &ledger, Some(expected), None).await? {
                return Ok(());
            }
            continue;
        }
        let Some((state, admission)) =
            super::super::super::partial_state::load_partial_replica_state(&read).await?
        else {
            return Err(invalid("scratch reaping admission is absent"));
        };
        drop(read);
        let now = crate::telemetry::unix_time_ms();
        let mut claimed = false;
        for owner in ledger.values_mut() {
            if !owner.reaping
                && (abandoned || owner.expires_at_ms <= now || owner.epoch != state.epoch_id())
            {
                owner.reaping = true;
                claimed = true;
            }
        }
        // The durable fence precedes every deletion. A crash leaves a Reaping
        // owner, so later cleanup can resume without permitting new appends.
        if claimed {
            let _ = write(storage, &ledger, expected, Some(admission)).await?;
            continue;
        }
        let expired = ledger
            .iter()
            .filter(|(_, owner)| owner.reaping)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return Ok(());
        }
        for id in expired {
            clear_frames(
                storage,
                uuid::Uuid::parse_str(&id).map_err(|_| invalid("invalid scratch owner"))?,
            )
            .await?;
            ledger.remove(&id);
        }
        if write(storage, &ledger, expected, Some(admission)).await? {
            return Ok(());
        }
    }
    Err(invalid("scratch reaping contention exceeded retry budget"))
}

// Renewal and the frame mutations share one CAS publication. A competing
// cleanup fence or another owner's renewal forces a fresh bounded snapshot.
pub(super) async fn commit_frames<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    id: uuid::Uuid,
    frames: &[(StorageKey, Bytes)],
) -> Result<(), LixError> {
    for _ in 0..16 {
        let read = storage.begin_read(Default::default()).await?;
        let (mut ledger, expected) = load(&read).await?;
        let (actual, admission) =
            super::super::super::partial_state::load_partial_replica_state(&read)
                .await?
                .ok_or_else(|| invalid("partial admission is absent during scratch renewal"))?;
        if actual != *state {
            return Err(LixError::new(
                super::super::super::runtime::PARTIAL_ADMISSION_CHANGED_CODE,
                "scratch renewal admission changed",
            ));
        }
        let owner = ledger
            .get_mut(&id.to_string())
            .filter(|owner| !owner.reaping && owner.epoch == state.epoch_id())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_READ_FULFILLMENT_RESTART",
                    "scratch operation was fenced",
                )
            })?;
        owner.expires_at_ms = crate::telemetry::unix_time_ms().saturating_add(TTL_MS);
        drop(read);
        let mut writes = storage.new_write_set();
        writes.put(
            STAGING_SPACE,
            ledger_key(),
            serde_json::to_vec(&ledger).map_err(|_| invalid("invalid scratch ownership ledger"))?,
        );
        let mut preconditions = vec![
            condition(expected),
            StoragePrecondition::KeyValueEquals {
                space: super::super::super::PARTIAL_REPLICA_STATE_SPACE,
                key: super::super::super::partial_state::partial_replica_state_key(),
                expected: admission,
            },
        ];
        for (key, bytes) in frames {
            writes.put(STAGING_SPACE, key.clone(), bytes.as_ref());
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: STAGING_SPACE,
                key: key.clone(),
            });
        }
        match storage
            .commit_partial_replica_write_set(
                super::super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Err(StorageWriteSetError::Storage(StorageError::PreconditionFailed(_))) => continue,
            result => return result.map(|_| ()).map_err(Into::into),
        }
    }
    Err(invalid("scratch renewal contention exceeded retry budget"))
}

pub(super) async fn reserve<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
) -> Result<(uuid::Uuid, Permit), LixError> {
    let id = uuid::Uuid::now_v7();
    let permit = Permit::acquire()?;
    reserve_owned(storage, state, id, permit)
        .await
        .map_err(|(error, _permit)| error)
}

pub(super) async fn reserve_owned<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    id: uuid::Uuid,
    permit: Permit,
) -> Result<(uuid::Uuid, Permit), (LixError, Permit)> {
    match std::panic::AssertUnwindSafe(reap_expired(storage))
        .catch_unwind()
        .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err((error, permit)),
        Err(_) => return Err((invalid("scratch reaper worker panicked"), permit)),
    }
    let reserved = async {
        for _ in 0..16 {
            let read = storage.begin_read(Default::default()).await?;
            let (mut ledger, expected) = load(&read).await?;
            let (actual, admission) =
                super::super::super::partial_state::load_partial_replica_state(&read)
                    .await?
                    .ok_or_else(|| invalid("partial admission is absent before staging"))?;
            if actual != *state {
                return Err(LixError::new(
                    super::super::super::runtime::PARTIAL_ADMISSION_CHANGED_CODE,
                    "scratch admission changed",
                ));
            }
            drop(read);
            let now = crate::telemetry::unix_time_ms();
            if ledger.len() >= REPOSITORY_OPERATIONS {
                return Err(LixError::new(
                    "LIX_NATIVE_RECIPE_WORK_BOUND",
                    "repository scratch operation quota exceeded",
                ));
            }
            ledger.insert(
                id.to_string(),
                Owner {
                    expires_at_ms: now.saturating_add(TTL_MS),
                    epoch: state.epoch_id().to_owned(),
                    reaping: false,
                },
            );
            if write(storage, &ledger, expected, Some(admission)).await? {
                return Ok(());
            }
        }
        Err(invalid(
            "scratch admission contention exceeded retry budget",
        ))
    };
    let reserved = match std::panic::AssertUnwindSafe(reserved)
        .catch_unwind()
        .await
    {
        Ok(result) => result,
        Err(_) => Err(invalid("scratch reservation worker panicked")),
    };
    match reserved {
        Ok(()) => Ok((id, permit)),
        Err(error) => {
            // A storage adapter may report an error after its durable commit
            // crossed the boundary. Keep both the UUID and process permit
            // until cleanup is acknowledged or a closed/fenced adapter hands
            // the durable record to the next exclusive owner's reaper.
            release_until_acknowledged(storage.clone(), id).await;
            Err((error, permit))
        }
    }
}

pub(super) async fn release_until_acknowledged<S: Storage + Clone + Send + Sync + 'static>(
    storage: StorageAdapter<S>,
    id: uuid::Uuid,
) -> bool {
    // A closed/fenced adapter cannot make any further writes. Keep the durable
    // owner record intact so the next exclusive owner can reap it, but let the
    // process-local permit go instead of retaining quota forever.
    let mut retry_delay_ms = 10;
    loop {
        let released = std::panic::AssertUnwindSafe(release(storage.clone(), id))
            .catch_unwind()
            .await;
        match released {
            Ok(Ok(())) => return true,
            Ok(Err(error))
                if error.code == LixError::CODE_STORAGE_CLOSED
                    || error.code == LixError::CODE_STORAGE_FENCED =>
            {
                return false;
            }
            _ => {}
        }
        super::super::super::platform::sleep(std::time::Duration::from_millis(retry_delay_ms))
            .await;
        retry_delay_ms = retry_delay_ms.saturating_mul(2).min(1_000);
    }
}
async fn clear_frames<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    id: uuid::Uuid,
) -> Result<(), LixError> {
    let range = StoragePrefix {
        bytes: Bytes::copy_from_slice(id.as_bytes()),
    }
    .to_range()?;
    for _ in 0..(MAX_RECORDS + MAX_PAYLOAD_BYTES / FRAME_BYTES).div_ceil(32) + 1 {
        let read = storage.begin_read(Default::default()).await?;
        let mut cursor = read
            .begin_scan(
                STAGING_SPACE,
                range.clone(),
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..Default::default()
                },
            )
            .await?;
        let (rows, _) = cursor.next_page(32).await?.into_parts();
        drop(cursor);
        drop(read);
        if rows.is_empty() {
            return Ok(());
        }
        let mut writes = storage.new_write_set();
        for row in rows {
            writes.delete(STAGING_SPACE, row.key);
        }
        storage
            .commit_partial_replica_write_set(
                super::super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;
    }
    Err(invalid("scratch reaping exceeded frame budget"))
}
pub(super) async fn release<S: Storage + Clone + Send + Sync + 'static>(
    storage: StorageAdapter<S>,
    id: uuid::Uuid,
) -> Result<(), LixError> {
    for _ in 0..16 {
        let read = storage.begin_read(Default::default()).await?;
        let (mut ledger, expected) = load(&read).await?;
        drop(read);
        let Some(owner) = ledger.get_mut(&id.to_string()) else {
            return Ok(());
        };
        if !owner.reaping {
            owner.reaping = true;
            let _ = write(&storage, &ledger, expected, None).await?;
            continue;
        }
        clear_frames(&storage, id).await?;
        ledger.remove(&id.to_string());
        if write(&storage, &ledger, expected, None).await? {
            return Ok(());
        }
    }
    Err(invalid("scratch release contention exceeded retry budget"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reaper_removes_expired_and_old_epoch_frames_without_touching_active_work() {
        let (storage, state, request) = super::super::tests::fixture().await;
        for old_epoch in [false, true] {
            let mut abandoned = super::super::tests::stage(&storage, &state, &request).await;
            let input = ReadInput {
                address: ReadInputAddress::BlobChunk(*blake3::hash(b"abandoned").as_bytes()),
                bytes: b"abandoned".to_vec(),
            };
            abandoned.append_page(vec![input]).await.unwrap();
            let mut active = super::super::tests::stage(&storage, &state, &request).await;
            let active_input = ReadInput {
                address: ReadInputAddress::BlobChunk(*blake3::hash(b"active").as_bytes()),
                bytes: b"active".to_vec(),
            };
            active.append_page(vec![active_input]).await.unwrap();
            let abandoned_key = abandoned.inputs[0].frames[0].clone();
            let active_key = active.inputs[0].frames[0].clone();
            let read = storage.begin_read(Default::default()).await.unwrap();
            let (mut ledger, expected) = load(&read).await.unwrap();
            drop(read);
            let owner = ledger.get_mut(&abandoned.id.to_string()).unwrap();
            if old_epoch {
                owner.epoch = uuid::Uuid::now_v7().to_string();
            } else {
                owner.expires_at_ms = 0;
            }
            assert!(write(&storage, &ledger, expected, None).await.unwrap());
            // Simulate a crashed process: leave its durable ownership and pages,
            // while releasing this test process's capacity permit.
            abandoned.released = true;
            abandoned.permit.take();
            drop(abandoned);
            reap_expired(&storage).await.unwrap();
            let read = storage.begin_read(Default::default()).await.unwrap();
            let (ledger, _) = load(&read).await.unwrap();
            assert_eq!(ledger.len(), 1);
            assert!(ledger.contains_key(&active.id.to_string()));
            let keys = [abandoned_key, active_key];
            let values = read
                .get_many(&[StorageGetManyRequest {
                    space: STAGING_SPACE,
                    keys: &keys,
                    opts: Default::default(),
                }])
                .await
                .unwrap()
                .values;
            assert!(values[0].is_none());
            assert!(values[1].is_some());
            drop(read);
            release(storage.clone(), active.id).await.unwrap();
            active.released = true;
            active.permit.take();
        }
    }
    #[tokio::test]
    async fn durable_reaping_fence_rejects_stale_append_and_cleanup_resumes_after_crash() {
        let (storage, state, request) = super::super::tests::fixture().await;
        let mut stage = super::super::tests::stage(&storage, &state, &request).await;
        stage
            .append_page(vec![ReadInput {
                address: ReadInputAddress::BlobChunk(*blake3::hash(b"before-fence").as_bytes()),
                bytes: b"before-fence".to_vec(),
            }])
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let (mut ledger, old_expected) = load(&read).await.unwrap();
        drop(read);
        ledger.get_mut(&stage.id.to_string()).unwrap().reaping = true;
        assert!(
            write(&storage, &ledger, old_expected.clone(), None)
                .await
                .unwrap()
        );
        // A writer that read ownership before the fence cannot publish after it.
        let stale_key = StorageKey(Bytes::from(
            [stage.id.as_bytes().as_slice(), b"late-frame"].concat(),
        ));
        let mut stale_writes = storage.new_write_set();
        stale_writes.put(STAGING_SPACE, stale_key.clone(), b"stale".as_slice());
        let result = storage
            .commit_partial_replica_write_set(
                super::super::super::super::partial_replica_write_capability(),
                stale_writes,
                StorageWriteOptions {
                    preconditions: vec![condition(old_expected)],
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            ))
        ));
        let error = commit_frames(
            &storage,
            &state,
            stage.id,
            &[(stale_key, Bytes::from_static(b"late"))],
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "LIX_READ_FULFILLMENT_RESTART");
        // The fence remains durable even if cleanup stops before its first page.
        reap_expired(&storage).await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(load(&read).await.unwrap().0.is_empty());
        let frames = read
            .get_many(&[StorageGetManyRequest {
                space: STAGING_SPACE,
                keys: &stage.inputs[0].frames,
                opts: Default::default(),
            }])
            .await
            .unwrap();
        assert!(frames.values.iter().all(Option::is_none));
        stage.released = true;
        stage.permit.take();
    }

    #[tokio::test]
    async fn final_reaping_fence_rejects_missing_or_already_fenced_owner() {
        let (storage, state, request) = super::super::tests::fixture().await;
        let mut stage = super::super::tests::stage(&storage, &state, &request).await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let missing = uuid::Uuid::now_v7();
        let error = stage_reaping_fence(
            &read,
            &state,
            missing,
            &mut writes,
            &mut preconditions,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "LIX_READ_FULFILLMENT_RESTART");
        drop(read);

        let read = storage.begin_read(Default::default()).await.unwrap();
        let (mut ledger, expected) = load(&read).await.unwrap();
        drop(read);
        ledger.get_mut(&stage.id.to_string()).unwrap().reaping = true;
        assert!(write(&storage, &ledger, expected, None).await.unwrap());

        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let error = stage_reaping_fence(
            &read,
            &state,
            stage.id,
            &mut writes,
            &mut preconditions,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "LIX_READ_FULFILLMENT_RESTART");
        drop(read);

        release(storage.clone(), stage.id).await.unwrap();
        stage.released = true;
        stage.permit.take();
    }

    #[tokio::test]
    async fn atomic_page_renewal_preserves_a_live_operation_beyond_its_previous_expiry() {
        let (storage, state, request) = super::super::tests::fixture().await;
        let mut stage = super::super::tests::stage(&storage, &state, &request).await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let (mut ledger, expected) = load(&read).await.unwrap();
        drop(read);
        ledger.get_mut(&stage.id.to_string()).unwrap().expires_at_ms = 0;
        assert!(write(&storage, &ledger, expected, None).await.unwrap());
        stage
            .append_page(vec![ReadInput {
                address: ReadInputAddress::BlobChunk(*blake3::hash(b"renewed").as_bytes()),
                bytes: b"renewed".to_vec(),
            }])
            .await
            .unwrap();
        reap_expired(&storage).await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let (ledger, _) = load(&read).await.unwrap();
        assert!(!ledger[&stage.id.to_string()].reaping);
        assert!(ledger[&stage.id.to_string()].expires_at_ms > crate::telemetry::unix_time_ms());
        let frames = read
            .get_many(&[StorageGetManyRequest {
                space: STAGING_SPACE,
                keys: &stage.inputs[0].frames,
                opts: Default::default(),
            }])
            .await
            .unwrap();
        assert!(frames.values.iter().all(Option::is_some));
        drop(read);
        release(storage.clone(), stage.id).await.unwrap();
        stage.released = true;
        stage.permit.take();
    }
    #[tokio::test]
    async fn newly_acquired_owner_reaps_abandoned_same_epoch_work_before_its_ttl() {
        let (storage, state, request) = super::super::tests::fixture().await;
        let mut abandoned = Vec::new();
        for index in 0..REPOSITORY_OPERATIONS {
            let mut stage = super::super::tests::stage(&storage, &state, &request).await;
            let bytes = vec![index as u8; 128];
            stage
                .append_page(vec![ReadInput {
                    address: ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes()),
                    bytes,
                }])
                .await
                .unwrap();
            stage.released = true;
            stage.permit.take();
            abandoned.push(stage.inputs[0].frames[0].clone());
        }
        reap_expired(&storage).await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert_eq!(load(&read).await.unwrap().0.len(), REPOSITORY_OPERATIONS);
        drop(read);
        reap_abandoned(&storage).await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(load(&read).await.unwrap().0.is_empty());
        assert!(
            read.get_many(&[StorageGetManyRequest {
                space: STAGING_SPACE,
                keys: &abandoned,
                opts: Default::default()
            }])
            .await
            .unwrap()
            .values
            .iter()
            .all(Option::is_none)
        );
        drop(read);
        let (id, permit) = reserve(&storage, &state).await.unwrap();
        release(storage.clone(), id).await.unwrap();
        drop(permit);
    }
}
