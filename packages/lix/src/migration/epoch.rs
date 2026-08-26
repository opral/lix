use std::{
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;

use crate::engine::{Engine, EngineOptions};
use crate::storage_adapter::{
    EpochBank, PutBatch, PutEntry, REPOSITORY_EPOCH_KEY, REPOSITORY_EPOCH_SPACE,
    Storage, StorageAdapter, StorageAdapterRead as _,
    StorageBeginScanOptions as BeginScanOptions, StorageCoreProjection as CoreProjection,
    StorageError, StorageGetManyRequest as GetManyRequest, StorageGetOptions as GetOptions,
    StorageKey as Key, StorageKeyRange as KeyRange, StoragePrecondition as Precondition,
    StorageProjectedValue as ProjectedValue, StorageRead, StorageReadOptions as ReadOptions,
    StorageValue as StoredValue, StorageWrite, StorageWriteOptions as WriteOptions,
};
use crate::{LixError, OpenMigrationReport, OpenPhase, OpenProgress, OpenProgressSink, OpenReport};

const POINTER_PREFIX: &str = "lix.repository-epoch.v1";
const LEGACY_FENCE: &[u8] = b"tracked-default-branch.v76-epoch-migrating";
const REPOSITORY_EPOCH_LEASE_KEY: &[u8] = b"lease";
const REPOSITORY_EPOCH_SOURCE_MARKER_KEY: &[u8] = b"source-marker";
const REPOSITORY_LEGACY_RETIRED_KEY: &[u8] = b"legacy-retired";
#[cfg(not(test))]
const MIGRATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const MIGRATION_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);
const MISSED_HEARTBEATS_BEFORE_RECOVERY: usize = 10;

pub(crate) struct EpochAdmission<S> {
    pub(crate) adapter: StorageAdapter<S>,
    pub(crate) report: OpenReport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PointerState {
    Active {
        bank: EpochBank,
        generation: u64,
        format: u32,
    },
    Migrating {
        source: EpochBank,
        source_format: u32,
        target: EpochBank,
        generation: u64,
        attempt: uuid::Uuid,
    },
}

pub(crate) async fn admit_repository<S>(
    storage: &S,
    progress: Option<&Arc<dyn OpenProgressSink>>,
) -> Result<EpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    'admission: loop {
        match load_pointer(storage).await? {
            Some((PointerState::Active { bank, generation, format }, bytes)) => {
                if format > crate::init::CURRENT_FORMAT_VERSION {
                    return Err(epoch_error(format!(
                        "repository epoch format v{format} is newer than this engine's v{}",
                        crate::init::CURRENT_FORMAT_VERSION
                    )));
                }
                if format < crate::init::CURRENT_FORMAT_VERSION {
                    return migrate_active(storage, bank, generation, format, bytes, progress).await;
                }
                if generation >= 2 {
                    let _ = schedule_legacy_retirement(storage.clone(), bytes.clone());
                }
                return Ok(EpochAdmission {
                    adapter: StorageAdapter::for_epoch(storage.clone(), bank, bytes),
                    report: OpenReport {
                        format,
                        initialized: false,
                        migration: None,
                    },
                });
            }
            Some((state @ PointerState::Migrating { .. }, bytes)) => {
                let PointerState::Migrating { source_format, .. } = state else {
                    unreachable!()
                };
                emit_migrating(progress, source_format);
                // A renewable lease distinguishes a slow live owner from an
                // interrupted one without trusting process clocks. Recovery
                // CASes both the exact pointer and the last observed lease.
                let mut observed_lease = load_lease(storage).await?;
                let observed_source_marker = load_source_marker(storage).await?;
                let mut missed = 0;
                loop {
                    portable_sleep(MIGRATION_HEARTBEAT_INTERVAL).await?;
                    match load_pointer(storage).await? {
                        Some((_, current)) if current == bytes => {}
                        _ => continue 'admission,
                    }
                    let current_lease = load_lease(storage).await?;
                    if current_lease != observed_lease {
                        observed_lease = current_lease;
                        missed = 0;
                        continue;
                    }
                    missed += 1;
                    if missed >= MISSED_HEARTBEATS_BEFORE_RECOVERY {
                        break;
                    }
                }
                if let Err(error) = recover_interrupted_migration(
                    storage,
                    state,
                    &bytes,
                    observed_lease.as_ref(),
                    observed_source_marker.as_ref(),
                )
                .await
                {
                    // A concurrent recovery or the original owner may have
                    // won the exact-pointer CAS. Reinspect before surfacing a
                    // genuine storage failure.
                    match load_pointer(storage).await? {
                        Some((_, current)) if current == bytes => {
                            if load_lease(storage).await? != observed_lease {
                                continue 'admission;
                            }
                            return Err(error);
                        }
                        _ => continue 'admission,
                    }
                }
            }
            None => return admit_legacy(storage, progress).await,
        }
    }
}

fn emit_migrating(progress: Option<&Arc<dyn OpenProgressSink>>, from_format: u32) {
    crate::handle::emit_open_progress(
        progress,
        OpenProgress {
            phase: OpenPhase::Migrating,
            from_format: (from_format != 0).then_some(from_format),
            to_format: crate::init::CURRENT_FORMAT_VERSION,
            completed: Some(0),
            total: None,
        },
    );
}

fn emit_validating(progress: Option<&Arc<dyn OpenProgressSink>>, from_format: u32) {
    crate::handle::emit_open_progress(
        progress,
        OpenProgress {
            phase: OpenPhase::Validating,
            from_format: (from_format != 0).then_some(from_format),
            to_format: crate::init::CURRENT_FORMAT_VERSION,
            completed: None,
            total: None,
        },
    );
}

async fn recover_interrupted_migration<S>(
    storage: &S,
    state: PointerState,
    migrating_bytes: &Bytes,
    observed_lease: Option<&Bytes>,
    observed_source_marker: Option<&Bytes>,
) -> Result<(), LixError>
where
    S: Storage,
{
    let PointerState::Migrating {
        source,
        source_format,
        generation,
        ..
    } = state
    else {
        return Err(epoch_error("cannot recover a non-migrating epoch pointer"));
    };
    let mut preconditions = vec![Precondition::KeyValueEquals {
        space: REPOSITORY_EPOCH_SPACE,
        key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
        expected: migrating_bytes.clone(),
    }];
    preconditions.push(match observed_lease {
        Some(expected) => Precondition::KeyValueEquals {
            space: REPOSITORY_EPOCH_SPACE,
            key: Key(Bytes::from_static(REPOSITORY_EPOCH_LEASE_KEY)),
            expected: expected.clone(),
        },
        None => Precondition::KeyAbsent {
            space: REPOSITORY_EPOCH_SPACE,
            key: Key(Bytes::from_static(REPOSITORY_EPOCH_LEASE_KEY)),
        },
    });
    if source == EpochBank::Legacy && source_format != 0 {
        let marker = observed_source_marker.ok_or_else(|| {
            epoch_error("interrupted legacy migration lost its exact source marker")
        })?;
        preconditions.push(Precondition::KeyValueEquals {
            space: REPOSITORY_EPOCH_SPACE,
            key: Key(Bytes::from_static(REPOSITORY_EPOCH_SOURCE_MARKER_KEY)),
            expected: marker.clone(),
        });
    }
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions,
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    match (source, source_format) {
        (EpochBank::Legacy, 0) => {
            delete_epoch_control(&mut write).await.map_err(storage_error)?;
        }
        (EpochBank::Legacy, _format) => {
            let marker = observed_source_marker.ok_or_else(|| {
                epoch_error("interrupted legacy migration lost its exact source marker")
            })?;
            write
                .put_many(
                    crate::init::REPOSITORY_PROTOCOL_SPACE,
                    single_put(
                        crate::init::REPOSITORY_PROTOCOL_KEY,
                        marker.clone(),
                    ),
                )
                .await
                .map_err(storage_error)?;
            delete_epoch_control(&mut write).await.map_err(storage_error)?;
            crate::storage_adapter::stage_mutation_revision(&mut write)
                .await
                .map_err(storage_error)?;
        }
        (bank, format) => {
            put_pointer(
                &mut write,
                encode_pointer(PointerState::Active {
                    bank,
                    generation: generation.saturating_sub(1),
                    format,
                }),
            )
            .await
            .map_err(storage_error)?;
            delete_lease(&mut write).await.map_err(storage_error)?;
        }
    }
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn admit_legacy<S>(
    storage: &S,
    progress: Option<&Arc<dyn OpenProgressSink>>,
) -> Result<EpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let legacy_status = super::inspect_lix(storage).await?;
    if matches!(legacy_status, super::MigrationStatus::Missing) {
        let target = EpochBank::A;
        let migrating_bytes = encode_pointer(PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target,
            generation: 1,
            attempt: uuid::Uuid::now_v7(),
        });
        if let Err(error) = publish_migration_claim_absent(storage, &migrating_bytes).await {
            if is_claim_conflict(&error) {
                return Box::pin(admit_repository(storage, progress)).await;
            }
            return Err(storage_error(error));
        }
        let _heartbeat = match start_migration_heartbeat(storage.clone(), migrating_bytes.clone()) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                delete_pointer(storage, &migrating_bytes).await?;
                return Err(error);
            }
        };
        let candidate = StorageAdapter::for_epoch_migration(
            storage.clone(),
            target,
            migrating_bytes.clone(),
        );
        let initialize = async {
            clear_bank(&candidate).await?;
            Engine::initialize_with_adapter(candidate.clone(), None).await?;
            Ok::<(), LixError>(())
        }
        .await;
        if let Err(error) = initialize {
            delete_pointer(storage, &migrating_bytes).await?;
            return Err(error);
        }
        let active = PointerState::Active {
            bank: target,
            generation: 1,
            format: crate::init::CURRENT_FORMAT_VERSION,
        };
        let active_bytes = encode_pointer(active);
        replace_pointer(storage, &migrating_bytes, &active_bytes).await?;
        return Ok(EpochAdmission {
            adapter: StorageAdapter::for_epoch(storage.clone(), target, active_bytes),
            report: OpenReport {
                format: crate::init::CURRENT_FORMAT_VERSION,
                initialized: true,
                migration: None,
            },
        });
    }

    let from_format = match legacy_status {
        super::MigrationStatus::Current { version }
        | super::MigrationStatus::Required {
            from_version: version,
            ..
        } => version,
        super::MigrationStatus::TooNew { found_version, .. } => {
            return Err(epoch_error(format!(
                "repository v{found_version} is newer than this engine"
            )));
        }
        super::MigrationStatus::Malformed | super::MigrationStatus::Missing => {
            return Err(epoch_error("repository has no valid versioned protocol marker"));
        }
    };
    if from_format < 72
        || !super::registry::has_complete_migration_path(
            from_format,
            crate::init::CURRENT_FORMAT_VERSION,
        )
    {
        return Err(epoch_error(format!(
            "repository v{from_format} predates the v{} complete-snapshot commit format; no automatic upgrade is available",
            crate::init::CURRENT_FORMAT_VERSION
        )));
    }
    let original_marker = load_storage_value(
        storage,
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        crate::init::REPOSITORY_PROTOCOL_KEY,
    )
    .await?
    .ok_or_else(|| epoch_error("repository protocol marker disappeared during inspection"))?;
    emit_migrating(progress, from_format);
    let source = StorageAdapter::new(storage.clone());
    let target_bank = EpochBank::A;
    let source_revision = source.load_mutation_revision().await.map_err(storage_error)?;
    let migrating = PointerState::Migrating {
        source: EpochBank::Legacy,
        source_format: from_format,
        target: target_bank,
        generation: 1,
        attempt: uuid::Uuid::now_v7(),
    };
    let migrating_bytes = encode_pointer(migrating);
    if let Err(error) = claim_legacy(
        storage,
        source_revision,
        &original_marker,
        &migrating_bytes,
    )
    .await
    {
        if matches!(error, StorageError::PreconditionFailed(_) | StorageError::WriteConflict) {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        return Err(storage_error(error));
    }
    let _heartbeat = match start_migration_heartbeat(storage.clone(), migrating_bytes.clone()) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            rollback_legacy(storage, &migrating_bytes, &original_marker).await?;
            return Err(error);
        }
    };

    let target = StorageAdapter::for_epoch_migration(
        storage.clone(),
        target_bank,
        migrating_bytes.clone(),
    );

    let migration_source = StorageAdapter::for_epoch_migration(
        storage.clone(),
        EpochBank::Legacy,
        migrating_bytes.clone(),
    );
    let fenced_revision = migration_source
        .load_mutation_revision()
        .await
        .map_err(storage_error)?;
    let candidate_result = async {
        clear_bank(&target).await?;
        let _ = copy_repository(&migration_source, &target).await?;
        let mut marker_write = target.new_write_set();
        marker_write.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            original_marker.as_ref(),
        );
        target
            .commit_write_set(marker_write, WriteOptions::default())
            .await
            .map_err(|error| epoch_error(format!("candidate marker write failed: {error}")))?;
        super::migrate_lix_with_adapter(
            storage.clone(),
            target.clone(),
            super::MigrationOptions::default(),
        )
        .await?;
        emit_validating(progress, from_format);
        match super::inspect_lix_with_adapter(&target).await? {
            super::MigrationStatus::Current { .. } => {}
            status => {
                return Err(epoch_error(format!(
                    "candidate repository validation observed {status:?}"
                )));
            }
        }
        let engine = Engine::new_with_adapter(target.clone(), EngineOptions::new()).await?;
        drop(engine);
        Ok::<(), LixError>(())
    }
    .await;
    if let Err(error) = candidate_result {
        rollback_legacy(storage, &migrating_bytes, &original_marker).await?;
        return Err(error);
    }

    let active = PointerState::Active {
        bank: target_bank,
        generation: 1,
        format: crate::init::CURRENT_FORMAT_VERSION,
    };
    let active_bytes = encode_pointer(active);
    if let Err(error) = activate_legacy(
        storage,
        &migrating_bytes,
        fenced_revision,
        &active_bytes,
    )
    .await
    {
        rollback_legacy(storage, &migrating_bytes, &original_marker).await?;
        return Err(storage_error(error));
    }
    Ok(EpochAdmission {
        adapter: StorageAdapter::for_epoch(storage.clone(), target_bank, active_bytes),
        report: OpenReport {
            format: crate::init::CURRENT_FORMAT_VERSION,
            initialized: false,
            migration: Some(OpenMigrationReport {
                from_format,
                to_format: crate::init::CURRENT_FORMAT_VERSION,
            }),
        },
    })
}

async fn migrate_active<S>(
    storage: &S,
    source_bank: EpochBank,
    source_generation: u64,
    from_format: u32,
    active_source_bytes: Bytes,
    progress: Option<&Arc<dyn OpenProgressSink>>,
) -> Result<EpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if !super::registry::has_complete_migration_path(
        from_format,
        crate::init::CURRENT_FORMAT_VERSION,
    ) {
        return Err(epoch_error(format!(
            "repository epoch v{from_format} has no registered upgrade path to v{}",
            crate::init::CURRENT_FORMAT_VERSION
        )));
    }
    let source = StorageAdapter::for_epoch(
        storage.clone(),
        source_bank,
        active_source_bytes.clone(),
    );
    emit_migrating(progress, from_format);
    let target_bank = source_bank.alternate();
    let source_revision = source.load_mutation_revision().await.map_err(storage_error)?;
    let migrating = PointerState::Migrating {
        source: source_bank,
        source_format: from_format,
        target: target_bank,
        generation: source_generation.saturating_add(1),
        attempt: uuid::Uuid::now_v7(),
    };
    let migrating_bytes = encode_pointer(migrating);
    let mut claim = match source
        .begin_migration_write(WriteOptions {
            await_durable: true,
            preconditions: vec![StorageAdapter::<S>::mutation_revision_precondition(
                source_revision,
            )],
            ..WriteOptions::default()
        })
        .await
    {
        Ok(claim) => claim,
        Err(error) if is_claim_conflict(&error) => {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        Err(error) => return Err(storage_error(error)),
    };
    put_pointer(&mut claim, migrating_bytes.clone())
        .await
        .map_err(storage_error)?;
    put_lease(&mut claim, Bytes::from_static(b"0"))
        .await
        .map_err(storage_error)?;
    if let Err(error) = claim.commit().await {
        if is_claim_conflict(&error) {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        return Err(storage_error(error));
    }
    let _heartbeat = match start_migration_heartbeat(storage.clone(), migrating_bytes.clone()) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            replace_pointer(storage, &migrating_bytes, &active_source_bytes).await?;
            return Err(error);
        }
    };

    let target = StorageAdapter::for_epoch_migration(
        storage.clone(),
        target_bank,
        migrating_bytes.clone(),
    );
    let migration_source = StorageAdapter::for_epoch_migration(
        storage.clone(),
        source_bank,
        migrating_bytes.clone(),
    );

    let candidate_result = async {
        clear_bank(&target).await?;
        let _ = copy_repository(&migration_source, &target).await?;
        super::migrate_lix_with_adapter(
            storage.clone(),
            target.clone(),
            super::MigrationOptions::default(),
        )
        .await?;
        emit_validating(progress, from_format);
        let engine = Engine::new_with_adapter(target.clone(), EngineOptions::new()).await?;
        drop(engine);
        Ok::<(), LixError>(())
    }
    .await;
    if let Err(error) = candidate_result {
        replace_pointer(storage, &migrating_bytes, &active_source_bytes).await?;
        return Err(error);
    }

    let active = PointerState::Active {
        bank: target_bank,
        generation: source_generation.saturating_add(1),
        format: crate::init::CURRENT_FORMAT_VERSION,
    };
    let active_bytes = encode_pointer(active);
    replace_pointer(storage, &migrating_bytes, &active_bytes).await?;
    // Keep the immediately previous bank for rollback. Once a later epoch has
    // activated, the pre-epoch layout is older than that rollback window and
    // can be reclaimed without making cleanup part of publication success.
    let _ = schedule_legacy_retirement(storage.clone(), active_bytes.clone());
    Ok(EpochAdmission {
        adapter: StorageAdapter::for_epoch(storage.clone(), target_bank, active_bytes),
        report: OpenReport {
            format: crate::init::CURRENT_FORMAT_VERSION,
            initialized: false,
            migration: Some(OpenMigrationReport {
                from_format,
                to_format: crate::init::CURRENT_FORMAT_VERSION,
            }),
        },
    })
}

fn schedule_legacy_retirement<S>(storage: S, active_pointer: Bytes) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    crate::background_task::spawn("lix-retire-legacy-repository-layout", move || async move {
        let _ = retire_legacy_layout(&storage, &active_pointer).await;
    })
}

async fn retire_legacy_layout<S>(storage: &S, active_pointer: &Bytes) -> Result<(), LixError>
where
    S: Storage + Clone,
{
    if load_storage_value(
        storage,
        REPOSITORY_EPOCH_SPACE,
        REPOSITORY_LEGACY_RETIRED_KEY,
    )
    .await?
    .is_some()
    {
        return Ok(());
    }
    let legacy = StorageAdapter::new(storage.clone());
    for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
        if space.id == REPOSITORY_EPOCH_SPACE.id {
            continue;
        }
        legacy
            .clear_space(
                space,
                WriteOptions {
                    await_durable: true,
                    preconditions: vec![Precondition::KeyValueEquals {
                        space: REPOSITORY_EPOCH_SPACE,
                        key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                        expected: active_pointer.clone(),
                    }],
                    ..WriteOptions::default()
                },
            )
            .await
            .map_err(storage_error)?;
    }
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![Precondition::KeyValueEquals {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                expected: active_pointer.clone(),
            }],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    write
        .put_many(
            REPOSITORY_EPOCH_SPACE,
            single_put(REPOSITORY_LEGACY_RETIRED_KEY, Bytes::from_static(b"1")),
        )
        .await
        .map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn clear_bank<S>(adapter: &StorageAdapter<S>) -> Result<(), LixError>
where
    S: Storage,
{
    for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
        if space.id == REPOSITORY_EPOCH_SPACE.id {
            continue;
        }
        adapter
            .clear_space(space, WriteOptions::default())
            .await
            .map_err(storage_error)?;
    }
    Ok(())
}

async fn copy_repository<S>(
    source: &StorageAdapter<S>,
    target: &StorageAdapter<S>,
) -> Result<Option<Bytes>, LixError>
where
    S: Storage,
{
    let read = source
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
    let revision = StorageAdapter::<S>::load_mutation_revision_from_read(&read)
        .await
        .map_err(storage_error)?;
    for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
        if space.id == REPOSITORY_EPOCH_SPACE.id {
            continue;
        }
        let mut cursor = read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions {
                    projection: CoreProjection::FullValue,
                    ..BeginScanOptions::default()
                },
            )
            .await
            .map_err(storage_error)?;
        while let Some(entries) = cursor.next_chunk().await.map_err(storage_error)? {
            let mut writes = target.new_write_set();
            for entry in entries {
                let ProjectedValue::FullValue(value) = entry.value else {
                    return Err(epoch_error("full-value epoch scan returned a key-only row"));
                };
                writes.put(space, entry.key, StoredValue { bytes: value });
            }
            target
                .commit_write_set(writes, WriteOptions::default())
                .await
                .map_err(|error| epoch_error(format!("copy target write failed: {error}")))?;
        }
    }
    Ok(revision)
}

async fn load_pointer<S>(storage: &S) -> Result<Option<(PointerState, Bytes)>, LixError>
where
    S: Storage + ?Sized,
{
    let read = storage.begin_read(ReadOptions::default()).await.map_err(storage_error)?;
    let keys = [Key(Bytes::from_static(REPOSITORY_EPOCH_KEY))];
    let values = read
        .get_many(&[GetManyRequest {
            space: REPOSITORY_EPOCH_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .map_err(storage_error)?;
    let Some(ProjectedValue::FullValue(bytes)) = values.values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let state = decode_pointer(&bytes)?;
    Ok(Some((state, bytes)))
}

async fn load_lease<S>(storage: &S) -> Result<Option<Bytes>, LixError>
where
    S: Storage + ?Sized,
{
    load_storage_value(
        storage,
        REPOSITORY_EPOCH_SPACE,
        REPOSITORY_EPOCH_LEASE_KEY,
    )
    .await
}

async fn load_source_marker<S>(storage: &S) -> Result<Option<Bytes>, LixError>
where
    S: Storage + ?Sized,
{
    load_storage_value(
        storage,
        REPOSITORY_EPOCH_SPACE,
        REPOSITORY_EPOCH_SOURCE_MARKER_KEY,
    )
    .await
}

async fn load_storage_value<S>(
    storage: &S,
    space: crate::storage_adapter::StorageSpace,
    key: &'static [u8],
) -> Result<Option<Bytes>, LixError>
where
    S: Storage + ?Sized,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
    let keys = [Key(Bytes::from_static(key))];
    let values = read
        .get_many(&[GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .map_err(storage_error)?;
    Ok(values
        .values
        .into_iter()
        .next()
        .flatten()
        .and_then(|value| match value {
            ProjectedValue::FullValue(bytes) => Some(bytes),
            ProjectedValue::KeyOnly => None,
        }))
}

struct MigrationHeartbeat {
    stopped: Arc<AtomicBool>,
}

impl Drop for MigrationHeartbeat {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
    }
}

fn start_migration_heartbeat<S>(
    storage: S,
    migrating: Bytes,
) -> Result<MigrationHeartbeat, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let stopped = Arc::new(AtomicBool::new(false));
    let task_stopped = Arc::clone(&stopped);
    let task = async move {
        let mut current = Bytes::from_static(b"0");
        let mut sequence = 1_u64;
        loop {
            heartbeat_sleep().await;
            if task_stopped.load(Ordering::Acquire) {
                break;
            }
            let next = Bytes::from(sequence.to_string());
            if advance_lease(&storage, &migrating, &current, &next)
                .await
                .is_err()
            {
                match (load_pointer(&storage).await, load_lease(&storage).await) {
                    (Ok(Some((_, pointer))), Ok(Some(lease)))
                        if pointer == migrating && lease == next =>
                    {
                        current = next;
                        sequence = sequence.saturating_add(1);
                        continue;
                    }
                    (Ok(Some((_, pointer))), Ok(Some(lease)))
                        if pointer == migrating && lease == current =>
                    {
                        continue;
                    }
                    (Err(_), _) | (_, Err(_)) => continue,
                    _ => break,
                }
            }
            current = next;
            sequence = sequence.saturating_add(1);
        }
    };
    crate::background_task::spawn("lix-repository-migration-heartbeat", move || task)?;
    Ok(MigrationHeartbeat { stopped })
}

async fn heartbeat_sleep() {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    std::thread::sleep(MIGRATION_HEARTBEAT_INTERVAL);
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    crate::sync::sleep(MIGRATION_HEARTBEAT_INTERVAL).await;
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn portable_sleep(duration: Duration) -> Result<(), LixError> {
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    crate::background_task::spawn("lix-repository-migration-wait", move || async move {
        std::thread::sleep(duration);
        let _ = done_tx.send(());
    })?;
    done_rx.await.map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "repository migration wait task stopped unexpectedly",
        )
    })
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn portable_sleep(duration: Duration) -> Result<(), LixError> {
    crate::sync::sleep(duration).await;
    Ok(())
}

async fn advance_lease<S>(
    storage: &S,
    migrating: &Bytes,
    current: &Bytes,
    next: &Bytes,
) -> Result<(), StorageError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REPOSITORY_EPOCH_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                    expected: migrating.clone(),
                },
                Precondition::KeyValueEquals {
                    space: REPOSITORY_EPOCH_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_EPOCH_LEASE_KEY)),
                    expected: current.clone(),
                },
            ],
            ..WriteOptions::default()
        })
        .await?;
    put_lease(&mut write, next.clone()).await?;
    write.commit().await?;
    Ok(())
}

async fn publish_pointer_absent<S>(storage: &S, active: &Bytes) -> Result<(), LixError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![Precondition::KeyAbsent {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
            }],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    put_pointer(&mut write, active.clone()).await.map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn publish_migration_claim_absent<S>(
    storage: &S,
    migrating: &Bytes,
) -> Result<(), StorageError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![Precondition::KeyAbsent {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
            }],
            ..WriteOptions::default()
        })
        .await?;
    put_pointer(&mut write, migrating.clone()).await?;
    put_lease(&mut write, Bytes::from_static(b"0")).await?;
    write.commit().await?;
    Ok(())
}

async fn claim_legacy<S>(
    storage: &S,
    revision: Option<Bytes>,
    marker: &Bytes,
    migrating: &Bytes,
) -> Result<(), StorageError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![
                Precondition::KeyAbsent {
                    space: REPOSITORY_EPOCH_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                },
                Precondition::KeyValueEquals {
                    space: crate::init::REPOSITORY_PROTOCOL_SPACE,
                    key: Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
                    expected: marker.clone(),
                },
                StorageAdapter::<S>::mutation_revision_precondition(revision),
            ],
            ..WriteOptions::default()
        })
        .await?;
    put_pointer(&mut write, migrating.clone()).await?;
    put_lease(&mut write, Bytes::from_static(b"0")).await?;
    write
        .put_many(
            REPOSITORY_EPOCH_SPACE,
            single_put(REPOSITORY_EPOCH_SOURCE_MARKER_KEY, marker.clone()),
        )
        .await?;
    write
        .put_many(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            single_put(crate::init::REPOSITORY_PROTOCOL_KEY, Bytes::from_static(LEGACY_FENCE)),
        )
        .await?;
    crate::storage_adapter::stage_mutation_revision(&mut write).await?;
    write.commit().await?;
    Ok(())
}

async fn activate_legacy<S>(
    storage: &S,
    migrating: &Bytes,
    source_revision: Option<Bytes>,
    active: &Bytes,
) -> Result<(), StorageError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REPOSITORY_EPOCH_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                    expected: migrating.clone(),
                },
                StorageAdapter::<S>::mutation_revision_precondition(source_revision),
            ],
            ..WriteOptions::default()
        })
        .await?;
    put_pointer(&mut write, active.clone()).await?;
    delete_lease(&mut write).await?;
    write.commit().await?;
    Ok(())
}

async fn rollback_legacy<S>(
    storage: &S,
    migrating: &Bytes,
    marker: &Bytes,
) -> Result<(), LixError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![Precondition::KeyValueEquals {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                expected: migrating.clone(),
            }],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    write
        .put_many(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            single_put(crate::init::REPOSITORY_PROTOCOL_KEY, marker.clone()),
        )
        .await
        .map_err(storage_error)?;
    delete_epoch_control(&mut write).await.map_err(storage_error)?;
    crate::storage_adapter::stage_mutation_revision(&mut write)
        .await
        .map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn replace_pointer<S>(storage: &S, expected: &Bytes, replacement: &Bytes) -> Result<(), LixError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![Precondition::KeyValueEquals {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                expected: expected.clone(),
            }],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    put_pointer(&mut write, replacement.clone())
        .await
        .map_err(storage_error)?;
    delete_lease(&mut write).await.map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn delete_pointer<S>(storage: &S, expected: &Bytes) -> Result<(), LixError>
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![Precondition::KeyValueEquals {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                expected: expected.clone(),
            }],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    delete_epoch_control(&mut write).await.map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn put_pointer<W>(write: &mut W, bytes: Bytes) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .put_many(REPOSITORY_EPOCH_SPACE, single_put(REPOSITORY_EPOCH_KEY, bytes))
        .await
}

async fn put_lease<W>(write: &mut W, bytes: Bytes) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .put_many(
            REPOSITORY_EPOCH_SPACE,
            single_put(REPOSITORY_EPOCH_LEASE_KEY, bytes),
        )
        .await
}

async fn delete_lease<W>(write: &mut W) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .delete_many(
            REPOSITORY_EPOCH_SPACE,
            &[
                Key(Bytes::from_static(REPOSITORY_EPOCH_LEASE_KEY)),
                Key(Bytes::from_static(REPOSITORY_EPOCH_SOURCE_MARKER_KEY)),
            ],
        )
        .await
}

async fn delete_epoch_control<W>(write: &mut W) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .delete_many(
            REPOSITORY_EPOCH_SPACE,
            &[
                Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                Key(Bytes::from_static(REPOSITORY_EPOCH_LEASE_KEY)),
                Key(Bytes::from_static(REPOSITORY_EPOCH_SOURCE_MARKER_KEY)),
            ],
        )
        .await
}

fn single_put(key: &'static [u8], value: Bytes) -> PutBatch {
    PutBatch {
        entries: vec![PutEntry {
            key: Key(Bytes::from_static(key)),
            value: StoredValue { bytes: value },
        }],
    }
}

fn encode_pointer(state: PointerState) -> Bytes {
    let text = match state {
        PointerState::Active {
            bank,
            generation,
            format,
        } => format!(
            "{POINTER_PREFIX}|active|{}|{generation}|{format}",
            bank_code(bank)
        ),
        PointerState::Migrating {
            source,
            source_format,
            target,
            generation,
            attempt,
        } => format!(
            "{POINTER_PREFIX}|migrating|{}|{}|{generation}|{source_format}|{attempt}",
            bank_code(source),
            bank_code(target),
        ),
    };
    Bytes::from(text)
}

fn decode_pointer(bytes: &Bytes) -> Result<PointerState, LixError> {
    let text = std::str::from_utf8(bytes).map_err(|_| epoch_error("epoch pointer is not UTF-8"))?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.first().copied() != Some(POINTER_PREFIX) {
        return Err(epoch_error("epoch pointer has an unsupported encoding"));
    }
    match parts.get(1).copied() {
        Some("active") if parts.len() == 5 => Ok(PointerState::Active {
            bank: parse_bank(parts[2])?,
            generation: parse_generation(parts[3])?,
            format: parse_format(parts[4])?,
        }),
        Some("migrating") if parts.len() == 7 => Ok(PointerState::Migrating {
            source: parse_bank(parts[2])?,
            target: parse_bank(parts[3])?,
            generation: parse_generation(parts[4])?,
            source_format: parse_format(parts[5])?,
            attempt: uuid::Uuid::parse_str(parts[6])
                .map_err(|_| epoch_error("epoch pointer migration attempt is invalid"))?,
        }),
        _ => Err(epoch_error("epoch pointer state is invalid")),
    }
}

fn parse_generation(value: &str) -> Result<u64, LixError> {
    value
        .parse::<u64>()
        .map_err(|_| epoch_error("epoch pointer generation is invalid"))
}

fn parse_format(value: &str) -> Result<u32, LixError> {
    value
        .parse::<u32>()
        .map_err(|_| epoch_error("epoch pointer format is invalid"))
}

fn bank_code(bank: EpochBank) -> &'static str {
    match bank {
        EpochBank::Legacy => "legacy",
        EpochBank::A => "a",
        EpochBank::B => "b",
    }
}

fn parse_bank(value: &str) -> Result<EpochBank, LixError> {
    match value {
        "legacy" => Ok(EpochBank::Legacy),
        "a" => Ok(EpochBank::A),
        "b" => Ok(EpochBank::B),
        _ => Err(epoch_error("epoch pointer bank is invalid")),
    }
}

fn epoch_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}

fn storage_error(error: StorageError) -> LixError {
    if matches!(error, StorageError::ReadExpired) {
        return LixError::from(error);
    }
    LixError::new(
        "LIX_ERROR_REPOSITORY_UPGRADE",
        format!("repository upgrade storage error: {error}"),
    )
}

fn is_claim_conflict(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::PreconditionFailed(_) | StorageError::WriteConflict
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageWriteOptions;

    async fn seed_active_v75(storage: &crate::Memory, bank: EpochBank, generation: u64) -> Bytes {
        let adapter = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        Engine::initialize_with_adapter(adapter.clone(), None)
            .await
            .unwrap();
        let mut writes = adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            crate::init::REPOSITORY_PROTOCOL_V75,
        );
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let active = encode_pointer(PointerState::Active {
            bank,
            generation,
            format: 75,
        });
        publish_pointer_absent(storage, &active).await.unwrap();
        active
    }

    #[test]
    fn pointer_encoding_round_trips() {
        for state in [
            PointerState::Active {
                bank: EpochBank::A,
                generation: 7,
                format: 76,
            },
            PointerState::Migrating {
                source: EpochBank::A,
                source_format: 75,
                target: EpochBank::B,
                generation: 8,
                attempt: uuid::Uuid::from_u128(1),
            },
        ] {
            let bytes = encode_pointer(state);
            assert_eq!(decode_pointer(&bytes).unwrap(), state);
        }

        let first = encode_pointer(PointerState::Migrating {
            source: EpochBank::A,
            source_format: 75,
            target: EpochBank::B,
            generation: 8,
            attempt: uuid::Uuid::from_u128(10),
        });
        let retry = encode_pointer(PointerState::Migrating {
            source: EpochBank::A,
            source_format: 75,
            target: EpochBank::B,
            generation: 8,
            attempt: uuid::Uuid::from_u128(11),
        });
        assert_ne!(first, retry, "migration retries must not reuse a fence");
    }

    #[tokio::test]
    async fn stale_epoch_adapter_is_fenced_after_pointer_change() {
        let storage = crate::Memory::new();
        let admitted = admit_repository(&storage, None).await.unwrap();
        let replacement = encode_pointer(PointerState::Active {
            bank: EpochBank::B,
            generation: 2,
            format: crate::init::CURRENT_FORMAT_VERSION,
        });
        let mut raw = storage.begin_write(WriteOptions::default()).await.unwrap();
        put_pointer(&mut raw, replacement).await.unwrap();
        raw.commit().await.unwrap();

        match admitted.adapter.begin_read(ReadOptions::default()).await {
            Err(error) => assert_eq!(error, StorageError::Fenced),
            Ok(_) => panic!("stale epoch adapter must not admit a new read"),
        }
        assert_eq!(
            admitted.adapter.load_mutation_revision().await.unwrap_err(),
            StorageError::Fenced
        );

        let mut writes = admitted.adapter.new_write_set();
        writes.put(
            crate::json_store::JSON_SPACE,
            &b"stale"[..],
            &b"write"[..],
        );
        let error = admitted
            .adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));
    }

    #[tokio::test]
    async fn retried_migration_fences_every_capability_from_the_previous_attempt() {
        let storage = crate::Memory::new();
        let first = encode_pointer(PointerState::Migrating {
            source: EpochBank::A,
            source_format: 75,
            target: EpochBank::B,
            generation: 8,
            attempt: uuid::Uuid::from_u128(20),
        });
        publish_migration_claim_absent(&storage, &first)
            .await
            .unwrap();
        let stale = StorageAdapter::for_epoch_migration(
            storage.clone(),
            EpochBank::B,
            first.clone(),
        );
        let retry = encode_pointer(PointerState::Migrating {
            source: EpochBank::A,
            source_format: 75,
            target: EpochBank::B,
            generation: 8,
            attempt: uuid::Uuid::from_u128(21),
        });
        replace_pointer(&storage, &first, &retry).await.unwrap();

        match stale.begin_read(ReadOptions::default()).await {
            Err(error) => assert_eq!(error, StorageError::Fenced),
            Ok(_) => panic!("a prior migration attempt must not admit a new read"),
        }
        assert_eq!(
            stale.load_mutation_revision().await.unwrap_err(),
            StorageError::Fenced
        );
        let mut writes = stale.new_write_set();
        writes.put(crate::json_store::JSON_SPACE, &b"stale"[..], &b"write"[..]);
        assert!(matches!(
            stale
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .unwrap_err(),
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));
    }

    #[tokio::test]
    async fn failed_candidate_validation_restores_legacy_marker() {
        let storage = crate::Memory::new();
        let mut raw = storage.begin_write(WriteOptions::default()).await.unwrap();
        raw.put_many(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            single_put(
                crate::init::REPOSITORY_PROTOCOL_KEY,
                Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V75),
            ),
        )
        .await
        .unwrap();
        raw.commit().await.unwrap();

        let error = match admit_repository(&storage, None).await {
            Ok(_) => panic!("incomplete candidate must fail validation"),
            Err(error) => error,
        };
        assert!(error.message.contains("not initialized"));
        assert!(load_pointer(&storage).await.unwrap().is_none());
        assert_eq!(
            super::super::inspect_lix(&storage).await.unwrap(),
            super::super::MigrationStatus::Required {
                from_version: 75,
                to_version: crate::init::CURRENT_FORMAT_VERSION,
            }
        );
    }

    #[tokio::test]
    async fn interrupted_fresh_claim_rolls_back_for_a_clean_retry() {
        let storage = crate::Memory::new();
        let state = PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(2),
        };
        let bytes = encode_pointer(state);
        publish_migration_claim_absent(&storage, &bytes)
            .await
            .unwrap();

        let lease = load_lease(&storage).await.unwrap().unwrap();
        recover_interrupted_migration(&storage, state, &bytes, Some(&lease), None)
            .await
            .unwrap();
        assert!(load_pointer(&storage).await.unwrap().is_none());
        let admitted = admit_repository(&storage, None).await.unwrap();
        assert!(admitted.report.initialized);
    }

    #[tokio::test]
    async fn interrupted_legacy_claim_restores_the_exact_transitional_marker() {
        let storage = crate::Memory::new();
        let marker = Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V72_COMMIT_REWRITE);
        let mut seed = storage.begin_write(WriteOptions::default()).await.unwrap();
        seed.put_many(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            single_put(crate::init::REPOSITORY_PROTOCOL_KEY, marker.clone()),
        )
        .await
        .unwrap();
        seed.commit().await.unwrap();
        let state = PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 72,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(3),
        };
        let bytes = encode_pointer(state);
        claim_legacy(&storage, None, &marker, &bytes)
            .await
            .unwrap();
        let lease = load_lease(&storage).await.unwrap().unwrap();
        let stored_marker = load_source_marker(&storage).await.unwrap().unwrap();

        recover_interrupted_migration(
            &storage,
            state,
            &bytes,
            Some(&lease),
            Some(&stored_marker),
        )
        .await
        .unwrap();

        assert_eq!(
            load_storage_value(
                &storage,
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                crate::init::REPOSITORY_PROTOCOL_KEY,
            )
            .await
            .unwrap(),
            Some(marker),
        );
        assert!(load_pointer(&storage).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn interrupted_active_claim_restores_source_then_upgrades() {
        let storage = crate::Memory::new();
        let active = seed_active_v75(&storage, EpochBank::A, 7).await;
        let state = PointerState::Migrating {
            source: EpochBank::A,
            source_format: 75,
            target: EpochBank::B,
            generation: 8,
            attempt: uuid::Uuid::from_u128(4),
        };
        let migrating = encode_pointer(state);
        replace_pointer(&storage, &active, &migrating).await.unwrap();

        let lease = Bytes::from_static(b"0");
        let mut raw = storage.begin_write(WriteOptions::default()).await.unwrap();
        put_lease(&mut raw, lease.clone()).await.unwrap();
        raw.commit().await.unwrap();
        recover_interrupted_migration(&storage, state, &migrating, Some(&lease), None)
            .await
            .unwrap();
        assert_eq!(load_pointer(&storage).await.unwrap().unwrap().1, active);

        let admitted = admit_repository(&storage, None).await.unwrap();
        assert_eq!(admitted.adapter.epoch_bank(), EpochBank::B);
        assert_eq!(
            admitted.report.migration,
            Some(OpenMigrationReport {
                from_format: 75,
                to_format: 76,
            })
        );
    }

    #[tokio::test]
    async fn concurrent_fresh_opens_converge_on_one_active_epoch() {
        let storage = crate::Memory::new();
        let (first, second) = tokio::join!(
            admit_repository(&storage, None),
            admit_repository(&storage, None)
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.adapter.epoch_bank(), EpochBank::A);
        assert_eq!(second.adapter.epoch_bank(), EpochBank::A);
        assert_ne!(first.report.initialized, second.report.initialized);
    }

    #[tokio::test]
    async fn admission_recovers_a_claim_whose_heartbeat_stopped() {
        let storage = crate::Memory::new();
        let state = PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(5),
        };
        let bytes = encode_pointer(state);
        publish_migration_claim_absent(&storage, &bytes)
            .await
            .unwrap();

        let admitted = admit_repository(&storage, None).await.unwrap();
        assert!(admitted.report.initialized);
        assert!(matches!(
            load_pointer(&storage).await.unwrap().unwrap().0,
            PointerState::Active { .. }
        ));
    }

    #[tokio::test]
    async fn live_heartbeat_prevents_recovery_of_a_slow_owner() {
        let storage = crate::Memory::new();
        let state = PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(6),
        };
        let bytes = encode_pointer(state);
        publish_migration_claim_absent(&storage, &bytes)
            .await
            .unwrap();
        let heartbeat = start_migration_heartbeat(storage.clone(), bytes.clone()).unwrap();
        let waiting_storage = storage.clone();
        let waiter = tokio::spawn(async move { admit_repository(&waiting_storage, None).await });

        crate::sync::sleep(Duration::from_millis(250)).await;
        assert_eq!(load_pointer(&storage).await.unwrap().unwrap().1, bytes);
        drop(heartbeat);
        delete_pointer(&storage, &bytes).await.unwrap();

        let admitted = waiter.await.unwrap().unwrap();
        assert!(admitted.report.initialized);
    }
}
