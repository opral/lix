use std::{ops::Bound, sync::Arc, time::Duration};

use bytes::Bytes;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use futures_util::{FutureExt as _, select_biased};

use crate::LixError;
use crate::engine::{Engine, EngineOptions};
use crate::open_types::{
    OpenMigrationReport, OpenPhase, OpenProgress, OpenProgressSink, OpenReport, emit_open_progress,
};
use crate::storage_adapter::{
    EpochBank, MAX_SCAN_PAGE_ROWS, MemoryRead, MemoryWrite, PutBatch, PutEntry,
    REPOSITORY_EPOCH_KEY, REPOSITORY_EPOCH_SPACE, Storage, StorageAdapter, StorageAdapterRead as _,
    StorageBeginScanOptions as BeginScanOptions, StorageCommitResult as CommitResult,
    StorageCoreProjection as CoreProjection, StorageError, StorageGetManyRequest as GetManyRequest,
    StorageGetManyResult as GetManyResult, StorageGetOptions as GetOptions, StorageKey as Key,
    StorageKeyRange as KeyRange, StoragePrecondition as Precondition,
    StorageProjectedValue as ProjectedValue, StorageRead, StorageReadEntry as ReadEntry,
    StorageReadOptions as ReadOptions, StorageScanChunk as ScanChunk,
    StorageScanCursor as ScanCursor, StorageScanSource, StorageSessionToken, StorageSpace,
    StorageValue as StoredValue, StorageWrite, StorageWriteOptions as WriteOptions,
};

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

fn durable_candidate_write_options() -> WriteOptions {
    WriteOptions {
        // A durable epoch pointer must never outlive candidate rows or the
        // deletion of stale rows in the bank it makes reachable.
        await_durable: true,
        ..WriteOptions::default()
    }
}

fn epoch_data_spaces() -> impl Iterator<Item = StorageSpace> {
    crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
        .iter()
        .copied()
}

pub(crate) struct EpochAdmission<S> {
    pub(crate) adapter: StorageAdapter<S>,
    pub(crate) report: OpenReport,
}

/// Owns one hidden, durably fenced epoch while a complete external state is
/// imported. Only the exact-pointer publication makes the candidate visible.
pub(crate) struct FreshEpochImport<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    storage: S,
    candidate: StorageAdapter<S>,
    claim: Bytes,
    publication: uuid::Uuid,
    heartbeat: Option<MigrationHeartbeat>,
    cleanup: Option<FreshEpochCleanup>,
}

enum FreshEpochCleanupCommand {
    Cleanup,
    Disarm,
}

struct FreshEpochCleanup {
    command: Option<tokio::sync::oneshot::Sender<FreshEpochCleanupCommand>>,
    claimed: Option<tokio::sync::oneshot::Receiver<Result<(), LixError>>>,
    done: Option<tokio::sync::oneshot::Receiver<Result<(), LixError>>>,
}

impl FreshEpochCleanup {
    async fn wait_for_claim(&mut self) -> Result<(), LixError> {
        self.claimed
            .take()
            .expect("fresh epoch claim completion receiver is present")
            .await
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "snapshot restore claim task stopped without reporting completion",
                )
            })?
    }

    async fn cleanup(mut self) -> Result<(), LixError> {
        let command = self
            .command
            .take()
            .expect("fresh epoch cleanup command sender is present");
        command
            .send(FreshEpochCleanupCommand::Cleanup)
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "snapshot restore cleanup task stopped before receiving cancellation",
                )
            })?;
        self.done
            .take()
            .expect("fresh epoch cleanup completion receiver is present")
            .await
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "snapshot restore cleanup task stopped without reporting completion",
                )
            })?
    }

    fn disarm(mut self) {
        if let Some(command) = self.command.take() {
            let _ = command.send(FreshEpochCleanupCommand::Disarm);
        }
    }
}

impl Drop for FreshEpochCleanup {
    fn drop(&mut self) {
        // Closing the command channel is the cancellation signal. The cleanup
        // task is started before the claim is attempted, so Drop never needs
        // to create a task or block on asynchronous storage work.
        self.command.take();
    }
}

pub(crate) async fn begin_fresh_epoch_import<S>(storage: S) -> Result<FreshEpochImport<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let target = EpochBank::A;
    let publication = uuid::Uuid::now_v7();
    let claim = encode_pointer(PointerState::Migrating {
        source: EpochBank::Legacy,
        source_format: 0,
        target,
        generation: 1,
        attempt: publication,
    });
    let candidate = StorageAdapter::for_epoch_migration(storage.clone(), target, claim.clone());
    let mut cleanup = start_fresh_epoch_cleanup(storage.clone(), candidate.clone(), claim.clone())?;
    cleanup.wait_for_claim().await?;
    let heartbeat = match start_migration_heartbeat(storage.clone(), claim.clone()) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            let cleaned = cleanup.cleanup().await;
            return Err(with_cleanup_error(error, cleaned));
        }
    };
    if let Err(error) = clear_bank(&candidate).await {
        let cleaned = cleanup.cleanup().await;
        let stopped = heartbeat.stop().await;
        return Err(with_cleanup_error(
            error,
            combine_cleanup_results(Ok(()), cleaned, stopped),
        ));
    }
    Ok(FreshEpochImport {
        storage,
        candidate,
        claim,
        publication,
        heartbeat: Some(heartbeat),
        cleanup: Some(cleanup),
    })
}

impl<S> FreshEpochImport<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn candidate(&self) -> &StorageAdapter<S> {
        &self.candidate
    }

    pub(crate) async fn write_exact_batch(
        &self,
        space: StorageSpace,
        batch: PutBatch,
    ) -> Result<(), LixError> {
        let mut write = self
            .candidate
            .begin_migration_write(durable_candidate_write_options())
            .await
            .map_err(storage_error)?;
        write.put_many(space, batch).await.map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok(())
    }

    pub(crate) async fn publish(mut self, format: u32) -> Result<S, LixError> {
        let active_bytes = encode_pointer(PointerState::Active {
            bank: EpochBank::A,
            generation: 1,
            format,
            publication: Some(self.publication),
        });
        if let Err(error) = replace_pointer(&self.storage, &self.claim, &active_bytes).await {
            if error.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN {
                if let Some(heartbeat) = self.heartbeat.take() {
                    let _ = heartbeat.stop().await;
                }
                return Err(error);
            }
            match load_pointer(&self.storage).await {
                Ok(Some((_, bytes))) if bytes == active_bytes => {}
                Ok(Some((_, bytes))) if bytes == self.claim => {
                    let cleanup = self.abort().await;
                    return Err(with_cleanup_error(error, cleanup));
                }
                Ok(_) => {
                    if let Some(heartbeat) = self.heartbeat.take() {
                        let _ = heartbeat.stop().await;
                    }
                    return Err(error);
                }
                Err(inspect_error) => {
                    if let Some(heartbeat) = self.heartbeat.take() {
                        let _ = heartbeat.stop().await;
                    }
                    return Err(LixError::new(
                        error.code.clone(),
                        format!(
                            "{error}; could not resolve snapshot publication outcome: {inspect_error}"
                        ),
                    ));
                }
            }
        }
        // Publication is now exact and durable. Cancellation while joining the
        // heartbeat must not tear down the epoch that was just made active.
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.disarm();
        }
        if let Some(heartbeat) = self.heartbeat.take() {
            // The active pointer is the durable commit point. A heartbeat join
            // failure after it cannot turn a committed restore into a failure.
            let _ = heartbeat.stop().await;
        }
        Ok(self.storage)
    }

    pub(crate) async fn abort(mut self) -> Result<(), LixError> {
        let cleaned = match self.cleanup.take() {
            Some(cleanup) => cleanup.cleanup().await,
            None => Ok(()),
        };
        let stopped = match self.heartbeat.take() {
            Some(heartbeat) => heartbeat.stop().await,
            None => Ok(()),
        };
        combine_cleanup_results(Ok(()), cleaned, stopped)
    }
}

fn start_fresh_epoch_cleanup<S>(
    storage: S,
    candidate: StorageAdapter<S>,
    claim: Bytes,
) -> Result<FreshEpochCleanup, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (command, receive_command) = tokio::sync::oneshot::channel();
    let (report_claimed, claimed) = tokio::sync::oneshot::channel();
    let (report_done, done) = tokio::sync::oneshot::channel();
    crate::background_task::spawn(
        "lix-snapshot-restore-cleanup",
        move || async move {
            // Claim acquisition is owned by this task, not by the restore
            // future. JavaScript promises and remote commits can continue after
            // their Rust waiter is dropped; keeping the waiter here guarantees
            // that cancellation cannot run cleanup against an absent pointer
            // and then let an in-flight claim appear after cleanup has exited.
            if let Err(error) = claim_fresh_import(&storage, &claim).await {
                let cleaned = cleanup_fresh_epoch_claim(&storage, &candidate, &claim).await;
                let error = with_cleanup_error(error, cleaned);
                let _ = report_done.send(Ok(()));
                let _ = report_claimed.send(Err(error));
                return;
            }
            if report_claimed.send(Ok(())).is_err() {
                // The restore was cancelled before it observed ownership. The
                // claim is now settled and exact, so it is safe to remove it.
                let result = cleanup_fresh_epoch_claim(&storage, &candidate, &claim).await;
                let _ = report_done.send(result);
                return;
            }
            let should_cleanup = !matches!(
                receive_command.await,
                Ok(FreshEpochCleanupCommand::Disarm)
            );
            let result = if should_cleanup {
                cleanup_fresh_epoch_claim(&storage, &candidate, &claim).await
            } else {
                Ok(())
            };
            let _ = report_done.send(result);
        },
    )?;
    Ok(FreshEpochCleanup {
        command: Some(command),
        claimed: Some(claimed),
        done: Some(done),
    })
}

async fn cleanup_fresh_epoch_claim<S>(
    storage: &S,
    candidate: &StorageAdapter<S>,
    claim: &Bytes,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let cleared = clear_bank(&candidate).await;
    // Always release the exact claim. A later admission clears its hidden
    // target before use, so stale candidate rows are safer than an orphaned
    // epoch-control pointer that makes restore permanently unretryable.
    let deleted = delete_pointer_resolving_outcome(storage, claim).await;
    combine_cleanup_results(cleared, deleted, Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PointerState {
    Active {
        bank: EpochBank,
        generation: u64,
        format: u32,
        publication: Option<uuid::Uuid>,
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
            Some((
                PointerState::Active {
                    bank,
                    generation,
                    format,
                    ..
                },
                bytes,
            )) => {
                if format > crate::init::CURRENT_FORMAT_VERSION {
                    return Err(epoch_error(format!(
                        "repository epoch format v{format} is newer than this engine's v{}",
                        crate::init::CURRENT_FORMAT_VERSION
                    )));
                }
                if format < crate::init::CURRENT_FORMAT_VERSION {
                    return migrate_active(storage, bank, generation, format, bytes, progress)
                        .await;
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
    emit_open_progress(
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
    emit_open_progress(
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
    S: Storage + Clone,
{
    let PointerState::Migrating {
        source,
        source_format,
        target,
        generation,
        ..
    } = state
    else {
        return Err(epoch_error("cannot recover a non-migrating epoch pointer"));
    };
    if source == EpochBank::Legacy && source_format == 0 {
        return recover_interrupted_fresh_import(
            storage,
            target,
            generation,
            migrating_bytes,
            observed_lease,
        )
        .await;
    }
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
    let mut restored_active = None;
    match (source, source_format) {
        (EpochBank::Legacy, 0) => unreachable!("fresh import recovery returns above"),
        (EpochBank::Legacy, _format) => {
            let marker = observed_source_marker.ok_or_else(|| {
                epoch_error("interrupted legacy migration lost its exact source marker")
            })?;
            write
                .put_many(
                    crate::init::REPOSITORY_PROTOCOL_SPACE,
                    single_put(crate::init::REPOSITORY_PROTOCOL_KEY, marker.clone()),
                )
                .await
                .map_err(storage_error)?;
            delete_epoch_control(&mut write)
                .await
                .map_err(storage_error)?;
            crate::storage_adapter::stage_mutation_revision(&mut write)
                .await
                .map_err(storage_error)?;
        }
        (bank, format) => {
            let active = encode_pointer(PointerState::Active {
                bank,
                generation: generation.saturating_sub(1),
                format,
                publication: None,
            });
            put_pointer(&mut write, active.clone())
                .await
                .map_err(storage_error)?;
            delete_lease(&mut write).await.map_err(storage_error)?;
            restored_active = Some(active);
        }
    }
    let commit = write.commit().await;
    if let Some(active) = restored_active {
        resolve_exact_pointer_commit(storage, commit, &active)
            .await
            .map_err(storage_error)?;
    } else {
        commit.map_err(storage_error)?;
    }
    Ok(())
}

async fn recover_interrupted_fresh_import<S>(
    storage: &S,
    target: EpochBank,
    generation: u64,
    migrating_bytes: &Bytes,
    observed_lease: Option<&Bytes>,
) -> Result<(), LixError>
where
    S: Storage + Clone,
{
    // Recovery must own a distinct pointer before touching the candidate. The
    // prior importer and all of its adapters are fenced by this transition.
    let recovery_bytes = encode_pointer(PointerState::Migrating {
        source: EpochBank::Legacy,
        source_format: 0,
        target,
        generation,
        attempt: uuid::Uuid::now_v7(),
    });
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
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions,
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    put_pointer(&mut write, recovery_bytes.clone())
        .await
        .map_err(storage_error)?;
    put_lease(&mut write, Bytes::from_static(b"0"))
        .await
        .map_err(storage_error)?;
    resolve_exact_pointer_commit(storage, write.commit().await, &recovery_bytes)
        .await
        .map_err(storage_error)?;

    let recovery =
        StorageAdapter::for_epoch_migration((*storage).clone(), target, recovery_bytes.clone());
    clear_bank(&recovery).await?;
    delete_pointer_resolving_outcome(storage, &recovery_bytes).await
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
        let legacy = StorageAdapter::new(storage.clone());
        if legacy
            .load_mutation_revision()
            .await
            .map_err(storage_error)?
            .is_some()
        {
            return Err(crate::init::unsupported_repository_protocol_error());
        }
        let target = EpochBank::A;
        let migrating_bytes = encode_pointer(PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target,
            generation: 1,
            attempt: uuid::Uuid::now_v7(),
        });
        if let Err(error) = publish_migration_claim_absent(storage, &migrating_bytes).await {
            if is_admission_race(&error) {
                return Box::pin(admit_repository(storage, progress)).await;
            }
            return Err(storage_error(error));
        }
        let heartbeat = match start_migration_heartbeat(storage.clone(), migrating_bytes.clone()) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                delete_pointer(storage, &migrating_bytes).await?;
                return Err(error);
            }
        };
        let result = async {
            let candidate = StorageAdapter::for_epoch_migration(
                storage.clone(),
                target,
                migrating_bytes.clone(),
            );
            // Allocate and publish the empty bank, but leave repository
            // initialization to the normal engine-open path. Sync bootstrap
            // learns the authority's default branch only after admission, and
            // that path must be able to supply it to the initializer. An active
            // empty bank is restart-safe: the next open observes the missing
            // protocol marker and completes initialization.
            if let Err(error) = clear_bank(&candidate).await {
                delete_pointer(storage, &migrating_bytes).await?;
                return Err(error);
            }
            let active = PointerState::Active {
                bank: target,
                generation: 1,
                format: crate::init::CURRENT_FORMAT_VERSION,
                publication: None,
            };
            let active_bytes = encode_pointer(active);
            replace_pointer(storage, &migrating_bytes, &active_bytes).await?;
            Ok(EpochAdmission {
                adapter: StorageAdapter::for_epoch(storage.clone(), target, active_bytes),
                report: OpenReport {
                    format: crate::init::CURRENT_FORMAT_VERSION,
                    // This admission owns the fresh open; the engine-open path
                    // completes initialization after sync can supply its
                    // authoritative default branch.
                    initialized: true,
                    migration: None,
                },
            })
        }
        .await;
        return finish_after_heartbeat(heartbeat, result).await;
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
            return Err(epoch_error(
                "repository has no valid versioned protocol marker",
            ));
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
    let source_revision = match source.load_mutation_revision().await {
        Ok(revision) => revision,
        Err(error) if is_admission_race(&error) => {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        Err(error) => return Err(storage_error(error)),
    };
    let migrating = PointerState::Migrating {
        source: EpochBank::Legacy,
        source_format: from_format,
        target: target_bank,
        generation: 1,
        attempt: uuid::Uuid::now_v7(),
    };
    let migrating_bytes = encode_pointer(migrating);
    if let Err(error) =
        claim_legacy(storage, source_revision, &original_marker, &migrating_bytes).await
    {
        if is_admission_race(&error) {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        return Err(storage_error(error));
    }
    let heartbeat = match start_migration_heartbeat(storage.clone(), migrating_bytes.clone()) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            rollback_legacy(storage, &migrating_bytes, &original_marker).await?;
            return Err(error);
        }
    };

    let result = async {
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
                .commit_write_set(marker_write, durable_candidate_write_options())
                .await
                .map_err(|error| epoch_error(format!("candidate marker write failed: {error}")))?;
            super::migrate_lix_with_adapter(
                storage.clone(),
                target.clone(),
                super::MigrationOptions::automatic(),
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
            publication: None,
        };
        let active_bytes = encode_pointer(active);
        if let Err(error) =
            activate_legacy(storage, &migrating_bytes, fenced_revision, &active_bytes).await
        {
            if matches!(error, StorageError::CommitOutcomeUnknown(_)) {
                return Err(LixError::from(error));
            }
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
    .await;
    finish_after_heartbeat(heartbeat, result).await
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
    let source =
        StorageAdapter::for_epoch(storage.clone(), source_bank, active_source_bytes.clone());
    emit_migrating(progress, from_format);
    let target_bank = source_bank.alternate();
    let source_revision = match source.load_mutation_revision().await {
        Ok(revision) => revision,
        Err(error) if is_admission_race(&error) => {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        Err(error) => return Err(storage_error(error)),
    };
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
        Err(error) if is_admission_race(&error) => {
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
    if let Err(error) =
        resolve_exact_pointer_commit(storage, claim.commit().await, &migrating_bytes).await
    {
        if is_admission_race(&error) {
            return Box::pin(admit_repository(storage, progress)).await;
        }
        return Err(storage_error(error));
    }
    let heartbeat = match start_migration_heartbeat(storage.clone(), migrating_bytes.clone()) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            replace_pointer(storage, &migrating_bytes, &active_source_bytes).await?;
            return Err(error);
        }
    };

    let result = async {
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
                super::MigrationOptions::automatic(),
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
            publication: None,
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
    .await;
    finish_after_heartbeat(heartbeat, result).await
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
    for space in crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
        .iter()
        .copied()
    {
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
    for space in epoch_data_spaces() {
        adapter
            .clear_space(space, durable_candidate_write_options())
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
    drop(read);
    for space in epoch_data_spaces() {
        let mut lower = Bound::Unbounded;
        loop {
            // The migration claim makes the source bank immutable. Reopen a
            // bounded read for every page so backends whose read generations
            // expire after any commit (notably OPFS) can publish the previous
            // page to the target bank without invalidating the next source
            // page. The exclusive key bound is the durable continuation.
            let (entries, has_more) = read_copy_page(source, space, &lower, &revision).await?;
            if entries.is_empty() {
                break;
            }
            let next_lower = Bound::Excluded(
                entries
                    .last()
                    .expect("a storage scan chunk cannot be empty")
                    .key
                    .clone(),
            );
            let mut writes = target.new_write_set();
            for entry in entries {
                let ProjectedValue::FullValue(value) = entry.value else {
                    return Err(epoch_error("full-value epoch scan returned a key-only row"));
                };
                writes.put(space, entry.key, StoredValue { bytes: value });
            }
            target
                .commit_write_set(writes, durable_candidate_write_options())
                .await
                .map_err(|error| epoch_error(format!("copy target write failed: {error}")))?;
            if !has_more {
                break;
            }
            lower = next_lower;
        }
    }
    Ok(revision)
}

async fn read_copy_page<S>(
    source: &StorageAdapter<S>,
    space: StorageSpace,
    lower: &Bound<Key>,
    expected_revision: &Option<Bytes>,
) -> Result<(Vec<ReadEntry>, bool), LixError>
where
    S: Storage,
{
    let mut retry = crate::common::ExpiredReadRetryState::default();
    loop {
        let result = async {
            let read = source
                .begin_read(ReadOptions::default())
                .await
                .map_err(storage_error)?;
            let observed_revision = StorageAdapter::<S>::load_mutation_revision_from_read(&read)
                .await
                .map_err(storage_error)?;
            if &observed_revision != expected_revision {
                return Err(epoch_error(
                    "source repository changed while its epoch was being copied",
                ));
            }
            let mut cursor = read
                .begin_scan(
                    space,
                    KeyRange {
                        lower: lower.clone(),
                        upper: Bound::Unbounded,
                    },
                    BeginScanOptions {
                        projection: CoreProjection::FullValue,
                        ..BeginScanOptions::default()
                    },
                )
                .await
                .map_err(storage_error)?;
            cursor
                .next_page(MAX_SCAN_PAGE_ROWS)
                .await
                .map_err(storage_error)
                .map(ScanChunk::into_parts)
        }
        .await;
        match result {
            Ok(page) => return Ok(page),
            Err(error) => {
                let Some(delay) = retry.next_delay(&error) else {
                    return Err(error);
                };
                tokio::task::yield_now().await;
                if !delay.is_zero() {
                    crate::sync::sleep(delay).await;
                }
            }
        }
    }
}

async fn load_pointer<S>(storage: &S) -> Result<Option<(PointerState, Bytes)>, LixError>
where
    S: Storage + ?Sized,
{
    let Some(bytes) = load_pointer_bytes(storage).await.map_err(storage_error)? else {
        return Ok(None);
    };
    let state = decode_pointer(&bytes)?;
    Ok(Some((state, bytes)))
}

async fn load_pointer_bytes<S>(storage: &S) -> Result<Option<Bytes>, StorageError>
where
    S: Storage + ?Sized,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let keys = [Key(Bytes::from_static(REPOSITORY_EPOCH_KEY))];
    let values = read
        .get_many(&[GetManyRequest {
            space: REPOSITORY_EPOCH_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    match values.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(bytes)) => Ok(Some(bytes)),
        Some(ProjectedValue::KeyOnly) => Err(StorageError::Corruption(
            "epoch pointer full-value read returned key-only data".to_string(),
        )),
        None => Ok(None),
    }
}

async fn resolve_exact_pointer_commit<S>(
    storage: &S,
    result: Result<CommitResult, StorageError>,
    expected: &Bytes,
) -> Result<(), StorageError>
where
    S: Storage + ?Sized,
{
    match result {
        Ok(_) => Ok(()),
        Err(StorageError::CommitOutcomeUnknown(message)) => {
            match load_pointer_bytes(storage).await {
                Ok(Some(observed)) if observed == *expected => Ok(()),
                Ok(_) => Err(StorageError::CommitOutcomeUnknown(message)),
                Err(read_error) => Err(StorageError::CommitOutcomeUnknown(format!(
                    "{message}; exact-pointer resolution read failed: {read_error}"
                ))),
            }
        }
        Err(error) => Err(error),
    }
}

async fn load_lease<S>(storage: &S) -> Result<Option<Bytes>, LixError>
where
    S: Storage + ?Sized,
{
    load_storage_value(storage, REPOSITORY_EPOCH_SPACE, REPOSITORY_EPOCH_LEASE_KEY).await
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
    space: StorageSpace,
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

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
type HeartbeatStopSender = std::sync::mpsc::Sender<()>;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
type HeartbeatStopReceiver = std::sync::mpsc::Receiver<()>;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
type HeartbeatStopSender = tokio::sync::watch::Sender<bool>;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
type HeartbeatStopReceiver = tokio::sync::watch::Receiver<bool>;

struct MigrationHeartbeat {
    stop: HeartbeatStopSender,
    done: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl MigrationHeartbeat {
    async fn stop(mut self) -> Result<(), LixError> {
        signal_heartbeat_stop(&self.stop);
        let done = self
            .done
            .take()
            .expect("migration heartbeat completion receiver is present");
        done.await.map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "repository migration heartbeat stopped without releasing its storage handle",
            )
        })
    }
}

async fn finish_after_heartbeat<T>(
    heartbeat: MigrationHeartbeat,
    result: Result<T, LixError>,
) -> Result<T, LixError> {
    let stopped = heartbeat.stop().await;
    if result
        .as_ref()
        .is_err_and(|error| error.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN)
    {
        return result;
    }
    stopped?;
    result
}

impl Drop for MigrationHeartbeat {
    fn drop(&mut self) {
        signal_heartbeat_stop(&self.stop);
    }
}

fn start_migration_heartbeat<S>(
    storage: S,
    migrating: Bytes,
) -> Result<MigrationHeartbeat, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let (stop, mut stop_rx) = std::sync::mpsc::channel();
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
    let (done_tx, done) = tokio::sync::oneshot::channel();
    let task = async move {
        let mut current = Bytes::from_static(b"0");
        let mut sequence = 1_u64;
        while wait_for_heartbeat_interval(&mut stop_rx).await {
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
        // Completion is also the lifetime barrier: release every task-owned
        // storage clone before waking the opener that is joining us.
        drop(storage);
        drop(migrating);
        let _ = done_tx.send(());
    };
    crate::background_task::spawn("lix-repository-migration-heartbeat", move || task)?;
    Ok(MigrationHeartbeat {
        stop,
        done: Some(done),
    })
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn signal_heartbeat_stop(stop: &HeartbeatStopSender) {
    let _ = stop.send(());
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn signal_heartbeat_stop(stop: &HeartbeatStopSender) {
    let _ = stop.send(true);
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn wait_for_heartbeat_interval(stop: &mut HeartbeatStopReceiver) -> bool {
    matches!(
        stop.recv_timeout(MIGRATION_HEARTBEAT_INTERVAL),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    )
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn wait_for_heartbeat_interval(stop: &mut HeartbeatStopReceiver) -> bool {
    if *stop.borrow() {
        return false;
    }
    let timer = crate::sync::sleep(MIGRATION_HEARTBEAT_INTERVAL).fuse();
    let changed = stop.changed().fuse();
    futures_util::pin_mut!(timer, changed);
    select_biased! {
        _ = changed => false,
        _ = timer => true,
    }
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
    put_pointer(&mut write, active.clone())
        .await
        .map_err(storage_error)?;
    resolve_exact_pointer_commit(storage, write.commit().await, active)
        .await
        .map_err(storage_error)?;
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
            preconditions: vec![
                Precondition::KeyAbsent {
                    space: REPOSITORY_EPOCH_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                },
                StorageAdapter::<S>::mutation_revision_precondition(None),
            ],
            ..WriteOptions::default()
        })
        .await?;
    put_pointer(&mut write, migrating.clone()).await?;
    put_lease(&mut write, Bytes::from_static(b"0")).await?;
    resolve_exact_pointer_commit(storage, write.commit().await, migrating).await
}

async fn claim_fresh_import<S>(storage: &S, migrating: &Bytes) -> Result<(), LixError>
where
    S: Storage + Clone,
{
    let mut retried_cancelled_handoff = false;
    loop {
        match try_claim_fresh_import(storage, migrating).await {
            Ok(()) => return Ok(()),
            Err(StorageError::PreconditionFailed(failures)) => {
                let Some((state, bytes)) = load_pointer(storage).await? else {
                    // A cancelled importer may release its exact claim between
                    // our failed claim commit and this resolving read. The
                    // control-space precondition is then the only failed item.
                    // Retry the full atomic emptiness check once; a persistent
                    // failure is real pointerless destination state.
                    if !retried_cancelled_handoff
                        && !failures.is_empty()
                        && failures.iter().all(|failure| failure.index == 0)
                    {
                        retried_cancelled_handoff = true;
                        continue;
                    }
                    return Err(nonempty_snapshot_destination());
                };
                retried_cancelled_handoff = false;
                if !matches!(
                    state,
                    PointerState::Migrating {
                        source: EpochBank::Legacy,
                        source_format: 0,
                        ..
                    }
                ) {
                    return Err(nonempty_snapshot_destination());
                }
                wait_for_stale_fresh_import(storage, state, &bytes).await?;
            }
            Err(error) => return Err(storage_error(error)),
        }
    }
}

async fn try_claim_fresh_import<S>(storage: &S, migrating: &Bytes) -> Result<(), StorageError>
where
    S: Storage,
{
    let mut preconditions = Vec::with_capacity(
        crate::storage_spaces::SNAPSHOT_STORAGE_SPACES.len()
            + crate::storage_spaces::RETIRED_STORAGE_SPACES.len()
            + 1,
    );
    preconditions.push(Precondition::RangeEmpty {
        space: REPOSITORY_EPOCH_SPACE,
        range: KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
    });
    preconditions.extend(
        crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
            .iter()
            .chain(
                crate::storage_spaces::RETIRED_STORAGE_SPACES
                    .iter()
                    .filter(|retired| !retired.emitted_in_lixsnap_v1)
                    .map(|retired| &retired.space),
            )
            .copied()
            .map(|space| Precondition::RangeEmpty {
                space,
                range: KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
            }),
    );
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions,
            ..WriteOptions::default()
        })
        .await?;
    put_pointer(&mut write, migrating.clone()).await?;
    put_lease(&mut write, Bytes::from_static(b"0")).await?;
    resolve_exact_pointer_commit(storage, write.commit().await, migrating).await
}

async fn wait_for_stale_fresh_import<S>(
    storage: &S,
    state: PointerState,
    migrating_bytes: &Bytes,
) -> Result<(), LixError>
where
    S: Storage + Clone,
{
    let mut observed_lease = load_lease(storage).await?;
    let mut missed = 0;
    loop {
        portable_sleep(MIGRATION_HEARTBEAT_INTERVAL).await?;
        match load_pointer(storage).await? {
            Some((_, current)) if current == *migrating_bytes => {}
            _ => return Ok(()),
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
    match recover_interrupted_migration(
        storage,
        state,
        migrating_bytes,
        observed_lease.as_ref(),
        None,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(error) => match load_pointer(storage).await? {
            Some((_, current)) if current == *migrating_bytes => {
                if load_lease(storage).await? == observed_lease {
                    Err(error)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        },
    }
}

fn nonempty_snapshot_destination() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        "snapshot restore destination is not empty",
    )
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
            single_put(
                crate::init::REPOSITORY_PROTOCOL_KEY,
                Bytes::from_static(LEGACY_FENCE),
            ),
        )
        .await?;
    crate::storage_adapter::stage_mutation_revision(&mut write).await?;
    resolve_exact_pointer_commit(storage, write.commit().await, migrating).await
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
    resolve_exact_pointer_commit(storage, write.commit().await, active).await
}

async fn rollback_legacy<S>(storage: &S, migrating: &Bytes, marker: &Bytes) -> Result<(), LixError>
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
    delete_epoch_control(&mut write)
        .await
        .map_err(storage_error)?;
    crate::storage_adapter::stage_mutation_revision(&mut write)
        .await
        .map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn replace_pointer<S>(
    storage: &S,
    expected: &Bytes,
    replacement: &Bytes,
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
    resolve_exact_pointer_commit(storage, write.commit().await, replacement)
        .await
        .map_err(storage_error)?;
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
    delete_epoch_control(&mut write)
        .await
        .map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn delete_pointer_resolving_outcome<S>(storage: &S, expected: &Bytes) -> Result<(), LixError>
where
    S: Storage,
{
    match delete_pointer(storage, expected).await {
        Ok(()) => Ok(()),
        Err(error) => match load_pointer(storage).await {
            Ok(Some((_, current))) if current == *expected => Err(error),
            Ok(_) => Ok(()),
            Err(inspect_error) => Err(LixError::new(
                error.code.clone(),
                format!("{error}; could not resolve epoch cleanup outcome: {inspect_error}"),
            )),
        },
    }
}

fn combine_cleanup_results(
    cleared: Result<(), LixError>,
    deleted: Result<(), LixError>,
    stopped: Result<(), LixError>,
) -> Result<(), LixError> {
    let mut errors = [cleared.err(), deleted.err(), stopped.err()]
        .into_iter()
        .flatten();
    let Some(first) = errors.next() else {
        return Ok(());
    };
    let code = first.code.clone();
    let mut message = first.to_string();
    for error in errors {
        message.push_str("; additional cleanup failure: ");
        message.push_str(&error.to_string());
    }
    Err(LixError::new(code, message))
}

fn with_cleanup_error(primary: LixError, cleanup: Result<(), LixError>) -> LixError {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => LixError::new(
            primary.code.clone(),
            format!("{primary}; snapshot restore cleanup also failed: {cleanup}"),
        ),
    }
}

async fn put_pointer<W>(write: &mut W, bytes: Bytes) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .put_many(
            REPOSITORY_EPOCH_SPACE,
            single_put(REPOSITORY_EPOCH_KEY, bytes),
        )
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
            publication,
        } => match publication {
            Some(publication) => format!(
                "{POINTER_PREFIX}|active|{}|{generation}|{format}|{publication}",
                bank_code(bank)
            ),
            None => format!(
                "{POINTER_PREFIX}|active|{}|{generation}|{format}",
                bank_code(bank)
            ),
        },
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
            publication: None,
        }),
        Some("active") if parts.len() == 6 => Ok(PointerState::Active {
            bank: parse_bank(parts[2])?,
            generation: parse_generation(parts[3])?,
            format: parse_format(parts[4])?,
            publication: Some(
                uuid::Uuid::parse_str(parts[5])
                    .map_err(|_| epoch_error("epoch pointer publication is invalid"))?,
            ),
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
    if matches!(
        error,
        StorageError::ReadExpired | StorageError::CommitOutcomeUnknown(_)
    ) {
        return LixError::from(error);
    }
    LixError::new(
        "LIX_ERROR_REPOSITORY_UPGRADE",
        format!("repository upgrade storage error: {error}"),
    )
}

fn is_admission_race(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::PreconditionFailed(_) | StorageError::WriteConflict | StorageError::Fenced
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageWriteOptions;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

    #[test]
    fn candidate_epoch_writes_are_durable_and_cover_snapshot_wire_spaces() {
        assert!(durable_candidate_write_options().await_durable);
        assert_eq!(
            epoch_data_spaces().collect::<Vec<_>>().as_slice(),
            crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
        );
    }

    #[derive(Clone, Debug)]
    struct CommitExpiringStorage {
        inner: crate::Memory,
        generation: Arc<AtomicU64>,
        expire_next_page: Arc<AtomicBool>,
    }

    impl CommitExpiringStorage {
        fn new() -> Self {
            Self {
                inner: crate::Memory::new(),
                generation: Arc::new(AtomicU64::new(0)),
                expire_next_page: Arc::new(AtomicBool::new(false)),
            }
        }

        fn expire_next_page(&self) {
            self.expire_next_page.store(true, Ordering::Release);
        }
    }

    struct CommitExpiringRead {
        inner: MemoryRead,
        generation: Arc<AtomicU64>,
        observed_generation: u64,
        expire_next_page: Arc<AtomicBool>,
    }

    impl CommitExpiringRead {
        fn validate(&self) -> Result<(), StorageError> {
            if self.generation.load(Ordering::Acquire) == self.observed_generation {
                Ok(())
            } else {
                Err(StorageError::ReadExpired)
            }
        }
    }

    struct CommitExpiringScan<'a> {
        inner: ScanCursor<'a>,
        generation: Arc<AtomicU64>,
        observed_generation: u64,
        expire_next_page: Arc<AtomicBool>,
    }

    impl StorageScanSource for CommitExpiringScan<'_> {
        fn next_page(
            &mut self,
            limit_rows: usize,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>>
        {
            Box::pin(async move {
                if self.expire_next_page.swap(false, Ordering::AcqRel) {
                    self.generation.fetch_add(1, Ordering::AcqRel);
                    return Err(StorageError::ReadExpired);
                }
                if self.generation.load(Ordering::Acquire) != self.observed_generation {
                    return Err(StorageError::ReadExpired);
                }
                self.inner.next_page(limit_rows).await
            })
        }
    }

    impl StorageRead for CommitExpiringRead {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            self.validate()?;
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            opts: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.validate()?;
            let checked_range = range.clone();
            let inner = self.inner.begin_scan(space, range, opts).await?;
            ScanCursor::from_source(
                checked_range,
                opts.order,
                CommitExpiringScan {
                    inner,
                    generation: Arc::clone(&self.generation),
                    observed_generation: self.observed_generation,
                    expire_next_page: Arc::clone(&self.expire_next_page),
                },
            )
        }
    }

    struct CommitExpiringWrite {
        inner: MemoryWrite,
        generation: Arc<AtomicU64>,
    }

    impl StorageWrite for CommitExpiringWrite {
        async fn put_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.put_many(space, entries).await
        }

        async fn replace_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.replace_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[Key],
        ) -> Result<(), StorageError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: StorageSpace,
            range: KeyRange,
        ) -> Result<(), StorageError> {
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<CommitResult, StorageError> {
            let result = self.inner.commit().await?;
            self.generation.fetch_add(1, Ordering::AcqRel);
            Ok(result)
        }

        async fn rollback(self) -> Result<(), StorageError> {
            self.inner.rollback().await
        }
    }

    impl Storage for CommitExpiringStorage {
        type Read<'a> = CommitExpiringRead;
        type Write<'a> = CommitExpiringWrite;

        async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            let inner = self.inner.begin_read(opts).await?;
            Ok(CommitExpiringRead {
                inner,
                generation: Arc::clone(&self.generation),
                observed_generation: self.generation.load(Ordering::Acquire),
                expire_next_page: Arc::clone(&self.expire_next_page),
            })
        }

        async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
            Ok(CommitExpiringWrite {
                inner: self.inner.begin_write(opts).await?,
                generation: Arc::clone(&self.generation),
            })
        }
    }

    #[derive(Clone)]
    struct PostCommitUnknownStorage {
        inner: crate::Memory,
        commit_fault: Arc<AtomicU8>,
        fail_next_read: Arc<AtomicBool>,
    }

    const COMMIT_FAULT_NONE: u8 = 0;
    const COMMIT_FAULT_PRE_UNKNOWN: u8 = 1;
    const COMMIT_FAULT_POST_UNKNOWN: u8 = 2;
    const COMMIT_FAULT_POST_UNKNOWN_AND_READ: u8 = 3;

    impl PostCommitUnknownStorage {
        fn new() -> Self {
            Self {
                inner: crate::Memory::new(),
                commit_fault: Arc::new(AtomicU8::new(COMMIT_FAULT_NONE)),
                fail_next_read: Arc::new(AtomicBool::new(false)),
            }
        }

        fn fail_next_commit(&self) {
            self.commit_fault
                .store(COMMIT_FAULT_POST_UNKNOWN, Ordering::Release);
        }

        fn fail_next_commit_before_apply(&self) {
            self.commit_fault
                .store(COMMIT_FAULT_PRE_UNKNOWN, Ordering::Release);
        }

        fn fail_next_commit_and_resolution_read(&self) {
            self.commit_fault
                .store(COMMIT_FAULT_POST_UNKNOWN_AND_READ, Ordering::Release);
        }
    }

    struct PostCommitUnknownWrite {
        inner: MemoryWrite,
        commit_fault: u8,
        fail_next_read: Arc<AtomicBool>,
    }

    impl Storage for PostCommitUnknownStorage {
        type Read<'a> = MemoryRead;
        type Write<'a> = PostCommitUnknownWrite;

        async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self.fail_next_read.swap(false, Ordering::AcqRel) {
                return Err(StorageError::Io(
                    "injected exact-pointer resolution read failure".to_string(),
                ));
            }
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            Ok(PostCommitUnknownWrite {
                inner: self.inner.begin_write(options).await?,
                commit_fault: self.commit_fault.swap(COMMIT_FAULT_NONE, Ordering::AcqRel),
                fail_next_read: Arc::clone(&self.fail_next_read),
            })
        }
    }

    impl StorageWrite for PostCommitUnknownWrite {
        async fn put_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.put_many(space, entries).await
        }

        async fn replace_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.replace_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[Key],
        ) -> Result<(), StorageError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: StorageSpace,
            range: KeyRange,
        ) -> Result<(), StorageError> {
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<CommitResult, StorageError> {
            if self.commit_fault == COMMIT_FAULT_PRE_UNKNOWN {
                return Err(StorageError::CommitOutcomeUnknown(
                    "injected pre-commit failure".to_string(),
                ));
            }
            let result = self.inner.commit().await?;
            if self.commit_fault == COMMIT_FAULT_POST_UNKNOWN_AND_READ {
                self.fail_next_read.store(true, Ordering::Release);
            }
            if matches!(
                self.commit_fault,
                COMMIT_FAULT_POST_UNKNOWN | COMMIT_FAULT_POST_UNKNOWN_AND_READ
            ) {
                return Err(StorageError::CommitOutcomeUnknown(
                    "injected post-commit failure".to_string(),
                ));
            }
            Ok(result)
        }

        async fn rollback(self) -> Result<(), StorageError> {
            self.inner.rollback().await
        }
    }

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
            publication: None,
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
                publication: None,
            },
            PointerState::Active {
                bank: EpochBank::A,
                generation: 7,
                format: 76,
                publication: Some(uuid::Uuid::from_u128(2)),
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
    async fn copy_reopens_source_pages_after_target_commits_expire_reads() {
        let storage = CommitExpiringStorage::new();
        let source_seed = StorageAdapter::for_epoch_unfenced(storage.clone(), EpochBank::A);
        let row_count = MAX_SCAN_PAGE_ROWS + 17;
        let mut writes = source_seed.new_write_set();
        for index in 0..row_count {
            let key = u64::try_from(index).unwrap().to_be_bytes();
            let value = [u8::try_from(index % 251).unwrap()];
            writes.put(
                crate::json_store::JSON_SPACE,
                key.as_slice(),
                value.as_slice(),
            );
        }
        source_seed
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let migrating = encode_pointer(PointerState::Migrating {
            source: EpochBank::A,
            source_format: crate::init::CURRENT_FORMAT_VERSION,
            target: EpochBank::B,
            generation: 2,
            attempt: uuid::Uuid::from_u128(30),
        });
        publish_migration_claim_absent(&storage, &migrating)
            .await
            .unwrap();
        let source =
            StorageAdapter::for_epoch_migration(storage.clone(), EpochBank::A, migrating.clone());
        let target = StorageAdapter::for_epoch_migration(storage, EpochBank::B, migrating.clone());

        source.storage().expire_next_page();
        copy_repository(&source, &target)
            .await
            .expect("copy must reopen an expired source generation between target pages");

        let read = target.begin_read(ReadOptions::default()).await.unwrap();
        let mut cursor = read
            .begin_scan(
                crate::json_store::JSON_SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .unwrap();
        let copied = cursor.collect_all().await.unwrap();
        assert_eq!(copied.len(), row_count);
        for (index, entry) in copied.into_iter().enumerate() {
            assert_eq!(
                entry.key.0.as_ref(),
                u64::try_from(index).unwrap().to_be_bytes()
            );
            assert_eq!(
                entry.value,
                ProjectedValue::FullValue(Bytes::from(vec![u8::try_from(index % 251).unwrap(),])),
            );
        }
    }

    #[tokio::test]
    async fn stale_epoch_adapter_is_fenced_after_pointer_change() {
        let storage = crate::Memory::new();
        let admitted = admit_repository(&storage, None).await.unwrap();
        let replacement = encode_pointer(PointerState::Active {
            bank: EpochBank::B,
            generation: 2,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: None,
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
        writes.put(crate::json_store::JSON_SPACE, &b"stale"[..], &b"write"[..]);
        let error = admitted
            .adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap_err();
        assert_eq!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(StorageError::Fenced)
        );
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
        let stale =
            StorageAdapter::for_epoch_migration(storage.clone(), EpochBank::B, first.clone());
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
        assert_eq!(
            stale
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .unwrap_err(),
            crate::storage_adapter::StorageWriteSetError::Storage(StorageError::Fenced)
        );
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
    async fn fresh_recovery_claims_ownership_before_clearing_candidate() {
        let storage = crate::Memory::new();
        let state = PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(40),
        };
        let bytes = encode_pointer(state);
        publish_migration_claim_absent(&storage, &bytes)
            .await
            .unwrap();
        let candidate =
            StorageAdapter::for_epoch_migration(storage.clone(), EpochBank::A, bytes.clone());
        let mut write = candidate
            .begin_migration_write(WriteOptions::default())
            .await
            .unwrap();
        write
            .put_many(
                crate::json_store::JSON_SPACE,
                single_put(b"candidate", Bytes::from_static(b"preserved")),
            )
            .await
            .unwrap();
        write.commit().await.unwrap();

        let observed = Bytes::from_static(b"0");
        let advanced = Bytes::from_static(b"1");
        advance_lease(&storage, &bytes, &observed, &advanced)
            .await
            .unwrap();
        assert!(
            recover_interrupted_migration(&storage, state, &bytes, Some(&observed), None)
                .await
                .is_err(),
            "a changed lease must defeat stale recovery"
        );

        let read = candidate.begin_read(ReadOptions::default()).await.unwrap();
        let mut cursor = read
            .begin_scan(
                crate::json_store::JSON_SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(cursor.collect_all().await.unwrap().len(), 1);

        recover_interrupted_migration(&storage, state, &bytes, Some(&advanced), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_fresh_import_is_reclaimed_by_direct_retry() {
        let storage = crate::Memory::new();
        let first = begin_fresh_epoch_import(storage.clone()).await.unwrap();
        let first_claim = first.claim.clone();
        drop(first);

        let second = begin_fresh_epoch_import(storage.clone())
            .await
            .expect("a direct retry should reclaim the cancelled import");
        assert_ne!(second.claim, first_claim);
        assert!(
            load_pointer(&storage)
                .await
                .unwrap()
                .is_some_and(|(_, bytes)| bytes == second.claim)
        );
        second.abort().await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_fresh_claim_commit_is_resolved_as_owned() {
        let storage = PostCommitUnknownStorage::new();
        storage.fail_next_commit();

        let import = begin_fresh_epoch_import(storage.clone())
            .await
            .expect("the exact committed claim resolves ambiguous acknowledgement");
        assert!(
            load_pointer(&storage)
                .await
                .unwrap()
                .is_some_and(|(_, bytes)| bytes == import.claim)
        );
        import.abort().await.unwrap();
    }

    #[tokio::test]
    async fn precommit_unknown_fresh_claim_preserves_unknown_outcome() {
        let storage = PostCommitUnknownStorage::new();
        storage.fail_next_commit_before_apply();

        let error = match begin_fresh_epoch_import(storage.clone()).await {
            Ok(_) => panic!("a pre-commit unknown claim cannot be acknowledged"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN);
        assert!(load_pointer(&storage).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ambiguous_fresh_publication_is_resolved_by_attempt_identity() {
        let storage = PostCommitUnknownStorage::new();
        let mut import = begin_fresh_epoch_import(storage.clone()).await.unwrap();
        if let Some(heartbeat) = import.heartbeat.take() {
            heartbeat.stop().await.unwrap();
        }
        let publication = import.publication;
        storage.fail_next_commit();

        import
            .publish(crate::init::CURRENT_FORMAT_VERSION)
            .await
            .expect("the exact active publication resolves ambiguous acknowledgement");
        assert!(matches!(
            load_pointer(&storage).await.unwrap(),
            Some((
                PointerState::Active {
                    publication: Some(observed),
                    ..
                },
                _
            )) if observed == publication
        ));
    }

    #[tokio::test]
    async fn failed_pointer_resolution_read_preserves_unknown_outcome() {
        let storage = PostCommitUnknownStorage::new();
        let migrating = encode_pointer(PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(50),
        });
        publish_migration_claim_absent(&storage, &migrating)
            .await
            .unwrap();
        let active = encode_pointer(PointerState::Active {
            bank: EpochBank::A,
            generation: 1,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: Some(uuid::Uuid::from_u128(50)),
        });
        storage.fail_next_commit_and_resolution_read();

        let error = replace_pointer(&storage, &migrating, &active)
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN);
        assert!(error.message.contains("resolution read failed"));
        assert!(
            load_pointer(&storage)
                .await
                .unwrap()
                .is_some_and(|(_, bytes)| bytes == active)
        );
    }

    #[tokio::test]
    async fn old_snapshot_claim_and_activation_resolve_postcommit_unknown() {
        let storage = PostCommitUnknownStorage::new();
        let marker = Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V75);
        let mut seed = storage.begin_write(WriteOptions::default()).await.unwrap();
        seed.put_many(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            single_put(crate::init::REPOSITORY_PROTOCOL_KEY, marker.clone()),
        )
        .await
        .unwrap();
        seed.commit().await.unwrap();

        let migrating = encode_pointer(PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 75,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(51),
        });
        storage.fail_next_commit();
        claim_legacy(&storage, None, &marker, &migrating)
            .await
            .expect("the exact old-snapshot claim resolves post-commit unknown");
        let source = StorageAdapter::for_epoch_migration(
            storage.clone(),
            EpochBank::Legacy,
            migrating.clone(),
        );
        let source_revision = source.load_mutation_revision().await.unwrap();
        let active = encode_pointer(PointerState::Active {
            bank: EpochBank::A,
            generation: 1,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: None,
        });
        storage.fail_next_commit();
        activate_legacy(&storage, &migrating, source_revision, &active)
            .await
            .expect("the exact old-snapshot activation resolves post-commit unknown");
        assert!(
            load_pointer(&storage)
                .await
                .unwrap()
                .is_some_and(|(_, bytes)| bytes == active)
        );
    }

    #[tokio::test]
    async fn missing_legacy_marker_with_repository_state_fails_closed() {
        let storage = crate::Memory::new();
        let legacy = StorageAdapter::new(storage.clone());
        let mut writes = legacy.new_write_set();
        writes.put(
            crate::json_store::JSON_SPACE,
            &b"preserved"[..],
            &b"repository-state"[..],
        );
        legacy
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let error = match admit_repository(&storage, None).await {
            Ok(_) => panic!("nonempty markerless storage must not become a fresh epoch"),
            Err(error) => error,
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
        assert!(load_pointer(&storage).await.unwrap().is_none());
        assert!(legacy.load_mutation_revision().await.unwrap().is_some());

        let claim = encode_pointer(PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(8),
        });
        assert!(matches!(
            publish_migration_claim_absent(&storage, &claim).await,
            Err(StorageError::PreconditionFailed(_))
        ));
        assert!(load_pointer(&storage).await.unwrap().is_none());
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
        claim_legacy(&storage, None, &marker, &bytes).await.unwrap();
        let lease = load_lease(&storage).await.unwrap().unwrap();
        let stored_marker = load_source_marker(&storage).await.unwrap().unwrap();

        recover_interrupted_migration(&storage, state, &bytes, Some(&lease), Some(&stored_marker))
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
        replace_pointer(&storage, &active, &migrating)
            .await
            .unwrap();

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
    async fn stale_active_upgrade_reenters_admission_after_winner_activates() {
        let storage = crate::Memory::new();
        let stale_active = seed_active_v75(&storage, EpochBank::A, 7).await;
        let winner = StorageAdapter::for_epoch_unfenced(storage.clone(), EpochBank::B);
        Engine::initialize_with_adapter(winner, None).await.unwrap();
        let winner_active = encode_pointer(PointerState::Active {
            bank: EpochBank::B,
            generation: 8,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: None,
        });
        replace_pointer(&storage, &stale_active, &winner_active)
            .await
            .unwrap();

        let admitted = migrate_active(&storage, EpochBank::A, 7, 75, stale_active, None)
            .await
            .expect("a losing opener should join the winner's active epoch");

        assert_eq!(admitted.adapter.epoch_bank(), EpochBank::B);
        assert_eq!(admitted.report.migration, None);
    }

    #[test]
    fn pointer_fencing_is_an_admission_race() {
        assert!(is_admission_race(&StorageError::Fenced));
    }

    #[tokio::test]
    async fn open_reports_initialization_after_empty_epoch_publication_restart() {
        let storage = crate::Memory::new();
        let admission = admit_repository(&storage, None).await.unwrap();
        assert!(admission.report.initialized);
        drop(admission);

        let lix = crate::open_lix()
            .with_storage(storage)
            .await
            .expect("open should initialize the already-published empty epoch");
        assert!(lix.open_report().initialized);
        lix.close().await.unwrap();
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
        heartbeat.stop().await.unwrap();
        delete_pointer(&storage, &bytes).await.unwrap();

        let admitted = waiter.await.unwrap().unwrap();
        assert!(admitted.report.initialized);
    }

    #[tokio::test]
    async fn stopping_heartbeat_releases_its_storage_handle() {
        let storage = crate::Memory::new();
        let state = PointerState::Migrating {
            source: EpochBank::Legacy,
            source_format: 0,
            target: EpochBank::A,
            generation: 1,
            attempt: uuid::Uuid::from_u128(7),
        };
        let bytes = encode_pointer(state);
        publish_migration_claim_absent(&storage, &bytes)
            .await
            .unwrap();
        assert_eq!(storage.shared_handle_count(), 1);

        let heartbeat = start_migration_heartbeat(storage.clone(), bytes).unwrap();
        assert_eq!(storage.shared_handle_count(), 2);
        heartbeat.stop().await.unwrap();

        assert_eq!(
            storage.shared_handle_count(),
            1,
            "stop completion must be a barrier after the task-owned handle drops"
        );
    }
}
