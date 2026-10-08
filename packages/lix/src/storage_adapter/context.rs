use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use bytes::Bytes;
use tracing::Instrument as _;

use crate::storage::{
    CommitResult, KeyRange, Memory, Precondition, Prefix, PutBatch, PutEntry, ReadOptions, Storage,
    StorageChangeWatch, StorageError, StorageWrite, StoredValue, WriteOptions,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageAdapterReadScope, StorageSpace, StorageWriteSet,
    StorageWriteSetError, StorageWriteSetStats,
};

use super::epoch::{EpochBank, EpochRouting, EpochStorageWrite};

use super::spaces::{
    REVISION_KEY_MUTATION, REVISION_KEY_OBSERVABLE, REVISION_KEY_TRACKED_MUTATION, REVISION_SPACE,
    load_revision, revision_key,
};

/// One authoring role per engine and its clones; changing role replaces the
/// previous admission rather than accumulating permissions across receipts.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicaWriterMode {
    None = 0,
    Full = 1,
    Partial = 2,
}

/// Installation authority never crosses full/partial receipt ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicaWriteAdmission {
    Ordinary,
    FullInstaller,
    PartialInstaller,
}

#[derive(Clone, Copy)]
enum ObservableRevisionIntent<'a> {
    ClassifyWriteSet,
    NativeDependencyAvailability(
        &'a crate::sync::NativeDependencyAvailabilityCapability,
    ),
}

#[derive(Clone, Debug)]
pub struct StorageAdapter<StorageImpl = Memory> {
    durability: crate::Durability,
    storage: StorageImpl,
    routing: EpochRouting,
    authority_writer: Arc<AtomicBool>,
    replica_writer: Arc<AtomicU8>,
}

#[expect(missing_debug_implementations)]
pub struct PreparedStorageCommit<'a, StorageImpl>
where
    StorageImpl: Storage + 'a,
{
    write: EpochStorageWrite<StorageImpl::Write<'a>>,
    stats: StorageWriteSetStats,
}

impl<StorageImpl> StorageAdapter<StorageImpl>
where
    StorageImpl: Storage,
{
    pub(crate) fn durability(&self) -> crate::Durability {
        self.durability
    }

    pub(crate) fn with_durability(mut self, durability: crate::Durability) -> Self {
        self.durability = durability;
        self
    }

    pub fn new(storage: StorageImpl) -> Self {
        Self {
            storage,
            durability: crate::Durability::default(),
            routing: EpochRouting::legacy(),
            authority_writer: Arc::new(AtomicBool::new(false)),
            replica_writer: Arc::new(AtomicU8::new(ReplicaWriterMode::None as u8)),
        }
    }

    // Preserve epoch routing and its exact claim while constructing a private
    // Lix handle. An already-session-bound store returns the same session token.
    pub(crate) async fn with_session(
        self,
    ) -> Result<StorageAdapter<crate::storage::StorageSession<StorageImpl>>, StorageError> {
        Ok(StorageAdapter {
            storage: crate::storage::StorageSession::acquire(self.storage).await?,
            durability: self.durability,
            routing: self.routing,
            authority_writer: self.authority_writer,
            replica_writer: self.replica_writer,
        })
    }

    pub(crate) fn storage(&self) -> &StorageImpl {
        &self.storage
    }

    /// Routes engine storage into one hidden physical epoch bank without
    /// admitting writes. Migration construction and validation use this while
    /// the stable pointer is in a non-active state.
    pub(crate) fn for_epoch_unfenced(storage: StorageImpl, bank: EpochBank) -> Self {
        Self {
            storage,
            durability: crate::Durability::default(),
            routing: EpochRouting::unfenced(bank),
            authority_writer: Arc::new(AtomicBool::new(false)),
            replica_writer: Arc::new(AtomicU8::new(ReplicaWriterMode::None as u8)),
        }
    }

    /// Routes engine storage into the active physical epoch and fences every
    /// write on the exact stable-pointer bytes observed during admission.
    pub(crate) fn for_epoch(
        storage: StorageImpl,
        bank: EpochBank,
        expected_pointer: Bytes,
    ) -> Self {
        Self {
            storage,
            durability: crate::Durability::default(),
            routing: EpochRouting::fenced(bank, expected_pointer),
            authority_writer: Arc::new(AtomicBool::new(false)),
            replica_writer: Arc::new(AtomicU8::new(ReplicaWriterMode::None as u8)),
        }
    }

    /// Recovery may read an old generation but must never mutate its only copy.
    pub(crate) fn for_retained_epoch(
        storage: StorageImpl,
        bank: EpochBank,
        expected_pointer: Bytes,
    ) -> Self {
        Self {
            storage,
            durability: crate::Durability::default(),
            routing: EpochRouting::retained(bank, expected_pointer),
            authority_writer: Arc::new(AtomicBool::new(false)),
            replica_writer: Arc::new(AtomicU8::new(ReplicaWriterMode::None as u8)),
        }
    }

    /// Routes candidate construction into an epoch bank, fences every write
    /// on the exact migration claim, and forces each commit durable before the
    /// active pointer can publish the candidate.
    pub(crate) fn for_epoch_migration(
        storage: StorageImpl,
        bank: EpochBank,
        expected_pointer: Bytes,
    ) -> Self {
        Self {
            storage,
            durability: crate::Durability::default(),
            routing: EpochRouting::migration(bank, expected_pointer),
            authority_writer: Arc::new(AtomicBool::new(false)),
            replica_writer: Arc::new(AtomicU8::new(ReplicaWriterMode::None as u8)),
        }
    }

    pub(crate) fn epoch_bank(&self) -> EpochBank {
        self.routing.bank()
    }

    /// Grants this engine's adapter clones the authority write lane after the
    /// durable authority marker has been installed or verified. Only server
    /// admission calls this; another plain engine over the same backend owns a
    /// distinct flag and remains fenced by the marker.
    pub(crate) fn admit_sync_authority_writer(&self) {
        self.authority_writer.store(true, Ordering::Release);
    }

    /// Admits only an engine opened through the authenticated sync lifecycle.
    pub(crate) fn admit_sync_replica_writer(&self) {
        self.replica_writer
            .store(ReplicaWriterMode::Full as u8, Ordering::Release);
    }

    /// Only the partial sync owner admits authoring after durable account,
    /// remote and epoch checks. The capability is not full-state certification.
    pub(crate) fn admit_partial_replica_writer(
        &self,
        _capability: crate::sync::PartialReplicaWriteCapability,
    ) {
        self.replica_writer
            .store(ReplicaWriterMode::Partial as u8, Ordering::Release);
    }

    pub async fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> Result<StorageAdapterReadScope<StorageImpl::Read<'_>>, StorageError> {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_read_view();
        #[cfg(feature = "storage-benches")]
        let read = {
            let backend_started = crate::sql_profile::is_active().then(std::time::Instant::now);
            let result = self.storage.begin_read(opts).await;
            if let Some(started) = backend_started {
                crate::sql_profile::record_wait_or_read_phase(
                    crate::sql_profile::WaitOrReadPhase::StorageBackendBeginRead,
                    started.elapsed(),
                );
            }
            result?
        };
        #[cfg(not(feature = "storage-benches"))]
        let read = self.storage.begin_read(opts).await?;
        #[cfg(feature = "storage-benches")]
        {
            let epoch_started = crate::sql_profile::is_active().then(std::time::Instant::now);
            let result = self.routing.validate_read(&read).await;
            if let Some(started) = epoch_started {
                crate::sql_profile::record_wait_or_read_phase(
                    crate::sql_profile::WaitOrReadPhase::StorageEpochValidation,
                    started.elapsed(),
                );
            }
            result?;
        }
        #[cfg(not(feature = "storage-benches"))]
        self.routing.validate_read(&read).await?;
        Ok(StorageAdapterReadScope::with_routing(
            read,
            self.routing.clone(),
        ))
    }

    pub(crate) async fn watch_for_changes(&self) -> Result<StorageChangeWatch, StorageError> {
        self.storage.watch_for_changes().await
    }

    pub fn new_write_set(&self) -> StorageWriteSet {
        StorageWriteSet::new()
    }

    pub(crate) async fn begin_migration_write(
        &self,
        opts: WriteOptions,
    ) -> Result<EpochStorageWrite<StorageImpl::Write<'_>>, StorageError> {
        let mut opts = opts;
        opts.await_durable |= self.durability == crate::Durability::Durable;
        let (opts, fence_precondition_index) = self.routing.route_write_options(opts)?;
        let write = self.storage.begin_write(opts).await?;
        Ok(EpochStorageWrite::new(
            write,
            self.routing.clone(),
            fence_precondition_index,
        ))
    }

    /// Publish a prevalidated detached-candidate migration through the epoch
    /// claim while keeping raw storage writes inside the adapter layer.
    pub(crate) async fn commit_migration_write_set(
        &self,
        writes: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<CommitResult, StorageWriteSetError> {
        let has_storage_mutations = writes.has_storage_mutations();
        let has_observable_mutations = writes.has_observable_mutations();
        let mut write = self
            .begin_migration_write(opts)
            .await
            .map_err(StorageWriteSetError::Storage)?;
        if let Err(error) = writes.lower_into(&mut write).await {
            let _ = write.rollback().await;
            return Err(error);
        }
        if has_storage_mutations
            && let Err(error) = stage_storage_revisions(&mut write, has_observable_mutations).await
        {
            let _ = write.rollback().await;
            return Err(StorageWriteSetError::Storage(error));
        }
        write.commit().await.map_err(StorageWriteSetError::Storage)
    }

    pub async fn begin_read_transaction(
        &self,
    ) -> Result<Box<StorageAdapterReadTransaction<StorageImpl::Read<'_>>>, crate::LixError> {
        Ok(Box::new(StorageAdapterReadTransaction {
            read: self.begin_read(ReadOptions::default()).await?,
        }))
    }

    pub async fn begin_write_transaction(
        &self,
    ) -> Result<Box<StorageAdapterWriteTransaction<'_, StorageImpl>>, crate::LixError> {
        Ok(Box::new(StorageAdapterWriteTransaction {
            storage: self,
            read: self.begin_read(ReadOptions::default()).await?,
        }))
    }

    pub async fn commit_write_set(
        &self,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError> {
        let prepared = self.prepare_write_set(write_set, opts).await?;
        prepared
            .commit()
            .await
            .map_err(StorageWriteSetError::Storage)
    }

    /// Commits storage produced by the certified replica installer. This is
    /// intentionally crate-private: ordinary engine writers must retain the
    /// durable receipt fence even when they open the same backend through a
    /// second process-local engine.
    pub(crate) async fn commit_certified_replica_write_set(
        &self,
        _capability: crate::sync::CertifiedReplicaWriteCapability,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError> {
        let prepared = self
            .prepare_write_set_with_replica_capability(
                write_set,
                opts,
                ReplicaWriteAdmission::FullInstaller,
                ObservableRevisionIntent::ClassifyWriteSet,
            )
            .await?;
        prepared
            .commit()
            .await
            .map_err(StorageWriteSetError::Storage)
    }

    pub async fn prepare_write_set(
        &self,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<PreparedStorageCommit<'_, StorageImpl>, StorageWriteSetError> {
        self.prepare_write_set_with_replica_capability(
            write_set,
            opts,
            ReplicaWriteAdmission::Ordinary,
            ObservableRevisionIntent::ClassifyWriteSet,
        )
        .await
    }

    pub(crate) async fn commit_partial_replica_write_set(
        &self,
        _capability: crate::sync::PartialReplicaWriteCapability,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError> {
        self.prepare_write_set_with_replica_capability(
            write_set,
            opts,
            ReplicaWriteAdmission::PartialInstaller,
            ObservableRevisionIntent::ClassifyWriteSet,
        )
        .await?
        .commit()
        .await
        .map_err(StorageWriteSetError::Storage)
    }

    pub(crate) async fn commit_partial_native_dependency_availability_write_set(
        &self,
        _partial_capability: crate::sync::PartialReplicaWriteCapability,
        availability: crate::sync::NativeDependencyAvailabilityCapability,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError> {
        self.prepare_write_set_with_replica_capability(
            write_set,
            opts,
            ReplicaWriteAdmission::PartialInstaller,
            ObservableRevisionIntent::NativeDependencyAvailability(&availability),
        )
        .await?
        .commit()
        .await
        .map_err(StorageWriteSetError::Storage)
    }

    async fn prepare_write_set_with_replica_capability(
        &self,
        mut write_set: StorageWriteSet,
        mut opts: WriteOptions,
        admission: ReplicaWriteAdmission,
        observable_intent: ObservableRevisionIntent<'_>,
    ) -> Result<PreparedStorageCommit<'_, StorageImpl>, StorageWriteSetError> {
        if let ObservableRevisionIntent::NativeDependencyAvailability(capability) =
            observable_intent
        {
            require_native_dependency_availability_guards(&write_set, &opts, capability)?;
            // A validated dependency install is acknowledged only after its
            // object bytes and the exact partial-replica epoch guard are
            // durable together.
            opts.await_durable = true;
        }
        let writer_mode = self.replica_writer.load(Ordering::Acquire);
        let may_write_full = admission == ReplicaWriteAdmission::FullInstaller
            || (admission == ReplicaWriteAdmission::Ordinary
                && writer_mode == ReplicaWriterMode::Full as u8);
        if !may_write_full {
            // This atomic absence precondition closes the race between an
            // ordinary writer's coherent read and initial receipt install.
            // Plain engines sharing receipt-bound storage remain fenced. Only
            // the admitted sync engine may author a durable local outbox.
            opts.preconditions.push(Precondition::KeyAbsent {
                space: crate::sync::SYNC_REPLICA_STATE_SPACE,
                key: crate::sync::replica_state_key(),
            });
        }
        // An admitted full replica and its installer must still reject partial
        // ownership. Conversely partial installation retains the full fence,
        // even if the adapter previously admitted a full replica writer.
        let may_write_partial = admission == ReplicaWriteAdmission::PartialInstaller
            || (admission == ReplicaWriteAdmission::Ordinary
                && writer_mode == ReplicaWriterMode::Partial as u8);
        if may_write_partial
            && write_set.has_range_delete_outside(crate::session::UPLOAD_MANIFEST_LEAF_SPACE)
        {
            return Err(StorageWriteSetError::Admission(crate::LixError::new(
                "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                "partial replica range deletion requires a domain publication owner",
            )));
        }
        if may_write_partial {
            let graph_guards =
                crate::sync::partial_serving::commit_graph_guards(&write_set, &opts.preconditions)
                    .map_err(StorageWriteSetError::Admission)?;
            opts.preconditions.extend(graph_guards);
        }
        if !may_write_partial {
            if write_set.has_mutations_in_space(crate::sync::PARTIAL_REPLICA_STATE_SPACE)
                && !write_set.partial_bootstrap_authorized()
            {
                return Err(StorageWriteSetError::Admission(crate::LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "partial receipt publication requires the sync owner's write capability",
                )));
            }
            opts.preconditions.push(Precondition::KeyAbsent {
                space: crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                key: crate::sync::partial_replica_state_key(),
            });
        }
        let mut write_set_domain_prepared = false;
        if (may_write_partial
            || write_set.has_mutations_in_space(crate::sync::PARTIAL_REPLICA_STATE_SPACE))
            && crate::sync::partial_serving::has_coordinate_mutations(&write_set)
        {
            let read = self
                .begin_read(ReadOptions::default())
                .await
                .map_err(StorageWriteSetError::Storage)?;
            let (prepared, guards) =
                crate::sync::partial_serving::prepare_write(&read, write_set, false)
                    .await
                    .map_err(StorageWriteSetError::Admission)?;
            write_set = prepared;
            opts.preconditions.extend(guards);
            write_set_domain_prepared = true;
        }
        if write_set_domain_prepared
            && let ObservableRevisionIntent::NativeDependencyAvailability(capability) =
                observable_intent
        {
            // Domain preparation is allowed to add guards, never extra writes
            // under a dependency-only visibility intent.
            require_native_dependency_availability_guards(&write_set, &opts, capability)?;
        }
        let has_storage_mutations = write_set.has_storage_mutations();
        let has_observable_mutations = match observable_intent {
            ObservableRevisionIntent::ClassifyWriteSet => write_set.has_observable_mutations(),
            ObservableRevisionIntent::NativeDependencyAvailability(_) => false,
        };
        if self.authority_writer.load(Ordering::Acquire) {
            opts.preconditions.push(Precondition::KeyValueEquals {
                space: crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                key: crate::sync::authority_state_key(),
                expected: Bytes::from_static(crate::sync::AUTHORITY_STATE_VALUE),
            });
        } else {
            // Authority ownership is durable, while admission is process-local.
            // A plain second engine therefore cannot publish tracked state
            // without the repository event that connected replicas consume.
            opts.preconditions.push(Precondition::KeyAbsent {
                space: crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                key: crate::sync::authority_state_key(),
            });
        }
        opts.batch_capacity_hint_bytes = opts
            .batch_capacity_hint_bytes
            .max(write_set.backend_batch_capacity_hint_bytes());
        // Apply policy after transaction grouping so durable acknowledgement
        // does not disable commit cohorts. Buffered policy never clears an
        // internal request for stronger publication or synchronization guarantees.
        opts.await_durable |= self.durability == crate::Durability::Durable;
        let (opts, fence_precondition_index) = self.routing.route_write_options(opts)?;
        let write = self
            .storage
            .begin_write(opts)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.storage_writer_wait"
            ))
            .await
            .map_err(StorageWriteSetError::Storage)?;
        let mut write =
            EpochStorageWrite::new(write, self.routing.clone(), fence_precondition_index);
        let lowered = async {
            let mut stats = write_set.lower_into(&mut write).await?;
            if has_storage_mutations {
                // Adapter tokens are not caller mutations and stay out of the
                // returned stats. The physical token remains unconditional;
                // the observer token is staged in the same revision batch
                // only when this write set touched an observable space.
                stats.observable_revision =
                    stage_storage_revisions(&mut write, has_observable_mutations)
                    .await
                    .map_err(StorageWriteSetError::Storage)?;
            }
            Ok::<_, StorageWriteSetError>(stats)
        }
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.storage_lowering"
        ))
        .await;
        let stats = match lowered {
            Ok(stats) => stats,
            Err(error) => {
                let _ = write.rollback().await;
                return Err(error);
            }
        };
        Ok(PreparedStorageCommit { write, stats })
    }

    pub(crate) async fn load_mutation_revision(&self) -> Result<Option<Bytes>, StorageError> {
        let read = self.begin_read(ReadOptions::default()).await?;
        Self::load_mutation_revision_from_read(&read).await
    }

    pub(crate) async fn load_observable_revision(&self) -> Result<Option<Bytes>, StorageError> {
        let read = self.begin_read(ReadOptions::default()).await?;
        Self::load_observable_revision_from_read(&read).await
    }

    pub(crate) fn tracked_mutation_revision_precondition(expected: Option<Bytes>) -> Precondition {
        expected.map_or_else(
            || Precondition::KeyAbsent {
                space: REVISION_SPACE,
                key: revision_key(REVISION_KEY_TRACKED_MUTATION),
            },
            |expected| Precondition::KeyValueEquals {
                space: REVISION_SPACE,
                key: revision_key(REVISION_KEY_TRACKED_MUTATION),
                expected,
            },
        )
    }

    pub(crate) fn mutation_revision_precondition(expected: Option<Bytes>) -> Precondition {
        expected.map_or_else(
            || Precondition::KeyAbsent {
                space: REVISION_SPACE,
                key: revision_key(REVISION_KEY_MUTATION),
            },
            |expected| Precondition::KeyValueEquals {
                space: REVISION_SPACE,
                key: revision_key(REVISION_KEY_MUTATION),
                expected,
            },
        )
    }

    pub(crate) fn stage_tracked_mutation_revision(write_set: &mut StorageWriteSet) {
        write_set.put(
            REVISION_SPACE,
            revision_key(REVISION_KEY_TRACKED_MUTATION),
            uuid::Uuid::now_v7().as_bytes().as_slice(),
        );
    }

    pub(crate) async fn load_mutation_revision_from_read<R>(
        read: &R,
    ) -> Result<Option<Bytes>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        load_revision(read, REVISION_KEY_MUTATION).await
    }

    pub(crate) async fn load_observable_revision_from_read<R>(
        read: &R,
    ) -> Result<Option<Bytes>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        load_revision(read, REVISION_KEY_OBSERVABLE).await
    }

    pub(crate) async fn load_tracked_mutation_revision_from_read<R>(
        read: &R,
    ) -> Result<Option<Bytes>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        load_revision(read, REVISION_KEY_TRACKED_MUTATION).await
    }

    pub async fn delete_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: WriteOptions,
    ) -> Result<CommitResult, StorageError> {
        let mut opts = opts;
        if !self.routing.is_migration_writer() {
            if self.replica_writer.load(Ordering::Acquire) == ReplicaWriterMode::Partial as u8 {
                return Err(StorageError::Corruption(
                    "partial replica range deletion requires a checked atomic write set".into(),
                ));
            }
            opts.preconditions.push(Precondition::KeyAbsent {
                space: crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                key: crate::sync::partial_replica_state_key(),
            });
            if self.replica_writer.load(Ordering::Acquire) != ReplicaWriterMode::Full as u8 {
                opts.preconditions.push(Precondition::KeyAbsent {
                    space: crate::sync::SYNC_REPLICA_STATE_SPACE,
                    key: crate::sync::replica_state_key(),
                });
            }
        }
        opts.await_durable |= self.durability == crate::Durability::Durable;
        let (opts, fence_precondition_index) = self.routing.route_write_options(opts)?;
        let write = self.storage.begin_write(opts).await?;
        let mut write =
            EpochStorageWrite::new(write, self.routing.clone(), fence_precondition_index);
        if let Err(error) = write.delete_range(space, range).await {
            let _ = write.rollback().await;
            return Err(error);
        }
        if let Err(error) = stage_storage_revisions(
            &mut write,
            space.visibility == crate::storage::StorageSpaceVisibility::Observable,
        )
        .await
        {
            let _ = write.rollback().await;
            return Err(error);
        }
        write.commit().await
    }

    pub async fn delete_prefix(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: WriteOptions,
    ) -> Result<CommitResult, StorageError> {
        self.delete_range(space, prefix.to_range()?, opts).await
    }

    pub async fn clear_space(
        &self,
        space: StorageSpace,
        opts: WriteOptions,
    ) -> Result<CommitResult, StorageError> {
        self.delete_range(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            opts,
        )
        .await
    }
}

fn require_native_dependency_availability_guards(
    write_set: &StorageWriteSet,
    opts: &WriteOptions,
    capability: &crate::sync::NativeDependencyAvailabilityCapability,
) -> Result<(), StorageWriteSetError> {
    let invalid = || {
        StorageWriteSetError::Admission(crate::LixError::new(
            "LIX_NATIVE_DEPENDENCY_AVAILABILITY_INVALID",
            "dependency availability intent does not match its validated native objects and partial epoch guards",
        ))
    };

    if !write_set.is_exact_native_dependency_availability(capability) {
        return Err(invalid());
    }

    let state_key = crate::sync::partial_replica_state_key();
    let expected_state = capability.partial_replica_state();
    if !opts.preconditions.iter().any(|condition| {
        matches!(
            condition,
            Precondition::KeyValueEquals {
                space,
                key,
                expected,
            } if *space == crate::sync::PARTIAL_REPLICA_STATE_SPACE
                && key == &state_key
                && expected == expected_state
        )
    }) {
        return Err(invalid());
    }

    if capability.entries().iter().any(|entry| {
        !opts.preconditions.iter().any(|condition| {
            matches!(
                condition,
                Precondition::KeyAbsent { space, key }
                    if *space == entry.space && key.0.as_ref() == entry.key.as_ref()
            )
        })
    }) {
        return Err(invalid());
    }

    Ok(())
}

pub(crate) async fn stage_mutation_revision<W>(write: &mut W) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    // Remaining low-level callers are visible migration/publication paths.
    // Private work uses the canonical write-set classifier above instead.
    stage_storage_revisions(write, true).await.map(|_| ())
}

/// Stages the physical mutation token and, for canonical visible changes, the
/// observer token in one revision-space batch. Raw `Storage::begin_write`
/// callers bypass this engine-owned logical mutation boundary.
pub(crate) async fn stage_storage_revisions<W>(
    write: &mut W,
    observable: bool,
) -> Result<Option<[u8; 16]>, StorageError>
where
    W: StorageWrite,
{
    let mut entries = vec![PutEntry {
        key: revision_key(REVISION_KEY_MUTATION),
        value: StoredValue {
            bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
        },
    }];
    let observable_revision = observable.then(|| *uuid::Uuid::now_v7().as_bytes());
    if let Some(revision) = observable_revision {
        entries.push(PutEntry {
            key: revision_key(REVISION_KEY_OBSERVABLE),
            value: StoredValue {
                bytes: Bytes::copy_from_slice(&revision),
            },
        });
    }
    write.put_many(REVISION_SPACE, PutBatch { entries }).await?;
    Ok(observable_revision)
}

pub(crate) async fn load_repository_mutation_revision<R>(
    read: &R,
) -> Result<Option<Bytes>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    load_revision(read, REVISION_KEY_MUTATION).await
}

pub(crate) fn repository_mutation_revision_precondition(expected: Option<Bytes>) -> Precondition {
    expected.map_or_else(
        || Precondition::KeyAbsent {
            space: REVISION_SPACE,
            key: revision_key(REVISION_KEY_MUTATION),
        },
        |expected| Precondition::KeyValueEquals {
            space: REVISION_SPACE,
            key: revision_key(REVISION_KEY_MUTATION),
            expected,
        },
    )
}

impl<'a, StorageImpl> PreparedStorageCommit<'a, StorageImpl>
where
    StorageImpl: Storage + 'a,
{
    pub async fn commit(self) -> Result<(CommitResult, StorageWriteSetStats), StorageError> {
        let result = self
            .write
            .commit()
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.storage_commit_accepted_visible"
            ))
            .await?;
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_write(self.stats);
        Ok((result, self.stats))
    }

    pub async fn rollback(self) -> Result<(), StorageError> {
        self.write.rollback().await
    }
}

#[expect(missing_debug_implementations)]
pub struct StorageAdapterReadTransaction<R>
where
    R: crate::storage::StorageRead,
{
    read: StorageAdapterReadScope<R>,
}

impl<R> StorageAdapterReadTransaction<R>
where
    R: crate::storage::StorageRead,
{
    pub async fn rollback(self: Box<Self>) -> Result<(), crate::LixError> {
        drop(self);
        Ok(())
    }
}

impl<R> StorageAdapterRead for StorageAdapterReadTransaction<R>
where
    R: crate::storage::StorageRead,
{
    fn get_many(
        &self,
        requests: &[crate::storage::GetManyRequest<'_>],
    ) -> impl Future<Output = Result<crate::storage::GetManyResult, StorageError>> + Send {
        self.read.get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: crate::storage::BeginScanOptions,
    ) -> impl Future<Output = Result<crate::storage::ScanCursor<'_>, StorageError>> + Send {
        self.read.begin_scan(space, range, opts)
    }
}

#[expect(missing_debug_implementations)]
pub struct StorageAdapterWriteTransaction<'a, StorageImpl>
where
    StorageImpl: Storage,
{
    storage: &'a StorageAdapter<StorageImpl>,
    read: StorageAdapterReadScope<StorageImpl::Read<'a>>,
}

impl<StorageImpl> StorageAdapterWriteTransaction<'_, StorageImpl>
where
    StorageImpl: Storage,
{
    pub async fn commit(self: Box<Self>) -> Result<(), crate::LixError> {
        drop(self);
        Ok(())
    }

    pub async fn rollback(self: Box<Self>) -> Result<(), crate::LixError> {
        drop(self);
        Ok(())
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub async fn write_set(
        &mut self,
        write_set: StorageWriteSet,
    ) -> Result<StorageWriteSetStats, crate::LixError> {
        let (_commit, stats) = self
            .storage
            .commit_write_set(write_set, WriteOptions::default())
            .await?;
        Ok(stats)
    }
}

impl<StorageImpl> StorageAdapterRead for StorageAdapterWriteTransaction<'_, StorageImpl>
where
    StorageImpl: Storage,
{
    fn get_many(
        &self,
        requests: &[crate::storage::GetManyRequest<'_>],
    ) -> impl Future<Output = Result<crate::storage::GetManyResult, StorageError>> + Send {
        self.read.get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: crate::storage::BeginScanOptions,
    ) -> impl Future<Output = Result<crate::storage::ScanCursor<'_>, StorageError>> + Send {
        self.read.begin_scan(space, range, opts)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::storage::{
        GetOptions, Key, KeyRange, Memory, Precondition, ProjectedValue, ReadOptions, SpaceId,
        StorageError, StorageWrite, StoredValue, WriteOptions,
    };
    use crate::storage_adapter::{PointReadPlan, StorageAdapter, StorageSpace};

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space() -> StorageSpace {
        StorageSpace::mutable(SpaceId(1), "test.space")
    }

    #[tokio::test]
    async fn replica_write_admission_never_crosses_receipt_ownership() {
        use super::{ObservableRevisionIntent, ReplicaWriteAdmission, ReplicaWriterMode};
        for full_receipt in [false, true] {
            for partial_receipt in [false, true] {
                for writer_mode in [
                    ReplicaWriterMode::None,
                    ReplicaWriterMode::Full,
                    ReplicaWriterMode::Partial,
                ] {
                    for admission in [
                        ReplicaWriteAdmission::Ordinary,
                        ReplicaWriteAdmission::FullInstaller,
                        ReplicaWriteAdmission::PartialInstaller,
                    ] {
                        let storage = StorageAdapter::new(Memory::new());
                        let mut seed = storage.new_write_set();
                        if full_receipt {
                            seed.put(
                                crate::sync::SYNC_REPLICA_STATE_SPACE,
                                crate::sync::replica_state_key(),
                                value("full receipt"),
                            );
                        }
                        if partial_receipt {
                            seed.put(
                                crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                                crate::sync::partial_replica_state_key(),
                                value("partial receipt"),
                            );
                        }
                        // Receipt contents are deliberately opaque here: the
                        // storage fence tests ownership presence, not codecs.
                        let mut raw = storage
                            .begin_migration_write(WriteOptions::default())
                            .await
                            .unwrap();
                        seed.lower_into(&mut raw).await.unwrap();
                        raw.commit().await.unwrap();
                        // Exercise the private role matrix without exposing a
                        // test-only constructor for the sync capability.
                        storage
                            .replica_writer
                            .store(writer_mode as u8, std::sync::atomic::Ordering::Release);
                        let mut writes = storage.new_write_set();
                        writes.put(space(), key("candidate"), value("must be atomic"));
                        let result = match storage
                            .prepare_write_set_with_replica_capability(
                                writes,
                                WriteOptions::default(),
                                admission,
                                ObservableRevisionIntent::ClassifyWriteSet,
                            )
                            .await
                        {
                            Ok(prepared) => prepared.commit().await.is_ok(),
                            Err(_) => false,
                        };
                        let full_allowed = !full_receipt
                            || admission == ReplicaWriteAdmission::FullInstaller
                            || (admission == ReplicaWriteAdmission::Ordinary
                                && writer_mode == ReplicaWriterMode::Full);
                        let partial_allowed = !partial_receipt
                            || admission == ReplicaWriteAdmission::PartialInstaller
                            || (admission == ReplicaWriteAdmission::Ordinary
                                && writer_mode == ReplicaWriterMode::Partial);
                        let expected = full_allowed && partial_allowed;
                        assert_eq!(
                            result, expected,
                            "full={full_receipt} partial={partial_receipt} writer={writer_mode:?} installer={admission:?}"
                        );
                        let read = storage.begin_read(ReadOptions::default()).await.unwrap();
                        let stored = PointReadPlan::new(space(), &[key("candidate")])
                            .materialize(&read, GetOptions::default())
                            .await
                            .unwrap();
                        assert_eq!(
                            stored.value[0].is_some(),
                            expected,
                            "rejected publication must retain no mutation"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn context_commit_and_snapshot_read_are_async_and_coherent() {
        let storage = StorageAdapter::new(Memory::new());
        let mut seed = storage.new_write_set();
        seed.put(space(), key("a"), value("A"));
        storage
            .commit_write_set(seed, WriteOptions::default())
            .await
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin read");
        let revision = StorageAdapter::<Memory>::load_mutation_revision_from_read(&read)
            .await
            .expect("revision");

        let mut later = storage.new_write_set();
        later.put(space(), key("a"), value("B"));
        storage
            .commit_write_set(later, WriteOptions::default())
            .await
            .expect("later commit");

        let value = PointReadPlan::new(space(), &[key("a")])
            .materialize(&read, GetOptions::default())
            .await
            .expect("read old snapshot");
        assert_eq!(
            value.value,
            [Some(ProjectedValue::FullValue(Bytes::from_static(b"A")))]
        );
        assert_eq!(
            StorageAdapter::<Memory>::load_mutation_revision_from_read(&read)
                .await
                .expect("old revision"),
            revision
        );
        assert_ne!(
            storage
                .load_mutation_revision()
                .await
                .expect("latest revision"),
            revision
        );
    }

    #[tokio::test]
    async fn observable_revision_separates_private_and_visible_commits() {
        let storage = StorageAdapter::new(Memory::new());
        let private_space = crate::sync::PARTIAL_READ_INTEREST_SPACE;

        assert_eq!(storage.load_mutation_revision().await.unwrap(), None);
        assert_eq!(storage.load_observable_revision().await.unwrap(), None);

        let mut private = storage.new_write_set();
        private.put(private_space, key("recipe"), value("query recipe"));
        let (_, private_stats) = storage
            .commit_write_set(private, WriteOptions::default())
            .await
            .expect("private journal commit");
        assert_eq!(private_stats.observable_revision, None);
        let physical_after_private = storage.load_mutation_revision().await.unwrap();
        assert!(
            physical_after_private.is_some(),
            "private writes still rotate m"
        );
        assert_eq!(
            storage.load_observable_revision().await.unwrap(),
            None,
            "the first private write must preserve an absent observable token"
        );

        let mut visible = storage.new_write_set();
        visible.put(space(), key("visible"), value("result"));
        let (_, visible_stats) = storage
            .commit_write_set(visible, WriteOptions::default())
            .await
            .expect("visible commit");
        let physical_after_visible = storage.load_mutation_revision().await.unwrap();
        let observable_after_visible = storage.load_observable_revision().await.unwrap();
        assert_ne!(physical_after_visible, physical_after_private);
        assert!(observable_after_visible.is_some());
        let expected_observable_revision: [u8; 16] = observable_after_visible
            .as_deref()
            .expect("visible revision exists")
            .try_into()
            .expect("observable revision is a 16-byte UUID");
        assert_eq!(
            visible_stats.observable_revision,
            Some(expected_observable_revision),
            "returned stats carry the exact token staged by the accepted commit"
        );

        let mut mixed = storage.new_write_set();
        mixed.put(private_space, key("recipe-2"), value("another recipe"));
        mixed.put(space(), key("visible-2"), value("another result"));
        storage
            .commit_write_set(mixed, WriteOptions::default())
            .await
            .expect("mixed commit");
        assert_ne!(
            storage.load_observable_revision().await.unwrap(),
            observable_after_visible,
            "a mixed private/visible commit remains observable"
        );
    }

    #[tokio::test]
    async fn failed_visible_cas_does_not_advance_observable_revision() {
        let storage = StorageAdapter::new(Memory::new());
        let mut seed = storage.new_write_set();
        seed.put(space(), key("seed"), value("seed"));
        storage
            .commit_write_set(seed, WriteOptions::default())
            .await
            .expect("seed visible token");
        let before = storage.load_observable_revision().await.unwrap();

        let mut writes = storage.new_write_set();
        writes.put(space(), key("candidate"), value("must roll back"));
        let error = storage
            .commit_write_set(
                writes,
                WriteOptions {
                    preconditions: vec![Precondition::KeyValueEquals {
                        space: space(),
                        key: key("absent"),
                        expected: Bytes::from_static(b"not present"),
                    }],
                    ..WriteOptions::default()
                },
            )
            .await
            .expect_err("failed CAS must roll back the visible write and revision");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));
        assert_eq!(storage.load_observable_revision().await.unwrap(), before);
    }

    #[tokio::test]
    async fn a_second_adapter_observes_the_external_visible_revision() {
        let memory = Memory::new();
        let observer_adapter = StorageAdapter::new(memory.clone());
        let writer_adapter = StorageAdapter::new(memory);
        let before = observer_adapter.load_observable_revision().await.unwrap();

        let mut writes = writer_adapter.new_write_set();
        writes.put(space(), key("external"), value("from another adapter"));
        writer_adapter
            .commit_write_set(writes, WriteOptions::default())
            .await
            .expect("external adapter commit");

        assert_ne!(
            observer_adapter.load_observable_revision().await.unwrap(),
            before,
            "the observable token is durable shared storage state"
        );
    }

    #[tokio::test]
    async fn range_only_write_sets_rotate_revision_tokens_by_space_visibility() {
        let storage = StorageAdapter::new(Memory::new());
        let all = KeyRange {
            lower: std::ops::Bound::Unbounded,
            upper: std::ops::Bound::Unbounded,
        };
        let mut private_range = storage.new_write_set();
        private_range
            .delete_range_exclusive(crate::sync::PARTIAL_READ_INTEREST_SPACE, all.clone())
            .unwrap();
        storage
            .commit_write_set(private_range, WriteOptions::default())
            .await
            .expect("private range delete");
        assert!(storage.load_mutation_revision().await.unwrap().is_some());
        assert_eq!(storage.load_observable_revision().await.unwrap(), None);

        let mut visible_range = storage.new_write_set();
        visible_range.delete_range_exclusive(space(), all).unwrap();
        storage
            .commit_write_set(visible_range, WriteOptions::default())
            .await
            .expect("visible range delete");
        assert!(storage.load_observable_revision().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn direct_range_deletes_rotate_revision_tokens_by_space_visibility() {
        let storage = StorageAdapter::new(Memory::new());
        let all = KeyRange {
            lower: std::ops::Bound::Unbounded,
            upper: std::ops::Bound::Unbounded,
        };

        storage
            .delete_range(
                crate::sync::PARTIAL_READ_INTEREST_SPACE,
                all.clone(),
                WriteOptions::default(),
            )
            .await
            .expect("direct private range delete");
        assert!(storage.load_mutation_revision().await.unwrap().is_some());
        assert_eq!(storage.load_observable_revision().await.unwrap(), None);

        storage
            .delete_range(space(), all, WriteOptions::default())
            .await
            .expect("direct visible range delete");
        assert!(storage.load_observable_revision().await.unwrap().is_some());
    }
}
