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
    REVISION_KEY_MUTATION, REVISION_KEY_TRACKED_MUTATION, REVISION_SPACE, load_revision,
    revision_key,
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

#[derive(Clone, Debug)]
pub struct StorageAdapter<StorageImpl = Memory> {
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
    pub fn new(storage: StorageImpl) -> Self {
        Self {
            storage,
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
        let read = self.storage.begin_read(opts).await?;
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
        let (opts, fence_precondition_index) = self.routing.route_write_options(opts)?;
        let write = self.storage.begin_write(opts).await?;
        Ok(EpochStorageWrite::new(
            write,
            self.routing.clone(),
            fence_precondition_index,
        ))
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
        )
        .await?
        .commit()
        .await
        .map_err(StorageWriteSetError::Storage)
    }

    async fn prepare_write_set_with_replica_capability(
        &self,
        write_set: StorageWriteSet,
        mut opts: WriteOptions,
        admission: ReplicaWriteAdmission,
    ) -> Result<PreparedStorageCommit<'_, StorageImpl>, StorageWriteSetError> {
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
        if !may_write_partial {
            opts.preconditions.push(Precondition::KeyAbsent {
                space: crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                key: crate::sync::partial_replica_state_key(),
            });
        }
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
            let stats = write_set.lower_into(&mut write).await?;
            if stats.staged_puts > 0 || stats.staged_deletes > 0 {
                // The adapter's own mutation token is not a caller mutation,
                // so it stays out of the write set (and out of the returned
                // stats). It now lands in the shared revision space, next to
                // every other revision the same commit rotated.
                stage_mutation_revision(&mut write)
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
        let (opts, fence_precondition_index) = self.routing.route_write_options(opts)?;
        let write = self.storage.begin_write(opts).await?;
        let mut write =
            EpochStorageWrite::new(write, self.routing.clone(), fence_precondition_index);
        if let Err(error) = write.delete_range(space, range).await {
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

pub(crate) async fn stage_mutation_revision<W>(write: &mut W) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .put_many(
            REVISION_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: revision_key(REVISION_KEY_MUTATION),
                    value: StoredValue {
                        bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
                    },
                }],
            },
        )
        .await
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
        GetOptions, Key, Memory, ProjectedValue, ReadOptions, SpaceId, StoredValue, WriteOptions,
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
        use super::{ReplicaWriteAdmission, ReplicaWriterMode};
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
                        storage
                            .commit_write_set(seed, WriteOptions::default())
                            .await
                            .unwrap();
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
}
