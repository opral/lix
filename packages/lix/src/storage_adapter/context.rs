use std::ops::Bound;

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

use super::spaces::{
    REVISION_KEY_MUTATION, REVISION_KEY_TRACKED_MUTATION, REVISION_SPACE, load_revision,
    revision_key,
};

#[derive(Clone, Debug)]
pub struct StorageAdapter<StorageImpl = Memory> {
    storage: StorageImpl,
}

#[expect(missing_debug_implementations)]
pub struct PreparedStorageCommit<'a, StorageImpl>
where
    StorageImpl: Storage + 'a,
{
    write: StorageImpl::Write<'a>,
    stats: StorageWriteSetStats,
}

impl<StorageImpl> StorageAdapter<StorageImpl>
where
    StorageImpl: Storage,
{
    pub fn new(storage: StorageImpl) -> Self {
        Self { storage }
    }

    pub async fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> Result<StorageAdapterReadScope<StorageImpl::Read<'_>>, StorageError> {
        self.storage
            .begin_read(opts)
            .await
            .map(StorageAdapterReadScope::new)
    }

    pub(crate) async fn watch_for_changes(&self) -> Result<StorageChangeWatch, StorageError> {
        self.storage.watch_for_changes().await
    }

    pub fn new_write_set(&self) -> StorageWriteSet {
        StorageWriteSet::new()
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

    pub async fn prepare_write_set(
        &self,
        write_set: StorageWriteSet,
        mut opts: WriteOptions,
    ) -> Result<PreparedStorageCommit<'_, StorageImpl>, StorageWriteSetError> {
        opts.batch_capacity_hint_bytes = opts
            .batch_capacity_hint_bytes
            .max(write_set.backend_batch_capacity_hint_bytes());
        let mut write = self
            .storage
            .begin_write(opts)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.storage_writer_wait"
            ))
            .await
            .map_err(StorageWriteSetError::Storage)?;
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
        let read = self.storage.begin_read(ReadOptions::default()).await?;
        Self::load_mutation_revision_from_read(&StorageAdapterReadScope::new(read)).await
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
        let mut write = self.storage.begin_write(opts).await?;
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

async fn stage_mutation_revision<W>(write: &mut W) -> Result<(), StorageError>
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
