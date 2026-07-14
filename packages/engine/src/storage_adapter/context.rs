use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{
    CommitResult, CoreProjection, GetOptions, Key, KeyRange, Memory, Prefix, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, Storage, StorageError, StorageWrite, StoredValue,
    WriteOptions,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageAdapterReadScope, StorageSpace, StorageWriteSet,
    StorageWriteSetError, StorageWriteSetStats,
};

use super::spaces::MUTATION_REVISION_SPACE;

const MUTATION_REVISION_KEY: &[u8] = b"global";

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
        opts: WriteOptions,
    ) -> Result<PreparedStorageCommit<'_, StorageImpl>, StorageWriteSetError> {
        let mut write = self
            .storage
            .begin_write(opts)
            .await
            .map_err(StorageWriteSetError::Storage)?;
        let stats = match write_set.lower_into(&mut write).await {
            Ok(stats) => stats,
            Err(error) => {
                let _ = write.rollback().await;
                return Err(error);
            }
        };
        if stats.staged_puts > 0 || stats.staged_deletes > 0 {
            if let Err(error) = stage_mutation_revision(&mut write).await {
                let _ = write.rollback().await;
                return Err(StorageWriteSetError::Storage(error));
            }
        }
        Ok(PreparedStorageCommit { write, stats })
    }

    pub(crate) async fn load_mutation_revision(&self) -> Result<Option<Bytes>, StorageError> {
        let read = self.storage.begin_read(ReadOptions::default()).await?;
        Self::load_mutation_revision_from_read(&StorageAdapterReadScope::new(read)).await
    }

    pub(crate) async fn load_mutation_revision_from_read<R>(
        read: &R,
    ) -> Result<Option<Bytes>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        let values = read
            .get_many(
                MUTATION_REVISION_SPACE.id,
                &[mutation_revision_key()],
                GetOptions {
                    projection: CoreProjection::FullValue,
                },
            )
            .await?;
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

    pub async fn delete_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: WriteOptions,
    ) -> Result<CommitResult, StorageError> {
        let mut write = self.storage.begin_write(opts).await?;
        if let Err(error) = write.delete_range(space.id, range).await {
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

fn mutation_revision_key() -> Key {
    Key(Bytes::from_static(MUTATION_REVISION_KEY))
}

async fn stage_mutation_revision<W>(write: &mut W) -> Result<(), StorageError>
where
    W: StorageWrite,
{
    write
        .put_many(
            MUTATION_REVISION_SPACE.id,
            PutBatch {
                entries: vec![PutEntry {
                    key: mutation_revision_key(),
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
        let result = self.write.commit().await?;
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
        space: crate::storage::SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<crate::storage::GetManyResult, StorageError>> + Send {
        self.read.get_many(space, keys, opts)
    }

    fn scan(
        &self,
        space: crate::storage::SpaceId,
        range: KeyRange,
        opts: crate::storage::ScanOptions,
    ) -> impl Future<Output = Result<crate::storage::ScanChunk, StorageError>> + Send {
        self.read.scan(space, range, opts)
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
        space: crate::storage::SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<crate::storage::GetManyResult, StorageError>> + Send {
        self.read.get_many(space, keys, opts)
    }

    fn scan(
        &self,
        space: crate::storage::SpaceId,
        range: KeyRange,
        opts: crate::storage::ScanOptions,
    ) -> impl Future<Output = Result<crate::storage::ScanChunk, StorageError>> + Send {
        self.read.scan(space, range, opts)
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
        StorageSpace::new(SpaceId(1), "test.space")
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
