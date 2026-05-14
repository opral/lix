use crate::backend_v2::{Backend, BackendError, CommitResult, ReadOptions, WriteOptions};
use crate::storage_v2::{
    StorageReadScope, StorageWriteSet, StorageWriteSetError, StorageWriteSetStats,
};

#[derive(Clone, Debug)]
pub struct StorageContext<B> {
    backend: B,
}

impl<B> StorageContext<B>
where
    B: Backend,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> Result<StorageReadScope<B::Read<'_>>, BackendError> {
        self.backend.begin_read(opts).map(StorageReadScope::new)
    }

    pub fn new_write_set(&self) -> StorageWriteSet {
        StorageWriteSet::new()
    }

    pub fn commit_write_set(
        &self,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError> {
        write_set.commit(&self.backend, opts)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::backend_v2::{
        ConformanceBackend, GetOptions, Key, ProjectedValue, ReadOptions, SpaceId, StoredValue,
        WriteOptions,
    };
    use crate::storage_v2::{StorageContext, StorageReader, StorageSpace};

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space(id: u32) -> StorageSpace {
        StorageSpace::new(SpaceId(id))
    }

    #[test]
    fn context_commits_write_set_and_reads_through_storage_contract() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("b"), value("B"));
        writes.stage_put(space(2), key("a"), value("other"));
        writes.stage_delete(space(2), key("missing"));

        let (_commit, stats) = storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("commit write set");

        assert_eq!(stats.staged_puts, 3);
        assert_eq!(stats.staged_deletes, 1);
        assert_eq!(stats.touched_spaces, 2);
        assert_eq!(stats.put_batches, 2);
        assert_eq!(stats.delete_batches, 1);
        assert_eq!(stats.backend_calls, 3);

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let slots = read
            .get_many_caller_order(space(1), &[key("a"), key("b")], GetOptions::default())
            .expect("read back values");
        assert_eq!(
            slots.into_iter().map(|slot| slot.value).collect::<Vec<_>>(),
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
            ]
        );
    }

    #[test]
    fn context_read_scope_pins_snapshot_across_later_commits() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("a"), value("A"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let mut later = storage.new_write_set();
        later.stage_put(space(1), key("a"), value("B"));
        storage
            .commit_write_set(later, WriteOptions::default())
            .expect("later commit");

        let slots = read
            .get_many_caller_order(space(1), &[key("a")], GetOptions::default())
            .expect("read old scope");

        assert_eq!(
            slots[0].value,
            Some(ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );
    }
}
