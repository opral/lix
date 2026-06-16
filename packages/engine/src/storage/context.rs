use std::ops::Bound;

use bytes::Bytes;

use crate::backend::{
    Backend, BackendError, BackendWrite, CommitResult, CoreProjection, GetOptions, InMemoryBackend,
    Key, KeyRange, Prefix, ProjectedValue, PutBatch, PutEntry, ReadOptions, StoredValue,
    WriteOptions, get_many,
};
use crate::storage::{
    StorageRead, StorageReadScope, StorageSpace, StorageWriteSet, StorageWriteSetError,
    StorageWriteSetStats,
};

use super::spaces::MUTATION_REVISION_SPACE;

const MUTATION_REVISION_KEY: &[u8] = b"global";

#[derive(Clone, Debug)]
pub struct StorageContext<B = InMemoryBackend> {
    backend: B,
}

#[expect(missing_debug_implementations)]
pub struct PreparedStorageCommit<'a, B>
where
    B: Backend + 'a,
{
    write: Option<B::Write<'a>>,
    stats: Option<StorageWriteSetStats>,
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

    pub async fn begin_read_transaction(
        &self,
    ) -> Result<Box<StorageReadTransaction<B::Read<'_>>>, crate::LixError> {
        Ok(Box::new(StorageReadTransaction {
            read: self.begin_read(ReadOptions::default())?,
        }))
    }

    pub async fn begin_write_transaction(
        &self,
    ) -> Result<Box<StorageWriteTransaction<'_, B>>, crate::LixError> {
        Ok(Box::new(StorageWriteTransaction {
            storage: self,
            read: self.begin_read(ReadOptions::default())?,
        }))
    }

    pub fn commit_write_set(
        &self,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError> {
        let prepared = self.prepare_write_set(write_set, opts)?;
        prepared.commit().map_err(StorageWriteSetError::Backend)
    }

    pub fn prepare_write_set(
        &self,
        write_set: StorageWriteSet,
        opts: WriteOptions,
    ) -> Result<PreparedStorageCommit<'_, B>, StorageWriteSetError> {
        let mut write = self
            .backend
            .begin_write(opts)
            .map_err(StorageWriteSetError::Backend)?;
        let stats = match write_set.lower_into(&mut write) {
            Ok(stats) => stats,
            Err(error) => {
                let _ = write.rollback();
                return Err(error);
            }
        };
        if stats.staged_puts > 0 || stats.staged_deletes > 0 {
            if let Err(error) = stage_mutation_revision(&mut write) {
                let _ = write.rollback();
                return Err(StorageWriteSetError::Backend(error));
            }
        }
        Ok(PreparedStorageCommit {
            write: Some(write),
            stats: Some(stats),
        })
    }

    pub(crate) fn load_mutation_revision(&self) -> Result<Option<Bytes>, BackendError> {
        let read = self.backend.begin_read(ReadOptions::default())?;
        let values = get_many(
            &read,
            MUTATION_REVISION_SPACE.id,
            &[mutation_revision_key()],
            GetOptions {
                projection: CoreProjection::FullValue,
                ..GetOptions::default()
            },
        )?;
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

    pub fn delete_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: WriteOptions,
    ) -> Result<CommitResult, BackendError> {
        let mut write = self.backend.begin_write(opts)?;
        if let Err(error) = write.delete_range(space.id, range) {
            let _ = write.rollback();
            return Err(error);
        }
        write.commit()
    }

    pub fn delete_prefix(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: WriteOptions,
    ) -> Result<CommitResult, BackendError> {
        self.delete_range(space, prefix.to_range()?, opts)
    }

    pub fn clear_space(
        &self,
        space: StorageSpace,
        opts: WriteOptions,
    ) -> Result<CommitResult, BackendError> {
        self.delete_range(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            opts,
        )
    }
}

fn mutation_revision_key() -> Key {
    Key(Bytes::from_static(MUTATION_REVISION_KEY))
}

fn stage_mutation_revision<W>(write: &mut W) -> Result<(), BackendError>
where
    W: BackendWrite,
{
    write.put_many(
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
}

impl<'a, B> PreparedStorageCommit<'a, B>
where
    B: Backend + 'a,
{
    pub fn commit(mut self) -> Result<(CommitResult, StorageWriteSetStats), BackendError> {
        let write = self
            .write
            .take()
            .expect("prepared storage commit should contain a backend write");
        let result = write.commit()?;
        let stats = self
            .stats
            .take()
            .expect("prepared storage commit should contain stats");
        Ok((result, stats))
    }

    pub fn rollback(mut self) -> Result<(), BackendError> {
        self.write.take().map_or(Ok(()), BackendWrite::rollback)
    }
}

impl<'a, B> Drop for PreparedStorageCommit<'a, B>
where
    B: Backend + 'a,
{
    fn drop(&mut self) {
        if let Some(write) = self.write.take() {
            let _ = write.rollback();
        }
    }
}

#[expect(missing_debug_implementations)]
pub struct StorageReadTransaction<R>
where
    R: crate::backend::BackendRead,
{
    read: StorageReadScope<R>,
}

impl<R> StorageReadTransaction<R>
where
    R: crate::backend::BackendRead,
{
    pub async fn rollback(self: Box<Self>) -> Result<(), crate::LixError> {
        Ok(())
    }
}

impl<R> StorageRead for StorageReadTransaction<R>
where
    R: crate::backend::BackendRead,
{
    type BackendRead = R;

    fn with_backend<T>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        self.read.with_backend(f)
    }
}

#[expect(missing_debug_implementations)]
pub struct StorageWriteTransaction<'a, B>
where
    B: Backend,
{
    storage: &'a StorageContext<B>,
    read: StorageReadScope<B::Read<'a>>,
}

impl<B> StorageWriteTransaction<'_, B>
where
    B: Backend,
{
    pub async fn commit(self: Box<Self>) -> Result<(), crate::LixError> {
        Ok(())
    }

    pub async fn rollback(self: Box<Self>) -> Result<(), crate::LixError> {
        Ok(())
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub fn write_set(
        &mut self,
        write_set: StorageWriteSet,
    ) -> Result<StorageWriteSetStats, crate::LixError> {
        let (_commit, stats) = self
            .storage
            .commit_write_set(write_set, WriteOptions::default())?;
        Ok(stats)
    }
}

impl<'a, B> StorageRead for StorageWriteTransaction<'a, B>
where
    B: Backend,
{
    type BackendRead = B::Read<'a>;

    fn with_backend<T>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        self.read.with_backend(f)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::backend::{
        GetOptions, InMemoryBackend, Key, ProjectedValue, ReadOptions, SpaceId, StoredValue,
        WriteOptions,
    };
    use crate::storage::{PointReadPlan, StorageContext, StorageSpace};

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space(id: u32) -> StorageSpace {
        match id {
            1 => StorageSpace::new(SpaceId(1), "test.space.one"),
            2 => StorageSpace::new(SpaceId(2), "test.space.two"),
            _ => StorageSpace::new(SpaceId(id), "test.space.other"),
        }
    }

    #[test]
    fn context_commits_write_set_and_reads_through_storage_contract() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        writes.put(space(2), key("a"), value("other"));
        writes.delete(space(2), key("missing"));

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
        let result = PointReadPlan::new(space(1), &[key("a"), key("b")])
            .materialize(&read, GetOptions::default())
            .expect("read back values");
        assert_eq!(
            result.value,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
            ]
        );
    }

    #[test]
    fn context_read_scope_pins_snapshot_across_later_commits() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let mut later = storage.new_write_set();
        later.put(space(1), key("a"), value("B"));
        storage
            .commit_write_set(later, WriteOptions::default())
            .expect("later commit");

        let result = PointReadPlan::new(space(1), &[key("a")])
            .materialize(&read, GetOptions::default())
            .expect("read old scope");

        assert_eq!(
            result.value[0],
            Some(ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );
    }
}

#[cfg(test)]
mod shape_tests {
    use std::cell::{Cell, RefCell};
    use std::ops::Bound;
    use std::rc::Rc;

    use bytes::Bytes;

    use crate::backend::{
        Backend, BackendError, BackendRead, BackendWrite, CommitResult, GetOptions, Key, KeyRange,
        PointVisitor, ProjectedValue, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions,
        ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions, WriteStats,
    };
    use crate::storage::{PointReadPlan, ScanPlan, StorageContext, StorageReadScope, StorageSpace};

    use super::MUTATION_REVISION_SPACE;

    #[test]
    fn write_set_commit_stamps_mutation_revision_in_same_backend_commit() {
        let backend = CountingBackend::default();
        let storage = StorageContext::new(backend.clone());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(2), key("a"), value("B"));
        writes.put(space(3), key("a"), value("C"));

        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("commit write set");

        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 1);
        assert_eq!(backend.state.put_batches.borrow().len(), 4);
        assert_eq!(
            backend
                .state
                .put_batches
                .borrow()
                .iter()
                .map(|(space, keys)| (*space, keys.clone()))
                .collect::<Vec<_>>(),
            vec![
                (SpaceId(1), vec![key("a")]),
                (SpaceId(2), vec![key("a")]),
                (SpaceId(3), vec![key("a")]),
                (MUTATION_REVISION_SPACE.id, vec![key("global")]),
            ]
        );
    }

    #[test]
    fn point_read_m_requested_u_unique_sends_u_backend_keys() {
        let read = SpyRead::default();
        let scope = StorageReadScope::new(read.clone());

        let result = PointReadPlan::new(
            space(1),
            &[key("b"), key("a"), key("b"), key("missing"), key("a")],
        )
        .materialize(&scope, GetOptions::default())
        .expect("point read");

        assert_eq!(
            read.get_many_keys.borrow().clone(),
            vec![key("b"), key("a"), key("missing")]
        );
        assert_eq!(
            result.value,
            vec![
                Some(ProjectedValue::FullValue(key("b").0)),
                Some(ProjectedValue::FullValue(key("a").0)),
                Some(ProjectedValue::FullValue(key("b").0)),
                None,
                Some(ProjectedValue::FullValue(key("a").0)),
            ]
        );
    }

    #[test]
    fn prefix_scan_calls_scan_range_once() {
        let read = SpyRead::default();
        let scope = StorageReadScope::new(read.clone());

        ScanPlan::prefix(
            space(1),
            crate::backend::Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .collect(&scope, ScanOptions::default())
        .expect("prefix scan");

        assert_eq!(read.scan_range_calls.get(), 1);
        assert_eq!(
            read.scan_range.borrow().clone(),
            Some(KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("b")),
            })
        );
    }

    #[test]
    fn delete_range_lowers_to_one_backend_delete_range() {
        let backend = CountingBackend::default();
        let storage = StorageContext::new(backend.clone());

        storage
            .delete_range(
                space(7),
                KeyRange {
                    lower: Bound::Included(key("a")),
                    upper: Bound::Excluded(key("c")),
                },
                WriteOptions::default(),
            )
            .expect("delete range");

        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 1);
        assert_eq!(backend.state.delete_many_calls.get(), 0);
        assert_eq!(
            backend.state.delete_ranges.borrow().as_slice(),
            &[KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("c")),
            }]
        );
    }

    #[test]
    fn delete_prefix_lowers_to_one_backend_delete_range() {
        let backend = CountingBackend::default();
        let storage = StorageContext::new(backend.clone());

        storage
            .delete_prefix(
                space(7),
                crate::backend::Prefix {
                    bytes: Bytes::from_static(b"ab"),
                },
                WriteOptions::default(),
            )
            .expect("delete prefix");

        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 1);
        assert_eq!(backend.state.delete_many_calls.get(), 0);
        assert_eq!(
            backend.state.delete_ranges.borrow().as_slice(),
            &[KeyRange {
                lower: Bound::Included(key("ab")),
                upper: Bound::Excluded(key("ac")),
            }]
        );
    }

    #[test]
    fn clear_space_lowers_to_one_backend_delete_range() {
        let backend = CountingBackend::default();
        let storage = StorageContext::new(backend.clone());

        storage
            .clear_space(space(7), WriteOptions::default())
            .expect("clear space");

        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 1);
        assert_eq!(backend.state.delete_many_calls.get(), 0);
        assert_eq!(
            backend.state.delete_ranges.borrow().as_slice(),
            &[KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            }]
        );
    }

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Rc<CountingState>,
    }

    #[derive(Default)]
    struct CountingState {
        begin_write_calls: Cell<u64>,
        commit_calls: Cell<u64>,
        delete_many_calls: Cell<u64>,
        put_batches: RefCell<Vec<(SpaceId, Vec<Key>)>>,
        delete_ranges: RefCell<Vec<KeyRange>>,
    }

    struct CountingWrite {
        state: Rc<CountingState>,
        put_batches: Vec<(SpaceId, Vec<Key>)>,
        delete_ranges: Vec<KeyRange>,
    }

    impl Backend for CountingBackend {
        type Read<'a>
            = SpyRead
        where
            Self: 'a;

        type Write<'a>
            = CountingWrite
        where
            Self: 'a;
        fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
            Ok(SpyRead::default())
        }

        fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
            self.state
                .begin_write_calls
                .set(self.state.begin_write_calls.get() + 1);
            Ok(CountingWrite {
                state: Rc::clone(&self.state),
                put_batches: Vec::new(),
                delete_ranges: Vec::new(),
            })
        }
    }

    impl BackendWrite for CountingWrite {
        fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
            self.put_batches.push((
                space,
                entries.entries.into_iter().map(|entry| entry.key).collect(),
            ));
            Ok(())
        }

        fn delete_many(&mut self, _space: SpaceId, _keys: &[Key]) -> Result<(), BackendError> {
            self.state
                .delete_many_calls
                .set(self.state.delete_many_calls.get() + 1);
            Ok(())
        }

        fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
            let _ = space;
            self.delete_ranges.push(range);
            Ok(())
        }

        fn commit(self) -> Result<CommitResult, BackendError> {
            self.state
                .commit_calls
                .set(self.state.commit_calls.get() + 1);
            self.state.put_batches.borrow_mut().extend(self.put_batches);
            self.state
                .delete_ranges
                .borrow_mut()
                .extend(self.delete_ranges);
            Ok(CommitResult {
                commit_id: None,
                stats: WriteStats::default(),
            })
        }

        fn rollback(self) -> Result<(), BackendError> {
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct SpyRead {
        get_many_keys: Rc<RefCell<Vec<Key>>>,
        scan_range_calls: Rc<Cell<u64>>,
        scan_range: Rc<RefCell<Option<KeyRange>>>,
    }

    impl BackendRead for SpyRead {
        fn visit_keys<V>(
            &self,
            _space: SpaceId,
            keys: &[Key],
            _opts: GetOptions<'_>,
            visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            self.get_many_keys.replace(keys.to_vec());
            for (index, key) in keys.iter().enumerate() {
                let value = (key.0.as_ref() != b"missing")
                    .then_some(ProjectedValueRef::FullValue(key.0.as_ref()));
                visitor.visit(index, key, value)?;
            }
            Ok(())
        }

        fn scan<V>(
            &self,
            _space: SpaceId,
            range: KeyRange,
            _opts: ScanOptions<'_>,
            _visitor: &mut V,
        ) -> Result<ScanResult, BackendError>
        where
            V: ScanVisitor + ?Sized,
        {
            self.scan_range_calls.set(self.scan_range_calls.get() + 1);
            self.scan_range.replace(Some(range));
            Ok(ScanResult::default())
        }
    }

    fn space(id: u32) -> StorageSpace {
        StorageSpace::new(SpaceId(id), "shape.test.space")
    }

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }
}
