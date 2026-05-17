use std::ops::Bound;

use crate::backend_v2::{
    Backend, BackendError, BackendWrite, CommitResult, KeyRange, Prefix, ReadOptions, WriteOptions,
};
use crate::storage_v2::{
    StorageReadScope, StorageSpace, StorageWriteSet, StorageWriteSetError, StorageWriteSetStats,
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

    pub fn delete_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: WriteOptions,
    ) -> Result<CommitResult, BackendError> {
        let mut write = self.backend.begin_write(opts)?;
        let physical_range = space.encode_range(range, None);
        if let Err(error) = write.delete_range(physical_range) {
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::backend_v2::{
        ConformanceBackend, GetOptions, Key, ProjectedValue, ReadOptions, SpaceId, StoredValue,
        WriteOptions,
    };
    use crate::storage_v2::{PointReadPlan, StorageContext, StorageSpace};

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
        let storage = StorageContext::new(ConformanceBackend::new());
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
        let storage = StorageContext::new(ConformanceBackend::new());
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

    use crate::backend_v2::{
        Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
        BufferedRangeScan, CommitResult, GetOptions, Key, KeyRange, PointVisitor, ProjectedValue,
        ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId,
        StoredValue, WriteConcurrency, WriteOptions, WriteStats,
    };
    use crate::storage_v2::{
        PointReadPlan, ScanPlan, StorageContext, StorageReadScope, StorageSpace,
    };

    #[test]
    fn write_set_across_g_spaces_lowers_to_g_put_many_calls_and_one_commit() {
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
        assert_eq!(backend.state.put_batches.borrow().len(), 3);
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
                .into_iter()
                .map(|key| space(1).encode_key(&key))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            result.value,
            vec![
                Some(ProjectedValue::FullValue(space(1).encode_key(&key("b")).0)),
                Some(ProjectedValue::FullValue(space(1).encode_key(&key("a")).0)),
                Some(ProjectedValue::FullValue(space(1).encode_key(&key("b")).0)),
                Some(ProjectedValue::FullValue(
                    space(1).encode_key(&key("missing")).0
                )),
                Some(ProjectedValue::FullValue(space(1).encode_key(&key("a")).0)),
            ]
        );
    }

    #[test]
    fn prefix_scan_calls_scan_range_once() {
        let read = SpyRead::default();
        let scope = StorageReadScope::new(read.clone());

        ScanPlan::prefix(
            space(1),
            crate::backend_v2::Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .collect(&scope, ScanOptions::default())
        .expect("prefix scan");

        assert_eq!(read.scan_range_calls.get(), 1);
        assert_eq!(
            read.scan_range.borrow().clone(),
            Some(KeyRange {
                lower: Bound::Included(space(1).encode_key(&key("a"))),
                upper: Bound::Excluded(space(1).encode_key(&key("b"))),
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
                lower: Bound::Included(space(7).encode_key(&key("a"))),
                upper: Bound::Excluded(space(7).encode_key(&key("c"))),
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
                crate::backend_v2::Prefix {
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
                lower: Bound::Included(space(7).encode_key(&key("ab"))),
                upper: Bound::Excluded(space(7).encode_key(&key("ac"))),
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
                lower: Bound::Included(Key(Bytes::from_static(b"\0\0\0\x07"))),
                upper: Bound::Excluded(Key(Bytes::from_static(b"\0\0\0\x08"))),
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

        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities::v0(WriteConcurrency::SingleWriter)
        }

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
        fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
            let space = entries
                .entries
                .first()
                .map(|entry| physical_space(&entry.key))
                .unwrap_or(SpaceId(0));
            self.put_batches.push((
                space,
                entries
                    .entries
                    .into_iter()
                    .map(|entry| logical_key(entry.key))
                    .collect(),
            ));
            Ok(())
        }

        fn delete_many(&mut self, _keys: &[Key]) -> Result<(), BackendError> {
            self.state
                .delete_many_calls
                .set(self.state.delete_many_calls.get() + 1);
            Ok(())
        }

        fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
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
        type RangeScan<'a> = BufferedRangeScan;

        fn visit_keys<V>(
            &self,
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

        fn with_range_scan<T, F>(
            &self,
            range: KeyRange,
            _opts: ScanOptions<'_>,
            f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            self.scan_range_calls.set(self.scan_range_calls.get() + 1);
            self.scan_range.replace(Some(range));
            let mut cursor = BufferedRangeScan::default();
            f(&mut cursor)
        }
    }

    fn space(id: u32) -> StorageSpace {
        StorageSpace::new(SpaceId(id), "shape.test.space")
    }

    fn physical_space(key: &Key) -> SpaceId {
        SpaceId(u32::from_be_bytes(
            key.0[..4].try_into().expect("space prefix"),
        ))
    }

    fn logical_key(key: Key) -> Key {
        Key(key.0.slice(4..))
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
