use std::collections::HashMap;
use std::fmt;

use crate::backend::{
    Backend, BackendError, BackendWrite, CommitResult, Key, PutBatch, PutEntry, SpaceId,
    StoredValue, WriteOptions,
};
use crate::storage::{StorageSpace, StorageWriteSetStats};
use ahash::RandomState;

type FastHashBuilder = RandomState;

#[derive(Clone, Debug)]
pub struct StorageWriteSet {
    groups: Vec<StorageWriteGroup>,
    group_index: HashMap<SpaceId, usize, FastHashBuilder>,
    stats: StorageWriteSetStats,
}

#[derive(Clone, Debug)]
struct StorageWriteGroup {
    space: StorageSpace,
    puts: Vec<PutEntry>,
    deletes: Vec<Key>,
    conflicting_declarations: Vec<StorageSpace>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageWriteSetError {
    ConflictingSpaceDeclaration {
        id: SpaceId,
        existing_name: &'static str,
        incoming_name: &'static str,
    },
    DuplicateMutation {
        space: StorageSpace,
        key: Key,
    },
    Backend(BackendError),
}

impl StorageWriteSet {
    /// Creates an empty canonical write set.
    ///
    /// Callers must stage at most one final mutation for each `(space, key)`.
    /// Debug builds validate that contract before lowering or commit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a canonical write set with capacity hints.
    pub fn with_capacity(_expected_mutations: usize, expected_spaces: usize) -> Self {
        Self {
            groups: Vec::with_capacity(expected_spaces),
            group_index: HashMap::with_capacity_and_hasher(
                expected_spaces,
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            ),
            stats: StorageWriteSetStats::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.groups
            .iter()
            .all(|group| group.puts.is_empty() && group.deletes.is_empty())
    }

    pub fn put(&mut self, space: StorageSpace, key: Key, value: StoredValue) {
        self.stats.staged_puts += 1;
        self.stats.written_bytes += value.bytes.len() as u64;
        self.group_mut(space).puts.push(PutEntry { key, value });
    }

    pub fn delete(&mut self, space: StorageSpace, key: Key) {
        self.stats.staged_deletes += 1;
        self.group_mut(space).deletes.push(key);
    }

    /// Reserves capacity for a storage space's grouped puts and deletes.
    ///
    /// This is most useful with canonical construction, where domain stores can
    /// often count final mutations before staging them.
    pub fn reserve_space(
        &mut self,
        space: StorageSpace,
        expected_puts: usize,
        expected_deletes: usize,
    ) {
        let group = self.group_mut(space);
        group.puts.reserve(expected_puts);
        group.deletes.reserve(expected_deletes);
    }

    pub fn extend(&mut self, other: StorageWriteSet) {
        for group in other.groups {
            let space = group.space;
            let conflicting_declarations = group.conflicting_declarations;
            for put in group.puts {
                self.put(space, put.key, put.value);
            }
            for delete in group.deletes {
                self.delete(space, delete);
            }

            let target = self.group_mut(space);
            target
                .conflicting_declarations
                .extend(conflicting_declarations);
        }
    }

    pub fn stats(&self) -> StorageWriteSetStats {
        self.stats.clone()
    }

    /// Validates the canonical write-set contract.
    ///
    /// This performs the full duplicate/conflicting-declaration scan only in
    /// debug builds. Release builds treat validation as a no-op so production
    /// lowering stays on the canonical hot path.
    pub fn validate(&self) -> Result<(), StorageWriteSetError> {
        #[cfg(not(debug_assertions))]
        {
            Ok(())
        }

        #[cfg(debug_assertions)]
        {
            for group in &self.groups {
                if let Some(incoming) = group.conflicting_declarations.first() {
                    return Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                        id: group.space.id,
                        existing_name: group.space.name,
                        incoming_name: incoming.name,
                    });
                }
            }

            let mut seen = HashMap::<(SpaceId, Key), StorageSpace, FastHashBuilder>::with_hasher(
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            );
            for group in &self.groups {
                for put in &group.puts {
                    let key = (group.space.id, put.key.clone());
                    if seen.insert(key, group.space).is_some() {
                        return Err(StorageWriteSetError::DuplicateMutation {
                            space: group.space,
                            key: put.key.clone(),
                        });
                    }
                }
                for delete in &group.deletes {
                    let key = (group.space.id, delete.clone());
                    if seen.insert(key, group.space).is_some() {
                        return Err(StorageWriteSetError::DuplicateMutation {
                            space: group.space,
                            key: delete.clone(),
                        });
                    }
                }
            }
            Ok(())
        }
    }

    pub fn lower_into<W>(self, write: &mut W) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: BackendWrite,
    {
        #[cfg(debug_assertions)]
        self.validate()?;
        self.lower_validated_into(write)
    }

    fn lower_validated_into<W>(
        self,
        write: &mut W,
    ) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: BackendWrite,
    {
        let StorageWriteSet {
            groups, mut stats, ..
        } = self;

        for group in groups {
            if !group.puts.is_empty() {
                stats.put_batches += 1;
                stats.backend_calls += 1;
                let entries = group
                    .puts
                    .into_iter()
                    .map(|entry| PutEntry {
                        key: group.space.encode_key(&entry.key),
                        value: entry.value,
                    })
                    .collect();
                write
                    .put_many(PutBatch { entries })
                    .map_err(StorageWriteSetError::Backend)?;
            }
            if !group.deletes.is_empty() {
                stats.delete_batches += 1;
                stats.backend_calls += 1;
                let deletes = group
                    .deletes
                    .iter()
                    .map(|key| group.space.encode_key(key))
                    .collect::<Vec<_>>();
                write
                    .delete_many(&deletes)
                    .map_err(StorageWriteSetError::Backend)?;
            }
        }

        Ok(stats)
    }

    pub fn commit<B>(
        self,
        backend: &B,
        opts: WriteOptions,
    ) -> Result<(CommitResult, StorageWriteSetStats), StorageWriteSetError>
    where
        B: Backend,
    {
        #[cfg(debug_assertions)]
        self.validate()?;
        let mut write = backend
            .begin_write(opts)
            .map_err(StorageWriteSetError::Backend)?;
        let stats = match self.lower_validated_into(&mut write) {
            Ok(stats) => stats,
            Err(error) => {
                let _ = write.rollback();
                return Err(error);
            }
        };
        let result = write.commit().map_err(StorageWriteSetError::Backend)?;
        Ok((result, stats))
    }

    fn group_mut(&mut self, space: StorageSpace) -> &mut StorageWriteGroup {
        if let Some(index) = self.group_index.get(&space.id).copied() {
            let group = &mut self.groups[index];
            if group.space.name != space.name {
                group.conflicting_declarations.push(space);
            }
            return group;
        }

        let index = self.groups.len();
        self.group_index.insert(space.id, index);
        self.stats.touched_spaces += 1;
        self.groups.push(StorageWriteGroup::new(space));
        let group = &mut self.groups[index];
        if group.space.name != space.name {
            group.conflicting_declarations.push(space);
        }
        group
    }
}

impl Default for StorageWriteSet {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            group_index: HashMap::with_hasher(FastHashBuilder::with_seeds(0, 0, 0, 0)),
            stats: StorageWriteSetStats::default(),
        }
    }
}

impl StorageWriteGroup {
    fn new(space: StorageSpace) -> Self {
        Self {
            space,
            puts: Vec::new(),
            deletes: Vec::new(),
            conflicting_declarations: Vec::new(),
        }
    }
}

impl fmt::Display for StorageWriteSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageWriteSetError::ConflictingSpaceDeclaration {
                id,
                existing_name,
                incoming_name,
            } => write!(
                f,
                "conflicting storage space declarations for {id:?}: {existing_name} vs {incoming_name}"
            ),
            StorageWriteSetError::DuplicateMutation { space, key } => {
                write!(f, "duplicate storage mutation for {space}/{key:?}")
            }
            StorageWriteSetError::Backend(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for StorageWriteSetError {}

impl From<BackendError> for StorageWriteSetError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use crate::backend::{
        Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
        BufferedRangeScan, CommitResult, GetOptions, InMemoryBackend, Key, KeyRange, PointVisitor,
        PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId, StoredValue,
        WriteConcurrency, WriteOptions, WriteStats,
    };
    use crate::storage::{StorageSpace, StorageWriteSet, StorageWriteSetError};

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
    fn write_set_rejects_duplicate_final_mutations() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.delete(space(1), key("a"));

        let error = writes
            .commit(&backend, WriteOptions::default())
            .expect_err("duplicate mutation should fail");

        assert!(matches!(
            error,
            StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: ref duplicate_key
            } if duplicate_space == space(1) && *duplicate_key == key("a")
        ));
        assert!(error
            .to_string()
            .contains("duplicate storage mutation for test.space.one"));
    }

    #[test]
    fn duplicate_puts_are_rejected() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("a"), value("B"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));
    }

    #[test]
    fn duplicate_deletes_are_rejected() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.delete(space(1), key("a"));
        writes.delete(space(1), key("a"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));
    }

    #[test]
    fn put_records_stats_without_immediate_duplicate_validation() {
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("a"), value("B"));

        let stats = writes.stats();
        assert_eq!(stats.staged_puts, 2);
        assert_eq!(stats.written_bytes, 2);

        assert!(matches!(
            writes.validate(),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));
    }

    #[test]
    fn delete_records_stats_without_immediate_duplicate_validation() {
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.delete(space(1), key("a"));

        let stats = writes.stats();
        assert_eq!(stats.staged_puts, 1);
        assert_eq!(stats.staged_deletes, 1);

        assert!(matches!(
            writes.validate(),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));
    }

    #[test]
    fn canonical_staging_tracks_stats_and_lowers_without_duplicate_index() {
        let mut writes = StorageWriteSet::with_capacity(3, 2);
        writes.reserve_space(space(1), 2, 0);
        writes.reserve_space(space(2), 0, 1);
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        writes.delete(space(2), key("c"));

        let stats = writes.stats();
        assert_eq!(stats.staged_puts, 2);
        assert_eq!(stats.staged_deletes, 1);
        assert_eq!(stats.touched_spaces, 2);
        assert_eq!(stats.written_bytes, 2);

        let mut write = CountingWrite::default();
        let stats = writes.lower_into(&mut write).expect("lower");

        assert_eq!(write.put_batches.borrow().len(), 1);
        assert_eq!(write.delete_batches.borrow().len(), 1);
        assert_eq!(stats.put_batches, 1);
        assert_eq!(stats.delete_batches, 1);
        assert_eq!(stats.backend_calls, 2);
    }

    #[test]
    fn conflicting_space_declarations_are_rejected() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );
        writes.put(
            StorageSpace::new(SpaceId(1), "test.space.renamed"),
            key("b"),
            value("B"),
        );

        let error = writes
            .commit(&backend, WriteOptions::default())
            .expect_err("conflicting space declaration should fail");

        assert!(matches!(
            error,
            StorageWriteSetError::ConflictingSpaceDeclaration {
                id: SpaceId(1),
                existing_name: "test.space.one",
                incoming_name: "test.space.renamed",
            }
        ));
    }

    #[test]
    fn write_set_groups_by_space_id_not_name() {
        let mut writes = StorageWriteSet::new();
        writes.put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );
        writes.put(
            StorageSpace::new(SpaceId(1), "test.space.renamed"),
            key("b"),
            value("B"),
        );

        let stats = writes.stats();

        assert_eq!(stats.touched_spaces, 1);
        assert!(matches!(
            writes.validate(),
            Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                id: SpaceId(1),
                existing_name: "test.space.one",
                incoming_name: "test.space.renamed",
            })
        ));
    }

    #[test]
    fn conflicting_space_declaration_across_extend_is_rejected() {
        let backend = InMemoryBackend::new();
        let mut left = StorageWriteSet::new();
        left.put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );

        let mut right = StorageWriteSet::new();
        right.put(
            StorageSpace::new(SpaceId(1), "test.space.renamed"),
            key("b"),
            value("B"),
        );

        left.extend(right);

        assert!(matches!(
            left.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                id: SpaceId(1),
                existing_name: "test.space.one",
                incoming_name: "test.space.renamed",
            })
        ));
    }

    #[test]
    fn duplicate_validation_happens_before_opening_backend_write() {
        let backend = CountingBackend::default();
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("a"), value("B"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::DuplicateMutation { .. })
        ));
        assert_eq!(backend.state.begin_write_calls.get(), 0);
        assert_eq!(backend.state.commit_calls.get(), 0);
        assert_eq!(backend.state.rollback_calls.get(), 0);
    }

    #[test]
    fn conflicting_space_validation_happens_before_opening_backend_write() {
        let backend = CountingBackend::default();
        let mut writes = StorageWriteSet::new();
        writes.put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );
        writes.put(
            StorageSpace::new(SpaceId(1), "test.space.renamed"),
            key("b"),
            value("B"),
        );

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::ConflictingSpaceDeclaration { .. })
        ));
        assert_eq!(backend.state.begin_write_calls.get(), 0);
        assert_eq!(backend.state.commit_calls.get(), 0);
        assert_eq!(backend.state.rollback_calls.get(), 0);
    }

    #[test]
    fn lower_failure_rolls_back_once() {
        let backend = CountingBackend::failing(FailPoint::PutMany);
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::Backend(BackendError::Io(message))) if message == "put_many failed"
        ));
        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 0);
        assert_eq!(backend.state.rollback_calls.get(), 1);
    }

    #[test]
    fn delete_lower_failure_rolls_back_once() {
        let backend = CountingBackend::failing(FailPoint::DeleteMany);
        let mut writes = StorageWriteSet::new();
        writes.delete(space(1), key("a"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::Backend(BackendError::Io(message))) if message == "delete_many failed"
        ));
        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 0);
        assert_eq!(backend.state.rollback_calls.get(), 1);
    }

    #[test]
    fn commit_failure_is_reported_without_successful_commit_stats() {
        let backend = CountingBackend::failing(FailPoint::Commit);
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::Backend(BackendError::Durability))
        ));
        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 1);
        assert_eq!(backend.state.rollback_calls.get(), 0);
        assert!(backend.state.put_batches.borrow().is_empty());
    }

    #[test]
    fn same_key_in_different_spaces_is_allowed() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(2), key("a"), value("B"));

        writes
            .commit(&backend, WriteOptions::default())
            .expect("different spaces are independent");
    }

    #[test]
    fn lower_into_groups_by_space_and_operation() {
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        writes.put(space(2), key("a"), value("C"));
        writes.delete(space(1), key("c"));
        writes.delete(space(2), key("d"));

        let mut write = CountingWrite::default();
        let stats = writes.lower_into(&mut write).expect("lower");

        assert_eq!(write.put_batches.borrow().len(), 2);
        assert_eq!(write.delete_batches.borrow().len(), 2);
        assert_eq!(write.commit_calls.get(), 0);
        assert_eq!(stats.put_batches, 2);
        assert_eq!(stats.delete_batches, 2);
        assert_eq!(stats.backend_calls, 4);
    }

    #[test]
    fn commit_uses_one_backend_write_and_one_commit() {
        let backend = CountingBackend::default();
        let mut writes = StorageWriteSet::new();
        writes.put(space(1), key("a"), value("A"));
        writes.delete(space(1), key("b"));

        writes
            .commit(&backend, WriteOptions::default())
            .expect("commit");

        assert_eq!(backend.state.begin_write_calls.get(), 1);
        assert_eq!(backend.state.commit_calls.get(), 1);
        assert_eq!(backend.state.rollback_calls.get(), 0);
        assert_eq!(backend.state.put_batches.borrow().len(), 1);
        assert_eq!(backend.state.delete_batches.borrow().len(), 1);
    }

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Rc<CountingState>,
    }

    #[derive(Default)]
    struct CountingState {
        begin_write_calls: Cell<u64>,
        commit_calls: Cell<u64>,
        rollback_calls: Cell<u64>,
        put_batches: RefCell<Vec<(SpaceId, Vec<Key>)>>,
        delete_batches: RefCell<Vec<(SpaceId, Vec<Key>)>>,
        fail_point: Cell<Option<FailPoint>>,
    }

    #[derive(Clone, Default)]
    struct CountingRead;

    struct CountingWrite {
        state: Rc<CountingState>,
        commit_calls: Cell<u64>,
        put_batches: RefCell<Vec<(SpaceId, Vec<Key>)>>,
        delete_batches: RefCell<Vec<(SpaceId, Vec<Key>)>>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FailPoint {
        PutMany,
        DeleteMany,
        Commit,
    }

    impl CountingBackend {
        fn failing(fail_point: FailPoint) -> Self {
            let backend = Self::default();
            backend.state.fail_point.set(Some(fail_point));
            backend
        }
    }

    impl Default for CountingWrite {
        fn default() -> Self {
            Self {
                state: Rc::new(CountingState::default()),
                commit_calls: Cell::new(0),
                put_batches: RefCell::new(Vec::new()),
                delete_batches: RefCell::new(Vec::new()),
            }
        }
    }

    impl Backend for CountingBackend {
        type Read<'a>
            = CountingRead
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
            Ok(CountingRead)
        }

        fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
            self.state
                .begin_write_calls
                .set(self.state.begin_write_calls.get() + 1);
            Ok(CountingWrite {
                state: Rc::clone(&self.state),
                commit_calls: Cell::new(0),
                put_batches: RefCell::new(Vec::new()),
                delete_batches: RefCell::new(Vec::new()),
            })
        }
    }

    impl BackendRead for CountingRead {
        type RangeScan<'a> = BufferedRangeScan;

        fn visit_keys<V>(
            &self,
            _keys: &[Key],
            _opts: GetOptions<'_>,
            _visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            unimplemented!("not used by write-set tests")
        }

        fn with_range_scan<T, F>(
            &self,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
            _f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            unimplemented!("not used by write-set tests")
        }
    }

    impl BackendWrite for CountingWrite {
        fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
            if self.state.fail_point.get() == Some(FailPoint::PutMany) {
                return Err(BackendError::Io("put_many failed".to_string()));
            }
            let space = entries
                .entries
                .first()
                .map(|entry| physical_space(&entry.key))
                .unwrap_or(SpaceId(0));
            let keys = entries
                .entries
                .into_iter()
                .map(|entry| logical_key(entry.key))
                .collect();
            self.put_batches.borrow_mut().push((space, keys));
            Ok(())
        }

        fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
            if self.state.fail_point.get() == Some(FailPoint::DeleteMany) {
                return Err(BackendError::Io("delete_many failed".to_string()));
            }
            let space = keys.first().map(physical_space).unwrap_or(SpaceId(0));
            self.delete_batches
                .borrow_mut()
                .push((space, keys.iter().cloned().map(logical_key).collect()));
            Ok(())
        }

        fn delete_range(&mut self, _range: KeyRange) -> Result<(), BackendError> {
            Ok(())
        }

        fn commit(self) -> Result<CommitResult, BackendError> {
            self.state
                .commit_calls
                .set(self.state.commit_calls.get() + 1);
            if self.state.fail_point.get() == Some(FailPoint::Commit) {
                return Err(BackendError::Durability);
            }
            self.state
                .put_batches
                .borrow_mut()
                .extend(self.put_batches.into_inner());
            self.state
                .delete_batches
                .borrow_mut()
                .extend(self.delete_batches.into_inner());
            Ok(CommitResult {
                commit_id: None,
                stats: WriteStats::default(),
            })
        }

        fn rollback(self) -> Result<(), BackendError> {
            self.state
                .rollback_calls
                .set(self.state.rollback_calls.get() + 1);
            Ok(())
        }
    }

    fn physical_space(key: &Key) -> SpaceId {
        SpaceId(u32::from_be_bytes(
            key.0[..4].try_into().expect("space prefix"),
        ))
    }

    fn logical_key(key: Key) -> Key {
        Key(key.0.slice(4..))
    }
}
