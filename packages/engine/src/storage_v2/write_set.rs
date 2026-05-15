use std::collections::{BTreeMap, HashSet};
use std::fmt;

use crate::backend_v2::{
    Backend, BackendError, BackendWrite, CommitResult, Key, PutBatch, PutEntry, SpaceId,
    StoredValue, WriteOptions,
};
use crate::storage_v2::{StorageSpace, StorageWriteSetStats};
use ahash::RandomState;

type FastHashBuilder = RandomState;

#[derive(Clone, Debug, Default)]
pub struct StorageWriteSet {
    groups: BTreeMap<SpaceId, StorageWriteGroup>,
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.groups
            .values()
            .all(|group| group.puts.is_empty() && group.deletes.is_empty())
    }

    pub fn stage_put(&mut self, space: StorageSpace, key: Key, value: StoredValue) {
        self.group_mut(space).puts.push(PutEntry { key, value });
    }

    pub fn stage_delete(&mut self, space: StorageSpace, key: Key) {
        self.group_mut(space).deletes.push(key);
    }

    pub fn extend(&mut self, other: StorageWriteSet) {
        for group in other.groups.into_values() {
            let target = self.group_mut(group.space);
            target.puts.extend(group.puts);
            target.deletes.extend(group.deletes);
            target
                .conflicting_declarations
                .extend(group.conflicting_declarations);
        }
    }

    pub fn stats(&self) -> StorageWriteSetStats {
        let mut stats = StorageWriteSetStats {
            touched_spaces: self.groups.len() as u64,
            ..StorageWriteSetStats::default()
        };

        for group in self.groups.values() {
            stats.staged_puts += group.puts.len() as u64;
            stats.staged_deletes += group.deletes.len() as u64;
            stats.written_bytes += group
                .puts
                .iter()
                .map(|entry| entry.value.bytes.len() as u64)
                .sum::<u64>();
        }

        stats
    }

    pub fn validate(&self) -> Result<(), StorageWriteSetError> {
        for group in self.groups.values() {
            if let Some(incoming) = group.conflicting_declarations.first() {
                return Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                    id: group.space.id,
                    existing_name: group.space.name,
                    incoming_name: incoming.name,
                });
            }
        }

        let mut mutation_count = 0;
        for group in self.groups.values() {
            mutation_count += group.puts.len() + group.deletes.len();
        }

        let mut seen = HashSet::<(SpaceId, &Key), FastHashBuilder>::with_capacity_and_hasher(
            mutation_count,
            FastHashBuilder::with_seeds(0, 0, 0, 0),
        );
        for group in self.groups.values() {
            for put in &group.puts {
                if !seen.insert((group.space.id, &put.key)) {
                    return Err(StorageWriteSetError::DuplicateMutation {
                        space: group.space,
                        key: put.key.clone(),
                    });
                }
            }
            for delete in &group.deletes {
                if !seen.insert((group.space.id, delete)) {
                    return Err(StorageWriteSetError::DuplicateMutation {
                        space: group.space,
                        key: delete.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn lower_into<W>(self, write: &mut W) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: BackendWrite,
    {
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
        let mut stats = StorageWriteSetStats {
            touched_spaces: self.groups.len() as u64,
            ..StorageWriteSetStats::default()
        };

        for group in self.groups.into_values() {
            if !group.puts.is_empty() {
                stats.staged_puts += group.puts.len() as u64;
                stats.written_bytes += group
                    .puts
                    .iter()
                    .map(|entry| entry.value.bytes.len() as u64)
                    .sum::<u64>();
                stats.put_batches += 1;
                stats.backend_calls += 1;
                write
                    .put_many(
                        group.space.id,
                        PutBatch {
                            entries: group.puts,
                        },
                    )
                    .map_err(StorageWriteSetError::Backend)?;
            }
            if !group.deletes.is_empty() {
                stats.staged_deletes += group.deletes.len() as u64;
                stats.delete_batches += 1;
                stats.backend_calls += 1;
                write
                    .delete_many(group.space.id, &group.deletes)
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
        let group = self
            .groups
            .entry(space.id)
            .or_insert_with(|| StorageWriteGroup::new(space));
        if group.space.name != space.name {
            group.conflicting_declarations.push(space);
        }
        group
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

    use crate::backend_v2::{
        Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
        ConformanceBackend, GetManyResult, GetOptions, Key, KeyRange, PutBatch, ReadOptions,
        ScanOptions, ScanPage, SpaceId, StoredValue, WriteConcurrency, WriteOptions, WriteStats,
    };
    use crate::storage_v2::{StorageSpace, StorageWriteSet, StorageWriteSetError};

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
        let backend = ConformanceBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_delete(space(1), key("a"));

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
        let backend = ConformanceBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("a"), value("B"));

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
        let backend = ConformanceBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.stage_delete(space(1), key("a"));
        writes.stage_delete(space(1), key("a"));

        assert!(matches!(
            writes.commit(&backend, WriteOptions::default()),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));
    }

    #[test]
    fn conflicting_space_declarations_are_rejected() {
        let backend = ConformanceBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.stage_put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );
        writes.stage_put(
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
        writes.stage_put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );
        writes.stage_put(
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
        let backend = ConformanceBackend::new();
        let mut left = StorageWriteSet::new();
        left.stage_put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );

        let mut right = StorageWriteSet::new();
        right.stage_put(
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
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("a"), value("B"));

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
        writes.stage_put(
            StorageSpace::new(SpaceId(1), "test.space.one"),
            key("a"),
            value("A"),
        );
        writes.stage_put(
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
        writes.stage_put(space(1), key("a"), value("A"));

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
        writes.stage_delete(space(1), key("a"));

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
        writes.stage_put(space(1), key("a"), value("A"));

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
        let backend = ConformanceBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(2), key("a"), value("B"));

        writes
            .commit(&backend, WriteOptions::default())
            .expect("different spaces are independent");
    }

    #[test]
    fn lower_into_groups_by_space_and_operation() {
        let mut writes = StorageWriteSet::new();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("b"), value("B"));
        writes.stage_put(space(2), key("a"), value("C"));
        writes.stage_delete(space(1), key("c"));
        writes.stage_delete(space(2), key("d"));

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
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_delete(space(1), key("b"));

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

    #[derive(Default)]
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
        fn get_many(
            &self,
            _space: SpaceId,
            _keys: &[Key],
            _opts: GetOptions<'_>,
        ) -> Result<GetManyResult, BackendError> {
            unimplemented!("not used by write-set tests")
        }

        fn scan_range(
            &self,
            _space: SpaceId,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
        ) -> Result<ScanPage, BackendError> {
            unimplemented!("not used by write-set tests")
        }
    }

    impl BackendWrite for CountingWrite {
        fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
            if self.state.fail_point.get() == Some(FailPoint::PutMany) {
                return Err(BackendError::Io("put_many failed".to_string()));
            }
            let keys = entries.entries.into_iter().map(|entry| entry.key).collect();
            self.put_batches.borrow_mut().push((space, keys));
            Ok(())
        }

        fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
            if self.state.fail_point.get() == Some(FailPoint::DeleteMany) {
                return Err(BackendError::Io("delete_many failed".to_string()));
            }
            self.delete_batches
                .borrow_mut()
                .push((space, keys.to_vec()));
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
}
