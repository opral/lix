use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use crate::backend_v2::{
    Backend, BackendError, BackendWrite, CommitResult, Key, PutBatch, PutEntry, SpaceId,
    StoredValue, WriteOptions,
};
use crate::storage_v2::{StorageSpace, StorageWriteSetStats};
use ahash::RandomState;

type FastHashBuilder = RandomState;

#[derive(Clone, Debug)]
pub struct StorageWriteSet {
    groups: Vec<StorageWriteGroup>,
    group_index: HashMap<SpaceId, usize, FastHashBuilder>,
    mutation_index: Option<HashMap<(SpaceId, Key), StorageSpace, FastHashBuilder>>,
    duplicate_mutations: Vec<(StorageSpace, Key)>,
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
    /// Creates an empty checked write set.
    ///
    /// Checked write sets validate duplicate `(space, key)` mutations while
    /// staging. This is the safe default for generic callers and tests.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a checked write set with capacity hints.
    ///
    /// Use this when the caller has not already canonicalized final mutations.
    pub fn checked_with_capacity(expected_mutations: usize, expected_spaces: usize) -> Self {
        Self {
            groups: Vec::with_capacity(expected_spaces),
            group_index: HashMap::with_capacity_and_hasher(
                expected_spaces,
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            ),
            mutation_index: Some(HashMap::with_capacity_and_hasher(
                expected_mutations,
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            )),
            duplicate_mutations: Vec::new(),
            stats: StorageWriteSetStats::default(),
        }
    }

    /// Creates a canonical write set with capacity hints.
    ///
    /// Canonical write sets skip per-mutation duplicate validation. Use this
    /// only when the caller has already produced at most one final mutation for
    /// each `(StorageSpace.id, Key)`.
    pub fn canonical_with_capacity(_expected_mutations: usize, expected_spaces: usize) -> Self {
        Self {
            groups: Vec::with_capacity(expected_spaces),
            group_index: HashMap::with_capacity_and_hasher(
                expected_spaces,
                FastHashBuilder::with_seeds(0, 0, 0, 0),
            ),
            mutation_index: None,
            duplicate_mutations: Vec::new(),
            stats: StorageWriteSetStats::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.groups
            .iter()
            .all(|group| group.puts.is_empty() && group.deletes.is_empty())
    }

    pub fn stage_put(&mut self, space: StorageSpace, key: Key, value: StoredValue) {
        if let Err(error) = self.try_stage_put(space, key, value) {
            self.record_stage_error(error);
        }
    }

    pub fn stage_delete(&mut self, space: StorageSpace, key: Key) {
        if let Err(error) = self.try_stage_delete(space, key) {
            self.record_stage_error(error);
        }
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

    /// Stages a final put from a caller that has already canonicalized
    /// mutations for each `(space, key)`.
    pub fn stage_canonical_put(&mut self, space: StorageSpace, key: Key, value: StoredValue) {
        self.stats.staged_puts += 1;
        self.stats.written_bytes += value.bytes.len() as u64;
        self.group_mut(space).puts.push(PutEntry { key, value });
    }

    /// Stages a final delete from a caller that has already canonicalized
    /// mutations for each `(space, key)`.
    pub fn stage_canonical_delete(&mut self, space: StorageSpace, key: Key) {
        self.stats.staged_deletes += 1;
        self.group_mut(space).deletes.push(key);
    }

    pub fn try_stage_put(
        &mut self,
        space: StorageSpace,
        key: Key,
        value: StoredValue,
    ) -> Result<(), StorageWriteSetError> {
        self.record_mutation(space, &key)?;
        self.stats.staged_puts += 1;
        self.stats.written_bytes += value.bytes.len() as u64;
        self.group_mut(space).puts.push(PutEntry { key, value });
        Ok(())
    }

    pub fn try_stage_delete(
        &mut self,
        space: StorageSpace,
        key: Key,
    ) -> Result<(), StorageWriteSetError> {
        self.record_mutation(space, &key)?;
        self.stats.staged_deletes += 1;
        self.group_mut(space).deletes.push(key);
        Ok(())
    }

    pub fn extend(&mut self, other: StorageWriteSet) {
        self.duplicate_mutations.extend(other.duplicate_mutations);

        for group in other.groups {
            let space = group.space;
            let conflicting_declarations = group.conflicting_declarations;
            for put in group.puts {
                self.stage_put(space, put.key, put.value);
            }
            for delete in group.deletes {
                self.stage_delete(space, delete);
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

    pub fn validate(&self) -> Result<(), StorageWriteSetError> {
        for group in &self.groups {
            if let Some(incoming) = group.conflicting_declarations.first() {
                return Err(StorageWriteSetError::ConflictingSpaceDeclaration {
                    id: group.space.id,
                    existing_name: group.space.name,
                    incoming_name: incoming.name,
                });
            }
        }

        if let Some((space, key)) = self.duplicate_mutations.first() {
            return Err(StorageWriteSetError::DuplicateMutation {
                space: *space,
                key: key.clone(),
            });
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

    fn record_mutation(
        &mut self,
        space: StorageSpace,
        key: &Key,
    ) -> Result<(), StorageWriteSetError> {
        let mutation_index = self
            .mutation_index
            .get_or_insert_with(|| HashMap::with_hasher(FastHashBuilder::with_seeds(0, 0, 0, 0)));

        match mutation_index.entry((space.id, key.clone())) {
            Entry::Occupied(_) => Err(StorageWriteSetError::DuplicateMutation {
                space,
                key: key.clone(),
            }),
            Entry::Vacant(entry) => {
                entry.insert(space);
                Ok(())
            }
        }
    }

    fn record_stage_error(&mut self, error: StorageWriteSetError) {
        match error {
            StorageWriteSetError::DuplicateMutation { space, key } => {
                self.duplicate_mutations.push((space, key));
            }
            StorageWriteSetError::ConflictingSpaceDeclaration { .. }
            | StorageWriteSetError::Backend(_) => {
                unreachable!("infallible staging can only record duplicate mutation errors")
            }
        }
    }
}

impl Default for StorageWriteSet {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            group_index: HashMap::with_hasher(FastHashBuilder::with_seeds(0, 0, 0, 0)),
            mutation_index: None,
            duplicate_mutations: Vec::new(),
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

    use crate::backend_v2::{
        Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, BufferedScanCursor,
        CommitResult, ConformanceBackend, GetOptions, Key, KeyRange, PointVisitor, PutBatch,
        ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId, StoredValue, WriteConcurrency,
        WriteOptions, WriteStats,
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
    fn try_stage_put_rejects_duplicate_immediately() {
        let mut writes = StorageWriteSet::new();
        writes
            .try_stage_put(space(1), key("a"), value("A"))
            .expect("first put");

        assert!(matches!(
            writes.try_stage_put(space(1), key("a"), value("B")),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));

        let stats = writes.stats();
        assert_eq!(stats.staged_puts, 1);
        assert_eq!(stats.written_bytes, 1);
    }

    #[test]
    fn try_stage_delete_rejects_duplicate_against_put_immediately() {
        let mut writes = StorageWriteSet::new();
        writes
            .try_stage_put(space(1), key("a"), value("A"))
            .expect("put");

        assert!(matches!(
            writes.try_stage_delete(space(1), key("a")),
            Err(StorageWriteSetError::DuplicateMutation {
                space: duplicate_space,
                key: duplicate_key
            }) if duplicate_space == space(1) && duplicate_key == key("a")
        ));

        let stats = writes.stats();
        assert_eq!(stats.staged_puts, 1);
        assert_eq!(stats.staged_deletes, 0);
    }

    #[test]
    fn canonical_staging_tracks_stats_and_lowers_without_duplicate_index() {
        let mut writes = StorageWriteSet::canonical_with_capacity(3, 2);
        writes.reserve_space(space(1), 2, 0);
        writes.reserve_space(space(2), 0, 1);
        writes.stage_canonical_put(space(1), key("a"), value("A"));
        writes.stage_canonical_put(space(1), key("b"), value("B"));
        writes.stage_canonical_delete(space(2), key("c"));

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
        type ScanCursor<'a>
            = BufferedScanCursor
        where
            Self: 'a;

        fn visit_many<V>(
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

        fn open_scan_cursor(
            &self,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
        ) -> Result<Self::ScanCursor<'_>, BackendError> {
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
