use std::collections::HashMap;
use std::fmt;

use crate::backend::{
    Backend, BackendError, BackendWrite, CommitResult, Key, PutBatch, PutEntry, SpaceId,
    StoredValue, WriteOptions,
};
use crate::storage::{StorageSpace, StorageWriteSetStats};
use ahash::RandomState;
use bytes::Bytes;

type FastHashBuilder = RandomState;

pub trait IntoStorageSpace {
    fn into_storage_space(self) -> StorageSpace;
}

impl IntoStorageSpace for StorageSpace {
    fn into_storage_space(self) -> StorageSpace {
        self
    }
}

pub trait IntoStorageKey {
    fn into_storage_key(self) -> Key;
}

impl IntoStorageKey for Key {
    fn into_storage_key(self) -> Key {
        self
    }
}

impl IntoStorageKey for Vec<u8> {
    fn into_storage_key(self) -> Key {
        Key(Bytes::from(self))
    }
}

impl IntoStorageKey for &[u8] {
    fn into_storage_key(self) -> Key {
        Key(Bytes::copy_from_slice(self))
    }
}

pub trait IntoStorageValue {
    fn into_storage_value(self) -> StoredValue;
}

impl IntoStorageValue for StoredValue {
    fn into_storage_value(self) -> StoredValue {
        self
    }
}

impl IntoStorageValue for Vec<u8> {
    fn into_storage_value(self) -> StoredValue {
        StoredValue {
            bytes: Bytes::from(self),
        }
    }
}

impl IntoStorageValue for &[u8] {
    fn into_storage_value(self) -> StoredValue {
        StoredValue {
            bytes: Bytes::copy_from_slice(self),
        }
    }
}

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
    /// The set validates that contract before lowering or commit.
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

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn apply<B>(
        self,
        writer: &mut crate::storage::context::StorageWriteTransaction<'_, B>,
    ) -> Result<StorageWriteSetStats, crate::LixError>
    where
        B: Backend,
    {
        writer.write_set(self).await
    }

    pub fn put<S, K, V>(&mut self, space: S, key: K, value: V)
    where
        S: IntoStorageSpace,
        K: IntoStorageKey,
        V: IntoStorageValue,
    {
        let value = value.into_storage_value();
        self.stats.staged_puts += 1;
        self.stats.written_bytes += value.bytes.len() as u64;
        self.group_mut(space.into_storage_space())
            .puts
            .push(PutEntry {
                key: key.into_storage_key(),
                value,
            });
    }

    /// Stages a content-addressed put, coalescing an identical put already in
    /// this write set.
    ///
    /// A same-key, different-value mutation is still staged twice so normal
    /// duplicate validation rejects the hash/key invariant violation.
    pub(crate) fn put_content_addressed(
        &mut self,
        space: StorageSpace,
        key: Key,
        value: StoredValue,
    ) {
        let already_staged = self
            .group_mut(space)
            .puts
            .iter()
            .any(|put| put.key == key && put.value == value);
        if !already_staged {
            self.put(space, key, value);
        }
    }

    pub fn delete<S, K>(&mut self, space: S, key: K)
    where
        S: IntoStorageSpace,
        K: IntoStorageKey,
    {
        self.stats.staged_deletes += 1;
        self.group_mut(space.into_storage_space())
            .deletes
            .push(key.into_storage_key());
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

    pub fn extend(&mut self, other: Self) {
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
        self.stats
    }

    /// Validates the canonical write-set contract.
    ///
    /// This performs the full duplicate/conflicting-declaration scan before
    /// lowering so the backend never receives ambiguous final mutations.
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

    pub async fn lower_into<W>(
        self,
        write: &mut W,
    ) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: BackendWrite,
    {
        self.validate()?;
        self.lower_validated_into(write).await
    }

    async fn lower_validated_into<W>(
        self,
        write: &mut W,
    ) -> Result<StorageWriteSetStats, StorageWriteSetError>
    where
        W: BackendWrite,
    {
        let Self {
            groups, mut stats, ..
        } = self;

        for mut group in groups {
            // Lower each space batch in ascending key order. Hash-keyed
            // spaces such as json_store produce effectively random insertion
            // order; sorted batches let B-tree backends write with cursor
            // locality instead of a fresh seek per key. Most other spaces
            // already produce key order (BTreeMap
            // iteration, time-ordered ids), so the common case is a
            // read-only scan.
            let puts_sorted = group
                .puts
                .is_sorted_by(|left, right| left.key.0 <= right.key.0);
            let deletes_sorted = group.deletes.is_sorted();
            #[cfg(feature = "storage-benches")]
            if order_stats_enabled() && !group.puts.is_empty() {
                eprintln!(
                    "write-set-order space={} puts={} puts_sorted={puts_sorted} deletes={} deletes_sorted={deletes_sorted}",
                    group.space.name,
                    group.puts.len(),
                    group.deletes.len(),
                );
            }
            if !puts_sorted {
                group
                    .puts
                    .sort_unstable_by(|left, right| left.key.0.cmp(&right.key.0));
            }
            if !deletes_sorted {
                group.deletes.sort_unstable();
            }
            if !group.puts.is_empty() {
                stats.put_batches += 1;
                stats.backend_calls += 1;
                write
                    .put_many(
                        group.space.id,
                        PutBatch {
                            entries: group.puts,
                        },
                    )
                    .await
                    .map_err(StorageWriteSetError::Backend)?;
            }
            if !group.deletes.is_empty() {
                stats.delete_batches += 1;
                stats.backend_calls += 1;
                write
                    .delete_many(group.space.id, &group.deletes)
                    .await
                    .map_err(StorageWriteSetError::Backend)?;
            }
        }

        Ok(stats)
    }

    pub async fn commit<B>(
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
            .await
            .map_err(StorageWriteSetError::Backend)?;
        let stats = match self.lower_validated_into(&mut write).await {
            Ok(stats) => stats,
            Err(error) => {
                let _ = write.rollback().await;
                return Err(error);
            }
        };
        let result = write
            .commit()
            .await
            .map_err(StorageWriteSetError::Backend)?;
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
            Self::ConflictingSpaceDeclaration {
                id,
                existing_name,
                incoming_name,
            } => write!(
                f,
                "conflicting storage space declarations for {id:?}: {existing_name} vs {incoming_name}"
            ),
            Self::DuplicateMutation { space, key } => {
                write!(f, "duplicate storage mutation for {space}/{key:?}")
            }
            Self::Backend(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for StorageWriteSetError {}

impl From<BackendError> for StorageWriteSetError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

#[cfg(feature = "storage-benches")]
fn order_stats_enabled() -> bool {
    use std::sync::LazyLock;
    static ENABLED: LazyLock<bool> =
        LazyLock::new(|| std::env::var_os("LIX_WRITE_SET_ORDER_STATS").is_some());
    *ENABLED
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::backend::{InMemoryBackend, Key, SpaceId, StoredValue, WriteOptions};
    use crate::storage::{StorageSpace, StorageWriteSet, StorageWriteSetError};

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
    async fn write_set_rejects_duplicate_final_mutations_before_backend_write() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(), key("a"), value("A"));
        writes.delete(space(), key("a"));

        let error = writes
            .commit(&backend, WriteOptions::default())
            .await
            .expect_err("duplicate mutation");

        assert!(matches!(
            error,
            StorageWriteSetError::DuplicateMutation { .. }
        ));
    }

    #[tokio::test]
    async fn write_set_lowers_batches_and_commits_asynchronously() {
        let backend = InMemoryBackend::new();
        let mut writes = StorageWriteSet::new();
        writes.put(space(), key("b"), value("B"));
        writes.put(space(), key("a"), value("A"));
        writes.delete(space(), key("missing"));

        let (commit, stats) = writes
            .commit(&backend, WriteOptions::default())
            .await
            .expect("commit");

        assert_eq!(stats.put_batches, 1);
        assert_eq!(stats.delete_batches, 1);
        assert_eq!(commit.stats.put_entries, 2);
        assert_eq!(commit.stats.deleted_entries, 1);
    }
}
