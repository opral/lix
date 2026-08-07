use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::storage::{
    Key, Precondition, PutBatch, PutEntry, Storage, StorageError, StorageWrite, StoredValue,
    WriteOptions,
};

use super::codec::corruption;
use super::model::{GlobalSelectorV1, global_selector_key};
use super::object::{OBJECT_SPACE, ObjectId};
use super::tree::ImmutableObjectSet;
use super::view::{CoherentView, SELECTOR_SPACE};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SelectorExpectation {
    Absent,
    Equals(Bytes),
}

/// One prepared atomic publication. It always exact-CASes and rotates the
/// global epoch, including fully deduplicated and root-only publications.
/// Object and selector puts are staged into the same storage write; no extra
/// flush or round trip exists at this boundary.
#[derive(Debug)]
pub(crate) struct PreparedPublication {
    expected_global: Bytes,
    next_global: GlobalSelectorV1,
    selector_expectations: BTreeMap<Bytes, SelectorExpectation>,
    selector_puts: BTreeMap<Bytes, Bytes>,
    selector_deletes: BTreeSet<Bytes>,
    object_puts: ImmutableObjectSet,
    object_deletes: BTreeSet<ObjectId>,
}

impl PreparedPublication {
    /// Starts a branch/state publication and fences both raw selectors from
    /// the one coherent view used to derive it.
    pub(crate) fn from_branch_view<R>(view: &CoherentView<R>) -> Result<Self, StorageError>
    where
        R: crate::storage::StorageRead,
    {
        let mut publication = Self::from_global_epoch(view)?;
        publication.expect_selector(
            super::model::branch_selector_key(view.branch_id()),
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )?;
        Ok(publication)
    }

    /// Starts a receipt/GC publication whose only repository-wide read fence
    /// is the exact authenticated global selector. Receipt-specific expected
    /// bytes or absence must be added before commit.
    pub(crate) fn from_global_epoch<R>(view: &CoherentView<R>) -> Result<Self, StorageError>
    where
        R: crate::storage::StorageRead,
    {
        Ok(Self {
            expected_global: view.raw_global_selector().clone(),
            next_global: view.global_selector().rotated()?,
            selector_expectations: BTreeMap::new(),
            selector_puts: BTreeMap::new(),
            selector_deletes: BTreeSet::new(),
            object_puts: ImmutableObjectSet::default(),
            object_deletes: BTreeSet::new(),
        })
    }

    pub(crate) fn set_repository_root(&mut self, root: ObjectId) -> Result<(), StorageError> {
        if root == ObjectId::ZERO {
            return Err(corruption("publication repository root is zero"));
        }
        self.next_global.repository_root = root;
        Ok(())
    }

    pub(crate) fn expect_selector(
        &mut self,
        key: Bytes,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        if key == global_selector_key() {
            return Err(corruption(
                "global selector expectation is owned by the epoch fence",
            ));
        }
        match self.selector_expectations.get(&key) {
            Some(existing) if existing != &expected => Err(corruption(
                "publication assigns conflicting expectations to one selector",
            )),
            Some(_) => Ok(()),
            None => {
                self.selector_expectations.insert(key, expected);
                Ok(())
            }
        }
    }

    pub(crate) fn put_selector(
        &mut self,
        key: Bytes,
        value: Bytes,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        if key == global_selector_key() || self.selector_deletes.contains(&key) {
            return Err(corruption("publication has an invalid selector put"));
        }
        self.expect_selector(key.clone(), expected)?;
        match self.selector_puts.get(&key) {
            Some(existing) if existing != &value => {
                Err(corruption("publication assigns two values to one selector"))
            }
            Some(_) => Ok(()),
            None => {
                self.selector_puts.insert(key, value);
                Ok(())
            }
        }
    }

    pub(crate) fn delete_selector(
        &mut self,
        key: Bytes,
        expected: Bytes,
    ) -> Result<(), StorageError> {
        if key == global_selector_key() || self.selector_puts.contains_key(&key) {
            return Err(corruption("publication has an invalid selector delete"));
        }
        self.expect_selector(key.clone(), SelectorExpectation::Equals(expected))?;
        self.selector_deletes.insert(key);
        Ok(())
    }

    pub(crate) fn put_object(&mut self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        if self.object_deletes.contains(&id) {
            return Err(corruption("publication both puts and deletes one object"));
        }
        self.object_puts.insert(id, bytes)
    }

    pub(crate) fn put_objects(&mut self, objects: ImmutableObjectSet) -> Result<(), StorageError> {
        for (id, bytes) in objects.iter() {
            self.put_object(id, bytes.clone())?;
        }
        Ok(())
    }

    pub(crate) fn delete_object(&mut self, id: ObjectId) -> Result<(), StorageError> {
        if id == ObjectId::ZERO || self.object_puts.get(id).is_some() {
            return Err(corruption("publication has an invalid object delete"));
        }
        self.object_deletes.insert(id);
        Ok(())
    }

    pub(crate) async fn commit<S>(self, storage: &S) -> Result<(), StorageError>
    where
        S: Storage,
    {
        let mut preconditions = Vec::with_capacity(1 + self.selector_expectations.len());
        preconditions.push(Precondition::KeyValueEquals {
            space: SELECTOR_SPACE,
            key: Key(global_selector_key()),
            expected: self.expected_global.clone(),
        });
        for (key, expected) in &self.selector_expectations {
            preconditions.push(match expected {
                SelectorExpectation::Absent => Precondition::KeyAbsent {
                    space: SELECTOR_SPACE,
                    key: Key(key.clone()),
                },
                SelectorExpectation::Equals(expected) => Precondition::KeyValueEquals {
                    space: SELECTOR_SPACE,
                    key: Key(key.clone()),
                    expected: expected.clone(),
                },
            });
        }
        let next_global = self.next_global.encode()?;
        let capacity = self
            .object_puts
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>()
            .saturating_add(
                self.selector_puts
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>(),
            )
            .saturating_add(next_global.len());
        let mut write = storage
            .begin_write(WriteOptions {
                preconditions,
                batch_capacity_hint_bytes: capacity,
                ..WriteOptions::default()
            })
            .await?;
        if !self.object_puts.is_empty() {
            write
                .put_many(
                    OBJECT_SPACE,
                    PutBatch {
                        entries: self
                            .object_puts
                            .iter()
                            .map(|(id, bytes)| PutEntry {
                                key: Key(Bytes::copy_from_slice(id.as_bytes())),
                                value: StoredValue {
                                    bytes: bytes.clone(),
                                },
                            })
                            .collect(),
                    },
                )
                .await?;
        }
        let mut selector_entries = Vec::with_capacity(self.selector_puts.len() + 1);
        selector_entries.push(PutEntry {
            key: Key(global_selector_key()),
            value: StoredValue { bytes: next_global },
        });
        selector_entries.extend(self.selector_puts.into_iter().map(|(key, value)| PutEntry {
            key: Key(key),
            value: StoredValue { bytes: value },
        }));
        write
            .put_many(
                SELECTOR_SPACE,
                PutBatch {
                    entries: selector_entries,
                },
            )
            .await?;
        if !self.selector_deletes.is_empty() {
            let keys = self
                .selector_deletes
                .into_iter()
                .map(Key)
                .collect::<Vec<_>>();
            write.delete_many(SELECTOR_SPACE, &keys).await?;
        }
        if !self.object_deletes.is_empty() {
            let keys = self
                .object_deletes
                .into_iter()
                .map(|id| Key(Bytes::copy_from_slice(id.as_bytes())))
                .collect::<Vec<_>>();
            write.delete_many(OBJECT_SPACE, &keys).await?;
        }
        write.commit().await?;
        Ok(())
    }
}
