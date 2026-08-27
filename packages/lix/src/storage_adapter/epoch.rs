use bytes::Bytes;

use crate::storage::{
    CommitResult, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, Precondition,
    ProjectedValue, PutBatch, SpaceId, StorageError, StorageRead, StorageSpace, StorageWrite,
    WriteOptions,
};

/// The stable, unscoped control space whose active-pointer value selects the
/// physical repository epoch. Every ordinary repository space is routed into
/// the selected bank; this one must remain reachable before a bank is known.
pub(crate) const REPOSITORY_EPOCH_SPACE: StorageSpace = StorageSpace::declare(
    SpaceId(0x0009_0001),
    "repository.epoch.v1",
    crate::storage::ValueSemantics::Mutable,
);
pub(crate) const REPOSITORY_EPOCH_KEY: &[u8] = b"active";

const BANK_MASK: u32 = 0xc000_0000;
const BANK_A_PREFIX: u32 = 0x4000_0000;
const BANK_B_PREFIX: u32 = 0x8000_0000;

/// Physical bank selected for engine-declared repository spaces.
///
/// `Legacy` preserves the pre-epoch physical layout. Banks A and B occupy
/// disjoint `SpaceId` ranges in the same supplied [`crate::storage::Storage`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum EpochBank {
    #[default]
    Legacy,
    A,
    B,
}

impl EpochBank {
    const fn prefix(self) -> u32 {
        match self {
            Self::Legacy => 0,
            Self::A => BANK_A_PREFIX,
            Self::B => BANK_B_PREFIX,
        }
    }

    pub(crate) const fn alternate(self) -> Self {
        match self {
            Self::Legacy | Self::B => Self::A,
            Self::A => Self::B,
        }
    }
}

/// Immutable routing state cloned into every read and write scope belonging to
/// one engine. The exact pointer bytes double as the stale-writer fence.
#[derive(Clone, Debug, Default)]
pub(super) struct EpochRouting {
    bank: EpochBank,
    expected_pointer: Option<Bytes>,
    force_durable: bool,
}

impl EpochRouting {
    pub(super) fn legacy() -> Self {
        Self::default()
    }

    pub(super) fn unfenced(bank: EpochBank) -> Self {
        Self {
            bank,
            expected_pointer: None,
            force_durable: true,
        }
    }

    pub(super) fn fenced(bank: EpochBank, expected_pointer: Bytes) -> Self {
        Self {
            bank,
            expected_pointer: Some(expected_pointer),
            force_durable: false,
        }
    }

    pub(super) fn migration(bank: EpochBank, expected_pointer: Bytes) -> Self {
        Self {
            bank,
            expected_pointer: Some(expected_pointer),
            force_durable: true,
        }
    }

    pub(super) fn bank(&self) -> EpochBank {
        self.bank
    }

    pub(super) fn map_space(&self, space: StorageSpace) -> StorageSpace {
        if self.bank == EpochBank::Legacy || space.id == REPOSITORY_EPOCH_SPACE.id {
            return space;
        }
        assert_eq!(
            space.id.0 & BANK_MASK,
            0,
            "logical storage space ids must leave the epoch-bank bits clear"
        );
        StorageSpace {
            id: SpaceId(self.bank.prefix() | space.id.0),
            ..space
        }
    }

    pub(super) async fn validate_read<R>(&self, read: &R) -> Result<(), StorageError>
    where
        R: StorageRead + ?Sized,
    {
        let Some(expected) = &self.expected_pointer else {
            return Ok(());
        };
        let keys = [Key(Bytes::from_static(REPOSITORY_EPOCH_KEY))];
        let values = read
            .get_many(&[GetManyRequest {
                space: REPOSITORY_EPOCH_SPACE,
                keys: &keys,
                opts: GetOptions {
                    projection: CoreProjection::FullValue,
                },
            }])
            .await?;
        match values.values.into_iter().next().flatten() {
            Some(ProjectedValue::FullValue(actual)) if actual == *expected => Ok(()),
            _ => Err(StorageError::Fenced),
        }
    }

    pub(super) fn route_write_options(
        &self,
        mut options: WriteOptions,
    ) -> Result<(WriteOptions, Option<usize>), StorageError> {
        if self.force_durable {
            options.await_durable = true;
        }
        options.preconditions = options
            .preconditions
            .into_iter()
            .map(|precondition| self.map_precondition(precondition))
            .collect::<Result<Vec<_>, _>>()?;
        let fence_precondition_index = self
            .expected_pointer
            .as_ref()
            .map(|_| options.preconditions.len());
        if let Some(expected) = &self.expected_pointer {
            options.preconditions.push(Precondition::KeyValueEquals {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                expected: expected.clone(),
            });
        }
        Ok((options, fence_precondition_index))
    }

    fn map_precondition(&self, precondition: Precondition) -> Result<Precondition, StorageError> {
        Ok(match precondition {
            Precondition::KeyAbsent { space, key } => Precondition::KeyAbsent {
                space: self.map_space(space),
                key,
            },
            Precondition::KeyPresent { space, key } => Precondition::KeyPresent {
                space: self.map_space(space),
                key,
            },
            Precondition::KeyValueHashEquals { space, key, hash } => {
                Precondition::KeyValueHashEquals {
                    space: self.map_space(space),
                    key,
                    hash,
                }
            }
            Precondition::KeyValueEquals {
                space,
                key,
                expected,
            } => Precondition::KeyValueEquals {
                space: self.map_space(space),
                key,
                expected,
            },
            Precondition::RangeEmpty { space, range } => Precondition::RangeEmpty {
                space: self.map_space(space),
                range,
            },
            // This backend-specialized precondition contains an already
            // physical key but no StorageSpace, so routing it would silently
            // target the legacy mutable range. No engine path uses it.
            Precondition::BranchEquals { .. } if self.bank != EpochBank::Legacy => {
                return Err(StorageError::Corruption(
                    "BranchEquals cannot be used through epoch-routed storage".to_string(),
                ));
            }
            precondition @ Precondition::BranchEquals { .. } => precondition,
        })
    }

    pub(super) fn mix_snapshot_cache_key(&self, key: Option<u128>) -> Option<u128> {
        key.map(|key| {
            let bank = u128::from(self.bank.prefix());
            let pointer = self.expected_pointer.as_ref().map_or(0, |pointer| {
                let hash = blake3::hash(pointer);
                let mut prefix = [0_u8; 16];
                prefix.copy_from_slice(&hash.as_bytes()[..16]);
                u128::from_le_bytes(prefix)
            });
            key ^ (bank << 64) ^ pointer
        })
    }
}

/// Routes one raw backend write into an epoch bank without changing the
/// backend contract or the engine's logical write set.
pub(crate) struct EpochStorageWrite<W> {
    inner: W,
    routing: EpochRouting,
    fence_precondition_index: Option<usize>,
}

impl<W> EpochStorageWrite<W> {
    pub(super) fn new(
        inner: W,
        routing: EpochRouting,
        fence_precondition_index: Option<usize>,
    ) -> Self {
        Self {
            inner,
            routing,
            fence_precondition_index,
        }
    }
}

impl<W> StorageWrite for EpochStorageWrite<W>
where
    W: StorageWrite,
{
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.put_many(self.routing.map_space(space), entries)
    }

    fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner
            .replace_many(self.routing.map_space(space), entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.delete_many(self.routing.map_space(space), keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner
            .delete_range(self.routing.map_space(space), range)
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            match self.inner.commit().await {
                Err(StorageError::PreconditionFailed(failures))
                    if self.fence_precondition_index.is_some_and(|fence_index| {
                        failures.iter().any(|failure| failure.index == fence_index)
                    }) =>
                {
                    Err(StorageError::Fenced)
                }
                result => result,
            }
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.rollback()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Memory, StorageSpaceRole, ValueIntegrity, ValueSemantics};
    use crate::storage_adapter::{PointReadPlan, StorageAdapter, StorageGetOptions, StorageKey};

    fn logical_space(id: u32) -> StorageSpace {
        StorageSpace {
            id: SpaceId(id),
            name: "test.logical",
            value_semantics: ValueSemantics::Mutable,
            value_integrity: ValueIntegrity::BackendVerified,
            role: StorageSpaceRole::Authoritative,
        }
    }

    #[test]
    fn banks_are_disjoint_and_reversible_by_mask() {
        let logical = logical_space(0x0004_0011);
        let a = EpochRouting::unfenced(EpochBank::A).map_space(logical);
        let b = EpochRouting::unfenced(EpochBank::B).map_space(logical);
        assert_eq!(a.id, SpaceId(0x4004_0011));
        assert_eq!(b.id, SpaceId(0x8004_0011));
        assert_ne!(a.id, b.id);
        assert_eq!(a.id.0 & !BANK_MASK, logical.id.0);
        assert_eq!(b.id.0 & !BANK_MASK, logical.id.0);
    }

    #[test]
    fn epoch_pointer_space_is_never_banked() {
        for bank in [EpochBank::Legacy, EpochBank::A, EpochBank::B] {
            assert_eq!(
                EpochRouting::unfenced(bank).map_space(REPOSITORY_EPOCH_SPACE),
                REPOSITORY_EPOCH_SPACE
            );
        }
    }

    #[test]
    fn repository_protocol_space_is_banked() {
        let logical = crate::init::REPOSITORY_PROTOCOL_SPACE;
        assert_eq!(
            EpochRouting::unfenced(EpochBank::Legacy)
                .map_space(logical)
                .id,
            logical.id
        );
        assert_eq!(
            EpochRouting::unfenced(EpochBank::A).map_space(logical).id,
            SpaceId(BANK_A_PREFIX | logical.id.0)
        );
    }

    #[test]
    fn exact_pointer_fence_is_unscoped() {
        let expected = Bytes::from_static(b"active-v76-a");
        let (options, fence_precondition_index) =
            EpochRouting::fenced(EpochBank::A, expected.clone())
                .route_write_options(WriteOptions::default())
                .unwrap();
        assert_eq!(fence_precondition_index, Some(0));
        assert_eq!(
            options.preconditions,
            vec![Precondition::KeyValueEquals {
                space: REPOSITORY_EPOCH_SPACE,
                key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                expected,
            }]
        );
    }

    async fn load_value(adapter: &StorageAdapter<Memory>, space: StorageSpace) -> Option<Bytes> {
        let read = adapter
            .begin_read(crate::storage::ReadOptions::default())
            .await
            .unwrap();
        PointReadPlan::new(space, &[StorageKey(Bytes::from_static(b"key"))])
            .materialize(&read, StorageGetOptions::default())
            .await
            .unwrap()
            .value
            .into_iter()
            .next()
            .flatten()
            .and_then(|value| match value {
                ProjectedValue::FullValue(value) => Some(value),
                ProjectedValue::KeyOnly => None,
            })
    }

    #[tokio::test]
    async fn adapters_isolate_the_same_logical_key_between_banks() {
        let memory = Memory::new();
        let logical = logical_space(0x0004_0100);
        let a = StorageAdapter::for_epoch_unfenced(memory.clone(), EpochBank::A);
        let b = StorageAdapter::for_epoch_unfenced(memory, EpochBank::B);

        let mut writes = a.new_write_set();
        writes.put(logical, &b"key"[..], &b"from-a"[..]);
        a.commit_write_set(writes, WriteOptions::default())
            .await
            .unwrap();
        let mut writes = b.new_write_set();
        writes.put(logical, &b"key"[..], &b"from-b"[..]);
        b.commit_write_set(writes, WriteOptions::default())
            .await
            .unwrap();

        assert_eq!(load_value(&a, logical).await.unwrap().as_ref(), b"from-a");
        assert_eq!(load_value(&b, logical).await.unwrap().as_ref(), b"from-b");
    }

    #[tokio::test]
    async fn prepared_epoch_write_is_fenced_when_active_pointer_changes() {
        let memory = Memory::new();
        let control = StorageAdapter::new(memory.clone());
        let expected = Bytes::from_static(b"active-a-generation-1");
        let mut seed = control.new_write_set();
        seed.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            expected.as_ref(),
        );
        control
            .commit_write_set(seed, WriteOptions::default())
            .await
            .unwrap();

        let stale = StorageAdapter::for_epoch(memory.clone(), EpochBank::A, expected);
        let mut writes = stale.new_write_set();
        writes.put(logical_space(0x0004_0100), &b"key"[..], &b"late"[..]);
        let prepared = stale
            .prepare_write_set(writes, WriteOptions::default())
            .await
            .unwrap();

        let mut advance = control.new_write_set();
        advance.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            &b"migrating-to-b"[..],
        );
        control
            .commit_write_set(advance, WriteOptions::default())
            .await
            .unwrap();

        let error = prepared
            .commit()
            .await
            .expect_err("stale epoch writer must be fenced");
        assert_eq!(error, StorageError::Fenced);
    }

    #[tokio::test]
    async fn caller_precondition_failure_is_not_misclassified_as_fencing() {
        let memory = Memory::new();
        let control = StorageAdapter::new(memory.clone());
        let expected = Bytes::from_static(b"active-a-generation-1");
        let mut seed = control.new_write_set();
        seed.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            expected.as_ref(),
        );
        control
            .commit_write_set(seed, WriteOptions::default())
            .await
            .unwrap();

        let logical = logical_space(0x0004_0100);
        let adapter = StorageAdapter::for_epoch(memory, EpochBank::A, expected);
        let mut seed = adapter.new_write_set();
        seed.put(logical, &b"occupied"[..], &b"value"[..]);
        adapter
            .commit_write_set(seed, WriteOptions::default())
            .await
            .unwrap();

        let mut writes = adapter.new_write_set();
        writes.put(logical, &b"other"[..], &b"value"[..]);
        let error = adapter
            .commit_write_set(
                writes,
                WriteOptions {
                    preconditions: vec![Precondition::KeyAbsent {
                        space: logical,
                        key: Key(Bytes::from_static(b"occupied")),
                    }],
                    ..WriteOptions::default()
                },
            )
            .await
            .expect_err("caller precondition should fail");
        assert_eq!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(vec![crate::storage::PreconditionFailure {
                    index: 0,
                }])
            )
        );
    }
}
