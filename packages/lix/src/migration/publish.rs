use std::ops::Bound;

use bytes::Bytes;

use crate::LixError;
use crate::init::{REPOSITORY_PROTOCOL_KEY, REPOSITORY_PROTOCOL_SPACE, REPOSITORY_PROTOCOL_VALUE};
use crate::storage_adapter::{
    PutBatch, PutEntry, Storage, StorageError, StorageKey as Key, StorageKeyRange as KeyRange,
    StoragePrecondition as Precondition, StorageSpace, StorageValue as StoredValue, StorageWrite,
    StorageWriteOptions as WriteOptions, ValueIntegrity, ValueSemantics,
};

/// Fully preflighted physical mutations. Construction stays private so the
/// executor cannot publish a partially validated migration.
#[derive(Debug)]
pub(super) struct PublicationPlan {
    replacements: Vec<(StorageSpace, PutBatch)>,
    mutable_puts: Vec<(StorageSpace, PutBatch)>,
    cleared_spaces: Vec<StorageSpace>,
    max_entries: usize,
    max_bytes: usize,
    entries: usize,
    bytes: usize,
}

impl PublicationPlan {
    pub(super) fn bounded(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            replacements: Vec::new(),
            mutable_puts: Vec::new(),
            cleared_spaces: Vec::new(),
            max_entries,
            max_bytes,
            entries: 0,
            bytes: 0,
        }
    }

    pub(super) fn add_builder_batch(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), LixError> {
        self.account(&entries)?;
        if space.value_semantics == ValueSemantics::Immutable
            && space.value_integrity != ValueIntegrity::ContentAddressed
        {
            self.replacements.push((space, entries));
        } else {
            self.mutable_puts.push((space, entries));
        }
        Ok(())
    }

    pub(super) fn replace_immutable(
        &mut self,
        space: StorageSpace,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), LixError> {
        if space.value_semantics != ValueSemantics::Immutable {
            return Err(plan_error("immutable replacement targeted a mutable space"));
        }
        let batch = put_batch(entries)?;
        self.account(&batch)?;
        self.replacements.push((space, batch));
        Ok(())
    }

    pub(super) fn put_mutable(
        &mut self,
        space: StorageSpace,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), LixError> {
        if space.value_semantics != ValueSemantics::Mutable {
            return Err(plan_error(
                "mutable migration put targeted an immutable space",
            ));
        }
        let batch = put_batch(entries)?;
        self.account(&batch)?;
        self.mutable_puts.push((space, batch));
        Ok(())
    }

    pub(super) fn clear_space(&mut self, space: StorageSpace) {
        if !self.cleared_spaces.contains(&space) {
            self.cleared_spaces.push(space);
        }
    }

    fn account(&mut self, batch: &PutBatch) -> Result<(), LixError> {
        let entries = batch.entries.len();
        let bytes = batch
            .entries
            .iter()
            .try_fold(0usize, |total, entry| {
                total
                    .checked_add(entry.key.0.len())?
                    .checked_add(entry.value.bytes.len())
            })
            .ok_or_else(|| plan_error("migration publication size overflows usize"))?;
        self.entries = self
            .entries
            .checked_add(entries)
            .ok_or_else(|| plan_error("migration publication entry count overflows usize"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| plan_error("migration publication bytes overflow usize"))?;
        if self.entries > self.max_entries || self.bytes > self.max_bytes {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                format!(
                    "migration publication exceeds configured bounds: {} entries, {} bytes",
                    self.entries, self.bytes
                ),
            ));
        }
        Ok(())
    }
}

impl Default for PublicationPlan {
    fn default() -> Self {
        Self::bounded(usize::MAX, usize::MAX)
    }
}

/// Publishes one already-complete migration plan in a single durable backend
/// transaction. The marker precondition fences concurrent writers and the
/// marker update shares the same atomic commit as every authority rewrite.
pub(super) async fn publish<S>(
    storage: &S,
    expected_mutation_revision: Option<Bytes>,
    expected_protocol_value: &'static [u8],
    target_protocol_value: &'static [u8],
    plan: PublicationPlan,
) -> Result<(), LixError>
where
    S: Storage,
{
    let marker_batch = put_batch(vec![(
        REPOSITORY_PROTOCOL_KEY.to_vec(),
        target_protocol_value.to_vec(),
    )])?;
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REPOSITORY_PROTOCOL_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                    expected: Bytes::from_static(expected_protocol_value),
                },
                crate::storage_adapter::StorageAdapter::<S>::mutation_revision_precondition(
                    expected_mutation_revision,
                ),
            ],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;

    let stage_result: Result<(), StorageError> = async {
        for space in plan.cleared_spaces {
            write
                .delete_range(
                    space,
                    KeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                )
                .await?;
        }
        for (space, entries) in plan.replacements {
            write.replace_many(space, entries).await?;
        }
        for (space, entries) in plan.mutable_puts {
            write.put_many(space, entries).await?;
        }
        write
            .put_many(REPOSITORY_PROTOCOL_SPACE, marker_batch)
            .await?;
        crate::storage_adapter::stage_mutation_revision(&mut write).await?;
        Ok(())
    }
    .await;
    if let Err(error) = stage_result {
        let mapped = storage_error(error);
        let _ = write.rollback().await;
        return Err(mapped);
    }
    write.commit().await.map_err(storage_error)?;
    Ok(())
}

fn put_batch(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<PutBatch, LixError> {
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(plan_error(
            "migration plan contains a duplicate physical key",
        ));
    }
    Ok(PutBatch {
        entries: entries
            .into_iter()
            .map(|(key, value)| PutEntry {
                key: Key(Bytes::from(key)),
                value: StoredValue {
                    bytes: Bytes::from(value),
                },
            })
            .collect(),
    })
}

fn plan_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

fn storage_error(error: StorageError) -> LixError {
    let code = match &error {
        StorageError::CommitOutcomeUnknown(_) => "LIX_ERROR_MIGRATION_COMMIT_OUTCOME_UNKNOWN",
        StorageError::PreconditionFailed(_)
        | StorageError::WriteConflict
        | StorageError::Fenced => "LIX_ERROR_MIGRATION_CONCURRENT_MUTATION",
        _ => LixError::CODE_INTERNAL_ERROR,
    };
    LixError::new(code, format!("repository migration storage error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::CURRENT_FORMAT_VERSION;
    use crate::storage::{
        CoreProjection, GetManyRequest, GetOptions, Memory, ProjectedValue, ReadOptions,
        StorageRead,
    };

    const IMMUTABLE: StorageSpace = crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE;

    #[tokio::test]
    async fn replaces_immutable_bytes_and_marker_atomically() {
        let storage = Memory::new();
        let mut seed = storage.begin_write(WriteOptions::default()).await.unwrap();
        seed.put_many(
            IMMUTABLE,
            put_batch(vec![(b"same-key".to_vec(), b"v68".to_vec())]).unwrap(),
        )
        .await
        .unwrap();
        seed.put_many(
            REPOSITORY_PROTOCOL_SPACE,
            put_batch(vec![(
                REPOSITORY_PROTOCOL_KEY.to_vec(),
                b"tracked-default-branch.v68".to_vec(),
            )])
            .unwrap(),
        )
        .await
        .unwrap();
        seed.commit().await.unwrap();

        let mut plan = PublicationPlan::default();
        plan.replace_immutable(IMMUTABLE, vec![(b"same-key".to_vec(), b"v69".to_vec())])
            .unwrap();
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let revision = adapter.load_mutation_revision().await.unwrap();
        publish(
            &storage,
            revision,
            b"tracked-default-branch.v68",
            REPOSITORY_PROTOCOL_VALUE,
            plan,
        )
        .await
        .unwrap();

        let read = storage.begin_read(ReadOptions::default()).await.unwrap();
        let immutable_keys = [Key(Bytes::from_static(b"same-key"))];
        let marker_keys = [Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY))];
        let result = read
            .get_many(&[
                GetManyRequest {
                    space: IMMUTABLE,
                    keys: &immutable_keys,
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                },
                GetManyRequest {
                    space: REPOSITORY_PROTOCOL_SPACE,
                    keys: &marker_keys,
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            result.values,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"v69"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(
                    REPOSITORY_PROTOCOL_VALUE
                ))),
            ]
        );
    }

    #[tokio::test]
    async fn mutation_revision_fences_a_stale_preflight() {
        let storage = Memory::new();
        let mut seed = storage.begin_write(WriteOptions::default()).await.unwrap();
        seed.put_many(
            REPOSITORY_PROTOCOL_SPACE,
            put_batch(vec![(
                REPOSITORY_PROTOCOL_KEY.to_vec(),
                b"tracked-default-branch.v68".to_vec(),
            )])
            .unwrap(),
        )
        .await
        .unwrap();
        seed.commit().await.unwrap();
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let stale_revision = adapter.load_mutation_revision().await.unwrap();

        let mut concurrent = adapter.new_write_set();
        concurrent.put(REPOSITORY_PROTOCOL_SPACE, &b"unrelated"[..], &b"write"[..]);
        adapter
            .commit_write_set(concurrent, WriteOptions::default())
            .await
            .unwrap();

        let error = publish(
            &storage,
            stale_revision,
            b"tracked-default-branch.v68",
            REPOSITORY_PROTOCOL_VALUE,
            PublicationPlan::default(),
        )
        .await
        .expect_err("stale migration must be fenced");
        assert!(error.to_string().contains("precondition failed"));
        assert_eq!(
            crate::migration::inspect_lix(&storage)
                .await
                .unwrap(),
            crate::migration::MigrationStatus::Required {
                from_version: 68,
                to_version: CURRENT_FORMAT_VERSION,
            }
        );
    }
}
