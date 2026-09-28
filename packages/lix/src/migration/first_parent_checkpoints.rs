//! v82 -> v83: rewrite packed commit records with a strict first-parent
//! nearest-checkpoint summary. Missing ancestry remains unavailable.
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::LixError;
use crate::changelog::{
    COMMIT_RECORD_FORMAT_VERSION, CommitId, CommitRecord, FirstParentCheckpointSummary,
};
use crate::storage_adapter::{
    Storage, StorageAdapter, StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection,
    StorageKeyRange, StorageProjectedValue,
};

use super::api::MigrationOptions;
use super::checkpoint_metadata::CommitRecordV7;
use super::publish::{PublicationPlan, publish};

fn failure(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message)
}

fn limit() -> LixError {
    LixError::new(
        "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
        "first-parent checkpoint summary migration exceeds configured bounds",
    )
}

fn decode_record(bytes: &[u8]) -> Result<CommitRecord, LixError> {
    if let Ok(record) = crate::storage_codec::decode::<CommitRecord>("commit record", bytes)
        && record.format_version == COMMIT_RECORD_FORMAT_VERSION
    {
        return Ok(record);
    }
    let old = crate::storage_codec::decode::<CommitRecordV7>("legacy v7 commit record", bytes)
        .map_err(|_| failure("checkpoint summary migration found an invalid v7/v8 record"))?;
    if old.format_version != 7 {
        return Err(failure(
            "checkpoint summary migration found an unexpected record version",
        ));
    }
    Ok(CommitRecord {
        format_version: COMMIT_RECORD_FORMAT_VERSION,
        commit_id: old.commit_id,
        generation: old.generation,
        parent_commit_ids: old.parent_commit_ids,
        base_commit_id: old.base_commit_id,
        first_parent_jump_commit_id: old.first_parent_jump_commit_id,
        first_parent_jump_span: old.first_parent_jump_span,
        account_id: old.account_id,
        created_at: old.created_at,
        touched_scope_digest: old.touched_scope_digest,
        is_checkpoint: old.is_checkpoint,
        first_parent_checkpoint_summary: None,
    })
}

pub(super) async fn append_plan<R: StorageAdapterRead + ?Sized>(
    read: &R,
    options: MigrationOptions,
    plan: &mut PublicationPlan,
) -> Result<(), LixError> {
    let mut scan = read
        .begin_scan(
            crate::changelog::COMMIT_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..Default::default()
            },
        )
        .await?;
    let mut records = BTreeMap::<CommitId, (Vec<u8>, CommitRecord)>::new();
    let mut scanned_bytes = 0usize;
    while let Some(entries) = scan.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(failure("checkpoint summary commit scan omitted a value"));
            };
            scanned_bytes = scanned_bytes.checked_add(bytes.len()).ok_or_else(limit)?;
            if records.len() >= options.max_changes || scanned_bytes > options.max_preflight_bytes {
                return Err(limit());
            }
            let record = decode_record(&bytes)?;
            if entry.key.0.as_ref() != crate::changelog::commit_key(record.commit_id).as_slice() {
                return Err(failure(format!(
                    "checkpoint summary record key does not match commit '{}'",
                    record.commit_id
                )));
            }
            if records
                .insert(record.commit_id, (entry.key.0.to_vec(), record.clone()))
                .is_some()
            {
                return Err(failure(format!(
                    "checkpoint summary migration found duplicate commit '{}'",
                    record.commit_id
                )));
            }
        }
    }
    let mut ordered = records
        .values()
        .map(|(_, record)| (record.generation, record.commit_id))
        .collect::<Vec<_>>();
    ordered.sort_unstable();
    let mut summaries = BTreeMap::<CommitId, Option<FirstParentCheckpointSummary>>::new();
    for (_, commit_id) in ordered {
        let record = &records[&commit_id].1;
        let parent_id = record.parent_commit_ids.first().copied();
        let parent_record = parent_id.and_then(|parent| records.get(&parent).map(|(_, r)| r));
        if let (Some(parent_id), Some(parent)) = (parent_id, parent_record)
            && parent.generation >= record.generation
        {
            return Err(failure(format!(
                "checkpoint summary migration found non-advancing first parent '{parent_id}' for '{commit_id}'"
            )));
        }
        let derived = if let Some(parent_id) = parent_id {
            match parent_record {
                None => None,
                Some(parent) if parent.is_checkpoint => Some(FirstParentCheckpointSummary {
                    previous_checkpoint_id: Some(parent_id),
                    first_parent_distance: 1,
                }),
                Some(_) => {
                    if let Some(summary) = summaries.get(&parent_id).copied().flatten() {
                        let first_parent_distance = if summary.previous_checkpoint_id.is_some() {
                            summary
                                .first_parent_distance
                                .checked_add(1)
                                .ok_or_else(|| failure("checkpoint distance exceeds u64"))?
                        } else {
                            0
                        };
                        Some(FirstParentCheckpointSummary {
                            previous_checkpoint_id: summary.previous_checkpoint_id,
                            first_parent_distance,
                        })
                    } else {
                        None
                    }
                }
            }
        } else {
            Some(FirstParentCheckpointSummary {
                previous_checkpoint_id: None,
                first_parent_distance: 0,
            })
        };
        let derived = match derived.and_then(|summary| {
            summary
                .previous_checkpoint_id
                .map_or(Some(summary), |target| {
                    records.get(&target).map(|_| summary)
                })
        }) {
            Some(summary) => {
                if let Some(target_id) = summary.previous_checkpoint_id
                    && !records[&target_id].1.is_checkpoint
                {
                    return Err(failure(format!(
                        "checkpoint summary target '{target_id}' is not a checkpoint"
                    )));
                }
                Some(summary)
            }
            None => None,
        };
        summaries.insert(commit_id, derived);
    }
    let mut rewritten = Vec::with_capacity(records.len());
    for (key, mut record) in records.into_values() {
        record.first_parent_checkpoint_summary = summaries[&record.commit_id];
        rewritten.push((key, crate::storage_codec::encode("commit record", &record)?));
    }
    plan.put_mutable(crate::changelog::COMMIT_SPACE, rewritten)
}

async fn rewrite<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    partial: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let marker = super::api::load_repository_protocol_marker(adapter)
        .await?
        .ok_or_else(|| failure("checkpoint summary migration has no repository marker"))?;
    let (source_marker, target_marker) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V82,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V83,
        )
    } else {
        (
            crate::init::REPOSITORY_PROTOCOL_V82,
            crate::init::REPOSITORY_PROTOCOL_V83,
        )
    };
    if marker.as_ref() == target_marker {
        read.finish()?;
        return Ok(());
    }
    if marker.as_ref() != source_marker {
        return Err(failure(
            "checkpoint summary migration observed an unexpected protocol marker",
        ));
    }
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    let mut plan = PublicationPlan::bounded(options.max_changes, options.max_preflight_bytes);
    append_plan(&read, options, &mut plan).await?;
    read.finish()?;
    publish(adapter, revision, source_marker, target_marker, plan).await
}

pub(super) async fn migrate<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    partial: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    rewrite(adapter, options, partial).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangelogReader as _;
    use crate::common::LixTimestamp;
    use crate::storage::Memory;
    use crate::storage_adapter::{
        PutBatch, PutEntry, StorageKey, StorageReadOptions, StorageValue, StorageWrite as _,
        StorageWriteOptions,
    };

    fn id(name: &str) -> CommitId {
        CommitId::for_test_label(name)
    }

    fn v7(
        commit_id: CommitId,
        generation: u64,
        parent_commit_ids: Vec<CommitId>,
        is_checkpoint: bool,
    ) -> CommitRecordV7 {
        let (first_parent_jump_commit_id, first_parent_jump_span) =
            match parent_commit_ids.as_slice() {
                [parent] => (*parent, 1),
                _ => (commit_id, 0),
            };
        CommitRecordV7 {
            format_version: 7,
            commit_id,
            generation,
            parent_commit_ids,
            base_commit_id: None,
            first_parent_jump_commit_id,
            first_parent_jump_span,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            created_at: LixTimestamp::expect_parse(
                "checkpoint summary migration timestamp",
                "2026-09-01T00:00:00Z",
            ),
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            is_checkpoint,
        }
    }

    #[tokio::test]
    async fn v82_full_and_partial_migrations_rewrite_real_v7_commit_records() {
        for partial in [false, true] {
            migrate_v7_checkpoint_fixture(partial).await;
        }
    }

    async fn migrate_v7_checkpoint_fixture(partial: bool) {
        let storage = Memory::new();
        let adapter = StorageAdapter::new(storage.clone());
        let root = v7(id("summary-root"), 0, Vec::new(), false);
        let first_checkpoint = v7(
            id("summary-first-checkpoint"),
            1,
            vec![root.commit_id],
            true,
        );
        let main = v7(
            id("summary-main"),
            2,
            vec![first_checkpoint.commit_id],
            false,
        );
        let side = v7(id("summary-side-root"), 40, vec![root.commit_id], false);
        let secondary_checkpoint = v7(
            id("summary-secondary-checkpoint"),
            41,
            vec![side.commit_id],
            true,
        );
        let merge = v7(
            id("summary-merge"),
            42,
            vec![main.commit_id, secondary_checkpoint.commit_id],
            false,
        );
        let missing_parent = id("summary-missing-parent");
        let sparse = v7(
            id("summary-sparse-boundary"),
            100,
            vec![missing_parent],
            false,
        );
        let first_checkpoint_id = first_checkpoint.commit_id;
        let main_id = main.commit_id;
        let merge_id = merge.commit_id;
        let sparse_id = sparse.commit_id;
        let legacy = [
            root,
            first_checkpoint,
            main,
            side,
            secondary_checkpoint,
            merge,
            sparse,
        ];
        let mut values = legacy
            .iter()
            .map(|record| {
                Ok((
                    crate::changelog::commit_key(record.commit_id),
                    crate::storage_codec::encode("v7 test commit", record)?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()
            .expect("v7 records encode");
        values.push((
            crate::init::REPOSITORY_PROTOCOL_KEY.to_vec(),
            if partial {
                crate::init::PARTIAL_REPOSITORY_PROTOCOL_V82.to_vec()
            } else {
                crate::init::REPOSITORY_PROTOCOL_V82.to_vec()
            },
        ));
        let entries: Vec<PutEntry> = values
            .into_iter()
            .map(|(key, value)| {
                if key == crate::init::REPOSITORY_PROTOCOL_KEY {
                    PutEntry {
                        key: StorageKey(key.into()),
                        value: StorageValue {
                            bytes: value.into(),
                        },
                    }
                } else {
                    PutEntry {
                        key: StorageKey(key.into()),
                        value: StorageValue {
                            bytes: value.into(),
                        },
                    }
                }
            })
            .collect();
        let mut write = adapter
            .begin_migration_write(StorageWriteOptions::default())
            .await
            .expect("seed write should open");
        write
            .put_many(
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                PutBatch {
                    entries: vec![
                        entries
                            .iter()
                            .find(|entry| {
                                entry.key.0.as_ref() == crate::init::REPOSITORY_PROTOCOL_KEY
                            })
                            .expect("protocol marker entry")
                            .clone(),
                    ],
                },
            )
            .await
            .expect("protocol marker should seed");
        write
            .put_many(
                crate::changelog::COMMIT_SPACE,
                PutBatch {
                    entries: entries
                        .into_iter()
                        .filter(|entry| {
                            entry.key.0.as_ref() != crate::init::REPOSITORY_PROTOCOL_KEY
                        })
                        .collect(),
                },
            )
            .await
            .expect("legacy commits should seed");
        write.commit().await.expect("seed should commit");

        migrate(&adapter, MigrationOptions::default(), partial)
            .await
            .expect("v7 records should migrate atomically");
        let expected_marker = if partial {
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V83
        } else {
            crate::init::REPOSITORY_PROTOCOL_V83
        };
        let marker = crate::migration::api::load_repository_protocol_marker(&adapter)
            .await
            .expect("migrated protocol marker should read")
            .expect("migrated protocol marker should exist");
        assert_eq!(marker.as_ref(), expected_marker);
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let ids = legacy
            .iter()
            .map(|record| record.commit_id)
            .collect::<Vec<_>>();
        let mut changelog = crate::changelog::ChangelogContext::new().reader(&read);
        let migrated = changelog
            .load_commits(crate::changelog::CommitLoadRequest { commit_ids: &ids })
            .await
            .expect("v8 records should decode");
        let migrated = migrated
            .into_iter()
            .map(|(id, record)| (id, record.expect("record remains present")))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            migrated[&main_id].first_parent_checkpoint_summary,
            Some(FirstParentCheckpointSummary {
                previous_checkpoint_id: Some(first_checkpoint_id),
                first_parent_distance: 1,
            })
        );
        assert_eq!(
            migrated[&merge_id].first_parent_checkpoint_summary,
            Some(FirstParentCheckpointSummary {
                previous_checkpoint_id: Some(first_checkpoint_id),
                first_parent_distance: 2,
            }),
            "the merge's deep secondary checkpoint must not enter its summary"
        );
        assert_eq!(
            migrated[&sparse_id].first_parent_checkpoint_summary, None,
            "a missing sparse parent must remain unavailable"
        );
        drop(changelog);
        drop(read);
    }
}
