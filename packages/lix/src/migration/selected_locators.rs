use super::api::MigrationOptions;
use super::publish::PublicationPlan;
use crate::LixError;
use crate::changelog::{ChangeId, ChangeLoadRequest, ChangelogContext, ChangelogReader};
use crate::storage_adapter::{StorageAdapterRead, StoragePrefix, StorageProjectedValue};

pub(super) async fn stage_repair(
    read: &(impl StorageAdapterRead + ?Sized),
    plan: &mut PublicationPlan,
    options: &MigrationOptions,
    entries: &mut usize,
    bytes: &mut usize,
) -> Result<(), LixError> {
    let bounded = super::bounded_read::BoundedRead::new(
        read,
        options.max_changes.saturating_sub(*entries),
        options.max_preflight_bytes.saturating_sub(*bytes),
    );
    let result = collect_repair(read, &bounded).await;
    let (used_entries, used_bytes) = bounded.usage()?;
    *entries = entries.saturating_add(used_entries);
    *bytes = bytes.saturating_add(used_bytes);
    if let Some(retained) = result? {
        plan.replace_mutable_space(
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            retained,
        )?;
    }
    Ok(())
}

async fn collect_repair<R: StorageAdapterRead + ?Sized>(
    read: &R,
    bounded: &super::bounded_read::BoundedRead<'_, R>,
) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, LixError> {
    let mut cursor = read
        .begin_scan(
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            StoragePrefix {
                bytes: bytes::Bytes::new(),
            }
            .to_range()?,
            Default::default(),
        )
        .await?;
    let mut retained = Vec::new();
    let mut candidates = Vec::new();
    let mut candidate_entries = Vec::new();
    while let Some(page) = cursor.next_chunk().await? {
        let mut page_locators = Vec::with_capacity(page.len());
        let mut ids = Vec::with_capacity(page.len());
        for entry in page {
            let StorageProjectedValue::FullValue(raw) = entry.value else {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_FAILED",
                    "locator scan omitted a value",
                ));
            };
            bounded.charge(1, raw.len() + entry.key.0.len())?;
            let id = ChangeId::new(uuid::Uuid::from_slice(&entry.key.0).map_err(|_| {
                LixError::new(
                    "LIX_ERROR_MIGRATION_FAILED",
                    "change locator key is not a UUID",
                )
            })?);
            let locator = crate::tracked_state::decode_change_locator(id, &raw)?;
            ids.push(id);
            page_locators.push((locator, (entry.key.0.to_vec(), raw.to_vec())));
        }
        let canonical = ChangelogContext::new()
            .reader(bounded)
            .load_changes(ChangeLoadRequest { change_ids: &ids })
            .await?;
        if canonical.len() != page_locators.len() {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_FAILED",
                "canonical change count mismatch",
            ));
        }
        for ((locator, encoded), (id, canonical)) in page_locators.into_iter().zip(canonical) {
            if locator.change_id != *id {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_FAILED",
                    "canonical change identity mismatch",
                ));
            }
            if let Some(canonical) = canonical {
                if canonical.change_id != *id {
                    return Err(LixError::new(
                        "LIX_ERROR_MIGRATION_FAILED",
                        "canonical record identity mismatch",
                    ));
                }
                candidates.push((
                    locator,
                    crate::tracked_state::TrackedStateKey {
                        schema_key: canonical.schema_key,
                        file_id: canonical.file_id,
                        row_pk: canonical.row_pk,
                    },
                ));
                candidate_entries.push(encoded);
            } else {
                retained.push(encoded);
            }
        }
    }
    // Group across the entire scan, not per page: an owner inventory is read once.
    let selected =
        crate::tracked_state::change_locators_select_references_for_migration(bounded, &candidates)
            .await?;
    if selected.len() != candidate_entries.len() {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "locator classification count mismatch",
        ));
    }
    let repaired = selected.iter().any(|selected| *selected);
    retained.extend(
        candidate_entries
            .into_iter()
            .zip(selected)
            .filter_map(|(entry, selected)| (!selected).then_some(entry)),
    );
    Ok(repaired.then_some(retained))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangelogAppend;
    use crate::storage_adapter::{Memory, StorageAdapter};

    #[tokio::test]
    async fn upgrade_retires_selected_locator_without_changing_canonical_payload() {
        exercise_repair(false).await;
    }

    #[tokio::test]
    async fn malformed_canonical_payload_does_not_authorize_locator_retirement() {
        exercise_repair(true).await;
    }

    async fn exercise_repair(corrupt: bool) {
        let adapter = StorageAdapter::new(Memory::new());
        crate::engine::Engine::initialize_with_adapter(adapter.clone(), None)
            .await
            .unwrap();
        let engine = crate::engine::Engine::new_with_adapter(
            adapter.clone(),
            crate::engine::EngineOptions::new(),
        )
        .await
        .unwrap();
        let session = engine.open_session().await.unwrap();
        session
            .execute(
                &format!(
                    "INSERT INTO lix_key_value (key,value) VALUES ('locator-migration','{}')",
                    "x".repeat(64 * 1024)
                ),
                &[],
            )
            .await
            .unwrap();
        let checkpoint = session.execute("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))", &[]).await.unwrap().rows()[0].get::<String>("commit_id").unwrap();
        drop(session);
        drop(engine);
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let checkpoint_id =
            crate::changelog::CommitId::parse_lix(&checkpoint, "test checkpoint").unwrap();
        let body = crate::sync::load_sync_commit_for_migration_test(&read, checkpoint_id)
            .await
            .unwrap()
            .unwrap();
        let (ordinal, member) = body
            .members
            .iter()
            .enumerate()
            .find(|(_, member)| !member.authored && member.schema_key == "lix_key_value")
            .unwrap();
        let id = ChangeId::parse_lix(&member.change_id, "test selected change").unwrap();
        let canonical = crate::tracked_state::load_change_record_by_id(&read, id)
            .await
            .unwrap()
            .unwrap();
        let locator = crate::tracked_state::CommitDeltaChangeLocator {
            change_id: id,
            commit_id: crate::changelog::CommitId::parse_lix(&checkpoint, "test checkpoint")
                .unwrap(),
            segment_index: 0,
            ordinal: u16::try_from(ordinal).unwrap(),
        };
        assert!(
            crate::tracked_state::change_locator_selects_reference_for_migration(&read, locator)
                .await
                .unwrap()
        );
        let mut writes = adapter.new_write_set();
        ChangelogContext::new()
            .writer(&mut &read, &mut writes)
            .stage_certified_sparse_append(ChangelogAppend {
                commits: Vec::new(),
                changes: vec![canonical.clone()],
            })
            .await
            .unwrap();
        crate::tracked_state::stage_change_locators(&mut writes, &[locator]);
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            crate::init::REPOSITORY_PROTOCOL_V79,
        );
        drop(read);
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut byte_limit = None;
        if corrupt {
            let corrupted = CorruptCanonical(&read);
            let mut plan = PublicationPlan::bounded(usize::MAX, usize::MAX);
            let error = stage_repair(
                &corrupted,
                &mut plan,
                &MigrationOptions::default(),
                &mut 0,
                &mut 0,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
            assert!(
                error
                    .message
                    .starts_with("failed to decode change record with musli storage:")
            );
        } else {
            // Cover all scanned metadata, but not the large canonical value.
            let mut metadata_bytes = 0;
            for space in [
                crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            ] {
                let mut cursor = read
                    .begin_scan(
                        space,
                        StoragePrefix {
                            bytes: bytes::Bytes::new(),
                        }
                        .to_range()
                        .unwrap(),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                while let Some(page) = cursor.next_chunk().await.unwrap() {
                    for entry in page {
                        let StorageProjectedValue::FullValue(raw) = entry.value else {
                            panic!("missing scan value")
                        };
                        metadata_bytes += entry.key.0.len() + raw.len();
                    }
                }
            }
            byte_limit = Some(metadata_bytes + 1024);
        }
        drop(read);
        if let Some(byte_limit) = byte_limit {
            let error = super::super::incorporation::migrate(
                &adapter,
                MigrationOptions {
                    max_changes: usize::MAX,
                    max_preflight_bytes: byte_limit,
                },
                false,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
        }
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let marker = crate::storage_adapter::PointReadPlan::new(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            &[crate::storage_adapter::StorageKey(
                bytes::Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY),
            )],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten();
        assert_eq!(
            marker,
            Some(StorageProjectedValue::FullValue(bytes::Bytes::from_static(
                crate::init::REPOSITORY_PROTOCOL_V79
            )))
        );
        let mut cursor = read
            .begin_scan(
                crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                StoragePrefix {
                    bytes: bytes::Bytes::copy_from_slice(id.as_uuid().as_bytes()),
                }
                .to_range()
                .unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(cursor.collect_all().await.unwrap().len(), 1);
        drop(cursor);
        drop(read);
        super::super::incorporation::migrate(&adapter, MigrationOptions::default(), false)
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut cursor = read
            .begin_scan(
                crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                StoragePrefix {
                    bytes: bytes::Bytes::copy_from_slice(id.as_uuid().as_bytes()),
                }
                .to_range()
                .unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
        assert!(cursor.collect_all().await.unwrap().is_empty());
        let loaded = ChangelogContext::new()
            .reader(&read)
            .load_changes(ChangeLoadRequest { change_ids: &[id] })
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .1;
        assert_eq!(loaded, Some(canonical));
        assert_eq!(
            crate::sync::load_sync_commit_for_migration_test(&read, checkpoint_id)
                .await
                .unwrap()
                .unwrap(),
            body
        );
    }

    struct CorruptCanonical<'a, R>(&'a R);

    impl<R: StorageAdapterRead> StorageAdapterRead for CorruptCanonical<'_, R> {
        async fn get_many(
            &self,
            requests: &[crate::storage_adapter::StorageGetManyRequest<'_>],
        ) -> Result<
            crate::storage_adapter::StorageGetManyResult,
            crate::storage_adapter::StorageError,
        > {
            let mut result = self.0.get_many(requests).await?;
            let mut index = 0;
            for request in requests {
                for _ in request.keys {
                    if request.space == crate::changelog::CHANGE_SPACE
                        && result.values[index].is_some()
                    {
                        result.values[index] = Some(StorageProjectedValue::FullValue(
                            bytes::Bytes::from_static(b"invalid canonical record"),
                        ));
                    }
                    index += 1;
                }
            }
            Ok(result)
        }

        async fn begin_scan(
            &self,
            space: crate::storage_adapter::StorageSpace,
            range: crate::storage_adapter::StorageKeyRange,
            options: crate::storage_adapter::StorageBeginScanOptions,
        ) -> Result<
            crate::storage_adapter::StorageScanCursor<'_>,
            crate::storage_adapter::StorageError,
        > {
            self.0.begin_scan(space, range, options).await
        }
    }
}
