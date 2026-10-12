//! Browser-shaped partial replicas with resident query indexes and pending edits.
//! Historical protocol/journal metadata is staged explicitly; the native rows
//! come from canonical HTTP hydration and local SQL, not historical codec dumps.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hydrated_browser_partial_indexes_and_pending_rows_survive_v82_to_v87_upgrade() {
    let authority = Authority::new_with_browser_files(true).await;
    for format in 82..=crate::init::CURRENT_FORMAT_VERSION {
        let authenticated = crate::sync::authenticate_partial_conversion(authority.options(), None)
            .await
            .unwrap();
        let state = authenticated.state().clone();
        let storage = crate::storage::StorageSession::acquire(
            crate::sync::durable_memory_for_test(crate::Memory::new()),
        )
        .await
        .unwrap();
        let installed = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        let (engine, session) =
            Engine::new_partial_replica(installed.adapter.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            Arc::new(state.clone()),
            crate::sync::partial_replica_write_capability(),
        );
        installed
            .adapter
            .admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let remote = crate::sync::execute_hydrating_over_http(
            &session,
            &installed.adapter,
            &state,
            authenticated.server(),
            "SELECT value FROM lix_key_value WHERE key = 'upgrade-test'",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(remote.rows().len(), 1);
        crate::sync::execute_hydrating_over_http(
            &session,
            &installed.adapter,
            &state,
            authenticated.server(),
            "INSERT INTO lix_key_value (key, value) VALUES ('browser-pending', 'local-pending')",
            &[],
        )
        .await
        .unwrap();
        crate::sync::execute_hydrating_over_http(
            &session,
            &installed.adapter,
            &state,
            authenticated.server(),
            "SELECT value FROM legacy_custom_note WHERE id = 'legacy'",
            &[],
        )
        .await
        .unwrap();
        crate::sync::execute_hydrating_over_http(
            &session,
            &installed.adapter,
            &state,
            authenticated.server(),
            "INSERT INTO legacy_custom_note(id,value) VALUES('browser-pending-note','indexed-pending')",
            &[],
        ).await.unwrap();
        drop(session);
        drop(engine);
        let read = installed
            .adapter
            .begin_read(ReadOptions::default())
            .await
            .unwrap();
        let indexes = read
            .begin_scan(
                crate::hot_state::INDEX_SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                Default::default(),
            )
            .await
            .unwrap()
            .next_page(512)
            .await
            .unwrap()
            .into_parts()
            .0;
        assert!(
            !indexes.is_empty(),
            "real hydrated and authored SQL must populate resident indexes"
        );
        let serving = crate::sync::partial_serving::preservation_entries(&read)
            .await
            .unwrap();
        assert!(!serving.is_empty());
        drop(read);
        if format < crate::init::CURRENT_FORMAT_VERSION {
            stage_repository_format_for_test(&storage, true, format)
                .await
                .unwrap();
        } else {
            // Current-format browser databases can still require the owned
            // metadata-upgrade path when their read-interest journal is older.
            let journal_key =
                crate::storage_codec::id_string::uuid_bytes_from_canonical(state.epoch_id())
                    .unwrap();
            let mut writes = installed.adapter.new_write_set();
            writes.put(
                crate::sync::PARTIAL_READ_INTEREST_SPACE,
                journal_key.as_slice(),
                serde_json::to_vec(&serde_json::json!({
                    "version": 2, "epochId": state.epoch_id(), "recipes": [{
                        "kind": "filesystem_paths", "file_ids": [],
                        "branch_ids": [state.descriptor().selected_branch.branch_id],
                        "include_blob_refs": true, "cache_small_blob_data": true
                    }]
                }))
                .unwrap(),
            );
            let mut write = installed
                .adapter
                .begin_migration_write(WriteOptions::default())
                .await
                .unwrap();
            writes.lower_into(&mut write).await.unwrap();
            write.commit().await.unwrap();
        }
        drop(installed.adapter);
        let before = super::super::super::public_api::content_digest(&storage)
            .await
            .unwrap();
        let source_pointer = load_pointer(&storage).await.unwrap().unwrap().1;
        let report = match super::super::super::public_api::migrate_owned_with_progress(
            &storage,
            super::super::super::MigrationOptions::default(),
            None,
        )
        .await
        {
            Ok(report) => report,
            Err(error) => {
                assert_eq!(
                    load_pointer(&storage).await.unwrap().unwrap().1,
                    source_pointer,
                    "failed validation must restore the exact source pointer"
                );
                assert_eq!(
                    super::super::super::public_api::content_digest(&storage)
                        .await
                        .unwrap(),
                    before,
                    "failed validation must leave the complete source content unchanged"
                );
                eprintln!(
                    "v{format} validation rejection preserved the original pointer and content digest"
                );
                panic!("browser-shaped v{format} upgrade failed: {error:?}");
            }
        };
        assert_eq!(report.before.format, Some(format));
        assert!(
            report.semantic_preservation_verified,
            "report must verify the source-derived preservation contract"
        );
        let migrated = inspect_existing_epoch_adapter(&storage).await.unwrap();
        let read = migrated.begin_read(ReadOptions::default()).await.unwrap();
        let actual_indexes = read
            .begin_scan(
                crate::hot_state::INDEX_SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                Default::default(),
            )
            .await
            .unwrap()
            .next_page(512)
            .await
            .unwrap()
            .into_parts()
            .0;
        let expected = indexes
            .iter()
            .map(|entry| {
                let ProjectedValue::FullValue(value) = &entry.value else {
                    panic!("full index values required");
                };
                (entry.key.0.clone(), value.clone())
            })
            .collect::<Vec<_>>();
        let actual = actual_indexes
            .iter()
            .map(|entry| {
                let ProjectedValue::FullValue(value) = &entry.value else {
                    panic!("full index values required");
                };
                (entry.key.0.clone(), value.clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected, actual,
            "every real resident index record must survive unchanged"
        );
        drop(read);
        let source_read = migrated.begin_read(ReadOptions::default()).await.unwrap();
        assert_eq!(
            crate::sync::partial_serving::preservation_entries(&source_read)
                .await
                .unwrap(),
            serving,
            "branch serving coordinates must survive migration"
        );
        assert!(
            crate::sync::legacy_read_interest_journal_upgrade(&source_read)
                .await
                .unwrap()
                .is_none(),
            "the current-format metadata repair must canonicalize old journals"
        );
        drop(source_read);
        let (engine, session) =
            Engine::new_partial_replica(migrated.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        let pending = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'browser-pending'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            pending.rows()[0].get::<serde_json::Value>("value").unwrap(),
            "local-pending"
        );
        let indexed_pending = session
            .execute(
                "SELECT value FROM legacy_custom_note WHERE id='browser-pending-note'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            indexed_pending.rows()[0].get::<String>("value").unwrap(),
            "indexed-pending"
        );
        let local_head = crate::branch::BranchHeadControlContext::new()
            .reader(&migrated.begin_read(ReadOptions::default()).await.unwrap())
            .load(&state.descriptor().selected_branch.branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            local_head.head_commit_id.to_string(),
            state.descriptor().selected_branch.head.commit_id,
            "pending local commits must not become acknowledged by migration"
        );
        drop(session);
        drop(engine);
        let authority_rows = crate::open_lix()
            .with_storage(authority.storage.clone())
            .await
            .unwrap();
        assert!(
            authority_rows
                .execute(
                    "SELECT value FROM lix_key_value WHERE key = 'browser-pending'",
                    &[]
                )
                .await
                .unwrap()
                .rows()
                .is_empty(),
            "migration must not upload unacknowledged local rows"
        );
        authority_rows.close().await.unwrap();
    }
}
