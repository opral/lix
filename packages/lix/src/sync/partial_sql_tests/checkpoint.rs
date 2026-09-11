use super::*;

#[tokio::test]
async fn descriptor_only_checkpoint_preserves_native_serving_basis_and_reopens() {
    for selected in [false, true] {
        let width = 16usize;
        let authority = open_lix().await.unwrap();
        let values = (0..width)
            .map(|index| format!("('partial-demand-{index:06}', 'before')"))
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let descriptor = serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000399".to_owned(),
            descriptor,
        )
        .unwrap();
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let (engine, session) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            std::sync::Arc::new(state.clone()),
            crate::sync::partial_replica_write_capability(),
        );
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let mut fetches = Fetches::default();
        for index in [0, 1] {
            execute_hydrating(
                &session,
                &storage,
                &state,
                &authority,
                "UPDATE lix_key_value SET value = 'edited' WHERE key = $1",
                &[Value::Text(format!("partial-demand-{index:06}"))],
                &mut fetches,
            )
            .await
            .unwrap();
        }
        let checkpoint = if selected {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key = 'partial-demand-000000'))"
        } else {
            "SELECT commit_id FROM lix_create_checkpoint()"
        };
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            checkpoint,
            &[],
            &mut fetches,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "partial checkpoint selected={selected}: {error}, details={:?}",
                error.details
            )
        });
        let diff = execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            "SELECT COUNT(*) AS n FROM lix_diff('lix_key_value')",
            &[],
            &mut fetches,
        )
        .await
        .unwrap();
        assert_eq!(
            diff.rows()[0].get::<i64>("n").unwrap(),
            if selected { 15 } else { 0 }
        );
        let branch_id = &state.descriptor().selected_branch.branch_id;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let prepared = crate::sync::partial_checkpoint_upload::prepare_partial_checkpoint_upload(
            &read,
            &state,
            branch_id,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap_or_else(|error| panic!("checkpoint export selected={selected}: {error}"))
        .unwrap();
        assert!(
            prepared
                .request
                .commits
                .iter()
                .any(|commit| commit.is_checkpoint)
        );
        assert!(prepared.request.commits.len() >= 3);
        let mut writes = storage.new_write_set();
        let mut guards = crate::sync::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            &state,
            branch_id,
            &prepared.upload,
        )
        .await
        .unwrap();
        guards.extend(prepared.control_guard);
        drop(read);
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        authority
            .push_sync_repository_for_account(&prepared.request, state.active_account_id())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "checkpoint authority import selected={selected}: {error}, details={:?}",
                    error.details
                )
            });
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = crate::sync::partial_push_state::stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            &state,
            branch_id,
            &prepared.upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        for index in [0, 1] {
            assert_eq!(
                value(
                    authority
                        .execute(
                            "SELECT value FROM lix_key_value WHERE key = $1",
                            &[Value::Text(format!("partial-demand-{index:06}"))]
                        )
                        .await
                        .unwrap()
                ),
                "edited"
            );
        }
        session.close().await.unwrap();
        drop(session);
        drop(engine);
        drop(storage);
        let storage = StorageAdapter::new(memory);
        let (_engine, reopened) =
            Engine::new_partial_replica(storage, EngineOptions::new(), &state)
                .await
                .unwrap_or_else(|error| {
                    panic!("checkpoint stranded admission selected={selected}: {error}")
                });
        for index in [0, 1] {
            assert_eq!(
                value(
                    reopened
                        .execute(
                            "SELECT value FROM lix_key_value WHERE key = $1",
                            &[Value::Text(format!("partial-demand-{index:06}"))]
                        )
                        .await
                        .unwrap()
                ),
                "edited"
            );
        }
        reopened.close().await.unwrap();
    }
}
