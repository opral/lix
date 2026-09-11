use super::*;
use crate::sync::partial_push_state::load_partial_push_state;
use crate::sync::partial_upload_cycle::upload_partial_once;

#[tokio::test]
#[ignore = "manual descriptor-only upload recovery integration gate"]
async fn descriptor_only_upload_retries_accepted_request_preserving_newer_local_edits() {
    for width in [16usize, 1600] {
        let authority = open_lix().await.unwrap();
        let values = (0..width)
            .map(|index| format!("('upload-{index:06}', 'before')"))
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value (key,value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            "00000000-0000-7000-8000-000000003399".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let branch = state.descriptor().selected_branch.branch_id.clone();
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
        let update = "UPDATE lix_key_value SET value=$2 WHERE key=$1";
        let select = "SELECT value FROM lix_key_value WHERE key=$1";
        let key = Value::Text("upload-000000".into());
        let mut fetches = Fetches::default();
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            update,
            &[key.clone(), Value::Text("uploaded".into())],
            &mut fetches,
        )
        .await
        .unwrap();
        prepare_baseline_jump_spines(&storage, &state, &authority, &mut fetches)
            .await
            .unwrap();
        let remote = &authority;
        let account = state.active_account_id();
        let attempt = "00000000-0000-7000-8000-000000003400";
        let lost = upload_partial_once(
            &storage,
            &state,
            &branch,
            attempt.into(),
            32,
            1024 * 1024,
            |request| async move {
                remote
                    .push_sync_repository_for_account(&request, account)
                    .await?;
                Err(LixError::unknown("simulated lost successful response"))
            },
        )
        .await
        .unwrap_err();
        assert_eq!(lost.message, "simulated lost successful response");
        session
            .execute(update, &[key.clone(), Value::Text("newer".into())])
            .await
            .unwrap();
        let before = admitted_controls(&storage, &state).await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let captured = load_partial_push_state(&read, &state, &branch)
            .await
            .unwrap()
            .0
            .prepared
            .unwrap();
        drop(read);
        assert_eq!(captured.attempt_id, attempt);
        let received_slot = std::sync::Mutex::new(None);
        let received = &received_slot;
        assert!(
            upload_partial_once(
                &storage,
                &state,
                &branch,
                "00000000-0000-7000-8000-000000003401".into(),
                32,
                1024 * 1024,
                |request| async move {
                    *received.lock().unwrap() = request.ref_updates[0].head_commit_id.clone();
                    remote
                        .push_sync_repository_for_account(&request, account)
                        .await
                }
            )
            .await
            .unwrap()
        );
        assert_eq!(*received.lock().unwrap(), Some(captured.target.head));
        assert_eq!(admitted_controls(&storage, &state).await.unwrap(), before);
        assert_eq!(
            value(session.execute(select, &[key.clone()]).await.unwrap()),
            "newer"
        );
        assert_eq!(
            value(authority.execute(select, &[key.clone()]).await.unwrap()),
            "uploaded"
        );
        assert!(
            upload_partial_once(
                &storage,
                &state,
                &branch,
                "00000000-0000-7000-8000-000000003402".into(),
                32,
                1024 * 1024,
                |request| async move {
                    remote
                        .push_sync_repository_for_account(&request, account)
                        .await
                }
            )
            .await
            .unwrap()
        );
        assert_eq!(
            value(authority.execute(select, &[key]).await.unwrap()),
            "newer"
        );
        assert_eq!(
            authority
                .execute(
                    "SELECT key FROM lix_key_value WHERE key LIKE 'upload-%'",
                    &[]
                )
                .await
                .unwrap()
                .rows()
                .len(),
            width
        );
        session.close().await.unwrap();
        drop(session);
        drop(engine);
        drop(storage);
        let storage = StorageAdapter::new(memory);
        let (_engine, session) = Engine::new_partial_replica(storage, EngineOptions::new(), &state)
            .await
            .unwrap();
        assert_eq!(
            value(
                session
                    .execute(select, &[Value::Text("upload-000000".into())])
                    .await
                    .unwrap()
            ),
            "newer"
        );
        session.close().await.unwrap();
    }
}
