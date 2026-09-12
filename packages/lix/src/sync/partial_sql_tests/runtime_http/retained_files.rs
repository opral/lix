//! Real HTTP retained-body admission: blob preparation precedes body acceptance.
use super::*;

#[tokio::test]
async fn retained_file_body_upload_is_ref_free_and_exact_retry_is_idempotent() {
    retained_file_case(false).await;
}

#[tokio::test]
async fn retained_selected_checkpoint_and_newer_file_edit_preserve_all_body_pins() {
    retained_file_case(true).await;
}

async fn retained_file_case(checkpoint: bool) {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let transport = HttpSyncTransport::connect_with(
        Client {
            server,
            lose_body: Arc::new(AtomicBool::new(true)),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let state = PartialReplicaState::from_leased(
        transport.protocol_url().into(),
        authority.active_account_id().into(),
        uuid::Uuid::now_v7().to_string(),
        wrapper.wire,
    )
    .unwrap();
    let storage = StorageAdapter::new(Memory::new());
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
        Arc::new(state.clone()),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "INSERT INTO lix_file(path,content) VALUES('/retained.txt',$1)",
        &[Value::Blob(vec![b'x'; 96 * 1024].into())],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    if checkpoint {
        execute_hydrating(
            &session, &storage, &state, &authority,
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file') WHERE to_path='/retained.txt'))", &[], &mut Fetches::default(),
        ).await.unwrap();
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            "UPDATE lix_file SET content=$1 WHERE path='/retained.txt'",
            &[Value::Blob(vec![b'y'; 96 * 1024].into())],
            &mut Fetches::default(),
        )
        .await
        .unwrap();
    }
    if checkpoint {
        crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
            .await
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('concurrent-authority','R')",
                &[],
            )
            .await
            .unwrap();
    }
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&state.descriptor().selected_branch.branch_id)
        .await
        .unwrap()
        .unwrap();
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: state.descriptor().selected_branch.branch_id.clone(),
        base_commit_id: state.descriptor().selected_branch.head.commit_id.clone(),
        expected_authority_head_commit_id: remote.selected_branch.head.commit_id.clone(),
        captured_local_head_commit_id: control.head_commit_id.to_string(),
        expected_authority_checkpoint_commit_id: state
            .descriptor()
            .selected_branch
            .checkpoint
            .commit_id
            .clone(),
        captured_local_checkpoint_commit_id: state
            .descriptor()
            .selected_branch
            .checkpoint
            .commit_id
            .clone(),
        checkpoint_commit_id: state
            .descriptor()
            .selected_branch
            .checkpoint
            .commit_id
            .clone(),
        global_head_commit_id: state.descriptor().global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: state
            .descriptor()
            .global_branch
            .checkpoint
            .commit_id
            .clone(),
    };
    let wave = crate::sync::partial_merge_runtime::captured_wave(
        &read,
        &request,
        &request.base_commit_id,
        None,
    )
    .await
    .unwrap();
    drop(read);
    let exact = serde_json::to_vec(&wave).unwrap();
    assert!(wave.bodies.ref_updates.is_empty());
    let blobs = crate::sync::repository::sync_commit_blob_ids(&wave.bodies.commits).unwrap();
    assert!(!blobs.is_empty());
    for blob in &blobs {
        assert!(
            authority
                .get_sync_blob_manifest(blob)
                .await
                .unwrap()
                .is_none()
        );
    }
    // This helper must not push refs or bodies as a side effect.
    crate::sync::partial_blob_upload::prepare_partial_upload_blobs(
        &storage,
        &state,
        &transport,
        &wave.bodies,
    )
    .await
    .unwrap();
    let before = authority.partial_replica_descriptor(None).await.unwrap();
    for blob in &blobs {
        assert!(
            authority
                .get_sync_blob_manifest(blob)
                .await
                .unwrap()
                .is_some()
        );
    }
    let lost = transport.retained_body_wave(&wave).await.unwrap_err();
    assert_eq!(lost.code, "TEST_LOST_BODY_ACK");
    crate::sync::partial_blob_upload::prepare_partial_upload_blobs(
        &storage,
        &state,
        &transport,
        &wave.bodies,
    )
    .await
    .unwrap();
    let accepted = transport.retained_body_wave(&wave).await.unwrap();
    assert_eq!(accepted.accepted_tip, request.captured_local_head_commit_id);
    assert_eq!(serde_json::to_vec(&wave).unwrap(), exact);
    let adapter = authority.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let identity = crate::gc::NativeUploadAttemptIdentity {
        repository_id: authority.lix_id().into(),
        account_id: authority.active_account_id().into(),
        branch_id: request.branch_id.clone(),
        attempt_id: request.attempt_id.clone(),
    };
    let (retained, _) = crate::gc::load_native_upload_attempt(&read, &identity)
        .await
        .unwrap()
        .unwrap();
    let roots = retained.retained_body_roots().unwrap();
    for body in &wave.bodies.commits {
        assert!(roots.contains(
            &crate::changelog::CommitId::parse_lix(&body.commit_id, "test body").unwrap()
        ));
    }
    drop(read);
    transport.retained_body_wave(&wave).await.unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let (retried, _) = crate::gc::load_native_upload_attempt(&read, &identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retried.retained_body_roots().unwrap(),
        roots,
        "repeated wave cannot grow pins"
    );
    drop(read);
    let after = authority.partial_replica_descriptor(None).await.unwrap();
    assert_eq!(after.selected_branch.head, before.selected_branch.head);
    assert_eq!(
        after.selected_branch.checkpoint,
        before.selected_branch.checkpoint
    );
    assert_eq!(after.global_branch.head, before.global_branch.head);
    let mut injected = wave.clone();
    injected.bodies.commits[0].parent_commit_ids = vec![uuid::Uuid::now_v7().to_string()];
    assert!(
        transport.retained_body_wave(&injected).await.is_err(),
        "an unproved dependency cannot become a retained anchor"
    );
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let (unchanged, _) = crate::gc::load_native_upload_attempt(&read, &identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.retained_body_roots().unwrap(), roots);
    drop(read);
    let mut oversized = wave.clone();
    oversized.bodies.commits = std::iter::repeat_n(wave.bodies.commits[0].clone(), 33).collect();
    assert!(
        oversized.validate().is_err(),
        "retained waves remain capped at 32 commits"
    );
}
