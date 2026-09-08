// Included inside repository.rs's tests module.

fn upload_proof_update(label: &str) -> SyncRefUpdate {
    let source = CommitId::for_test_label("prepared-source").to_string();
    let target = CommitId::for_test_label(label).to_string();
    SyncRefUpdate {
        branch_id: GLOBAL_BRANCH_ID.to_owned(),
        expected_head_commit_id: Some(source.clone()),
        expected_checkpoint_commit_id: Some(source),
        head_commit_id: Some(target.clone()),
        checkpoint_commit_id: Some(target),
    }
}

async fn commit_upload_proof(adapter: &StorageAdapter<Memory>, update: &SyncRefUpdate) {
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    let mut writes = adapter.new_write_set();
    let mut preconditions = Vec::new();
    super::super::upload_proof::stage_merge_proof(&read, &mut writes, &mut preconditions, update)
        .await
        .unwrap();
    drop(read);
    adapter
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                ..StorageWriteOptions::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn prepared_ref_proofs_keep_exact_pairs_and_require_matching_source() {
    let adapter = StorageAdapter::new(Memory::new());
    let first = upload_proof_update("prepared-target");
    commit_upload_proof(&adapter, &first).await;
    let mut second = first.clone();
    second.checkpoint_commit_id =
        Some(CommitId::for_test_label("different-checkpoint").to_string());
    commit_upload_proof(&adapter, &second).await;
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    let mut guards = Vec::new();
    let targets = super::super::upload_proof::load_copy_targets(
        &read,
        &mut guards,
        GLOBAL_BRANCH_ID,
        first.expected_head_commit_id.as_deref().unwrap(),
        first.expected_checkpoint_commit_id.as_deref().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        targets.len(),
        2,
        "the same head with distinct checkpoints names distinct attempts"
    );
    assert_eq!(guards.len(), 1);
    let obsolete = super::super::upload_proof::load_copy_targets(
        &read,
        &mut Vec::new(),
        GLOBAL_BRANCH_ID,
        first.expected_head_commit_id.as_deref().unwrap(),
        "a-new-checkpoint-coordinate",
    )
    .await
    .unwrap();
    assert!(
        obsolete.is_empty(),
        "head equality cannot hide a changed source checkpoint"
    );
    drop(read);
    second.expected_head_commit_id = second.head_commit_id.clone();
    second.expected_checkpoint_commit_id = second.checkpoint_commit_id.clone();
    commit_upload_proof(&adapter, &second).await;
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    let (proof, _) = super::super::upload_proof::load_proof(&read, GLOBAL_BRANCH_ID)
        .await
        .unwrap();
    assert_eq!(
        proof.unwrap().targets.len(),
        1,
        "advancing source retires old attempts"
    );
}

#[tokio::test]
async fn restore_proof_absence_guard_rejects_concurrent_preparation() {
    let adapter = StorageAdapter::new(Memory::new());
    let update = upload_proof_update("in-flight");
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    let mut preconditions = Vec::new();
    assert!(
        super::super::upload_proof::load_copy_targets(
            &read,
            &mut preconditions,
            GLOBAL_BRANCH_ID,
            update.expected_head_commit_id.as_deref().unwrap(),
            update.expected_checkpoint_commit_id.as_deref().unwrap()
        )
        .await
        .unwrap()
        .is_empty()
    );
    drop(read);
    commit_upload_proof(&adapter, &update).await;
    let mut writes = adapter.new_write_set();
    writes.put(
        SYNC_SEQUENCE_SPACE,
        StorageKey(Bytes::from_static(b"simulated-restore")),
        b"restore".to_vec(),
    );
    let error = adapter
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                ..StorageWriteOptions::default()
            },
        )
        .await
        .expect_err("a restore cannot miss a concurrently prepared publication");
    assert_eq!(
        LixError::from(error).code,
        LixError::CODE_TRANSACTION_CONFLICT
    );
}

#[tokio::test]
async fn prepared_ref_proof_limit_never_forgets_an_in_flight_target() {
    let adapter = StorageAdapter::new(Memory::new());
    for index in 0..64 {
        commit_upload_proof(
            &adapter,
            &upload_proof_update(&format!("bounded-target-{index}")),
        )
        .await;
    }
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    let error = super::super::upload_proof::stage_merge_proof(
        &read,
        &mut adapter.new_write_set(),
        &mut Vec::new(),
        &upload_proof_update("overflow-target"),
    )
    .await
    .expect_err("capacity must refuse, never evict proof");
    assert_eq!(error.code, "LIX_SYNC_PREPARED_REF_LIMIT");
    let (proof, _) = super::super::upload_proof::load_proof(&read, GLOBAL_BRANCH_ID)
        .await
        .unwrap();
    assert_eq!(proof.unwrap().targets.len(), 64);
    drop(read);
    commit_upload_proof(&adapter, &upload_proof_update("bounded-target-0")).await;
}

#[tokio::test]
async fn ordinary_upload_ack_preserves_a_later_restore_without_prior_reset() {
    for (append_before_restore, competing_server_write) in
        [(false, false), (true, false), (true, true)]
    {
        let authority = open_lix().await.expect("authority opens");
        write_key_value(&authority, "ordinary-restore", "restore-target").await;
        let target = current_branch_head(&authority).await;
        write_key_value(&authority, "ordinary-restore", "authority-tip").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot");
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        hydrate_history_commit(&authority, &replica, &target).await;
        write_key_value(&replica, "ordinary-restore", "prepared-prefix").await;
        let request = replica
            .build_sync_push(TEST_REMOTE, 128)
            .await
            .expect("ordinary upload builds")
            .expect("pending prefix");
        authority
            .push_sync_repository(&request)
            .await
            .expect("authority accepts prefix before acknowledgment reaches replica");
        if append_before_restore {
            write_key_value(&replica, "ordinary-restore", "unprepared-child").await;
        }
        replica
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(target.clone())],
            )
            .await
            .expect("user restores before seeing prefix acknowledgment");
        let restored_head = current_branch_head(&replica).await;
        if competing_server_write {
            authority
                .set_sync_role(super::super::SyncRole::Authority)
                .unwrap();
            write_key_value(&authority, "ordinary-restore", "foreign-wins").await;
        }
        let delta = authority
            .pull_sync_repository(Some(0), 128)
            .await
            .expect("acknowledgment page");
        replica
            .apply_sync_repository_pull(TEST_REMOTE, &delta)
            .await
            .expect("apply acknowledgment");
        if competing_server_write {
            assert_eq!(
                read_key_value(&replica, "ordinary-restore").await,
                "foreign-wins"
            );
            assert!(
                replica
                    .build_sync_push(TEST_REMOTE, 128)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert_eq!(current_branch_head(&replica).await, restored_head);
            assert_eq!(
                read_key_value(&replica, "ordinary-restore").await,
                "restore-target"
            );
            assert!(
                replica
                    .build_sync_push(TEST_REMOTE, 128)
                    .await
                    .unwrap()
                    .is_some(),
                "newer restore remains pending"
            );
        }
    }
}
