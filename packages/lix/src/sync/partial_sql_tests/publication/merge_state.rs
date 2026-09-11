use super::*;
use crate::sync::partial_merge_protocol::{PartialMergeReceipt, PartialMergeRequest};
use crate::sync::partial_merge_state::{
    load_partial_merge_state, stage_capture_partial_merge, stage_record_partial_merge_receipt,
};
async fn captured_request(
    authority: &Lix<Memory>,
    engine: &Engine<Memory>,
    old: &PartialReplicaState,
) -> PartialMergeRequest {
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let branch = &old.descriptor().selected_branch.branch_id;
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(&read, old, branch)
        .await
        .unwrap();
    let controls = admitted_controls(&storage, old).await.unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: branch.clone(),
        base_commit_id: push.confirmed.head,
        expected_authority_head_commit_id: remote.selected_branch.head.commit_id,
        captured_local_head_commit_id: controls[0].head_commit_id.to_string(),
        checkpoint_commit_id: push.confirmed.checkpoint,
        global_head_commit_id: remote.global_branch.head.commit_id,
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id,
    }
}
#[tokio::test]
async fn recorded_merge_receipt_preserves_newer_local_tip_and_original_confirmation() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='captured-L' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES('remote-only','R')",
            &[],
        )
        .await
        .unwrap();
    let request = captured_request(&authority, &engine, &old).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_capture_partial_merge(&read, &mut writes, &old, &request)
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
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='newer-L2' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let controls = admitted_controls(&storage, &old).await.unwrap();
    // Bookkeeping fixture only: native M verification belongs to adoption,
    // and this test deliberately never publishes M as a serving control.
    let receipt = PartialMergeReceipt {
        request: request.clone(),
        merge_commit_id: uuid::Uuid::now_v7().to_string(),
    };
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_record_partial_merge_receipt(&read, &mut writes, &old, &receipt)
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
    assert_eq!(admitted_controls(&storage, &old).await.unwrap(), controls);
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (push, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, &request.branch_id)
            .await
            .unwrap();
    assert_eq!(push.confirmed.head, request.base_commit_id);
    let (record, _, _) = load_partial_merge_state(&read, &old, &request.branch_id)
        .await
        .unwrap();
    assert_eq!(record.unwrap().authority_receipt, Some(receipt.clone()));
    let mut changed = receipt;
    changed.merge_commit_id = uuid::Uuid::now_v7().to_string();
    assert!(
        stage_record_partial_merge_receipt(&read, &mut storage.new_write_set(), &old, &changed)
            .await
            .is_err()
    );
    drop(read);
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("newer-L2")
    );
}
#[tokio::test]
async fn merge_capture_racing_a_local_write_fails_without_creating_an_attempt() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='L' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES('remote-only','R')",
            &[],
        )
        .await
        .unwrap();
    let request = captured_request(&authority, &engine, &old).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_capture_partial_merge(&read, &mut writes, &old, &request)
        .await
        .unwrap();
    drop(read);
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='L2' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    assert!(
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        load_partial_merge_state(&read, &old, &request.branch_id)
            .await
            .unwrap()
            .0
            .is_none()
    );
}

#[tokio::test]
async fn merge_capture_invalidates_staged_ack_and_fences_both_ordinary_lanes() {
    use crate::sync::partial_push_state::{
        PartialPushCoordinate, PreparedPartialUpload, load_partial_push_state,
        stage_acknowledge_partial_upload, stage_prepare_partial_upload,
    };
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='captured-L' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES('remote-only','R')",
            &[],
        )
        .await
        .unwrap();
    let request = captured_request(&authority, &engine, &old).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (selected, _, _) = load_partial_push_state(&read, &old, &request.branch_id)
        .await
        .unwrap();
    let upload = PreparedPartialUpload {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        expected: selected.confirmed.clone(),
        target: PartialPushCoordinate {
            head: request.captured_local_head_commit_id.clone(),
            checkpoint: request.checkpoint_commit_id.clone(),
        },
    };
    let mut writes = storage.new_write_set();
    let preconditions =
        stage_prepare_partial_upload(&read, &mut writes, &old, &request.branch_id, &upload)
            .await
            .unwrap();
    drop(read);
    storage
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // ACK was fully staged before merge capture, against exactly the same
    // unchanged push record. Its merge-absence guard must reject publication.
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut ack_writes = storage.new_write_set();
    let ack_guards = stage_acknowledge_partial_upload(
        &read,
        &mut ack_writes,
        &old,
        &request.branch_id,
        &upload,
        true,
    )
    .await
    .unwrap();
    let mut capture = storage.new_write_set();
    let preconditions = stage_capture_partial_merge(&read, &mut capture, &old, &request)
        .await
        .unwrap();
    drop(read);
    storage
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            capture,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                ack_writes,
                StorageWriteOptions {
                    preconditions: ack_guards,
                    await_durable: true,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );

    let read = storage.begin_read(Default::default()).await.unwrap();
    let (unchanged, _, _) = load_partial_push_state(&read, &old, &request.branch_id)
        .await
        .unwrap();
    assert_eq!(unchanged.confirmed, upload.expected);
    assert_eq!(unchanged.prepared.as_ref(), Some(&upload));
    let (merge, _, _) = load_partial_merge_state(&read, &old, &request.branch_id)
        .await
        .unwrap();
    assert_eq!(merge.unwrap().original_upload, Some(upload.clone()));
    for branch in [request.branch_id.as_str(), crate::GLOBAL_BRANCH_ID] {
        let (push, _, _) = load_partial_push_state(&read, &old, branch).await.unwrap();
        let attempt = if branch == request.branch_id {
            upload.clone()
        } else {
            PreparedPartialUpload {
                attempt_id: uuid::Uuid::now_v7().to_string(),
                expected: push.confirmed.clone(),
                target: push.confirmed,
            }
        };
        let error = stage_prepare_partial_upload(
            &read,
            &mut storage.new_write_set(),
            &old,
            branch,
            &attempt,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "LIX_PARTIAL_REPLICA_MERGE_PENDING");
        for ref_accepted in [false, true] {
            let error = stage_acknowledge_partial_upload(
                &read,
                &mut storage.new_write_set(),
                &old,
                branch,
                &attempt,
                ref_accepted,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "LIX_PARTIAL_REPLICA_MERGE_PENDING");
        }
    }
}
