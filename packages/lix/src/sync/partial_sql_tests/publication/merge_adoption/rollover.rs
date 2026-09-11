//! Draft child of partial_sql_tests::publication::merge_adoption.
use super::*;
use crate::sync::partial_merge_state::{
    stage_acknowledge_partial_merge_body_wave, stage_prepare_partial_merge_body_wave,
    stage_rollover_partial_merge,
};

async fn rollover_fixture() -> (
    Lix<Memory>,
    Arc<Engine<Memory>>,
    SessionContext<Memory>,
    Arc<PartialReplicaState>,
    crate::sync::partial_merge_protocol::PartialMergeReceipt,
    PartialMergeRequest,
) {
    let (authority, engine, session, old, next) = merged_fixture().await;
    let storage = engine.storage();
    session
        .execute(
            "UPDATE lix_key_value SET value='newer-L2' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let record = load_partial_merge_state(&read, &old, &old.descriptor().selected_branch.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    let receipt = record.authority_receipt.clone().unwrap();
    let local = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&record.request.branch_id)
        .await
        .unwrap()
        .unwrap();
    let request = PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        base_commit_id: record.request.captured_local_head_commit_id.clone(),
        expected_authority_head_commit_id: next.descriptor().selected_branch.head.commit_id.clone(),
        captured_local_head_commit_id: local.head_commit_id.to_string(),
        ..record.request.clone()
    };
    drop(read);
    // Native M is an explicit authenticated dependency, not a fabricated test
    // record. The M==R2 proof terminates without scanning unrelated ancestry.
    hydrate_metadata(
        &storage,
        &old,
        &authority,
        NativeMetadataRef::CommitGraphRecord(receipt.merge_commit_id.clone()),
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    (authority, engine, session, old, receipt, request)
}

#[tokio::test]
async fn rollover_preserves_original_confirmation_and_rejects_prior_outcomes() {
    let (_authority, engine, session, old, prior, request) = rollover_fixture().await;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let before = load_partial_merge_state(&read, &old, &request.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_rollover_partial_merge(&read, &mut writes, &old, &request)
        .await
        .unwrap();
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let after = load_partial_merge_state(&read, &old, &request.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    let mut replay = storage.new_write_set();
    let _replay_guards = stage_rollover_partial_merge(&read, &mut replay, &old, &request)
        .await
        .unwrap();
    assert!(replay.is_empty());
    assert_eq!(after.original_confirmed, before.original_confirmed);
    assert_eq!(after.original_upload, before.original_upload);
    assert_eq!(
        after.request.base_commit_id,
        prior.request.captured_local_head_commit_id
    );
    assert_eq!(after.previous_receipt.as_ref(), Some(&prior));
    assert!(after.authority_receipt.is_none());
    assert_eq!(after.accepted_body_tip, request.base_commit_id);
    let (push, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, &request.branch_id)
            .await
            .unwrap();
    assert_eq!(
        push.confirmed, before.original_confirmed,
        "B must not advance to L or M before serving adoption"
    );
    let mut rejected = storage.new_write_set();
    assert!(
        stage_record_partial_merge_receipt(&read, &mut rejected, &old, &prior)
            .await
            .is_err()
    );
    assert!(
        stage_acknowledge_partial_merge_body_wave(
            &read,
            &mut rejected,
            &old,
            &prior.request,
            &prior.request.base_commit_id,
            &prior.request.captured_local_head_commit_id
        )
        .await
        .is_err()
    );
    assert!(rejected.is_empty());
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
async fn prepared_body_wave_survives_reload_and_stale_ack_cannot_advance_twice() {
    let (_authority, engine, _session, old, _prior, request) = rollover_fixture().await;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_rollover_partial_merge(&read, &mut writes, &old, &request)
        .await
        .unwrap();
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_prepare_partial_merge_body_wave(
        &read,
        &mut writes,
        &old,
        &request,
        &request.base_commit_id,
        &request.captured_local_head_commit_id,
    )
    .await
    .unwrap();
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let loaded = load_partial_merge_state(&read, &old, &request.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    assert_eq!(
        loaded.prepared_body_wave.as_ref().unwrap().previous,
        request.base_commit_id
    );
    assert_eq!(
        loaded.prepared_body_wave.as_ref().unwrap().target,
        request.captured_local_head_commit_id
    );
    let mut retry = storage.new_write_set();
    let _retry_guards = stage_prepare_partial_merge_body_wave(
        &read,
        &mut retry,
        &old,
        &request,
        &request.base_commit_id,
        &request.captured_local_head_commit_id,
    )
    .await
    .unwrap();
    assert!(
        retry.is_empty(),
        "identical durable wave retry must reuse its coordinates"
    );
    let mut wrong = storage.new_write_set();
    assert!(
        stage_prepare_partial_merge_body_wave(
            &read,
            &mut wrong,
            &old,
            &request,
            &request.base_commit_id,
            &request.expected_authority_head_commit_id
        )
        .await
        .is_err()
    );
    assert!(wrong.is_empty());
    let mut first_ack = storage.new_write_set();
    let first_guards = stage_acknowledge_partial_merge_body_wave(
        &read,
        &mut first_ack,
        &old,
        &request,
        &request.base_commit_id,
        &request.captured_local_head_commit_id,
    )
    .await
    .unwrap();
    let mut stale_ack = storage.new_write_set();
    let stale_guards = stage_acknowledge_partial_merge_body_wave(
        &read,
        &mut stale_ack,
        &old,
        &request,
        &request.base_commit_id,
        &request.captured_local_head_commit_id,
    )
    .await
    .unwrap();
    drop(read);
    commit_bookkeeping(&storage, first_ack, first_guards).await;
    assert!(
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                stale_ack,
                StorageWriteOptions {
                    preconditions: stale_guards,
                    await_durable: true,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    let after = load_partial_merge_state(&read, &old, &request.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    assert_eq!(
        after.accepted_body_tip,
        request.captured_local_head_commit_id
    );
    assert!(after.prepared_body_wave.is_none());
    let (push, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, &request.branch_id)
            .await
            .unwrap();
    assert_eq!(push.confirmed, after.original_confirmed);
}

mod restart;
