use super::*;
use crate::sync::partial_merge_state::{
    stage_capture_restarted_partial_merge, stage_prepare_partial_merge_restart,
    stage_record_partial_merge_restart_outcome,
};
use crate::sync::{
    PartialAttemptRestartOutcome, PartialAttemptRestartReceipt, PartialAttemptRestartRequest,
};

async fn restarting() -> (
    Lix<Memory>,
    Arc<Engine<Memory>>,
    SessionContext<Memory>,
    Arc<PartialReplicaState>,
    PartialAttemptRestartRequest,
) {
    let (authority, engine, session, state, _, request) = rollover_fixture().await;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_rollover_partial_merge(&read, &mut writes, &state, &request)
        .await
        .unwrap();
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    let intent = PartialAttemptRestartRequest {
        old: request,
        next_attempt_id: uuid::Uuid::now_v7().to_string(),
    };
    (authority, engine, session, state, intent)
}
// These are client-state tests. Authority expiration/authentication is exercised
// by partial_attempt_restart's server tests; this fixture supplies its exact DTO.
fn restarted(
    state: &PartialReplicaState,
    intent: &PartialAttemptRestartRequest,
) -> PartialAttemptRestartOutcome {
    let receipt: PartialAttemptRestartReceipt = serde_json::from_value(serde_json::json!({
        "version":1,"repositoryId":state.repository_id(),"accountId":state.active_account_id(),"request":intent,
    })).unwrap();
    PartialAttemptRestartOutcome::Restarted { receipt }
}
async fn persist_restart(
    engine: &Engine<Memory>,
    state: &PartialReplicaState,
    intent: &PartialAttemptRestartRequest,
    terminal: bool,
) {
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = if terminal {
        stage_record_partial_merge_restart_outcome(
            &read,
            &mut writes,
            state,
            intent,
            &restarted(state, intent),
        )
        .await
        .unwrap()
    } else {
        stage_prepare_partial_merge_restart(&read, &mut writes, state, intent)
            .await
            .unwrap()
    };
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
}
#[tokio::test]
async fn lost_restart_reply_reuses_durable_uuid_and_terminal_restart_rejects_old_work() {
    let (_, engine, _, state, intent) = restarting().await;
    persist_restart(&engine, &state, &intent, false).await;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let record = load_partial_merge_state(&read, &state, &intent.old.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    assert_eq!(record.restart.as_ref().unwrap().request, intent);
    let mut retry = storage.new_write_set();
    let _guards = stage_prepare_partial_merge_restart(&read, &mut retry, &state, &intent)
        .await
        .unwrap();
    assert!(retry.is_empty());
    let different = PartialAttemptRestartRequest {
        next_attempt_id: uuid::Uuid::now_v7().to_string(),
        ..intent.clone()
    };
    assert!(
        stage_prepare_partial_merge_restart(&read, &mut retry, &state, &different)
            .await
            .is_err()
    );
    assert!(
        stage_prepare_partial_merge_body_wave(
            &read,
            &mut retry,
            &state,
            &intent.old,
            &intent.old.base_commit_id,
            &intent.old.captured_local_head_commit_id
        )
        .await
        .is_err()
    );
    drop(read);
    persist_restart(&engine, &state, &intent, true).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut rejected = storage.new_write_set();
    assert!(
        stage_acknowledge_partial_merge_body_wave(
            &read,
            &mut rejected,
            &state,
            &intent.old,
            &intent.old.base_commit_id,
            &intent.old.captured_local_head_commit_id
        )
        .await
        .is_err()
    );
    let stale = crate::sync::partial_merge_protocol::PartialMergeReceipt {
        request: intent.old.clone(),
        merge_commit_id: uuid::Uuid::now_v7().to_string(),
    };
    assert!(
        stage_record_partial_merge_receipt(&read, &mut rejected, &state, &stale)
            .await
            .is_err()
    );
    assert!(rejected.is_empty());
    let after = load_partial_merge_state(&read, &state, &intent.old.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    assert_eq!(after.original_confirmed, record.original_confirmed);
    assert!(after.restart.unwrap().receipt.is_some());
}
async fn prepare_capture(
    engine: &Engine<Memory>,
    state: &PartialReplicaState,
    authority: &Lix<Memory>,
    request: &PartialMergeRequest,
) -> (
    crate::storage_adapter::StorageWriteSet,
    Vec<crate::storage_adapter::StoragePrecondition>,
) {
    let storage = engine.storage();
    let mut seen = BTreeSet::new();
    for _ in 0..256 {
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let error =
            match stage_capture_restarted_partial_merge(&read, &mut writes, state, request).await {
                Ok(guards) => return (writes, guards),
                Err(error) => error,
            };
        drop(read);
        if let Some(address) = NativeObjectRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("object:{address:?}")), "{error}");
            hydrate_native_object(
                &storage,
                state,
                address,
                32 * 1024 * 1024,
                |request| async move { authority.read_sync_native_object_range(&request).await },
            )
            .await
            .unwrap();
        } else if let Some(address) = NativeMetadataRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("metadata:{address:?}")), "{error}");
            hydrate_metadata(&storage, state, authority, address, &mut Fetches::default())
                .await
                .unwrap();
        } else {
            panic!("restart capture: {error:?}")
        }
    }
    panic!("restart capture budget");
}
#[tokio::test]
async fn restart_capture_fences_newer_l3_and_preserves_original_upload() {
    let (authority, engine, session, state, intent) = restarting().await;
    persist_restart(&engine, &state, &intent, false).await;
    persist_restart(&engine, &state, &intent, true).await;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let before = load_partial_merge_state(&read, &state, &intent.old.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    drop(read);
    let mut request = PartialMergeRequest {
        attempt_id: intent.next_attempt_id.clone(),
        ..intent.old.clone()
    };
    let (writes, guards) = prepare_capture(&engine, &state, &authority, &request).await;
    session
        .execute(
            "UPDATE lix_key_value SET value='newer-L3' WHERE key='resident'",
            &[],
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
    let current = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&request.branch_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        load_partial_merge_state(&read, &state, &request.branch_id)
            .await
            .unwrap()
            .0
            .unwrap()
            .restart
            .unwrap()
            .receipt
            .is_some()
    );
    request.captured_local_head_commit_id = current.head_commit_id.to_string();
    drop(read);
    let (writes, guards) = prepare_capture(&engine, &state, &authority, &request).await;
    commit_bookkeeping(&storage, writes, guards).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let after = load_partial_merge_state(&read, &state, &request.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    assert_eq!(after.request, request);
    assert_eq!(after.original_confirmed, before.original_confirmed);
    assert_eq!(after.original_upload, before.original_upload);
    assert_eq!(after.previous_receipt, before.previous_receipt);
    assert!(after.restart.is_none());
    assert!(after.prepared_body_wave.is_none());
    assert!(after.authority_receipt.is_none());
    assert_eq!(after.accepted_body_tip, request.base_commit_id);
    drop(read);
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("newer-L3")
    );
}
