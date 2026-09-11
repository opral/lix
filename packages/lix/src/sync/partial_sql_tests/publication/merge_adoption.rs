use super::*;
use crate::sync::partial_merge_protocol::{PartialMergeRequest, RetainedBodyWaveRequest};
use crate::sync::partial_merge_state::{
    load_partial_merge_state, stage_capture_partial_merge, stage_record_partial_merge_receipt,
};
use crate::sync::partial_publication::{PartialRecoveryPolicy, prepare_partial_publication};

async fn commit_bookkeeping(
    storage: &StorageAdapter<Memory>,
    writes: crate::storage_adapter::StorageWriteSet,
    guards: Vec<crate::storage_adapter::StoragePrecondition>,
) {
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
}
async fn merged_fixture() -> (
    Lix<Memory>,
    Arc<Engine<Memory>>,
    SessionContext<Memory>,
    Arc<PartialReplicaState>,
    Arc<PartialReplicaState>,
) {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='local-L' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "SELECT value FROM lix_key_value WHERE key='remote-new'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('remote-new','remote-R')",
            &[],
        )
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut upload = crate::sync::partial_upload::prepare_partial_ordinary_upload(
        &read,
        &old,
        &remote.selected_branch.branch_id,
        uuid::Uuid::now_v7().to_string(),
        32,
        1024 * 1024,
    )
    .await
    .unwrap()
    .unwrap();
    let request = PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id,
        base_commit_id: upload.upload.expected.head.clone(),
        expected_authority_head_commit_id: remote.selected_branch.head.commit_id,
        captured_local_head_commit_id: upload.upload.target.head.clone(),
        checkpoint_commit_id: upload.upload.expected.checkpoint.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id,
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id,
    };
    let mut writes = storage.new_write_set();
    let guards = stage_capture_partial_merge(&read, &mut writes, &old, &request)
        .await
        .unwrap();
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    upload.request.ref_updates.clear();
    authority
        .push_retained_body_wave_for_account(
            &RetainedBodyWaveRequest {
                request: request.clone(),
                expected_previous_commit_id: request.base_commit_id.clone(),
                bodies: upload.request,
            },
            authority.active_account_id(),
        )
        .await
        .unwrap();
    let receipt = authority
        .merge_partial_replica_for_account(&request, authority.active_account_id())
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let guards = stage_record_partial_merge_receipt(&read, &mut writes, &old, &receipt)
        .await
        .unwrap();
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    (authority, engine, session, old, next)
}
async fn prepare_merge_hydrating(
    engine: &Engine<Memory>,
    old: &PartialReplicaState,
    next: Arc<PartialReplicaState>,
    authority: &Lix<Memory>,
) -> PreparedPartialPublication {
    let storage = engine.storage();
    let deadline = crate::sync::http::CandidateBaselineDeadline::for_test(
        &next.baseline_lease().lease_id,
        std::time::Duration::from_secs(300),
    );
    let mut seen = BTreeSet::new();
    for _ in 0..256 {
        let error = match prepare_partial_publication(
            engine,
            next.clone(),
            deadline.clone(),
            PartialRecoveryPolicy::NativeMerge,
        )
        .await
        {
            Ok(Some(prepared)) => return prepared,
            Ok(None) => panic!("merge must publish"),
            Err(error) => error,
        };
        if let Some(address) = NativeObjectRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("object:{address:?}")), "{error}");
            hydrate_native_object(
                &storage,
                old,
                address,
                32 * 1024 * 1024,
                |request| async move { authority.read_sync_native_object_range(&request).await },
            )
            .await
            .unwrap();
        } else if let Some(address) = NativeMetadataRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("metadata:{address:?}")), "{error}");
            hydrate_metadata(&storage, old, authority, address, &mut Fetches::default())
                .await
                .unwrap();
        } else {
            panic!("merge preparation: {error:?}")
        }
    }
    panic!("bounded merge preparation exhausted");
}
#[tokio::test]
async fn native_merge_adoption_atomically_settles_outbox_and_preserves_local_and_remote_rows() {
    let (authority, engine, session, old, next) = merged_fixture().await;
    let storage = engine.storage();
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("local-L")
    );
    assert!(
        session
            .execute(
                "SELECT value FROM lix_key_value WHERE key='remote-new'",
                &[]
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    let prepared = prepare_merge_hydrating(&engine, &old, next.clone(), &authority).await;
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("local-L")
    );
    assert!(
        value(
            session
                .execute(
                    "SELECT value FROM lix_key_value WHERE key='remote-new'",
                    &[]
                )
                .await
                .unwrap()
        )
        .contains("remote-R")
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        load_partial_merge_state(&read, &next, &next.descriptor().selected_branch.branch_id)
            .await
            .unwrap()
            .0
            .is_none()
    );
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
        &read,
        &next,
        &next.descriptor().selected_branch.branch_id,
    )
    .await
    .unwrap();
    assert_eq!(
        push.confirmed.head,
        next.descriptor().selected_branch.head.commit_id
    );
    assert!(push.prepared.is_none());
    drop(read);
    let (reopened, reopened_session) =
        Engine::new_partial_replica(storage, EngineOptions::new(), &next)
            .await
            .unwrap();
    reopened
        .sync_mode()
        .admit_partial_replica(next, crate::sync::partial_replica_write_capability());
    assert!(
        value(
            reopened_session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("local-L")
    );
}
#[tokio::test]
async fn newer_local_edit_after_merge_preparation_rejects_adoption_and_preserves_outbox() {
    let (authority, engine, session, old, next) = merged_fixture().await;
    let storage = engine.storage();
    let prepared = prepare_merge_hydrating(&engine, &old, next, &authority).await;
    session
        .execute(
            "UPDATE lix_key_value SET value='newer-L2' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        publish_prepared_partial(engine.clone(), prepared)
            .await
            .is_err()
    );
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(old.as_ref())
    );
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("newer-L2")
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    let record = load_partial_merge_state(&read, &old, &old.descriptor().selected_branch.branch_id)
        .await
        .unwrap()
        .0
        .unwrap();
    assert!(record.authority_receipt.is_some());
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
        &read,
        &old,
        &record.request.branch_id,
    )
    .await
    .unwrap();
    assert_eq!(push.confirmed.head, record.request.base_commit_id);
}

mod rollover;

#[tokio::test]
async fn recorded_receipt_without_native_merge_parents_never_adopts_or_clears_local_edits() {
    let (_authority, engine, session, old, next) = merged_fixture().await;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let branch = &old.descriptor().selected_branch.branch_id;
    let (record, raw, guards) = load_partial_merge_state(&read, &old, branch).await.unwrap();
    let record = record.unwrap();
    let mut wire: serde_json::Value = serde_json::from_slice(&raw.unwrap()).unwrap();
    // Simulate an authenticated but incorrect authority outcome referring to
    // the actual local ordinary commit. It has valid native bytes and UUIDs,
    // but it does not contain the claimed pair of merge parents.
    wire["authorityReceipt"]["mergeCommitId"] =
        serde_json::Value::String(record.request.captured_local_head_commit_id);
    let mut writes = storage.new_write_set();
    writes.put(
        crate::sync::partial_merge_state::PARTIAL_BRANCH_MERGE_SPACE,
        crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
            uuid::Uuid::parse_str(branch).unwrap().as_bytes(),
        )),
        serde_json::to_vec(&wire).unwrap(),
    );
    drop(read);
    commit_bookkeeping(&storage, writes, guards).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let error =
        crate::sync::partial_merge_settlement::verify_partial_merge_settlement(&read, &old, &next)
            .await
            .err()
            .unwrap();
    assert_eq!(error.code, "LIX_PARTIAL_MERGE_STATE_INVALID");
    assert!(
        load_partial_merge_state(&read, &old, branch)
            .await
            .unwrap()
            .0
            .is_some()
    );
    drop(read);
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(old.as_ref())
    );
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("local-L")
    );
}
