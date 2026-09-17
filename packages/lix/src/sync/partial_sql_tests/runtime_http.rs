//! Real canonical HTTP handler and client dispatcher, with a lost body ACK.
use super::*;
use crate::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use crate::sync::http::{HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse};
use crate::sync::{SyncTransportFuture, partial_merge_runtime::prepare_descriptor_with_merge};
use http_body_util::BodyExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
#[derive(Clone)]
struct Client {
    server: LixServerProtocol<Memory>,
    lose_body: Arc<AtomicBool>,
}
impl RawHttpClient for Client {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let retained = request.url.ends_with("/sync/retained-bodies");
            let mut builder = http::Request::builder()
                .method(request.method)
                .uri(request.url);
            for (name, value) in request.headers {
                builder = builder.header(name, value);
            }
            let response = self
                .server
                .handle(
                    builder
                        .body(ServerProtocolBody::from(request.body.unwrap_or_default()))
                        .unwrap(),
                    ServerProtocolContext::anonymous(),
                )
                .await;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|e| LixError::unknown(e.to_string()))?
                .to_bytes()
                .to_vec();
            assert!(body.len() <= request.response_limit);
            if retained && status.is_success() && self.lose_body.swap(false, Ordering::SeqCst) {
                return Err(LixError::new(
                    "TEST_LOST_BODY_ACK",
                    "authority committed body wave before connection failed",
                ));
            }
            Ok(RawHttpResponse {
                status: status.as_u16(),
                status_text: status.to_string(),
                body,
            })
        })
    }
}
#[derive(Clone)]
struct CountPublicationRequests {
    inner: Client,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    blob_reply: Arc<std::sync::Mutex<Option<Vec<crate::sync::SyncBlobManifest>>>>,
}
impl RawHttpClient for CountPublicationRequests {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        if request.method == "GET" && request.url.contains("/sync/blob?") {
            if let Some(reply) = self.blob_reply.lock().unwrap().clone() {
                return Box::pin(async move {
                    Ok(RawHttpResponse {
                        status: 200,
                        status_text: "OK".into(),
                        body: serde_json::to_vec(&reply).unwrap(),
                    })
                });
            }
        }
        if request.url.ends_with("/sync/push")
            || (request.method != "GET"
                && (request.url.contains("/sync/blob") || request.url.contains("/sync/chunk")))
        {
            self.requests.lock().unwrap().push(request.url.clone());
        }
        self.inner.send(request)
    }
}
#[tokio::test]
async fn http_dispatcher_recovers_lost_wave_and_preserves_newer_local_edit() {
    lost_wave_with_pending_edit(false, PendingScenario::Ordinary, 1).await;
}

#[tokio::test]
async fn http_dispatcher_accepts_current_authority_and_preserves_newer_local_edit() {
    lost_wave_with_pending_edit(true, PendingScenario::Ordinary, 1).await;
}

#[tokio::test]
async fn http_dispatcher_accepts_global_drift_and_preserves_newer_local_edit() {
    lost_wave_with_pending_edit(true, PendingScenario::GlobalBeforeReceipt, 1).await;
}

#[tokio::test]
async fn http_dispatcher_settles_after_global_drift_with_newer_local_edit() {
    lost_wave_with_pending_edit(true, PendingScenario::GlobalAfterReceipt, 1).await;
}

#[tokio::test]
async fn local_branch_creation_between_pending_selected_edits_converges() {
    lost_wave_with_pending_edit(true, PendingScenario::LocalBranch, 1).await;
}

#[tokio::test]
async fn http_dispatcher_accepts_account_admission_during_pending_merge() {
    lost_wave_with_pending_edit(true, PendingScenario::AccountAdmission, 1).await;
}

#[tokio::test]
async fn http_dispatcher_pages_long_offline_history_against_divergent_authority() {
    lost_wave_with_pending_edit(true, PendingScenario::Ordinary, 1025).await;
}

#[tokio::test]
async fn http_dispatcher_pages_long_checkpoint_source_against_divergent_authority() {
    lost_wave_with_pending_edit(true, PendingScenario::Checkpoint, 1025).await;
}

#[tokio::test]
async fn http_dispatcher_pages_multiple_pending_checkpoints_against_divergent_authority() {
    lost_wave_with_pending_edit(true, PendingScenario::Checkpoints, 1025).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingScenario {
    Ordinary,
    GlobalBeforeReceipt,
    GlobalAfterReceipt,
    AccountAdmission,
    Checkpoint,
    Checkpoints,
    LocalBranch,
}

async fn advance_unrelated_global(authority: &Lix<Memory>) {
    authority
        .open_another_session()
        .with_branch(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap()
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('global-race','changed')",
            &[],
        )
        .await
        .unwrap();
}

async fn lost_wave_with_pending_edit(
    advance_after_descriptor: bool,
    scenario: PendingScenario,
    offline_commits: usize,
) {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('local','B'),('remote','B')",
            &[],
        )
        .await
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let lose_body = Arc::new(AtomicBool::new(true));
    let transport = HttpSyncTransport::connect_with(
        Client { server, lose_body },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let old = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            wrapper.wire,
        )
        .unwrap(),
    );
    transport
        .bind_native_baseline_lease(old.baseline_lease())
        .unwrap();
    let storage = StorageAdapter::new(Memory::new());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
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
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
            .await
            .unwrap();
    let engine = Arc::new(engine);
    engine
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let mut fetches = Fetches::default();
    for sql in [
        "SELECT key,value FROM lix_key_value WHERE key IN ('local','remote')",
        "UPDATE lix_key_value SET value='L' WHERE key='local'",
    ] {
        execute_hydrating(&session, &storage, &old, &authority, sql, &[], &mut fetches)
            .await
            .unwrap();
    }
    for index in 1..offline_commits {
        session
            .execute(
                &format!("UPDATE lix_key_value SET value='offline-{index}' WHERE key='local'"),
                &[],
            )
            .await
            .unwrap();
        if scenario == PendingScenario::Checkpoints && index == offline_commits / 2 {
            execute_hydrating(
                &session, &storage, &old, &authority,
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))",
                &[], &mut fetches,
            ).await.unwrap();
        }
    }
    let pending_checkpoint = if matches!(
        scenario,
        PendingScenario::Checkpoint | PendingScenario::Checkpoints
    ) {
        execute_hydrating(
            &session, &storage, &old, &authority,
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))",
            &[], &mut fetches,
        ).await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        Some(
            crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load(&old.descriptor().selected_branch.branch_id)
                .await
                .unwrap()
                .unwrap()
                .working_diff_checkpoint_commit_id
                .unwrap()
                .to_string(),
        )
    } else {
        None
    };
    prepare_baseline_jump_spines(&storage, &old, &authority, &mut fetches)
        .await
        .unwrap();
    // Serving installed durable authority ownership in another adapter.
    // Admit this test's existing authority handle through the same native owner.
    crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
        .await
        .unwrap();
    authority
        .execute("UPDATE lix_key_value SET value='R' WHERE key='remote'", &[])
        .await
        .unwrap();
    let captured_descriptor = transport.partial_replica_descriptor(None).await.unwrap();
    let captured_remote = captured_descriptor
        .wire
        .descriptor
        .selected_branch
        .head
        .commit_id
        .clone();
    if advance_after_descriptor {
        // Capture R, then accept R2 before the first retained wave reaches the
        // authority. Its otherwise valid immutable request must restart.
        authority
            .execute(
                "UPDATE lix_key_value SET value='R2' WHERE key='remote'",
                &[],
            )
            .await
            .unwrap();
    }
    let first = prepare_descriptor_with_merge(
        engine.clone(),
        old.clone(),
        &transport,
        captured_descriptor,
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await;
    assert_eq!(first.err().unwrap().code, "TEST_LOST_BODY_ACK");
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (pending, _, _) = crate::sync::partial_merge_state::load_partial_merge_state(
        &read,
        &old,
        &old.descriptor().selected_branch.branch_id,
    )
    .await
    .unwrap();
    let pending = pending.unwrap();
    assert!(pending.prepared_body_wave.is_some());
    assert_eq!(pending.accepted_body_tip, pending.request.base_commit_id);
    assert!(pending.authority_receipt.is_none());
    assert_eq!(
        pending.request.expected_authority_head_commit_id,
        captured_remote
    );
    drop(read);
    if scenario == PendingScenario::GlobalBeforeReceipt {
        advance_unrelated_global(&authority).await;
    } else if scenario == PendingScenario::AccountAdmission {
        authority
            .ensure_account(&uuid::Uuid::now_v7().to_string(), "new principal", "human")
            .await
            .unwrap();
    }
    let remote_checkpoint = if advance_after_descriptor {
        authority
            .execute(
                "UPDATE lix_key_value SET value='R3' WHERE key='remote'",
                &[],
            )
            .await
            .unwrap();
        authority.execute("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))", &[]).await.unwrap();
        Some(
            authority
                .partial_replica_descriptor(None)
                .await
                .unwrap()
                .selected_branch
                .checkpoint
                .commit_id,
        )
    } else {
        None
    };
    // Native foreground write stays offline while an exact remote wave is pending.
    session
        .execute("UPDATE lix_key_value SET value='L2' WHERE key='local'", &[])
        .await
        .unwrap();
    let created_branch = if scenario == PendingScenario::LocalBranch {
        let branch = global_during_merge::create_branch(&session, &storage, &old, &transport).await;
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            "UPDATE lix_key_value SET value='L3' WHERE key='local'",
            &[],
            &mut Fetches::default(),
        )
        .await
        .unwrap();
        Some(branch)
    } else {
        None
    };
    // Recover the captured L result first; exact local CAS prevents adopting over L2.
    let second = prepare_descriptor_with_merge(
        engine.clone(),
        old.clone(),
        &transport,
        transport.partial_replica_descriptor(None).await.unwrap(),
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await;
    let second_error = second.err().expect("newer local work remains pending");
    assert_eq!(
        second_error.code, "LIX_PARTIAL_REPLICA_MERGE_PENDING",
        "{second_error:?}"
    );
    if scenario == PendingScenario::GlobalAfterReceipt {
        advance_unrelated_global(&authority).await;
    }
    let mut rounds = 0;
    let prepared = loop {
        rounds += 1;
        assert!(
            rounds <= 8,
            "bounded prefixes must converge after writers stop"
        );
        if created_branch.is_some() {
            global_during_merge::upload_if_ready(&storage, &old, &transport).await;
        }
        let result = prepare_descriptor_with_merge(
            engine.clone(),
            old.clone(),
            &transport,
            transport.partial_replica_descriptor(None).await.unwrap(),
            crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
        )
        .await;
        match result {
            Ok(crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared)) => {
                break prepared;
            }
            Err(error) if error.code == "LIX_PARTIAL_REPLICA_MERGE_PENDING" => {}
            Err(error)
                if created_branch.is_some()
                    && error.code == "LIX_PARTIAL_REPLICA_REBASE_REQUIRED" => {}
            Err(error) => panic!("prefix reconciliation failed: {error}"),
            Ok(_) => panic!("merged descriptor must be prepared"),
        }
    };
    if offline_commits > 1024 {
        assert!(rounds >= 4, "large local history must use bounded prefixes");
    }
    crate::sync::partial_publication::publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    let local = session
        .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
        .await
        .unwrap();
    let remote = session
        .execute("SELECT value FROM lix_key_value WHERE key='remote'", &[])
        .await
        .unwrap();
    assert!(format!("{local:?}").contains(if created_branch.is_some() { "L3" } else { "L2" }));
    if let Some(branch) = created_branch {
        assert!(
            authority
                .partial_replica_descriptor(Some(&branch))
                .await
                .is_ok(),
            "GLOBAL upload must publish the branch whose source was pending L2"
        );
        assert_eq!(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
                .await
                .unwrap()
                .rows(),
            authority
                .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
                .await
                .unwrap()
                .rows(),
        );
    }
    assert!(format!("{remote:?}").contains(if advance_after_descriptor { "R3" } else { "R" }));
    if let Some(checkpoint) = pending_checkpoint.or(remote_checkpoint) {
        assert_eq!(
            authority
                .partial_replica_descriptor(None)
                .await
                .unwrap()
                .selected_branch
                .checkpoint
                .commit_id,
            checkpoint,
            "only a newly captured checkpoint intent changes the authority checkpoint"
        );
    }
}

#[tokio::test]
async fn file_checkpoint_upload_retries_lost_ack_with_original_content() {
    file_checkpoint_upload_case(false, 1024 * 1024, false, None).await;
}

#[tokio::test]
async fn file_checkpoint_upload_pages_offline_edits_and_repeated_checkpoints() {
    file_checkpoint_upload_case(true, 1024 * 1024, false, None).await;
}

#[tokio::test]
async fn file_checkpoint_upload_pages_aggregate_wire_bytes() {
    file_checkpoint_upload_case(true, 8 * 1024, false, None).await;
}

#[tokio::test]
async fn file_ordinary_upload_pages_aggregate_wire_bytes() {
    file_checkpoint_upload_case(true, 8 * 1024, true, None).await;
}

#[tokio::test]
async fn checkpoint_publishes_cold_authority_blobs_without_hydrating_content() {
    file_checkpoint_upload_case(false, 1024 * 1024, false, Some(false)).await;
}

#[tokio::test]
async fn checkpoint_publishes_deferred_authority_blobs_without_hydrating_content() {
    file_checkpoint_upload_case(false, 1024 * 1024, false, Some(true)).await;
}

async fn file_checkpoint_upload_case(
    paging: bool,
    wire_budget: usize,
    ordinary_only: bool,
    cold_blob: Option<bool>,
) {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/checkpoint.bin',$1)",
            &[Value::Blob(vec![17u8; 96 * 1024].into())],
        )
        .await
        .unwrap();
    if paging {
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('checkpoint-excluded','before')",
                &[],
            )
            .await
            .unwrap();
    }
    let cold_content = vec![31u8; 96 * 1024];
    let cold_id = crate::binary_cas::CanonicalBlobManifest::from_bytes(&cold_content).blob_id;
    if cold_blob.is_some() {
        authority
            .execute(
                "INSERT INTO lix_file(path,content) VALUES('/cold.bin',$1)",
                &[Value::Blob(cold_content.clone().into())],
            )
            .await
            .unwrap();
    }
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let publication_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let blob_reply = Arc::new(std::sync::Mutex::new(None));
    let transport = HttpSyncTransport::connect_with(
        CountPublicationRequests {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            requests: Arc::clone(&publication_requests),
            blob_reply: blob_reply.clone(),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let old = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            wrapper.wire,
        )
        .unwrap(),
    );
    let storage = StorageAdapter::new(Memory::new());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
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
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
            .await
            .unwrap();
    let engine = Arc::new(engine);
    engine
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

    let mut fetches = Fetches::default();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "SELECT id,path FROM lix_file WHERE path='/checkpoint.bin'",
        &[],
        &mut fetches,
    )
    .await
    .unwrap();
    if paging {
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            "UPDATE lix_key_value SET value='still-pending' WHERE key='checkpoint-excluded'",
            &[],
            &mut fetches,
        )
        .await
        .unwrap();
    }
    let mut content = Vec::new();
    for checkpoint_index in 0..if paging { 2 } else { 1 } {
        for edit in 0..if paging { 40 } else { 1 } {
            content = vec![(43 + checkpoint_index * 41 + edit) as u8; 96 * 1024];
            execute_hydrating(
                &session,
                &storage,
                &old,
                &authority,
                "UPDATE lix_file SET content=$1 WHERE path='/checkpoint.bin'",
                &[Value::Blob(content.clone().into())],
                &mut fetches,
            )
            .await
            .unwrap();
        }
        if !ordinary_only {
            let checkpoint_sql = if cold_blob.is_some() {
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file')))"
            } else {
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file') WHERE to_path='/checkpoint.bin'))"
            };
            execute_hydrating(
                &session,
                &storage,
                &old,
                &authority,
                checkpoint_sql,
                &[],
                &mut fetches,
            )
            .await
            .unwrap();
        }
    }
    if paging {
        content = vec![199u8; 96 * 1024];
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            "UPDATE lix_file SET content=$1 WHERE path='/checkpoint.bin'",
            &[Value::Blob(content.clone().into())],
            &mut fetches,
        )
        .await
        .unwrap();
    }
    let branch = &old.descriptor().selected_branch.branch_id;
    if let Some(deferred) = cold_blob {
        assert!(
            !crate::sync::partial_blob::manifest_is_resident(&storage, &old, cold_id)
                .await
                .unwrap()
        );
        let mut manifest = authority
            .get_sync_blob_manifest(&cold_id.to_hex())
            .await
            .unwrap()
            .unwrap();
        manifest.inline_bytes_base64 = None;
        if deferred {
            crate::sync::partial_blob::install_manifest(&storage, &old, cold_id, &manifest)
                .await
                .unwrap();
        }
        let mut unrelated = manifest.clone();
        unrelated.blob_id = "00".repeat(32);
        let mut malformed = manifest.clone();
        malformed.size_bytes += 1;
        let mut captured = None;
        for reply in [vec![], vec![unrelated], vec![malformed]] {
            *blob_reply.lock().unwrap() = Some(reply);
            let error = crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &old,
                branch,
                uuid::Uuid::now_v7().to_string(),
                32,
                wire_budget,
                |request| {
                    let (storage, old, transport) = (&storage, &old, &transport);
                    async move {
                        crate::sync::partial_blob_upload::push_partial_with_blobs(
                            storage, old, transport, &request,
                        )
                        .await
                    }
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
            let read = storage.begin_read(Default::default()).await.unwrap();
            let (push, _, _) =
                crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
                    .await
                    .unwrap();
            let pending = push
                .prepared
                .expect("unconfirmed content retains exact publication");
            if let Some(previous) = &captured {
                assert_eq!(previous, &pending);
            } else {
                captured = Some(pending);
            }
            assert!(publication_requests.lock().unwrap().is_empty());
        }
        *blob_reply.lock().unwrap() = None;
    }
    let first = crate::sync::partial_upload_cycle::upload_partial_once(
        &storage,
        &old,
        branch,
        uuid::Uuid::now_v7().to_string(),
        32,
        wire_budget,
        |request| {
            let (storage, old, transport) = (&storage, &old, &transport);
            async move {
                assert!(request.commits.len() <= 32);
                assert!(serde_json::to_vec(&request).unwrap().len() <= wire_budget);
                crate::sync::partial_blob_upload::push_partial_with_blobs(
                    storage, old, transport, &request,
                )
                .await?;
                Err(LixError::new(
                    "TEST_LOST_CHECKPOINT_ACK",
                    "authority accepted checkpoint before reply was lost",
                ))
            }
        },
    )
    .await
    .unwrap_err();
    assert_eq!(first.code, "TEST_LOST_CHECKPOINT_ACK");
    if !paging {
        let requests = publication_requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "96KiB checkpoint publication is one HTTP request: {requests:?}"
        );
        assert!(requests[0].ends_with("/sync/push"));
    }
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (pending, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
            .await
            .unwrap();
    let captured = pending
        .prepared
        .expect("lost response retains exact upload");
    drop(read);
    if paging {
        let too_small = crate::sync::partial_upload_cycle::upload_partial_once(
            &storage,
            &old,
            branch,
            uuid::Uuid::now_v7().to_string(),
            32,
            1,
            |_| async {
                panic!("a captured upload must not be recut or sent under a smaller retry budget")
            },
        )
        .await
        .unwrap_err();
        assert_eq!(too_small.code, "LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED");
        let read = storage.begin_read(Default::default()).await.unwrap();
        let (unchanged, _, _) =
            crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
                .await
                .unwrap();
        assert_eq!(unchanged.prepared.as_ref(), Some(&captured));
    }
    assert!(
        crate::sync::partial_upload_cycle::upload_partial_once(
            &storage,
            &old,
            branch,
            uuid::Uuid::now_v7().to_string(),
            32,
            wire_budget,
            |request| {
                let (storage, old, transport) = (&storage, &old, &transport);
                async move {
                    crate::sync::partial_blob_upload::push_partial_with_blobs(
                        storage, old, transport, &request,
                    )
                    .await
                }
            },
        )
        .await
        .unwrap()
    );
    let mut extra_waves = 0;
    let mut lost_checkpoint_ack = false;
    if paging {
        assert_eq!(
            captured.target.checkpoint, captured.expected.checkpoint,
            "ordinary source prefix must retain authority checkpoint"
        );
        loop {
            let result = crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &old,
                branch,
                uuid::Uuid::now_v7().to_string(),
                32,
                wire_budget,
                |request| {
                    let (storage, old, transport, lost) =
                        (&storage, &old, &transport, &mut lost_checkpoint_ack);
                    async move {
                        assert!(request.commits.len() <= 32);
                        assert!(serde_json::to_vec(&request).unwrap().len() <= wire_budget);
                        let changes_checkpoint = request
                            .ref_updates
                            .iter()
                            .any(|r| r.checkpoint_commit_id != r.expected_checkpoint_commit_id);
                        let response = crate::sync::partial_blob_upload::push_partial_with_blobs(
                            storage, old, transport, &request,
                        )
                        .await?;
                        if changes_checkpoint && !*lost {
                            *lost = true;
                            return Err(LixError::new(
                                "TEST_LOST_CHECKPOINT_ACK",
                                "lost checkpoint wave ACK",
                            ));
                        }
                        Ok(response)
                    }
                },
            )
            .await;
            match result {
                Ok(false) => break,
                Ok(true) => {}
                Err(error) if error.code == "TEST_LOST_CHECKPOINT_ACK" => {
                    // The worker refreshes descriptors between retries. Exact
                    // authority coordinates recover this ACK before refusing to
                    // replace the still-newer local working state.
                    let refreshed = prepare_descriptor_with_merge(
                        engine.clone(),
                        old.clone(),
                        &transport,
                        transport.partial_replica_descriptor(None).await.unwrap(),
                        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
                    )
                    .await;
                    assert_eq!(
                        refreshed
                            .err()
                            .expect("newer local state remains pending")
                            .code,
                        "LIX_PARTIAL_REPLICA_REBASE_REQUIRED"
                    );
                    let read = storage.begin_read(Default::default()).await.unwrap();
                    let (recovered, _, _) =
                        crate::sync::partial_push_state::load_partial_push_state(
                            &read, &old, branch,
                        )
                        .await
                        .unwrap();
                    assert!(
                        recovered.prepared.is_none(),
                        "descriptor retry settles the exact accepted wave"
                    );
                }
                Err(error) => panic!("paged checkpoint upload: {error}"),
            }
            let excluded = authority
                .execute(
                    "SELECT value FROM lix_key_value WHERE key='checkpoint-excluded'",
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(
                excluded.rows()[0]
                    .get::<serde_json::Value>("value")
                    .unwrap(),
                serde_json::json!("still-pending"),
                "intermediate selected checkpoints must preserve excluded working rows"
            );
            extra_waves += 1;
            assert!(
                extra_waves < 128,
                "checkpoint upload must make bounded progress"
            );
        }
        assert!(extra_waves >= 3);
        assert_eq!(lost_checkpoint_ack, !ordinary_only);
        if ordinary_only {
            assert_eq!(
                captured.target.checkpoint,
                old.descriptor().selected_branch.checkpoint.commit_id
            );
        }
    }
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (settled, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
            .await
            .unwrap();
    assert!(settled.prepared.is_none());
    if !paging {
        assert_eq!(settled.confirmed, captured.target);
    }
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(branch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.confirmed.head, control.head_commit_id.to_string());
    assert_eq!(
        Some(settled.confirmed.checkpoint.clone()),
        control
            .working_diff_checkpoint_commit_id
            .map(|id| id.to_string())
    );
    drop(read);
    let descriptor = authority
        .partial_replica_descriptor(Some(branch))
        .await
        .unwrap();
    assert_eq!(
        descriptor.selected_branch.checkpoint.commit_id,
        settled.confirmed.checkpoint
    );
    let result = authority
        .execute(
            "SELECT content FROM lix_file WHERE path='/checkpoint.bin'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.rows()[0].get::<Vec<u8>>("content").unwrap(), content);
    if let Some(deferred) = cold_blob {
        let cold = authority
            .execute("SELECT content FROM lix_file WHERE path='/cold.bin'", &[])
            .await
            .unwrap();
        assert_eq!(
            cold.rows()[0].get::<Vec<u8>>("content").unwrap(),
            cold_content
        );

        assert_eq!(
            crate::sync::partial_blob::manifest_is_resident(&storage, &old, cold_id)
                .await
                .unwrap(),
            deferred
        );
        let chunks = crate::binary_cas::CanonicalBlobManifest::from_bytes(&cold_content).chunks;
        let read = storage.begin_read(Default::default()).await.unwrap();
        for chunk in chunks {
            assert!(
                crate::binary_cas::load_verified_chunk(&read, chunk.hash)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                crate::binary_cas::chunk_presence_many(&read, &[chunk.hash])
                    .await
                    .unwrap(),
                vec![false]
            );
        }
    }
}

mod retained_files;

mod conflict_file;

mod global_during_merge;
mod included_upload;

mod combined_body_limit;
mod large_blob_upload;

mod branch_switch_recovery;

mod transaction_hydration;

mod recovery_latency;
