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
#[tokio::test]
async fn http_dispatcher_recovers_lost_wave_and_preserves_newer_local_edit() {
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
    let first = prepare_descriptor_with_merge(
        engine.clone(),
        old.clone(),
        &transport,
        transport.partial_replica_descriptor(None).await.unwrap(),
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
    drop(read);
    // Native foreground write stays offline while an exact remote wave is pending.
    session
        .execute("UPDATE lix_key_value SET value='L2' WHERE key='local'", &[])
        .await
        .unwrap();
    // Recover the captured L result first; exact local CAS prevents adopting over L2.
    let second = prepare_descriptor_with_merge(
        engine.clone(),
        old.clone(),
        &transport,
        transport.partial_replica_descriptor(None).await.unwrap(),
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await;
    assert_eq!(
        second.err().unwrap().code,
        "LIX_PARTIAL_REPLICA_MERGE_PENDING"
    );
    let third = prepare_descriptor_with_merge(
        engine.clone(),
        old.clone(),
        &transport,
        transport.partial_replica_descriptor(None).await.unwrap(),
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await
    .unwrap();
    let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = third else {
        panic!("merged descriptor must be prepared")
    };
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
    assert!(format!("{local:?}").contains("L2"));
    assert!(format!("{remote:?}").contains("R"));
}

#[tokio::test]
async fn file_checkpoint_upload_retries_lost_ack_with_original_content() {
    file_checkpoint_upload_case(false, 1024 * 1024, false).await;
}

#[tokio::test]
async fn file_checkpoint_upload_pages_offline_edits_and_repeated_checkpoints() {
    file_checkpoint_upload_case(true, 1024 * 1024, false).await;
}

#[tokio::test]
async fn file_checkpoint_upload_pages_aggregate_wire_bytes() {
    file_checkpoint_upload_case(true, 8 * 1024, false).await;
}

#[tokio::test]
async fn file_ordinary_upload_pages_aggregate_wire_bytes() {
    file_checkpoint_upload_case(true, 8 * 1024, true).await;
}

async fn file_checkpoint_upload_case(paging: bool, wire_budget: usize, ordinary_only: bool) {
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
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let transport = HttpSyncTransport::connect_with(
        Client {
            server,
            lose_body: Arc::new(AtomicBool::new(false)),
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
            execute_hydrating(&session, &storage, &old, &authority,
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file') WHERE to_path='/checkpoint.bin'))",
            &[], &mut fetches,
        ).await.unwrap();
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
}
