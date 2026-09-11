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
