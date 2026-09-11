//! Actual HTTP receipt recovery for concurrent native descriptor additions.
use super::*;
use crate::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use crate::sync::SyncTransportFuture;
use crate::sync::http::{HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse};
use http_body_util::BodyExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
#[derive(Clone)]
struct GlobalClient {
    server: LixServerProtocol<Memory>,
    lose_merge: Arc<AtomicBool>,
    lose_ordinary: Arc<AtomicBool>,
    lose_cleanup: Arc<AtomicBool>,
}
impl RawHttpClient for GlobalClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let merge = request.url.ends_with("/sync/migration/global/merge");
            let ordinary = request.url.ends_with("/sync/push");
            let cleanup = request.url.ends_with("/sync/migration/global/cleanup");
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
            if merge && status.is_success() && self.lose_merge.swap(false, Ordering::SeqCst) {
                return Err(LixError::new(
                    "TEST_LOST_GLOBAL_MERGE_REPLY",
                    "native M committed before disconnect",
                ));
            }
            if ordinary && status.is_success() && self.lose_ordinary.swap(false, Ordering::SeqCst) {
                return Err(LixError::new(
                    "TEST_LOST_ORDINARY_REPLY",
                    "native creation group committed before disconnect",
                ));
            }
            if cleanup && status.is_success() && self.lose_cleanup.swap(false, Ordering::SeqCst) {
                return Err(LixError::new(
                    "TEST_LOST_GLOBAL_CLEANUP_REPLY",
                    "pin cleanup committed before disconnect",
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
async fn replica(
    transport: &HttpSyncTransport<GlobalClient>,
) -> (
    Arc<Engine<Memory>>,
    SessionContext<Memory>,
    Arc<PartialReplicaState>,
) {
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let old = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            crate::ANONYMOUS_ACCOUNT_ID.into(),
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
    (engine, session, old)
}
async fn create_local(
    session: &SessionContext<Memory>,
    engine: &Engine<Memory>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<GlobalClient>,
    name: &str,
) -> String {
    let lease = transport
        .fork_native_baseline_lease(state.baseline_lease())
        .unwrap();
    let branch_id = uuid::Uuid::now_v7().to_string();
    let mut seen = BTreeSet::new();
    for _ in 0..512 {
        match session
            .create_branch(crate::CreateBranchOptions {
                id: Some(branch_id.clone()),
                name: name.into(),
                from_commit_id: None,
            })
            .await
        {
            Ok(branch) => return branch.id,
            Err(error) => {
                let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
                    .unwrap()
                    .unwrap_or_else(|| panic!("unexpected create error: {error:?}"));
                assert!(
                    seen.insert(format!("{demand:?}")),
                    "repeated create dependency"
                );
                crate::sync::partial_runtime::hydrate_demand(
                    &engine.storage(),
                    state,
                    &lease,
                    demand,
                )
                .await
                .unwrap();
            }
        }
    }
    panic!("branch creation exceeded bounded native hydration");
}
async fn run_concurrent_global_creation(newer_global: bool) {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('resident','unchanged'),('race','before')",
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
    let lost = Arc::new(AtomicBool::new(false));
    let lost_cleanup = Arc::new(AtomicBool::new(false));
    let transport = HttpSyncTransport::connect_with(
        GlobalClient {
            server,
            lose_merge: lost.clone(),
            lose_ordinary: Arc::new(AtomicBool::new(false)),
            lose_cleanup: lost_cleanup.clone(),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let (a, sa, base_a) = replica(&transport).await;
    let (b, sb, base_b) = replica(&transport).await;
    let storage_b = b.storage();
    execute_hydrating(
        &sb,
        &storage_b,
        &base_b,
        &authority,
        "SELECT value FROM lix_key_value WHERE key IN ('resident','race')",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let child_a = create_local(&sa, &a, &base_a, &transport, "concurrent-a").await;
    let child_b = create_local(&sb, &b, &base_b, &transport, "concurrent-b").await;
    let storage_a = a.storage();
    let state_ref = base_a.as_ref();
    let storage_ref = &storage_a;
    let transport_ref = &transport;
    assert!(
        crate::sync::partial_upload_cycle::upload_partial_once(
            &storage_a,
            &base_a,
            crate::GLOBAL_BRANCH_ID,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
            |request| async move {
                crate::sync::partial_blob_upload::push_partial_with_blobs(
                    storage_ref,
                    state_ref,
                    transport_ref,
                    &request,
                )
                .await
            }
        )
        .await
        .unwrap()
    );
    lost.store(true, Ordering::SeqCst);
    let result = crate::sync::partial_global_merge_runtime::prepare_descriptor_with_global_merge(
        b.clone(),
        base_b.clone(),
        &transport,
        transport.partial_replica_descriptor(None).await.unwrap(),
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await;
    assert_eq!(result.err().unwrap().code, "TEST_LOST_GLOBAL_MERGE_REPLY");
    let head = transport
        .partial_replica_descriptor(None)
        .await
        .unwrap()
        .wire
        .descriptor
        .global_branch
        .head
        .commit_id;
    let read = storage_b.begin_read(Default::default()).await.unwrap();
    let pending =
        crate::sync::partial_global_merge_state::load_partial_global_merge_state(&read, &base_b)
            .await
            .unwrap()
            .0
            .unwrap();
    assert!(pending.receipt.is_none());
    assert_eq!(pending.acknowledged_roots.len(), 2);
    drop(read);
    let child_c = if newer_global {
        Some(create_local(&sb, &b, &base_b, &transport, "concurrent-c-after-capture").await)
    } else {
        None
    };
    // This bare session has no public SQL demand worker. Resolve only the
    // exact dependencies reported by the UPDATE after its GLOBAL basis changed.
    let lease = transport
        .fork_native_baseline_lease(base_b.baseline_lease())
        .unwrap();
    let mut seen = BTreeSet::new();
    loop {
        match sb
            .execute(
                "UPDATE lix_key_value SET value='after-capture' WHERE key='race'",
                &[],
            )
            .await
        {
            Ok(_) => break,
            Err(error) => {
                let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
                    .unwrap()
                    .unwrap_or_else(|| panic!("unexpected race UPDATE error: {error:?}"));
                assert!(
                    seen.len() < 512 && seen.insert(format!("{demand:?}")),
                    "repeated race UPDATE dependency"
                );
                crate::sync::partial_runtime::hydrate_demand(&storage_b, &base_b, &lease, demand)
                    .await
                    .unwrap();
            }
        }
    }
    let mut prepared =
        crate::sync::partial_global_merge_runtime::prepare_descriptor_with_global_merge(
            b.clone(),
            base_b.clone(),
            &transport,
            transport.partial_replica_descriptor(None).await.unwrap(),
            crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
        )
        .await
        .unwrap();
    if newer_global {
        assert!(matches!(
            prepared,
            crate::sync::partial_reconcile::PreparedDescriptor::LocalProgress
        ));
        let read = storage_b.begin_read(Default::default()).await.unwrap();
        let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
            &read,
            &base_b,
            crate::GLOBAL_BRANCH_ID,
        )
        .await
        .unwrap();
        let control =
            crate::branch::observe_branch_control_coordinate(&read, crate::GLOBAL_BRANCH_ID)
                .await
                .unwrap()
                .control
                .unwrap();
        assert_eq!(
            push.confirmed.head,
            pending.request.captured_local_head_commit_id
        );
        assert_ne!(
            control.head_commit_id.to_string(),
            push.confirmed.head,
            "prefix ACK must preserve L2"
        );
        drop(read);
        prepared = crate::sync::partial_global_merge_runtime::prepare_descriptor_with_global_merge(
            b.clone(),
            base_b.clone(),
            &transport,
            transport.partial_replica_descriptor(None).await.unwrap(),
            crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
        )
        .await
        .unwrap();
    }
    let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = prepared else {
        panic!("expected GLOBAL adoption")
    };
    crate::sync::partial_publication::publish_prepared_partial(b.clone(), prepared)
        .await
        .unwrap();
    let final_head = transport
        .partial_replica_descriptor(None)
        .await
        .unwrap()
        .wire
        .descriptor
        .global_branch
        .head
        .commit_id;
    if newer_global {
        assert_ne!(final_head, head, "L2 requires its own native merge");
    } else {
        assert_eq!(
            final_head, head,
            "lost reply retry must not create another M"
        );
    }
    for child in [&child_a, &child_b].into_iter().chain(child_c.iter()) {
        assert_eq!(
            transport
                .partial_replica_descriptor(Some(child))
                .await
                .unwrap()
                .wire
                .descriptor
                .selected_branch
                .branch_id,
            *child
        );
    }
    let current = b.sync_mode().partial_admission().unwrap();
    lost_cleanup.store(true, Ordering::SeqCst);
    let error = crate::sync::partial_global_merge_runtime::cleanup_adopted_global_attempt(
        &storage_b, &current, &transport,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "TEST_LOST_GLOBAL_CLEANUP_REPLY");
    let read = storage_b.begin_read(Default::default()).await.unwrap();
    assert!(
        crate::sync::partial_global_merge_state::load_partial_global_merge_state(&read, &current)
            .await
            .unwrap()
            .0
            .unwrap()
            .upload_settled
    );
    drop(read);

    assert!(
        crate::sync::partial_global_merge_runtime::cleanup_adopted_global_attempt(
            &b.storage(),
            &current,
            &transport
        )
        .await
        .unwrap()
    );
    assert!(
        !crate::sync::partial_global_merge_runtime::cleanup_adopted_global_attempt(
            &b.storage(),
            &current,
            &transport
        )
        .await
        .unwrap()
    );
    assert_eq!(
        value(
            sb.execute("SELECT value FROM lix_key_value WHERE key='race'", &[])
                .await
                .unwrap()
        ),
        "after-capture"
    );
    assert_eq!(
        value(
            sb.execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        ),
        "unchanged"
    );
}

#[tokio::test]
async fn concurrent_global_branch_creation_recovers_lost_merge_reply_and_cleans_pin() {
    run_concurrent_global_creation(false).await;
}
#[tokio::test]
async fn concurrent_global_branch_creation_settles_only_captured_prefix_before_merging_l2() {
    run_concurrent_global_creation(true).await;
}

#[tokio::test]
async fn lost_ordinary_creation_ack_then_later_global_advance_recovers_by_native_inclusion() {
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
    let lost = Arc::new(AtomicBool::new(true));
    let transport = HttpSyncTransport::connect_with(
        GlobalClient {
            server,
            lose_merge: Arc::new(AtomicBool::new(false)),
            lose_ordinary: lost,
            lose_cleanup: Arc::new(AtomicBool::new(false)),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let (engine, session, base) = replica(&transport).await;
    let child = create_local(&session, &engine, &base, &transport, "lost-ordinary-child").await;
    let storage = engine.storage();
    let storage_ref = &storage;
    let state_ref = base.as_ref();
    let transport_ref = &transport;
    let error = crate::sync::partial_upload_cycle::upload_partial_once(
        &storage,
        &base,
        crate::GLOBAL_BRANCH_ID,
        uuid::Uuid::now_v7().to_string(),
        32,
        1024 * 1024,
        |request| async move {
            crate::sync::partial_blob_upload::push_partial_with_blobs(
                storage_ref,
                state_ref,
                transport_ref,
                &request,
            )
            .await
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "TEST_LOST_ORDINARY_REPLY");
    crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
        .await
        .unwrap();
    authority
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "later-authority-child".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let remote_head = wrapper.wire.descriptor.global_branch.head.commit_id.clone();
    let result = crate::sync::partial_global_merge_runtime::prepare_descriptor_with_global_merge(
        engine.clone(),
        base.clone(),
        &transport,
        wrapper,
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await
    .unwrap();
    let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = result else {
        panic!("inclusion must prepare current authority descriptor")
    };
    crate::sync::partial_publication::publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    assert_eq!(
        transport
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .wire
            .descriptor
            .global_branch
            .head
            .commit_id,
        remote_head,
        "inclusion recovery must not manufacture a duplicate descriptor merge"
    );
    assert_eq!(
        transport
            .partial_replica_descriptor(Some(&child))
            .await
            .unwrap()
            .wire
            .descriptor
            .selected_branch
            .branch_id,
        child
    );
    let current = engine.sync_mode().partial_admission().unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        crate::sync::partial_global_merge_state::load_partial_global_merge_state(&read, &current)
            .await
            .unwrap()
            .0
            .is_none()
    );
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
        &read,
        &current,
        crate::GLOBAL_BRANCH_ID,
    )
    .await
    .unwrap();
    assert!(push.prepared.is_none());
    assert_eq!(push.confirmed.head, remote_head);
}
